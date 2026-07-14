//! Amazon S3 byte store for externalized repository payloads.

use async_trait::async_trait;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::future::Future;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::external::{add_std_duration, ContentUrlSigner, ExternalByteStore};
use crate::views::SignedContentUrl;

const MEBIBYTE: usize = 1024 * 1024;
const MIN_MULTIPART_PART_SIZE: usize = 5 * MEBIBYTE;
const MAX_MULTIPART_PART_SIZE_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_PARTS: usize = 10_000;
const MAX_MULTIPART_CONCURRENCY: usize = 64;

/// Tuning options for large S3 uploads.
///
/// Values are normalized on construction to satisfy S3's multipart limits and to cap the number
/// of concurrent part buffers. Individual uploads may increase the effective part size further so
/// that no upload uses more than 10,000 parts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct S3MultipartUploadOptions {
    threshold: usize,
    part_size: usize,
    max_concurrency: usize,
}

impl S3MultipartUploadOptions {
    /// Create normalized multipart upload options.
    ///
    /// `threshold` is the smallest value eligible for multipart upload, `part_size` is the desired
    /// bytes per request, and `max_concurrency` bounds both in-flight requests and cloned part
    /// buffers. The threshold and part size are clamped to at least S3's 5 MiB minimum non-final
    /// part size, the part size is capped at 5 GiB, and concurrency is clamped to `1..=64`.
    #[must_use]
    pub fn new(threshold: usize, part_size: usize, max_concurrency: usize) -> Self {
        Self {
            threshold: threshold.max(MIN_MULTIPART_PART_SIZE),
            part_size: part_size.clamp(MIN_MULTIPART_PART_SIZE, maximum_multipart_part_size()),
            max_concurrency: max_concurrency.clamp(1, MAX_MULTIPART_CONCURRENCY),
        }
    }

    /// Return the smallest value eligible for multipart upload, in bytes.
    #[must_use]
    pub fn threshold(&self) -> usize {
        self.threshold
    }

    /// Return the configured target multipart part size, in bytes.
    #[must_use]
    pub fn part_size(&self) -> usize {
        self.part_size
    }

    /// Return the maximum number of concurrent part requests and buffers.
    #[must_use]
    pub fn max_concurrency(&self) -> usize {
        self.max_concurrency
    }
}

impl Default for S3MultipartUploadOptions {
    fn default() -> Self {
        Self::new(64 * MEBIBYTE, 8 * MEBIBYTE, 4)
    }
}

/// External byte store backed by Amazon S3 or an S3-compatible service.
#[derive(Clone)]
pub struct S3ByteStore {
    client: aws_sdk_s3::Client,
    bucket: String,
    multipart: S3MultipartUploadOptions,
}

impl S3ByteStore {
    /// Create an S3 byte store from an existing SDK client and bucket name.
    #[must_use]
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            multipart: S3MultipartUploadOptions::default(),
        }
    }

    /// Set the tuning options used for large multipart uploads.
    ///
    /// The supplied options have already been normalized by
    /// [`S3MultipartUploadOptions::new`]. Small values below the configured threshold continue to
    /// use a conditional single-request `PutObject`.
    #[must_use]
    pub fn with_multipart_options(mut self, options: S3MultipartUploadOptions) -> Self {
        self.multipart = options;
        self
    }

    /// Return the configured multipart upload options.
    #[must_use]
    pub fn multipart_options(&self) -> S3MultipartUploadOptions {
        self.multipart
    }

    /// Load AWS configuration from the environment and create an S3 byte store.
    ///
    /// # Errors
    ///
    /// This constructor does not contact S3; errors are returned by subsequent store operations.
    pub async fn from_env(bucket: impl Into<String>) -> Result<Self> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(Self::new(aws_sdk_s3::Client::new(&config), bucket))
    }

    /// Borrow the configured bucket name.
    #[must_use]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    async fn put_multipart_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if self.existing_object_matches(key, bytes.len()).await? {
            return Ok(());
        }

        let part_size = effective_part_size(bytes.len(), self.multipart.part_size)?;
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| Error::Backend(format!("create s3 multipart upload {key}: {err}")))?;
        let upload_id = response.upload_id().ok_or_else(|| {
            Error::Backend(format!(
                "create s3 multipart upload {key}: response omitted upload id"
            ))
        })?;

        let completed_parts = match self.upload_parts(key, upload_id, bytes, part_size).await {
            Ok(parts) => parts,
            Err(err) => return Err(self.abort_after_error(key, upload_id, err).await),
        };
        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();
        match self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .if_none_match("*")
            .send()
            .await
        {
            Ok(_) => {
                if self.existing_object_matches(key, bytes.len()).await? {
                    Ok(())
                } else {
                    Err(Error::Backend(format!(
                        "completed s3 multipart upload {key} but object is missing"
                    )))
                }
            }
            Err(err) if is_conditional_write_conflict_error(&err) => {
                let completion_error =
                    Error::Backend(format!("complete s3 multipart upload {key}: {err}"));
                if let Err(abort_error) = self.abort_multipart_upload(key, upload_id).await {
                    return Err(Error::Backend(format!(
                        "{completion_error}; additionally, {abort_error}"
                    )));
                }
                if self.existing_object_matches(key, bytes.len()).await? {
                    Ok(())
                } else {
                    Err(completion_error)
                }
            }
            Err(err) => {
                let error = Error::Backend(format!("complete s3 multipart upload {key}: {err}"));
                Err(self.abort_after_error(key, upload_id, error).await)
            }
        }
    }

    async fn upload_parts(
        &self,
        key: &str,
        upload_id: &str,
        bytes: &[u8],
        part_size: usize,
    ) -> Result<Vec<CompletedPart>> {
        let mut next_offset = 0;
        let mut next_part_number = 1_i32;
        let mut requests = FuturesUnordered::new();
        let mut completed = Vec::with_capacity(bytes.len().div_ceil(part_size));
        let mut first_error = None;

        while requests.len() < self.multipart.max_concurrency && next_offset < bytes.len() {
            let (request, end) = self.part_request(
                key,
                upload_id,
                bytes,
                next_offset,
                part_size,
                next_part_number,
            );
            requests.push(request);
            next_offset = end;
            next_part_number += 1;
        }

        while let Some(result) = requests.next().await {
            match result {
                Ok(part) if first_error.is_none() => completed.push(part),
                Ok(_) => {}
                Err(err) if first_error.is_none() => first_error = Some(err),
                Err(_) => {}
            }
            if first_error.is_none() && next_offset < bytes.len() {
                let (request, end) = self.part_request(
                    key,
                    upload_id,
                    bytes,
                    next_offset,
                    part_size,
                    next_part_number,
                );
                requests.push(request);
                next_offset = end;
                next_part_number += 1;
            }
        }

        if let Some(err) = first_error {
            return Err(err);
        }
        completed.sort_unstable_by_key(|part| part.part_number().unwrap_or_default());
        Ok(completed)
    }

    fn part_request<'a>(
        &'a self,
        key: &'a str,
        upload_id: &'a str,
        bytes: &[u8],
        offset: usize,
        part_size: usize,
        part_number: i32,
    ) -> (impl Future<Output = Result<CompletedPart>> + 'a, usize) {
        let end = offset.saturating_add(part_size).min(bytes.len());
        let body = bytes[offset..end].to_vec();
        (self.upload_part(key, upload_id, part_number, body), end)
    }

    async fn upload_part(
        &self,
        key: &str,
        upload_id: &str,
        part_number: i32,
        bytes: Vec<u8>,
    ) -> Result<CompletedPart> {
        let content_length = i64::try_from(bytes.len())
            .map_err(|_| Error::Backend("s3 multipart part length exceeds i64".to_owned()))?;
        let output = self
            .client
            .upload_part()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(part_number)
            .content_length(content_length)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|err| {
                Error::Backend(format!(
                    "upload s3 multipart part {part_number} for {key}: {err}"
                ))
            })?;
        let e_tag = output.e_tag().ok_or_else(|| {
            Error::Backend(format!(
                "upload s3 multipart part {part_number} for {key}: response omitted ETag"
            ))
        })?;
        Ok(CompletedPart::builder()
            .part_number(part_number)
            .e_tag(e_tag)
            .build())
    }

    async fn existing_object_matches(&self, key: &str, expected_len: usize) -> Result<bool> {
        let output = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(err) if is_missing_object_error(&err) => return Ok(false),
            Err(err) => return Err(Error::Backend(format!("head s3 object {key}: {err}"))),
        };
        let expected_len = i64::try_from(expected_len)
            .map_err(|_| Error::Backend("s3 object length exceeds i64".to_owned()))?;
        let actual_len = output.content_length().ok_or_else(|| {
            Error::Backend(format!(
                "head s3 object {key}: response omitted content length"
            ))
        })?;
        if actual_len != expected_len {
            return Err(Error::Backend(format!(
                "existing s3 object {key} has length {actual_len}, expected {expected_len}"
            )));
        }
        Ok(true)
    }

    async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<()> {
        self.client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|err| Error::Backend(format!("abort s3 multipart upload {key}: {err}")))?;
        Ok(())
    }

    async fn abort_after_error(&self, key: &str, upload_id: &str, error: Error) -> Error {
        match self.abort_multipart_upload(key, upload_id).await {
            Ok(()) => error,
            Err(abort_error) => Error::Backend(format!("{error}; additionally, {abort_error}")),
        }
    }
}

#[async_trait]
impl ExternalByteStore for S3ByteStore {
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .if_none_match("*")
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(err) if is_precondition_failed_error(&err) => Ok(()),
            Err(err) => Err(Error::Backend(format!("put s3 object {key}: {err}"))),
        }
    }

    async fn put_large_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if bytes.len() < self.multipart.threshold {
            return self.put_if_absent(key, bytes).await;
        }
        self.put_multipart_if_absent(key, bytes).await
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let response = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) if is_missing_object_error(&err) => return Ok(None),
            Err(err) => return Err(Error::Backend(format!("get s3 object {key}: {err}"))),
        };
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|err| Error::Backend(format!("read s3 object body {key}: {err}")))?;
        Ok(Some(bytes.to_vec()))
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Vec<u8>>> {
        if len == 0 {
            return Ok(Some(Vec::new()));
        }
        let end = start
            .checked_add(len)
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| Error::Backend("s3 range overflow".to_owned()))?;
        let response = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .range(format!("bytes={start}-{end}"))
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) if is_missing_object_error(&err) => return Ok(None),
            Err(err) if is_invalid_range_error(&err) => return Ok(Some(Vec::new())),
            Err(err) => return Err(Error::Backend(format!("get s3 object range {key}: {err}"))),
        };
        let bytes = response
            .body
            .collect()
            .await
            .map_err(|err| Error::Backend(format!("read s3 object range body {key}: {err}")))?;
        Ok(Some(bytes.to_vec()))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|err| Error::Backend(format!("delete s3 object {key}: {err}")))?;
        Ok(())
    }
}

fn effective_part_size(value_len: usize, configured_part_size: usize) -> Result<usize> {
    let minimum_for_part_limit = value_len.div_ceil(MAX_MULTIPART_PARTS);
    let part_size = configured_part_size
        .max(minimum_for_part_limit)
        .max(MIN_MULTIPART_PART_SIZE);
    if part_size > maximum_multipart_part_size() {
        return Err(Error::Backend(
            "s3 multipart value exceeds the 10,000 part limit".to_owned(),
        ));
    }
    Ok(part_size)
}

fn maximum_multipart_part_size() -> usize {
    usize::try_from(MAX_MULTIPART_PART_SIZE_BYTES).unwrap_or(usize::MAX)
}

#[async_trait]
impl ContentUrlSigner for S3ByteStore {
    async fn presign_get(
        &self,
        key: &str,
        issued_at: OffsetDateTime,
        expires_in: Duration,
    ) -> Result<SignedContentUrl> {
        let config = PresigningConfig::builder()
            .start_time(system_time_from_offset(issued_at)?)
            .expires_in(expires_in)
            .build()
            .map_err(|err| Error::Backend(format!("build s3 presigning config: {err}")))?;
        let request = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(config)
            .await
            .map_err(|err| Error::Backend(format!("presign s3 object {key}: {err}")))?;
        Ok(SignedContentUrl {
            method: request.method().to_owned(),
            url: request.uri().to_owned(),
            headers: request
                .headers()
                .map(|(key, value)| (key.to_owned(), value.to_owned()))
                .collect(),
            expires_at: add_std_duration(issued_at, expires_in)?,
        })
    }
}

fn is_missing_object_error<E>(err: &E) -> bool
where
    E: std::fmt::Debug + std::fmt::Display,
{
    let text = format!("{err} {err:?}");
    text.contains("NoSuchKey") || text.contains("NotFound") || text.contains("status code: 404")
}

fn is_precondition_failed_error<E>(err: &E) -> bool
where
    E: std::fmt::Debug + std::fmt::Display,
{
    let text = format!("{err} {err:?}");
    text.contains("PreconditionFailed")
        || text.contains("Precondition Failed")
        || text.contains("status code: 412")
}

fn is_conditional_write_conflict_error<E>(err: &E) -> bool
where
    E: std::fmt::Debug + std::fmt::Display,
{
    if is_precondition_failed_error(err) {
        return true;
    }
    let text = format!("{err} {err:?}");
    text.contains("ConditionalRequestConflict")
        || text.contains("Conditional Request Conflict")
        || text.contains("status code: 409")
}

fn is_invalid_range_error<E>(err: &E) -> bool
where
    E: std::fmt::Debug + std::fmt::Display,
{
    let text = format!("{err} {err:?}");
    text.contains("InvalidRange")
        || text.contains("Requested Range Not Satisfiable")
        || text.contains("status code: 416")
}

fn system_time_from_offset(value: OffsetDateTime) -> Result<SystemTime> {
    let seconds = value.unix_timestamp();
    if seconds < 0 {
        return Err(Error::Backend(
            "signed URL issue time is before unix epoch".to_owned(),
        ));
    }
    UNIX_EPOCH
        .checked_add(Duration::new(seconds as u64, value.nanosecond()))
        .ok_or_else(|| Error::Backend("signed URL issue time exceeds system time".to_owned()))
}
