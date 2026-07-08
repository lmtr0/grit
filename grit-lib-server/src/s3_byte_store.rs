//! Amazon S3 byte store for externalized repository payloads.

use async_trait::async_trait;
use aws_sdk_s3::primitives::ByteStream;

use crate::error::{Error, Result};
use crate::external::ExternalByteStore;

/// External byte store backed by Amazon S3 or an S3-compatible service.
#[derive(Clone)]
pub struct S3ByteStore {
    client: aws_sdk_s3::Client,
    bucket: String,
}

impl S3ByteStore {
    /// Create an S3 byte store from an existing SDK client and bucket name.
    #[must_use]
    pub fn new(client: aws_sdk_s3::Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
        }
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
}

#[async_trait]
impl ExternalByteStore for S3ByteStore {
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await
            .map_err(|err| Error::Backend(format!("put s3 object {key}: {err}")))?;
        Ok(())
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

fn is_missing_object_error<E>(err: &E) -> bool
where
    E: std::fmt::Display,
{
    let text = err.to_string();
    text.contains("NoSuchKey") || text.contains("NotFound")
}
