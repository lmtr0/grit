//! External byte storage for immutable hosted Git payloads.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use async_trait::async_trait;
use time::OffsetDateTime;

use crate::error::{Error, Result};
use crate::views::SignedContentUrl;

/// Immutable byte storage used by hybrid repository backends.
#[async_trait]
pub trait ExternalByteStore: Send + Sync {
    /// Store `bytes` at `key` when the key is not already present.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the external byte store.
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()>;

    /// Read all bytes stored at `key`.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the external byte store.
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;

    /// Read a byte range from `key`.
    ///
    /// The default implementation reads the full value and slices it locally. Backends with native
    /// ranged reads should override this method.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the external byte store or range conversion errors.
    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Vec<u8>>> {
        let Some(bytes) = self.get(key).await? else {
            return Ok(None);
        };
        let start = usize::try_from(start)
            .map_err(|_| Error::Backend("external byte range start exceeds usize".to_owned()))?;
        let len = usize::try_from(len)
            .map_err(|_| Error::Backend("external byte range length exceeds usize".to_owned()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| Error::Backend("external byte range overflow".to_owned()))?;
        if start >= bytes.len() {
            return Ok(Some(Vec::new()));
        }
        Ok(Some(bytes[start..end.min(bytes.len())].to_vec()))
    }

    /// Delete bytes stored at `key`.
    ///
    /// # Errors
    ///
    /// Returns backend errors from the external byte store.
    async fn delete(&self, key: &str) -> Result<()>;
}

/// Creates signed URLs for direct client reads from an external byte store.
#[async_trait]
pub trait ContentUrlSigner: Send + Sync {
    /// Return a signed URL for reading `key`.
    ///
    /// # Errors
    ///
    /// Returns signer backend errors or invalid expiration errors.
    async fn presign_get(
        &self,
        key: &str,
        issued_at: OffsetDateTime,
        expires_in: Duration,
    ) -> Result<SignedContentUrl>;
}

/// In-memory byte store for tests and local prototyping.
#[derive(Default)]
pub struct MemoryByteStore {
    bytes: RwLock<HashMap<String, Vec<u8>>>,
}

/// Deterministic signer for tests and local prototyping.
pub struct StaticContentUrlSigner {
    base_url: String,
}

impl StaticContentUrlSigner {
    /// Create a signer that appends storage keys to `base_url`.
    #[must_use]
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
        }
    }
}

impl MemoryByteStore {
    /// Create an empty in-memory byte store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Return the number of stored keys.
    ///
    /// # Errors
    ///
    /// Returns an error when the internal lock is poisoned.
    pub fn len(&self) -> Result<usize> {
        self.bytes
            .read()
            .map(|bytes| bytes.len())
            .map_err(|_| Error::Backend("memory byte store lock poisoned".to_owned()))
    }

    /// Return whether the store contains no keys.
    ///
    /// # Errors
    ///
    /// Returns an error when the internal lock is poisoned.
    pub fn is_empty(&self) -> Result<bool> {
        self.bytes
            .read()
            .map(|bytes| bytes.is_empty())
            .map_err(|_| Error::Backend("memory byte store lock poisoned".to_owned()))
    }
}

#[async_trait]
impl ExternalByteStore for MemoryByteStore {
    async fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.bytes
            .write()
            .map(|mut stored| {
                stored
                    .entry(key.to_owned())
                    .or_insert_with(|| bytes.to_vec());
            })
            .map_err(|_| Error::Backend("memory byte store lock poisoned".to_owned()))
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.bytes
            .read()
            .map(|stored| stored.get(key).cloned())
            .map_err(|_| Error::Backend("memory byte store lock poisoned".to_owned()))
    }

    async fn get_range(&self, key: &str, start: u64, len: u64) -> Result<Option<Vec<u8>>> {
        let start = usize::try_from(start)
            .map_err(|_| Error::Backend("memory byte range start exceeds usize".to_owned()))?;
        let len = usize::try_from(len)
            .map_err(|_| Error::Backend("memory byte range length exceeds usize".to_owned()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| Error::Backend("memory byte range overflow".to_owned()))?;
        self.bytes
            .read()
            .map(|stored| {
                stored.get(key).map(|bytes| {
                    if start >= bytes.len() {
                        Vec::new()
                    } else {
                        bytes[start..end.min(bytes.len())].to_vec()
                    }
                })
            })
            .map_err(|_| Error::Backend("memory byte store lock poisoned".to_owned()))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.bytes
            .write()
            .map(|mut stored| {
                stored.remove(key);
            })
            .map_err(|_| Error::Backend("memory byte store lock poisoned".to_owned()))
    }
}

#[async_trait]
impl ContentUrlSigner for StaticContentUrlSigner {
    async fn presign_get(
        &self,
        key: &str,
        issued_at: OffsetDateTime,
        expires_in: Duration,
    ) -> Result<SignedContentUrl> {
        let expires_at = add_std_duration(issued_at, expires_in)?;
        Ok(SignedContentUrl {
            method: "GET".to_owned(),
            url: format!(
                "{}/{}?signed=1",
                self.base_url.trim_end_matches('/'),
                encode_url_path(key)
            ),
            headers: Vec::new(),
            expires_at,
        })
    }
}

pub(crate) fn add_std_duration(
    instant: OffsetDateTime,
    duration: Duration,
) -> Result<OffsetDateTime> {
    let seconds = i64::try_from(duration.as_secs())
        .map_err(|_| Error::Backend("signed URL duration exceeds i64 seconds".to_owned()))?;
    let nanos = i32::try_from(duration.subsec_nanos())
        .map_err(|_| Error::Backend("signed URL duration nanos exceeds i32".to_owned()))?;
    instant
        .checked_add(time::Duration::new(seconds, nanos))
        .ok_or_else(|| Error::Backend("signed URL expiration timestamp overflow".to_owned()))
}

fn encode_url_path(path: &str) -> String {
    path.split('/')
        .map(encode_url_component)
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_url_component(component: &str) -> String {
    let mut out = String::new();
    for byte in component.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}
