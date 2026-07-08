//! Cache and invalidation traits for server-hosted repositories.

use async_trait::async_trait;

use crate::error::Result;
use crate::ids::{RepositoryId, TenantId};

/// Cache key for server repository data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CacheKey {
    /// Object lookup by hex object id.
    Object(String),
    /// Ref lookup by ref name.
    Ref(String),
    /// Ref-list snapshot by prefix.
    RefList(String),
    /// Tree/path lookup.
    TreePath { tree: String, path: String },
    /// Config lookup by key.
    Config(String),
}

/// Value stored in a cache backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheValue {
    /// Opaque cache payload bytes.
    pub bytes: Vec<u8>,
}

impl CacheValue {
    /// Create a cache value from bytes.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            bytes: bytes.into(),
        }
    }
}

/// A repository-scoped cache backend.
#[async_trait]
pub trait Cache: Send + Sync {
    /// Read a cached value.
    async fn get(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<Option<CacheValue>>;

    /// Store a cached value.
    async fn put(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
        value: CacheValue,
    ) -> Result<()>;

    /// Invalidate one cached value.
    async fn invalidate(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<()>;
}

/// Event emitted after durable repository state changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidationEvent {
    /// Tenant whose repository changed.
    pub tenant: TenantId,
    /// Repository that changed.
    pub repository: RepositoryId,
    /// Cache keys invalidated by the transaction.
    pub keys: Vec<CacheKey>,
}

/// Pub/sub invalidation channel.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish an invalidation event after the durable transaction commits.
    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()>;
}
