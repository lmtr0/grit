//! Layered cache composition for local near caches over shared remote caches.

use std::sync::Arc;

use async_trait::async_trait;

use crate::cache::{Cache, CacheKey, CacheValue};
use crate::error::Result;
use crate::ids::{RepositoryId, TenantId};

/// Cache that reads through a near cache before a shared backing cache.
pub struct LayeredCache<N, F> {
    near: Arc<N>,
    far: Arc<F>,
}

impl<N, F> LayeredCache<N, F> {
    /// Create a layered cache.
    ///
    /// `near` is intended for local in-memory storage and `far` for a shared cache such as Redis.
    #[must_use]
    pub fn new(near: Arc<N>, far: Arc<F>) -> Self {
        Self { near, far }
    }

    /// Borrow the near cache layer.
    #[must_use]
    pub fn near(&self) -> &Arc<N> {
        &self.near
    }

    /// Borrow the far cache layer.
    #[must_use]
    pub fn far(&self) -> &Arc<F> {
        &self.far
    }
}

#[async_trait]
impl<N, F> Cache for LayeredCache<N, F>
where
    N: Cache,
    F: Cache,
{
    async fn get(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<Option<CacheValue>> {
        if let Some(value) = self.near.get(tenant, repository, key).await? {
            return Ok(Some(value));
        }
        let Some(value) = self.far.get(tenant, repository, key).await? else {
            return Ok(None);
        };
        self.near
            .put(tenant, repository, key, value.clone())
            .await?;
        Ok(Some(value))
    }

    async fn put(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
        value: CacheValue,
    ) -> Result<()> {
        self.far.put(tenant, repository, key, value.clone()).await?;
        self.near.put(tenant, repository, key, value).await
    }

    async fn invalidate(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<()> {
        self.near.invalidate(tenant, repository, key).await?;
        self.far.invalidate(tenant, repository, key).await
    }

    async fn invalidate_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        self.near.invalidate_repository(tenant, repository).await?;
        self.far.invalidate_repository(tenant, repository).await
    }
}
