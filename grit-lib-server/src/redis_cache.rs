//! Redis cache backend for hosted repository data.

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::cache::{Cache, CacheKey, CacheValue, CACHE_KEY_VERSION};
use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};

/// Redis-backed cache using versioned repository-scoped keys.
#[derive(Clone)]
pub struct RedisCache {
    manager: ConnectionManager,
    namespace: String,
}

impl RedisCache {
    /// Create a Redis cache from an existing connection manager.
    ///
    /// `namespace` is prefixed to all keys and should be stable for one deployment.
    #[must_use]
    pub fn new(manager: ConnectionManager, namespace: impl Into<String>) -> Self {
        Self {
            manager,
            namespace: namespace.into(),
        }
    }

    /// Connect to Redis and create a cache with the default namespace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] when the URL is invalid or Redis cannot be reached.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_namespace(url, "grit-cache").await
    }

    /// Connect to Redis and create a cache with a custom namespace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] when the URL is invalid or Redis cannot be reached.
    pub async fn connect_with_namespace(url: &str, namespace: impl Into<String>) -> Result<Self> {
        let client = redis::Client::open(url)
            .map_err(|err| Error::Cache(format!("create redis client: {err}")))?;
        let manager = client
            .get_connection_manager()
            .await
            .map_err(|err| Error::Cache(format!("connect redis cache: {err}")))?;
        Ok(Self::new(manager, namespace))
    }

    fn key(&self, tenant: &TenantId, repository: &RepositoryId, key: &CacheKey) -> String {
        format!(
            "{}:v{}:{}:{}:{}",
            self.namespace,
            CACHE_KEY_VERSION,
            encode_component(tenant.as_str()),
            encode_component(repository.as_str()),
            key_component(key)
        )
    }

    fn repository_prefix(&self, tenant: &TenantId, repository: &RepositoryId) -> String {
        format!(
            "{}:v{}:{}:{}:",
            self.namespace,
            CACHE_KEY_VERSION,
            encode_component(tenant.as_str()),
            encode_component(repository.as_str())
        )
    }
}

#[async_trait]
impl Cache for RedisCache {
    async fn get(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<Option<CacheValue>> {
        let mut connection = self.manager.clone();
        let redis_key = self.key(tenant, repository, key);
        let bytes: Option<Vec<u8>> = redis::cmd("GET")
            .arg(redis_key)
            .query_async(&mut connection)
            .await
            .map_err(|err| Error::Cache(format!("redis GET failed: {err}")))?;
        bytes.map(|bytes| CacheValue::decode(&bytes)).transpose()
    }

    async fn put(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
        value: CacheValue,
    ) -> Result<()> {
        let mut connection = self.manager.clone();
        let redis_key = self.key(tenant, repository, key);
        redis::cmd("SET")
            .arg(redis_key)
            .arg(value.encode())
            .query_async::<()>(&mut connection)
            .await
            .map_err(|err| Error::Cache(format!("redis SET failed: {err}")))
    }

    async fn invalidate(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
        key: &CacheKey,
    ) -> Result<()> {
        let mut connection = self.manager.clone();
        let redis_key = self.key(tenant, repository, key);
        redis::cmd("DEL")
            .arg(redis_key)
            .query_async::<()>(&mut connection)
            .await
            .map_err(|err| Error::Cache(format!("redis DEL failed: {err}")))
    }

    async fn invalidate_repository(
        &self,
        tenant: &TenantId,
        repository: &RepositoryId,
    ) -> Result<()> {
        let mut connection = self.manager.clone();
        let pattern = format!("{}*", self.repository_prefix(tenant, repository));
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(pattern)
            .query_async(&mut connection)
            .await
            .map_err(|err| Error::Cache(format!("redis KEYS failed: {err}")))?;
        if keys.is_empty() {
            return Ok(());
        }
        redis::cmd("DEL")
            .arg(keys)
            .query_async::<()>(&mut connection)
            .await
            .map_err(|err| Error::Cache(format!("redis repository DEL failed: {err}")))
    }
}

fn key_component(key: &CacheKey) -> String {
    match key {
        CacheKey::Object(oid) => format!("object:{}", encode_component(oid)),
        CacheKey::Ref(refname) => format!("ref:{}", encode_component(refname)),
        CacheKey::RefList(prefix) => format!("ref-list:{}", encode_component(prefix)),
        CacheKey::CommitSummary(oid) => format!("commit-summary:{}", encode_component(oid)),
        CacheKey::TreeList { tree, prefix } => format!(
            "tree-list:{}:{}",
            encode_component(tree),
            encode_component(prefix)
        ),
        CacheKey::TreePath { tree, path } => {
            format!(
                "tree-path:{}:{}",
                encode_component(tree),
                encode_component(path)
            )
        }
        CacheKey::Config(key) => format!("config:{}", encode_component(key)),
    }
}

fn encode_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(out, "{byte:02x}");
    }
    out
}
