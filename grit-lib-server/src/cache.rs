//! Cache and invalidation traits for server-hosted repositories.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::ids::{RepositoryId, TenantId};

/// Current cache-key namespace version.
pub const CACHE_KEY_VERSION: u16 = 1;

/// Current serialized cache-value envelope version.
pub const CACHE_VALUE_VERSION: u16 = 1;

/// Current invalidation-event wire version.
pub const INVALIDATION_EVENT_VERSION: u16 = 1;

/// Cache key for server repository data.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CacheKey {
    /// Object lookup by hex object id.
    Object(String),
    /// Ref lookup by ref name.
    Ref(String),
    /// Ref-list snapshot by prefix.
    RefList(String),
    /// Parsed commit summary by hex commit object id.
    CommitSummary(String),
    /// Direct-child tree listing by tree object id and name prefix.
    TreeList { tree: String, prefix: String },
    /// Direct-child tree/name lookup.
    TreePath { tree: String, path: String },
    /// Config lookup by key.
    Config(String),
}

impl CacheKey {
    /// Return the cache-key namespace version.
    #[must_use]
    pub fn version(&self) -> u16 {
        CACHE_KEY_VERSION
    }
}

/// Typed payload stored in a cache backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheValueKind {
    /// Unclassified bytes for compatibility with generic callers.
    Raw,
    /// Serialized object payload.
    Object,
    /// Serialized ref payload.
    Ref,
    /// Serialized ref-list payload.
    RefList,
    /// Serialized commit summary payload.
    CommitSummary,
    /// Serialized tree-list payload.
    TreeList,
    /// Serialized config value payload.
    Config,
}

impl CacheValueKind {
    fn code(self) -> u8 {
        match self {
            Self::Raw => 0,
            Self::Object => 1,
            Self::Ref => 2,
            Self::RefList => 3,
            Self::CommitSummary => 4,
            Self::TreeList => 5,
            Self::Config => 6,
        }
    }

    fn from_code(code: u8) -> Result<Self> {
        match code {
            0 => Ok(Self::Raw),
            1 => Ok(Self::Object),
            2 => Ok(Self::Ref),
            3 => Ok(Self::RefList),
            4 => Ok(Self::CommitSummary),
            5 => Ok(Self::TreeList),
            6 => Ok(Self::Config),
            other => Err(Error::Cache(format!("unknown cache value kind {other}"))),
        }
    }
}

/// Value stored in a cache backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheValue {
    /// Cache value envelope version.
    pub version: u16,
    /// Typed payload kind.
    pub kind: CacheValueKind,
    /// Opaque cache payload bytes.
    pub bytes: Vec<u8>,
}

impl CacheValue {
    /// Create a raw cache value from bytes.
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            version: CACHE_VALUE_VERSION,
            kind: CacheValueKind::Raw,
            bytes: bytes.into(),
        }
    }

    /// Create a typed cache value from bytes.
    #[must_use]
    pub fn typed(kind: CacheValueKind, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            version: CACHE_VALUE_VERSION,
            kind,
            bytes: bytes.into(),
        }
    }

    /// Encode this cache value into a stable binary envelope for remote caches.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(3 + self.bytes.len());
        out.extend_from_slice(&self.version.to_be_bytes());
        out.push(self.kind.code());
        out.extend_from_slice(&self.bytes);
        out
    }

    /// Decode a stable binary cache-value envelope.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] when the envelope is truncated, has an unsupported version, or
    /// names an unknown payload kind.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 3 {
            return Err(Error::Cache("cache value envelope is truncated".to_owned()));
        }
        let version = u16::from_be_bytes([bytes[0], bytes[1]]);
        if version != CACHE_VALUE_VERSION {
            return Err(Error::Cache(format!(
                "unsupported cache value version {version}"
            )));
        }
        Ok(Self {
            version,
            kind: CacheValueKind::from_code(bytes[2])?,
            bytes: bytes[3..].to_vec(),
        })
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

    /// Invalidate every cached value scoped to a repository.
    async fn invalidate_repository(
        &self,
        _tenant: &TenantId,
        _repository: &RepositoryId,
    ) -> Result<()> {
        Ok(())
    }
}

/// Kind of durable-state change represented by an invalidation event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InvalidationEventKind {
    /// An object was written.
    ObjectWrite {
        /// Hex object id that changed.
        oid: String,
    },
    /// A ref was written or replaced.
    RefWrite {
        /// Full ref name that changed.
        refname: String,
    },
    /// A ref was deleted.
    RefDelete {
        /// Full ref name that was deleted.
        refname: String,
    },
    /// A repository was deleted.
    RepositoryDelete,
    /// The tree browse index was rebuilt.
    TreeIndexRebuild {
        /// Optional root tree object id when only one tree was rebuilt.
        tree: Option<String>,
    },
    /// Repository config changed.
    ConfigUpdate {
        /// Config key that changed.
        key: String,
    },
}

/// Event emitted after durable repository state changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidationEvent {
    /// Invalidation event wire version.
    pub version: u16,
    /// Tenant whose repository changed.
    pub tenant: TenantId,
    /// Repository that changed.
    pub repository: RepositoryId,
    /// Durable-state change that drives cache invalidation.
    pub kind: InvalidationEventKind,
}

impl InvalidationEvent {
    /// Create a versioned invalidation event.
    #[must_use]
    pub fn new(tenant: TenantId, repository: RepositoryId, kind: InvalidationEventKind) -> Self {
        Self {
            version: INVALIDATION_EVENT_VERSION,
            tenant,
            repository,
            kind,
        }
    }

    /// Return the precise cache keys invalidated by this event when they can be enumerated.
    #[must_use]
    pub fn cache_keys(&self) -> Vec<CacheKey> {
        match &self.kind {
            InvalidationEventKind::ObjectWrite { oid } => {
                vec![
                    CacheKey::Object(oid.clone()),
                    CacheKey::CommitSummary(oid.clone()),
                ]
            }
            InvalidationEventKind::RefWrite { refname }
            | InvalidationEventKind::RefDelete { refname } => {
                let mut keys = vec![CacheKey::Ref(refname.clone())];
                keys.extend(
                    ref_list_prefixes(refname)
                        .into_iter()
                        .map(CacheKey::RefList),
                );
                keys
            }
            InvalidationEventKind::RepositoryDelete
            | InvalidationEventKind::TreeIndexRebuild { .. } => Vec::new(),
            InvalidationEventKind::ConfigUpdate { key } => vec![CacheKey::Config(key.clone())],
        }
    }

    /// Return whether this event should clear all repository-scoped cache entries.
    #[must_use]
    pub fn invalidates_repository(&self) -> bool {
        matches!(
            self.kind,
            InvalidationEventKind::RepositoryDelete
                | InvalidationEventKind::TreeIndexRebuild { .. }
        )
    }

    /// Serialize this event to versioned JSON bytes for pub/sub transport.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] if JSON serialization fails.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(&WireInvalidationEvent::from(self))
            .map_err(|err| Error::Cache(format!("serialize invalidation event: {err}")))
    }

    /// Deserialize a versioned JSON invalidation event from pub/sub transport bytes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Cache`] if JSON decoding fails, the wire version is unsupported, or the
    /// tenant/repository identifiers are invalid.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self> {
        let wire: WireInvalidationEvent = serde_json::from_slice(bytes)
            .map_err(|err| Error::Cache(format!("decode invalidation event: {err}")))?;
        if wire.version != INVALIDATION_EVENT_VERSION {
            return Err(Error::Cache(format!(
                "unsupported invalidation event version {}",
                wire.version
            )));
        }
        Ok(Self {
            version: wire.version,
            tenant: TenantId::new(wire.tenant)?,
            repository: RepositoryId::new(wire.repository)?,
            kind: wire.kind.into(),
        })
    }
}

/// Pub/sub invalidation channel.
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish an invalidation event after the durable transaction commits.
    async fn publish_invalidation(&self, event: InvalidationEvent) -> Result<()>;
}

/// Apply an invalidation event to a cache backend.
///
/// # Errors
///
/// Returns cache backend errors from repository or key invalidation.
pub async fn apply_invalidation<C>(cache: &C, event: &InvalidationEvent) -> Result<()>
where
    C: Cache,
{
    if event.invalidates_repository() {
        return cache
            .invalidate_repository(&event.tenant, &event.repository)
            .await;
    }
    for key in event.cache_keys() {
        cache
            .invalidate(&event.tenant, &event.repository, &key)
            .await?;
    }
    Ok(())
}

fn ref_list_prefixes(refname: &str) -> Vec<String> {
    let mut prefixes = vec![String::new()];
    let mut prefix = String::new();
    for component in refname
        .split('/')
        .take_while(|component| !component.is_empty())
    {
        prefix.push_str(component);
        prefix.push('/');
        prefixes.push(prefix.clone());
    }
    prefixes
}

#[derive(Serialize, Deserialize)]
struct WireInvalidationEvent {
    version: u16,
    tenant: String,
    repository: String,
    kind: WireInvalidationEventKind,
}

impl From<&InvalidationEvent> for WireInvalidationEvent {
    fn from(event: &InvalidationEvent) -> Self {
        Self {
            version: event.version,
            tenant: event.tenant.as_str().to_owned(),
            repository: event.repository.as_str().to_owned(),
            kind: (&event.kind).into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum WireInvalidationEventKind {
    ObjectWrite { oid: String },
    RefWrite { refname: String },
    RefDelete { refname: String },
    RepositoryDelete,
    TreeIndexRebuild { tree: Option<String> },
    ConfigUpdate { key: String },
}

impl From<&InvalidationEventKind> for WireInvalidationEventKind {
    fn from(kind: &InvalidationEventKind) -> Self {
        match kind {
            InvalidationEventKind::ObjectWrite { oid } => Self::ObjectWrite { oid: oid.clone() },
            InvalidationEventKind::RefWrite { refname } => Self::RefWrite {
                refname: refname.clone(),
            },
            InvalidationEventKind::RefDelete { refname } => Self::RefDelete {
                refname: refname.clone(),
            },
            InvalidationEventKind::RepositoryDelete => Self::RepositoryDelete,
            InvalidationEventKind::TreeIndexRebuild { tree } => {
                Self::TreeIndexRebuild { tree: tree.clone() }
            }
            InvalidationEventKind::ConfigUpdate { key } => Self::ConfigUpdate { key: key.clone() },
        }
    }
}

impl From<WireInvalidationEventKind> for InvalidationEventKind {
    fn from(kind: WireInvalidationEventKind) -> Self {
        match kind {
            WireInvalidationEventKind::ObjectWrite { oid } => Self::ObjectWrite { oid },
            WireInvalidationEventKind::RefWrite { refname } => Self::RefWrite { refname },
            WireInvalidationEventKind::RefDelete { refname } => Self::RefDelete { refname },
            WireInvalidationEventKind::RepositoryDelete => Self::RepositoryDelete,
            WireInvalidationEventKind::TreeIndexRebuild { tree } => Self::TreeIndexRebuild { tree },
            WireInvalidationEventKind::ConfigUpdate { key } => Self::ConfigUpdate { key },
        }
    }
}
