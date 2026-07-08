//! Server-side storage layer for hosting Grit repositories.
//!
//! This crate defines the backend boundary needed to host Git repositories without a filesystem
//! repository as the source of truth. `grit-lib` remains the Git-compatible engine; this crate
//! supplies repository identity, storage traits, cache invalidation hooks, and backend
//! implementations that can be adapted into protocol serving and UI browsing paths.

pub mod cache;
pub mod cached;
pub mod error;
pub mod ids;
pub mod import;
pub mod layered;
pub mod memory;
#[cfg(feature = "nats")]
pub mod nats_invalidation;
pub mod policy;
pub mod protocol;
#[cfg(feature = "redis")]
pub mod redis_cache;
pub mod repository;
#[cfg(feature = "sqlx-postgres")]
pub mod sqlx_postgres;
pub mod storage;
pub mod views;

/// Commonly used server-layer types.
pub mod prelude {
    pub use crate::cache::{
        apply_invalidation, Cache, CacheKey, CacheValue, CacheValueKind, EventPublisher,
        InvalidationEvent, InvalidationEventKind,
    };
    pub use crate::cached::CachedStorage;
    pub use crate::error::{Error, Result};
    pub use crate::ids::{RepositoryId, TenantId};
    pub use crate::import::{import_repository, ImportReport};
    pub use crate::layered::LayeredCache;
    #[cfg(feature = "nats")]
    pub use crate::nats_invalidation::{NatsInvalidationPublisher, NatsInvalidationSubscriber};
    pub use crate::policy::{
        AuditEvent, AuditOutcome, AuditSink, AuthorizationContext, AuthorizationProvider,
        NoAuthorization, NoopAuditSink, PolicyActor, PolicyDecision, PolicyRefUpdate,
        RefUpdatePolicyContext, RepositoryPermission, RepositoryPolicy,
    };
    pub use crate::protocol::receive_pack::{
        AllowAllPushPolicy, ProtectedRefPolicy, PushCommandKind, PushCommandStatus, PushPlan,
        PushPolicy, PushPolicyContext, QuarantinedObject, ReceivePackCapability,
        ReceivePackCommand, ReceivePackReport, ReceivePackRequest, ReceivePackService,
    };
    pub use crate::protocol::upload_pack::{
        AdvertisedRef, FetchPackPlan, FetchPackResponse, GitProtocolVersion, RefAdvertisement,
        UploadPackCapability, UploadPackRequest, UploadPackService,
    };
    #[cfg(feature = "redis")]
    pub use crate::redis_cache::RedisCache;
    pub use crate::repository::ServerRepository;
    pub use crate::storage::{
        BrowseIndex, CommitGraphStore, ConfigStore, IndexedCommit, ObjectStore, RefStore,
        ReflogEntry, ReflogStore, StoredObject, StoredRef,
    };
    pub use crate::views::{
        BlobView, BranchView, CommitComparison, CommitHistoryOptions, CommitHistoryPage,
        CommitSummary, CompareInputs, DiscoveredFile, RepositorySummary, TagView, TreeEntryView,
        TreeView,
    };
}
