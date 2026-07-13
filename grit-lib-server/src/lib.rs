//! Server-side storage layer for hosting Grit repositories.
//!
//! This crate defines the backend boundary needed to host Git repositories without a filesystem
//! repository as the source of truth. `grit-lib` remains the Git-compatible engine; this crate
//! supplies repository identity, storage traits, cache invalidation hooks, and backend
//! implementations that can be adapted into protocol serving and UI browsing paths.

pub mod cache;
pub mod cached;
pub mod error;
pub mod external;
#[cfg(feature = "externalized-postgres")]
pub mod externalized;
pub mod ids;
pub mod import;
pub mod layered;
pub mod maintenance;
pub mod memory;
#[cfg(feature = "nats")]
pub mod nats_invalidation;
mod packfile;
pub mod policy;
pub mod protocol;
#[cfg(feature = "redis")]
pub mod redis_cache;
pub mod repository;
#[cfg(feature = "s3")]
pub mod s3_byte_store;
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
    pub use crate::external::{
        ContentUrlSigner, ExternalByteStore, MemoryByteStore, StaticContentUrlSigner,
    };
    #[cfg(feature = "externalized-postgres")]
    pub use crate::externalized::{ExternalStorageOptions, PgExternalizedStorage};
    pub use crate::ids::{RepositoryId, TenantId};
    pub use crate::import::{
        import_repository, import_repository_with_execution_options,
        import_repository_with_options, ImportCheckpoint, ImportExecutionOptions, ImportOptions,
        ImportProgressEvent, ImportReport, NativePackImportMode,
    };
    pub use crate::layered::LayeredCache;
    pub use crate::maintenance::{
        check_repository_consistency, export_repository, repair_browse_index, ConsistencyIssue,
        ConsistencyReport, ExportOptions, ExportReport,
    };
    #[cfg(feature = "nats")]
    pub use crate::nats_invalidation::{NatsInvalidationPublisher, NatsInvalidationSubscriber};
    pub use crate::policy::{
        AuditEvent, AuditOutcome, AuditSink, AuthorizationContext, AuthorizationProvider,
        NoAuthorization, NoopAuditSink, PolicyActor, PolicyDecision, PolicyRefUpdate,
        RefUpdatePolicyContext, RepositoryPermission, RepositoryPolicy,
    };
    pub use crate::protocol::receive_pack::{
        AllowAllPushPolicy, ProtectedRefPolicy, PushCommandKind, PushCommandStatus, PushPlan,
        PushPolicy, PushPolicyContext, QuarantinedObject, ReceivePackAdvertisedRef,
        ReceivePackCapability, ReceivePackCommand, ReceivePackRefAdvertisement, ReceivePackReport,
        ReceivePackRequest, ReceivePackService,
    };
    pub use crate::protocol::upload_pack::{
        AdvertisedRef, FetchPackPlan, FetchPackResponse, GitProtocolVersion, RefAdvertisement,
        UploadPackCapability, UploadPackRequest, UploadPackService,
    };
    #[cfg(feature = "redis")]
    pub use crate::redis_cache::{RedisCache, RedisCacheOptions};
    pub use crate::repository::ServerRepository;
    #[cfg(feature = "s3")]
    pub use crate::s3_byte_store::S3ByteStore;
    pub use crate::storage::{
        BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication, ImportPublicationResult,
        ImportSession, ImportStateStore, ImportedPack, IndexedCommit, ObjectStore, PackMetadata,
        PackObjectIndex, PackStore, PackedObject, RefStore, ReflogEntry, ReflogStore, RepackPlan,
        StoredObject, StoredPack, StoredRef,
    };
    pub use crate::views::{
        BlobContentDelivery, BlobContentView, BlobDownloadOptions, BlobDownloadView,
        BlobMetadataView, BlobView, BranchView, CommitComparison, CommitHistoryOptions,
        CommitHistoryPage, CommitSummary, CompareInputs, DiscoveredFile, RepositorySummary,
        SignedContentUrl, TagView, TreeEntryView, TreeView,
    };
}
