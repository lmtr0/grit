//! Server-side storage layer for hosting Grit repositories.
//!
//! This crate defines the backend boundary needed to host Git repositories without a filesystem
//! repository as the source of truth. `grit-lib` remains the Git-compatible engine; this crate
//! supplies repository identity, storage traits, cache invalidation hooks, and backend
//! implementations that can be adapted into protocol serving and UI browsing paths.

pub mod admission;
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
pub mod resilience;
#[cfg(feature = "s3")]
pub mod s3_byte_store;
#[cfg(feature = "sqlx-postgres")]
pub mod sqlx_postgres;
pub mod storage;
pub mod tree_block;
#[cfg(feature = "sqlx-postgres")]
pub mod tree_path;
pub mod views;

/// Commonly used server-layer types.
pub mod prelude {
    pub use crate::admission::{
        AdmissionConfigError, AdmissionController, AdmissionDecision, AdmissionIdentity,
        AdmissionLimits, AdmissionPermit, AdmissionRejectReason, AdmissionRequest,
        AdmissionSnapshot, AdmissionTicket, BackendPressure, ClassCapacities, ResourceClass,
        ResourceWeights, RetryGuidance,
    };
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
    pub use crate::externalized::{
        ExternalOrphanSweepReport, ExternalStorageOptions, PgExternalizedStorage,
    };
    pub use crate::ids::{RepositoryId, TenantId};
    pub use crate::import::{
        import_repository, import_repository_with_execution_options,
        import_repository_with_options, ImportCheckpoint, ImportDestinationMetrics,
        ImportExecutionOptions, ImportMetrics, ImportObjectKindMetrics, ImportObjectMetrics,
        ImportOptions, ImportPhase, ImportProgressEvent, ImportProgressSnapshot, ImportReport,
        ImportSourceMetrics, NativePackImportMode,
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
    pub use crate::protocol::canonical_clone::{
        CanonicalCloneClaimResult, CanonicalCloneDeepen, CanonicalCloneError, CanonicalCloneFilter,
        CanonicalCloneFlightState, CanonicalCloneGeneration, CanonicalCloneKey,
        CanonicalCloneLease, CanonicalCloneLocator, CanonicalCloneManifest,
        CanonicalCloneManifestState, CanonicalCloneMutationResult, CanonicalCloneReceiver,
        CanonicalCloneRepresentation, CanonicalCloneScope, CanonicalCloneShallow,
        CanonicalCloneShape, CanonicalCloneSingleflight, CanonicalCloneStore,
        CanonicalCloneStoreCas, CanonicalCloneToken, CanonicalCloneVerification, ClonePackPurpose,
        MAX_CANONICAL_CLONE_WANTS,
    };
    pub use crate::protocol::canonical_clone_serve::{
        lookup_canonical_clone, serve_canonical_clone, CanonicalCloneByteSource,
        CanonicalCloneDataError, CanonicalCloneLookup, CanonicalCloneLookupError,
        CanonicalCloneServeError, CanonicalCloneServeOptions, CanonicalCloneServeReport,
        DEFAULT_CANONICAL_CLONE_RANGE_BYTES,
    };
    pub use crate::protocol::clone_admission::{
        abort_admitted_clone, CloneAdmissionCost, CloneAdmissionError, CloneAdmissionRequest,
        CloneAdmissionScheduler, CloneAdmissionTicket, CloneExecutionPermit, CloneQueueLimits,
        CloneQueueWorkBudget, CloneQueueWorkLimit, CloneScheduleDecision,
    };
    pub use crate::protocol::clone_metrics::{
        CloneBackendKind, CloneBackendMetrics, CloneCacheMetrics, CloneCancellationMetrics,
        CloneLimitError, CloneLimits, CloneMemoryMetrics, CloneMetricsRecorder, CloneMetricsReport,
        CloneObjectKindMetrics, CloneObjectMetrics, CloneOutcome, ClonePackMetrics, ClonePhase,
        ClonePhaseMetrics,
    };
    pub use crate::protocol::delta_pack::{
        plan_bounded_deltas, BoundedDeltaPackPlan, DeltaEntryAction, DeltaFallbackReason,
        DeltaObjectInput, DeltaPackOptions, DeltaPackOptionsError, DeltaPackWriteReport,
        DeltaPlannedEntry, DeltaPlanningReport, DeltaWorkBudget, DeltaWorkLimit,
        GeneratedDeltaPlan, MAX_DELTA_CANDIDATE_COMPARISONS, MAX_DELTA_DEPTH,
        MAX_DELTA_RETAINED_BASE_BYTES, MAX_DELTA_WINDOW, MAX_GENERATED_DELTA_INSTRUCTION_BYTES,
    };
    pub use crate::protocol::negotiation::{
        negotiate_bounded, NegotiationCommit, NegotiationCommitParent, NegotiationError,
        NegotiationLimitKind, NegotiationLimits, NegotiationMetadataSource, NegotiationPlan,
        NegotiationSpillFactory, NegotiationSpillRole, NegotiationSpillSet, NegotiationTree,
        NegotiationTreeEntry, NegotiationWorkBudget, NegotiationWorkLimit, StableObjectOrdinal,
    };
    pub use crate::protocol::pack_entry_reuse::{
        PackDependencySet, PackEntryPlan, PackEntrySourceError, PackRecompressReason,
        PackReuseCapabilities, ReusedDirectEntry, ReusedOfsDeltaEntry, ReusedRefDeltaEntry,
        StoredPackEntryKind, ValidatedPackEntry, MAX_REUSED_DELTA_INSTRUCTION_BYTES,
        MAX_REUSED_ENTRY_HEADER_BYTES,
    };
    pub use crate::protocol::pack_stream::{
        BoundedVecPackSink, CancellationGate, CancellationProbe, CancellationReport, NeverCancel,
        PackAbortReason, PackCancellationToken, PackChunkSink, PackSinkOperation, PackSinkState,
        PackStreamError, PackStreamLimits, PackStreamReport,
    };
    pub use crate::protocol::pack_writer::{
        IncrementalPackWriter, PackObjectWriteReport, PackWriterError, PackWriterFailure,
        PackWriterReport, PackWriterState,
    };
    pub use crate::protocol::push_metrics::{
        PushBackendFailureReason, PushBackendMetrics, PushCancellationReason, PushMemoryMetrics,
        PushMetricCounter, PushMetricsError, PushMetricsRecorder, PushMetricsReport, PushOutcome,
        PushPackMetrics, PushPhase, PushPhaseMetrics, PushQuarantineMetrics, PushRejectionReason,
        ReceivePackLimitError, ReceivePackLimitKind, ReceivePackLimits,
    };
    pub use crate::protocol::push_pack_validation::{
        validate_quarantined_pack, AuthorizedPushBase, AuthorizedPushBaseError,
        AuthorizedPushBaseProvider, NoAuthorizedPushBases, PushPackByteSpan, PushPackEntryKind,
        PushPackIndexRow, PushPackValidationControl, PushPackValidationError,
        PushPackValidationObservation, PushPackValidationOptions, PushPackValidationWork,
        PushPackValidationWorkLimit, PushStructuralMetadata, PushStructuralObject,
        PushValidationSummary, ValidatedPushPack,
    };
    pub use crate::protocol::push_quarantine::{
        MemoryPackQuarantine, MemoryQuarantineLimits, PackQuarantine, QuarantineDiscard,
        QuarantineDiscardReport, QuarantineError, QuarantineFence, QuarantineId,
        QuarantineIndexAttestation, QuarantineManifest, QuarantineOperation, QuarantineOwnerToken,
        QuarantinePromotion, QuarantinePromotionId, QuarantineReceivePackSink, QuarantineScope,
        QuarantineScratch, QuarantineSnapshot, QuarantineState,
    };
    pub use crate::protocol::receive_pack::{
        AllowAllPushPolicy, ProtectedRefPolicy, PushCommandKind, PushCommandStatus, PushPlan,
        PushPolicy, PushPolicyContext, QuarantinedObject, ReceivePackAdvertisedRef,
        ReceivePackCapability, ReceivePackCommand, ReceivePackRefAdvertisement, ReceivePackReport,
        ReceivePackRequest, ReceivePackService,
    };
    pub use crate::protocol::receive_pack_stream::{
        receive_pack_stream, validate_buffered_receive_pack, BoundedReceivePackCollector,
        BufferedPackSource, PackChunkSource, PackChunkSourceError, PackSourceChunk,
        ReceivePackAbortReason, ReceivePackChunkSink, ReceivePackChunkSinkError,
        ReceivePackEnvelope, ReceivePackStreamControl, ReceivePackStreamError,
        ReceivePackStreamObservation, ReceivePackStreamOptions, ReceivePackStreamReport,
        MAX_RECEIVE_PACK_CHUNK_BYTES,
    };
    pub use crate::protocol::upload_pack::{
        AdvertisedRef, FetchPackPlan, FetchPackResponse, GitProtocolVersion, RefAdvertisement,
        UploadPackCapability, UploadPackRequest, UploadPackResponseStreamFailure,
        UploadPackResponseStreamReport, UploadPackService, UploadPackStreamFailure,
        UploadPackStreamPreflightError, UploadPackStreamReport,
    };
    pub use crate::protocol::upload_pack_wire::{
        UploadPackWireMode, UploadPackWireReport, UploadPackWireSink, UploadPackWireState,
    };
    #[cfg(feature = "redis")]
    pub use crate::redis_cache::{RedisCache, RedisCacheOptions};
    pub use crate::repository::ServerRepository;
    pub use crate::resilience::{
        route_read, ComponentRecoveryObservation, FailoverDecision, PackManifestState,
        PackReplicaState, PrimaryRouteReason, ReadOperation, ReadRoute, RecoveryBlocker,
        RecoveryComponent, RecoveryObjective, RecoveryPlan, RecoveryReadiness, RegionalPackReplica,
        ReplicaHealth, ReplicaObservation, ReplicaPolicy, RepositoryGeneration, ResilienceError,
        RestoreChecks,
    };
    #[cfg(feature = "s3")]
    pub use crate::s3_byte_store::{S3ByteStore, S3MultipartUploadOptions};
    #[cfg(feature = "sqlx-postgres")]
    pub use crate::sqlx_postgres::{
        PgCacheInvalidationEvent, PgCacheInvalidationKind, PgCacheOutboxClaim,
        PgCacheOutboxClaimOptions, PgCacheOutboxClaimResult, PgCommitHistoryCursor,
        PgCommitHistoryOptions, PgCommitHistoryPage, PgMigrationCatchUpApplyResult,
        PgMigrationCatchUpBatch, PgMigrationCatchUpLag, PgMigrationCatchUpOperation,
        PgMigrationCatchUpOptions, PgMigrationCatchUpPolicy, PgMigrationCheckpoint,
        PgMigrationClaim, PgMigrationClaimOptions, PgMigrationClaimResult,
        PgMigrationCreateOptions, PgMigrationCutoverOptions, PgMigrationJournalCursor,
        PgMigrationObjectCursor, PgMigrationPackCursor, PgMigrationPhase, PgMigrationRefCursor,
        PgMigrationRollbackOptions, PgMigrationRouteOutcome, PgMigrationSession, PgMigrationSource,
        PgMigrationSourceToken, PgMigrationState, PgMigrationTreeCursor,
        PgMigrationVerificationCheck, PgMigrationVerificationCheckKind,
        PgMigrationVerificationMode, PgMigrationVerificationOptions, PgMigrationVerificationReport,
        PgMigrationVerificationResult, PgPackConsistencyProbe, PgPackMaintenanceCandidate,
        PgPackMaintenanceClaim, PgPackMaintenanceClaimOptions, PgPackMaintenanceCreateOptions,
        PgPackMaintenanceJob, PgPackMaintenanceOutcome, PgPackMaintenancePhase,
        PgPackMaintenancePolicy, PgRepositoryRow, PgRepositorySummary, PgRepositorySummaryInstall,
        PgRepositorySummaryOptions, PgRepositorySummaryRead, PgServerStorage,
        PgServerStorageTransaction, PgSummaryBranch, PgSummaryCommit, RepositoryPk,
        MAX_CACHE_OUTBOX_CLAIM_BATCH, MAX_COMMIT_HISTORY_PAGE_SIZE,
        MAX_COMMIT_HISTORY_PARENT_EDGES, MAX_MIGRATION_CATCH_UP_BATCH, MAX_MIGRATION_CLAIM_BATCH,
        MAX_PACK_MAINTENANCE_BATCH,
    };
    pub use crate::storage::{
        BrowseIndex, CommitGraphStore, ConfigStore, ImportPublication, ImportPublicationResult,
        ImportSession, ImportStateStore, ImportedPack, IndexedCommit, ObjectReadResult,
        ObjectStore, PackMetadata, PackObjectIndex, PackStore, PackedObject, RefStore, ReflogEntry,
        ReflogStore, RepackPlan, StoredObject, StoredPack, StoredRef, MAX_OBJECT_READ_BATCH,
    };
    pub use crate::tree_block::{
        decode_tree_block, encode_tree_block, find_tree_block_entry, tree_block_prefix_range,
        TreeBlockEntry, TreeBlockError, TREE_BLOCK_FORMAT_VERSION,
    };
    #[cfg(feature = "sqlx-postgres")]
    pub use crate::tree_path::{
        resolve_tree_path, DecodedTreeBlockCache, InvalidTreeBlockCacheLimits,
        InvalidTreePathComponent, ResolvedTreePath, TreeBlockCacheInsert, TreeBlockCacheLimits,
        TreeBlockLoader, TreePathComponent, TreePathLimits, TreePathResolveError,
    };
    pub use crate::views::{
        BlobContentDelivery, BlobContentView, BlobDownloadOptions, BlobDownloadView,
        BlobMetadataView, BlobView, BranchView, CommitComparison, CommitHistoryOptions,
        CommitHistoryPage, CommitSummary, CompareInputs, DiscoveredFile, RepositorySummary,
        SignedContentUrl, TagView, TreeEntryView, TreeView,
    };
}
