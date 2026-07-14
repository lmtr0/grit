//! Runtime-neutral replica routing and disaster-recovery policy.
//!
//! Regional failure handling is conservative: mutable and read-after-write operations always use
//! the primary, and immutable reads fall back to the primary unless a fresh, healthy replica has
//! explicitly observed every required repository generation. Recovery cutover is only ready when
//! coordinated metadata and immutable packs meet their separate objectives and repository checks
//! have succeeded without consulting cache state.

use std::collections::{HashMap, HashSet};

use grit_lib::objects::ObjectId;
use time::{Duration, OffsetDateTime};

/// Repository incarnation and independently advancing durable generations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RepositoryGeneration {
    /// Monotonic identity changed when a repository is deleted and recreated.
    pub incarnation: u64,
    /// Overall durable repository generation.
    pub repository: u64,
    /// Visible ref generation.
    pub refs: u64,
    /// Indexed history generation.
    pub history: u64,
    /// Repository configuration generation.
    pub config: u64,
}

impl RepositoryGeneration {
    /// Return whether this observation is at least as current as `required`.
    #[must_use]
    pub const fn satisfies(self, required: Self) -> bool {
        self.incarnation == required.incarnation
            && self.repository >= required.repository
            && self.refs >= required.refs
            && self.history >= required.history
            && self.config >= required.config
    }

    const fn is_valid(self) -> bool {
        self.incarnation > 0
    }
}

/// Operation whose backend route must be selected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadOperation {
    /// Immutable object or pack metadata read.
    ImmutableMetadata,
    /// Immutable commit-history or browse-index read.
    ImmutableHistory,
    /// Ref publication or compare-and-swap mutation.
    RefPublication,
    /// Import completion and visibility publication.
    ImportCompletion,
    /// Push validation or publication.
    Push,
    /// A mutable view that claims to represent current state.
    CurrentMutableView,
    /// A read that must observe a preceding write.
    ReadAfterWrite,
}

impl ReadOperation {
    const fn replica_safe(self) -> bool {
        matches!(self, Self::ImmutableMetadata | Self::ImmutableHistory)
    }
}

/// Health state reported for one database replica.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplicaHealth {
    /// Replica is accepting reads and replay is progressing.
    Healthy,
    /// Replica is reachable but should not receive correctness-sensitive reads.
    Degraded,
    /// Replica is unavailable.
    Unavailable,
}

/// Caller-observed state of a database replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaObservation {
    /// Stable replica identity used as the deterministic final tie-breaker.
    pub replica_id: String,
    /// Deployment region.
    pub region: String,
    /// Current health classification.
    pub health: ReplicaHealth,
    /// Repository generations replayed by this replica.
    pub generation: RepositoryGeneration,
    /// Caller-measured replay lag.
    pub replay_lag: Duration,
    /// Explicit timestamp at which this observation was sampled.
    pub sampled_at: OffsetDateTime,
}

/// Constraints for immutable replica reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaPolicy {
    /// Preferred region, selected ahead of allowed remote regions.
    pub preferred_region: String,
    /// Additional regions eligible for fallback.
    pub allowed_regions: Vec<String>,
    /// Maximum acceptable replay lag.
    pub max_replay_lag: Duration,
    /// Maximum age of a health observation.
    pub max_observation_age: Duration,
}

impl ReplicaPolicy {
    /// Validate bounds and region identifiers.
    ///
    /// # Errors
    ///
    /// Returns [`ResilienceError::Invalid`] for empty, duplicate, or unbounded region policy.
    pub fn validate(&self) -> Result<(), ResilienceError> {
        if !valid_identifier(&self.preferred_region, 128)
            || self.allowed_regions.len() > 32
            || self.max_replay_lag.is_negative()
            || self.max_observation_age.is_negative()
        {
            return Err(ResilienceError::Invalid("replica policy"));
        }
        let mut regions = HashSet::new();
        regions
            .try_reserve(self.allowed_regions.len())
            .map_err(|_| ResilienceError::Allocation)?;
        if self.allowed_regions.iter().any(|region| {
            !valid_identifier(region, 128)
                || region == &self.preferred_region
                || !regions.insert(region.as_str())
        }) {
            return Err(ResilienceError::Invalid("replica regions"));
        }
        Ok(())
    }

    fn region_rank(&self, region: &str) -> Option<usize> {
        if region == self.preferred_region {
            return Some(0);
        }
        self.allowed_regions
            .iter()
            .position(|allowed| allowed == region)
            .and_then(|index| index.checked_add(1))
    }
}

/// Reason a request was conservatively sent to the primary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PrimaryRouteReason {
    /// The operation mutates or promises current/read-after-write state.
    PrimaryRequired,
    /// No replica satisfied health, generation, lag, freshness, and region constraints.
    NoEligibleReplica,
}

/// Deterministic backend route.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadRoute {
    /// Use the primary database.
    Primary(PrimaryRouteReason),
    /// Use one generation-qualified replica.
    Replica {
        /// Selected stable replica identity.
        replica_id: String,
        /// Selected region.
        region: String,
    },
}

/// Select a backend without allowing an older replica to masquerade as current.
///
/// Selection orders eligible replicas by preferred-region rank, replay lag, newest observation,
/// and stable replica identity. All timestamps are supplied by the caller.
///
/// # Errors
///
/// Returns [`ResilienceError::Invalid`] for malformed policy or observations.
pub fn route_read(
    operation: ReadOperation,
    required: RepositoryGeneration,
    policy: &ReplicaPolicy,
    replicas: &[ReplicaObservation],
    observed_at: OffsetDateTime,
) -> Result<ReadRoute, ResilienceError> {
    policy.validate()?;
    if !required.is_valid() {
        return Err(ResilienceError::Invalid("required repository generation"));
    }
    if !operation.replica_safe() {
        return Ok(ReadRoute::Primary(PrimaryRouteReason::PrimaryRequired));
    }
    if replicas.len() > 1_024 {
        return Err(ResilienceError::Invalid("replica observation bound"));
    }
    let mut eligible = Vec::new();
    eligible
        .try_reserve(replicas.len())
        .map_err(|_| ResilienceError::Allocation)?;
    let mut replica_ids = HashSet::new();
    replica_ids
        .try_reserve(replicas.len())
        .map_err(|_| ResilienceError::Allocation)?;
    for replica in replicas {
        if !valid_identifier(&replica.replica_id, 256)
            || !replica_ids.insert(replica.replica_id.as_str())
            || !valid_identifier(&replica.region, 128)
            || replica.replay_lag.is_negative()
            || replica.sampled_at > observed_at
            || replica.sampled_at.checked_sub(replica.replay_lag).is_none()
            || !replica.generation.is_valid()
        {
            return Err(ResilienceError::Invalid("replica observation"));
        }
        let Some(region_rank) = policy.region_rank(&replica.region) else {
            continue;
        };
        if replica.health == ReplicaHealth::Healthy
            && replica.generation.satisfies(required)
            && replica.replay_lag <= policy.max_replay_lag
            && observed_at - replica.sampled_at <= policy.max_observation_age
        {
            eligible.push((region_rank, replica));
        }
    }
    eligible.sort_by(|(left_rank, left), (right_rank, right)| {
        left_rank
            .cmp(right_rank)
            .then_with(|| left.replay_lag.cmp(&right.replay_lag))
            .then_with(|| right.sampled_at.cmp(&left.sampled_at))
            .then_with(|| left.replica_id.cmp(&right.replica_id))
    });
    Ok(eligible.first().map_or(
        ReadRoute::Primary(PrimaryRouteReason::NoEligibleReplica),
        |(_, replica)| ReadRoute::Replica {
            replica_id: replica.replica_id.clone(),
            region: replica.region.clone(),
        },
    ))
}

/// Replication state for immutable regional pack bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackReplicaState {
    /// Copy is in progress and is not readable for recovery.
    Replicating,
    /// Copy exists but checksum verification is pending.
    Unverified,
    /// Copy is durable and checksum-verified.
    Verified,
    /// Copy was found corrupt or missing.
    Failed,
}

/// Region-aware location of one immutable content-addressed pack.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegionalPackReplica {
    /// Pack checksum; live-key content addressing remains unchanged.
    pub checksum: ObjectId,
    /// Opaque existing external storage key, not rewritten to encode region.
    pub storage_key: String,
    /// Storage region carried separately from the immutable key.
    pub region: String,
    /// Exact byte size.
    pub size_bytes: u64,
    /// Replication and verification state.
    pub state: PackReplicaState,
    /// Explicit last observation time.
    pub observed_at: OffsetDateTime,
}

impl RegionalPackReplica {
    /// Validate immutable pack location metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ResilienceError::Invalid`] for empty keys/regions or zero-sized packs.
    pub fn validate(&self) -> Result<(), ResilienceError> {
        if self.storage_key.trim().is_empty()
            || self.storage_key.len() > 2_048
            || self.region.trim().is_empty()
            || self.region.len() > 128
            || self.checksum.is_zero()
            || self.size_bytes == 0
        {
            return Err(ResilienceError::Invalid("regional pack replica"));
        }
        Ok(())
    }
}

/// Independently governed recovery component.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecoveryComponent {
    /// PostgreSQL metadata and mutable repository state.
    Metadata,
    /// Canonical immutable object and pack bytes.
    ImmutablePacks,
    /// Rebuildable application caches.
    Cache,
    /// Rebuildable clone-cache packs.
    CloneCache,
    /// Unpublished quarantine data.
    Quarantine,
}

impl RecoveryComponent {
    const ALL: [Self; 5] = [
        Self::Metadata,
        Self::ImmutablePacks,
        Self::Cache,
        Self::CloneCache,
        Self::Quarantine,
    ];
}

/// RPO and RTO for one component.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecoveryObjective {
    /// Governed component.
    pub component: RecoveryComponent,
    /// Maximum acceptable data-loss interval.
    pub rpo: Duration,
    /// Maximum acceptable recovery interval.
    pub rto: Duration,
}

/// Cache-independent restored repository verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreChecks {
    /// Exact PostgreSQL checkpoint whose restored data was inspected.
    pub metadata_checkpoint: String,
    /// Exact repository incarnation and generation vector that was inspected.
    pub generation: RepositoryGeneration,
    /// Explicit completion time of the cache-independent verification.
    pub verified_at: OffsetDateTime,
    /// All refs resolve to canonical objects.
    pub refs_valid: bool,
    /// Object graph closure is complete.
    pub object_closure_valid: bool,
    /// Browse data is present or rebuilt from canonical objects.
    pub browse_ready: bool,
    /// History data is present or rebuilt from canonical objects.
    pub history_ready: bool,
    /// Whether verification consulted cache state; readiness requires `false`.
    pub used_cache: bool,
}

/// Completeness marker for the immutable pack portion of a coordinated backup.
///
/// An explicit complete state permits a repository with zero packs to be distinguished from a
/// backup whose pack inventory has not been captured yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackManifestState {
    /// Pack discovery has not started or no durable inventory is available.
    Missing,
    /// Pack discovery is in progress and the listed replicas are not authoritative.
    Incomplete {
        /// Number of distinct immutable packs expected when discovery finishes.
        expected_pack_count: u64,
    },
    /// Pack discovery finished and the listed replicas are authoritative.
    Complete {
        /// Number of distinct immutable packs in the completed inventory.
        expected_pack_count: u64,
    },
}

/// One complete regional recovery plan and coordinated backup manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecoveryPlan {
    /// Stable PostgreSQL checkpoint identifier or LSN.
    pub metadata_checkpoint: String,
    /// Generations captured by the checkpoint.
    pub generation: RepositoryGeneration,
    /// Checksum of the immutable pack manifest bound to the checkpoint.
    pub pack_manifest_checksum: ObjectId,
    /// Whether the immutable pack manifest is missing, partial, or explicitly complete.
    pub pack_manifest_state: PackManifestState,
    /// Immutable regional pack locations; every location must be verified and unique.
    ///
    /// The same content checksum may appear in multiple regions.
    pub packs: Vec<RegionalPackReplica>,
    /// Separate objectives for every recovery component.
    pub objectives: Vec<RecoveryObjective>,
    /// Repository verification performed against canonical restored data.
    pub restore_checks: RestoreChecks,
    /// Explicit checkpoint/manifest completion time.
    pub completed_at: Option<OffsetDateTime>,
}

impl RecoveryPlan {
    /// Validate coordinated backup completeness and return restore readiness.
    ///
    /// Partial backups, unverified or duplicate pack locations, missing component objectives, or
    /// cache-dependent repository checks are never restore-ready. A complete empty pack manifest
    /// is accepted only when its explicit expected pack count is zero.
    ///
    /// # Errors
    ///
    /// Returns a typed validation or incompleteness error.
    pub fn restore_readiness(&self) -> Result<RecoveryReadiness, ResilienceError> {
        if self.metadata_checkpoint.trim().is_empty()
            || self.metadata_checkpoint.len() > 512
            || self.pack_manifest_checksum.is_zero()
            || self.packs.len() > 65_536
            || self.objectives.len() != RecoveryComponent::ALL.len()
        {
            return Err(ResilienceError::Invalid("recovery plan"));
        }
        if !self.generation.is_valid() {
            return Err(ResilienceError::Invalid("recovery generation"));
        }
        let expected_pack_count = match self.pack_manifest_state {
            PackManifestState::Missing | PackManifestState::Incomplete { .. } => {
                return Ok(RecoveryReadiness::Incomplete(RecoveryBlocker::PackManifest));
            }
            PackManifestState::Complete {
                expected_pack_count,
            } => expected_pack_count,
        };
        let Some(completed_at) = self.completed_at else {
            return Ok(RecoveryReadiness::Incomplete(
                RecoveryBlocker::BackupNotFinalized,
            ));
        };
        let mut locations = HashSet::new();
        locations
            .try_reserve(self.packs.len())
            .map_err(|_| ResilienceError::Allocation)?;
        let mut distinct_packs = HashSet::new();
        distinct_packs
            .try_reserve(self.packs.len())
            .map_err(|_| ResilienceError::Allocation)?;
        let mut pack_sizes = HashMap::new();
        pack_sizes
            .try_reserve(self.packs.len())
            .map_err(|_| ResilienceError::Allocation)?;
        for pack in &self.packs {
            pack.validate()?;
            if pack.state != PackReplicaState::Verified
                || pack.observed_at > completed_at
                || !locations.insert((pack.region.as_str(), pack.storage_key.as_str()))
            {
                return Ok(RecoveryReadiness::Incomplete(RecoveryBlocker::PackManifest));
            }
            distinct_packs.insert(pack.checksum);
            if pack_sizes
                .insert(pack.checksum, pack.size_bytes)
                .is_some_and(|size| size != pack.size_bytes)
            {
                return Err(ResilienceError::Invalid("pack replica size mismatch"));
            }
        }
        let distinct_pack_count = u64::try_from(distinct_packs.len())
            .map_err(|_| ResilienceError::Invalid("pack manifest count"))?;
        if distinct_pack_count != expected_pack_count {
            return Ok(RecoveryReadiness::Incomplete(RecoveryBlocker::PackManifest));
        }
        let mut components = HashSet::new();
        components
            .try_reserve(self.objectives.len())
            .map_err(|_| ResilienceError::Allocation)?;
        for objective in &self.objectives {
            if objective.rpo.is_negative()
                || objective.rto.is_negative()
                || !components.insert(objective.component)
            {
                return Err(ResilienceError::Invalid("recovery objectives"));
            }
        }
        if RecoveryComponent::ALL
            .iter()
            .any(|component| !components.contains(component))
        {
            return Err(ResilienceError::Invalid("missing recovery objective"));
        }
        let checks = &self.restore_checks;
        if checks.metadata_checkpoint != self.metadata_checkpoint
            || checks.generation != self.generation
            || checks.verified_at < completed_at
            || checks.used_cache
            || !checks.refs_valid
            || !checks.object_closure_valid
            || !checks.browse_ready
            || !checks.history_ready
        {
            return Ok(RecoveryReadiness::Incomplete(
                RecoveryBlocker::RepositoryVerification,
            ));
        }
        Ok(RecoveryReadiness::RestoreReady)
    }

    /// Evaluate RPO/RTO for a regional failure at caller-supplied times.
    ///
    /// A cutover is ready only if the manifest is restore-ready, each component's recoverable point
    /// is within its RPO, and its estimated readiness is within RTO and no later than `observed_at`.
    /// Otherwise the caller receives bounded retry guidance or a terminal objective violation.
    ///
    /// # Errors
    ///
    /// Returns validation errors for malformed or incomplete component observations.
    pub fn evaluate_failover(
        &self,
        failure_at: OffsetDateTime,
        observed_at: OffsetDateTime,
        components: &[ComponentRecoveryObservation],
    ) -> Result<FailoverDecision, ResilienceError> {
        if let RecoveryReadiness::Incomplete(blocker) = self.restore_readiness()? {
            return Ok(FailoverDecision::Unavailable(blocker));
        }
        if components.len() != RecoveryComponent::ALL.len() || observed_at < failure_at {
            return Err(ResilienceError::Invalid("failover observations"));
        }
        let completed_at = self
            .completed_at
            .ok_or(ResilienceError::Invalid("backup completion time"))?;
        if completed_at > failure_at || self.restore_checks.verified_at > observed_at {
            return Err(ResilienceError::Invalid("future recovery evidence"));
        }
        let mut seen = HashSet::with_capacity(components.len());
        let mut retry_at = observed_at;
        for observation in components {
            if !seen.insert(observation.component) || observation.recoverable_through > failure_at {
                return Err(ResilienceError::Invalid("recovery observation"));
            }
            let objective = self
                .objectives
                .iter()
                .find(|objective| objective.component == observation.component)
                .ok_or(ResilienceError::Invalid("recovery objective"))?;
            if failure_at - observation.recoverable_through > objective.rpo
                || observation.estimated_ready_at > failure_at.saturating_add(objective.rto)
            {
                return Ok(FailoverDecision::Unavailable(
                    RecoveryBlocker::ObjectiveExceeded,
                ));
            }
            retry_at = retry_at.max(observation.estimated_ready_at);
        }
        if retry_at > observed_at {
            return Ok(FailoverDecision::Retry {
                earliest: retry_at,
                latest: retry_at.saturating_add(Duration::seconds(30)),
            });
        }
        Ok(FailoverDecision::CutoverReady {
            generation: self.generation,
            metadata_checkpoint: self.metadata_checkpoint.clone(),
        })
    }
}

/// Component-specific recovery observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ComponentRecoveryObservation {
    /// Observed component.
    pub component: RecoveryComponent,
    /// Latest point known recoverable.
    pub recoverable_through: OffsetDateTime,
    /// Estimated time at which the component can serve its role.
    pub estimated_ready_at: OffsetDateTime,
}

/// Why a backup or failover cannot cut over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryBlocker {
    /// Metadata and pack backup was not durably finalized.
    BackupNotFinalized,
    /// Pack manifest is duplicated, incomplete, or unverified.
    PackManifest,
    /// Refs, closure, browse, or history checks failed or depended on cache.
    RepositoryVerification,
    /// A component exceeded its distinct RPO or RTO.
    ObjectiveExceeded,
}

/// Restore readiness of a coordinated backup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecoveryReadiness {
    /// Metadata, packs, and cache-independent checks are complete.
    RestoreReady,
    /// Recovery is blocked for the specified reason.
    Incomplete(RecoveryBlocker),
}

/// Regional failover outcome with explicit retry/cutover semantics.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailoverDecision {
    /// Cutover may publish the exact checkpoint and generation.
    CutoverReady {
        /// Generation the new primary may advertise.
        generation: RepositoryGeneration,
        /// Bound metadata checkpoint.
        metadata_checkpoint: String,
    },
    /// Recovery remains within RTO; retry only inside this bounded window.
    Retry {
        /// Earliest useful retry time.
        earliest: OffsetDateTime,
        /// Latest suggested retry time.
        latest: OffsetDateTime,
    },
    /// Cutover cannot satisfy backup verification or recovery objectives.
    Unavailable(RecoveryBlocker),
}

/// Replica/DR policy error.
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResilienceError {
    /// A bounded public value failed validation.
    #[error("invalid {0}")]
    Invalid(&'static str),
    /// A bounded temporary allocation failed.
    #[error("resilience policy allocation failed")]
    Allocation,
}

fn valid_identifier(value: &str, max_len: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max_len
}
