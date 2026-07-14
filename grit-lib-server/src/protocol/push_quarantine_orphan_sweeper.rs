//! Fenced, retry-safe cleanup of receive-pack quarantine and external promotion orphans.

use std::fmt;

use async_trait::async_trait;
use grit_lib::objects::ObjectId;
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_quarantine::{QuarantineId, QuarantineState};

const LEASE_TOKEN_BYTES: usize = 32;
const MAX_STORAGE_KEY_BYTES: usize = 4_096;
const MAX_BACKEND_BYTES: usize = 256;

/// Opaque caller-generated identity for one sweeper worker.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct PushOrphanWorkerToken([u8; LEASE_TOKEN_BYTES]);

impl PushOrphanWorkerToken {
    /// Construct a worker identity from caller-supplied secure random bytes.
    ///
    /// # Errors
    ///
    /// Returns [`PushOrphanSweepError::InvalidLimits`] for the all-zero sentinel.
    pub fn from_random_bytes(bytes: [u8; LEASE_TOKEN_BYTES]) -> Result<Self, PushOrphanSweepError> {
        if bytes.iter().all(|byte| *byte == 0) {
            return Err(PushOrphanSweepError::InvalidLimits);
        }
        Ok(Self(bytes))
    }

    /// Return bytes for durable lease comparison.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; LEASE_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for PushOrphanWorkerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PushOrphanWorkerToken([redacted])")
    }
}

/// Fenced lease held by one worker for one orphan candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushOrphanLease {
    /// Worker that owns the lease.
    pub worker: PushOrphanWorkerToken,
    /// Monotonically increasing lease generation.
    pub generation: u64,
    /// Explicit lease expiration time.
    pub expires_at: OffsetDateTime,
}

/// Exact immutable external object metadata required before deletion.
#[derive(Clone, PartialEq, Eq)]
pub struct PushExternalOrphanObject {
    /// Complete immutable object bytes.
    pub size_bytes: u64,
    /// Provider version or ETag bytes.
    pub version: Vec<u8>,
    /// Verified content checksum.
    pub checksum: ObjectId,
}

impl fmt::Debug for PushExternalOrphanObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PushExternalOrphanObject")
            .field("size_bytes", &self.size_bytes)
            .field("version", &"[opaque]")
            .field("checksum", &self.checksum)
            .finish()
    }
}

/// One leased external promotion orphan.
#[derive(Clone)]
pub struct PushExternalOrphanCandidate {
    #[cfg_attr(not(feature = "sqlx-postgres"), allow(dead_code))]
    repository_pk: i64,
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// External provider/backend name.
    pub backend: String,
    /// Provider key. Reports and debug output never expose this value.
    pub storage_key: String,
    /// Promotion quarantine identity.
    pub quarantine_id: QuarantineId,
    /// Promotion quarantine fencing generation.
    pub quarantine_generation: u64,
    /// Prepared push fingerprint.
    pub prepared_fingerprint: ObjectId,
    /// Promoted PACK checksum.
    pub pack_checksum: ObjectId,
    /// Expected complete immutable object bytes.
    pub size_bytes: u64,
    /// Expected immutable object metadata, absent until finalization is reconciled.
    pub object: Option<PushExternalOrphanObject>,
    /// Earliest eligible cleanup time.
    pub not_before: OffsetDateTime,
    /// Fenced worker lease.
    pub lease: PushOrphanLease,
}

impl fmt::Debug for PushExternalOrphanCandidate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PushExternalOrphanCandidate")
            .field("tenant", &self.tenant)
            .field("repository", &self.repository)
            .field("backend", &self.backend)
            .field("storage_key", &"[redacted]")
            .field("quarantine_id", &self.quarantine_id)
            .field("quarantine_generation", &self.quarantine_generation)
            .field("not_before", &self.not_before)
            .field("lease", &self.lease)
            .finish_non_exhaustive()
    }
}

/// One leased backend-neutral quarantine cleanup candidate.
#[derive(Clone, Debug)]
pub struct PushQuarantineOrphanCandidate {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// Quarantine identity.
    pub quarantine_id: QuarantineId,
    /// Current quarantine fencing generation.
    pub quarantine_generation: u64,
    /// Authoritatively observed lifecycle state.
    pub state: QuarantineState,
    /// Backing bytes attributed to the candidate.
    pub bytes: u64,
    /// Caller-supplied creation time for age reporting.
    pub created_at: OffsetDateTime,
    /// Fenced worker lease.
    pub lease: PushOrphanLease,
}

/// Explicit batch, byte, time, and retry limits for one sweep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushOrphanSweepLimits {
    /// Maximum external and quarantine candidates combined.
    pub max_candidates: usize,
    /// Maximum aggregate candidate bytes; one larger candidate may run alone.
    pub max_batch_bytes: u64,
    /// Absolute ceiling for a candidate that runs alone.
    pub hard_candidate_bytes: u64,
    /// Maximum aggregate external storage-key bytes leased in one call.
    pub max_key_bytes: usize,
    /// Explicit sweep start.
    pub started_at: OffsetDateTime,
    /// Explicit sweep deadline.
    pub deadline: OffsetDateTime,
    /// Explicit lease expiration chosen by the caller.
    pub lease_expires_at: OffsetDateTime,
    /// Earliest retry time used after ambiguous or reconcilable outcomes.
    pub retry_not_before: OffsetDateTime,
}

impl PushOrphanSweepLimits {
    /// Validate bounded allocation and explicit-time relationships.
    ///
    /// # Errors
    ///
    /// Returns [`PushOrphanSweepError::InvalidLimits`] for zero or inconsistent limits.
    pub fn validate(self) -> Result<Self, PushOrphanSweepError> {
        let hard = crate::protocol::push_metrics::ReceivePackLimits::HARD_MAX;
        let maximum_duration = time::Duration::try_from(hard.max_duration)
            .map_err(|_| PushOrphanSweepError::InvalidLimits)?;
        if self.max_candidates == 0
            || self.max_candidates > 65_536
            || self.max_batch_bytes == 0
            || self.hard_candidate_bytes < self.max_batch_bytes
            || self.hard_candidate_bytes > hard.max_pack_bytes
            || self.max_key_bytes == 0
            || self.max_key_bytes > self.max_candidates.saturating_mul(MAX_STORAGE_KEY_BYTES)
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > maximum_duration
            || self.lease_expires_at <= self.started_at
            || self.lease_expires_at > self.deadline
            || self.retry_not_before < self.lease_expires_at
        {
            return Err(PushOrphanSweepError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Explicit caller-observed cancellation and time state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushOrphanSweepObservation {
    /// Caller-observed time.
    pub observed_at: OffsetDateTime,
    /// Explicit cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral cancellation and time source.
pub trait PushOrphanSweepControl {
    /// Return the latest explicit observation.
    fn observe(&mut self) -> PushOrphanSweepObservation;
}

/// Deterministic work allowance for selection, proof, deletion, and acknowledgement.
pub trait PushOrphanSweepWork {
    /// Charge work units, returning `false` when exhausted.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed implementation of [`PushOrphanSweepWork`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushOrphanSweepWorkLimit {
    remaining: u64,
}

impl PushOrphanSweepWorkLimit {
    /// Construct an exact work allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Return remaining work units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl PushOrphanSweepWork for PushOrphanSweepWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Non-sensitive backend failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("push orphan backend operation failed")]
pub struct PushOrphanBackendError;

/// Result of a fresh authoritative SQL/reference proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushExternalDeleteAuthorization {
    /// No live reference, active session, or competing lease exists and exact deletion is allowed.
    DeleteExact,
    /// Live or potentially shared content protects the external object; clear this orphan row.
    Protected,
    /// Promotion/finalization state is ambiguous and must be reconciled later.
    Retry,
}

/// Durable metadata outcome acknowledged with the same fencing lease.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushExternalOrphanOutcome {
    /// Exact version was deleted.
    Deleted,
    /// Exact object was already absent.
    Missing,
    /// A live/shared reference proved the candidate is not deletable.
    Protected,
    /// Ambiguous provider or promotion state must be retried after `retry_not_before`.
    Retry,
}

/// PostgreSQL-authoritative lease, proof, and acknowledgement boundary.
#[async_trait]
pub trait PushExternalOrphanMetadata: Send + Sync {
    /// Lease eligible candidates using row locking or compare-and-set fencing.
    async fn lease_external_candidates(
        &self,
        worker: PushOrphanWorkerToken,
        observed_at: OffsetDateTime,
        limits: &PushOrphanSweepLimits,
    ) -> Result<Vec<PushExternalOrphanCandidate>, PushOrphanBackendError>;

    /// In a fresh authoritative check, prove whether exact deletion is still safe.
    async fn authorize_external_delete(
        &self,
        candidate: &PushExternalOrphanCandidate,
        observed_at: OffsetDateTime,
    ) -> Result<PushExternalDeleteAuthorization, PushOrphanBackendError>;

    /// Idempotently delete or update the candidate row under its exact lease generation.
    async fn acknowledge_external_outcome(
        &self,
        candidate: &PushExternalOrphanCandidate,
        outcome: PushExternalOrphanOutcome,
        observed_at: OffsetDateTime,
        retry_not_before: OffsetDateTime,
    ) -> Result<(), PushOrphanBackendError>;
}

/// Exact-version provider deletion result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushExternalDeleteResult {
    /// The exact immutable version was removed.
    Deleted,
    /// The key was already absent.
    Missing,
    /// The provider result is ambiguous and requires metadata reinspection.
    Ambiguous,
    /// A different version owns the key and was not touched.
    VersionMismatch,
}

/// Provider-neutral inspection and exact-version deletion boundary.
#[async_trait]
pub trait PushExternalOrphanDelete: Send + Sync {
    /// Inspect immutable metadata without downloading object bytes.
    async fn inspect_exact(
        &self,
        backend: &str,
        key: &str,
    ) -> Result<Option<PushExternalOrphanObject>, PushOrphanBackendError>;

    /// Delete only when key and opaque version match exactly.
    async fn delete_exact_version(
        &self,
        backend: &str,
        key: &str,
        version: &[u8],
    ) -> Result<PushExternalDeleteResult, PushOrphanBackendError>;
}

/// Result of one backend-neutral quarantine cleanup attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushQuarantineCleanupResult {
    /// Backing bytes and private metadata were removed.
    Deleted,
    /// Backing state was already absent.
    Missing,
    /// State became live or otherwise protected.
    Protected,
    /// A promoting state requires reconciliation and was not discarded.
    ReconcilePromotion,
    /// A transient cleanup result must be retried.
    Retry,
}

/// Backend-neutral fenced quarantine selection and cleanup boundary.
#[async_trait]
pub trait PushQuarantineOrphanCleanup: Send + Sync {
    /// Lease eligible rejected, cancelled-as-rejected, expired, or ambiguous promoting candidates.
    async fn lease_quarantine_candidates(
        &self,
        worker: PushOrphanWorkerToken,
        observed_at: OffsetDateTime,
        remaining_candidates: usize,
        remaining_bytes: u64,
        lease_expires_at: OffsetDateTime,
    ) -> Result<Vec<PushQuarantineOrphanCandidate>, PushOrphanBackendError>;

    /// Cleanup the exact fenced candidate, or reconcile but never discard `Promoting` state.
    async fn cleanup_quarantine_candidate(
        &self,
        candidate: &PushQuarantineOrphanCandidate,
        observed_at: OffsetDateTime,
        retry_not_before: OffsetDateTime,
    ) -> Result<PushQuarantineCleanupResult, PushOrphanBackendError>;
}

/// Quarantine cleanup source used when a backend has no durable quarantine registry.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPushQuarantineOrphans;

#[async_trait]
impl PushQuarantineOrphanCleanup for NoPushQuarantineOrphans {
    async fn lease_quarantine_candidates(
        &self,
        _worker: PushOrphanWorkerToken,
        _observed_at: OffsetDateTime,
        _remaining_candidates: usize,
        _remaining_bytes: u64,
        _lease_expires_at: OffsetDateTime,
    ) -> Result<Vec<PushQuarantineOrphanCandidate>, PushOrphanBackendError> {
        Ok(Vec::new())
    }

    async fn cleanup_quarantine_candidate(
        &self,
        _candidate: &PushQuarantineOrphanCandidate,
        _observed_at: OffsetDateTime,
        _retry_not_before: OffsetDateTime,
    ) -> Result<PushQuarantineCleanupResult, PushOrphanBackendError> {
        Ok(PushQuarantineCleanupResult::Missing)
    }
}

/// Non-sensitive aggregate result of one bounded sweep.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushOrphanSweepReport {
    /// External promotion rows leased.
    pub external_leased: u64,
    /// Quarantine candidates leased.
    pub quarantine_leased: u64,
    /// Aggregate expected bytes examined.
    pub examined_bytes: u64,
    /// Exact external versions deleted.
    pub external_deleted: u64,
    /// External objects already missing.
    pub external_missing: u64,
    /// External candidates protected by live/shared state.
    pub external_protected: u64,
    /// External candidates deferred for reconciliation or retry.
    pub external_retried: u64,
    /// Terminal quarantine backing stores deleted or already missing.
    pub quarantine_cleaned: u64,
    /// Quarantines protected from cleanup.
    pub quarantine_protected: u64,
    /// Promoting quarantines sent to reconciliation instead of discard.
    pub promotion_reconciliations: u64,
    /// Quarantine candidates deferred for retry.
    pub quarantine_retried: u64,
    /// Oldest eligible candidate age in whole non-negative seconds.
    pub oldest_candidate_age_seconds: u64,
}

/// Typed bounded-sweeper failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PushOrphanSweepError {
    /// Limits, explicit times, worker token, or returned candidate shape is invalid.
    #[error("invalid push orphan sweep limits or candidate")]
    InvalidLimits,
    /// Explicit cancellation was observed.
    #[error("push orphan sweep cancelled")]
    Cancelled,
    /// Deadline elapsed or observed time moved backwards.
    #[error("push orphan sweep deadline elapsed")]
    Deadline,
    /// Deterministic work allowance was exhausted.
    #[error("push orphan sweep work budget exhausted")]
    WorkBudget,
    /// Metadata, quarantine, or provider operation failed; leases make retry safe.
    #[error("push orphan sweep backend operation failed")]
    Backend,
    /// Candidate object metadata did not match the provider object.
    #[error("push orphan sweep found an immutable object metadata mismatch")]
    ObjectMismatch,
}

/// Sweep external promotion rows and backend-neutral quarantine backing stores.
///
/// External deletion always follows a fresh authoritative proof and exact provider inspection.
/// Ambiguous deletion is reinspected before acknowledgement. Crashes leave either an expiring
/// lease or an idempotently missing object. Promoting quarantines are reconciled, never discarded.
///
/// # Errors
///
/// Returns typed limit, cancellation, deadline, work, backend, or immutable-metadata failures.
#[allow(clippy::too_many_arguments)]
pub async fn sweep_push_orphans<M, P, Q, C, W>(
    metadata: &M,
    provider: &P,
    quarantines: &Q,
    worker: PushOrphanWorkerToken,
    control: &mut C,
    work: &mut W,
    limits: PushOrphanSweepLimits,
) -> Result<PushOrphanSweepReport, PushOrphanSweepError>
where
    M: PushExternalOrphanMetadata,
    P: PushExternalOrphanDelete,
    Q: PushQuarantineOrphanCleanup,
    C: PushOrphanSweepControl,
    W: PushOrphanSweepWork,
{
    let limits = limits.validate()?;
    let mut last_observed_at = limits.started_at;
    let initial = checkpoint(control, work, &limits, &mut last_observed_at, 1)?;
    if limits.lease_expires_at <= initial.observed_at
        || limits.retry_not_before <= initial.observed_at
    {
        return Err(PushOrphanSweepError::InvalidLimits);
    }
    let external = metadata
        .lease_external_candidates(worker, initial.observed_at, &limits)
        .await
        .map_err(|_| PushOrphanSweepError::Backend)?;
    let leased = checkpoint(control, work, &limits, &mut last_observed_at, 1)?;
    validate_external_batch(&external, worker, &limits, leased.observed_at)?;
    let mut report = PushOrphanSweepReport {
        external_leased: u64::try_from(external.len())
            .map_err(|_| PushOrphanSweepError::InvalidLimits)?,
        ..PushOrphanSweepReport::default()
    };

    for candidate in &external {
        let observation = checkpoint(control, work, &limits, &mut last_observed_at, 3)?;
        report.examined_bytes = report
            .examined_bytes
            .checked_add(candidate_size(candidate))
            .ok_or(PushOrphanSweepError::InvalidLimits)?;
        update_age(&mut report, observation.observed_at, candidate.not_before)?;
        let authorization = metadata
            .authorize_external_delete(candidate, observation.observed_at)
            .await
            .map_err(|_| PushOrphanSweepError::Backend)?;
        let proof_observation = checkpoint(control, work, &limits, &mut last_observed_at, 1)?;
        let outcome = match authorization {
            PushExternalDeleteAuthorization::Protected => PushExternalOrphanOutcome::Protected,
            PushExternalDeleteAuthorization::Retry => PushExternalOrphanOutcome::Retry,
            PushExternalDeleteAuthorization::DeleteExact => {
                delete_external_candidate(
                    provider,
                    candidate,
                    control,
                    work,
                    &limits,
                    &mut last_observed_at,
                )
                .await?
            }
        };
        metadata
            .acknowledge_external_outcome(
                candidate,
                outcome,
                proof_observation.observed_at,
                limits.retry_not_before,
            )
            .await
            .map_err(|_| PushOrphanSweepError::Backend)?;
        match outcome {
            PushExternalOrphanOutcome::Deleted => report.external_deleted += 1,
            PushExternalOrphanOutcome::Missing => report.external_missing += 1,
            PushExternalOrphanOutcome::Protected => report.external_protected += 1,
            PushExternalOrphanOutcome::Retry => report.external_retried += 1,
        }
    }

    let remaining_candidates = if report.examined_bytes >= limits.max_batch_bytes {
        0
    } else {
        limits.max_candidates.saturating_sub(external.len())
    };
    let remaining_bytes = limits.max_batch_bytes.saturating_sub(report.examined_bytes);
    let observation = checkpoint(control, work, &limits, &mut last_observed_at, 1)?;
    let quarantine_candidates = quarantines
        .lease_quarantine_candidates(
            worker,
            observation.observed_at,
            remaining_candidates,
            remaining_bytes,
            limits.lease_expires_at,
        )
        .await
        .map_err(|_| PushOrphanSweepError::Backend)?;
    let leased_quarantines = checkpoint(control, work, &limits, &mut last_observed_at, 1)?;
    validate_quarantine_batch(
        &quarantine_candidates,
        remaining_candidates,
        remaining_bytes,
        limits.hard_candidate_bytes,
        worker,
        limits.lease_expires_at,
        leased_quarantines.observed_at,
    )?;
    report.quarantine_leased = u64::try_from(quarantine_candidates.len())
        .map_err(|_| PushOrphanSweepError::InvalidLimits)?;
    for candidate in &quarantine_candidates {
        let observation = checkpoint(control, work, &limits, &mut last_observed_at, 2)?;
        report.examined_bytes = report
            .examined_bytes
            .checked_add(candidate.bytes)
            .ok_or(PushOrphanSweepError::InvalidLimits)?;
        update_age(&mut report, observation.observed_at, candidate.created_at)?;
        let result = quarantines
            .cleanup_quarantine_candidate(
                candidate,
                observation.observed_at,
                limits.retry_not_before,
            )
            .await
            .map_err(|_| PushOrphanSweepError::Backend)?;
        if candidate.state == QuarantineState::Promoting
            && !matches!(
                result,
                PushQuarantineCleanupResult::ReconcilePromotion
                    | PushQuarantineCleanupResult::Retry
            )
        {
            return Err(PushOrphanSweepError::InvalidLimits);
        }
        match result {
            PushQuarantineCleanupResult::Deleted | PushQuarantineCleanupResult::Missing => {
                report.quarantine_cleaned += 1;
            }
            PushQuarantineCleanupResult::Protected => report.quarantine_protected += 1,
            PushQuarantineCleanupResult::ReconcilePromotion => {
                if candidate.state != QuarantineState::Promoting {
                    return Err(PushOrphanSweepError::InvalidLimits);
                }
                report.promotion_reconciliations += 1;
            }
            PushQuarantineCleanupResult::Retry => report.quarantine_retried += 1,
        }
    }
    Ok(report)
}

async fn delete_external_candidate<P, C, W>(
    provider: &P,
    candidate: &PushExternalOrphanCandidate,
    control: &mut C,
    work: &mut W,
    limits: &PushOrphanSweepLimits,
    last_observed_at: &mut OffsetDateTime,
) -> Result<PushExternalOrphanOutcome, PushOrphanSweepError>
where
    P: PushExternalOrphanDelete,
    C: PushOrphanSweepControl,
    W: PushOrphanSweepWork,
{
    let Some(expected) = candidate.object.as_ref() else {
        return Ok(PushExternalOrphanOutcome::Retry);
    };
    let inspected = provider
        .inspect_exact(&candidate.backend, &candidate.storage_key)
        .await
        .map_err(|_| PushOrphanSweepError::Backend)?;
    checkpoint(control, work, limits, last_observed_at, 1)?;
    let Some(inspected) = inspected else {
        return Ok(PushExternalOrphanOutcome::Missing);
    };
    if inspected != *expected {
        return Err(PushOrphanSweepError::ObjectMismatch);
    }
    match provider
        .delete_exact_version(
            &candidate.backend,
            &candidate.storage_key,
            &expected.version,
        )
        .await
        .map_err(|_| PushOrphanSweepError::Backend)?
    {
        PushExternalDeleteResult::Deleted => Ok(PushExternalOrphanOutcome::Deleted),
        PushExternalDeleteResult::Missing => Ok(PushExternalOrphanOutcome::Missing),
        PushExternalDeleteResult::VersionMismatch => Ok(PushExternalOrphanOutcome::Retry),
        PushExternalDeleteResult::Ambiguous => {
            let after = provider
                .inspect_exact(&candidate.backend, &candidate.storage_key)
                .await
                .map_err(|_| PushOrphanSweepError::Backend)?;
            match after {
                None => Ok(PushExternalOrphanOutcome::Deleted),
                Some(object) if object == *expected => Ok(PushExternalOrphanOutcome::Retry),
                Some(_) => Err(PushOrphanSweepError::ObjectMismatch),
            }
        }
    }
}

fn validate_external_batch(
    candidates: &[PushExternalOrphanCandidate],
    worker: PushOrphanWorkerToken,
    limits: &PushOrphanSweepLimits,
    observed_at: OffsetDateTime,
) -> Result<(), PushOrphanSweepError> {
    if candidates.len() > limits.max_candidates {
        return Err(PushOrphanSweepError::InvalidLimits);
    }
    let mut bytes = 0_u64;
    let mut key_bytes = 0_usize;
    for candidate in candidates {
        let size = candidate_size(candidate);
        bytes = bytes
            .checked_add(size)
            .ok_or(PushOrphanSweepError::InvalidLimits)?;
        key_bytes = key_bytes
            .checked_add(candidate.storage_key.len())
            .ok_or(PushOrphanSweepError::InvalidLimits)?;
        if candidate.backend.is_empty()
            || candidate.backend.len() > MAX_BACKEND_BYTES
            || candidate.storage_key.is_empty()
            || candidate.storage_key.len() > MAX_STORAGE_KEY_BYTES
            || candidate.lease.worker != worker
            || candidate.lease.generation == 0
            || candidate.lease.expires_at != limits.lease_expires_at
            || candidate.not_before > observed_at
            || candidate.lease.expires_at <= observed_at
            || size > limits.hard_candidate_bytes
            || candidate.prepared_fingerprint.algo() != candidate.pack_checksum.algo()
            || candidate.object.as_ref().is_some_and(|object| {
                object.size_bytes != candidate.size_bytes
                    || object.checksum != candidate.pack_checksum
                    || object.version.is_empty()
                    || object.version.len() > MAX_STORAGE_KEY_BYTES
            })
        {
            return Err(PushOrphanSweepError::InvalidLimits);
        }
    }
    if key_bytes > limits.max_key_bytes || (bytes > limits.max_batch_bytes && candidates.len() != 1)
    {
        return Err(PushOrphanSweepError::InvalidLimits);
    }
    Ok(())
}

fn validate_quarantine_batch(
    candidates: &[PushQuarantineOrphanCandidate],
    max_candidates: usize,
    max_bytes: u64,
    hard_candidate_bytes: u64,
    worker: PushOrphanWorkerToken,
    lease_expires_at: OffsetDateTime,
    observed_at: OffsetDateTime,
) -> Result<(), PushOrphanSweepError> {
    if candidates.len() > max_candidates {
        return Err(PushOrphanSweepError::InvalidLimits);
    }
    let bytes = candidates.iter().try_fold(0_u64, |sum, candidate| {
        if candidate.quarantine_generation == 0
            || candidate.lease.generation == 0
            || candidate.lease.worker != worker
            || candidate.lease.expires_at != lease_expires_at
            || candidate.lease.expires_at <= observed_at
            || candidate.bytes > hard_candidate_bytes
            || !matches!(
                candidate.state,
                QuarantineState::Rejected | QuarantineState::Expired | QuarantineState::Promoting
            )
        {
            return Err(PushOrphanSweepError::InvalidLimits);
        }
        sum.checked_add(candidate.bytes)
            .ok_or(PushOrphanSweepError::InvalidLimits)
    })?;
    if bytes > max_bytes && candidates.len() != 1 {
        return Err(PushOrphanSweepError::InvalidLimits);
    }
    Ok(())
}

fn candidate_size(candidate: &PushExternalOrphanCandidate) -> u64 {
    candidate.size_bytes
}

fn checkpoint<C: PushOrphanSweepControl, W: PushOrphanSweepWork>(
    control: &mut C,
    work: &mut W,
    limits: &PushOrphanSweepLimits,
    last_observed_at: &mut OffsetDateTime,
    units: u64,
) -> Result<PushOrphanSweepObservation, PushOrphanSweepError> {
    let observation = control.observe();
    if observation.observed_at < *last_observed_at {
        return Err(PushOrphanSweepError::Deadline);
    }
    *last_observed_at = observation.observed_at;
    if observation.cancelled {
        return Err(PushOrphanSweepError::Cancelled);
    }
    if observation.observed_at < limits.started_at || observation.observed_at >= limits.deadline {
        return Err(PushOrphanSweepError::Deadline);
    }
    if !work.charge(units) {
        return Err(PushOrphanSweepError::WorkBudget);
    }
    Ok(observation)
}

fn update_age(
    report: &mut PushOrphanSweepReport,
    observed_at: OffsetDateTime,
    since: OffsetDateTime,
) -> Result<(), PushOrphanSweepError> {
    if since > observed_at {
        return Err(PushOrphanSweepError::InvalidLimits);
    }
    let seconds = (observed_at - since).whole_seconds();
    let seconds = u64::try_from(seconds).map_err(|_| PushOrphanSweepError::InvalidLimits)?;
    report.oldest_candidate_age_seconds = report.oldest_candidate_age_seconds.max(seconds);
    Ok(())
}

#[cfg(feature = "sqlx-postgres")]
mod postgres;
