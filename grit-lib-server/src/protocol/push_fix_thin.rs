//! Bounded conversion of validated thin receive-pack quarantines into self-contained PACKs.
//!
//! The source quarantine remains sealed and invisible. A deterministic plan authorizes each
//! external REF_DELTA base, and the rewriter copies validated source entries into a distinct empty
//! quarantine before streaming every required canonical base as a direct entry. It writes a new
//! header and trailer; bytes are never appended after the source trailer.

use async_trait::async_trait;
use flate2::{Compress, Compression, FlushCompress, Status};
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use std::collections::HashSet;
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_metrics::ReceivePackLimits;
use crate::protocol::push_pack_validation::{
    validate_quarantined_pack, NoAuthorizedPushBases, PushPackByteSpan, PushPackEntryKind,
    PushPackIndexRow, PushPackValidationControl, PushPackValidationError,
    PushPackValidationObservation, PushPackValidationOptions, PushPackValidationWork,
    ValidatedPushPack,
};
use crate::protocol::push_quarantine::{
    PackQuarantine, QuarantineFence, QuarantineManifest, QuarantineSnapshot, QuarantineState,
};

const PACK_HEADER_BYTES: u64 = 12;
const PACK_ENTRY_HEADER_MAX_BYTES: usize = 16;
const FIX_THIN_BASE_OVERHEAD: u64 = 128;
const FIX_THIN_STRUCTURAL_MULTIPLIER: u64 = 8;

/// Explicit authorization decision attached to existing-base metadata.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThinBaseAuthorization {
    /// Repository policy permits using the object as a push delta base.
    Allowed,
    /// Policy forbids using the object for this push.
    Denied,
}

/// Canonical metadata for one existing object that may thicken a PACK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedThinBase {
    /// Tenant to which the authorization is bound.
    pub tenant: TenantId,
    /// Repository to which the authorization is bound.
    pub repository: RepositoryId,
    /// Canonical object ID.
    pub oid: ObjectId,
    /// Canonical object kind.
    pub kind: ObjectKind,
    /// Exact decoded payload bytes.
    pub payload_bytes: u64,
    /// Explicit policy decision.
    pub authorization: ThinBaseAuthorization,
    /// Nonzero provider generation/version binding metadata and subsequent range reads.
    pub authorization_version: u64,
}

/// Non-sensitive existing-base source failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("authorized thin-pack base source failed")]
pub struct ThinBaseSourceError;

/// Repository-scoped canonical metadata and exact-range source for existing bases.
#[async_trait]
pub trait AuthorizedThinBaseProvider: Send {
    /// Describe requested bases without returning payload bytes.
    ///
    /// Rows may be returned in any order. Missing, duplicate, unrequested, denied, or differently
    /// scoped rows are rejected by the planner. Implementations must honor `max_metadata_bytes`
    /// before materializing the response.
    async fn describe_bases(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oids: &[ObjectId],
        max_metadata_bytes: u64,
    ) -> Result<Vec<AuthorizedThinBase>, ThinBaseSourceError>;

    /// Read exactly one bounded canonical payload range.
    ///
    /// Implementations must read from the same immutable canonical object described by
    /// [`Self::describe_bases`] at the exact `authorization_version`. The rewriter hashes the
    /// complete stream and rejects any mismatch.
    async fn read_base_range(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: ObjectId,
        authorization_version: u64,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, ThinBaseSourceError>;
}

/// Explicit deterministic thickening work checkpoint.
pub trait FixThinWork {
    /// Charge planning, range-copy, hashing, or compression work units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed thickening work allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixThinWorkLimit {
    remaining: u64,
}

impl FixThinWorkLimit {
    /// Construct an exact allowance.
    #[must_use]
    pub const fn new(units: u64) -> Self {
        Self { remaining: units }
    }

    /// Return remaining units.
    #[must_use]
    pub const fn remaining(&self) -> u64 {
        self.remaining
    }
}

impl FixThinWork for FixThinWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

struct ValidationWorkAdapter<'a, W>(&'a mut W);

impl<W> PushPackValidationWork for ValidationWorkAdapter<'_, W>
where
    W: FixThinWork,
{
    fn charge(&mut self, units: u64) -> bool {
        self.0.charge(units)
    }
}

struct ValidationControlAdapter<'a, C> {
    control: &'a mut C,
    last_observed_at: &'a mut OffsetDateTime,
    rollback_detected: bool,
}

impl<C> PushPackValidationControl for ValidationControlAdapter<'_, C>
where
    C: FixThinControl,
{
    fn observe(&mut self) -> PushPackValidationObservation {
        let observation = self.control.observe();
        if observation.observed_at < *self.last_observed_at {
            self.rollback_detected = true;
            return PushPackValidationObservation {
                observed_at: *self.last_observed_at,
                cancelled: true,
            };
        }
        *self.last_observed_at = observation.observed_at;
        PushPackValidationObservation {
            observed_at: observation.observed_at,
            cancelled: observation.cancelled,
        }
    }
}

/// Caller-supplied cancellation and monotonic-time observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixThinObservation {
    /// Explicit observation time.
    pub observed_at: OffsetDateTime,
    /// Explicit cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral thickening control.
pub trait FixThinControl {
    /// Return the latest explicit observation.
    fn observe(&mut self) -> FixThinObservation;
}

/// Bounded planner and streaming-rewriter configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixThinOptions {
    /// Shared receive-pack limits already used by incremental validation.
    pub limits: ReceivePackLimits,
    /// Maximum distinct external bases added.
    pub max_added_bases: usize,
    /// Maximum aggregate decoded bytes across added bases.
    pub max_added_base_bytes: u64,
    /// Maximum source quarantine range read.
    pub source_chunk_bytes: usize,
    /// Maximum existing-base range read.
    pub base_chunk_bytes: usize,
    /// Maximum compression/output append chunk.
    pub output_chunk_bytes: usize,
    /// Explicit phase start.
    pub started_at: OffsetDateTime,
    /// Explicit phase deadline.
    pub deadline: OffsetDateTime,
}

impl FixThinOptions {
    /// Validate hard ceilings and time relationships.
    ///
    /// # Errors
    ///
    /// Returns [`FixThinError::InvalidLimits`] for zero or inconsistent limits.
    pub fn validate(self) -> Result<Self, FixThinError> {
        let limits = self
            .limits
            .validate()
            .map_err(|_| FixThinError::InvalidLimits)?;
        let duration = time::Duration::try_from(limits.max_duration)
            .map_err(|_| FixThinError::InvalidLimits)?;
        let chunks = [
            self.source_chunk_bytes,
            self.base_chunk_bytes,
            self.output_chunk_bytes,
        ];
        let active_base_bytes = u64::try_from(self.base_chunk_bytes).ok().and_then(|base| {
            u64::try_from(self.output_chunk_bytes)
                .ok()
                .and_then(|output| base.checked_add(output))
        });
        if self.max_added_bases == 0
            || self.max_added_bases > limits.max_objects
            || self.max_added_base_bytes == 0
            || self.max_added_base_bytes > limits.max_inflated_bytes
            || chunks.contains(&0)
            || chunks.iter().any(|chunk| {
                u64::try_from(*chunk)
                    .ok()
                    .is_none_or(|bytes| bytes > limits.bytes_in_flight)
            })
            || active_base_bytes.is_none_or(|bytes| bytes > limits.bytes_in_flight)
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > duration
        {
            return Err(FixThinError::InvalidLimits);
        }
        Ok(Self { limits, ..self })
    }
}

/// Typed pre-publication thin-pack failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum FixThinError {
    /// Limits are invalid or inconsistent.
    #[error("invalid thin-pack fix limits")]
    InvalidLimits,
    /// Source validation evidence, index spans, or target quarantine state is inconsistent.
    #[error("thin-pack fix input binding is invalid")]
    InvalidBinding,
    /// No rewrite is required for an already self-contained PACK.
    #[error("thin-pack fix is not required")]
    NotRequired,
    /// A requested external base is missing.
    #[error("thin-pack external base is missing")]
    MissingBase,
    /// Existing-base metadata or payload does not match the authorized canonical object.
    #[error("thin-pack external base is corrupt")]
    CorruptBase,
    /// Existing-base repository scope or policy authorization is invalid.
    #[error("thin-pack external base is unauthorized")]
    UnauthorizedBase,
    /// Base count, bytes, object count, output bytes, or structural memory exceeded a bound.
    #[error("thin-pack fix exceeds configured bounds")]
    Limit,
    /// Source or target quarantine operation failed.
    #[error("thin-pack quarantine operation failed")]
    Quarantine,
    /// Compression made no progress or failed.
    #[error("thin-pack base compression failed")]
    Compression,
    /// Explicit deterministic work allowance was exhausted.
    #[error("thin-pack fix work budget exhausted")]
    WorkBudget,
    /// Explicit cancellation was observed.
    #[error("thin-pack fix cancelled")]
    Cancelled,
    /// Explicit deadline elapsed or time moved backwards.
    #[error("thin-pack fix deadline elapsed")]
    Deadline,
    /// A bounded allocation failed.
    #[error("thin-pack fix bounded allocation failed")]
    Allocation,
    /// Replacement PACK revalidation failed with a typed structural/index error.
    #[error("thin-pack replacement validation failed: {0}")]
    Validation(PushPackValidationError),
}

/// One deterministic direct-base addition in a [`FixThinPlan`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixThinBase {
    oid: ObjectId,
    kind: ObjectKind,
    payload_bytes: u64,
    authorization_version: u64,
}

impl FixThinBase {
    /// Return the canonical object ID.
    #[must_use]
    pub const fn oid(&self) -> ObjectId {
        self.oid
    }

    /// Return the canonical object kind.
    #[must_use]
    pub const fn kind(&self) -> ObjectKind {
        self.kind
    }

    /// Return exact decoded payload bytes.
    #[must_use]
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    /// Return the provider generation/version required for every payload range read.
    #[must_use]
    pub const fn authorization_version(&self) -> u64 {
        self.authorization_version
    }
}

/// Immutable deterministic plan for converting one sealed thin PACK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixThinPlan {
    tenant: TenantId,
    repository: RepositoryId,
    source_quarantine: crate::protocol::push_quarantine::QuarantineId,
    source_generation: u64,
    source_pack_checksum: ObjectId,
    source_index_checksum: ObjectId,
    bases: Vec<FixThinBase>,
    added_base_bytes: u64,
    work_units: u64,
    output_upper_bound: u64,
    fingerprint: ObjectId,
}

impl FixThinPlan {
    /// Borrow the tenant scope.
    #[must_use]
    pub const fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Borrow the repository scope.
    #[must_use]
    pub const fn repository(&self) -> &RepositoryId {
        &self.repository
    }

    /// Borrow required bases in deterministic OID order.
    #[must_use]
    pub fn bases(&self) -> &[FixThinBase] {
        &self.bases
    }

    /// Return exact distinct added-base count.
    #[must_use]
    pub fn added_base_count(&self) -> usize {
        self.bases.len()
    }

    /// Return exact aggregate decoded added-base bytes.
    #[must_use]
    pub const fn added_base_bytes(&self) -> u64 {
        self.added_base_bytes
    }

    /// Return deterministic planned work units.
    #[must_use]
    pub const fn work_units(&self) -> u64 {
        self.work_units
    }

    /// Return a conservative complete output byte bound.
    #[must_use]
    pub const fn output_upper_bound(&self) -> u64 {
        self.output_upper_bound
    }

    /// Return deterministic plan binding.
    #[must_use]
    pub const fn fingerprint(&self) -> ObjectId {
        self.fingerprint
    }
}

/// Exact streaming thickening measurements.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FixThinReport {
    /// Source PACK bytes read, excluding its header and trailer.
    pub copied_entry_bytes: u64,
    /// Canonical existing-base bytes read and hashed.
    pub added_base_bytes: u64,
    /// New compressed direct-entry bytes, including entry headers.
    pub added_packed_bytes: u64,
    /// Complete rewritten PACK bytes including trailer.
    pub output_pack_bytes: u64,
    /// Source range operations.
    pub source_reads: u64,
    /// Existing-base range operations.
    pub base_reads: u64,
    /// Target append operations.
    pub target_appends: u64,
}

/// Validated self-contained replacement quarantine and index.
#[derive(Clone, Debug)]
pub struct FixedThinPack {
    /// Complete self-contained validation result bound to the replacement quarantine.
    pub validated: ValidatedPushPack,
    /// Replacement immutable manifest.
    pub manifest: QuarantineManifest,
    /// Exact rewrite metrics.
    pub report: FixThinReport,
    /// Plan fingerprint used for this rewrite.
    pub plan_fingerprint: ObjectId,
}

/// Build a deterministic direct-base thickening plan.
///
/// Canonical existing objects are always recompressed as direct entries. Consequently their
/// storage-level delta dependencies are eliminated and cannot remain transitive dependencies in
/// the replacement PACK.
///
/// # Errors
///
/// Returns a typed pre-publication error for invalid validation evidence, unavailable or
/// unauthorized bases, inconsistent metadata, limits, cancellation, or provider failure.
pub async fn plan_fix_thin_pack<P, W, C>(
    validated: &ValidatedPushPack,
    source: &QuarantineSnapshot,
    provider: &mut P,
    options: FixThinOptions,
    work: &mut W,
    control: &mut C,
) -> Result<FixThinPlan, FixThinError>
where
    P: AuthorizedThinBaseProvider,
    W: FixThinWork,
    C: FixThinControl,
{
    let options = options.validate()?;
    let mut last_observed_at = options.started_at;
    checkpoint(&options, work, control, &mut last_observed_at, 1)?;
    let manifest = validate_source(validated, source)?;
    if validated.attestation.self_contained() {
        return Err(FixThinError::NotRequired);
    }
    let mut required = Vec::new();
    let mut required_set = HashSet::new();
    for row in &validated.index {
        if let PushPackEntryKind::RefDelta {
            base_oid,
            external: true,
        } = row.representation
        {
            if required_set.contains(&base_oid) {
                continue;
            }
            required_set
                .try_reserve(1)
                .map_err(|_| FixThinError::Allocation)?;
            required
                .try_reserve(1)
                .map_err(|_| FixThinError::Allocation)?;
            required_set.insert(base_oid);
            required.push(base_oid);
            if required.len() > options.max_added_bases {
                return Err(FixThinError::Limit);
            }
        }
    }
    if required.is_empty() {
        return Err(FixThinError::InvalidBinding);
    }
    required.sort_unstable();
    let requested = required;
    validate_prepared_working_set(
        validated.index.len(),
        requested.len(),
        options.limits.max_prepared_memory_bytes,
    )?;
    checkpoint(
        &options,
        work,
        control,
        &mut last_observed_at,
        u64::try_from(requested.len()).map_err(|_| FixThinError::Limit)?,
    )?;
    let described = provider
        .describe_bases(
            &source.tenant,
            &source.repository,
            &requested,
            options.limits.max_prepared_memory_bytes,
        )
        .await
        .map_err(|_| FixThinError::MissingBase)?;
    checkpoint(&options, work, control, &mut last_observed_at, 1)?;
    let mut requested_set = HashSet::new();
    requested_set
        .try_reserve(requested.len())
        .map_err(|_| FixThinError::Allocation)?;
    requested_set.extend(requested.iter().copied());
    let mut original_oids = HashSet::new();
    original_oids
        .try_reserve(validated.index.len())
        .map_err(|_| FixThinError::Allocation)?;
    original_oids.extend(validated.index.iter().map(|row| row.oid));
    let mut returned = HashSet::new();
    returned
        .try_reserve(requested.len())
        .map_err(|_| FixThinError::Allocation)?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(requested.len())
        .map_err(|_| FixThinError::Allocation)?;
    for base in described {
        if !requested_set.contains(&base.oid)
            || !returned.insert(base.oid)
            || original_oids.contains(&base.oid)
            || base.oid.algo() != manifest.hash_algo
        {
            return Err(FixThinError::CorruptBase);
        }
        if base.tenant != source.tenant
            || base.repository != source.repository
            || base.authorization != ThinBaseAuthorization::Allowed
        {
            return Err(FixThinError::UnauthorizedBase);
        }
        if base.payload_bytes > options.limits.max_object_bytes || base.authorization_version == 0 {
            return Err(FixThinError::Limit);
        }
        rows.push(base);
    }
    if rows.len() != requested.len() {
        return Err(FixThinError::MissingBase);
    }
    u64::try_from(rows.len())
        .ok()
        .and_then(|count| {
            count.checked_mul(std::mem::size_of::<FixThinBase>() as u64 + FIX_THIN_BASE_OVERHEAD)
        })
        .filter(|bytes| *bytes <= options.limits.max_prepared_memory_bytes)
        .ok_or(FixThinError::Limit)?;
    rows.sort_unstable_by_key(|base| base.oid);
    let mut bases = Vec::new();
    bases
        .try_reserve_exact(rows.len())
        .map_err(|_| FixThinError::Allocation)?;
    let mut added_base_bytes = 0_u64;
    let mut packed_upper = 0_u64;
    for base in rows {
        added_base_bytes = added_base_bytes
            .checked_add(base.payload_bytes)
            .filter(|bytes| *bytes <= options.max_added_base_bytes)
            .ok_or(FixThinError::Limit)?;
        let compressed_upper = zlib_upper_bound(base.payload_bytes)?;
        packed_upper = packed_upper
            .checked_add(PACK_ENTRY_HEADER_MAX_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(compressed_upper))
            .ok_or(FixThinError::Limit)?;
        bases.push(FixThinBase {
            oid: base.oid,
            kind: base.kind,
            payload_bytes: base.payload_bytes,
            authorization_version: base.authorization_version,
        });
    }
    validated
        .summary
        .inflated_bytes
        .checked_add(added_base_bytes)
        .filter(|bytes| *bytes <= options.limits.max_inflated_bytes)
        .ok_or(FixThinError::Limit)?;
    let output_upper_bound = manifest
        .pack_bytes
        .checked_add(packed_upper)
        .filter(|bytes| *bytes <= options.limits.max_pack_bytes)
        .ok_or(FixThinError::Limit)?;
    let object_count = validated
        .index
        .len()
        .checked_add(bases.len())
        .filter(|count| *count <= options.limits.max_objects)
        .and_then(|count| u32::try_from(count).ok())
        .ok_or(FixThinError::Limit)?;
    let copied = manifest
        .pack_bytes
        .checked_sub(PACK_HEADER_BYTES)
        .and_then(|bytes| bytes.checked_sub(u64::try_from(manifest.hash_algo.len()).ok()?))
        .ok_or(FixThinError::InvalidBinding)?;
    let work_units = copied
        .checked_add(added_base_bytes.checked_mul(2).ok_or(FixThinError::Limit)?)
        .and_then(|units| units.checked_add(u64::from(object_count)))
        .ok_or(FixThinError::Limit)?;
    let fingerprint = plan_fingerprint(
        &source.tenant,
        &source.repository,
        source,
        validated,
        &bases,
        added_base_bytes,
        work_units,
        output_upper_bound,
    )?;
    Ok(FixThinPlan {
        tenant: source.tenant.clone(),
        repository: source.repository.clone(),
        source_quarantine: source.id,
        source_generation: source.generation,
        source_pack_checksum: validated.attestation.pack_checksum(),
        source_index_checksum: validated.attestation.index_checksum(),
        bases,
        added_base_bytes,
        work_units,
        output_upper_bound,
        fingerprint,
    })
}

/// Stream a planned self-contained PACK into a distinct empty quarantine.
///
/// The source and target are independently fenced. On any error the source remains sealed and the
/// target remains unpublished for caller-driven idempotent discard. After any error, callers must
/// use their retained owner token to obtain the target's current fence and issue a terminal
/// discard; the originally supplied fence may be stale after a successful finish. Cleanup failure
/// should be queued for the quarantine orphan sweeper. The function never mutates or promotes the
/// source and does not promote the target.
///
/// # Errors
///
/// Returns a typed error for stale bindings, invalid target state, range/provider failures,
/// canonical hash mismatch, compression failure, limits, cancellation, or attestation failure.
#[allow(clippy::too_many_arguments)]
pub async fn rewrite_fix_thin_pack<S, T, P, W, C>(
    plan: &FixThinPlan,
    validated: ValidatedPushPack,
    source: &mut S,
    target: &mut T,
    target_fence: QuarantineFence,
    provider: &mut P,
    options: FixThinOptions,
    work: &mut W,
    control: &mut C,
) -> Result<FixedThinPack, FixThinError>
where
    S: PackQuarantine,
    T: PackQuarantine,
    P: AuthorizedThinBaseProvider,
    W: FixThinWork,
    C: FixThinControl,
{
    let options = options.validate()?;
    let mut last_observed_at = options.started_at;
    let source_snapshot = source.snapshot();
    let source_manifest = validate_source(&validated, &source_snapshot)?.clone();
    validate_plan(plan, &validated, &source_snapshot, &options)?;
    validate_target(plan, &source_snapshot, &target.snapshot(), target_fence)?;
    validate_entry_spans(&validated.index, &source_manifest)?;

    let total_count = validated
        .index
        .len()
        .checked_add(plan.bases.len())
        .and_then(|count| u32::try_from(count).ok())
        .ok_or(FixThinError::Limit)?;
    checkpoint(
        &options,
        work,
        control,
        &mut last_observed_at,
        u64::from(total_count),
    )?;
    let source_header = source
        .read_range(validated.fence, 0, PACK_HEADER_BYTES as usize)
        .await
        .map_err(|_| FixThinError::Quarantine)?;
    checkpoint(&options, work, control, &mut last_observed_at, 1)?;
    let mut source_reads = 1_u64;
    if source_reads > options.limits.max_range_operations {
        return Err(FixThinError::Limit);
    }
    let output_header = rewrite_pack_header(&source_header, validated.index.len(), total_count)?;
    let mut writer = RewriteWriter::new(
        target,
        target_fence,
        source_manifest.hash_algo,
        options.limits.max_pack_bytes,
        options.limits.max_object_operations,
        options.output_chunk_bytes,
    );
    writer
        .append_hashed(
            &output_header,
            &options,
            work,
            control,
            &mut last_observed_at,
        )
        .await?;
    checkpoint(&options, work, control, &mut last_observed_at, 1)?;

    let trailer_bytes =
        u64::try_from(source_manifest.hash_algo.len()).map_err(|_| FixThinError::InvalidBinding)?;
    let trailer_start = source_manifest
        .pack_bytes
        .checked_sub(trailer_bytes)
        .ok_or(FixThinError::InvalidBinding)?;
    let mut source_offset = PACK_HEADER_BYTES;
    while source_offset < trailer_start {
        let remaining = trailer_start
            .checked_sub(source_offset)
            .ok_or(FixThinError::InvalidBinding)?;
        let length = bounded_length(remaining, options.source_chunk_bytes)?;
        checkpoint(
            &options,
            work,
            control,
            &mut last_observed_at,
            u64::try_from(length).map_err(|_| FixThinError::Limit)?,
        )?;
        let next_source_reads = source_reads
            .checked_add(1)
            .filter(|reads| *reads <= options.limits.max_range_operations)
            .ok_or(FixThinError::Limit)?;
        let bytes = source
            .read_range(validated.fence, source_offset, length)
            .await
            .map_err(|_| FixThinError::Quarantine)?;
        checkpoint(&options, work, control, &mut last_observed_at, 1)?;
        if bytes.len() != length {
            return Err(FixThinError::Quarantine);
        }
        writer
            .append_hashed(&bytes, &options, work, control, &mut last_observed_at)
            .await?;
        checkpoint(&options, work, control, &mut last_observed_at, 1)?;
        source_offset = source_offset
            .checked_add(u64::try_from(length).map_err(|_| FixThinError::Limit)?)
            .ok_or(FixThinError::Limit)?;
        source_reads = next_source_reads;
    }

    let mut base_reads = 0_u64;
    let packed_before_bases = writer.offset;
    for base in &plan.bases {
        let remaining_reads = options
            .limits
            .max_external_operations
            .checked_sub(base_reads)
            .ok_or(FixThinError::Limit)?;
        let reads = write_direct_base(
            &mut writer,
            base,
            provider,
            &plan.tenant,
            &plan.repository,
            &options,
            work,
            control,
            &mut last_observed_at,
            remaining_reads,
        )
        .await?;
        base_reads = base_reads.checked_add(reads).ok_or(FixThinError::Limit)?;
        if base_reads > options.limits.max_external_operations {
            return Err(FixThinError::Limit);
        }
    }
    if writer.offset > plan.output_upper_bound {
        return Err(FixThinError::Limit);
    }
    let pack_checksum = writer.pack_hasher.clone().finish()?;
    writer
        .append_unhashed(
            pack_checksum.as_bytes(),
            &options,
            work,
            control,
            &mut last_observed_at,
        )
        .await?;
    checkpoint(&options, work, control, &mut last_observed_at, 1)?;
    let output_pack_bytes = writer.offset;
    if output_pack_bytes > plan.output_upper_bound {
        return Err(FixThinError::Limit);
    }
    let added_structural_bytes = plan.bases.iter().try_fold(0_u64, |total, base| {
        if base.kind == ObjectKind::Blob {
            return Ok(total);
        }
        let bytes = base
            .payload_bytes
            .checked_mul(FIX_THIN_STRUCTURAL_MULTIPLIER)
            .and_then(|bytes| bytes.checked_add(FIX_THIN_BASE_OVERHEAD))
            .ok_or(FixThinError::Limit)?;
        total.checked_add(bytes).ok_or(FixThinError::Limit)
    })?;
    let manifest = QuarantineManifest {
        hash_algo: source_manifest.hash_algo,
        pack_bytes: output_pack_bytes,
        object_count: total_count,
        pack_checksum: Some(pack_checksum),
        metadata_bytes: source_manifest
            .metadata_bytes
            .checked_add(added_structural_bytes)
            .ok_or(FixThinError::Limit)?,
        self_contained: false,
        index_checksum: None,
    };
    if manifest.metadata_bytes > options.limits.max_metadata_memory_bytes {
        return Err(FixThinError::Limit);
    }
    let target_appends = writer.appends;
    let finished_fence = writer
        .target
        .finish(writer.fence, Some(pack_checksum), manifest.clone())
        .await
        .map_err(|_| FixThinError::Quarantine)?;
    checkpoint(&options, work, control, &mut last_observed_at, 1)?;
    drop(writer);

    let mut validation_bases = NoAuthorizedPushBases;
    let mut validation_work = ValidationWorkAdapter(work);
    let mut validation_control = ValidationControlAdapter {
        control,
        last_observed_at: &mut last_observed_at,
        rollback_detected: false,
    };
    let validation_result = validate_quarantined_pack(
        target,
        finished_fence,
        &mut validation_bases,
        &mut validation_control,
        &mut validation_work,
        PushPackValidationOptions {
            limits: options.limits,
            range_chunk_bytes: options.source_chunk_bytes,
            max_index_bytes: options.limits.max_metadata_memory_bytes,
            started_at: options.started_at,
            deadline: options.deadline,
        },
    )
    .await;
    if validation_control.rollback_detected {
        return Err(FixThinError::Deadline);
    }
    let revalidated = validation_result.map_err(map_validation_error)?;
    if !revalidated.attestation.self_contained()
        || revalidated.attestation.pack_checksum() != pack_checksum
        || revalidated.attestation.object_count() != total_count
    {
        return Err(FixThinError::InvalidBinding);
    }
    let final_manifest = target
        .snapshot()
        .manifest
        .ok_or(FixThinError::InvalidBinding)?;

    let added_packed_bytes = output_pack_bytes
        .checked_sub(
            u64::try_from(source_manifest.hash_algo.len()).map_err(|_| FixThinError::Limit)?,
        )
        .and_then(|bytes| bytes.checked_sub(packed_before_bases))
        .ok_or(FixThinError::Limit)?;
    let copied_entry_bytes = trailer_start
        .checked_sub(PACK_HEADER_BYTES)
        .ok_or(FixThinError::InvalidBinding)?;
    Ok(FixedThinPack {
        validated: revalidated,
        manifest: final_manifest,
        report: FixThinReport {
            copied_entry_bytes,
            added_base_bytes: plan.added_base_bytes,
            added_packed_bytes,
            output_pack_bytes,
            source_reads,
            base_reads,
            target_appends,
        },
        plan_fingerprint: plan.fingerprint,
    })
}

struct RewriteWriter<'a, T> {
    target: &'a mut T,
    fence: QuarantineFence,
    offset: u64,
    maximum: u64,
    appends: u64,
    maximum_appends: u64,
    maximum_chunk: usize,
    pack_hasher: RawHasher,
}

impl<'a, T> RewriteWriter<'a, T>
where
    T: PackQuarantine,
{
    fn new(
        target: &'a mut T,
        fence: QuarantineFence,
        hash_algo: HashAlgo,
        maximum: u64,
        maximum_appends: u64,
        maximum_chunk: usize,
    ) -> Self {
        Self {
            target,
            fence,
            offset: 0,
            maximum,
            appends: 0,
            maximum_appends,
            maximum_chunk,
            pack_hasher: RawHasher::new(hash_algo),
        }
    }

    async fn append_hashed<W, C>(
        &mut self,
        bytes: &[u8],
        options: &FixThinOptions,
        work: &mut W,
        control: &mut C,
        last_observed_at: &mut OffsetDateTime,
    ) -> Result<(), FixThinError>
    where
        W: FixThinWork,
        C: FixThinControl,
    {
        self.pack_hasher.update(bytes);
        self.append_unhashed(bytes, options, work, control, last_observed_at)
            .await
    }

    async fn append_unhashed<W, C>(
        &mut self,
        bytes: &[u8],
        options: &FixThinOptions,
        work: &mut W,
        control: &mut C,
        last_observed_at: &mut OffsetDateTime,
    ) -> Result<(), FixThinError>
    where
        W: FixThinWork,
        C: FixThinControl,
    {
        for chunk in bytes.chunks(self.maximum_chunk) {
            checkpoint(options, work, control, last_observed_at, 0)?;
            let length = u64::try_from(chunk.len()).map_err(|_| FixThinError::Limit)?;
            let next = self
                .offset
                .checked_add(length)
                .filter(|offset| *offset <= self.maximum)
                .ok_or(FixThinError::Limit)?;
            let next_appends = self
                .appends
                .checked_add(1)
                .filter(|count| *count <= self.maximum_appends)
                .ok_or(FixThinError::Limit)?;
            let actual = self
                .target
                .append(self.fence, self.offset, chunk)
                .await
                .map_err(|_| FixThinError::Quarantine)?;
            if actual != next {
                return Err(FixThinError::Quarantine);
            }
            self.offset = next;
            self.appends = next_appends;
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn write_direct_base<T, P, W, C>(
    writer: &mut RewriteWriter<'_, T>,
    base: &FixThinBase,
    provider: &mut P,
    tenant: &TenantId,
    repository: &RepositoryId,
    options: &FixThinOptions,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
    maximum_reads: u64,
) -> Result<u64, FixThinError>
where
    T: PackQuarantine,
    P: AuthorizedThinBaseProvider,
    W: FixThinWork,
    C: FixThinControl,
{
    let header = encode_entry_header(base.kind, base.payload_bytes)?;
    writer
        .append_hashed(&header, options, work, control, last_observed_at)
        .await?;
    checkpoint(options, work, control, last_observed_at, 1)?;
    let mut object_hasher =
        CanonicalObjectHasher::new(base.kind, base.payload_bytes, base.oid.algo());
    let mut compressor = Compress::new(Compression::default(), true);
    let mut output = Vec::new();
    output
        .try_reserve_exact(options.output_chunk_bytes)
        .map_err(|_| FixThinError::Allocation)?;
    output.resize(options.output_chunk_bytes, 0);
    let mut payload_offset = 0_u64;
    let mut reads = 0_u64;
    while payload_offset < base.payload_bytes {
        let remaining = base
            .payload_bytes
            .checked_sub(payload_offset)
            .ok_or(FixThinError::CorruptBase)?;
        let length = bounded_length(remaining, options.base_chunk_bytes)?;
        let length_u64 = u64::try_from(length).map_err(|_| FixThinError::Limit)?;
        checkpoint(
            options,
            work,
            control,
            last_observed_at,
            length_u64.checked_mul(2).ok_or(FixThinError::Limit)?,
        )?;
        let input = provider
            .read_base_range(
                tenant,
                repository,
                base.oid,
                base.authorization_version,
                payload_offset,
                length,
            )
            .await
            .map_err(|_| FixThinError::MissingBase)?;
        checkpoint(options, work, control, last_observed_at, 1)?;
        if input.len() != length {
            return Err(FixThinError::CorruptBase);
        }
        reads = reads.checked_add(1).ok_or(FixThinError::Limit)?;
        if reads > maximum_reads {
            return Err(FixThinError::Limit);
        }
        object_hasher.update(&input);
        compress_input(
            writer,
            &mut compressor,
            &input,
            &mut output,
            options,
            work,
            control,
            last_observed_at,
        )
        .await?;
        payload_offset = payload_offset
            .checked_add(length_u64)
            .ok_or(FixThinError::Limit)?;
    }
    finish_compression(
        writer,
        &mut compressor,
        &mut output,
        options,
        work,
        control,
        last_observed_at,
    )
    .await?;
    if object_hasher.finish()? != base.oid {
        return Err(FixThinError::CorruptBase);
    }
    Ok(reads)
}

async fn compress_input<T, W, C>(
    writer: &mut RewriteWriter<'_, T>,
    compressor: &mut Compress,
    input: &[u8],
    output: &mut [u8],
    options: &FixThinOptions,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
) -> Result<(), FixThinError>
where
    T: PackQuarantine,
    W: FixThinWork,
    C: FixThinControl,
{
    let mut input_offset = 0_usize;
    while input_offset < input.len() {
        let before_in = compressor.total_in();
        let before_out = compressor.total_out();
        let status = compressor
            .compress(&input[input_offset..], output, FlushCompress::None)
            .map_err(|_| FixThinError::Compression)?;
        if status == Status::StreamEnd {
            return Err(FixThinError::Compression);
        }
        let consumed = usize::try_from(
            compressor
                .total_in()
                .checked_sub(before_in)
                .ok_or(FixThinError::Compression)?,
        )
        .map_err(|_| FixThinError::Limit)?;
        let produced = usize::try_from(
            compressor
                .total_out()
                .checked_sub(before_out)
                .ok_or(FixThinError::Compression)?,
        )
        .map_err(|_| FixThinError::Limit)?;
        input_offset = input_offset
            .checked_add(consumed)
            .ok_or(FixThinError::Limit)?;
        if produced > 0 {
            writer
                .append_hashed(
                    &output[..produced],
                    options,
                    work,
                    control,
                    last_observed_at,
                )
                .await?;
            checkpoint(options, work, control, last_observed_at, 1)?;
        }
        if consumed == 0 && produced == 0 {
            return Err(FixThinError::Compression);
        }
    }
    Ok(())
}

async fn finish_compression<T, W, C>(
    writer: &mut RewriteWriter<'_, T>,
    compressor: &mut Compress,
    output: &mut [u8],
    options: &FixThinOptions,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
) -> Result<(), FixThinError>
where
    T: PackQuarantine,
    W: FixThinWork,
    C: FixThinControl,
{
    loop {
        let before_in = compressor.total_in();
        let before_out = compressor.total_out();
        let status = compressor
            .compress(&[], output, FlushCompress::Finish)
            .map_err(|_| FixThinError::Compression)?;
        let consumed = compressor
            .total_in()
            .checked_sub(before_in)
            .ok_or(FixThinError::Compression)?;
        let produced = usize::try_from(
            compressor
                .total_out()
                .checked_sub(before_out)
                .ok_or(FixThinError::Compression)?,
        )
        .map_err(|_| FixThinError::Limit)?;
        if produced > 0 {
            writer
                .append_hashed(
                    &output[..produced],
                    options,
                    work,
                    control,
                    last_observed_at,
                )
                .await?;
            checkpoint(options, work, control, last_observed_at, 1)?;
        }
        if status == Status::StreamEnd {
            return Ok(());
        }
        if consumed == 0 && produced == 0 {
            return Err(FixThinError::Compression);
        }
    }
}

fn validate_source<'a>(
    validated: &ValidatedPushPack,
    source: &'a QuarantineSnapshot,
) -> Result<&'a QuarantineManifest, FixThinError> {
    let manifest = source
        .manifest
        .as_ref()
        .ok_or(FixThinError::InvalidBinding)?;
    let count = u32::try_from(validated.index.len()).map_err(|_| FixThinError::InvalidBinding)?;
    if source.state != QuarantineState::Validated
        || source.id != validated.fence.id()
        || source.generation != validated.fence.generation()
        || source.bytes != manifest.pack_bytes
        || manifest.object_count != count
        || validated.attestation.object_count() != count
        || !validated.attestation.validation_complete()
        || manifest.pack_checksum != Some(validated.attestation.pack_checksum())
        || manifest.index_checksum != Some(validated.attestation.index_checksum())
        || manifest.self_contained != validated.attestation.self_contained()
        || validated.summary.objects != u64::from(count)
    {
        return Err(FixThinError::InvalidBinding);
    }
    Ok(manifest)
}

fn validate_target(
    plan: &FixThinPlan,
    source: &QuarantineSnapshot,
    target: &QuarantineSnapshot,
    fence: QuarantineFence,
) -> Result<(), FixThinError> {
    if target.state != QuarantineState::Receiving
        || target.bytes != 0
        || target.manifest.is_some()
        || target.tenant != plan.tenant
        || target.repository != plan.repository
        || target.tenant != source.tenant
        || target.repository != source.repository
        || target.id == source.id
        || target.id != fence.id()
        || target.generation != fence.generation()
    {
        return Err(FixThinError::InvalidBinding);
    }
    Ok(())
}

fn validate_plan(
    plan: &FixThinPlan,
    validated: &ValidatedPushPack,
    source: &QuarantineSnapshot,
    options: &FixThinOptions,
) -> Result<(), FixThinError> {
    if plan.tenant != source.tenant
        || plan.repository != source.repository
        || plan.source_quarantine != source.id
        || plan.source_generation != source.generation
        || plan.source_pack_checksum != validated.attestation.pack_checksum()
        || plan.source_index_checksum != validated.attestation.index_checksum()
        || plan.bases.is_empty()
        || plan.bases.len() > options.max_added_bases
        || plan.added_base_bytes > options.max_added_base_bytes
        || plan.output_upper_bound > options.limits.max_pack_bytes
        || plan
            .bases
            .iter()
            .any(|base| base.authorization_version == 0)
        || plan
            .bases
            .windows(2)
            .any(|window| window[0].oid >= window[1].oid)
    {
        return Err(FixThinError::InvalidBinding);
    }
    let fingerprint = plan_fingerprint(
        &source.tenant,
        &source.repository,
        source,
        validated,
        &plan.bases,
        plan.added_base_bytes,
        plan.work_units,
        plan.output_upper_bound,
    )?;
    if fingerprint != plan.fingerprint {
        return Err(FixThinError::InvalidBinding);
    }
    Ok(())
}

fn validate_entry_spans(
    rows: &[PushPackIndexRow],
    manifest: &QuarantineManifest,
) -> Result<(), FixThinError> {
    let trailer_start = manifest
        .pack_bytes
        .checked_sub(
            u64::try_from(manifest.hash_algo.len()).map_err(|_| FixThinError::InvalidBinding)?,
        )
        .ok_or(FixThinError::InvalidBinding)?;
    let mut expected_offset = PACK_HEADER_BYTES;
    for (ordinal, row) in rows.iter().enumerate() {
        let ordinal = u32::try_from(ordinal).map_err(|_| FixThinError::InvalidBinding)?;
        let entry_end = span_end(row.entry)?;
        let header_end = span_end(row.header)?;
        let base_end = span_end(row.base_header)?;
        let compressed_end = span_end(row.compressed)?;
        if row.ordinal != ordinal
            || row.entry.offset != expected_offset
            || row.entry.length == 0
            || row.header.offset != row.entry.offset
            || row.header.length == 0
            || row.base_header.offset != header_end
            || base_end != row.compressed.offset
            || row.compressed.length == 0
            || compressed_end != entry_end
            || entry_end > trailer_start
            || matches!(row.representation, PushPackEntryKind::Direct)
                != (row.base_header.length == 0)
        {
            return Err(FixThinError::InvalidBinding);
        }
        expected_offset = entry_end;
    }
    if expected_offset != trailer_start {
        return Err(FixThinError::InvalidBinding);
    }
    Ok(())
}

fn span_end(span: PushPackByteSpan) -> Result<u64, FixThinError> {
    span.offset
        .checked_add(span.length)
        .ok_or(FixThinError::InvalidBinding)
}

fn rewrite_pack_header(
    source: &[u8],
    source_count: usize,
    output_count: u32,
) -> Result<[u8; PACK_HEADER_BYTES as usize], FixThinError> {
    let source: [u8; PACK_HEADER_BYTES as usize] = source
        .try_into()
        .map_err(|_| FixThinError::InvalidBinding)?;
    let version = u32::from_be_bytes([source[4], source[5], source[6], source[7]]);
    let count = u32::from_be_bytes([source[8], source[9], source[10], source[11]]);
    if &source[..4] != b"PACK"
        || !matches!(version, 2 | 3)
        || usize::try_from(count).ok() != Some(source_count)
    {
        return Err(FixThinError::InvalidBinding);
    }
    let mut output = source;
    output[8..12].copy_from_slice(&output_count.to_be_bytes());
    Ok(output)
}

fn bounded_length(remaining: u64, maximum: usize) -> Result<usize, FixThinError> {
    let maximum = u64::try_from(maximum).map_err(|_| FixThinError::InvalidLimits)?;
    usize::try_from(remaining.min(maximum)).map_err(|_| FixThinError::Limit)
}

fn map_validation_error(error: PushPackValidationError) -> FixThinError {
    match error {
        PushPackValidationError::Cancelled => FixThinError::Cancelled,
        PushPackValidationError::Deadline => FixThinError::Deadline,
        PushPackValidationError::WorkBudget => FixThinError::WorkBudget,
        PushPackValidationError::Quarantine => FixThinError::Quarantine,
        other => FixThinError::Validation(other),
    }
}

fn validate_prepared_working_set(
    original_objects: usize,
    distinct_bases: usize,
    maximum_bytes: u64,
) -> Result<(), FixThinError> {
    let oid_bytes =
        u64::try_from(std::mem::size_of::<ObjectId>()).map_err(|_| FixThinError::Limit)?;
    let hash_entry_bytes = oid_bytes
        .checked_add(FIX_THIN_BASE_OVERHEAD)
        .ok_or(FixThinError::Limit)?;
    let original_bytes = u64::try_from(original_objects)
        .ok()
        .and_then(|count| count.checked_mul(hash_entry_bytes))
        .ok_or(FixThinError::Limit)?;
    let provider_row_bytes = u64::try_from(std::mem::size_of::<AuthorizedThinBase>())
        .map_err(|_| FixThinError::Limit)?;
    let plan_row_bytes =
        u64::try_from(std::mem::size_of::<FixThinBase>()).map_err(|_| FixThinError::Limit)?;
    let per_base_bytes = hash_entry_bytes
        .checked_mul(3)
        .and_then(|bytes| bytes.checked_add(oid_bytes.checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(provider_row_bytes.checked_mul(2)?))
        .and_then(|bytes| bytes.checked_add(plan_row_bytes))
        .ok_or(FixThinError::Limit)?;
    original_bytes
        .checked_add(
            u64::try_from(distinct_bases)
                .ok()
                .and_then(|count| count.checked_mul(per_base_bytes))
                .ok_or(FixThinError::Limit)?,
        )
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or(FixThinError::Limit)?;
    Ok(())
}

fn checkpoint<W, C>(
    options: &FixThinOptions,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
    units: u64,
) -> Result<(), FixThinError>
where
    W: FixThinWork,
    C: FixThinControl,
{
    let observation = control.observe();
    if observation.observed_at < *last_observed_at {
        return Err(FixThinError::Deadline);
    }
    *last_observed_at = observation.observed_at;
    if observation.cancelled {
        return Err(FixThinError::Cancelled);
    }
    if observation.observed_at < options.started_at || observation.observed_at > options.deadline {
        return Err(FixThinError::Deadline);
    }
    if !work.charge(units) {
        return Err(FixThinError::WorkBudget);
    }
    Ok(())
}

fn zlib_upper_bound(input: u64) -> Result<u64, FixThinError> {
    input
        .checked_add(input.checked_add(16_382).ok_or(FixThinError::Limit)? / 16_383 * 5)
        .and_then(|bytes| bytes.checked_add(6))
        .ok_or(FixThinError::Limit)
}

fn encode_entry_header(kind: ObjectKind, size: u64) -> Result<Vec<u8>, FixThinError> {
    let type_code = match kind {
        ObjectKind::Commit => 1_u8,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
        ObjectKind::Tag => 4,
    };
    let mut output = Vec::new();
    output
        .try_reserve_exact(PACK_ENTRY_HEADER_MAX_BYTES)
        .map_err(|_| FixThinError::Allocation)?;
    let mut remaining = size;
    let mut first =
        ((type_code & 7) << 4) | u8::try_from(remaining & 0x0f).map_err(|_| FixThinError::Limit)?;
    remaining >>= 4;
    if remaining != 0 {
        first |= 0x80;
    }
    output.push(first);
    while remaining != 0 {
        let mut byte = u8::try_from(remaining & 0x7f).map_err(|_| FixThinError::Limit)?;
        remaining >>= 7;
        if remaining != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if output.len() > PACK_ENTRY_HEADER_MAX_BYTES {
            return Err(FixThinError::Limit);
        }
    }
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn plan_fingerprint(
    tenant: &TenantId,
    repository: &RepositoryId,
    source: &QuarantineSnapshot,
    validated: &ValidatedPushPack,
    bases: &[FixThinBase],
    added_base_bytes: u64,
    work_units: u64,
    output_upper_bound: u64,
) -> Result<ObjectId, FixThinError> {
    let hash_algo = validated.attestation.pack_checksum().algo();
    let mut hasher = RawHasher::new(hash_algo);
    hash_field(&mut hasher, b"grit-fix-thin-plan-v1")?;
    hash_field(&mut hasher, tenant.as_str().as_bytes())?;
    hash_field(&mut hasher, repository.as_str().as_bytes())?;
    hash_field(&mut hasher, source.id.as_bytes())?;
    hasher.update(&source.generation.to_be_bytes());
    hash_field(
        &mut hasher,
        validated.attestation.pack_checksum().as_bytes(),
    )?;
    hash_field(
        &mut hasher,
        validated.attestation.index_checksum().as_bytes(),
    )?;
    hasher.update(
        &u64::try_from(bases.len())
            .map_err(|_| FixThinError::Limit)?
            .to_be_bytes(),
    );
    for base in bases {
        hash_field(&mut hasher, base.oid.as_bytes())?;
        hasher.update(&[object_kind_code(base.kind)]);
        hasher.update(&base.payload_bytes.to_be_bytes());
        hasher.update(&base.authorization_version.to_be_bytes());
    }
    hasher.update(&added_base_bytes.to_be_bytes());
    hasher.update(&work_units.to_be_bytes());
    hasher.update(&output_upper_bound.to_be_bytes());
    hasher.finish()
}

fn hash_field(hasher: &mut RawHasher, bytes: &[u8]) -> Result<(), FixThinError> {
    hasher.update(
        &u64::try_from(bytes.len())
            .map_err(|_| FixThinError::Limit)?
            .to_be_bytes(),
    );
    hasher.update(bytes);
    Ok(())
}

fn object_kind_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Blob => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Commit => 3,
        ObjectKind::Tag => 4,
    }
}

#[derive(Clone)]
enum RawHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl RawHasher {
    fn new(hash_algo: HashAlgo) -> Self {
        match hash_algo {
            HashAlgo::Sha1 => Self::Sha1(Sha1::new()),
            HashAlgo::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hasher) => Sha1Digest::update(hasher, bytes),
            Self::Sha256(hasher) => Sha2Digest::update(hasher, bytes),
        }
    }

    fn finish(self) -> Result<ObjectId, FixThinError> {
        match self {
            Self::Sha1(hasher) => ObjectId::from_bytes(&hasher.finalize()),
            Self::Sha256(hasher) => ObjectId::from_bytes(&hasher.finalize()),
        }
        .map_err(|_| FixThinError::InvalidBinding)
    }
}

struct CanonicalObjectHasher {
    hasher: RawHasher,
}

impl CanonicalObjectHasher {
    fn new(kind: ObjectKind, size: u64, hash_algo: HashAlgo) -> Self {
        let mut hasher = RawHasher::new(hash_algo);
        hasher.update(format!("{kind} {size}\0").as_bytes());
        Self { hasher }
    }

    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn finish(self) -> Result<ObjectId, FixThinError> {
        self.hasher.finish()
    }
}
