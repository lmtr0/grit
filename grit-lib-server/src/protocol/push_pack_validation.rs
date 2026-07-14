//! Incremental bounded validation and indexing of quarantined receive-pack bytes.
//!
//! The validator reads exact private quarantine ranges, never makes objects ordinarily visible,
//! and advances the quarantine attestation only after every entry and structural object validates.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use flate2::{Decompress, FlushDecompress, Status};
use grit_lib::objects::{
    parse_commit, parse_tag, parse_tree_with_oid_len, CommitData, HashAlgo, ObjectId, ObjectKind,
    TagData, TreeEntry,
};
use grit_lib::unpack_objects::apply_delta;
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_metrics::ReceivePackLimits;
use crate::protocol::push_quarantine::{
    PackQuarantine, QuarantineFence, QuarantineIndexAttestation, QuarantineManifest,
    QuarantineState,
};

const MAX_ENTRY_HEADER_BYTES: usize = 16;
const INFLATE_BUFFER_BYTES: usize = 64 << 10;
const STRUCTURAL_MEMORY_MULTIPLIER: u64 = 8;
const STRUCTURAL_OBJECT_OVERHEAD: u64 = 256;

/// Exact immutable byte span inside a PACK.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushPackByteSpan {
    /// Absolute byte offset.
    pub offset: u64,
    /// Span length.
    pub length: u64,
}

/// Validated entry representation and dependency.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushPackEntryKind {
    /// Direct commit/tree/blob/tag representation.
    Direct,
    /// OFS_DELTA bound to a validated prior entry.
    OfsDelta {
        /// Absolute base entry offset.
        base_offset: u64,
        /// Canonical resolved base object ID.
        base_oid: ObjectId,
    },
    /// REF_DELTA bound by canonical object ID.
    RefDelta {
        /// Canonical base object ID.
        base_oid: ObjectId,
        /// Whether the authorized base came from outside this quarantine.
        external: bool,
    },
}

/// One ordered validated quarantine PACK index row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushPackIndexRow {
    /// Zero-based PACK entry ordinal.
    pub ordinal: u32,
    /// Canonical resolved object ID.
    pub oid: ObjectId,
    /// Canonical resolved object kind.
    pub kind: ObjectKind,
    /// Complete entry span including entry/base headers and zlib stream.
    pub entry: PushPackByteSpan,
    /// PACK type/declared-size header span.
    pub header: PushPackByteSpan,
    /// OFS/REF base header span, empty for direct entries.
    pub base_header: PushPackByteSpan,
    /// Exact one-stream compressed zlib span.
    pub compressed: PushPackByteSpan,
    /// PACK-header declared direct payload or delta-instruction bytes.
    pub declared_size: u64,
    /// Canonical resolved object payload bytes.
    pub resolved_size: u64,
    /// Resolved delta depth, zero for direct entries.
    pub delta_depth: u32,
    /// CRC32 of the complete packed entry bytes.
    pub crc32: u32,
    /// Direct or resolved delta representation.
    pub representation: PushPackEntryKind,
}

/// Parsed structural metadata retained for policy, closure, indexing, and audit preparation.
#[derive(Clone, Debug)]
pub enum PushStructuralMetadata {
    /// Parsed commit metadata.
    Commit(CommitData),
    /// Parsed ordered tree entries.
    Tree(Vec<TreeEntry>),
    /// Parsed annotated tag metadata.
    Tag(TagData),
}

/// One canonical structural object retained without blob payload bytes.
#[derive(Clone, Debug)]
pub struct PushStructuralObject {
    /// Canonical object ID.
    pub oid: ObjectId,
    /// Parsed structural value.
    pub metadata: PushStructuralMetadata,
}

/// Non-sensitive counts suitable for audit records without decoding the PACK again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PushValidationSummary {
    /// Total validated entries.
    pub objects: u64,
    /// Direct entries.
    pub direct_objects: u64,
    /// OFS_DELTA entries.
    pub ofs_deltas: u64,
    /// REF_DELTA entries.
    pub ref_deltas: u64,
    /// Resolved commits.
    pub commits: u64,
    /// Resolved trees.
    pub trees: u64,
    /// Resolved blobs.
    pub blobs: u64,
    /// Resolved annotated tags.
    pub tags: u64,
    /// Aggregate inflated direct payload and delta-instruction bytes.
    pub inflated_bytes: u64,
    /// Largest resolved delta depth.
    pub maximum_delta_depth: u32,
    /// Largest resolved-to-instruction byte ratio.
    pub maximum_delta_expansion_ratio: u64,
    /// Authorized bases read outside the quarantine.
    pub external_bases: u64,
}

/// Complete validated index, structural summary, and quarantine attestation.
#[derive(Clone, Debug)]
pub struct ValidatedPushPack {
    /// Ordered entry index.
    pub index: Vec<PushPackIndexRow>,
    /// Parsed non-blob structural objects.
    pub structural_objects: Vec<PushStructuralObject>,
    /// Non-sensitive audit/policy counts.
    pub summary: PushValidationSummary,
    /// Index and dependency evidence stored in the quarantine manifest.
    pub attestation: QuarantineIndexAttestation,
    /// Fence after the attestation upgrade.
    pub fence: QuarantineFence,
}

/// Explicit validation work checkpoint.
pub trait PushPackValidationWork {
    /// Charge deterministic parsing, inflation, hashing, or delta work units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-based validation work allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushPackValidationWorkLimit {
    remaining: u64,
}

impl PushPackValidationWorkLimit {
    /// Construct an exact work-unit allowance.
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

impl PushPackValidationWork for PushPackValidationWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Explicit caller-observed validation control state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushPackValidationObservation {
    /// Caller-observed monotonic time.
    pub observed_at: OffsetDateTime,
    /// Explicit cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral validation cancellation/time provider.
pub trait PushPackValidationControl {
    /// Return the latest explicit observation.
    fn observe(&mut self) -> PushPackValidationObservation;
}

/// Incremental indexer bounds and explicit request times.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PushPackValidationOptions {
    /// Shared push safety limits.
    pub limits: ReceivePackLimits,
    /// Maximum bytes in one private quarantine range read.
    pub range_chunk_bytes: usize,
    /// Maximum retained ordered index and dependency bookkeeping bytes.
    pub max_index_bytes: u64,
    /// Explicit validation start time.
    pub started_at: OffsetDateTime,
    /// Explicit validation deadline.
    pub deadline: OffsetDateTime,
}

impl PushPackValidationOptions {
    /// Validate range/index limits and explicit request duration.
    ///
    /// # Errors
    ///
    /// Returns [`PushPackValidationError::InvalidLimits`] for zero or inconsistent values.
    pub fn validate(self) -> Result<Self, PushPackValidationError> {
        let limits = self
            .limits
            .validate()
            .map_err(|_| PushPackValidationError::InvalidLimits)?;
        let range = u64::try_from(self.range_chunk_bytes)
            .map_err(|_| PushPackValidationError::InvalidLimits)?;
        let duration = time::Duration::try_from(limits.max_duration)
            .map_err(|_| PushPackValidationError::InvalidLimits)?;
        if self.range_chunk_bytes == 0
            || range > limits.bytes_in_flight
            || self.max_index_bytes == 0
            || self.max_index_bytes > limits.max_metadata_memory_bytes
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > duration
        {
            return Err(PushPackValidationError::InvalidLimits);
        }
        Ok(Self { limits, ..self })
    }
}

/// Explicit authorized existing base payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizedPushBase {
    /// Canonical base kind.
    pub kind: ObjectKind,
    /// Canonical base payload.
    pub data: Vec<u8>,
}

/// Non-sensitive authorized-base provider failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("authorized receive-pack base provider failed")]
pub struct AuthorizedPushBaseError;

/// Explicit repository-scoped provider for existing REF_DELTA bases.
#[async_trait]
pub trait AuthorizedPushBaseProvider: Send {
    /// Return one already-authorized base, or `None` when it is unavailable.
    ///
    /// Implementations must enforce `max_bytes` while reading so an oversized object is never
    /// materialized before this validator can reject it.
    async fn read_base(
        &mut self,
        tenant: &TenantId,
        repository: &RepositoryId,
        oid: ObjectId,
        max_bytes: u64,
    ) -> Result<Option<AuthorizedPushBase>, AuthorizedPushBaseError>;
}

/// Provider that forbids every external REF_DELTA base.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoAuthorizedPushBases;

#[async_trait]
impl AuthorizedPushBaseProvider for NoAuthorizedPushBases {
    async fn read_base(
        &mut self,
        _tenant: &TenantId,
        _repository: &RepositoryId,
        _oid: ObjectId,
        _max_bytes: u64,
    ) -> Result<Option<AuthorizedPushBase>, AuthorizedPushBaseError> {
        Ok(None)
    }
}

/// Typed pre-publication validation failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PushPackValidationError {
    /// Validation limits are zero or inconsistent.
    #[error("invalid incremental push pack validation limits")]
    InvalidLimits,
    /// Quarantine is not in validated private state or its manifest is incomplete.
    #[error("quarantine is not ready for incremental validation")]
    InvalidQuarantine,
    /// Quarantine range read or attestation failed.
    #[error("quarantine operation failed during pack validation")]
    Quarantine,
    /// Explicit cancellation was observed.
    #[error("incremental push pack validation cancelled")]
    Cancelled,
    /// Explicit deadline elapsed or time moved backwards.
    #[error("incremental push pack validation deadline elapsed")]
    Deadline,
    /// Explicit deterministic work budget was exhausted.
    #[error("incremental push pack validation work budget exhausted")]
    WorkBudget,
    /// PACK header, version, count, trailer, or entry boundary is invalid.
    #[error("incremental push pack structure is invalid")]
    InvalidPack,
    /// PACK entry header or OFS/REF base header is malformed.
    #[error("incremental push pack entry header is invalid")]
    InvalidHeader,
    /// Entry count or retained index memory exceeds its limit.
    #[error("incremental push pack index exceeds configured limits")]
    IndexLimit,
    /// Exactly one complete zlib stream was not present.
    #[error("incremental push pack compressed stream is invalid")]
    InvalidCompressedStream,
    /// Declared, inflated, or resolved object size is invalid or exceeds limits.
    #[error("incremental push pack object size exceeds configured limits")]
    ObjectSize,
    /// Delta instructions, depth, expansion, or base cache exceeded limits.
    #[error("incremental push pack delta exceeds configured limits")]
    DeltaLimit,
    /// Delta dependency is cyclic, missing, forward OFS, or otherwise unresolved.
    #[error("incremental push pack delta dependency is unresolved")]
    UnresolvedDependency,
    /// Existing-base provider failed or returned content with a different canonical ID.
    #[error("incremental push pack authorized base is invalid")]
    InvalidExternalBase,
    /// Two entries resolved to the same canonical object ID.
    #[error("incremental push pack contains duplicate object IDs")]
    DuplicateObject,
    /// Canonical structural object parsing failed.
    #[error("incremental push pack structural object is invalid")]
    InvalidStructuralObject,
    /// A bounded allocation failed.
    #[error("incremental push pack bounded allocation failed")]
    Allocation,
}

#[derive(Clone, Copy, Debug)]
enum Dependency {
    Direct(ObjectKind),
    Ofs { base_index: usize, base_offset: u64 },
    Ref { base_oid: ObjectId },
}

#[derive(Clone, Debug)]
struct ParsedEntry {
    ordinal: u32,
    entry_offset: u64,
    header: PushPackByteSpan,
    base_header: PushPackByteSpan,
    compressed: PushPackByteSpan,
    declared_size: u64,
    crc32: u32,
    dependency: Dependency,
    resolved: Option<ResolvedEntry>,
}

#[derive(Clone, Copy, Debug)]
struct ResolvedEntry {
    oid: ObjectId,
    kind: ObjectKind,
    size: u64,
    depth: u32,
    external: bool,
}

struct InflateResult {
    compressed_bytes: u64,
    inflated_bytes: u64,
    crc32: u32,
    collected: Option<Vec<u8>>,
    oid: Option<ObjectId>,
}

/// Incrementally validate/index a finished private quarantine and persist its attestation.
///
/// Blob payloads are hashed and released during direct validation. Delta results are retained only
/// inside the configured base-cache bound until dependency resolution completes. No recursion,
/// promotion, object-store publication, or hidden time is used.
///
/// # Errors
///
/// Returns typed quarantine, control/work, PACK/zlib/size, dependency/delta, structural parsing,
/// provider, duplicate, or allocation failures before visibility.
pub async fn validate_quarantined_pack<Q, B, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    bases: &mut B,
    control: &mut C,
    work: &mut W,
    options: PushPackValidationOptions,
) -> Result<ValidatedPushPack, PushPackValidationError>
where
    Q: PackQuarantine,
    B: AuthorizedPushBaseProvider,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    let options = options.validate()?;
    let snapshot = quarantine.snapshot();
    if snapshot.state != QuarantineState::Validated || snapshot.generation != fence.generation() {
        return Err(PushPackValidationError::InvalidQuarantine);
    }
    let manifest = snapshot
        .manifest
        .clone()
        .ok_or(PushPackValidationError::InvalidQuarantine)?;
    let pack_checksum = manifest
        .pack_checksum
        .ok_or(PushPackValidationError::InvalidPack)?;
    validate_manifest_shape(&manifest, pack_checksum)?;
    let mut checkpoint = ValidationCheckpoint::new(control, work, options);
    checkpoint.charge(1)?;
    let header = read_exact(quarantine, fence, 0, 12, &mut checkpoint).await?;
    if header.get(..4) != Some(b"PACK".as_slice()) {
        return Err(PushPackValidationError::InvalidPack);
    }
    let version = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
    let count = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
    if !matches!(version, 2 | 3)
        || count != manifest.object_count
        || usize::try_from(count).map_err(|_| PushPackValidationError::IndexLimit)?
            > options.limits.max_objects
    {
        return Err(PushPackValidationError::InvalidPack);
    }
    validate_index_budget(count, options.max_index_bytes)?;
    let trailer_start = manifest
        .pack_bytes
        .checked_sub(
            u64::try_from(manifest.hash_algo.len())
                .map_err(|_| PushPackValidationError::InvalidPack)?,
        )
        .ok_or(PushPackValidationError::InvalidPack)?;
    let trailer = read_exact(
        quarantine,
        fence,
        trailer_start,
        manifest.hash_algo.len(),
        &mut checkpoint,
    )
    .await?;
    if trailer.as_slice() != pack_checksum.as_bytes() {
        return Err(PushPackValidationError::InvalidPack);
    }

    let count_usize = usize::try_from(count).map_err(|_| PushPackValidationError::IndexLimit)?;
    let mut entries = Vec::new();
    entries
        .try_reserve(count_usize)
        .map_err(|_| PushPackValidationError::Allocation)?;
    let mut offsets = HashMap::new();
    offsets
        .try_reserve(count_usize)
        .map_err(|_| PushPackValidationError::Allocation)?;
    let mut oid_to_index = HashMap::new();
    oid_to_index
        .try_reserve(count_usize)
        .map_err(|_| PushPackValidationError::Allocation)?;
    let mut structural_objects = Vec::new();
    let mut structural_memory_bytes = 0_u64;
    let mut summary = PushValidationSummary::default();
    let mut cursor = 12_u64;

    for ordinal in 0..count {
        checkpoint.charge(1)?;
        let entry_offset = cursor;
        let (type_code, declared_size, header_bytes) = read_entry_header(
            quarantine,
            fence,
            &mut cursor,
            trailer_start,
            &mut checkpoint,
        )
        .await?;
        let header_span = PushPackByteSpan {
            offset: entry_offset,
            length: header_bytes,
        };
        let (dependency, base_span) = match type_code {
            1..=4 => (
                Dependency::Direct(object_kind(type_code)?),
                PushPackByteSpan {
                    offset: cursor,
                    length: 0,
                },
            ),
            6 => {
                let base_start = cursor;
                let distance = read_ofs_distance(
                    quarantine,
                    fence,
                    &mut cursor,
                    trailer_start,
                    &mut checkpoint,
                )
                .await?;
                let base_offset = entry_offset
                    .checked_sub(distance)
                    .ok_or(PushPackValidationError::InvalidHeader)?;
                let base_index = offsets
                    .get(&base_offset)
                    .copied()
                    .ok_or(PushPackValidationError::UnresolvedDependency)?;
                summary.ofs_deltas = checked_add(summary.ofs_deltas, 1)?;
                (
                    Dependency::Ofs {
                        base_index,
                        base_offset,
                    },
                    PushPackByteSpan {
                        offset: base_start,
                        length: cursor - base_start,
                    },
                )
            }
            7 => {
                let base_start = cursor;
                let base_bytes = read_exact(
                    quarantine,
                    fence,
                    cursor,
                    manifest.hash_algo.len(),
                    &mut checkpoint,
                )
                .await?;
                cursor = cursor
                    .checked_add(
                        u64::try_from(base_bytes.len())
                            .map_err(|_| PushPackValidationError::InvalidHeader)?,
                    )
                    .ok_or(PushPackValidationError::InvalidHeader)?;
                let base_oid = ObjectId::from_bytes(&base_bytes)
                    .map_err(|_| PushPackValidationError::InvalidHeader)?;
                if base_oid.is_zero() {
                    return Err(PushPackValidationError::InvalidHeader);
                }
                summary.ref_deltas = checked_add(summary.ref_deltas, 1)?;
                (
                    Dependency::Ref { base_oid },
                    PushPackByteSpan {
                        offset: base_start,
                        length: cursor - base_start,
                    },
                )
            }
            _ => return Err(PushPackValidationError::InvalidHeader),
        };
        let compressed_offset = cursor;
        let collect = match dependency {
            Dependency::Direct(ObjectKind::Blob) => false,
            Dependency::Direct(_) | Dependency::Ofs { .. } | Dependency::Ref { .. } => true,
        };
        let maximum = match dependency {
            Dependency::Direct(ObjectKind::Blob) => options.limits.max_object_bytes,
            Dependency::Direct(_) => options
                .limits
                .max_object_bytes
                .min(options.limits.max_metadata_memory_bytes),
            Dependency::Ofs { .. } | Dependency::Ref { .. } => {
                options.limits.max_delta_instruction_bytes
            }
        };
        if declared_size > maximum {
            return Err(PushPackValidationError::ObjectSize);
        }
        let aggregate_inflated = summary
            .inflated_bytes
            .checked_add(declared_size)
            .filter(|bytes| *bytes <= options.limits.max_inflated_bytes)
            .ok_or(PushPackValidationError::ObjectSize)?;
        let prefix = read_exact(
            quarantine,
            fence,
            entry_offset,
            usize::try_from(cursor - entry_offset)
                .map_err(|_| PushPackValidationError::InvalidHeader)?,
            &mut checkpoint,
        )
        .await?;
        let inflated = inflate_one(
            quarantine,
            fence,
            compressed_offset,
            trailer_start,
            declared_size,
            collect,
            match dependency {
                Dependency::Direct(kind) => Some(kind),
                _ => None,
            },
            &prefix,
            manifest.hash_algo,
            &mut checkpoint,
        )
        .await?;
        cursor = compressed_offset
            .checked_add(inflated.compressed_bytes)
            .ok_or(PushPackValidationError::InvalidPack)?;
        if inflated.inflated_bytes != declared_size {
            return Err(PushPackValidationError::ObjectSize);
        }
        summary.inflated_bytes = aggregate_inflated;
        let resolved = if let Dependency::Direct(kind) = dependency {
            let oid = inflated.oid.ok_or(PushPackValidationError::InvalidPack)?;
            if oid_to_index.insert(oid, entries.len()).is_some() {
                return Err(PushPackValidationError::DuplicateObject);
            }
            if let Some(data) = inflated.collected.as_deref() {
                retain_structural(
                    oid,
                    kind,
                    data,
                    manifest.hash_algo,
                    &mut structural_objects,
                    &mut structural_memory_bytes,
                    options.limits.max_structural_memory_bytes,
                    &mut summary,
                )?;
            } else {
                summary.blobs = checked_add(summary.blobs, 1)?;
            }
            summary.direct_objects = checked_add(summary.direct_objects, 1)?;
            Some(ResolvedEntry {
                oid,
                kind,
                size: declared_size,
                depth: 0,
                external: false,
            })
        } else {
            None
        };
        let index = entries.len();
        if offsets.insert(entry_offset, index).is_some() {
            return Err(PushPackValidationError::InvalidPack);
        }
        entries.push(ParsedEntry {
            ordinal,
            entry_offset,
            header: header_span,
            base_header: base_span,
            compressed: PushPackByteSpan {
                offset: compressed_offset,
                length: inflated.compressed_bytes,
            },
            declared_size,
            crc32: inflated.crc32,
            dependency,
            resolved,
        });
    }
    if cursor != trailer_start {
        return Err(PushPackValidationError::InvalidPack);
    }

    let mut cache: Vec<Option<Arc<[u8]>>> = Vec::new();
    cache
        .try_reserve(count_usize)
        .map_err(|_| PushPackValidationError::Allocation)?;
    cache.resize_with(count_usize, || None);
    let mut cache_bytes = 0_u64;
    let mut missing_external = HashSet::new();
    missing_external
        .try_reserve(count_usize)
        .map_err(|_| PushPackValidationError::Allocation)?;
    let mut unresolved = entries
        .iter()
        .filter(|entry| entry.resolved.is_none())
        .count();
    let mut used_external = false;
    while unresolved > 0 {
        let mut progress = 0_usize;
        for index in 0..entries.len() {
            if entries[index].resolved.is_some() {
                continue;
            }
            checkpoint.charge(1)?;
            let dependency = entries[index].dependency;
            let base = match dependency {
                Dependency::Direct(_) => continue,
                Dependency::Ofs { base_index, .. } => materialize_base(
                    quarantine,
                    fence,
                    base_index,
                    &entries,
                    &mut cache,
                    &mut cache_bytes,
                    manifest.hash_algo,
                    &mut checkpoint,
                    options,
                )
                .await?
                .map(|(kind, data, depth, oid)| (kind, data, depth, oid, false)),
                Dependency::Ref { base_oid } => {
                    if let Some(base_index) = oid_to_index.get(&base_oid).copied() {
                        materialize_base(
                            quarantine,
                            fence,
                            base_index,
                            &entries,
                            &mut cache,
                            &mut cache_bytes,
                            manifest.hash_algo,
                            &mut checkpoint,
                            options,
                        )
                        .await?
                        .map(|(kind, data, depth, oid)| (kind, data, depth, oid, false))
                    } else if missing_external.contains(&base_oid) {
                        None
                    } else {
                        checkpoint.external_operation()?;
                        match bases
                            .read_base(
                                &snapshot.tenant,
                                &snapshot.repository,
                                base_oid,
                                options.limits.max_object_bytes,
                            )
                            .await
                            .map_err(|_| PushPackValidationError::InvalidExternalBase)?
                        {
                            Some(base) => {
                                checkpoint.charge(1)?;
                                let size = u64::try_from(base.data.len())
                                    .map_err(|_| PushPackValidationError::InvalidExternalBase)?;
                                if size > options.limits.max_object_bytes
                                    || object_id(base.kind, &base.data, manifest.hash_algo)?
                                        != base_oid
                                {
                                    return Err(PushPackValidationError::InvalidExternalBase);
                                }
                                Some((base.kind, Arc::<[u8]>::from(base.data), 0, base_oid, true))
                            }
                            None => {
                                checkpoint.charge(1)?;
                                missing_external.insert(base_oid);
                                None
                            }
                        }
                    }
                }
            };
            let Some((base_kind, base_data, base_depth, base_oid, external)) = base else {
                continue;
            };
            let instructions = inflate_entry_data(
                quarantine,
                fence,
                &entries[index],
                trailer_start,
                manifest.hash_algo,
                &mut checkpoint,
            )
            .await?;
            let (source_size, result_size) = delta_sizes(&instructions)?;
            let base_size =
                u64::try_from(base_data.len()).map_err(|_| PushPackValidationError::DeltaLimit)?;
            if source_size != base_size || result_size > options.limits.max_object_bytes {
                return Err(PushPackValidationError::DeltaLimit);
            }
            if base_kind != ObjectKind::Blob
                && result_size > options.limits.max_metadata_memory_bytes
            {
                return Err(PushPackValidationError::DeltaLimit);
            }
            let instruction_size = u64::try_from(instructions.len())
                .map_err(|_| PushPackValidationError::DeltaLimit)?;
            base_size
                .checked_add(instruction_size)
                .and_then(|bytes| bytes.checked_add(result_size))
                .filter(|bytes| *bytes <= options.limits.bytes_in_flight)
                .ok_or(PushPackValidationError::DeltaLimit)?;
            let next_cache_bytes = cache_bytes
                .checked_add(result_size)
                .filter(|bytes| *bytes <= options.limits.max_delta_base_memory_bytes)
                .ok_or(PushPackValidationError::DeltaLimit)?;
            let expansion = result_size
                .checked_add(instruction_size.saturating_sub(1))
                .and_then(|value| value.checked_div(instruction_size.max(1)))
                .ok_or(PushPackValidationError::DeltaLimit)?;
            if expansion > options.limits.max_delta_expansion_ratio {
                return Err(PushPackValidationError::DeltaLimit);
            }
            let depth = base_depth
                .checked_add(1)
                .filter(|depth| *depth <= options.limits.max_delta_depth)
                .ok_or(PushPackValidationError::DeltaLimit)?;
            checkpoint.charge(result_size.saturating_add(instruction_size))?;
            let result = apply_delta(base_data.as_ref(), &instructions)
                .map_err(|_| PushPackValidationError::DeltaLimit)?;
            if u64::try_from(result.len()).ok() != Some(result_size) {
                return Err(PushPackValidationError::DeltaLimit);
            }
            let oid = object_id(base_kind, &result, manifest.hash_algo)?;
            if oid_to_index.insert(oid, index).is_some() {
                return Err(PushPackValidationError::DuplicateObject);
            }
            retain_structural(
                oid,
                base_kind,
                &result,
                manifest.hash_algo,
                &mut structural_objects,
                &mut structural_memory_bytes,
                options.limits.max_structural_memory_bytes,
                &mut summary,
            )?;
            if base_kind == ObjectKind::Blob {
                summary.blobs = checked_add(summary.blobs, 1)?;
            }
            if external {
                summary.external_bases = checked_add(summary.external_bases, 1)?;
                used_external = true;
            }
            summary.maximum_delta_depth = summary.maximum_delta_depth.max(depth);
            summary.maximum_delta_expansion_ratio =
                summary.maximum_delta_expansion_ratio.max(expansion);
            let result_bytes =
                u64::try_from(result.len()).map_err(|_| PushPackValidationError::DeltaLimit)?;
            if result_bytes != result_size {
                return Err(PushPackValidationError::DeltaLimit);
            }
            cache_bytes = next_cache_bytes;
            cache[index] = Some(Arc::<[u8]>::from(result));
            entries[index].resolved = Some(ResolvedEntry {
                oid,
                kind: base_kind,
                size: result_size,
                depth,
                external,
            });
            let _ = base_oid;
            progress += 1;
            unresolved -= 1;
        }
        if progress == 0 {
            return Err(PushPackValidationError::UnresolvedDependency);
        }
    }

    let mut index: Vec<PushPackIndexRow> = Vec::new();
    index
        .try_reserve(entries.len())
        .map_err(|_| PushPackValidationError::Allocation)?;
    for entry in entries {
        let resolved = entry
            .resolved
            .ok_or(PushPackValidationError::UnresolvedDependency)?;
        let representation = match entry.dependency {
            Dependency::Direct(_) => PushPackEntryKind::Direct,
            Dependency::Ofs {
                base_offset,
                base_index,
            } => {
                let base = index
                    .get(base_index)
                    .ok_or(PushPackValidationError::UnresolvedDependency)?;
                PushPackEntryKind::OfsDelta {
                    base_offset,
                    base_oid: base.oid,
                }
            }
            Dependency::Ref { base_oid } => PushPackEntryKind::RefDelta {
                base_oid,
                external: resolved.external,
            },
        };
        let entry_length = entry
            .compressed
            .offset
            .checked_add(entry.compressed.length)
            .and_then(|end| end.checked_sub(entry.entry_offset))
            .ok_or(PushPackValidationError::InvalidPack)?;
        index.push(PushPackIndexRow {
            ordinal: entry.ordinal,
            oid: resolved.oid,
            kind: resolved.kind,
            entry: PushPackByteSpan {
                offset: entry.entry_offset,
                length: entry_length,
            },
            header: entry.header,
            base_header: entry.base_header,
            compressed: entry.compressed,
            declared_size: entry.declared_size,
            resolved_size: resolved.size,
            delta_depth: resolved.depth,
            crc32: entry.crc32,
            representation,
        });
    }
    summary.objects =
        u64::try_from(index.len()).map_err(|_| PushPackValidationError::IndexLimit)?;
    let index_checksum = index_checksum(&index, pack_checksum, manifest.hash_algo)?;
    let attestation =
        QuarantineIndexAttestation::validated(pack_checksum, index_checksum, count, !used_external);
    checkpoint.charge(1)?;
    let fence = quarantine
        .attest_index(fence, attestation)
        .await
        .map_err(|_| PushPackValidationError::Quarantine)?;
    Ok(ValidatedPushPack {
        index,
        structural_objects,
        summary,
        attestation,
        fence,
    })
}

struct ValidationCheckpoint<'a, C, W> {
    control: &'a mut C,
    work: &'a mut W,
    options: PushPackValidationOptions,
    last_observed_at: Option<OffsetDateTime>,
    range_operations: u64,
    external_operations: u64,
}

impl<'a, C, W> ValidationCheckpoint<'a, C, W>
where
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    fn new(control: &'a mut C, work: &'a mut W, options: PushPackValidationOptions) -> Self {
        Self {
            control,
            work,
            options,
            last_observed_at: None,
            range_operations: 0,
            external_operations: 0,
        }
    }

    fn charge(&mut self, units: u64) -> Result<(), PushPackValidationError> {
        if !self.work.charge(units.max(1)) {
            return Err(PushPackValidationError::WorkBudget);
        }
        let observation = self.control.observe();
        if observation.cancelled {
            return Err(PushPackValidationError::Cancelled);
        }
        if observation.observed_at < self.options.started_at
            || observation.observed_at >= self.options.deadline
            || self
                .last_observed_at
                .is_some_and(|previous| observation.observed_at < previous)
        {
            return Err(PushPackValidationError::Deadline);
        }
        self.last_observed_at = Some(observation.observed_at);
        Ok(())
    }

    fn range_operation(&mut self) -> Result<(), PushPackValidationError> {
        self.range_operations = self
            .range_operations
            .checked_add(1)
            .filter(|count| *count <= self.options.limits.max_range_operations)
            .ok_or(PushPackValidationError::WorkBudget)?;
        Ok(())
    }

    fn external_operation(&mut self) -> Result<(), PushPackValidationError> {
        self.external_operations = self
            .external_operations
            .checked_add(1)
            .filter(|count| *count <= self.options.limits.max_external_operations)
            .ok_or(PushPackValidationError::WorkBudget)?;
        Ok(())
    }
}

async fn read_exact<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    offset: u64,
    length: usize,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
) -> Result<Vec<u8>, PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    if length > checkpoint.options.range_chunk_bytes {
        return Err(PushPackValidationError::InvalidLimits);
    }
    checkpoint.range_operation()?;
    checkpoint.charge(u64::try_from(length).map_err(|_| PushPackValidationError::WorkBudget)?)?;
    let bytes = quarantine
        .read_range(fence, offset, length)
        .await
        .map_err(|_| PushPackValidationError::Quarantine)?;
    checkpoint.charge(1)?;
    if bytes.len() != length {
        return Err(PushPackValidationError::Quarantine);
    }
    Ok(bytes)
}

async fn read_entry_header<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    cursor: &mut u64,
    end: u64,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
) -> Result<(u8, u64, u64), PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    let start = *cursor;
    let first = read_byte(quarantine, fence, cursor, end, checkpoint).await?;
    let type_code = (first >> 4) & 7;
    let mut size = u64::from(first & 15);
    let mut shift = 4_u32;
    let mut byte = first;
    let mut continued = false;
    while byte & 0x80 != 0 {
        continued = true;
        if usize::try_from(*cursor - start)
            .ok()
            .is_none_or(|length| length >= MAX_ENTRY_HEADER_BYTES)
        {
            return Err(PushPackValidationError::InvalidHeader);
        }
        byte = read_byte(quarantine, fence, cursor, end, checkpoint).await?;
        let payload = u64::from(byte & 0x7f);
        if shift >= u64::BITS || payload > (u64::MAX >> shift) {
            return Err(PushPackValidationError::InvalidHeader);
        }
        let component = payload << shift;
        size = size
            .checked_add(component)
            .ok_or(PushPackValidationError::InvalidHeader)?;
        shift = shift
            .checked_add(7)
            .ok_or(PushPackValidationError::InvalidHeader)?;
    }
    if continued && byte & 0x7f == 0 {
        return Err(PushPackValidationError::InvalidHeader);
    }
    Ok((type_code, size, *cursor - start))
}

async fn read_ofs_distance<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    cursor: &mut u64,
    end: u64,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
) -> Result<u64, PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    let start = *cursor;
    let mut byte = read_byte(quarantine, fence, cursor, end, checkpoint).await?;
    let mut value = u64::from(byte & 0x7f);
    if byte & 0x80 != 0 && value == 0 {
        return Err(PushPackValidationError::InvalidHeader);
    }
    while byte & 0x80 != 0 {
        if usize::try_from(*cursor - start)
            .ok()
            .is_none_or(|length| length >= MAX_ENTRY_HEADER_BYTES)
        {
            return Err(PushPackValidationError::InvalidHeader);
        }
        byte = read_byte(quarantine, fence, cursor, end, checkpoint).await?;
        value = value
            .checked_add(1)
            .filter(|current| *current <= (u64::MAX >> 7))
            .map(|current| current << 7)
            .and_then(|current| current.checked_add(u64::from(byte & 0x7f)))
            .ok_or(PushPackValidationError::InvalidHeader)?;
    }
    if value == 0 {
        return Err(PushPackValidationError::InvalidHeader);
    }
    Ok(value)
}

async fn read_byte<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    cursor: &mut u64,
    end: u64,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
) -> Result<u8, PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    if *cursor >= end {
        return Err(PushPackValidationError::InvalidHeader);
    }
    let bytes = read_exact(quarantine, fence, *cursor, 1, checkpoint).await?;
    *cursor = cursor
        .checked_add(1)
        .ok_or(PushPackValidationError::InvalidHeader)?;
    bytes
        .first()
        .copied()
        .ok_or(PushPackValidationError::InvalidHeader)
}

async fn inflate_one<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    offset: u64,
    end: u64,
    declared_size: u64,
    collect: bool,
    direct_kind: Option<ObjectKind>,
    entry_prefix: &[u8],
    hash_algo: HashAlgo,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
) -> Result<InflateResult, PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    let mut decoder = Decompress::new(true);
    let mut compressed_offset = offset;
    let mut output = [0_u8; INFLATE_BUFFER_BYTES];
    let mut collected = if collect { Some(Vec::new()) } else { None };
    let mut crc = crc32fast::Hasher::new();
    crc.update(entry_prefix);
    let mut object_hasher =
        direct_kind.map(|kind| ObjectHasher::new(kind, declared_size, hash_algo));
    loop {
        if compressed_offset >= end {
            return Err(PushPackValidationError::InvalidCompressedStream);
        }
        let available = end - compressed_offset;
        let length = usize::try_from(
            available.min(
                u64::try_from(checkpoint.options.range_chunk_bytes)
                    .map_err(|_| PushPackValidationError::InvalidLimits)?,
            ),
        )
        .map_err(|_| PushPackValidationError::InvalidLimits)?;
        let input = read_exact(quarantine, fence, compressed_offset, length, checkpoint).await?;
        let mut input_cursor = 0_usize;
        while input_cursor < input.len() {
            let before_in = decoder.total_in();
            let before_out = decoder.total_out();
            let status = decoder
                .decompress(&input[input_cursor..], &mut output, FlushDecompress::None)
                .map_err(|_| PushPackValidationError::InvalidCompressedStream)?;
            let consumed = usize::try_from(
                decoder
                    .total_in()
                    .checked_sub(before_in)
                    .ok_or(PushPackValidationError::InvalidCompressedStream)?,
            )
            .map_err(|_| PushPackValidationError::InvalidCompressedStream)?;
            let produced = usize::try_from(
                decoder
                    .total_out()
                    .checked_sub(before_out)
                    .ok_or(PushPackValidationError::ObjectSize)?,
            )
            .map_err(|_| PushPackValidationError::ObjectSize)?;
            if consumed == 0 && produced == 0 {
                return Err(PushPackValidationError::InvalidCompressedStream);
            }
            crc.update(&input[input_cursor..input_cursor + consumed]);
            input_cursor += consumed;
            compressed_offset = compressed_offset
                .checked_add(
                    u64::try_from(consumed)
                        .map_err(|_| PushPackValidationError::InvalidCompressedStream)?,
                )
                .ok_or(PushPackValidationError::InvalidCompressedStream)?;
            checkpoint.charge(
                u64::try_from(produced).map_err(|_| PushPackValidationError::WorkBudget)?,
            )?;
            if decoder.total_out() > declared_size {
                return Err(PushPackValidationError::ObjectSize);
            }
            if let Some(hasher) = &mut object_hasher {
                hasher.update(&output[..produced]);
            }
            if let Some(data) = &mut collected {
                let total = u64::try_from(data.len())
                    .ok()
                    .and_then(|current| current.checked_add(u64::try_from(produced).ok()?))
                    .filter(|total| *total <= declared_size)
                    .ok_or(PushPackValidationError::ObjectSize)?;
                let _ = total;
                data.try_reserve_exact(produced)
                    .map_err(|_| PushPackValidationError::Allocation)?;
                data.extend_from_slice(&output[..produced]);
            }
            if status == Status::StreamEnd {
                if decoder.total_out() != declared_size {
                    return Err(PushPackValidationError::ObjectSize);
                }
                return Ok(InflateResult {
                    compressed_bytes: decoder.total_in(),
                    inflated_bytes: decoder.total_out(),
                    crc32: crc.finalize(),
                    collected,
                    oid: object_hasher.map(ObjectHasher::finish).transpose()?,
                });
            }
        }
    }
}

async fn inflate_entry_data<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    entry: &ParsedEntry,
    trailer_start: u64,
    hash_algo: HashAlgo,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
) -> Result<Vec<u8>, PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    let prefix_length = entry
        .compressed
        .offset
        .checked_sub(entry.entry_offset)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or(PushPackValidationError::InvalidHeader)?;
    let prefix = read_exact(
        quarantine,
        fence,
        entry.entry_offset,
        prefix_length,
        checkpoint,
    )
    .await?;
    inflate_one(
        quarantine,
        fence,
        entry.compressed.offset,
        trailer_start,
        entry.declared_size,
        true,
        None,
        &prefix,
        hash_algo,
        checkpoint,
    )
    .await?
    .collected
    .ok_or(PushPackValidationError::InvalidCompressedStream)
}

async fn materialize_base<Q, C, W>(
    quarantine: &mut Q,
    fence: QuarantineFence,
    index: usize,
    entries: &[ParsedEntry],
    cache: &mut [Option<Arc<[u8]>>],
    cache_bytes: &mut u64,
    hash_algo: HashAlgo,
    checkpoint: &mut ValidationCheckpoint<'_, C, W>,
    options: PushPackValidationOptions,
) -> Result<Option<(ObjectKind, Arc<[u8]>, u32, ObjectId)>, PushPackValidationError>
where
    Q: PackQuarantine,
    C: PushPackValidationControl,
    W: PushPackValidationWork,
{
    let entry = entries
        .get(index)
        .ok_or(PushPackValidationError::UnresolvedDependency)?;
    let Some(resolved) = entry.resolved else {
        return Ok(None);
    };
    if let Some(data) = cache.get(index).and_then(|value| value.as_ref()) {
        return Ok(Some((
            resolved.kind,
            data.clone(),
            resolved.depth,
            resolved.oid,
        )));
    }
    if !matches!(entry.dependency, Dependency::Direct(_)) {
        return Ok(None);
    }
    let next_cache_bytes = cache_bytes
        .checked_add(entry.declared_size)
        .filter(|total| *total <= options.limits.max_delta_base_memory_bytes)
        .ok_or(PushPackValidationError::DeltaLimit)?;
    let prefix_length = entry
        .compressed
        .offset
        .checked_sub(entry.entry_offset)
        .and_then(|length| usize::try_from(length).ok())
        .ok_or(PushPackValidationError::InvalidHeader)?;
    let prefix = read_exact(
        quarantine,
        fence,
        entry.entry_offset,
        prefix_length,
        checkpoint,
    )
    .await?;
    let trailer_start = quarantine
        .snapshot()
        .manifest
        .and_then(|manifest| {
            manifest
                .pack_bytes
                .checked_sub(u64::try_from(hash_algo.len()).ok()?)
        })
        .ok_or(PushPackValidationError::InvalidQuarantine)?;
    let inflated = inflate_one(
        quarantine,
        fence,
        entry.compressed.offset,
        trailer_start,
        entry.declared_size,
        true,
        Some(resolved.kind),
        &prefix,
        hash_algo,
        checkpoint,
    )
    .await?;
    let data = inflated
        .collected
        .ok_or(PushPackValidationError::InvalidCompressedStream)?;
    if inflated.oid != Some(resolved.oid) {
        return Err(PushPackValidationError::InvalidPack);
    }
    let bytes = u64::try_from(data.len()).map_err(|_| PushPackValidationError::DeltaLimit)?;
    if bytes != entry.declared_size {
        return Err(PushPackValidationError::DeltaLimit);
    }
    *cache_bytes = next_cache_bytes;
    let data = Arc::<[u8]>::from(data);
    let slot = cache
        .get_mut(index)
        .ok_or(PushPackValidationError::UnresolvedDependency)?;
    *slot = Some(Arc::clone(&data));
    Ok(Some((resolved.kind, data, resolved.depth, resolved.oid)))
}

fn retain_structural(
    oid: ObjectId,
    kind: ObjectKind,
    data: &[u8],
    hash_algo: HashAlgo,
    output: &mut Vec<PushStructuralObject>,
    retained_bytes: &mut u64,
    maximum_bytes: u64,
    summary: &mut PushValidationSummary,
) -> Result<(), PushPackValidationError> {
    if kind == ObjectKind::Blob {
        return Ok(());
    }
    let estimated_bytes = u64::try_from(data.len())
        .ok()
        .and_then(|bytes| bytes.checked_mul(STRUCTURAL_MEMORY_MULTIPLIER))
        .and_then(|bytes| bytes.checked_add(STRUCTURAL_OBJECT_OVERHEAD))
        .ok_or(PushPackValidationError::ObjectSize)?;
    let new_total = retained_bytes
        .checked_add(estimated_bytes)
        .filter(|bytes| *bytes <= maximum_bytes)
        .ok_or(PushPackValidationError::ObjectSize)?;
    let metadata = match kind {
        ObjectKind::Commit => {
            let commit =
                parse_commit(data).map_err(|_| PushPackValidationError::InvalidStructuralObject)?;
            if !valid_reference(commit.tree, hash_algo)
                || commit
                    .parents
                    .iter()
                    .any(|oid| !valid_reference(*oid, hash_algo))
            {
                return Err(PushPackValidationError::InvalidStructuralObject);
            }
            summary.commits = checked_add(summary.commits, 1)?;
            Some(PushStructuralMetadata::Commit(commit))
        }
        ObjectKind::Tree => {
            let tree = parse_tree_with_oid_len(data, hash_algo.len())
                .map_err(|_| PushPackValidationError::InvalidStructuralObject)?;
            if tree
                .iter()
                .any(|entry| !valid_reference(entry.oid, hash_algo))
            {
                return Err(PushPackValidationError::InvalidStructuralObject);
            }
            summary.trees = checked_add(summary.trees, 1)?;
            Some(PushStructuralMetadata::Tree(tree))
        }
        ObjectKind::Tag => {
            let tag =
                parse_tag(data).map_err(|_| PushPackValidationError::InvalidStructuralObject)?;
            if !valid_reference(tag.object, hash_algo)
                || ObjectKind::from_tag_type_field(tag.object_type.as_bytes()).is_none()
            {
                return Err(PushPackValidationError::InvalidStructuralObject);
            }
            summary.tags = checked_add(summary.tags, 1)?;
            Some(PushStructuralMetadata::Tag(tag))
        }
        ObjectKind::Blob => None,
    };
    if let Some(metadata) = metadata {
        output
            .try_reserve(1)
            .map_err(|_| PushPackValidationError::Allocation)?;
        output.push(PushStructuralObject { oid, metadata });
        *retained_bytes = new_total;
    }
    Ok(())
}

fn valid_reference(oid: ObjectId, hash_algo: HashAlgo) -> bool {
    oid.algo() == hash_algo && !oid.is_zero()
}

#[derive(Clone)]
enum ObjectHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl ObjectHasher {
    fn new(kind: ObjectKind, size: u64, hash_algo: HashAlgo) -> Self {
        let header = format!("{} {size}\0", kind.as_str());
        match hash_algo {
            HashAlgo::Sha1 => {
                let mut hasher = Sha1::new();
                Sha1Digest::update(&mut hasher, header.as_bytes());
                Self::Sha1(hasher)
            }
            HashAlgo::Sha256 => {
                let mut hasher = Sha256::new();
                Sha2Digest::update(&mut hasher, header.as_bytes());
                Self::Sha256(hasher)
            }
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hasher) => Sha1Digest::update(hasher, bytes),
            Self::Sha256(hasher) => Sha2Digest::update(hasher, bytes),
        }
    }

    fn finish(self) -> Result<ObjectId, PushPackValidationError> {
        let digest = match self {
            Self::Sha1(hasher) => hasher.finalize().to_vec(),
            Self::Sha256(hasher) => hasher.finalize().to_vec(),
        };
        let oid =
            ObjectId::from_bytes(&digest).map_err(|_| PushPackValidationError::InvalidPack)?;
        if oid.is_zero() {
            return Err(PushPackValidationError::InvalidPack);
        }
        Ok(oid)
    }
}

fn object_id(
    kind: ObjectKind,
    data: &[u8],
    hash_algo: HashAlgo,
) -> Result<ObjectId, PushPackValidationError> {
    let mut hasher = ObjectHasher::new(
        kind,
        u64::try_from(data.len()).map_err(|_| PushPackValidationError::ObjectSize)?,
        hash_algo,
    );
    hasher.update(data);
    let oid = hasher.finish()?;
    if oid.is_zero() {
        return Err(PushPackValidationError::InvalidPack);
    }
    Ok(oid)
}

fn delta_sizes(delta: &[u8]) -> Result<(u64, u64), PushPackValidationError> {
    let mut cursor = 0_usize;
    let source = read_delta_varint(delta, &mut cursor)?;
    let result = read_delta_varint(delta, &mut cursor)?;
    Ok((source, result))
}

fn read_delta_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, PushPackValidationError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let mut continued = false;
    loop {
        let byte = bytes
            .get(*cursor)
            .copied()
            .ok_or(PushPackValidationError::DeltaLimit)?;
        *cursor = cursor
            .checked_add(1)
            .ok_or(PushPackValidationError::DeltaLimit)?;
        let payload = u64::from(byte & 0x7f);
        if shift >= u64::BITS || payload > (u64::MAX >> shift) {
            return Err(PushPackValidationError::DeltaLimit);
        }
        value = value
            .checked_add(payload << shift)
            .ok_or(PushPackValidationError::DeltaLimit)?;
        if byte & 0x80 == 0 {
            if continued && payload == 0 {
                return Err(PushPackValidationError::DeltaLimit);
            }
            return Ok(value);
        }
        continued = true;
        shift = shift
            .checked_add(7)
            .filter(|shift| *shift < 64)
            .ok_or(PushPackValidationError::DeltaLimit)?;
    }
}

fn index_checksum(
    rows: &[PushPackIndexRow],
    pack_checksum: ObjectId,
    hash_algo: HashAlgo,
) -> Result<ObjectId, PushPackValidationError> {
    let row_bytes = u64::try_from(hash_algo.len())
        .ok()
        .and_then(|oid_bytes| oid_bytes.checked_add(28))
        .ok_or(PushPackValidationError::IndexLimit)?;
    let payload_bytes = u64::try_from(hash_algo.len())
        .ok()
        .and_then(|checksum_bytes| {
            u64::try_from(rows.len())
                .ok()?
                .checked_mul(row_bytes)?
                .checked_add(checksum_bytes)
        })
        .ok_or(PushPackValidationError::IndexLimit)?;
    let mut hasher = ObjectHasher::new(ObjectKind::Blob, payload_bytes, hash_algo);
    hasher.update(pack_checksum.as_bytes());
    for row in rows {
        hasher.update(row.oid.as_bytes());
        hasher.update(&row.entry.offset.to_be_bytes());
        hasher.update(&row.entry.length.to_be_bytes());
        hasher.update(&row.resolved_size.to_be_bytes());
        hasher.update(&row.crc32.to_be_bytes());
    }
    let oid = hasher.finish()?;
    if oid.is_zero() {
        return Err(PushPackValidationError::InvalidPack);
    }
    Ok(oid)
}

fn validate_manifest_shape(
    manifest: &QuarantineManifest,
    checksum: ObjectId,
) -> Result<(), PushPackValidationError> {
    let minimum = 12_u64
        .checked_add(
            u64::try_from(manifest.hash_algo.len())
                .map_err(|_| PushPackValidationError::InvalidPack)?,
        )
        .ok_or(PushPackValidationError::InvalidPack)?;
    if manifest.pack_bytes < minimum
        || checksum.algo() != manifest.hash_algo
        || manifest.self_contained
        || manifest.index_checksum.is_some()
    {
        return Err(PushPackValidationError::InvalidQuarantine);
    }
    Ok(())
}

fn validate_index_budget(count: u32, maximum: u64) -> Result<(), PushPackValidationError> {
    let per_entry = u64::try_from(std::mem::size_of::<ParsedEntry>())
        .ok()
        .and_then(|bytes| {
            bytes.checked_add(u64::try_from(std::mem::size_of::<PushPackIndexRow>()).ok()?)
        })
        .and_then(|bytes| bytes.checked_add(128))
        .ok_or(PushPackValidationError::IndexLimit)?;
    u64::from(count)
        .checked_mul(per_entry)
        .filter(|bytes| *bytes <= maximum)
        .ok_or(PushPackValidationError::IndexLimit)?;
    Ok(())
}

fn object_kind(type_code: u8) -> Result<ObjectKind, PushPackValidationError> {
    match type_code {
        1 => Ok(ObjectKind::Commit),
        2 => Ok(ObjectKind::Tree),
        3 => Ok(ObjectKind::Blob),
        4 => Ok(ObjectKind::Tag),
        _ => Err(PushPackValidationError::InvalidHeader),
    }
}

fn checked_add(current: u64, additional: u64) -> Result<u64, PushPackValidationError> {
    current
        .checked_add(additional)
        .ok_or(PushPackValidationError::IndexLimit)
}
