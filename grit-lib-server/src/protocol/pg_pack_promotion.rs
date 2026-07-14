//! Bounded atomic promotion of validated push PACKs into PostgreSQL storage.
//!
//! Quarantine I/O, checksumming, allocations, and structural-row preparation happen before the
//! SQL transaction. The transaction then guards the repository generation and installs the PACK,
//! its object index, and an exact retry receipt without publishing refs. Browse/commit rows are
//! deferred until raw tree names and real DAG generations can be published correctly.

use grit_lib::objects::{HashAlgo, ObjectId};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use std::collections::HashSet;
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_pack_validation::ValidatedPushPack;
use crate::protocol::push_prepared::PreparedPush;
use crate::protocol::push_quarantine::{
    PackQuarantine, QuarantineFence, QuarantineId, QuarantineState,
};
use crate::sqlx_postgres::PgServerStorage;
use crate::storage::{PackMetadata, PackObjectIndex, StoredPack};

const INDEX_ROW_OVERHEAD: u64 = 96;

/// Explicit allocation, range-read, work, and time bounds for PostgreSQL PACK promotion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgPackPromotionLimits {
    /// Maximum complete compressed PACK bytes staged before the transaction.
    pub max_pack_bytes: u64,
    /// Maximum converted pack-index bytes.
    pub max_index_bytes: u64,
    /// Maximum private quarantine range read.
    pub range_chunk_bytes: usize,
    /// Explicit phase start.
    pub started_at: OffsetDateTime,
    /// Explicit phase deadline.
    pub deadline: OffsetDateTime,
}

impl PgPackPromotionLimits {
    fn validate(self) -> Result<Self, PgPackPromotionError> {
        let hard = crate::protocol::push_metrics::ReceivePackLimits::HARD_MAX;
        let range = u64::try_from(self.range_chunk_bytes)
            .map_err(|_| PgPackPromotionError::InvalidLimits)?;
        let max_duration = time::Duration::try_from(hard.max_duration)
            .map_err(|_| PgPackPromotionError::InvalidLimits)?;
        if self.max_pack_bytes == 0
            || self.max_pack_bytes > hard.max_pack_bytes
            || self.max_index_bytes == 0
            || self.max_index_bytes > hard.max_metadata_memory_bytes
            || self.range_chunk_bytes == 0
            || range > hard.bytes_in_flight
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > max_duration
        {
            return Err(PgPackPromotionError::InvalidLimits);
        }
        Ok(self)
    }
}

/// One caller-supplied monotonic-time and cancellation observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgPackPromotionObservation {
    /// Explicit observation time.
    pub observed_at: OffsetDateTime,
    /// Explicit cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral promotion cancellation and deadline source.
pub trait PgPackPromotionControl {
    /// Return the latest explicit observation.
    fn observe(&mut self) -> PgPackPromotionObservation;
}

/// Explicit deterministic promotion-work checkpoint.
pub trait PgPackPromotionWork {
    /// Charge staging or transactional preparation work units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed PostgreSQL promotion work allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PgPackPromotionWorkLimit {
    remaining: u64,
}

impl PgPackPromotionWorkLimit {
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

impl PgPackPromotionWork for PgPackPromotionWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Typed PostgreSQL promotion failure before ref publication.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PgPackPromotionError {
    /// Limits are zero, inconsistent, or above hard ceilings.
    #[error("invalid PostgreSQL pack promotion limits")]
    InvalidLimits,
    /// Prepared, validated, quarantine, or repository bindings disagree.
    #[error("PostgreSQL pack promotion binding mismatch")]
    Binding,
    /// The repository generation changed after push preparation.
    #[error("PostgreSQL pack promotion repository generation is stale")]
    StaleGeneration,
    /// A content-addressed checksum, row, or retry-receipt collision was detected.
    #[error("PostgreSQL pack promotion collision")]
    Collision,
    /// A private quarantine range read failed.
    #[error("PostgreSQL pack promotion quarantine read failed")]
    Quarantine,
    /// PACK bytes do not match the sealed checksum and trailer.
    #[error("PostgreSQL pack promotion checksum verification failed")]
    Checksum,
    /// Index or structural metadata exceeded a configured bound.
    #[error("PostgreSQL pack promotion metadata exceeds configured bounds")]
    MetadataLimit,
    /// A bounded allocation failed.
    #[error("PostgreSQL pack promotion bounded allocation failed")]
    Allocation,
    /// Explicit deterministic work allowance was exhausted.
    #[error("PostgreSQL pack promotion work budget exhausted")]
    WorkBudget,
    /// Explicit cancellation was observed.
    #[error("PostgreSQL pack promotion cancelled")]
    Cancelled,
    /// Explicit deadline elapsed or time moved backwards.
    #[error("PostgreSQL pack promotion deadline elapsed")]
    Deadline,
    /// The PostgreSQL backend rejected or failed the transaction.
    #[error("PostgreSQL pack promotion backend failed")]
    Backend,
}

/// Immutable evidence that PostgreSQL atomically installed a PACK and its metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PgPackPromotionReceipt {
    tenant: TenantId,
    repository: RepositoryId,
    quarantine_id: QuarantineId,
    quarantine_generation: u64,
    repository_generation: u64,
    pack_checksum: ObjectId,
    index_checksum: ObjectId,
    prepared_fingerprint: ObjectId,
    object_count: u32,
    size_bytes: u64,
    deduplicated: bool,
}

impl PgPackPromotionReceipt {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tenant: TenantId,
        repository: RepositoryId,
        quarantine_id: QuarantineId,
        quarantine_generation: u64,
        repository_generation: u64,
        pack_checksum: ObjectId,
        index_checksum: ObjectId,
        prepared_fingerprint: ObjectId,
        object_count: u32,
        size_bytes: u64,
        deduplicated: bool,
    ) -> Self {
        Self {
            tenant,
            repository,
            quarantine_id,
            quarantine_generation,
            repository_generation,
            pack_checksum,
            index_checksum,
            prepared_fingerprint,
            object_count,
            size_bytes,
            deduplicated,
        }
    }

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
    /// Return the source quarantine identity.
    #[must_use]
    pub const fn quarantine_id(&self) -> QuarantineId {
        self.quarantine_id
    }
    /// Return the exact sealed quarantine generation.
    #[must_use]
    pub const fn quarantine_generation(&self) -> u64 {
        self.quarantine_generation
    }
    /// Return the repository generation guarded by the transaction.
    #[must_use]
    pub const fn repository_generation(&self) -> u64 {
        self.repository_generation
    }
    /// Return the content-addressed PACK checksum.
    #[must_use]
    pub const fn pack_checksum(&self) -> ObjectId {
        self.pack_checksum
    }
    /// Return the ordered validated index checksum.
    #[must_use]
    pub const fn index_checksum(&self) -> ObjectId {
        self.index_checksum
    }
    /// Return the prepared metadata fingerprint.
    #[must_use]
    pub const fn prepared_fingerprint(&self) -> ObjectId {
        self.prepared_fingerprint
    }
    /// Return the installed PACK object count.
    #[must_use]
    pub const fn object_count(&self) -> u32 {
        self.object_count
    }
    /// Return complete installed PACK bytes.
    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    /// Return whether identical pack storage already existed.
    #[must_use]
    pub const fn deduplicated(&self) -> bool {
        self.deduplicated
    }
}

pub(crate) struct PgPackPromotionStage {
    pub(crate) tenant: TenantId,
    pub(crate) repository: RepositoryId,
    pub(crate) quarantine_id: QuarantineId,
    pub(crate) quarantine_generation: u64,
    pub(crate) repository_generation: u64,
    pub(crate) prepared_fingerprint: ObjectId,
    pub(crate) pack_checksum: ObjectId,
    pub(crate) index_checksum: ObjectId,
    pub(crate) pack: StoredPack,
    pub(crate) storage: PgPackPromotionStorage,
}

pub(crate) enum PgPackPromotionStorage {
    Database,
    External {
        backend_name: String,
        storage_key: String,
        storage_version: Vec<u8>,
        storage_checksum: ObjectId,
    },
}

impl PgPackPromotionStorage {
    pub(crate) fn backend_name(&self) -> Option<&str> {
        match self {
            Self::Database => None,
            Self::External { backend_name, .. } => Some(backend_name),
        }
    }

    pub(crate) fn storage_key(&self) -> Option<&str> {
        match self {
            Self::Database => None,
            Self::External { storage_key, .. } => Some(storage_key),
        }
    }

    pub(crate) fn storage_version(&self) -> Option<&[u8]> {
        match self {
            Self::Database => None,
            Self::External {
                storage_version, ..
            } => Some(storage_version),
        }
    }

    pub(crate) fn storage_checksum(&self) -> Option<&[u8]> {
        match self {
            Self::Database => None,
            Self::External {
                storage_checksum, ..
            } => Some(storage_checksum.as_bytes()),
        }
    }
}

pub(crate) enum PgPackPromotionInstallError {
    StaleGeneration,
    Collision,
    Backend,
}

impl From<PgPackPromotionInstallError> for PgPackPromotionError {
    fn from(error: PgPackPromotionInstallError) -> Self {
        match error {
            PgPackPromotionInstallError::StaleGeneration => Self::StaleGeneration,
            PgPackPromotionInstallError::Collision => Self::Collision,
            PgPackPromotionInstallError::Backend => Self::Backend,
        }
    }
}

/// Stage and atomically install one self-contained validated push PACK into PostgreSQL.
///
/// This function performs no ref, reflog, or quarantine lifecycle mutation. Failure leaves the
/// sealed source quarantine eligible for caller-driven retry or cleanup. An exact retry returns
/// the durable receipt written by the first successful transaction.
///
/// # Errors
///
/// Returns typed binding, limit, work, control, checksum, collision, generation, quarantine, or
/// backend failures. Any backend failure rolls back the complete SQL transaction.
pub async fn promote_pg_push_pack<Q, W, C>(
    backend: &PgServerStorage,
    prepared: &PreparedPush,
    validated: &ValidatedPushPack,
    quarantine: &mut Q,
    fence: QuarantineFence,
    limits: PgPackPromotionLimits,
    work: &mut W,
    control: &mut C,
) -> Result<PgPackPromotionReceipt, PgPackPromotionError>
where
    Q: PackQuarantine,
    W: PgPackPromotionWork,
    C: PgPackPromotionControl,
{
    let limits = limits.validate()?;
    let mut last_observed_at = limits.started_at;
    checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
    let snapshot = quarantine.snapshot();
    let binding = prepared.quarantine();
    let manifest = snapshot
        .manifest
        .as_ref()
        .ok_or(PgPackPromotionError::Binding)?;
    let pack_checksum = manifest
        .pack_checksum
        .ok_or(PgPackPromotionError::Binding)?;
    let index_checksum = manifest
        .index_checksum
        .ok_or(PgPackPromotionError::Binding)?;
    let object_count =
        u32::try_from(validated.index().len()).map_err(|_| PgPackPromotionError::MetadataLimit)?;
    if snapshot.state != QuarantineState::Validated
        || snapshot.tenant != *prepared.tenant()
        || snapshot.repository != *prepared.repository()
        || snapshot.id != fence.id()
        || snapshot.generation != fence.generation()
        || binding.id != snapshot.id
        || binding.generation != snapshot.generation
        || binding.manifest != *manifest
        || binding.attestation != validated.attestation()
        || validated.fence() != fence
        || prepared.hash_algo() != manifest.hash_algo
        || pack_checksum.algo() != manifest.hash_algo
        || index_checksum.algo() != manifest.hash_algo
        || prepared.fingerprint().algo() != manifest.hash_algo
        || prepared.pack_index() != validated.index()
        || manifest.pack_bytes != snapshot.bytes
        || manifest.pack_bytes > limits.max_pack_bytes
        || manifest.object_count != object_count
        || !manifest.self_contained
        || !validated.attestation().self_contained()
        || !validated.attestation().validation_complete()
        || validated.attestation().pack_checksum() != pack_checksum
        || validated.attestation().index_checksum() != index_checksum
        || validated.attestation().object_count() != object_count
        || prepared.repository_generation() == 0
    {
        return Err(PgPackPromotionError::Binding);
    }

    bounded_row_bytes(
        validated.index().len(),
        INDEX_ROW_OVERHEAD
            .checked_add(
                u64::try_from(std::mem::size_of::<PackObjectIndex>())
                    .map_err(|_| PgPackPromotionError::MetadataLimit)?,
            )
            .ok_or(PgPackPromotionError::MetadataLimit)?,
        limits.max_index_bytes,
    )?;
    let capacity =
        usize::try_from(manifest.pack_bytes).map_err(|_| PgPackPromotionError::MetadataLimit)?;
    let mut data = Vec::new();
    data.try_reserve_exact(capacity)
        .map_err(|_| PgPackPromotionError::Allocation)?;
    let mut hasher = PackBodyHasher::new(manifest.hash_algo);
    let trailer_start = manifest
        .pack_bytes
        .checked_sub(manifest.hash_algo.len() as u64)
        .ok_or(PgPackPromotionError::Binding)?;
    let mut offset = 0_u64;
    while offset < manifest.pack_bytes {
        let remaining = manifest
            .pack_bytes
            .checked_sub(offset)
            .ok_or(PgPackPromotionError::Binding)?;
        let length = usize::try_from(remaining.min(limits.range_chunk_bytes as u64))
            .map_err(|_| PgPackPromotionError::MetadataLimit)?;
        checkpoint(
            &limits,
            work,
            control,
            &mut last_observed_at,
            u64::try_from(length).map_err(|_| PgPackPromotionError::MetadataLimit)?,
        )?;
        let bytes = quarantine
            .read_range(fence, offset, length)
            .await
            .map_err(|_| PgPackPromotionError::Quarantine)?;
        checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
        if bytes.len() != length {
            return Err(PgPackPromotionError::Quarantine);
        }
        let end = offset
            .checked_add(length as u64)
            .ok_or(PgPackPromotionError::Binding)?;
        let hash_end = end.min(trailer_start);
        if hash_end > offset {
            let hashed =
                usize::try_from(hash_end - offset).map_err(|_| PgPackPromotionError::Binding)?;
            hasher.update(&bytes[..hashed]);
        }
        data.extend_from_slice(&bytes);
        offset = end;
    }
    let trailer = data
        .get(usize::try_from(trailer_start).map_err(|_| PgPackPromotionError::Binding)?..)
        .ok_or(PgPackPromotionError::Checksum)?;
    if trailer != pack_checksum.as_bytes() || hasher.finish()? != pack_checksum {
        return Err(PgPackPromotionError::Checksum);
    }

    let mut index = Vec::new();
    index
        .try_reserve_exact(validated.index().len())
        .map_err(|_| PgPackPromotionError::Allocation)?;
    let mut seen_oids = HashSet::new();
    seen_oids
        .try_reserve(validated.index().len())
        .map_err(|_| PgPackPromotionError::Allocation)?;
    let mut seen_offsets = HashSet::new();
    seen_offsets
        .try_reserve(validated.index().len())
        .map_err(|_| PgPackPromotionError::Allocation)?;
    for row in validated.index() {
        if !seen_oids.insert(row.oid) || !seen_offsets.insert(row.entry.offset) {
            return Err(PgPackPromotionError::Binding);
        }
        index.push(PackObjectIndex {
            oid: row.oid,
            kind: row.kind,
            offset: row.entry.offset,
            size: row.resolved_size,
            compressed_size: row.entry.length,
        });
    }
    checkpoint(
        &limits,
        work,
        control,
        &mut last_observed_at,
        u64::try_from(index.len()).map_err(|_| PgPackPromotionError::MetadataLimit)?,
    )?;
    let mut pack_checksum_bytes = Vec::new();
    pack_checksum_bytes
        .try_reserve_exact(manifest.hash_algo.len())
        .map_err(|_| PgPackPromotionError::Allocation)?;
    pack_checksum_bytes.extend_from_slice(pack_checksum.as_bytes());
    let mut index_checksum_bytes = Vec::new();
    index_checksum_bytes
        .try_reserve_exact(manifest.hash_algo.len())
        .map_err(|_| PgPackPromotionError::Allocation)?;
    index_checksum_bytes.extend_from_slice(index_checksum.as_bytes());
    let pack = StoredPack {
        metadata: PackMetadata {
            pack_checksum: pack_checksum_bytes,
            index_checksum: index_checksum_bytes,
            object_count,
            size_bytes: manifest.pack_bytes,
            storage_order: 0,
        },
        data,
        index,
    };
    backend
        .install_promoted_push_pack(PgPackPromotionStage {
            tenant: prepared.tenant().clone(),
            repository: prepared.repository().clone(),
            quarantine_id: snapshot.id,
            quarantine_generation: snapshot.generation,
            repository_generation: prepared.repository_generation(),
            prepared_fingerprint: prepared.fingerprint(),
            pack_checksum,
            index_checksum,
            pack,
            storage: PgPackPromotionStorage::Database,
        })
        .await
        .map_err(Into::into)
}

fn bounded_row_bytes(rows: usize, per_row: u64, maximum: u64) -> Result<(), PgPackPromotionError> {
    let bytes = u64::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(per_row))
        .ok_or(PgPackPromotionError::MetadataLimit)?;
    if bytes > maximum {
        return Err(PgPackPromotionError::MetadataLimit);
    }
    Ok(())
}

fn checkpoint<W, C>(
    limits: &PgPackPromotionLimits,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
    units: u64,
) -> Result<(), PgPackPromotionError>
where
    W: PgPackPromotionWork,
    C: PgPackPromotionControl,
{
    let observation = control.observe();
    if observation.observed_at < *last_observed_at {
        return Err(PgPackPromotionError::Deadline);
    }
    *last_observed_at = observation.observed_at;
    if observation.cancelled {
        return Err(PgPackPromotionError::Cancelled);
    }
    if observation.observed_at < limits.started_at || observation.observed_at >= limits.deadline {
        return Err(PgPackPromotionError::Deadline);
    }
    if !work.charge(units) {
        return Err(PgPackPromotionError::WorkBudget);
    }
    Ok(())
}

enum PackBodyHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl PackBodyHasher {
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
    fn finish(self) -> Result<ObjectId, PgPackPromotionError> {
        match self {
            Self::Sha1(hasher) => ObjectId::from_bytes(&hasher.finalize()),
            Self::Sha256(hasher) => ObjectId::from_bytes(&hasher.finalize()),
        }
        .map_err(|_| PgPackPromotionError::Checksum)
    }
}
