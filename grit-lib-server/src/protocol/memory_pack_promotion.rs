//! Staged atomic publication of validated push PACKs into [`MemoryBackend`](crate::memory::MemoryBackend).
//!
//! Quarantine reads, hashing, allocation, index conversion, and structural preparation complete
//! before the backend acquires its repository pack lock. Publication installs bytes, index rows,
//! object locations, and an idempotency receipt together; it never updates refs. Structural rows
//! are deliberately deferred until they can share an atomic transaction with browse and commit
//! graph storage and use a real existing-generation source.

use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};

use crate::ids::{RepositoryId, TenantId};
use crate::memory::MemoryBackend;
use crate::protocol::push_pack_validation::ValidatedPushPack;
use crate::protocol::push_prepared::PreparedPush;
use crate::protocol::push_quarantine::{
    PackQuarantine, QuarantineFence, QuarantineId, QuarantineState,
};
use crate::storage::{PackMetadata, PackObjectIndex, StoredPack};

const INDEX_ROW_OVERHEAD: u64 = 96;

/// Explicit allocation and range-read bounds for memory pack promotion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryPackPromotionLimits {
    /// Maximum complete compressed PACK bytes retained by the backend.
    pub max_pack_bytes: u64,
    /// Maximum converted pack-index bytes.
    pub max_index_bytes: u64,
    /// Maximum private quarantine range read.
    pub range_chunk_bytes: usize,
}

impl MemoryPackPromotionLimits {
    fn validate(self) -> Result<Self, MemoryPackPromotionError> {
        let hard = crate::protocol::push_metrics::ReceivePackLimits::HARD_MAX;
        let range = u64::try_from(self.range_chunk_bytes)
            .map_err(|_| MemoryPackPromotionError::InvalidLimits)?;
        if self.max_pack_bytes == 0
            || self.max_pack_bytes > hard.max_pack_bytes
            || self.max_index_bytes == 0
            || self.max_index_bytes > hard.max_metadata_memory_bytes
            || self.range_chunk_bytes == 0
            || range > hard.bytes_in_flight
        {
            return Err(MemoryPackPromotionError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Typed memory promotion failure before ref publication.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum MemoryPackPromotionError {
    /// Limits are zero, inconsistent, or above hard ceilings.
    #[error("invalid memory pack promotion limits")]
    InvalidLimits,
    /// Prepared, validated, quarantine, or repository bindings disagree.
    #[error("memory pack promotion binding mismatch")]
    Binding,
    /// The repository generation changed after push preparation.
    #[error("memory pack promotion repository generation is stale")]
    StaleGeneration,
    /// A content-addressed checksum or index collision was detected.
    #[error("memory pack promotion checksum or index collision")]
    Collision,
    /// A private quarantine range read failed.
    #[error("memory pack promotion quarantine read failed")]
    Quarantine,
    /// PACK bytes do not match the sealed checksum and trailer.
    #[error("memory pack promotion checksum verification failed")]
    Checksum,
    /// Index or structural metadata exceeded a configured bound.
    #[error("memory pack promotion metadata exceeds configured bounds")]
    MetadataLimit,
    /// A bounded allocation failed.
    #[error("memory pack promotion bounded allocation failed")]
    Allocation,
    /// The memory backend lock or pack preparation failed.
    #[error("memory pack promotion backend failed")]
    Backend,
}

/// Immutable evidence that a PACK and its metadata are available for later ref publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryPackPromotionReceipt {
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

impl MemoryPackPromotionReceipt {
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

    /// Return the repository generation that later ref publication must recheck.
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

    /// Return the prepared metadata fingerprint bound to this publication.
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

    /// Return whether identical content-addressed pack bytes were already installed.
    #[must_use]
    pub const fn deduplicated(&self) -> bool {
        self.deduplicated
    }
}

pub(crate) struct MemoryPackPromotionStage {
    pub(crate) tenant: TenantId,
    pub(crate) repository: RepositoryId,
    pub(crate) quarantine_id: QuarantineId,
    pub(crate) quarantine_generation: u64,
    pub(crate) repository_generation: u64,
    pub(crate) prepared_fingerprint: ObjectId,
    pub(crate) pack_checksum: ObjectId,
    pub(crate) index_checksum: ObjectId,
    pub(crate) pack: StoredPack,
}

/// Stage and atomically install one self-contained validated push PACK into memory storage.
///
/// This function performs no ref, reflog, event, or quarantine lifecycle mutation. On failure the
/// source quarantine remains sealed, invisible, and eligible for caller-driven cleanup. Retry with
/// the same exact bindings returns an idempotent receipt.
///
/// # Errors
///
/// Returns a typed binding, limit, allocation, checksum, generation, collision, quarantine, or
/// backend failure.
pub async fn promote_memory_push_pack<Q>(
    backend: &MemoryBackend,
    prepared: &PreparedPush,
    validated: &ValidatedPushPack,
    quarantine: &mut Q,
    fence: QuarantineFence,
    limits: MemoryPackPromotionLimits,
) -> Result<MemoryPackPromotionReceipt, MemoryPackPromotionError>
where
    Q: PackQuarantine,
{
    let limits = limits.validate()?;
    let snapshot = quarantine.snapshot();
    let binding = prepared.quarantine();
    let manifest = snapshot
        .manifest
        .as_ref()
        .ok_or(MemoryPackPromotionError::Binding)?;
    let pack_checksum = manifest
        .pack_checksum
        .ok_or(MemoryPackPromotionError::Binding)?;
    let index_checksum = manifest
        .index_checksum
        .ok_or(MemoryPackPromotionError::Binding)?;
    let count = u32::try_from(validated.index.len())
        .map_err(|_| MemoryPackPromotionError::MetadataLimit)?;
    if snapshot.state != QuarantineState::Validated
        || snapshot.tenant != *prepared.tenant()
        || snapshot.repository != *prepared.repository()
        || snapshot.id != fence.id()
        || snapshot.generation != fence.generation()
        || binding.id != snapshot.id
        || binding.generation != snapshot.generation
        || binding.manifest != *manifest
        || binding.attestation != validated.attestation
        || validated.fence != fence
        || prepared.hash_algo() != manifest.hash_algo
        || pack_checksum.algo() != manifest.hash_algo
        || index_checksum.algo() != manifest.hash_algo
        || prepared.fingerprint().algo() != manifest.hash_algo
        || prepared.pack_index() != validated.index
        || manifest.pack_bytes != snapshot.bytes
        || manifest.pack_bytes > limits.max_pack_bytes
        || manifest.object_count != count
        || !manifest.self_contained
        || !validated.attestation.self_contained()
        || !validated.attestation.validation_complete()
        || validated.attestation.pack_checksum() != pack_checksum
        || validated.attestation.index_checksum() != index_checksum
        || validated.attestation.object_count() != count
        || prepared.repository_generation() == 0
    {
        return Err(MemoryPackPromotionError::Binding);
    }

    let index_bytes = u64::try_from(validated.index.len())
        .ok()
        .and_then(|rows| {
            rows.checked_mul(
                u64::try_from(std::mem::size_of::<PackObjectIndex>())
                    .ok()?
                    .checked_add(INDEX_ROW_OVERHEAD)?,
            )
        })
        .filter(|bytes| *bytes <= limits.max_index_bytes)
        .ok_or(MemoryPackPromotionError::MetadataLimit)?;
    let _ = index_bytes;
    let capacity = usize::try_from(manifest.pack_bytes)
        .map_err(|_| MemoryPackPromotionError::MetadataLimit)?;
    let mut data = Vec::new();
    data.try_reserve_exact(capacity)
        .map_err(|_| MemoryPackPromotionError::Allocation)?;
    let mut hasher = PackBodyHasher::new(manifest.hash_algo);
    let trailer_start = manifest
        .pack_bytes
        .checked_sub(
            u64::try_from(manifest.hash_algo.len())
                .map_err(|_| MemoryPackPromotionError::Binding)?,
        )
        .ok_or(MemoryPackPromotionError::Binding)?;
    let mut offset = 0_u64;
    while offset < manifest.pack_bytes {
        let remaining = manifest
            .pack_bytes
            .checked_sub(offset)
            .ok_or(MemoryPackPromotionError::Binding)?;
        let length = usize::try_from(
            remaining.min(
                u64::try_from(limits.range_chunk_bytes)
                    .map_err(|_| MemoryPackPromotionError::InvalidLimits)?,
            ),
        )
        .map_err(|_| MemoryPackPromotionError::MetadataLimit)?;
        let bytes = quarantine
            .read_range(fence, offset, length)
            .await
            .map_err(|_| MemoryPackPromotionError::Quarantine)?;
        if bytes.len() != length {
            return Err(MemoryPackPromotionError::Quarantine);
        }
        let hash_end = offset
            .checked_add(u64::try_from(length).map_err(|_| MemoryPackPromotionError::Binding)?)
            .map(|end| end.min(trailer_start))
            .ok_or(MemoryPackPromotionError::Binding)?;
        if hash_end > offset {
            let hashed = usize::try_from(hash_end - offset)
                .map_err(|_| MemoryPackPromotionError::Binding)?;
            hasher.update(&bytes[..hashed]);
        }
        data.extend_from_slice(&bytes);
        offset = offset
            .checked_add(u64::try_from(length).map_err(|_| MemoryPackPromotionError::Binding)?)
            .ok_or(MemoryPackPromotionError::Binding)?;
    }
    let trailer = data
        .get(usize::try_from(trailer_start).map_err(|_| MemoryPackPromotionError::Binding)?..)
        .ok_or(MemoryPackPromotionError::Checksum)?;
    if trailer != pack_checksum.as_bytes() || hasher.finish()? != pack_checksum {
        return Err(MemoryPackPromotionError::Checksum);
    }

    let mut index = Vec::new();
    index
        .try_reserve_exact(validated.index.len())
        .map_err(|_| MemoryPackPromotionError::Allocation)?;
    for row in &validated.index {
        index.push(PackObjectIndex {
            oid: row.oid,
            kind: row.kind,
            offset: row.entry.offset,
            size: row.resolved_size,
            compressed_size: row.entry.length,
        });
    }
    let mut pack_checksum_bytes = Vec::new();
    pack_checksum_bytes
        .try_reserve_exact(manifest.hash_algo.len())
        .map_err(|_| MemoryPackPromotionError::Allocation)?;
    pack_checksum_bytes.extend_from_slice(pack_checksum.as_bytes());
    let mut index_checksum_bytes = Vec::new();
    index_checksum_bytes
        .try_reserve_exact(manifest.hash_algo.len())
        .map_err(|_| MemoryPackPromotionError::Allocation)?;
    index_checksum_bytes.extend_from_slice(index_checksum.as_bytes());
    let pack = StoredPack {
        metadata: PackMetadata {
            pack_checksum: pack_checksum_bytes,
            index_checksum: index_checksum_bytes,
            object_count: count,
            size_bytes: manifest.pack_bytes,
            storage_order: 0,
        },
        data,
        index,
    };
    backend.install_staged_push_pack(MemoryPackPromotionStage {
        tenant: prepared.tenant().clone(),
        repository: prepared.repository().clone(),
        quarantine_id: snapshot.id,
        quarantine_generation: snapshot.generation,
        repository_generation: prepared.repository_generation(),
        prepared_fingerprint: prepared.fingerprint(),
        pack_checksum,
        index_checksum,
        pack,
    })
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

    fn finish(self) -> Result<ObjectId, MemoryPackPromotionError> {
        match self {
            Self::Sha1(hasher) => ObjectId::from_bytes(&hasher.finalize()),
            Self::Sha256(hasher) => ObjectId::from_bytes(&hasher.finalize()),
        }
        .map_err(|_| MemoryPackPromotionError::Checksum)
    }
}

/// Hash a loose collision candidate without copying its payload.
pub(crate) fn canonical_promoted_object_id(
    kind: ObjectKind,
    data: &[u8],
    hash_algo: HashAlgo,
) -> Result<ObjectId, MemoryPackPromotionError> {
    let mut hasher = PackBodyHasher::new(hash_algo);
    hasher.update(format!("{kind} {}\0", data.len()).as_bytes());
    hasher.update(data);
    hasher.finish()
}
