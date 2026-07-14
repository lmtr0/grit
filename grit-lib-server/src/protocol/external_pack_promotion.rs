//! Bounded external-byte promotion with PostgreSQL as the visibility boundary.
//!
//! The provider conditionally creates a content-addressed final object from bounded quarantine
//! ranges. PostgreSQL publishes only verified external metadata, the pack index, and the exact
//! retry receipt. A separately committed session-owned orphan row makes a crash after finalization
//! reclaimable without making the bytes visible to ordinary repository reads.

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use std::collections::HashSet;
use std::fmt;
use time::OffsetDateTime;

use crate::externalized::PgExternalizedStorage;
use crate::protocol::pg_pack_promotion::{
    PgPackPromotionReceipt, PgPackPromotionStage, PgPackPromotionStorage,
};
use crate::protocol::push_pack_validation::ValidatedPushPack;
use crate::protocol::push_prepared::PreparedPush;
use crate::protocol::push_quarantine::{PackQuarantine, QuarantineFence, QuarantineState};
use crate::storage::{PackMetadata, PackObjectIndex, StoredPack};

const INDEX_ROW_OVERHEAD: u64 = 96;
const MAX_PROVIDER_VERSION_BYTES: usize = 4_096;
const MAX_BACKEND_NAME_BYTES: usize = 256;
const MAX_STORAGE_KEY_BYTES: usize = 4_096;
const MIN_NONFINAL_PART_BYTES: u64 = 5 * 1024 * 1024;
const MAX_PART_BYTES: u64 = 5 * 1024 * 1024 * 1024;
const MAX_MULTIPART_PARTS: u32 = 10_000;

/// Opaque immutable provider version or ETag binding.
#[derive(Clone, PartialEq, Eq)]
pub struct ExternalPackObjectVersion(Vec<u8>);

impl ExternalPackObjectVersion {
    /// Construct a non-empty bounded provider version.
    ///
    /// # Errors
    ///
    /// Returns [`ExternalPackPromotionError::ProviderMetadata`] for an empty or oversized value.
    pub fn new(bytes: Vec<u8>) -> Result<Self, ExternalPackPromotionError> {
        if bytes.is_empty() || bytes.len() > MAX_PROVIDER_VERSION_BYTES {
            return Err(ExternalPackPromotionError::ProviderMetadata);
        }
        Ok(Self(bytes))
    }

    /// Borrow opaque provider bytes for durable exact-retry binding.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ExternalPackObjectVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ExternalPackObjectVersion([opaque])")
    }
}

/// Verified immutable final-object metadata.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalPackObject {
    /// Complete PACK bytes, including its trailing checksum.
    pub size_bytes: u64,
    /// Git PACK body/trailer checksum verified by the provider.
    pub content_checksum: ObjectId,
    /// Immutable provider version used to disambiguate retries and completion results.
    pub version: ExternalPackObjectVersion,
}

/// Immutable conditional-create plan passed to the external provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalPackUploadPlan {
    /// Complete expected final bytes.
    pub size_bytes: u64,
    /// Expected Git PACK body/trailer checksum.
    pub content_checksum: ObjectId,
    /// Maximum individual part bytes.
    pub part_bytes: usize,
    /// Maximum parts submitted in one provider call.
    pub maximum_in_flight_parts: usize,
    /// Maximum total parts.
    pub maximum_parts: u32,
}

/// One positional immutable upload part.
#[derive(Debug)]
pub struct ExternalPackPart {
    /// One-based deterministic part ordinal.
    pub ordinal: u32,
    /// Exact byte offset in the final object.
    pub offset: u64,
    /// Checksum of this part's bytes using the repository hash algorithm.
    pub checksum: ObjectId,
    /// Owned bounded part bytes.
    pub bytes: Vec<u8>,
}

/// Result of conditionally beginning one content-addressed final-object creation.
pub enum ExternalPackPromotionBegin<U> {
    /// An immutable object already owns the final key.
    Existing(ExternalPackObject),
    /// A fresh invisible multipart/session upload may receive parts.
    Upload(U),
}

/// Non-sensitive provider operation failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("external pack provider operation failed")]
pub struct ExternalPackProviderError;

/// Provider-owned conditional upload session.
#[async_trait]
pub trait ExternalPackPromotionUpload: Send {
    /// Submit a bounded consecutive batch of positional parts.
    ///
    /// Implementations may upload the batch concurrently, but must not retain more than the
    /// supplied batch and must reject duplicate, missing, or overlapping positions.
    async fn upload_parts(
        &mut self,
        parts: Vec<ExternalPackPart>,
    ) -> Result<(), ExternalPackProviderError>;

    /// Atomically finalize the immutable object only if the final key is absent.
    ///
    /// Errors are ambiguous: callers always inspect the final key before deciding whether the
    /// operation failed.
    async fn complete(self) -> Result<ExternalPackObject, ExternalPackProviderError>;

    /// Abort unfinished provider state. This must never delete an already finalized shared key.
    async fn abort(self) -> Result<(), ExternalPackProviderError>;
}

/// Provider-neutral final-object inspection and conditional multipart creation.
#[async_trait]
pub trait ExternalPackPromotionStore: Send + Sync {
    /// Provider upload-session type.
    type Upload: ExternalPackPromotionUpload;

    /// Inspect immutable final-object metadata without downloading its bytes.
    async fn inspect_final(
        &self,
        key: &str,
    ) -> Result<Option<ExternalPackObject>, ExternalPackProviderError>;

    /// Begin an invisible upload or return the object already owning `key`.
    ///
    /// The provider must conditionally create the final object. It must never overwrite an
    /// existing key, even when an upload races another request.
    async fn begin_create_if_absent(
        &self,
        key: &str,
        plan: &ExternalPackUploadPlan,
    ) -> Result<ExternalPackPromotionBegin<Self::Upload>, ExternalPackProviderError>;
}

/// One caller-supplied monotonic-time and cancellation observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalPackPromotionObservation {
    /// Explicit observation time.
    pub observed_at: OffsetDateTime,
    /// Explicit cancellation signal.
    pub cancelled: bool,
}

/// Runtime-neutral external promotion cancellation and deadline source.
pub trait ExternalPackPromotionControl {
    /// Return the latest explicit observation.
    fn observe(&mut self) -> ExternalPackPromotionObservation;
}

/// Explicit deterministic external promotion work checkpoint.
pub trait ExternalPackPromotionWork {
    /// Charge quarantine-copy, hash, provider, or metadata work units.
    fn charge(&mut self, units: u64) -> bool;
}

/// Counter-backed external promotion work allowance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalPackPromotionWorkLimit {
    remaining: u64,
}

impl ExternalPackPromotionWorkLimit {
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

impl ExternalPackPromotionWork for ExternalPackPromotionWorkLimit {
    fn charge(&mut self, units: u64) -> bool {
        let Some(remaining) = self.remaining.checked_sub(units) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}

/// Explicit upload, allocation, work, time, and cleanup bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExternalPackPromotionLimits {
    /// Maximum complete PACK bytes.
    pub max_pack_bytes: u64,
    /// Maximum converted index memory.
    pub max_index_bytes: u64,
    /// Maximum bytes in one quarantine range/provider part.
    pub part_bytes: usize,
    /// Maximum parts retained and submitted in one provider call.
    pub maximum_in_flight_parts: usize,
    /// Maximum total parts.
    pub maximum_parts: u32,
    /// Explicit phase start.
    pub started_at: OffsetDateTime,
    /// Explicit phase deadline.
    pub deadline: OffsetDateTime,
    /// Earliest time at which a later sweeper may consider this session's final object.
    pub orphan_not_before: OffsetDateTime,
}

impl ExternalPackPromotionLimits {
    fn validate(self) -> Result<Self, ExternalPackPromotionError> {
        let hard = crate::protocol::push_metrics::ReceivePackLimits::HARD_MAX;
        let part_bytes = u64::try_from(self.part_bytes)
            .map_err(|_| ExternalPackPromotionError::InvalidLimits)?;
        let in_flight = part_bytes
            .checked_mul(
                u64::try_from(self.maximum_in_flight_parts)
                    .map_err(|_| ExternalPackPromotionError::InvalidLimits)?,
            )
            .ok_or(ExternalPackPromotionError::InvalidLimits)?;
        let max_duration = time::Duration::try_from(hard.max_duration)
            .map_err(|_| ExternalPackPromotionError::InvalidLimits)?;
        if self.max_pack_bytes == 0
            || self.max_pack_bytes > hard.max_pack_bytes
            || self.max_index_bytes == 0
            || self.max_index_bytes > hard.max_metadata_memory_bytes
            || self.part_bytes == 0
            || part_bytes > hard.bytes_in_flight
            || part_bytes > MAX_PART_BYTES
            || self.maximum_in_flight_parts == 0
            || in_flight > hard.bytes_in_flight
            || self.maximum_parts == 0
            || self.maximum_parts > MAX_MULTIPART_PARTS
            || usize::try_from(self.maximum_parts).ok().is_none()
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > max_duration
            || self.orphan_not_before < self.deadline
        {
            return Err(ExternalPackPromotionError::InvalidLimits);
        }
        Ok(self)
    }
}

/// Typed external promotion failure before ref publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ExternalPackPromotionError {
    /// Limits are zero, inconsistent, or above hard ceilings.
    #[error("invalid external pack promotion limits")]
    InvalidLimits,
    /// Prepared, validated, quarantine, or repository bindings disagree.
    #[error("external pack promotion binding mismatch")]
    Binding,
    /// Index metadata exceeded configured bounds.
    #[error("external pack promotion metadata exceeds configured bounds")]
    MetadataLimit,
    /// A bounded allocation failed.
    #[error("external pack promotion bounded allocation failed")]
    Allocation,
    /// Explicit deterministic work allowance was exhausted.
    #[error("external pack promotion work budget exhausted")]
    WorkBudget,
    /// Explicit cancellation was observed.
    #[error("external pack promotion cancelled")]
    Cancelled,
    /// Explicit deadline elapsed or time moved backwards.
    #[error("external pack promotion deadline elapsed")]
    Deadline,
    /// A private quarantine range read failed.
    #[error("external pack promotion quarantine read failed")]
    Quarantine,
    /// Quarantine bytes failed PACK body/trailer checksum verification.
    #[error("external pack promotion checksum verification failed")]
    Checksum,
    /// Final provider metadata was missing, malformed, stale, or ambiguous.
    #[error("external pack provider metadata is invalid")]
    ProviderMetadata,
    /// The provider operation failed and no exact final object resolved the ambiguity.
    #[error("external pack provider failed")]
    Provider,
    /// An existing content-addressed final object or SQL row conflicts with expected metadata.
    #[error("external pack promotion collision")]
    Collision,
    /// The repository generation changed after preparation.
    #[error("external pack promotion repository generation is stale")]
    StaleGeneration,
    /// PostgreSQL orphan tracking or atomic metadata publication failed.
    #[error("external pack promotion PostgreSQL operation failed")]
    Backend,
}

/// Stream and atomically publish one validated self-contained PACK through external storage.
///
/// Final bytes are verified before PostgreSQL publishes metadata. The source quarantine is never
/// mutated or deleted, and the workflow never deletes a shared content-addressed final object.
///
/// # Errors
///
/// Returns typed binding, bounds, allocation, work, control, quarantine, checksum, provider,
/// collision, generation, or PostgreSQL failures.
pub async fn promote_external_push_pack<B, Q, W, C>(
    backend: &PgExternalizedStorage<B>,
    prepared: &PreparedPush,
    validated: &ValidatedPushPack,
    quarantine: &mut Q,
    fence: QuarantineFence,
    limits: ExternalPackPromotionLimits,
    work: &mut W,
    control: &mut C,
) -> Result<PgPackPromotionReceipt, ExternalPackPromotionError>
where
    B: ExternalPackPromotionStore,
    Q: PackQuarantine,
    W: ExternalPackPromotionWork,
    C: ExternalPackPromotionControl,
{
    let limits = limits.validate()?;
    let mut last_observed_at = limits.started_at;
    checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
    let snapshot = quarantine.snapshot();
    let binding = prepared.quarantine();
    let manifest = snapshot
        .manifest
        .as_ref()
        .ok_or(ExternalPackPromotionError::Binding)?;
    let pack_checksum = manifest
        .pack_checksum
        .ok_or(ExternalPackPromotionError::Binding)?;
    let index_checksum = manifest
        .index_checksum
        .ok_or(ExternalPackPromotionError::Binding)?;
    let object_count = u32::try_from(validated.index().len())
        .map_err(|_| ExternalPackPromotionError::MetadataLimit)?;
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
        || !backend.options().write_packs_externally
        || backend.options().backend_name.is_empty()
        || backend.options().backend_name.len() > MAX_BACKEND_NAME_BYTES
        || !valid_key_prefix(&backend.options().key_prefix)
    {
        return Err(ExternalPackPromotionError::Binding);
    }
    validate_index_memory(validated.index().len(), limits.max_index_bytes)?;
    let part_count = manifest.pack_bytes.div_ceil(limits.part_bytes as u64);
    if part_count == 0
        || part_count > u64::from(limits.maximum_parts)
        || (part_count > 1 && (limits.part_bytes as u64) < MIN_NONFINAL_PART_BYTES)
    {
        return Err(ExternalPackPromotionError::MetadataLimit);
    }

    let storage_key = backend.pack_key(
        prepared.tenant(),
        prepared.repository(),
        pack_checksum.as_bytes(),
        manifest.hash_algo.name(),
    );
    if storage_key.is_empty() || storage_key.len() > MAX_STORAGE_KEY_BYTES {
        return Err(ExternalPackPromotionError::Binding);
    }
    backend
        .sql()
        .register_external_promotion_orphan(
            prepared.tenant(),
            prepared.repository(),
            &backend.options().backend_name,
            &storage_key,
            snapshot.id,
            snapshot.generation,
            prepared.fingerprint(),
            pack_checksum,
            manifest.pack_bytes,
            None,
            None,
            limits.orphan_not_before,
        )
        .await
        .map_err(map_install_error)?;

    let plan = ExternalPackUploadPlan {
        size_bytes: manifest.pack_bytes,
        content_checksum: pack_checksum,
        part_bytes: limits.part_bytes,
        maximum_in_flight_parts: limits.maximum_in_flight_parts,
        maximum_parts: limits.maximum_parts,
    };
    checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
    let begin = match backend
        .bytes()
        .begin_create_if_absent(&storage_key, &plan)
        .await
    {
        Ok(begin) => begin,
        Err(_) => {
            let object = inspect_exact(backend.bytes().as_ref(), &storage_key, &plan)
                .await?
                .ok_or(ExternalPackPromotionError::Provider)?;
            checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
            return finish_external_publication(
                backend,
                prepared,
                validated,
                snapshot.id,
                snapshot.generation,
                index_checksum,
                storage_key,
                object,
                &limits,
                work,
                control,
                &mut last_observed_at,
            )
            .await;
        }
    };
    checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
    let completed = match begin {
        ExternalPackPromotionBegin::Existing(object) => verify_exact_object(object, &plan)?,
        ExternalPackPromotionBegin::Upload(mut upload) => {
            let streamed = stream_parts(
                &mut upload,
                quarantine,
                fence,
                manifest.hash_algo,
                &plan,
                &limits,
                work,
                control,
                &mut last_observed_at,
            )
            .await;
            if let Err(error) = streamed {
                let _ = upload.abort().await;
                checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
                if !matches!(error, ExternalPackPromotionError::Provider) {
                    return Err(error);
                }
                let object = inspect_exact(backend.bytes().as_ref(), &storage_key, &plan)
                    .await?
                    .ok_or(error)?;
                checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
                object
            } else {
                match upload.complete().await {
                    Ok(object) => {
                        checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
                        verify_exact_object(object, &plan)?
                    }
                    Err(_) => {
                        checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
                        let object = inspect_exact(backend.bytes().as_ref(), &storage_key, &plan)
                            .await?
                            .ok_or(ExternalPackPromotionError::Provider)?;
                        checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
                        object
                    }
                }
            }
        }
    };
    checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
    let inspected = inspect_exact(backend.bytes().as_ref(), &storage_key, &plan)
        .await?
        .ok_or(ExternalPackPromotionError::ProviderMetadata)?;
    checkpoint(&limits, work, control, &mut last_observed_at, 1)?;
    if inspected != completed {
        return Err(ExternalPackPromotionError::ProviderMetadata);
    }
    finish_external_publication(
        backend,
        prepared,
        validated,
        snapshot.id,
        snapshot.generation,
        index_checksum,
        storage_key,
        inspected,
        &limits,
        work,
        control,
        &mut last_observed_at,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_external_publication<B, W, C>(
    backend: &PgExternalizedStorage<B>,
    prepared: &PreparedPush,
    validated: &ValidatedPushPack,
    quarantine_id: crate::protocol::push_quarantine::QuarantineId,
    quarantine_generation: u64,
    index_checksum: ObjectId,
    storage_key: String,
    object: ExternalPackObject,
    limits: &ExternalPackPromotionLimits,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
) -> Result<PgPackPromotionReceipt, ExternalPackPromotionError>
where
    B: ExternalPackPromotionStore,
    W: ExternalPackPromotionWork,
    C: ExternalPackPromotionControl,
{
    let pack_checksum = object.content_checksum;
    backend
        .sql()
        .register_external_promotion_orphan(
            prepared.tenant(),
            prepared.repository(),
            &backend.options().backend_name,
            &storage_key,
            quarantine_id,
            quarantine_generation,
            prepared.fingerprint(),
            pack_checksum,
            object.size_bytes,
            Some(object.version.as_bytes()),
            Some(object.content_checksum),
            limits.orphan_not_before,
        )
        .await
        .map_err(map_install_error)?;
    checkpoint(limits, work, control, last_observed_at, 1)?;
    checkpoint(
        limits,
        work,
        control,
        last_observed_at,
        u64::try_from(validated.index().len())
            .map_err(|_| ExternalPackPromotionError::MetadataLimit)?,
    )?;
    let index = prepare_index(validated)?;
    let pack = StoredPack {
        metadata: PackMetadata {
            pack_checksum: pack_checksum.as_bytes().to_vec(),
            index_checksum: index_checksum.as_bytes().to_vec(),
            object_count: u32::try_from(index.len())
                .map_err(|_| ExternalPackPromotionError::MetadataLimit)?,
            size_bytes: object.size_bytes,
            storage_order: 0,
        },
        data: Vec::new(),
        index,
    };
    backend
        .sql()
        .install_promoted_push_pack(PgPackPromotionStage {
            tenant: prepared.tenant().clone(),
            repository: prepared.repository().clone(),
            quarantine_id,
            quarantine_generation,
            repository_generation: prepared.repository_generation(),
            prepared_fingerprint: prepared.fingerprint(),
            pack_checksum,
            index_checksum,
            pack,
            storage: PgPackPromotionStorage::External {
                backend_name: backend.options().backend_name.clone(),
                storage_key,
                storage_version: object.version.as_bytes().to_vec(),
                storage_checksum: object.content_checksum,
            },
        })
        .await
        .map_err(map_install_error)
}

fn valid_key_prefix(prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    let trimmed = prefix.trim_matches('/');
    !trimmed.is_empty()
        && trimmed.split('/').all(|component| {
            !component.is_empty()
                && component != "."
                && component != ".."
                && component
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        })
}

async fn stream_parts<U, Q, W, C>(
    upload: &mut U,
    quarantine: &mut Q,
    fence: QuarantineFence,
    hash_algo: HashAlgo,
    plan: &ExternalPackUploadPlan,
    limits: &ExternalPackPromotionLimits,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
) -> Result<(), ExternalPackPromotionError>
where
    U: ExternalPackPromotionUpload,
    Q: PackQuarantine,
    W: ExternalPackPromotionWork,
    C: ExternalPackPromotionControl,
{
    let trailer_start = plan
        .size_bytes
        .checked_sub(hash_algo.len() as u64)
        .ok_or(ExternalPackPromotionError::Binding)?;
    let mut body_hasher = PromotionHasher::new(hash_algo);
    let mut trailer = Vec::new();
    trailer
        .try_reserve_exact(hash_algo.len())
        .map_err(|_| ExternalPackPromotionError::Allocation)?;
    let mut offset = 0_u64;
    let mut ordinal = 1_u32;
    while offset < plan.size_bytes {
        let mut parts = Vec::new();
        parts
            .try_reserve_exact(plan.maximum_in_flight_parts)
            .map_err(|_| ExternalPackPromotionError::Allocation)?;
        while parts.len() < plan.maximum_in_flight_parts && offset < plan.size_bytes {
            let remaining = plan
                .size_bytes
                .checked_sub(offset)
                .ok_or(ExternalPackPromotionError::Binding)?;
            let length = usize::try_from(remaining.min(plan.part_bytes as u64))
                .map_err(|_| ExternalPackPromotionError::MetadataLimit)?;
            checkpoint(limits, work, control, last_observed_at, length as u64)?;
            let bytes = quarantine
                .read_range(fence, offset, length)
                .await
                .map_err(|_| ExternalPackPromotionError::Quarantine)?;
            checkpoint(limits, work, control, last_observed_at, 1)?;
            if bytes.len() != length {
                return Err(ExternalPackPromotionError::Quarantine);
            }
            let end = offset
                .checked_add(length as u64)
                .ok_or(ExternalPackPromotionError::Binding)?;
            let hash_end = end.min(trailer_start);
            if hash_end > offset {
                let count = usize::try_from(hash_end - offset)
                    .map_err(|_| ExternalPackPromotionError::Binding)?;
                body_hasher.update(&bytes[..count]);
            }
            if end > trailer_start {
                let start = usize::try_from(trailer_start.saturating_sub(offset))
                    .map_err(|_| ExternalPackPromotionError::Binding)?;
                trailer.extend_from_slice(&bytes[start..]);
            }
            let checksum = PromotionHasher::digest(hash_algo, &bytes)?;
            parts.push(ExternalPackPart {
                ordinal,
                offset,
                checksum,
                bytes,
            });
            offset = end;
            ordinal = ordinal
                .checked_add(1)
                .ok_or(ExternalPackPromotionError::MetadataLimit)?;
        }
        upload
            .upload_parts(parts)
            .await
            .map_err(|_| ExternalPackPromotionError::Provider)?;
        checkpoint(limits, work, control, last_observed_at, 1)?;
    }
    if trailer != plan.content_checksum.as_bytes() || body_hasher.finish()? != plan.content_checksum
    {
        return Err(ExternalPackPromotionError::Checksum);
    }
    Ok(())
}

async fn inspect_exact<P: ExternalPackPromotionStore>(
    provider: &P,
    key: &str,
    plan: &ExternalPackUploadPlan,
) -> Result<Option<ExternalPackObject>, ExternalPackPromotionError> {
    provider
        .inspect_final(key)
        .await
        .map_err(|_| ExternalPackPromotionError::Provider)?
        .map(|object| verify_exact_object(object, plan))
        .transpose()
}

fn verify_exact_object(
    object: ExternalPackObject,
    plan: &ExternalPackUploadPlan,
) -> Result<ExternalPackObject, ExternalPackPromotionError> {
    if object.size_bytes != plan.size_bytes
        || object.content_checksum != plan.content_checksum
        || object.version.as_bytes().is_empty()
        || object.version.as_bytes().len() > MAX_PROVIDER_VERSION_BYTES
    {
        return Err(ExternalPackPromotionError::Collision);
    }
    Ok(object)
}

fn prepare_index(
    validated: &ValidatedPushPack,
) -> Result<Vec<PackObjectIndex>, ExternalPackPromotionError> {
    let mut index = Vec::new();
    index
        .try_reserve_exact(validated.index().len())
        .map_err(|_| ExternalPackPromotionError::Allocation)?;
    let mut oids = HashSet::new();
    oids.try_reserve(validated.index().len())
        .map_err(|_| ExternalPackPromotionError::Allocation)?;
    let mut offsets = HashSet::new();
    offsets
        .try_reserve(validated.index().len())
        .map_err(|_| ExternalPackPromotionError::Allocation)?;
    for row in validated.index() {
        if !oids.insert(row.oid) || !offsets.insert(row.entry.offset) {
            return Err(ExternalPackPromotionError::Binding);
        }
        index.push(PackObjectIndex {
            oid: row.oid,
            kind: row.kind,
            offset: row.entry.offset,
            size: row.resolved_size,
            compressed_size: row.entry.length,
        });
    }
    Ok(index)
}

fn validate_index_memory(rows: usize, maximum: u64) -> Result<(), ExternalPackPromotionError> {
    let per_row = INDEX_ROW_OVERHEAD
        .checked_add(
            u64::try_from(std::mem::size_of::<PackObjectIndex>())
                .map_err(|_| ExternalPackPromotionError::MetadataLimit)?,
        )
        .ok_or(ExternalPackPromotionError::MetadataLimit)?;
    let bytes = u64::try_from(rows)
        .ok()
        .and_then(|rows| rows.checked_mul(per_row))
        .ok_or(ExternalPackPromotionError::MetadataLimit)?;
    if bytes > maximum {
        return Err(ExternalPackPromotionError::MetadataLimit);
    }
    Ok(())
}

fn checkpoint<W, C>(
    limits: &ExternalPackPromotionLimits,
    work: &mut W,
    control: &mut C,
    last_observed_at: &mut OffsetDateTime,
    units: u64,
) -> Result<(), ExternalPackPromotionError>
where
    W: ExternalPackPromotionWork,
    C: ExternalPackPromotionControl,
{
    let observation = control.observe();
    if observation.observed_at < *last_observed_at {
        return Err(ExternalPackPromotionError::Deadline);
    }
    *last_observed_at = observation.observed_at;
    if observation.cancelled {
        return Err(ExternalPackPromotionError::Cancelled);
    }
    if observation.observed_at < limits.started_at || observation.observed_at >= limits.deadline {
        return Err(ExternalPackPromotionError::Deadline);
    }
    if !work.charge(units) {
        return Err(ExternalPackPromotionError::WorkBudget);
    }
    Ok(())
}

fn map_install_error(
    error: crate::protocol::pg_pack_promotion::PgPackPromotionInstallError,
) -> ExternalPackPromotionError {
    match error {
        crate::protocol::pg_pack_promotion::PgPackPromotionInstallError::StaleGeneration => {
            ExternalPackPromotionError::StaleGeneration
        }
        crate::protocol::pg_pack_promotion::PgPackPromotionInstallError::Collision => {
            ExternalPackPromotionError::Collision
        }
        crate::protocol::pg_pack_promotion::PgPackPromotionInstallError::Backend => {
            ExternalPackPromotionError::Backend
        }
    }
}

enum PromotionHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl PromotionHasher {
    fn new(algo: HashAlgo) -> Self {
        match algo {
            HashAlgo::Sha1 => Self::Sha1(Sha1::new()),
            HashAlgo::Sha256 => Self::Sha256(Sha256::new()),
        }
    }
    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hash) => Sha1Digest::update(hash, bytes),
            Self::Sha256(hash) => Sha2Digest::update(hash, bytes),
        }
    }
    fn finish(self) -> Result<ObjectId, ExternalPackPromotionError> {
        match self {
            Self::Sha1(hash) => ObjectId::from_bytes(&hash.finalize()),
            Self::Sha256(hash) => ObjectId::from_bytes(&hash.finalize()),
        }
        .map_err(|_| ExternalPackPromotionError::Checksum)
    }
    fn digest(algo: HashAlgo, bytes: &[u8]) -> Result<ObjectId, ExternalPackPromotionError> {
        let mut hash = Self::new(algo);
        hash.update(bytes);
        hash.finish()
    }
}
