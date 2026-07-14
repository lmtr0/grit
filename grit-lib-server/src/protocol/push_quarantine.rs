//! Tenant-scoped, fenced receive-pack quarantine lifecycle.
//!
//! Quarantine bytes are private to this abstraction until `promote` commits a validated manifest.
//! Identifiers and ownership tokens are caller-generated opaque randomness; this module performs
//! no random generation, clock reads, path derivation, or ordinary object-store writes.

use std::collections::VecDeque;
use std::fmt;

use async_trait::async_trait;
use grit_lib::objects::{HashAlgo, ObjectId};
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use time::OffsetDateTime;

use crate::ids::{RepositoryId, TenantId};
use crate::protocol::push_metrics::ReceivePackLimits;
use crate::protocol::receive_pack_stream::{
    ReceivePackAbortReason, ReceivePackChunkSink, ReceivePackChunkSinkError,
    ReceivePackStreamReport,
};

const OPAQUE_TOKEN_BYTES: usize = 32;

/// Opaque server-issued quarantine identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuarantineId([u8; OPAQUE_TOKEN_BYTES]);

impl QuarantineId {
    /// Construct an ID from caller-supplied secure random bytes.
    ///
    /// # Errors
    ///
    /// Rejects the all-zero sentinel.
    pub fn from_random_bytes(bytes: [u8; OPAQUE_TOKEN_BYTES]) -> Result<Self, QuarantineError> {
        validate_opaque_bytes(&bytes)?;
        Ok(Self(bytes))
    }

    /// Return opaque bytes for durable metadata serialization.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; OPAQUE_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for QuarantineId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QuarantineId([opaque])")
    }
}

/// Secret capability proving quarantine ownership.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct QuarantineOwnerToken([u8; OPAQUE_TOKEN_BYTES]);

impl QuarantineOwnerToken {
    /// Construct an owner token from caller-supplied secure random bytes.
    ///
    /// # Errors
    ///
    /// Rejects the all-zero sentinel.
    pub fn from_random_bytes(bytes: [u8; OPAQUE_TOKEN_BYTES]) -> Result<Self, QuarantineError> {
        validate_opaque_bytes(&bytes)?;
        Ok(Self(bytes))
    }

    fn matches(&self, other: &Self) -> bool {
        self.0
            .iter()
            .zip(other.0.iter())
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            })
            == 0
    }
}

impl fmt::Debug for QuarantineOwnerToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QuarantineOwnerToken([redacted])")
    }
}

/// Opaque server-issued promotion target identity.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuarantinePromotionId([u8; OPAQUE_TOKEN_BYTES]);

impl QuarantinePromotionId {
    /// Construct a promotion identity from caller-supplied secure random bytes.
    ///
    /// # Errors
    ///
    /// Rejects the all-zero sentinel.
    pub fn from_random_bytes(bytes: [u8; OPAQUE_TOKEN_BYTES]) -> Result<Self, QuarantineError> {
        validate_opaque_bytes(&bytes)?;
        Ok(Self(bytes))
    }

    /// Return opaque bytes for durable metadata serialization.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; OPAQUE_TOKEN_BYTES] {
        &self.0
    }
}

impl fmt::Debug for QuarantinePromotionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("QuarantinePromotionId([opaque])")
    }
}

/// Current fenced capability for one scoped quarantine.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct QuarantineFence {
    id: QuarantineId,
    owner: QuarantineOwnerToken,
    generation: u64,
}

impl QuarantineFence {
    /// Return the quarantine identity.
    #[must_use]
    pub const fn id(&self) -> QuarantineId {
        self.id
    }

    /// Return the monotonically increasing fencing generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

impl fmt::Debug for QuarantineFence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuarantineFence")
            .field("id", &self.id)
            .field("owner", &"[redacted]")
            .field("generation", &self.generation)
            .finish()
    }
}

/// Immutable tenant/repository quarantine ownership scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineScope {
    /// Owning tenant.
    pub tenant: TenantId,
    /// Owning repository.
    pub repository: RepositoryId,
    /// Opaque server-issued quarantine ID.
    pub id: QuarantineId,
    owner: QuarantineOwnerToken,
    /// Caller-supplied creation time.
    pub created_at: OffsetDateTime,
    /// Caller-supplied expiration time.
    pub expires_at: OffsetDateTime,
}

impl QuarantineScope {
    /// Construct a scoped quarantine without reading time or randomness.
    ///
    /// # Errors
    ///
    /// Rejects expiration at or before creation.
    pub fn new(
        tenant: TenantId,
        repository: RepositoryId,
        id: QuarantineId,
        owner: QuarantineOwnerToken,
        created_at: OffsetDateTime,
        expires_at: OffsetDateTime,
    ) -> Result<Self, QuarantineError> {
        if expires_at <= created_at {
            return Err(QuarantineError::InvalidExpiration);
        }
        if id.0 == owner.0 {
            return Err(QuarantineError::InvalidIdentity);
        }
        Ok(Self {
            tenant,
            repository,
            id,
            owner,
            created_at,
            expires_at,
        })
    }

    /// Return the initial generation-one ownership fence.
    #[must_use]
    pub const fn initial_fence(&self) -> QuarantineFence {
        QuarantineFence {
            id: self.id,
            owner: self.owner,
            generation: 1,
        }
    }
}

/// Durable quarantine lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineState {
    /// Bounded append and validation reads are allowed.
    Receiving,
    /// Size, checksum, and immutable manifest were validated.
    Validated,
    /// Atomic promotion is pending or retrying.
    Promoting,
    /// Promotion committed and the manifest may be made visible.
    Committed,
    /// Push rejection made bytes eligible for cleanup.
    Rejected,
    /// Explicit observed expiration made bytes eligible for cleanup.
    Expired,
}

/// Operation used by typed state-transition errors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineOperation {
    /// Append bytes.
    Append,
    /// Read a validation range.
    ReadRange,
    /// Finalize size/checksum/manifest validation.
    Finish,
    /// Promote validated bytes.
    Promote,
    /// Discard unpublished bytes.
    Discard,
}

/// Immutable validated PACK manifest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineManifest {
    /// Repository PACK hash algorithm.
    pub hash_algo: HashAlgo,
    /// Complete PACK bytes including trailer.
    pub pack_bytes: u64,
    /// Declared PACK object count.
    pub object_count: u32,
    /// PACK body checksum and trailer, or `None` for an empty no-pack request.
    pub pack_checksum: Option<ObjectId>,
    /// Prepared structural/index metadata bytes retained outside the PACK.
    pub metadata_bytes: u64,
    /// Whether later thin-pack validation proved the representation self-contained.
    pub self_contained: bool,
}

/// Point-in-time crash/retry metadata excluding the owner secret.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantineSnapshot {
    /// Tenant/repository ownership scope.
    pub tenant: TenantId,
    /// Tenant/repository ownership scope.
    pub repository: RepositoryId,
    /// Opaque quarantine ID.
    pub id: QuarantineId,
    /// Current lifecycle state.
    pub state: QuarantineState,
    /// Current fencing generation.
    pub generation: u64,
    /// Stored compressed bytes.
    pub bytes: u64,
    /// Immutable validated manifest when available.
    pub manifest: Option<QuarantineManifest>,
    /// Atomic promotion identity when promotion began.
    pub promotion: Option<QuarantinePromotionId>,
    /// Cleanup failed and should be retried idempotently.
    pub cleanup_pending: bool,
    /// Caller-supplied creation time.
    pub created_at: OffsetDateTime,
    /// Caller-supplied expiration time.
    pub expires_at: OffsetDateTime,
}

/// Successful idempotent promotion receipt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinePromotion {
    /// Current committed fence.
    pub fence: QuarantineFence,
    /// Atomic server-issued promotion target.
    pub promotion: QuarantinePromotionId,
    /// Committed immutable manifest.
    pub manifest: QuarantineManifest,
}

/// Explicit discard terminal category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineDiscard {
    /// Push was rejected before visibility.
    Rejected,
    /// Caller observed expiry at the supplied time.
    Expired {
        /// Explicit time used for the expiration decision.
        observed_at: OffsetDateTime,
    },
}

/// Discard/cleanup result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QuarantineDiscardReport {
    /// Fence after the terminal state transition.
    pub fence: QuarantineFence,
    /// Bytes eligible for cleanup when discard began.
    pub discarded_bytes: u64,
    /// Whether backing cleanup must be retried.
    pub cleanup_pending: bool,
}

/// Memory quarantine and append/range bounds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryQuarantineLimits {
    /// Maximum complete compressed PACK bytes.
    pub max_bytes: u64,
    /// In-memory bytes after which appends spill to private scratch backing.
    pub memory_threshold: usize,
    /// Maximum bytes accepted by one append call.
    pub max_append_bytes: usize,
    /// Maximum bytes returned by one validation range call.
    pub max_range_bytes: usize,
}

impl MemoryQuarantineLimits {
    /// Validate bounded memory and operation sizes.
    ///
    /// # Errors
    ///
    /// Returns [`QuarantineError::InvalidLimits`] for zero, inconsistent, or hard-maximum values.
    pub fn validate(self) -> Result<Self, QuarantineError> {
        let hard_bytes = ReceivePackLimits::HARD_MAX.max_pack_bytes;
        let memory =
            u64::try_from(self.memory_threshold).map_err(|_| QuarantineError::InvalidLimits)?;
        let append =
            u64::try_from(self.max_append_bytes).map_err(|_| QuarantineError::InvalidLimits)?;
        let range =
            u64::try_from(self.max_range_bytes).map_err(|_| QuarantineError::InvalidLimits)?;
        if self.max_bytes == 0
            || self.max_bytes > hard_bytes
            || self.memory_threshold == 0
            || self.max_append_bytes == 0
            || self.max_range_bytes == 0
            || memory > self.max_bytes
            || append > self.max_bytes
            || range > self.max_bytes
        {
            return Err(QuarantineError::InvalidLimits);
        }
        Ok(self)
    }
}

impl Default for MemoryQuarantineLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64_u64 << 30,
            memory_threshold: 8 << 20,
            max_append_bytes: 1 << 20,
            max_range_bytes: 4 << 20,
        }
    }
}

/// Typed quarantine failure without paths, client strings, or credentials.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum QuarantineError {
    /// Opaque identity/token used the reserved all-zero value.
    #[error("invalid quarantine opaque identity")]
    InvalidIdentity,
    /// Limits are zero, inconsistent, or exceed hard ceilings.
    #[error("invalid quarantine limits")]
    InvalidLimits,
    /// Expiration is invalid or has not yet elapsed.
    #[error("invalid quarantine expiration")]
    InvalidExpiration,
    /// Fence ID or owner capability does not match this quarantine.
    #[error("quarantine fence is unauthorized")]
    Unauthorized,
    /// Fence generation is stale or from a future state.
    #[error("quarantine fence generation is stale")]
    StaleFence,
    /// Operation is illegal in the current lifecycle state.
    #[error("quarantine operation {operation:?} is invalid in state {state:?}")]
    InvalidState {
        /// Attempted operation.
        operation: QuarantineOperation,
        /// Current lifecycle state.
        state: QuarantineState,
    },
    /// Empty appends are forbidden as no-progress writes.
    #[error("quarantine append is empty")]
    EmptyAppend,
    /// One append or read range exceeded its operation bound.
    #[error("quarantine operation exceeds its byte bound")]
    OperationLimit,
    /// Complete quarantine bytes exceeded their configured bound.
    #[error("quarantine exceeds its byte bound")]
    SizeLimit,
    /// Append did not start at the current size and was not an identical retry.
    #[error("quarantine append offset mismatch")]
    OffsetMismatch,
    /// Range arithmetic overflowed or exceeded stored bytes.
    #[error("quarantine range is invalid")]
    InvalidRange,
    /// Bounded memory allocation failed.
    #[error("quarantine allocation failed")]
    Allocation,
    /// Spill backing failed without exposing backend details.
    #[error("quarantine scratch backing failed")]
    Scratch,
    /// Final size, algorithm, checksum, or trailer did not match.
    #[error("quarantine checksum or size validation failed")]
    Checksum,
    /// Immutable manifest conflicts with a prior finish or promotion retry.
    #[error("quarantine manifest conflicts with prior state")]
    ManifestMismatch,
    /// No scratch backing was supplied when the memory threshold was crossed.
    #[error("quarantine scratch backing is required")]
    ScratchRequired,
    /// Fencing generation exhausted.
    #[error("quarantine fencing generation exhausted")]
    GenerationOverflow,
}

/// Private spill backing. Implementations must never expose bytes through ordinary object reads.
#[async_trait]
pub trait QuarantineScratch: Send {
    /// Idempotently append at `expected_offset` and return the resulting length.
    ///
    /// Retrying an already written range must compare the bytes and succeed only when identical;
    /// overlapping different bytes and gaps must fail without mutation.
    ///
    /// An error must leave the prior scratch length and bytes unchanged so a spill retry cannot
    /// mistake a partially applied append for durable progress.
    async fn append(&mut self, expected_offset: u64, bytes: &[u8]) -> Result<u64, QuarantineError>;

    /// Read exactly `length` private bytes from a validated range.
    async fn read_range(&mut self, offset: u64, length: usize) -> Result<Vec<u8>, QuarantineError>;

    /// Persist the immutable validated manifest without making bytes ordinarily visible.
    ///
    /// An error must not freeze a different manifest; retrying the same manifest is idempotent.
    async fn finish(&mut self, manifest: &QuarantineManifest) -> Result<(), QuarantineError>;

    /// Atomically establish backing-specific committed visibility for `promotion`.
    ///
    /// Retrying the same promotion and manifest must be harmless. No client-derived key is passed.
    /// An ambiguous failure may already have committed that exact pair, but a retry must discover
    /// it without creating a second visible copy or accepting different bytes/metadata.
    async fn promote(
        &mut self,
        promotion: QuarantinePromotionId,
        manifest: &QuarantineManifest,
    ) -> Result<(), QuarantineError>;

    /// Idempotently remove all unpublished scratch bytes.
    /// A failure must leave cleanup safely retryable.
    async fn discard(&mut self) -> Result<(), QuarantineError>;

    /// Synchronous best-effort cleanup used by [`Drop`].
    fn discard_best_effort(&mut self);
}

/// Async fenced quarantine operations.
#[async_trait]
pub trait PackQuarantine: Send {
    /// Return non-secret crash/retry metadata.
    fn snapshot(&self) -> QuarantineSnapshot;

    /// Return the exact currently retained PACK byte length without cloning snapshot metadata.
    fn received_bytes(&self) -> u64;

    /// Return the current fence after verifying the owner capability.
    fn current_fence(
        &self,
        owner: &QuarantineOwnerToken,
    ) -> Result<QuarantineFence, QuarantineError>;

    /// Append one bounded chunk at an exact expected offset.
    async fn append(
        &mut self,
        fence: QuarantineFence,
        expected_offset: u64,
        bytes: &[u8],
    ) -> Result<u64, QuarantineError>;

    /// Read one exact bounded private range for validation/indexing.
    async fn read_range(
        &mut self,
        fence: QuarantineFence,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, QuarantineError>;

    /// Validate exact size/checksum and freeze an immutable manifest.
    async fn finish(
        &mut self,
        fence: QuarantineFence,
        expected_checksum: Option<ObjectId>,
        manifest: QuarantineManifest,
    ) -> Result<QuarantineFence, QuarantineError>;

    /// Atomically promote a validated manifest using a server-issued target identity.
    async fn promote(
        &mut self,
        fence: QuarantineFence,
        promotion: QuarantinePromotionId,
    ) -> Result<QuarantinePromotion, QuarantineError>;

    /// Idempotently reject or expire unpublished bytes and attempt cleanup.
    async fn discard(
        &mut self,
        fence: QuarantineFence,
        terminal: QuarantineDiscard,
    ) -> Result<QuarantineDiscardReport, QuarantineError>;
}

enum QuarantineStorage {
    Memory(Vec<u8>),
    Scratch(Box<dyn QuarantineScratch>),
}

/// Bounded in-memory quarantine that spills into caller-supplied private scratch backing.
pub struct MemoryPackQuarantine {
    scope: QuarantineScope,
    limits: MemoryQuarantineLimits,
    state: QuarantineState,
    generation: u64,
    bytes: u64,
    storage: QuarantineStorage,
    spare_scratch: Option<Box<dyn QuarantineScratch>>,
    checksum: QuarantineChecksum,
    manifest: Option<QuarantineManifest>,
    promotion: Option<QuarantinePromotionId>,
    cleanup_pending: bool,
}

impl fmt::Debug for MemoryPackQuarantine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MemoryPackQuarantine")
            .field("scope", &self.scope)
            .field("limits", &self.limits)
            .field("state", &self.state)
            .field("generation", &self.generation)
            .field("bytes", &self.bytes)
            .field("manifest", &self.manifest)
            .field("promotion", &self.promotion)
            .field("cleanup_pending", &self.cleanup_pending)
            .finish_non_exhaustive()
    }
}

impl MemoryPackQuarantine {
    /// Construct an empty generation-one quarantine.
    ///
    /// `scratch`, when supplied, must be empty and private to this scope. It is used only after the
    /// memory threshold is crossed. The caller creates both random tokens and all scratch backing.
    ///
    /// # Errors
    ///
    /// Returns invalid limits or an object-format mismatch in future append validation.
    pub fn new(
        scope: QuarantineScope,
        hash_algo: HashAlgo,
        limits: MemoryQuarantineLimits,
        scratch: Option<Box<dyn QuarantineScratch>>,
    ) -> Result<Self, QuarantineError> {
        Ok(Self {
            scope,
            limits: limits.validate()?,
            state: QuarantineState::Receiving,
            generation: 1,
            bytes: 0,
            storage: QuarantineStorage::Memory(Vec::new()),
            spare_scratch: scratch,
            checksum: QuarantineChecksum::new(hash_algo),
            manifest: None,
            promotion: None,
            cleanup_pending: false,
        })
    }

    fn authorize_identity(&self, fence: QuarantineFence) -> Result<(), QuarantineError> {
        if fence.id != self.scope.id || !fence.owner.matches(&self.scope.owner) {
            return Err(QuarantineError::Unauthorized);
        }
        Ok(())
    }

    fn authorize_current(&self, fence: QuarantineFence) -> Result<(), QuarantineError> {
        self.authorize_identity(fence)?;
        if fence.generation != self.generation {
            return Err(QuarantineError::StaleFence);
        }
        Ok(())
    }

    fn next_generation(&self) -> Result<u64, QuarantineError> {
        self.generation
            .checked_add(1)
            .ok_or(QuarantineError::GenerationOverflow)
    }

    fn fence(&self) -> QuarantineFence {
        QuarantineFence {
            id: self.scope.id,
            owner: self.scope.owner,
            generation: self.generation,
        }
    }

    fn validate_manifest(
        &self,
        expected_checksum: Option<ObjectId>,
        manifest: &QuarantineManifest,
    ) -> Result<(), QuarantineError> {
        let empty = self.bytes == 0;
        let minimum_pack_bytes = 12_u64
            .checked_add(
                u64::try_from(self.checksum.hash_algo.len())
                    .map_err(|_| QuarantineError::Checksum)?,
            )
            .ok_or(QuarantineError::Checksum)?;
        let object_limit = u32::try_from(ReceivePackLimits::HARD_MAX.max_objects)
            .map_err(|_| QuarantineError::InvalidLimits)?;
        if manifest.hash_algo != self.checksum.hash_algo
            || manifest.pack_bytes != self.bytes
            || manifest.pack_checksum != expected_checksum
            || manifest.object_count > object_limit
            || manifest.metadata_bytes > ReceivePackLimits::HARD_MAX.max_metadata_memory_bytes
            || (empty && (expected_checksum.is_some() || manifest.object_count != 0))
            || (!empty
                && (self.bytes < minimum_pack_bytes
                    || expected_checksum
                        .is_none_or(|checksum| checksum.algo() != self.checksum.hash_algo)))
        {
            return Err(QuarantineError::Checksum);
        }
        Ok(())
    }

    async fn append_storage(&mut self, bytes: &[u8]) -> Result<(), QuarantineError> {
        let next = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).map_err(|_| QuarantineError::SizeLimit)?)
            .ok_or(QuarantineError::SizeLimit)?;
        let threshold = u64::try_from(self.limits.memory_threshold)
            .map_err(|_| QuarantineError::InvalidLimits)?;
        if matches!(self.storage, QuarantineStorage::Memory(_)) && next > threshold {
            self.spill_and_append(bytes).await?;
            return Ok(());
        }
        match &mut self.storage {
            QuarantineStorage::Memory(memory) => {
                memory
                    .try_reserve(bytes.len())
                    .map_err(|_| QuarantineError::Allocation)?;
                memory.extend_from_slice(bytes);
                Ok(())
            }
            QuarantineStorage::Scratch(scratch) => {
                let length = scratch.append(self.bytes, bytes).await?;
                if length != next {
                    return Err(QuarantineError::Scratch);
                }
                Ok(())
            }
        }
    }

    async fn spill_and_append(&mut self, bytes: &[u8]) -> Result<(), QuarantineError> {
        let existing = match &self.storage {
            QuarantineStorage::Memory(memory) => memory,
            QuarantineStorage::Scratch(_) => return Err(QuarantineError::Scratch),
        };
        let Some(scratch) = self.spare_scratch.as_mut() else {
            return Err(QuarantineError::ScratchRequired);
        };
        let existing_length = scratch.append(0, existing).await?;
        if existing_length != self.bytes {
            scratch.discard_best_effort();
            self.spare_scratch = None;
            return Err(QuarantineError::Scratch);
        }
        let next = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).map_err(|_| QuarantineError::SizeLimit)?)
            .ok_or(QuarantineError::SizeLimit)?;
        let appended = scratch.append(self.bytes, bytes).await?;
        if appended != next {
            scratch.discard_best_effort();
            self.spare_scratch = None;
            return Err(QuarantineError::Scratch);
        }
        let Some(scratch) = self.spare_scratch.take() else {
            return Err(QuarantineError::Scratch);
        };
        self.storage = QuarantineStorage::Scratch(scratch);
        Ok(())
    }

    async fn storage_range(
        &mut self,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, QuarantineError> {
        match &mut self.storage {
            QuarantineStorage::Memory(memory) => {
                let start = usize::try_from(offset).map_err(|_| QuarantineError::InvalidRange)?;
                let end = start
                    .checked_add(length)
                    .ok_or(QuarantineError::InvalidRange)?;
                let Some(slice) = memory.get(start..end) else {
                    return Err(QuarantineError::InvalidRange);
                };
                let mut result = Vec::new();
                result
                    .try_reserve(length)
                    .map_err(|_| QuarantineError::Allocation)?;
                result.extend_from_slice(slice);
                Ok(result)
            }
            QuarantineStorage::Scratch(scratch) => {
                let bytes = scratch.read_range(offset, length).await?;
                if bytes.len() != length {
                    return Err(QuarantineError::Scratch);
                }
                Ok(bytes)
            }
        }
    }

    async fn validate_pack_header(
        &mut self,
        manifest: &QuarantineManifest,
    ) -> Result<(), QuarantineError> {
        if self.bytes == 0 {
            return Ok(());
        }
        let header = self.storage_range(0, 12).await?;
        if header.get(..4) != Some(b"PACK".as_slice()) {
            return Err(QuarantineError::Checksum);
        }
        let version = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        let objects = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);
        if !matches!(version, 2 | 3) || objects != manifest.object_count {
            return Err(QuarantineError::Checksum);
        }
        Ok(())
    }

    async fn cleanup_storage(&mut self) -> Result<(), QuarantineError> {
        match &mut self.storage {
            QuarantineStorage::Memory(memory) => {
                *memory = Vec::new();
                Ok(())
            }
            QuarantineStorage::Scratch(scratch) => scratch.discard().await,
        }
    }
}

#[async_trait]
impl PackQuarantine for MemoryPackQuarantine {
    fn snapshot(&self) -> QuarantineSnapshot {
        QuarantineSnapshot {
            tenant: self.scope.tenant.clone(),
            repository: self.scope.repository.clone(),
            id: self.scope.id,
            state: self.state,
            generation: self.generation,
            bytes: self.bytes,
            manifest: self.manifest.clone(),
            promotion: self.promotion,
            cleanup_pending: self.cleanup_pending,
            created_at: self.scope.created_at,
            expires_at: self.scope.expires_at,
        }
    }

    fn received_bytes(&self) -> u64 {
        self.bytes
    }

    fn current_fence(
        &self,
        owner: &QuarantineOwnerToken,
    ) -> Result<QuarantineFence, QuarantineError> {
        if !owner.matches(&self.scope.owner) {
            return Err(QuarantineError::Unauthorized);
        }
        Ok(self.fence())
    }

    async fn append(
        &mut self,
        fence: QuarantineFence,
        expected_offset: u64,
        bytes: &[u8],
    ) -> Result<u64, QuarantineError> {
        self.authorize_current(fence)?;
        if self.state != QuarantineState::Receiving {
            return Err(QuarantineError::InvalidState {
                operation: QuarantineOperation::Append,
                state: self.state,
            });
        }
        if bytes.is_empty() {
            return Err(QuarantineError::EmptyAppend);
        }
        if bytes.len() > self.limits.max_append_bytes {
            return Err(QuarantineError::OperationLimit);
        }
        if expected_offset < self.bytes {
            let length = usize::try_from(self.bytes - expected_offset)
                .ok()
                .map(|available| available.min(bytes.len()))
                .ok_or(QuarantineError::OffsetMismatch)?;
            if length != bytes.len()
                || self
                    .storage_range(expected_offset, length)
                    .await?
                    .as_slice()
                    != bytes
            {
                return Err(QuarantineError::OffsetMismatch);
            }
            return Ok(self.bytes);
        }
        if expected_offset != self.bytes {
            return Err(QuarantineError::OffsetMismatch);
        }
        let next = self
            .bytes
            .checked_add(u64::try_from(bytes.len()).map_err(|_| QuarantineError::SizeLimit)?)
            .filter(|size| *size <= self.limits.max_bytes)
            .ok_or(QuarantineError::SizeLimit)?;
        self.append_storage(bytes).await?;
        self.checksum.update(bytes);
        self.bytes = next;
        Ok(self.bytes)
    }

    async fn read_range(
        &mut self,
        fence: QuarantineFence,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, QuarantineError> {
        self.authorize_current(fence)?;
        if matches!(
            self.state,
            QuarantineState::Rejected | QuarantineState::Expired
        ) {
            return Err(QuarantineError::InvalidState {
                operation: QuarantineOperation::ReadRange,
                state: self.state,
            });
        }
        if length > self.limits.max_range_bytes {
            return Err(QuarantineError::OperationLimit);
        }
        let end = offset
            .checked_add(u64::try_from(length).map_err(|_| QuarantineError::InvalidRange)?)
            .filter(|end| *end <= self.bytes)
            .ok_or(QuarantineError::InvalidRange)?;
        if end < offset {
            return Err(QuarantineError::InvalidRange);
        }
        self.storage_range(offset, length).await
    }

    async fn finish(
        &mut self,
        fence: QuarantineFence,
        expected_checksum: Option<ObjectId>,
        manifest: QuarantineManifest,
    ) -> Result<QuarantineFence, QuarantineError> {
        self.authorize_identity(fence)?;
        if self.state == QuarantineState::Validated {
            let retry_generation = fence
                .generation
                .checked_add(1)
                .is_some_and(|generation| generation == self.generation);
            if (fence.generation == self.generation || retry_generation)
                && self.manifest.as_ref() == Some(&manifest)
                && manifest.pack_checksum == expected_checksum
            {
                return Ok(self.fence());
            }
            return Err(QuarantineError::ManifestMismatch);
        }
        self.authorize_current(fence)?;
        if self.state != QuarantineState::Receiving {
            return Err(QuarantineError::InvalidState {
                operation: QuarantineOperation::Finish,
                state: self.state,
            });
        }
        self.validate_manifest(expected_checksum, &manifest)?;
        self.validate_pack_header(&manifest).await?;
        if !self.checksum.matches(expected_checksum)? {
            return Err(QuarantineError::Checksum);
        }
        if let QuarantineStorage::Scratch(scratch) = &mut self.storage {
            scratch.finish(&manifest).await?;
        }
        self.generation = self.next_generation()?;
        self.manifest = Some(manifest);
        self.state = QuarantineState::Validated;
        Ok(self.fence())
    }

    async fn promote(
        &mut self,
        fence: QuarantineFence,
        promotion: QuarantinePromotionId,
    ) -> Result<QuarantinePromotion, QuarantineError> {
        self.authorize_identity(fence)?;
        if promotion.0 == self.scope.id.0 || promotion.0 == self.scope.owner.0 {
            return Err(QuarantineError::InvalidIdentity);
        }
        if self.state == QuarantineState::Committed {
            if self.promotion == Some(promotion)
                && fence.generation <= self.generation
                && self
                    .generation
                    .checked_sub(fence.generation)
                    .is_some_and(|distance| distance <= 2)
            {
                let Some(manifest) = self.manifest.clone() else {
                    return Err(QuarantineError::ManifestMismatch);
                };
                return Ok(QuarantinePromotion {
                    fence: self.fence(),
                    promotion,
                    manifest,
                });
            }
            return Err(QuarantineError::ManifestMismatch);
        }
        if self
            .manifest
            .as_ref()
            .is_none_or(|manifest| !manifest.self_contained)
        {
            return Err(QuarantineError::ManifestMismatch);
        }
        if self.state == QuarantineState::Validated {
            self.authorize_current(fence)?;
            self.generation = self.next_generation()?;
            self.state = QuarantineState::Promoting;
            self.promotion = Some(promotion);
        } else if self.state == QuarantineState::Promoting {
            let retry_generation = fence
                .generation
                .checked_add(1)
                .is_some_and(|generation| generation == self.generation);
            if self.promotion != Some(promotion)
                || (fence.generation != self.generation && !retry_generation)
            {
                return Err(QuarantineError::StaleFence);
            }
        } else {
            return Err(QuarantineError::InvalidState {
                operation: QuarantineOperation::Promote,
                state: self.state,
            });
        }
        let Some(manifest) = self.manifest.clone() else {
            return Err(QuarantineError::ManifestMismatch);
        };
        let committed_generation = self.next_generation()?;
        if let QuarantineStorage::Scratch(scratch) = &mut self.storage {
            scratch.promote(promotion, &manifest).await?;
        }
        self.generation = committed_generation;
        self.state = QuarantineState::Committed;
        Ok(QuarantinePromotion {
            fence: self.fence(),
            promotion,
            manifest,
        })
    }

    async fn discard(
        &mut self,
        fence: QuarantineFence,
        terminal: QuarantineDiscard,
    ) -> Result<QuarantineDiscardReport, QuarantineError> {
        self.authorize_identity(fence)?;
        let target = match terminal {
            QuarantineDiscard::Rejected => QuarantineState::Rejected,
            QuarantineDiscard::Expired { observed_at } => {
                if observed_at < self.scope.expires_at {
                    return Err(QuarantineError::InvalidExpiration);
                }
                QuarantineState::Expired
            }
        };
        if self.state == QuarantineState::Committed {
            return Err(QuarantineError::InvalidState {
                operation: QuarantineOperation::Discard,
                state: self.state,
            });
        }
        if self.state == QuarantineState::Promoting {
            return Err(QuarantineError::InvalidState {
                operation: QuarantineOperation::Discard,
                state: self.state,
            });
        }
        if matches!(
            self.state,
            QuarantineState::Rejected | QuarantineState::Expired
        ) {
            if self.state != target
                || (fence.generation != self.generation
                    && fence
                        .generation
                        .checked_add(1)
                        .is_none_or(|generation| generation != self.generation))
            {
                return Err(QuarantineError::StaleFence);
            }
        } else {
            self.authorize_current(fence)?;
            self.generation = self.next_generation()?;
            self.state = target;
        }
        let discarded_bytes = self.bytes;
        self.cleanup_pending = true;
        match self.cleanup_storage().await {
            Ok(()) => self.cleanup_pending = false,
            Err(_) => {}
        }
        Ok(QuarantineDiscardReport {
            fence: self.fence(),
            discarded_bytes,
            cleanup_pending: self.cleanup_pending,
        })
    }
}

impl Drop for MemoryPackQuarantine {
    fn drop(&mut self) {
        if let Some(scratch) = &mut self.spare_scratch {
            scratch.discard_best_effort();
        }
        if self.state == QuarantineState::Committed {
            return;
        }
        if matches!(
            self.state,
            QuarantineState::Rejected | QuarantineState::Expired
        ) && !self.cleanup_pending
        {
            return;
        }
        match &mut self.storage {
            QuarantineStorage::Memory(memory) => *memory = Vec::new(),
            QuarantineStorage::Scratch(scratch) => scratch.discard_best_effort(),
        }
    }
}

/// Receive-stream sink adapter retaining the typed quarantine error for diagnostics.
pub struct QuarantineReceivePackSink<Q> {
    quarantine: Q,
    fence: QuarantineFence,
    hash_algo: HashAlgo,
    last_error: Option<QuarantineError>,
    terminal: bool,
}

impl<Q> QuarantineReceivePackSink<Q>
where
    Q: PackQuarantine,
{
    /// Bind a current fence and negotiated hash algorithm to a quarantine.
    #[must_use]
    pub fn new(quarantine: Q, fence: QuarantineFence, hash_algo: HashAlgo) -> Self {
        Self {
            quarantine,
            fence,
            hash_algo,
            last_error: None,
            terminal: false,
        }
    }

    /// Return the last typed quarantine failure hidden behind the transport sink error.
    #[must_use]
    pub const fn last_error(&self) -> Option<QuarantineError> {
        self.last_error
    }

    /// Consume the adapter and return quarantine ownership and its latest fence.
    #[must_use]
    pub fn into_parts(self) -> (Q, QuarantineFence, Option<QuarantineError>) {
        (self.quarantine, self.fence, self.last_error)
    }

    fn record_error(&mut self, error: QuarantineError) -> ReceivePackChunkSinkError {
        if self.last_error.is_none() {
            self.last_error = Some(error);
        }
        ReceivePackChunkSinkError
    }
}

#[async_trait]
impl<Q> ReceivePackChunkSink for QuarantineReceivePackSink<Q>
where
    Q: PackQuarantine,
{
    async fn append(&mut self, chunk: &[u8]) -> Result<(), ReceivePackChunkSinkError> {
        if self.terminal {
            return Err(ReceivePackChunkSinkError);
        }
        let offset = self.quarantine.received_bytes();
        if let Err(error) = self.quarantine.append(self.fence, offset, chunk).await {
            return Err(self.record_error(error));
        }
        Ok(())
    }

    async fn finish(
        &mut self,
        report: ReceivePackStreamReport,
    ) -> Result<(), ReceivePackChunkSinkError> {
        if self.terminal {
            return Err(ReceivePackChunkSinkError);
        }
        let manifest = QuarantineManifest {
            hash_algo: self.hash_algo,
            pack_bytes: report.pack_bytes,
            object_count: report.declared_objects,
            pack_checksum: report.pack_checksum,
            metadata_bytes: 0,
            self_contained: false,
        };
        match self
            .quarantine
            .finish(self.fence, report.pack_checksum, manifest)
            .await
        {
            Ok(fence) => {
                self.fence = fence;
                self.terminal = true;
                Ok(())
            }
            Err(error) => Err(self.record_error(error)),
        }
    }

    async fn abort(&mut self, _reason: ReceivePackAbortReason) {
        if self.terminal {
            return;
        }
        match self
            .quarantine
            .discard(self.fence, QuarantineDiscard::Rejected)
            .await
        {
            Ok(report) => self.fence = report.fence,
            Err(error) => {
                let _ = self.record_error(error);
            }
        }
        self.terminal = true;
    }
}

#[derive(Clone)]
enum QuarantineHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl QuarantineHasher {
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

    fn finish(self) -> Vec<u8> {
        match self {
            Self::Sha1(hasher) => hasher.finalize().to_vec(),
            Self::Sha256(hasher) => hasher.finalize().to_vec(),
        }
    }
}

#[derive(Clone)]
struct QuarantineChecksum {
    hash_algo: HashAlgo,
    hasher: QuarantineHasher,
    trailer: VecDeque<u8>,
}

impl QuarantineChecksum {
    fn new(hash_algo: HashAlgo) -> Self {
        Self {
            hash_algo,
            hasher: QuarantineHasher::new(hash_algo),
            trailer: VecDeque::with_capacity(hash_algo.len()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.trailer.push_back(*byte);
            if self.trailer.len() > self.hash_algo.len() {
                if let Some(body) = self.trailer.pop_front() {
                    self.hasher.update(std::slice::from_ref(&body));
                }
            }
        }
    }

    fn matches(&self, expected: Option<ObjectId>) -> Result<bool, QuarantineError> {
        let Some(expected) = expected else {
            return Ok(self.trailer.is_empty());
        };
        if expected.algo() != self.hash_algo || self.trailer.len() != self.hash_algo.len() {
            return Ok(false);
        }
        let digest = self.hasher.clone().finish();
        Ok(digest.as_slice() == expected.as_bytes()
            && self
                .trailer
                .iter()
                .copied()
                .eq(expected.as_bytes().iter().copied()))
    }
}

fn validate_opaque_bytes(bytes: &[u8; OPAQUE_TOKEN_BYTES]) -> Result<(), QuarantineError> {
    if bytes.iter().all(|byte| *byte == 0) {
        return Err(QuarantineError::InvalidIdentity);
    }
    Ok(())
}
