//! Generation-safe lookup and bounded serving of verified canonical clone packs.

use async_trait::async_trait;
use grit_lib::objects::HashAlgo;
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use time::OffsetDateTime;

use crate::protocol::canonical_clone::{
    CanonicalCloneError, CanonicalCloneFlightState, CanonicalCloneGeneration, CanonicalCloneKey,
    CanonicalCloneLease, CanonicalCloneLocator, CanonicalCloneManifest, CanonicalCloneStore,
};
use crate::protocol::pack_stream::{
    CancellationGate, CancellationProbe, PackAbortReason, PackChunkSink, PackStreamError,
    PackStreamLimits,
};
use crate::protocol::upload_pack_wire::{
    UploadPackWireMode, UploadPackWireReport, UploadPackWireSink,
};

/// Default upper bound for one durable cached-pack range read.
pub const DEFAULT_CANONICAL_CLONE_RANGE_BYTES: usize = 64 * 1024;

/// Async random-access boundary for immutable canonical pack bytes.
#[async_trait]
pub trait CanonicalCloneByteSource: Send + Sync {
    /// Backend-specific read failure.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Return the immutable value's complete length, if it exists.
    async fn content_length(
        &self,
        locator: &CanonicalCloneLocator,
    ) -> Result<Option<u64>, Self::Error>;

    /// Read at most `length` bytes beginning at the exact byte offset.
    async fn read_range(
        &self,
        locator: &CanonicalCloneLocator,
        offset: u64,
        length: usize,
    ) -> Result<Option<Vec<u8>>, Self::Error>;
}

/// Exact result of a generation-safe canonical cache lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanonicalCloneLookup {
    /// A verified manifest for the exact requested generation is reusable.
    Hit(CanonicalCloneManifest),
    /// No durable singleflight exists for this key.
    Missing,
    /// Another builder currently owns the exact generation.
    Building(CanonicalCloneLease),
    /// State exists but is failed or idle and may be claimed by a builder.
    Claimable,
    /// State or manifest belongs to another generation or is invalidated/ineligible.
    Ineligible,
}

/// Failure while loading a canonical cache state.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalCloneLookupError<E: std::error::Error + 'static> {
    /// The store adapter rejected the read.
    #[error("canonical clone state lookup failed")]
    Store(#[source] E),
    /// Persisted canonical state violated model invariants.
    #[error(transparent)]
    Model(#[from] CanonicalCloneError),
}

/// Load a canonical state without ever serving bytes across a generation boundary.
///
/// # Errors
///
/// Returns typed store or model validation failures. A mismatched durable generation is reported
/// as [`CanonicalCloneLookup::Ineligible`] rather than falling through to a stale hit.
pub fn lookup_canonical_clone<C: CanonicalCloneStore>(
    store: &C,
    key: CanonicalCloneKey,
    generation: CanonicalCloneGeneration,
    observed_at: OffsetDateTime,
) -> Result<CanonicalCloneLookup, CanonicalCloneLookupError<C::Error>> {
    let Some(state) = store.load(&key).map_err(CanonicalCloneLookupError::Store)? else {
        return Ok(CanonicalCloneLookup::Missing);
    };
    if state.key() != key || state.generation() != generation {
        return Ok(CanonicalCloneLookup::Ineligible);
    }
    if state
        .last_observed_at()
        .is_some_and(|previous| observed_at < previous)
    {
        return Err(CanonicalCloneError::NonMonotonicTime.into());
    }
    match state.state() {
        CanonicalCloneFlightState::Published(manifest) => {
            manifest.validate()?;
            if manifest.key != key || manifest.generation != generation || !manifest.is_eligible() {
                return Ok(CanonicalCloneLookup::Ineligible);
            }
            Ok(CanonicalCloneLookup::Hit(manifest.clone()))
        }
        CanonicalCloneFlightState::Building(lease) if observed_at < lease.expires_at => {
            Ok(CanonicalCloneLookup::Building(lease.clone()))
        }
        CanonicalCloneFlightState::Building(_) => Ok(CanonicalCloneLookup::Claimable),
        CanonicalCloneFlightState::Idle { .. } | CanonicalCloneFlightState::Failed { .. } => {
            Ok(CanonicalCloneLookup::Claimable)
        }
        CanonicalCloneFlightState::Invalidated { .. } => Ok(CanonicalCloneLookup::Ineligible),
    }
}

/// Bounded cached-pack serving configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalCloneServeOptions {
    /// Maximum bytes requested from the durable byte source at once.
    pub range_bytes: usize,
    /// Logical and physical upload-pack response limits.
    pub stream_limits: PackStreamLimits,
}

impl CanonicalCloneServeOptions {
    fn validate(self) -> Result<Self, PackStreamError> {
        let stream_limits = self.stream_limits.validate()?;
        if self.range_bytes == 0 || self.range_bytes > stream_limits.max_chunk_bytes {
            return Err(PackStreamError::InvalidLimits);
        }
        Ok(Self {
            range_bytes: self.range_bytes,
            stream_limits,
        })
    }
}

impl Default for CanonicalCloneServeOptions {
    fn default() -> Self {
        Self {
            range_bytes: DEFAULT_CANONICAL_CLONE_RANGE_BYTES,
            stream_limits: PackStreamLimits::default(),
        }
    }
}

/// Successful bounded two-pass cached response counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanonicalCloneServeReport {
    /// Bytes verified before any response output.
    pub verified_bytes: u64,
    /// Number of bounded verification reads.
    pub verification_reads: u64,
    /// Number of bounded serving reads.
    pub serving_reads: u64,
    /// Logical and physical upload-pack counters.
    pub wire: UploadPackWireReport,
}

/// Immutable byte-source corruption or inconsistency.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalCloneDataError<E: std::error::Error + 'static> {
    /// Byte storage rejected a metadata or range read.
    #[error("canonical clone byte read failed")]
    Source(#[source] E),
    /// Manifest or request generation is not eligible for this response.
    #[error("canonical clone manifest is stale or ineligible")]
    Ineligible,
    /// The immutable payload disappeared.
    #[error("canonical clone bytes are missing")]
    Missing,
    /// Durable length differs from the verified manifest.
    #[error("canonical clone byte length differs from manifest")]
    LengthMismatch,
    /// A range read returned a short, empty, or oversized result.
    #[error("canonical clone range read violated its exact bound")]
    InvalidRange,
    /// The PACK header is malformed or describes a different object count.
    #[error("canonical clone PACK header differs from manifest")]
    InvalidHeader,
    /// The PACK body digest or trailing checksum differs from the manifest.
    #[error("canonical clone PACK checksum differs from manifest")]
    ChecksumMismatch,
    /// Checked byte accounting overflowed.
    #[error("canonical clone byte accounting overflow")]
    ByteOverflow,
}

/// Cached serving failure before or after upload-pack output begins.
#[derive(Debug, thiserror::Error)]
pub enum CanonicalCloneServeError<E: std::error::Error + 'static> {
    /// Cache validation failed before the ACK/NAK prelude was emitted.
    #[error(transparent)]
    Data(#[from] CanonicalCloneDataError<E>),
    /// Stream validation or output failed.
    #[error(transparent)]
    Stream(#[from] PackStreamError),
}

struct VerifiedPack {
    bytes: u64,
    reads: u64,
}

/// Verify a cached raw PACK completely, then reread it in bounded chunks through upload-pack
/// framing. No response byte is written until the first pass validates length, header, object
/// count, body digest, and trailing checksum.
///
/// # Errors
///
/// Returns typed manifest/source corruption or bounded stream failures. A post-prelude source
/// failure terminally aborts the wire sink before it is returned.
pub async fn serve_canonical_clone<B, P>(
    source: &B,
    manifest: &CanonicalCloneManifest,
    generation: CanonicalCloneGeneration,
    mode: UploadPackWireMode,
    downstream: &mut dyn PackChunkSink,
    options: CanonicalCloneServeOptions,
    cancellation: P,
) -> Result<CanonicalCloneServeReport, CanonicalCloneServeError<B::Error>>
where
    B: CanonicalCloneByteSource,
    P: CancellationProbe,
{
    manifest
        .validate()
        .map_err(|_| CanonicalCloneDataError::Ineligible)?;
    if manifest.generation != generation || !manifest.is_eligible() {
        return Err(CanonicalCloneDataError::Ineligible.into());
    }
    let options = options.validate()?;
    let mut gate = CancellationGate::new(cancellation, options.stream_limits)?;
    gate.check_now()?;
    let source_length = source
        .content_length(&manifest.locator)
        .await
        .map_err(CanonicalCloneDataError::Source)?
        .ok_or(CanonicalCloneDataError::Missing)?;
    if source_length != manifest.size_bytes {
        return Err(CanonicalCloneDataError::LengthMismatch.into());
    }
    let object_count = usize::try_from(manifest.object_count)
        .map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
    UploadPackWireSink::validate_response(
        None,
        mode,
        options.stream_limits,
        manifest.hash_algo.len(),
        object_count,
    )?;
    let serve_chunk = options
        .range_bytes
        .min(mode.max_pack_chunk_bytes(options.stream_limits.max_chunk_bytes));
    if serve_chunk == 0 {
        return Err(PackStreamError::InvalidLimits.into());
    }
    validate_wire_budget(
        manifest.size_bytes,
        mode,
        serve_chunk,
        options.stream_limits,
    )?;
    let verified = verify_pack(source, manifest, options.range_bytes, &mut gate).await?;

    gate.check_now()?;
    let mut wire = UploadPackWireSink::new(downstream, mode, options.stream_limits)?;
    wire.write_prelude(None).await?;
    let mut serving_verifier = PackVerifier::new(manifest)?;
    let mut offset = 0_u64;
    let mut serving_reads = 0_u64;
    while offset < manifest.size_bytes {
        let length = bounded_length(manifest.size_bytes, offset, serve_chunk)?;
        if let Err(error) = gate.check_now() {
            let _ = wire.abort(PackAbortReason::Cancelled).await;
            return Err(error.into());
        }
        let bytes = match source.read_range(&manifest.locator, offset, length).await {
            Ok(Some(bytes)) if bytes.len() == length => bytes,
            Ok(Some(_)) => {
                let _ = wire.abort(PackAbortReason::BackendFailure).await;
                return Err(CanonicalCloneDataError::InvalidRange.into());
            }
            Ok(None) => {
                let _ = wire.abort(PackAbortReason::BackendFailure).await;
                return Err(CanonicalCloneDataError::Missing.into());
            }
            Err(error) => {
                let _ = wire.abort(PackAbortReason::BackendFailure).await;
                return Err(CanonicalCloneDataError::Source(error).into());
            }
        };
        if let Err(error) = gate.check_now() {
            let _ = wire.abort(PackAbortReason::Cancelled).await;
            return Err(error.into());
        }
        if let Err(error) = gate.observe_bytes(
            u64::try_from(length).map_err(|_| CanonicalCloneDataError::ByteOverflow)?,
        ) {
            let _ = wire.abort(PackAbortReason::Cancelled).await;
            return Err(error.into());
        }
        if let Err(error) = serving_verifier.update(&bytes) {
            let _ = wire.abort(PackAbortReason::BackendFailure).await;
            return Err(error.into());
        }
        if let Err(error) = wire.write_chunk(&bytes).await {
            return Err(error.into());
        }
        offset = offset
            .checked_add(u64::try_from(length).map_err(|_| CanonicalCloneDataError::ByteOverflow)?)
            .ok_or(CanonicalCloneDataError::ByteOverflow)?;
        serving_reads = serving_reads
            .checked_add(1)
            .ok_or(CanonicalCloneDataError::ByteOverflow)?;
    }
    if let Err(error) = serving_verifier.finish(manifest) {
        let _ = wire.abort(PackAbortReason::BackendFailure).await;
        return Err(error.into());
    }
    wire.finish().await?;
    Ok(CanonicalCloneServeReport {
        verified_bytes: verified.bytes,
        verification_reads: verified.reads,
        serving_reads,
        wire: wire.wire_report(),
    })
}

async fn verify_pack<B, P>(
    source: &B,
    manifest: &CanonicalCloneManifest,
    range_bytes: usize,
    gate: &mut CancellationGate<P>,
) -> Result<VerifiedPack, CanonicalCloneServeError<B::Error>>
where
    B: CanonicalCloneByteSource,
    P: CancellationProbe,
{
    let mut verifier = PackVerifier::new(manifest)?;
    let mut offset = 0_u64;
    let mut reads = 0_u64;
    while offset < manifest.size_bytes {
        let length = bounded_length(manifest.size_bytes, offset, range_bytes)?;
        gate.check_now()?;
        let bytes = source
            .read_range(&manifest.locator, offset, length)
            .await
            .map_err(CanonicalCloneDataError::Source)?
            .ok_or(CanonicalCloneDataError::Missing)?;
        if bytes.len() != length {
            return Err(CanonicalCloneDataError::InvalidRange.into());
        }
        gate.check_now()?;
        let observed = u64::try_from(length).map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
        gate.observe_bytes(observed)?;
        verifier.update(&bytes)?;
        offset = offset
            .checked_add(observed)
            .ok_or(CanonicalCloneDataError::ByteOverflow)?;
        reads = reads
            .checked_add(1)
            .ok_or(CanonicalCloneDataError::ByteOverflow)?;
    }
    verifier.finish(manifest)?;
    Ok(VerifiedPack {
        bytes: offset,
        reads,
    })
}

struct PackVerifier {
    hash_algo: HashAlgo,
    total_bytes: u64,
    trailer_start: u64,
    offset: u64,
    header: [u8; 12],
    trailer: Vec<u8>,
    sha1: Sha1,
    sha256: Sha256,
}

impl PackVerifier {
    fn new<E: std::error::Error + 'static>(
        manifest: &CanonicalCloneManifest,
    ) -> Result<Self, CanonicalCloneDataError<E>> {
        let trailer_len = manifest.hash_algo.len();
        let trailer_start = manifest
            .size_bytes
            .checked_sub(
                u64::try_from(trailer_len).map_err(|_| CanonicalCloneDataError::ByteOverflow)?,
            )
            .ok_or(CanonicalCloneDataError::InvalidHeader)?;
        if trailer_start < 12 {
            return Err(CanonicalCloneDataError::InvalidHeader);
        }
        let mut trailer = Vec::new();
        trailer
            .try_reserve_exact(trailer_len)
            .map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
        Ok(Self {
            hash_algo: manifest.hash_algo,
            total_bytes: manifest.size_bytes,
            trailer_start,
            offset: 0,
            header: [0; 12],
            trailer,
            sha1: Sha1::new(),
            sha256: Sha256::new(),
        })
    }

    fn update<E: std::error::Error + 'static>(
        &mut self,
        bytes: &[u8],
    ) -> Result<(), CanonicalCloneDataError<E>> {
        if bytes.is_empty() {
            return Err(CanonicalCloneDataError::InvalidRange);
        }
        let length =
            u64::try_from(bytes.len()).map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CanonicalCloneDataError::ByteOverflow)?;
        if end > self.total_bytes {
            return Err(CanonicalCloneDataError::InvalidRange);
        }

        if self.offset < 12 {
            let header_start =
                usize::try_from(self.offset).map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
            let header_bytes = (12 - header_start).min(bytes.len());
            self.header[header_start..header_start + header_bytes]
                .copy_from_slice(&bytes[..header_bytes]);
        }

        if self.offset < self.trailer_start {
            let hashed_end = end.min(self.trailer_start);
            let hashed_len = usize::try_from(hashed_end - self.offset)
                .map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
            match self.hash_algo {
                HashAlgo::Sha1 => self.sha1.update(&bytes[..hashed_len]),
                HashAlgo::Sha256 => self.sha256.update(&bytes[..hashed_len]),
            }
        }
        if end > self.trailer_start {
            let trailer_offset = self.offset.max(self.trailer_start) - self.offset;
            let trailer_offset = usize::try_from(trailer_offset)
                .map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
            self.trailer.extend_from_slice(&bytes[trailer_offset..]);
        }
        self.offset = end;
        Ok(())
    }

    fn finish<E: std::error::Error + 'static>(
        self,
        manifest: &CanonicalCloneManifest,
    ) -> Result<(), CanonicalCloneDataError<E>> {
        if self.offset != self.total_bytes {
            return Err(CanonicalCloneDataError::LengthMismatch);
        }
        let count = u32::from_be_bytes([
            self.header[8],
            self.header[9],
            self.header[10],
            self.header[11],
        ]);
        if &self.header[..4] != b"PACK"
            || u32::from_be_bytes([
                self.header[4],
                self.header[5],
                self.header[6],
                self.header[7],
            ]) != 2
            || count != manifest.object_count
        {
            return Err(CanonicalCloneDataError::InvalidHeader);
        }
        let checksum_matches = match self.hash_algo {
            HashAlgo::Sha1 => {
                let digest = self.sha1.finalize();
                digest.as_slice() == self.trailer
                    && digest.as_slice() == manifest.pack_checksum.as_bytes()
            }
            HashAlgo::Sha256 => {
                let digest = self.sha256.finalize();
                digest.as_slice() == self.trailer
                    && digest.as_slice() == manifest.pack_checksum.as_bytes()
            }
        };
        if !checksum_matches {
            return Err(CanonicalCloneDataError::ChecksumMismatch);
        }
        Ok(())
    }
}

fn validate_wire_budget(
    pack_bytes: u64,
    mode: UploadPackWireMode,
    chunk_bytes: usize,
    limits: PackStreamLimits,
) -> Result<(), PackStreamError> {
    let chunk_bytes = u64::try_from(chunk_bytes).map_err(|_| PackStreamError::ByteOverflow)?;
    if chunk_bytes == 0 {
        return Err(PackStreamError::InvalidLimits);
    }
    let chunks = pack_bytes / chunk_bytes + u64::from(pack_bytes % chunk_bytes != 0);
    let framing = if mode == UploadPackWireMode::Raw {
        0
    } else {
        chunks
            .checked_mul(5)
            .and_then(|bytes| bytes.checked_add(4))
            .ok_or(PackStreamError::ByteOverflow)?
    };
    let physical = pack_bytes
        .checked_add(8) // canonical no-haves responses always emit "NAK\n" as one pkt-line
        .and_then(|bytes| bytes.checked_add(framing))
        .ok_or(PackStreamError::ByteOverflow)?;
    if physical > limits.max_total_bytes {
        return Err(PackStreamError::TotalLimit);
    }
    Ok(())
}

fn bounded_length<E: std::error::Error + 'static>(
    total: u64,
    offset: u64,
    maximum: usize,
) -> Result<usize, CanonicalCloneDataError<E>> {
    let remaining = total
        .checked_sub(offset)
        .ok_or(CanonicalCloneDataError::ByteOverflow)?;
    let maximum = u64::try_from(maximum).map_err(|_| CanonicalCloneDataError::ByteOverflow)?;
    usize::try_from(remaining.min(maximum)).map_err(|_| CanonicalCloneDataError::ByteOverflow)
}
