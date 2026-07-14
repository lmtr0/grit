//! Bounded, transport-neutral receive-pack input streaming.
//!
//! Sources and sinks are deliberately runtime agnostic. Each awaited sink append is the
//! backpressure point: the receiver never asks the source for another chunk until the current
//! chunk has been accepted by the quarantine sink. Time, idle state, and cancellation are
//! observations supplied by the caller rather than hidden clock reads.

use std::collections::VecDeque;

use async_trait::async_trait;
use grit_lib::objects::HashAlgo;
use sha1::{Digest as Sha1Digest, Sha1};
use sha2::{Digest as Sha2Digest, Sha256};
use time::{Duration, OffsetDateTime};

use crate::protocol::push_metrics::{
    ReceivePackLimitError, ReceivePackLimitKind, ReceivePackLimits,
};
use crate::protocol::receive_pack::{
    ReceivePackCapability, ReceivePackCommand, ReceivePackRequest,
};

/// Absolute source chunk ceiling accepted by the streaming receiver.
pub const MAX_RECEIVE_PACK_CHUNK_BYTES: usize = 16 << 20;

/// Commands and request metadata separated from the streamed PACK body.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReceivePackEnvelope {
    /// Ref update commands in client order.
    pub commands: Vec<ReceivePackCommand>,
    /// Capabilities requested on the first command line.
    pub capabilities: Vec<ReceivePackCapability>,
    /// Push options in client order, without logging or cache-key normalization.
    pub push_options: Vec<String>,
}

impl ReceivePackEnvelope {
    /// Separate a legacy buffered request into its command envelope and PACK body without copying.
    #[must_use]
    pub fn split_buffered(request: ReceivePackRequest) -> (Self, Vec<u8>) {
        (
            Self {
                commands: request.commands,
                capabilities: request.capabilities,
                push_options: Vec::new(),
            },
            request.pack,
        )
    }

    /// Validate bounded command, capability, and push-option shape before reading a PACK body.
    ///
    /// # Errors
    ///
    /// Returns the first exceeded receive-pack request dimension or invalid limit configuration.
    pub fn validate(&self, limits: ReceivePackLimits) -> Result<(), ReceivePackStreamError> {
        if self
            .commands
            .iter()
            .any(|command| command.refname.is_empty() || contains_framing_control(&command.refname))
            || self
                .push_options
                .iter()
                .any(|option| contains_framing_control(option))
            || self.capabilities.iter().any(capability_has_invalid_value)
            || (!self.push_options.is_empty()
                && !self
                    .capabilities
                    .contains(&ReceivePackCapability::PushOptions))
        {
            return Err(ReceivePackStreamError::InvalidEnvelope);
        }
        let wanted_objects = self
            .commands
            .iter()
            .filter(|command| !command.new_oid.is_zero())
            .count();
        let push_option_bytes = self.push_options.iter().try_fold(0_u64, |total, option| {
            let bytes = u64::try_from(option.len()).map_err(|_| {
                ReceivePackStreamError::Limit(ReceivePackLimitError::Exceeded(
                    ReceivePackLimitKind::PushOptionBytes,
                ))
            })?;
            total
                .checked_add(bytes)
                .ok_or(ReceivePackStreamError::Limit(
                    ReceivePackLimitError::Exceeded(ReceivePackLimitKind::PushOptionBytes),
                ))
        })?;
        limits.validate_request(
            self.estimated_bytes()?,
            self.commands.len(),
            wanted_objects,
            self.capabilities.len(),
            self.push_options.len(),
            push_option_bytes,
            0,
        )?;
        Ok(())
    }

    /// Convert a separated envelope and explicitly collected body into the legacy request shape.
    ///
    /// # Errors
    ///
    /// Returns [`ReceivePackStreamError::UnsupportedPushOptions`] because the legacy shape has no
    /// push-option field, or a pack/request byte limit error.
    pub fn into_buffered_request(
        self,
        pack: Vec<u8>,
        limits: ReceivePackLimits,
    ) -> Result<ReceivePackRequest, ReceivePackStreamError> {
        if !self.push_options.is_empty() {
            return Err(ReceivePackStreamError::UnsupportedPushOptions);
        }
        let request_bytes = self
            .estimated_bytes()?
            .checked_add(pack.len())
            .ok_or(ReceivePackStreamError::RequestLimit)?;
        let wanted_objects = self
            .commands
            .iter()
            .filter(|command| !command.new_oid.is_zero())
            .count();
        limits.validate_request(
            request_bytes,
            self.commands.len(),
            wanted_objects,
            self.capabilities.len(),
            0,
            0,
            pack.len(),
        )?;
        Ok(ReceivePackRequest {
            commands: self.commands,
            capabilities: self.capabilities,
            pack,
        })
    }

    fn estimated_bytes(&self) -> Result<usize, ReceivePackStreamError> {
        let command_bytes = self.commands.iter().try_fold(0_usize, |total, command| {
            total
                .checked_add(command.refname.len())
                .and_then(|value| value.checked_add(command.old_oid.as_bytes().len()))
                .and_then(|value| value.checked_add(command.new_oid.as_bytes().len()))
                .and_then(|value| value.checked_add(7))
                .ok_or(ReceivePackStreamError::RequestLimit)
        })?;
        let capability_bytes =
            self.capabilities
                .iter()
                .try_fold(0_usize, |total, capability| {
                    total
                        .checked_add(capability_wire_len(capability)?)
                        .and_then(|value| value.checked_add(1))
                        .ok_or(ReceivePackStreamError::RequestLimit)
                })?;
        let push_option_bytes = self
            .push_options
            .iter()
            .try_fold(0_usize, |total, option| {
                total
                    .checked_add(option.len())
                    .and_then(|value| value.checked_add(5))
                    .ok_or(ReceivePackStreamError::RequestLimit)
            })?;
        command_bytes
            .checked_add(capability_bytes)
            .and_then(|value| value.checked_add(push_option_bytes))
            .and_then(|value| value.checked_add(8))
            .ok_or(ReceivePackStreamError::RequestLimit)
    }

    fn validate_hash_algo(&self, hash_algo: HashAlgo) -> Result<(), ReceivePackStreamError> {
        let commands_match = self.commands.iter().all(|command| {
            command.old_oid.algo() == hash_algo && command.new_oid.algo() == hash_algo
        });
        let capabilities_match = self.capabilities.iter().all(|capability| {
            !matches!(capability, ReceivePackCapability::ObjectFormat(value) if *value != hash_algo)
        });
        if !commands_match || !capabilities_match {
            return Err(ReceivePackStreamError::InvalidObjectFormat);
        }
        Ok(())
    }
}

/// One source read result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PackSourceChunk {
    /// One nonempty chunk. The receiver validates nonemptiness and configured size.
    Data(Vec<u8>),
    /// Clean end of the PACK body.
    End,
}

/// Non-sensitive source failure classification.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackChunkSourceError {
    /// Client or upstream transport disconnected.
    #[error("receive-pack source disconnected")]
    Disconnected,
    /// Source adapter failed without exposing request or backend details.
    #[error("receive-pack source failed")]
    Failed,
    /// Compatibility source could not allocate a bounded output chunk.
    #[error("receive-pack source allocation failed")]
    Allocation,
}

/// Asynchronous source of bounded PACK body chunks.
#[async_trait]
pub trait PackChunkSource: Send {
    /// Read the next source chunk.
    ///
    /// Implementations must return [`PackSourceChunk::End`] once at EOF. Empty data is rejected by
    /// the receiver as a no-progress source bug.
    async fn read_chunk(&mut self) -> Result<PackSourceChunk, PackChunkSourceError>;
}

/// Borrowed compatibility source over an already bounded byte slice.
#[derive(Debug)]
pub struct BufferedPackSource<'a> {
    bytes: &'a [u8],
    cursor: usize,
    chunk_bytes: usize,
    ended: bool,
}

impl<'a> BufferedPackSource<'a> {
    /// Construct a compatibility source after validating total and per-chunk bounds.
    ///
    /// # Errors
    ///
    /// Rejects zero/oversized chunks or a body exceeding `max_pack_bytes` before any copy.
    pub fn new(
        bytes: &'a [u8],
        chunk_bytes: usize,
        max_pack_bytes: u64,
    ) -> Result<Self, ReceivePackStreamError> {
        let length = u64::try_from(bytes.len()).map_err(|_| ReceivePackStreamError::PackLimit)?;
        if chunk_bytes == 0
            || chunk_bytes > MAX_RECEIVE_PACK_CHUNK_BYTES
            || max_pack_bytes == 0
            || max_pack_bytes > ReceivePackLimits::HARD_MAX.max_pack_bytes
        {
            return Err(ReceivePackStreamError::InvalidLimits);
        }
        if length > max_pack_bytes {
            return Err(ReceivePackStreamError::PackLimit);
        }
        Ok(Self {
            bytes,
            cursor: 0,
            chunk_bytes,
            ended: false,
        })
    }
}

#[async_trait]
impl PackChunkSource for BufferedPackSource<'_> {
    async fn read_chunk(&mut self) -> Result<PackSourceChunk, PackChunkSourceError> {
        if self.ended || self.cursor == self.bytes.len() {
            self.ended = true;
            return Ok(PackSourceChunk::End);
        }
        let end = match self.cursor.checked_add(self.chunk_bytes) {
            Some(end) => end.min(self.bytes.len()),
            None => self.bytes.len(),
        };
        let slice = &self.bytes[self.cursor..end];
        let mut chunk = Vec::new();
        chunk
            .try_reserve(slice.len())
            .map_err(|_| PackChunkSourceError::Allocation)?;
        chunk.extend_from_slice(slice);
        self.cursor = end;
        Ok(PackSourceChunk::Data(chunk))
    }
}

/// Explicit cancellation, deadline, and idle observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceivePackStreamObservation {
    /// Caller-observed monotonic wall time.
    pub observed_at: OffsetDateTime,
    /// Caller-measured idle duration since source progress.
    pub idle_for: Duration,
    /// Explicit cancellation or disconnect signal.
    pub cancelled: bool,
}

/// Caller-owned observation provider; this trait performs no clock reads.
pub trait ReceivePackStreamControl {
    /// Return the latest explicit stream observation.
    fn observe(&mut self) -> ReceivePackStreamObservation;
}

/// Validated stream pump configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceivePackStreamOptions {
    /// Receive-pack request and PACK limits.
    pub limits: ReceivePackLimits,
    /// Largest source chunk accepted by the receiver.
    pub max_chunk_bytes: usize,
    /// Explicit request start time.
    pub started_at: OffsetDateTime,
    /// Explicit request deadline.
    pub deadline: OffsetDateTime,
    /// Maximum caller-observed idle duration.
    pub max_idle: Duration,
}

impl ReceivePackStreamOptions {
    /// Validate stream limits and explicit time relationships.
    ///
    /// # Errors
    ///
    /// Returns invalid limits for zero/inconsistent chunk, idle, deadline, or request duration.
    pub fn validate(self) -> Result<Self, ReceivePackStreamError> {
        let limits = self.limits.validate()?;
        let chunk_bytes = u64::try_from(self.max_chunk_bytes)
            .map_err(|_| ReceivePackStreamError::InvalidLimits)?;
        if self.max_chunk_bytes == 0
            || self.max_chunk_bytes > MAX_RECEIVE_PACK_CHUNK_BYTES
            || chunk_bytes > limits.bytes_in_flight
            || self.max_idle <= Duration::ZERO
            || self.deadline <= self.started_at
            || self.deadline - self.started_at > duration_to_time(limits.max_duration)?
        {
            return Err(ReceivePackStreamError::InvalidLimits);
        }
        Ok(Self { limits, ..self })
    }
}

/// Terminal quarantine sink state transition reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReceivePackAbortReason {
    /// Explicit cancellation or disconnect.
    Cancelled,
    /// Deadline or idle observation elapsed.
    Timeout,
    /// Source adapter failed.
    Source,
    /// Request, chunk, PACK, or checksum validation failed.
    InvalidInput,
    /// Sink failed while accepting or finalizing quarantine bytes.
    Sink,
}

/// Non-sensitive quarantine sink failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("receive-pack quarantine sink failed")]
pub struct ReceivePackChunkSinkError;

/// Backpressure-capable destination for unpublished PACK quarantine bytes.
#[async_trait]
pub trait ReceivePackChunkSink: Send {
    /// Append one validated, nonempty bounded chunk. Awaiting this call is the backpressure point.
    async fn append(&mut self, chunk: &[u8]) -> Result<(), ReceivePackChunkSinkError>;

    /// Finalize the complete structurally validated quarantine body without publishing refs.
    async fn finish(
        &mut self,
        report: ReceivePackStreamReport,
    ) -> Result<(), ReceivePackChunkSinkError>;

    /// Best-effort terminal discard after any source, control, validation, or sink failure.
    async fn abort(&mut self, reason: ReceivePackAbortReason);
}

/// Structurally validated streamed PACK summary.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReceivePackStreamReport {
    /// Complete PACK bytes including trailer, or zero for a no-pack request.
    pub pack_bytes: u64,
    /// Declared PACK object count, or zero for a no-pack request.
    pub declared_objects: u32,
    /// Whether the request carried no PACK body.
    pub empty: bool,
}

/// Typed receive-pack stream failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum ReceivePackStreamError {
    /// Stream configuration is zero or inconsistent.
    #[error("invalid receive-pack stream limits")]
    InvalidLimits,
    /// Envelope/request safety limit failed.
    #[error(transparent)]
    Limit(#[from] ReceivePackLimitError),
    /// Envelope contains transport framing bytes in a parsed string field.
    #[error("receive-pack envelope contains invalid framing data")]
    InvalidEnvelope,
    /// Commands or object-format capability disagree with the negotiated hash algorithm.
    #[error("receive-pack envelope object format is inconsistent")]
    InvalidObjectFormat,
    /// Source failed terminally.
    #[error(transparent)]
    Source(#[from] PackChunkSourceError),
    /// Explicit cancellation was observed.
    #[error("receive-pack stream cancelled")]
    Cancelled,
    /// Explicit deadline elapsed.
    #[error("receive-pack stream deadline elapsed")]
    Deadline,
    /// Explicit idle observation exceeded its limit.
    #[error("receive-pack stream idle limit elapsed")]
    Idle,
    /// Source returned an empty data chunk instead of progress or EOF.
    #[error("receive-pack source returned an empty chunk")]
    NoProgress,
    /// One source chunk exceeded the configured bound.
    #[error("receive-pack source chunk exceeds configured limit")]
    ChunkTooLarge,
    /// PACK body exceeded its configured total.
    #[error("receive-pack PACK body exceeds configured limit")]
    PackLimit,
    /// Envelope plus PACK body exceeded its configured total.
    #[error("receive-pack request exceeds configured limit")]
    RequestLimit,
    /// Nonempty PACK was shorter than its header and hash trailer.
    #[error("receive-pack PACK body is truncated")]
    TruncatedPack,
    /// PACK signature was invalid.
    #[error("receive-pack PACK signature is invalid")]
    InvalidSignature,
    /// PACK version is unsupported.
    #[error("receive-pack PACK version is unsupported")]
    UnsupportedVersion,
    /// Declared object count exceeded its configured limit.
    #[error("receive-pack PACK object count exceeds configured limit")]
    ObjectLimit,
    /// Incremental trailer checksum did not match the body.
    #[error("receive-pack PACK trailer checksum mismatch")]
    Checksum,
    /// Quarantine sink failed.
    #[error(transparent)]
    Sink(#[from] ReceivePackChunkSinkError),
    /// Legacy buffered request cannot represent push options.
    #[error("legacy buffered receive-pack request cannot represent push options")]
    UnsupportedPushOptions,
    /// Explicit compatibility collector allocation failed.
    #[error("receive-pack compatibility buffer allocation failed")]
    Allocation,
}

/// Stream one PACK body into an unpublished sink with bounded memory and explicit observations.
///
/// The function reads at most one chunk ahead and never calls `read_chunk` again until the sink
/// has accepted the preceding chunk. Any terminal error stops reads and best-effort aborts the
/// sink. An empty body is valid for delete-only pushes and updates to already stored objects.
///
/// # Errors
///
/// Returns typed envelope/limit, source, cancellation/deadline/idle, PACK validation, or sink
/// failures. No output or live repository metadata is produced by this transport-neutral pump.
pub async fn receive_pack_stream<S, K, C>(
    envelope: &ReceivePackEnvelope,
    hash_algo: HashAlgo,
    source: &mut S,
    sink: &mut K,
    control: &mut C,
    options: ReceivePackStreamOptions,
) -> Result<ReceivePackStreamReport, ReceivePackStreamError>
where
    S: PackChunkSource,
    K: ReceivePackChunkSink,
    C: ReceivePackStreamControl,
{
    let options = options.validate()?;
    envelope.validate(options.limits)?;
    envelope.validate_hash_algo(hash_algo)?;
    let envelope_bytes = envelope.estimated_bytes()?;
    let mut validator = PackPrefixValidator::new(hash_algo, options.limits.max_objects);
    let mut pack_bytes = 0_u64;
    let mut last_observed_at = None;

    loop {
        if let Err(error) = check_observation(control.observe(), options, &mut last_observed_at) {
            sink.abort(abort_reason(error)).await;
            return Err(error);
        }
        let chunk = match source.read_chunk().await {
            Ok(PackSourceChunk::Data(chunk)) => chunk,
            Ok(PackSourceChunk::End) => break,
            Err(error) => {
                sink.abort(ReceivePackAbortReason::Source).await;
                return Err(error.into());
            }
        };
        if chunk.is_empty() {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::NoProgress);
        }
        if chunk.len() > options.max_chunk_bytes {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::ChunkTooLarge);
        }
        if let Err(error) = check_observation(control.observe(), options, &mut last_observed_at) {
            sink.abort(abort_reason(error)).await;
            return Err(error);
        }
        let Ok(chunk_bytes) = u64::try_from(chunk.len()) else {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::PackLimit);
        };
        let Some(next_pack_bytes) = pack_bytes.checked_add(chunk_bytes) else {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::PackLimit);
        };
        pack_bytes = next_pack_bytes;
        if pack_bytes > options.limits.max_pack_bytes {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::PackLimit);
        }
        let Some(request_bytes) = u64::try_from(envelope_bytes)
            .ok()
            .and_then(|bytes| bytes.checked_add(pack_bytes))
        else {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::RequestLimit);
        };
        if request_bytes > options.limits.max_request_bytes {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(ReceivePackStreamError::RequestLimit);
        }
        if let Err(error) = validator.update(&chunk) {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(error);
        }
        if let Err(error) = sink.append(&chunk).await {
            sink.abort(ReceivePackAbortReason::Sink).await;
            return Err(error.into());
        }
    }

    if let Err(error) = check_observation(control.observe(), options, &mut last_observed_at) {
        sink.abort(abort_reason(error)).await;
        return Err(error);
    }
    let report = match validator.finish(pack_bytes) {
        Ok(report) => report,
        Err(error) => {
            sink.abort(ReceivePackAbortReason::InvalidInput).await;
            return Err(error);
        }
    };
    if let Err(error) = sink.finish(report).await {
        sink.abort(ReceivePackAbortReason::Sink).await;
        return Err(error.into());
    }
    Ok(report)
}

/// Validate a legacy buffered request through the streaming source/sink state machine.
///
/// This compatibility helper is intentionally bounded and may temporarily retain both the input
/// body and its collected replacement. Streaming HTTP/quarantine adapters should call
/// [`receive_pack_stream`] directly instead.
///
/// # Errors
///
/// Returns the same typed envelope, control, PACK validation, allocation, source, or sink failures
/// as [`receive_pack_stream`].
pub async fn validate_buffered_receive_pack<C>(
    request: ReceivePackRequest,
    hash_algo: HashAlgo,
    control: &mut C,
    options: ReceivePackStreamOptions,
) -> Result<ReceivePackRequest, ReceivePackStreamError>
where
    C: ReceivePackStreamControl,
{
    let options = options.validate()?;
    let (envelope, pack) = ReceivePackEnvelope::split_buffered(request);
    let retained_bytes = u64::try_from(pack.len())
        .ok()
        .and_then(|bytes| bytes.checked_mul(2))
        .ok_or(ReceivePackStreamError::RequestLimit)?;
    options.limits.validate_bytes_in_flight(retained_bytes)?;
    let mut source = BufferedPackSource::new(
        &pack,
        options.max_chunk_bytes,
        options.limits.max_pack_bytes,
    )?;
    let mut collector = BoundedReceivePackCollector::new(options.limits.max_pack_bytes)?;
    receive_pack_stream(
        &envelope,
        hash_algo,
        &mut source,
        &mut collector,
        control,
        options,
    )
    .await?;
    drop(source);
    drop(pack);
    let collected = collector.into_bytes()?;
    envelope.into_buffered_request(collected, options.limits)
}

/// Explicitly bounded compatibility sink that retains the complete PACK body.
#[derive(Debug)]
pub struct BoundedReceivePackCollector {
    bytes: Vec<u8>,
    max_bytes: u64,
    state: CollectorState,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum CollectorState {
    #[default]
    Open,
    Finished,
    Aborted,
}

impl BoundedReceivePackCollector {
    /// Construct an empty compatibility collector with a prevalidated total bound.
    ///
    /// # Errors
    ///
    /// Rejects a zero bound or a bound above the receive-pack hard maximum.
    pub fn new(max_bytes: u64) -> Result<Self, ReceivePackStreamError> {
        if max_bytes == 0 || max_bytes > ReceivePackLimits::HARD_MAX.max_pack_bytes {
            return Err(ReceivePackStreamError::InvalidLimits);
        }
        Ok(Self {
            bytes: Vec::new(),
            max_bytes,
            state: CollectorState::Open,
        })
    }

    /// Consume a successfully finished collector and return its complete PACK body.
    ///
    /// # Errors
    ///
    /// Returns a sink failure if finish did not complete or the collector was aborted.
    pub fn into_bytes(self) -> Result<Vec<u8>, ReceivePackStreamError> {
        if self.state != CollectorState::Finished {
            return Err(ReceivePackStreamError::Sink(ReceivePackChunkSinkError));
        }
        Ok(self.bytes)
    }
}

#[async_trait]
impl ReceivePackChunkSink for BoundedReceivePackCollector {
    async fn append(&mut self, chunk: &[u8]) -> Result<(), ReceivePackChunkSinkError> {
        if self.state != CollectorState::Open {
            return Err(ReceivePackChunkSinkError);
        }
        let next = u64::try_from(chunk.len())
            .ok()
            .and_then(|bytes| u64::try_from(self.bytes.len()).ok()?.checked_add(bytes))
            .filter(|bytes| *bytes <= self.max_bytes)
            .ok_or(ReceivePackChunkSinkError)?;
        let additional = usize::try_from(next)
            .ok()
            .and_then(|next| next.checked_sub(self.bytes.len()))
            .ok_or(ReceivePackChunkSinkError)?;
        self.bytes
            .try_reserve_exact(additional)
            .map_err(|_| ReceivePackChunkSinkError)?;
        self.bytes.extend_from_slice(chunk);
        Ok(())
    }

    async fn finish(
        &mut self,
        _report: ReceivePackStreamReport,
    ) -> Result<(), ReceivePackChunkSinkError> {
        if self.state != CollectorState::Open {
            return Err(ReceivePackChunkSinkError);
        }
        self.state = CollectorState::Finished;
        Ok(())
    }

    async fn abort(&mut self, _reason: ReceivePackAbortReason) {
        self.bytes.clear();
        self.state = CollectorState::Aborted;
    }
}

enum IncrementalHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl IncrementalHasher {
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

struct PackPrefixValidator {
    hash_algo: HashAlgo,
    hasher: IncrementalHasher,
    trailer: VecDeque<u8>,
    header: [u8; 12],
    header_bytes: usize,
    declared_objects: Option<u32>,
    max_objects: usize,
}

impl PackPrefixValidator {
    fn new(hash_algo: HashAlgo, max_objects: usize) -> Self {
        Self {
            hash_algo,
            hasher: IncrementalHasher::new(hash_algo),
            trailer: VecDeque::with_capacity(hash_algo.len()),
            header: [0; 12],
            header_bytes: 0,
            declared_objects: None,
            max_objects,
        }
    }

    fn update(&mut self, chunk: &[u8]) -> Result<(), ReceivePackStreamError> {
        let total = self
            .trailer
            .len()
            .checked_add(chunk.len())
            .ok_or(ReceivePackStreamError::PackLimit)?;
        let body_bytes = if total > self.hash_algo.len() {
            total - self.hash_algo.len()
        } else {
            0
        };
        let from_trailer = body_bytes.min(self.trailer.len());
        for _ in 0..from_trailer {
            let Some(byte) = self.trailer.pop_front() else {
                return Err(ReceivePackStreamError::TruncatedPack);
            };
            self.update_body(std::slice::from_ref(&byte))?;
        }
        let from_chunk = body_bytes
            .checked_sub(from_trailer)
            .ok_or(ReceivePackStreamError::TruncatedPack)?;
        self.update_body(&chunk[..from_chunk])?;
        self.trailer.extend(&chunk[from_chunk..]);
        Ok(())
    }

    fn update_body(&mut self, bytes: &[u8]) -> Result<(), ReceivePackStreamError> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.hasher.update(bytes);
        if self.header_bytes < self.header.len() {
            let remaining = self.header.len() - self.header_bytes;
            let copy = remaining.min(bytes.len());
            self.header[self.header_bytes..self.header_bytes + copy]
                .copy_from_slice(&bytes[..copy]);
            self.header_bytes += copy;
            if self.header_bytes == self.header.len() {
                self.validate_header()?;
            }
        }
        Ok(())
    }

    fn validate_header(&mut self) -> Result<(), ReceivePackStreamError> {
        if &self.header[..4] != b"PACK" {
            return Err(ReceivePackStreamError::InvalidSignature);
        }
        let version = u32::from_be_bytes([
            self.header[4],
            self.header[5],
            self.header[6],
            self.header[7],
        ]);
        if version != 2 && version != 3 {
            return Err(ReceivePackStreamError::UnsupportedVersion);
        }
        let objects = u32::from_be_bytes([
            self.header[8],
            self.header[9],
            self.header[10],
            self.header[11],
        ]);
        let object_count =
            usize::try_from(objects).map_err(|_| ReceivePackStreamError::ObjectLimit)?;
        if object_count > self.max_objects {
            return Err(ReceivePackStreamError::ObjectLimit);
        }
        self.declared_objects = Some(objects);
        Ok(())
    }

    fn finish(self, pack_bytes: u64) -> Result<ReceivePackStreamReport, ReceivePackStreamError> {
        if pack_bytes == 0 {
            return Ok(ReceivePackStreamReport {
                empty: true,
                ..ReceivePackStreamReport::default()
            });
        }
        if self.header_bytes != self.header.len() || self.trailer.len() != self.hash_algo.len() {
            return Err(ReceivePackStreamError::TruncatedPack);
        }
        let expected = self.hasher.finish();
        if !expected.iter().copied().eq(self.trailer) {
            return Err(ReceivePackStreamError::Checksum);
        }
        Ok(ReceivePackStreamReport {
            pack_bytes,
            declared_objects: self.declared_objects.unwrap_or_default(),
            empty: false,
        })
    }
}

fn check_observation(
    observation: ReceivePackStreamObservation,
    options: ReceivePackStreamOptions,
    last_observed_at: &mut Option<OffsetDateTime>,
) -> Result<(), ReceivePackStreamError> {
    if observation.cancelled {
        return Err(ReceivePackStreamError::Cancelled);
    }
    if observation.observed_at < options.started_at
        || last_observed_at.is_some_and(|previous| observation.observed_at < previous)
    {
        return Err(ReceivePackStreamError::InvalidLimits);
    }
    if observation.observed_at >= options.deadline {
        return Err(ReceivePackStreamError::Deadline);
    }
    if observation.idle_for < Duration::ZERO {
        return Err(ReceivePackStreamError::InvalidLimits);
    }
    if observation.idle_for > options.max_idle {
        return Err(ReceivePackStreamError::Idle);
    }
    *last_observed_at = Some(observation.observed_at);
    Ok(())
}

fn abort_reason(error: ReceivePackStreamError) -> ReceivePackAbortReason {
    match error {
        ReceivePackStreamError::Cancelled => ReceivePackAbortReason::Cancelled,
        ReceivePackStreamError::Deadline | ReceivePackStreamError::Idle => {
            ReceivePackAbortReason::Timeout
        }
        ReceivePackStreamError::Source(_) => ReceivePackAbortReason::Source,
        ReceivePackStreamError::Sink(_) => ReceivePackAbortReason::Sink,
        _ => ReceivePackAbortReason::InvalidInput,
    }
}

fn duration_to_time(duration: std::time::Duration) -> Result<Duration, ReceivePackStreamError> {
    Duration::try_from(duration).map_err(|_| ReceivePackStreamError::InvalidLimits)
}

fn capability_wire_len(
    capability: &ReceivePackCapability,
) -> Result<usize, ReceivePackStreamError> {
    let length = match capability {
        ReceivePackCapability::ReportStatus => "report-status".len(),
        ReceivePackCapability::ReportStatusV2 => "report-status-v2".len(),
        ReceivePackCapability::SideBand64k => "side-band-64k".len(),
        ReceivePackCapability::DeleteRefs => "delete-refs".len(),
        ReceivePackCapability::Atomic => "atomic".len(),
        ReceivePackCapability::OfsDelta => "ofs-delta".len(),
        ReceivePackCapability::PushOptions => "push-options".len(),
        ReceivePackCapability::Agent(agent) => "agent="
            .len()
            .checked_add(agent.len())
            .ok_or(ReceivePackStreamError::RequestLimit)?,
        ReceivePackCapability::ObjectFormat(hash_algo) => "object-format="
            .len()
            .checked_add(hash_algo.name().len())
            .ok_or(ReceivePackStreamError::RequestLimit)?,
        ReceivePackCapability::Other(value) => value.len(),
    };
    Ok(length)
}

fn capability_has_invalid_value(capability: &ReceivePackCapability) -> bool {
    match capability {
        ReceivePackCapability::Agent(value) | ReceivePackCapability::Other(value) => {
            value.is_empty() || contains_framing_control(value)
        }
        _ => false,
    }
}

fn contains_framing_control(value: &str) -> bool {
    value
        .as_bytes()
        .iter()
        .any(|byte| matches!(*byte, b'\0' | b'\r' | b'\n'))
}
