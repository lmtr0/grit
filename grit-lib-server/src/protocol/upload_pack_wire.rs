//! Streaming upload-pack ACK/NAK and sideband framing.
//!
//! The decorator accepts logical raw PACK chunks and applies wire framing only after hashing. Its
//! [`PackChunkSink`] report therefore starts at zero after the prelude and never includes pkt-line
//! bytes, while [`UploadPackWireReport`] exposes physical downstream traffic separately.

use async_trait::async_trait;
use grit_lib::objects::ObjectId;

use crate::protocol::pack_stream::{
    PackAbortReason, PackChunkSink, PackSinkOperation, PackSinkState, PackStreamError,
    PackStreamLimits, PackStreamReport,
};
use crate::protocol::upload_pack::UploadPackCapability;

const LEGACY_SIDEBAND_PAYLOAD: usize = 995;
const SIDEBAND_64K_PAYLOAD: usize = 65_515;
const MAX_PKT_LINE_BYTES: usize = 65_520;

/// Negotiated upload-pack response framing.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UploadPackWireMode {
    /// ACK/NAK pkt-line followed directly by raw PACK bytes.
    #[default]
    Raw,
    /// Channel-1 sideband frames with a 1,000-byte pkt-line maximum.
    SideBand,
    /// Channel-1 sideband frames with Git's 65,520-byte pkt-line maximum.
    SideBand64k,
}

impl UploadPackWireMode {
    /// Select framing with `side-band-64k` taking precedence over `side-band`.
    #[must_use]
    pub fn negotiate(capabilities: &[UploadPackCapability]) -> Self {
        if capabilities
            .iter()
            .any(|capability| matches!(capability, UploadPackCapability::SideBand64k))
        {
            return Self::SideBand64k;
        }
        if capabilities
            .iter()
            .any(|capability| matches!(capability, UploadPackCapability::SideBand))
        {
            return Self::SideBand;
        }
        Self::Raw
    }

    const fn frame_payload_bytes(self) -> Option<usize> {
        match self {
            Self::Raw => None,
            Self::SideBand => Some(LEGACY_SIDEBAND_PAYLOAD),
            Self::SideBand64k => Some(SIDEBAND_64K_PAYLOAD),
        }
    }

    /// Largest logical PACK chunk that can be accepted atomically in this mode.
    #[must_use]
    pub const fn max_pack_chunk_bytes(self, configured: usize) -> usize {
        match self.frame_payload_bytes() {
            Some(payload) if configured < payload => configured,
            Some(payload) => payload,
            None => configured,
        }
    }
}

/// Upload-pack wire decorator state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UploadPackWireState {
    /// Validated but ACK/NAK was not emitted.
    #[default]
    Ready,
    /// Prelude was emitted and raw PACK chunks are accepted.
    Streaming,
    /// Optional flush and downstream finish completed.
    Finished,
    /// Framing or downstream failure terminated the response.
    Aborted(PackAbortReason),
}

/// Distinct logical PACK and physical wire counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UploadPackWireReport {
    /// Current decorator state.
    pub state: UploadPackWireState,
    /// Negotiated framing.
    pub mode: UploadPackWireMode,
    /// Logical raw PACK bytes accepted from the writer.
    pub raw_pack_bytes: u64,
    /// Logical raw PACK chunks accepted from the writer.
    pub raw_pack_chunks: u64,
    /// Physical prelude bytes accepted downstream.
    pub prelude_bytes: u64,
    /// Physical sideband frame count; zero in raw mode.
    pub sideband_frames: u64,
    /// Physical bytes accepted downstream, including prelude and flush.
    pub wire_bytes: u64,
    /// Physical downstream writes, including prelude and flush.
    pub wire_chunks: u64,
}

/// Sink decorator that streams one upload-pack response without copying the complete PACK.
pub struct UploadPackWireSink<'a> {
    downstream: &'a mut dyn PackChunkSink,
    mode: UploadPackWireMode,
    limits: PackStreamLimits,
    state: UploadPackWireState,
    report: UploadPackWireReport,
    raw_report: PackStreamReport,
}

impl<'a> UploadPackWireSink<'a> {
    /// Validate the exact ACK/NAK prelude and minimum empty PACK wire shape without output.
    ///
    /// # Errors
    ///
    /// Returns allocation, arithmetic, chunk, or total-limit errors before the downstream sink is
    /// touched.
    pub fn validate_response(
        common: Option<&ObjectId>,
        mode: UploadPackWireMode,
        limits: PackStreamLimits,
        trailer_bytes: usize,
        planned_objects: usize,
    ) -> Result<(), PackStreamError> {
        let limits = limits.validate()?;
        let prelude = build_prelude(common)?;
        if prelude.len() > limits.max_chunk_bytes {
            return Err(PackStreamError::ChunkTooLarge);
        }
        let minimum_entry_bytes = planned_objects
            .checked_mul(9)
            .ok_or(PackStreamError::ByteOverflow)?;
        let minimum_raw = 12_usize
            .checked_add(trailer_bytes)
            .and_then(|bytes| bytes.checked_add(minimum_entry_bytes))
            .ok_or(PackStreamError::ByteOverflow)?;
        let logical_budget = Self::logical_pack_budget(common, mode, limits, planned_objects)?;
        let minimum_raw = u64::try_from(minimum_raw).map_err(|_| PackStreamError::ByteOverflow)?;
        if minimum_raw > logical_budget {
            return Err(PackStreamError::TotalLimit);
        }
        Ok(())
    }

    /// Return the raw PACK byte budget after reserving fixed prelude/minimum-frame/flush overhead.
    ///
    /// Variable sideband overhead from additional compression chunks remains checked before every
    /// physical frame write.
    ///
    /// # Errors
    ///
    /// Returns checked arithmetic, allocation, or total-limit errors.
    pub fn logical_pack_budget(
        common: Option<&ObjectId>,
        mode: UploadPackWireMode,
        limits: PackStreamLimits,
        planned_objects: usize,
    ) -> Result<u64, PackStreamError> {
        let limits = limits.validate()?;
        let prelude = build_prelude(common)?;
        let framing = if mode == UploadPackWireMode::Raw {
            0
        } else {
            let minimum_frames = planned_objects
                .checked_mul(2)
                .and_then(|frames| frames.checked_add(2))
                .ok_or(PackStreamError::ByteOverflow)?;
            5_usize
                .checked_mul(minimum_frames)
                .and_then(|bytes| bytes.checked_add(4))
                .ok_or(PackStreamError::ByteOverflow)?
        };
        let fixed_overhead = prelude
            .len()
            .checked_add(framing)
            .ok_or(PackStreamError::ByteOverflow)?;
        let fixed_overhead =
            u64::try_from(fixed_overhead).map_err(|_| PackStreamError::ByteOverflow)?;
        limits
            .max_total_bytes
            .checked_sub(fixed_overhead)
            .ok_or(PackStreamError::TotalLimit)
    }

    /// Validate a fresh downstream sink without emitting bytes.
    ///
    /// # Errors
    ///
    /// Returns limit/state errors when framing cannot safely begin.
    pub fn new(
        downstream: &'a mut dyn PackChunkSink,
        mode: UploadPackWireMode,
        limits: PackStreamLimits,
    ) -> Result<Self, PackStreamError> {
        let limits = limits.validate()?;
        let downstream_report = downstream.report();
        if downstream.state() != PackSinkState::Open
            || downstream_report.emitted_bytes != 0
            || downstream_report.emitted_chunks != 0
        {
            return Err(PackStreamError::InvalidState {
                operation: PackSinkOperation::Write,
                state: downstream.state(),
            });
        }
        if mode.frame_payload_bytes().is_some_and(|payload| {
            payload
                .checked_add(5)
                .is_none_or(|frame| frame > limits.max_chunk_bytes)
        }) {
            return Err(PackStreamError::InvalidLimits);
        }
        Ok(Self {
            downstream,
            mode,
            limits,
            state: UploadPackWireState::Ready,
            report: UploadPackWireReport {
                mode,
                ..UploadPackWireReport::default()
            },
            raw_report: PackStreamReport::default(),
        })
    }

    /// Emit exactly one ACK or NAK pkt-line before the raw PACK header.
    ///
    /// # Errors
    ///
    /// Returns state, allocation, wire-limit, or downstream errors and terminally aborts on any
    /// post-construction failure.
    pub async fn write_prelude(
        &mut self,
        common: Option<&ObjectId>,
    ) -> Result<(), PackStreamError> {
        if self.state != UploadPackWireState::Ready {
            return Err(self.invalid_state(PackSinkOperation::Write));
        }
        let frame = match build_prelude(common) {
            Ok(frame) => frame,
            Err(error) => return Err(error),
        };
        let prelude_bytes =
            u64::try_from(frame.len()).map_err(|_| PackStreamError::ByteOverflow)?;
        if frame.len() > self.limits.max_chunk_bytes || prelude_bytes > self.limits.max_total_bytes
        {
            return Err(PackStreamError::TotalLimit);
        }
        match self.write_wire(&frame).await {
            Ok(()) => {
                self.report.prelude_bytes = prelude_bytes;
                self.state = UploadPackWireState::Streaming;
                self.report.state = self.state;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Return physical wire counters, distinct from [`PackChunkSink::report`].
    #[must_use]
    pub const fn wire_report(&self) -> UploadPackWireReport {
        self.report
    }

    async fn write_wire(&mut self, bytes: &[u8]) -> Result<(), PackStreamError> {
        if bytes.len() > self.limits.max_chunk_bytes {
            return Err(self
                .fail(
                    PackStreamError::ChunkTooLarge,
                    PackAbortReason::LimitExceeded,
                )
                .await);
        }
        let growth = u64::try_from(bytes.len())
            .ok()
            .and_then(|bytes| self.report.wire_bytes.checked_add(bytes))
            .zip(self.report.wire_chunks.checked_add(1));
        let Some((next_bytes, next_chunks)) = growth else {
            return Err(self
                .fail(
                    PackStreamError::ByteOverflow,
                    PackAbortReason::LimitExceeded,
                )
                .await);
        };
        if next_bytes > self.limits.max_total_bytes {
            return Err(self
                .fail(PackStreamError::TotalLimit, PackAbortReason::LimitExceeded)
                .await);
        }
        if let Err(error) = self.downstream.write_chunk(bytes).await {
            let reason = self
                .downstream
                .report()
                .aborted
                .unwrap_or(PackAbortReason::SinkFailure);
            if self.downstream.state() == PackSinkState::Open {
                let _ = self.downstream.abort(reason).await;
            }
            self.state = UploadPackWireState::Aborted(reason);
            self.report.state = self.state;
            self.raw_report.aborted = Some(reason);
            return Err(error);
        }
        self.report.wire_bytes = next_bytes;
        self.report.wire_chunks = next_chunks;
        Ok(())
    }

    async fn fail(&mut self, error: PackStreamError, reason: PackAbortReason) -> PackStreamError {
        if self.downstream.state() == PackSinkState::Open {
            let _ = self.downstream.abort(reason).await;
        }
        self.state = UploadPackWireState::Aborted(reason);
        self.report.state = self.state;
        self.raw_report.aborted = Some(reason);
        error
    }

    fn invalid_state(&self, operation: PackSinkOperation) -> PackStreamError {
        PackStreamError::InvalidState {
            operation,
            state: match self.state {
                UploadPackWireState::Ready | UploadPackWireState::Streaming => PackSinkState::Open,
                UploadPackWireState::Finished => PackSinkState::Finished,
                UploadPackWireState::Aborted(reason) => PackSinkState::Aborted(reason),
            },
        }
    }
}

#[async_trait]
impl PackChunkSink for UploadPackWireSink<'_> {
    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), PackStreamError> {
        if self.state != UploadPackWireState::Streaming {
            return Err(self.invalid_state(PackSinkOperation::Write));
        }
        if bytes.is_empty() {
            return Ok(());
        }
        let growth = u64::try_from(bytes.len())
            .ok()
            .and_then(|bytes| self.report.raw_pack_bytes.checked_add(bytes))
            .zip(self.report.raw_pack_chunks.checked_add(1));
        let Some((next_raw, next_chunks)) = growth else {
            return Err(self
                .fail(
                    PackStreamError::ByteOverflow,
                    PackAbortReason::LimitExceeded,
                )
                .await);
        };
        if bytes.len() > self.limits.max_chunk_bytes || next_raw > self.limits.max_total_bytes {
            return Err(self
                .fail(PackStreamError::TotalLimit, PackAbortReason::LimitExceeded)
                .await);
        }

        if let Some(max_payload) = self.mode.frame_payload_bytes() {
            if bytes.len() > max_payload {
                return Err(self
                    .fail(
                        PackStreamError::ChunkTooLarge,
                        PackAbortReason::LimitExceeded,
                    )
                    .await);
            }
            let Some(next_frames) = self.report.sideband_frames.checked_add(1) else {
                return Err(self
                    .fail(
                        PackStreamError::ByteOverflow,
                        PackAbortReason::LimitExceeded,
                    )
                    .await);
            };
            let frame = match build_sideband_frame(bytes) {
                Ok(frame) => frame,
                Err(error) => {
                    return Err(self.fail(error, PackAbortReason::SinkFailure).await);
                }
            };
            self.write_wire(&frame).await?;
            self.report.sideband_frames = next_frames;
        } else {
            self.write_wire(bytes).await?;
        }
        self.report.raw_pack_bytes = next_raw;
        self.report.raw_pack_chunks = next_chunks;
        self.raw_report.emitted_bytes = next_raw;
        self.raw_report.emitted_chunks = next_chunks;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), PackStreamError> {
        if self.state != UploadPackWireState::Streaming {
            return Err(self.invalid_state(PackSinkOperation::Finish));
        }
        if self.mode != UploadPackWireMode::Raw {
            self.write_wire(b"0000").await?;
        }
        if let Err(error) = self.downstream.finish().await {
            let reason = self
                .downstream
                .report()
                .aborted
                .unwrap_or(PackAbortReason::SinkFailure);
            if self.downstream.state() == PackSinkState::Open {
                let _ = self.downstream.abort(reason).await;
            }
            self.state = UploadPackWireState::Aborted(reason);
            self.report.state = self.state;
            self.raw_report.aborted = Some(reason);
            return Err(error);
        }
        self.state = UploadPackWireState::Finished;
        self.report.state = self.state;
        self.raw_report.finished = true;
        Ok(())
    }

    async fn abort(&mut self, reason: PackAbortReason) -> Result<(), PackStreamError> {
        if !matches!(
            self.state,
            UploadPackWireState::Ready | UploadPackWireState::Streaming
        ) {
            return Err(self.invalid_state(PackSinkOperation::Abort));
        }
        let result = if self.downstream.state() == PackSinkState::Open {
            self.downstream.abort(reason).await
        } else {
            Ok(())
        };
        self.state = UploadPackWireState::Aborted(reason);
        self.report.state = self.state;
        self.raw_report.aborted = Some(reason);
        result
    }

    fn state(&self) -> PackSinkState {
        match self.state {
            UploadPackWireState::Ready | UploadPackWireState::Streaming => PackSinkState::Open,
            UploadPackWireState::Finished => PackSinkState::Finished,
            UploadPackWireState::Aborted(reason) => PackSinkState::Aborted(reason),
        }
    }

    fn report(&self) -> PackStreamReport {
        self.raw_report
    }
}

fn build_prelude(common: Option<&ObjectId>) -> Result<Vec<u8>, PackStreamError> {
    let payload_len = common.map_or(4, |oid| 5 + oid.algo().hex_len());
    let total_len = payload_len
        .checked_add(4)
        .ok_or(PackStreamError::ByteOverflow)?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(total_len)
        .map_err(|_| PackStreamError::Allocation)?;
    frame.extend_from_slice(&pkt_line_prefix(total_len)?);
    if let Some(oid) = common {
        frame.extend_from_slice(b"ACK ");
        frame.extend_from_slice(oid.to_hex().as_bytes());
        frame.push(b'\n');
    } else {
        frame.extend_from_slice(b"NAK\n");
    }
    Ok(frame)
}

fn build_sideband_frame(payload: &[u8]) -> Result<Vec<u8>, PackStreamError> {
    let total_len = payload
        .len()
        .checked_add(5)
        .ok_or(PackStreamError::ByteOverflow)?;
    let mut frame = Vec::new();
    frame
        .try_reserve_exact(total_len)
        .map_err(|_| PackStreamError::Allocation)?;
    frame.extend_from_slice(&pkt_line_prefix(total_len)?);
    frame.push(1);
    frame.extend_from_slice(payload);
    Ok(frame)
}

fn pkt_line_prefix(length: usize) -> Result<[u8; 4], PackStreamError> {
    if length > MAX_PKT_LINE_BYTES {
        return Err(PackStreamError::ChunkTooLarge);
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    Ok([
        HEX[(length >> 12) & 0xf],
        HEX[(length >> 8) & 0xf],
        HEX[(length >> 4) & 0xf],
        HEX[length & 0xf],
    ])
}
