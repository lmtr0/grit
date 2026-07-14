//! Runtime-neutral, backpressure-capable output boundary for pack streaming.
//!
//! An async `write_chunk` call is the backpressure point: producers must await it before reusing
//! their buffer or advancing ordered output. This module provides no task spawning, clocks, HTTP
//! types, or runtime-specific cancellation.

use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;

/// Hard and caller-configurable pack-stream resource limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackStreamLimits {
    /// Largest chunk accepted by a sink.
    pub max_chunk_bytes: usize,
    /// Largest complete response accepted by a sink.
    pub max_total_bytes: u64,
    /// Maximum bytes a producer may queue awaiting a transport sink.
    pub max_queued_bytes: usize,
    /// Maximum bytes owned by all producer/transport in-flight output stages.
    pub max_in_flight_bytes: usize,
    /// Maximum emitted bytes between cancellation checks.
    pub cancellation_checkpoint_bytes: u64,
}

impl PackStreamLimits {
    /// Absolute ceilings accepted by [`Self::validate`].
    pub const HARD_MAX: Self = Self {
        max_chunk_bytes: 16 << 20,
        max_total_bytes: 1_u64 << 44,
        max_queued_bytes: 1 << 30,
        max_in_flight_bytes: if usize::BITS >= 64 {
            4_usize << 30
        } else {
            usize::MAX
        },
        cancellation_checkpoint_bytes: 1_u64 << 30,
    };

    /// Validate nonzero, ordered limits before allocating buffers.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::InvalidLimits`] when limits are zero, inconsistent, or exceed
    /// absolute ceilings.
    pub fn validate(self) -> Result<Self, PackStreamError> {
        let hard = Self::HARD_MAX;
        let chunk_bytes =
            u64::try_from(self.max_chunk_bytes).map_err(|_| PackStreamError::InvalidLimits)?;
        if self.max_chunk_bytes == 0
            || self.max_total_bytes == 0
            || self.max_queued_bytes == 0
            || self.max_in_flight_bytes == 0
            || self.cancellation_checkpoint_bytes == 0
            || self.max_chunk_bytes > self.max_queued_bytes
            || self.max_queued_bytes > self.max_in_flight_bytes
            || chunk_bytes > self.cancellation_checkpoint_bytes
            || self.max_chunk_bytes > hard.max_chunk_bytes
            || self.max_total_bytes > hard.max_total_bytes
            || self.max_queued_bytes > hard.max_queued_bytes
            || self.max_in_flight_bytes > hard.max_in_flight_bytes
            || self.cancellation_checkpoint_bytes > hard.cancellation_checkpoint_bytes
        {
            return Err(PackStreamError::InvalidLimits);
        }
        Ok(self)
    }
}

impl Default for PackStreamLimits {
    fn default() -> Self {
        Self {
            max_chunk_bytes: 64 << 10,
            max_total_bytes: 256_u64 << 30,
            max_queued_bytes: 4 << 20,
            max_in_flight_bytes: 64 << 20,
            cancellation_checkpoint_bytes: 1 << 20,
        }
    }
}

/// Non-sensitive terminal abort category safe to expose across adapters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackAbortReason {
    /// Caller cancellation was observed.
    Cancelled,
    /// Client transport disconnected.
    ClientDisconnected,
    /// A validated stream limit was reached.
    LimitExceeded,
    /// A storage read failed.
    BackendFailure,
    /// Pack encoding failed after streaming began.
    EncodingFailure,
    /// Output transport failed.
    SinkFailure,
}

/// Durable state of one sink instance.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PackSinkState {
    /// Chunks may be written.
    #[default]
    Open,
    /// `finish` completed exactly once.
    Finished,
    /// `abort` completed and discarded unpublished compatibility bytes.
    Aborted(PackAbortReason),
}

/// Sink operation rejected by the state machine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackSinkOperation {
    /// Write a data chunk.
    Write,
    /// Finish the response.
    Finish,
    /// Abort the response.
    Abort,
}

/// Pack stream boundary failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackStreamError {
    /// Stream limits are zero, inconsistent, or exceed the hard ceiling.
    #[error("invalid pack stream limits")]
    InvalidLimits,
    /// An operation was attempted after finish or abort.
    #[error("pack sink operation {operation:?} is invalid in state {state:?}")]
    InvalidState {
        /// Attempted operation.
        operation: PackSinkOperation,
        /// Current sink state.
        state: PackSinkState,
    },
    /// One chunk exceeded the configured per-call bound.
    #[error("pack stream chunk exceeds configured limit")]
    ChunkTooLarge,
    /// The next chunk would exceed the configured response bound.
    #[error("pack stream exceeds configured total")]
    TotalLimit,
    /// Byte accounting overflowed.
    #[error("pack stream byte accounting overflow")]
    ByteOverflow,
    /// A bounded compatibility buffer could not reserve memory.
    #[error("pack stream buffer allocation failed")]
    Allocation,
    /// A transport sink failed without exposing adapter or request details.
    #[error("pack stream output sink failed")]
    SinkFailure,
    /// Cancellation was observed at an explicit checkpoint.
    #[error("pack stream cancelled")]
    Cancelled,
}

/// Observable stream counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PackStreamReport {
    /// Bytes accepted by completed `write_chunk` calls.
    pub emitted_bytes: u64,
    /// Completed nonempty chunk writes.
    pub emitted_chunks: u64,
    /// Whether `finish` succeeded.
    pub finished: bool,
    /// Terminal abort category, if any.
    pub aborted: Option<PackAbortReason>,
    /// Cancellation checks performed by a merged cancellation gate.
    pub cancellation_checks: u64,
    /// Whether a cancellation check observed cancellation.
    pub cancellation_observed: bool,
}

impl PackStreamReport {
    /// Merge cancellation counters from the producer's gate.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::ByteOverflow`] without changing the report when the exact check
    /// count cannot be represented.
    pub fn merge_cancellation(
        &mut self,
        cancellation: CancellationReport,
    ) -> Result<(), PackStreamError> {
        let checks = self
            .cancellation_checks
            .checked_add(cancellation.checks)
            .ok_or(PackStreamError::ByteOverflow)?;
        self.cancellation_checks = checks;
        self.cancellation_observed |= cancellation.observed;
        Ok(())
    }
}

/// Async output boundary implemented by buffered and transport-backed sinks.
///
/// Implementations must not return from `write_chunk` until the bytes are accepted into their
/// bounded output path. A write error must accept no bytes from that call and must leave the sink
/// terminally aborted, except [`PackStreamError::InvalidState`] which preserves the existing
/// terminal state. The trait is object-safe under the crate's `async_trait` convention and its
/// returned futures are `Send` because implementations are [`Send`].
#[async_trait]
pub trait PackChunkSink: Send {
    /// Write one bounded chunk, awaiting downstream backpressure.
    ///
    /// # Errors
    ///
    /// Returns state, capacity, allocation, or adapter-specific stream errors.
    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), PackStreamError>;

    /// Publish successful completion exactly once.
    ///
    /// An implementation that encounters a transport error must leave itself terminally aborted
    /// with [`PackAbortReason::SinkFailure`] and return [`PackStreamError::SinkFailure`].
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::InvalidState`] after finish or abort.
    async fn finish(&mut self) -> Result<(), PackStreamError>;

    /// Terminate the stream with a non-sensitive reason exactly once.
    ///
    /// For an open sink this operation must release unpublished resources and establish the
    /// aborted state even when adapter cleanup is best-effort.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::InvalidState`] after finish or abort.
    async fn abort(&mut self, reason: PackAbortReason) -> Result<(), PackStreamError>;

    /// Return current state without changing it.
    fn state(&self) -> PackSinkState;

    /// Return current counters without changing them.
    fn report(&self) -> PackStreamReport;
}

/// Runtime-neutral cancellation observation boundary.
pub trait CancellationProbe: Send + Sync {
    /// Return whether cancellation has been requested.
    fn is_cancelled(&self) -> bool;
}

/// Cloneable cancellation token backed by an atomic flag.
#[derive(Clone, Debug, Default)]
pub struct PackCancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl PackCancellationToken {
    /// Request cancellation. Repeating the call is harmless.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

impl CancellationProbe for PackCancellationToken {
    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

/// Cancellation probe that never cancels.
#[derive(Clone, Copy, Debug, Default)]
pub struct NeverCancel;

impl CancellationProbe for NeverCancel {
    fn is_cancelled(&self) -> bool {
        false
    }
}

/// Cancellation checkpoint counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CancellationReport {
    /// Probe calls performed.
    pub checks: u64,
    /// Whether cancellation was observed.
    pub observed: bool,
}

/// Byte-interval cancellation helper for a future pack writer.
#[derive(Clone, Debug)]
pub struct CancellationGate<P> {
    probe: P,
    max_observation_bytes: u64,
    checkpoint_bytes: u64,
    bytes_since_check: u64,
    report: CancellationReport,
}

impl<P: CancellationProbe> CancellationGate<P> {
    /// Construct a gate from validated stream limits and a caller-owned probe.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::InvalidLimits`] for invalid limits.
    pub fn new(probe: P, limits: PackStreamLimits) -> Result<Self, PackStreamError> {
        let limits = limits.validate()?;
        Ok(Self {
            probe,
            max_observation_bytes: u64::try_from(limits.max_chunk_bytes)
                .map_err(|_| PackStreamError::InvalidLimits)?,
            checkpoint_bytes: limits.cancellation_checkpoint_bytes,
            bytes_since_check: 0,
            report: CancellationReport::default(),
        })
    }

    /// Account progress and check cancellation when the configured interval is reached.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::ByteOverflow`] or [`PackStreamError::Cancelled`].
    pub fn observe_bytes(&mut self, bytes: u64) -> Result<(), PackStreamError> {
        if bytes > self.max_observation_bytes {
            return Err(PackStreamError::ChunkTooLarge);
        }
        self.bytes_since_check = self
            .bytes_since_check
            .checked_add(bytes)
            .ok_or(PackStreamError::ByteOverflow)?;
        if self.bytes_since_check < self.checkpoint_bytes {
            return Ok(());
        }
        self.bytes_since_check %= self.checkpoint_bytes;
        self.check_now()
    }

    /// Check immediately, including before the first backend read or output byte.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::Cancelled`] when the probe is cancelled.
    pub fn check_now(&mut self) -> Result<(), PackStreamError> {
        self.report.checks = self
            .report
            .checks
            .checked_add(1)
            .ok_or(PackStreamError::ByteOverflow)?;
        if self.probe.is_cancelled() {
            self.report.observed = true;
            return Err(PackStreamError::Cancelled);
        }
        Ok(())
    }

    /// Return cancellation counters.
    #[must_use]
    pub const fn report(&self) -> CancellationReport {
        self.report
    }
}

/// Buffered compatibility sink that publishes bytes only after successful finish.
///
/// This sink enforces chunk and total response bounds. It has no asynchronous producer queue, so
/// `max_queued_bytes` and `max_in_flight_bytes` remain contracts for a future transport/producer
/// pipeline rather than claims about this single retained compatibility buffer.
#[derive(Debug)]
pub struct BoundedVecPackSink {
    limits: PackStreamLimits,
    staging: Vec<u8>,
    published: Option<Vec<u8>>,
    state: PackSinkState,
    report: PackStreamReport,
}

impl BoundedVecPackSink {
    /// Construct an empty bounded sink without allocating the response maximum.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::InvalidLimits`] for invalid limits.
    pub fn new(limits: PackStreamLimits) -> Result<Self, PackStreamError> {
        Ok(Self {
            limits: limits.validate()?,
            staging: Vec::new(),
            published: None,
            state: PackSinkState::Open,
            report: PackStreamReport::default(),
        })
    }

    /// Borrow published bytes after successful finish.
    #[must_use]
    pub fn bytes(&self) -> Option<&[u8]> {
        self.published.as_deref()
    }

    /// Consume the sink and return bytes only if it finished successfully.
    ///
    /// # Errors
    ///
    /// Returns [`PackStreamError::InvalidState`] unless the sink is finished.
    pub fn into_bytes(mut self) -> Result<Vec<u8>, PackStreamError> {
        self.published.take().ok_or(PackStreamError::InvalidState {
            operation: PackSinkOperation::Finish,
            state: self.state,
        })
    }

    fn require_open(&self, operation: PackSinkOperation) -> Result<(), PackStreamError> {
        if self.state != PackSinkState::Open {
            return Err(PackStreamError::InvalidState {
                operation,
                state: self.state,
            });
        }
        Ok(())
    }

    fn fail_write(&mut self, error: PackStreamError, reason: PackAbortReason) -> PackStreamError {
        self.staging = Vec::new();
        self.state = PackSinkState::Aborted(reason);
        self.report.aborted = Some(reason);
        error
    }
}

#[async_trait]
impl PackChunkSink for BoundedVecPackSink {
    async fn write_chunk(&mut self, bytes: &[u8]) -> Result<(), PackStreamError> {
        self.require_open(PackSinkOperation::Write)?;
        if bytes.is_empty() {
            return Ok(());
        }
        if bytes.len() > self.limits.max_chunk_bytes {
            return Err(self.fail_write(
                PackStreamError::ChunkTooLarge,
                PackAbortReason::LimitExceeded,
            ));
        }
        let next = u64::try_from(self.staging.len())
            .ok()
            .and_then(|length| length.checked_add(u64::try_from(bytes.len()).ok()?))
            .ok_or(PackStreamError::ByteOverflow);
        let next = match next {
            Ok(next) => next,
            Err(error) => {
                return Err(self.fail_write(error, PackAbortReason::LimitExceeded));
            }
        };
        if next > self.limits.max_total_bytes {
            return Err(
                self.fail_write(PackStreamError::TotalLimit, PackAbortReason::LimitExceeded)
            );
        }
        let next_chunks = match self.report.emitted_chunks.checked_add(1) {
            Some(chunks) => chunks,
            None => {
                return Err(self.fail_write(
                    PackStreamError::ByteOverflow,
                    PackAbortReason::LimitExceeded,
                ));
            }
        };
        if self.staging.try_reserve(bytes.len()).is_err() {
            return Err(self.fail_write(PackStreamError::Allocation, PackAbortReason::SinkFailure));
        }
        self.staging.extend_from_slice(bytes);
        self.report.emitted_bytes = next;
        self.report.emitted_chunks = next_chunks;
        Ok(())
    }

    async fn finish(&mut self) -> Result<(), PackStreamError> {
        self.require_open(PackSinkOperation::Finish)?;
        self.published = Some(mem::take(&mut self.staging));
        self.state = PackSinkState::Finished;
        self.report.finished = true;
        Ok(())
    }

    async fn abort(&mut self, reason: PackAbortReason) -> Result<(), PackStreamError> {
        self.require_open(PackSinkOperation::Abort)?;
        self.staging = Vec::new();
        self.state = PackSinkState::Aborted(reason);
        self.report.aborted = Some(reason);
        Ok(())
    }

    fn state(&self) -> PackSinkState {
        self.state
    }

    fn report(&self) -> PackStreamReport {
        self.report
    }
}
