//! Correctness-first incremental non-delta PACK v2 writer.

use flate2::{Compress, Compression, FlushCompress, Status};
use grit_lib::objects::{HashAlgo, ObjectKind};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::packfile::{encode_pack_object_header, pack_type_code};
use crate::protocol::clone_metrics::CloneLimits;
use crate::protocol::pack_entry_reuse::{
    encode_header_u64, encode_ofs_delta_distance, PackEntryPlan,
};
use crate::protocol::pack_stream::{
    CancellationGate, CancellationProbe, CancellationReport, PackAbortReason, PackChunkSink,
    PackSinkState, PackStreamError, PackStreamLimits, PackStreamReport,
};
use crate::storage::StoredObject;

/// Incremental writer lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackWriterState {
    /// Validated, but no response bytes were sent.
    Ready,
    /// PACK header was sent and direct entries are expected.
    Streaming,
    /// Trailer and sink finish completed.
    Finished,
    /// A post-start error aborted the sink.
    Aborted,
}

/// Writer state or encoding failure.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackWriterError {
    /// Planned object count cannot be represented by PACK v2.
    #[error("pack object count exceeds u32")]
    ObjectCount,
    /// Clone or stream limits are invalid or inconsistent.
    #[error("invalid incremental pack limits")]
    InvalidLimits,
    /// Operation is not valid in the current writer state.
    #[error("incremental pack operation is invalid in state {0:?}")]
    InvalidState(PackWriterState),
    /// More entries were supplied than declared in the header.
    #[error("incremental pack received too many objects")]
    TooManyObjects,
    /// Finish was requested before every planned entry was written.
    #[error("incremental pack object count does not match plan")]
    ObjectCountMismatch,
    /// Decoded or compressed byte accounting overflowed.
    #[error("incremental pack byte accounting overflow")]
    ByteOverflow,
    /// Output would exceed validated clone or stream limits.
    #[error("incremental pack exceeds output limit")]
    OutputLimit,
    /// A bounded compression/header buffer could not be reserved.
    #[error("incremental pack buffer allocation failed")]
    Allocation,
    /// Zlib did not complete a valid stream.
    #[error("incremental pack compression failed")]
    Compression,
    /// A recompression plan was executed without its decoded fallback object.
    #[error("incremental pack recompression fallback object is missing")]
    MissingFallback,
    /// Cancellation was observed at an explicit checkpoint.
    #[error("incremental pack cancelled")]
    Cancelled,
    /// Output sink rejected a write, finish, or abort.
    #[error("incremental pack sink failed: {0}")]
    Sink(PackStreamError),
}

/// Writer and sink counters captured at success or failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackWriterReport {
    /// Current writer state.
    pub state: PackWriterState,
    /// Object count declared in the header.
    pub planned_objects: u32,
    /// Complete direct entries written.
    pub written_objects: u32,
    /// Decoded payload bytes consumed.
    pub decoded_bytes: u64,
    /// Zlib stream bytes emitted for entries.
    pub compressed_bytes: u64,
    /// Largest decoded object retained by this sequential writer.
    pub peak_decoded_object_bytes: u64,
    /// Largest bounded compressed chunk.
    pub peak_compressed_chunk_bytes: u64,
    /// Sink counters and terminal state.
    pub stream: PackStreamReport,
    /// Cancellation checkpoints.
    pub cancellation: CancellationReport,
}

/// Original typed writer error with counters preserved after failure handling.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
#[error("{error}")]
pub struct PackWriterFailure {
    /// Original error that triggered failure.
    pub error: PackWriterError,
    /// Point-in-time report after best-effort abort.
    pub report: PackWriterReport,
}

/// Per-entry work returned after the complete entry reaches the sink.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PackObjectWriteReport {
    /// Git object kind.
    pub kind: ObjectKind,
    /// Decoded object bytes.
    pub decoded_bytes: u64,
    /// Direct-entry zlib bytes, excluding its PACK entry header.
    pub compressed_bytes: u64,
}

enum PackHasher {
    Sha1(Sha1),
    Sha256(Sha256),
}

impl PackHasher {
    fn new(hash_algo: HashAlgo) -> Self {
        match hash_algo {
            HashAlgo::Sha1 => Self::Sha1(Sha1::new()),
            HashAlgo::Sha256 => Self::Sha256(Sha256::new()),
        }
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Sha1(hasher) => hasher.update(bytes),
            Self::Sha256(hasher) => hasher.update(bytes),
        }
    }

    fn finalize(self) -> Vec<u8> {
        match self {
            Self::Sha1(hasher) => hasher.finalize().to_vec(),
            Self::Sha256(hasher) => hasher.finalize().to_vec(),
        }
    }
}

/// Sequential PACK v2 writer over one backpressure-capable sink.
pub struct IncrementalPackWriter<'a, P> {
    sink: &'a mut dyn PackChunkSink,
    state: PackWriterState,
    planned_objects: u32,
    written_objects: u32,
    hash_algo: HashAlgo,
    hasher: Option<PackHasher>,
    stream_limits: PackStreamLimits,
    max_output_bytes: u64,
    output_bytes: u64,
    decoded_bytes: u64,
    compressed_bytes: u64,
    peak_decoded_object_bytes: u64,
    peak_compressed_chunk_bytes: u64,
    cancellation: CancellationGate<P>,
}

impl<'a, P: CancellationProbe> IncrementalPackWriter<'a, P> {
    /// Validate a complete plan before any bytes can be sent.
    ///
    /// # Errors
    ///
    /// Returns count or limit errors without touching the sink.
    pub fn new(
        sink: &'a mut dyn PackChunkSink,
        planned_objects: usize,
        hash_algo: HashAlgo,
        stream_limits: PackStreamLimits,
        clone_limits: CloneLimits,
        cancellation: P,
    ) -> Result<Self, PackWriterError> {
        let planned_objects =
            u32::try_from(planned_objects).map_err(|_| PackWriterError::ObjectCount)?;
        let stream_limits = stream_limits
            .validate()
            .map_err(|_| PackWriterError::InvalidLimits)?;
        let clone_limits = clone_limits
            .validate()
            .map_err(|_| PackWriterError::InvalidLimits)?;
        let planned_count =
            usize::try_from(planned_objects).map_err(|_| PackWriterError::ObjectCount)?;
        let sink_report = sink.report();
        let max_output_bytes = stream_limits
            .max_total_bytes
            .min(clone_limits.max_output_bytes);
        let max_chunk_bytes = u64::try_from(stream_limits.max_chunk_bytes)
            .map_err(|_| PackWriterError::InvalidLimits)?;
        let minimum_pack_bytes = 12_u64
            .checked_add(u64::try_from(hash_algo.len()).map_err(|_| PackWriterError::ByteOverflow)?)
            .ok_or(PackWriterError::ByteOverflow)?;
        if planned_count > clone_limits.max_selected_objects
            || max_chunk_bytes > clone_limits.encoded_bytes_in_flight
            || stream_limits.max_chunk_bytes < 12_usize.max(hash_algo.len())
            || max_output_bytes < minimum_pack_bytes
            || sink.state() != PackSinkState::Open
            || sink_report.emitted_bytes != 0
            || sink_report.emitted_chunks != 0
        {
            return Err(PackWriterError::InvalidLimits);
        }
        let cancellation = CancellationGate::new(cancellation, stream_limits)
            .map_err(|_| PackWriterError::InvalidLimits)?;
        Ok(Self {
            sink,
            state: PackWriterState::Ready,
            planned_objects,
            written_objects: 0,
            hash_algo,
            hasher: Some(PackHasher::new(hash_algo)),
            stream_limits,
            max_output_bytes,
            output_bytes: 0,
            decoded_bytes: 0,
            compressed_bytes: 0,
            peak_decoded_object_bytes: 0,
            peak_compressed_chunk_bytes: 0,
            cancellation,
        })
    }

    /// Emit the PACK v2 signature, version, and planned count.
    ///
    /// # Errors
    ///
    /// A failure before the header is accepted leaves an untouched sink open. Once any bytes are
    /// accepted, cancellation or output failure terminally aborts the stream.
    pub async fn start(&mut self) -> Result<(), PackWriterFailure> {
        if self.state != PackWriterState::Ready {
            return Err(self.failure(PackWriterError::InvalidState(self.state)));
        }
        let result = async {
            self.cancellation
                .check_now()
                .map_err(map_stream_writer_error)?;
            let mut header = [0_u8; 12];
            header[..4].copy_from_slice(b"PACK");
            header[4..8].copy_from_slice(&2_u32.to_be_bytes());
            header[8..].copy_from_slice(&self.planned_objects.to_be_bytes());
            self.write_hashed(&header).await?;
            Ok(())
        }
        .await;
        match result {
            Ok(()) => {
                self.state = PackWriterState::Streaming;
                Ok(())
            }
            Err(error) => Err(self.abort_for(error).await),
        }
    }

    /// Check cancellation immediately before a backend object read.
    ///
    /// # Errors
    ///
    /// Aborts an already-started stream when cancellation is observed.
    pub async fn checkpoint_before_read(&mut self) -> Result<(), PackWriterFailure> {
        if self.state != PackWriterState::Streaming {
            return Err(self.failure(PackWriterError::InvalidState(self.state)));
        }
        match self
            .cancellation
            .check_now()
            .map_err(map_stream_writer_error)
        {
            Ok(()) => Ok(()),
            Err(error) => Err(self.abort_for(error).await),
        }
    }

    /// Write one complete direct object entry using bounded zlib output chunks.
    ///
    /// # Errors
    ///
    /// Aborts on ordering, allocation, compression, cancellation, limit, or sink failure.
    pub async fn write_object(
        &mut self,
        object: &StoredObject,
    ) -> Result<PackObjectWriteReport, PackWriterFailure> {
        if self.state != PackWriterState::Streaming {
            return Err(self.failure(PackWriterError::InvalidState(self.state)));
        }
        if self.written_objects >= self.planned_objects {
            return Err(self.abort_for(PackWriterError::TooManyObjects).await);
        }
        let next_written_objects = self
            .written_objects
            .checked_add(1)
            .ok_or_else(|| self.failure(PackWriterError::ByteOverflow))?;
        match self.write_object_inner(object).await {
            Ok(report) => {
                self.written_objects = next_written_objects;
                Ok(report)
            }
            Err(error) => Err(self.abort_for(error).await),
        }
    }

    /// Emit the incremental body checksum and finish the sink exactly once.
    ///
    /// # Errors
    ///
    /// Aborts when the supplied object count is incomplete or trailer/output fails.
    pub async fn finish(&mut self) -> Result<PackWriterReport, PackWriterFailure> {
        if self.state != PackWriterState::Streaming {
            return Err(self.failure(PackWriterError::InvalidState(self.state)));
        }
        if self.written_objects != self.planned_objects {
            return Err(self.abort_for(PackWriterError::ObjectCountMismatch).await);
        }
        if let Err(error) = self
            .cancellation
            .check_now()
            .map_err(map_stream_writer_error)
        {
            return Err(self.abort_for(error).await);
        }
        let Some(hasher) = self.hasher.take() else {
            return Err(self
                .abort_for(PackWriterError::InvalidState(self.state))
                .await);
        };
        let trailer = hasher.finalize();
        let result = async {
            self.write_unhashed(&trailer).await?;
            self.sink.finish().await.map_err(PackWriterError::Sink)
        }
        .await;
        match result {
            Ok(()) => {
                self.state = PackWriterState::Finished;
                Ok(self.report())
            }
            Err(error) => Err(self.abort_for(error).await),
        }
    }

    /// Abort a started writer for an external backend/protocol failure.
    ///
    /// A ready writer whose sink has accepted no bytes remains ready and open.
    pub async fn abort(&mut self, reason: PackAbortReason) -> PackWriterReport {
        if matches!(
            self.state,
            PackWriterState::Ready | PackWriterState::Streaming
        ) {
            let output_started = self.sink.report().emitted_bytes > 0;
            if output_started && self.sink.state() == PackSinkState::Open {
                let _ = self.sink.abort(reason).await;
            }
            if output_started || self.sink.state() != PackSinkState::Open {
                self.state = PackWriterState::Aborted;
            }
        }
        self.report()
    }

    /// Return current writer, sink, and cancellation counters.
    #[must_use]
    pub fn report(&self) -> PackWriterReport {
        PackWriterReport {
            state: self.state,
            planned_objects: self.planned_objects,
            written_objects: self.written_objects,
            decoded_bytes: self.decoded_bytes,
            compressed_bytes: self.compressed_bytes,
            peak_decoded_object_bytes: self.peak_decoded_object_bytes,
            peak_compressed_chunk_bytes: self.peak_compressed_chunk_bytes,
            stream: self.sink.report(),
            cancellation: self.cancellation.report(),
        }
    }

    /// Return the absolute offset where the next PACK entry header will be written.
    ///
    /// Callers use this offset when proving that an OFS-delta base is earlier in the same output
    /// pack. The value includes the 12-byte PACK header and every completely accepted entry byte.
    #[must_use]
    pub const fn next_entry_offset(&self) -> u64 {
        self.output_bytes
    }

    /// Execute one validated compressed-entry plan or the existing recompression fallback.
    ///
    /// `fallback` is required only for [`PackEntryPlan::RecompressFallback`]. Reused entries emit a
    /// fresh PACK entry header and stream retained zlib bytes in bounded chunks. OFS-delta distance
    /// is recomputed from the writer's current output offset, never copied from the source pack.
    ///
    /// # Errors
    ///
    /// Aborts on invalid writer state/order, a missing fallback object, stale OFS placement,
    /// allocation/accounting/limit failures, cancellation, or downstream failure.
    pub async fn write_entry_plan(
        &mut self,
        plan: &PackEntryPlan,
        fallback: Option<&StoredObject>,
    ) -> Result<PackObjectWriteReport, PackWriterFailure> {
        if matches!(plan, PackEntryPlan::RecompressFallback(_)) {
            let Some(object) = fallback else {
                return Err(self.abort_for(PackWriterError::MissingFallback).await);
            };
            return self.write_object(object).await;
        }
        if self.state != PackWriterState::Streaming {
            return Err(self.failure(PackWriterError::InvalidState(self.state)));
        }
        if self.written_objects >= self.planned_objects {
            return Err(self.abort_for(PackWriterError::TooManyObjects).await);
        }
        let next_written_objects = self
            .written_objects
            .checked_add(1)
            .ok_or_else(|| self.failure(PackWriterError::ByteOverflow))?;
        match self.write_reused_entry_inner(plan).await {
            Ok(report) => {
                self.written_objects = next_written_objects;
                Ok(report)
            }
            Err(error) => Err(self.abort_for(error).await),
        }
    }

    async fn write_reused_entry_inner(
        &mut self,
        plan: &PackEntryPlan,
    ) -> Result<PackObjectWriteReport, PackWriterError> {
        self.cancellation
            .check_now()
            .map_err(map_stream_writer_error)?;
        let (kind, decoded_size, declared_size, type_code, base_bytes, compressed) = match plan {
            PackEntryPlan::Direct(entry) => (
                entry.kind,
                entry.declared_size,
                entry.declared_size,
                pack_type_code(entry.kind),
                Vec::new(),
                entry.compressed.as_ref(),
            ),
            PackEntryPlan::RefDelta(entry) => {
                let mut base_bytes = Vec::new();
                base_bytes
                    .try_reserve_exact(entry.base.as_bytes().len())
                    .map_err(|_| PackWriterError::Allocation)?;
                base_bytes.extend_from_slice(entry.base.as_bytes());
                (
                    entry.result_kind,
                    entry.result_size,
                    entry.declared_size,
                    7,
                    base_bytes,
                    entry.compressed.as_ref(),
                )
            }
            PackEntryPlan::OfsDelta(entry) => {
                let distance = self
                    .output_bytes
                    .checked_sub(entry.base_output_offset)
                    .filter(|distance| *distance > 0)
                    .ok_or(PackWriterError::ByteOverflow)?;
                let mut distance_bytes = Vec::new();
                distance_bytes
                    .try_reserve_exact(10)
                    .map_err(|_| PackWriterError::Allocation)?;
                encode_ofs_delta_distance(&mut distance_bytes, distance)?;
                (
                    entry.result_kind,
                    entry.result_size,
                    entry.declared_size,
                    6,
                    distance_bytes,
                    entry.compressed.as_ref(),
                )
            }
            PackEntryPlan::RecompressFallback(_) => {
                return Err(PackWriterError::MissingFallback);
            }
        };
        let mut header = Vec::new();
        header
            .try_reserve_exact(16)
            .map_err(|_| PackWriterError::Allocation)?;
        encode_header_u64(&mut header, type_code, declared_size)?;
        self.write_hashed(&header).await?;
        if !base_bytes.is_empty() {
            self.write_hashed(&base_bytes).await?;
        }
        let mut compressed_bytes = 0_u64;
        for chunk in compressed.chunks(self.stream_limits.max_chunk_bytes) {
            self.cancellation
                .check_now()
                .map_err(map_stream_writer_error)?;
            let length = u64::try_from(chunk.len()).map_err(|_| PackWriterError::ByteOverflow)?;
            compressed_bytes = compressed_bytes
                .checked_add(length)
                .ok_or(PackWriterError::ByteOverflow)?;
            self.write_compressed_hashed(chunk).await?;
        }
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(decoded_size)
            .ok_or(PackWriterError::ByteOverflow)?;
        Ok(PackObjectWriteReport {
            kind,
            decoded_bytes: decoded_size,
            compressed_bytes,
        })
    }

    async fn write_object_inner(
        &mut self,
        object: &StoredObject,
    ) -> Result<PackObjectWriteReport, PackWriterError> {
        let decoded_bytes =
            u64::try_from(object.data.len()).map_err(|_| PackWriterError::ByteOverflow)?;
        self.peak_decoded_object_bytes = self.peak_decoded_object_bytes.max(decoded_bytes);
        let mut header = Vec::new();
        header
            .try_reserve_exact(16)
            .map_err(|_| PackWriterError::Allocation)?;
        encode_pack_object_header(&mut header, pack_type_code(object.kind), object.data.len());
        self.write_hashed(&header).await?;

        let mut output = Vec::new();
        output
            .try_reserve_exact(self.stream_limits.max_chunk_bytes)
            .map_err(|_| PackWriterError::Allocation)?;
        output.resize(self.stream_limits.max_chunk_bytes, 0);
        let mut compressor = Compress::new(Compression::default(), true);
        let mut input_offset = 0_usize;
        let mut object_compressed = 0_u64;
        loop {
            self.cancellation
                .check_now()
                .map_err(map_stream_writer_error)?;
            let input_end = input_offset
                .checked_add(self.stream_limits.max_chunk_bytes)
                .map(|end| end.min(object.data.len()))
                .ok_or(PackWriterError::ByteOverflow)?;
            let flush = if input_end == object.data.len() {
                FlushCompress::Finish
            } else {
                FlushCompress::None
            };
            let input_before = compressor.total_in();
            let output_before = compressor.total_out();
            let status = compressor
                .compress(&object.data[input_offset..input_end], &mut output, flush)
                .map_err(|_| PackWriterError::Compression)?;
            let consumed = compressor
                .total_in()
                .checked_sub(input_before)
                .ok_or(PackWriterError::ByteOverflow)?;
            let produced = compressor
                .total_out()
                .checked_sub(output_before)
                .ok_or(PackWriterError::ByteOverflow)?;
            input_offset = input_offset
                .checked_add(usize::try_from(consumed).map_err(|_| PackWriterError::ByteOverflow)?)
                .ok_or(PackWriterError::ByteOverflow)?;
            let produced = usize::try_from(produced).map_err(|_| PackWriterError::ByteOverflow)?;
            if produced > 0 {
                let produced_bytes =
                    u64::try_from(produced).map_err(|_| PackWriterError::ByteOverflow)?;
                let next_object_compressed = object_compressed
                    .checked_add(produced_bytes)
                    .ok_or(PackWriterError::ByteOverflow)?;
                self.write_compressed_hashed(&output[..produced]).await?;
                object_compressed = next_object_compressed;
            }
            if status == Status::StreamEnd {
                if input_offset != object.data.len() {
                    return Err(PackWriterError::Compression);
                }
                break;
            }
            if consumed == 0 && produced == 0 {
                return Err(PackWriterError::Compression);
            }
        }
        self.decoded_bytes = self
            .decoded_bytes
            .checked_add(decoded_bytes)
            .ok_or(PackWriterError::ByteOverflow)?;
        Ok(PackObjectWriteReport {
            kind: object.kind,
            decoded_bytes,
            compressed_bytes: object_compressed,
        })
    }

    async fn write_hashed(&mut self, bytes: &[u8]) -> Result<(), PackWriterError> {
        self.write_hashed_chunk(bytes, false).await.map(|_| ())
    }

    async fn write_compressed_hashed(&mut self, bytes: &[u8]) -> Result<(), PackWriterError> {
        self.write_hashed_chunk(bytes, true).await.map(|_| ())
    }

    async fn write_hashed_chunk(
        &mut self,
        bytes: &[u8],
        compressed: bool,
    ) -> Result<u64, PackWriterError> {
        self.ensure_output(bytes.len(), true)?;
        let byte_count = u64::try_from(bytes.len()).map_err(|_| PackWriterError::ByteOverflow)?;
        let output_bytes = self
            .output_bytes
            .checked_add(byte_count)
            .ok_or(PackWriterError::ByteOverflow)?;
        let compressed_bytes = if compressed {
            Some(
                self.compressed_bytes
                    .checked_add(byte_count)
                    .ok_or(PackWriterError::ByteOverflow)?,
            )
        } else {
            None
        };
        let hasher = self
            .hasher
            .as_mut()
            .ok_or(PackWriterError::InvalidState(self.state))?;
        self.sink
            .write_chunk(bytes)
            .await
            .map_err(PackWriterError::Sink)?;
        hasher.update(bytes);
        self.output_bytes = output_bytes;
        if let Some(compressed_bytes) = compressed_bytes {
            self.compressed_bytes = compressed_bytes;
            self.peak_compressed_chunk_bytes = self.peak_compressed_chunk_bytes.max(byte_count);
        }
        self.cancellation
            .observe_bytes(byte_count)
            .map_err(map_stream_writer_error)?;
        Ok(byte_count)
    }

    async fn write_unhashed(&mut self, bytes: &[u8]) -> Result<(), PackWriterError> {
        self.ensure_output(bytes.len(), false)?;
        let byte_count = u64::try_from(bytes.len()).map_err(|_| PackWriterError::ByteOverflow)?;
        let output_bytes = self
            .output_bytes
            .checked_add(byte_count)
            .ok_or(PackWriterError::ByteOverflow)?;
        self.sink
            .write_chunk(bytes)
            .await
            .map_err(PackWriterError::Sink)?;
        self.output_bytes = output_bytes;
        Ok(())
    }

    fn ensure_output(&self, bytes: usize, reserve_trailer: bool) -> Result<(), PackWriterError> {
        let bytes = u64::try_from(bytes).map_err(|_| PackWriterError::ByteOverflow)?;
        let trailer = if reserve_trailer {
            u64::try_from(self.hash_algo.len()).map_err(|_| PackWriterError::ByteOverflow)?
        } else {
            0
        };
        self.output_bytes
            .checked_add(bytes)
            .and_then(|total| total.checked_add(trailer))
            .filter(|total| *total <= self.max_output_bytes)
            .map(|_| ())
            .ok_or(PackWriterError::OutputLimit)
    }

    async fn abort_for(&mut self, error: PackWriterError) -> PackWriterFailure {
        let reason = abort_reason(error);
        let output_started = self.sink.report().emitted_bytes > 0;
        if output_started && self.sink.state() == PackSinkState::Open {
            let _ = self.sink.abort(reason).await;
        }
        if output_started || self.sink.state() != PackSinkState::Open {
            self.state = PackWriterState::Aborted;
        }
        self.failure(error)
    }

    fn failure(&self, error: PackWriterError) -> PackWriterFailure {
        PackWriterFailure {
            error,
            report: self.report(),
        }
    }
}

fn map_stream_writer_error(error: PackStreamError) -> PackWriterError {
    match error {
        PackStreamError::Cancelled => PackWriterError::Cancelled,
        PackStreamError::ByteOverflow => PackWriterError::ByteOverflow,
        PackStreamError::Allocation => PackWriterError::Allocation,
        PackStreamError::TotalLimit | PackStreamError::ChunkTooLarge => {
            PackWriterError::OutputLimit
        }
        other => PackWriterError::Sink(other),
    }
}

fn abort_reason(error: PackWriterError) -> PackAbortReason {
    match error {
        PackWriterError::Cancelled => PackAbortReason::Cancelled,
        PackWriterError::InvalidLimits
        | PackWriterError::ObjectCount
        | PackWriterError::TooManyObjects
        | PackWriterError::ObjectCountMismatch
        | PackWriterError::ByteOverflow
        | PackWriterError::OutputLimit => PackAbortReason::LimitExceeded,
        PackWriterError::Sink(_) => PackAbortReason::SinkFailure,
        PackWriterError::InvalidState(_)
        | PackWriterError::Allocation
        | PackWriterError::Compression
        | PackWriterError::MissingFallback => PackAbortReason::EncodingFailure,
    }
}
