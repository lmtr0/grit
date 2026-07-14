//! Conservative planning for reuse of immutable compressed PACK entries.

use std::collections::HashMap;
use std::sync::Arc;

use flate2::{Decompress, FlushDecompress, Status};
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};

use crate::protocol::pack_writer::PackWriterError;
use crate::storage::{PackMetadata, StoredObject};

/// Maximum encoded PACK entry header accepted from a validated source pack.
pub const MAX_REUSED_ENTRY_HEADER_BYTES: usize = 16;
/// Maximum inflated delta bytes retained temporarily while validating instructions.
pub const MAX_REUSED_DELTA_INSTRUCTION_BYTES: usize = 64 * 1024 * 1024;

/// Source representation of a previously validated PACK entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoredPackEntryKind {
    /// Complete non-delta object representation.
    Direct(ObjectKind),
    /// Delta instructions whose base is named by object ID.
    RefDelta {
        /// Resolved base object ID recorded during source-pack validation.
        base: ObjectId,
        /// Decoded base size verified while resolving the source delta chain.
        base_size: u64,
        /// Final object kind after applying the delta.
        result_kind: ObjectKind,
    },
    /// Delta instructions whose source offset was resolved to an object ID during validation.
    OfsDelta {
        /// Resolved base object ID; the old source-pack distance is deliberately not retained.
        base: ObjectId,
        /// Decoded base size verified while resolving the source delta chain.
        base_size: u64,
        /// Final object kind after applying the delta.
        result_kind: ObjectKind,
    },
}

impl StoredPackEntryKind {
    const fn type_code(self) -> u8 {
        match self {
            Self::Direct(ObjectKind::Commit) => 1,
            Self::Direct(ObjectKind::Tree) => 2,
            Self::Direct(ObjectKind::Blob) => 3,
            Self::Direct(ObjectKind::Tag) => 4,
            Self::OfsDelta { .. } => 6,
            Self::RefDelta { .. } => 7,
        }
    }
}

/// Structural source-pack validation failure that makes compressed reuse ineligible.
#[derive(Clone, Copy, Debug, thiserror::Error, PartialEq, Eq)]
pub enum PackEntrySourceError {
    /// Pack metadata checksum width or total size is inconsistent with its object format.
    #[error("source pack metadata is invalid")]
    InvalidPack,
    /// Entry header or compressed stream does not fit completely before the source trailer.
    #[error("source pack entry is out of bounds")]
    OutOfBounds,
    /// Encoded source entry header is truncated, oversized, or non-canonical.
    #[error("source pack entry header is invalid")]
    InvalidHeader,
    /// Header type differs from the validated source index kind.
    #[error("source pack entry type differs from validated metadata")]
    TypeMismatch,
    /// Header-declared inflated representation size differs from validated source metadata.
    #[error("source pack entry size differs from validated metadata")]
    SizeMismatch,
    /// A retained zlib stream cannot be empty.
    #[error("source pack entry has an empty compressed stream")]
    EmptyCompressedStream,
    /// Retained bytes are not exactly one complete zlib stream of the declared size.
    #[error("source pack entry zlib stream is invalid")]
    InvalidCompressedStream,
    /// Delta header base/result sizes differ from resolved source-chain metadata.
    #[error("source pack delta header differs from resolved metadata")]
    InvalidDeltaHeader,
    /// Delta instructions are malformed or do not produce the resolved result size.
    #[error("source pack delta instructions are invalid")]
    InvalidDeltaInstructions,
    /// Delta validation would exceed its bounded temporary allocation.
    #[error("source pack delta instructions exceed the validation bound")]
    DeltaInstructionsTooLarge,
    /// Checked size conversion or source-bound arithmetic overflowed.
    #[error("source pack entry arithmetic overflow")]
    ByteOverflow,
}

/// Opaque evidence that source metadata, entry header, and byte bounds agreed at validation time.
///
/// Construction checks the immutable pack checksum width, source bounds, exact encoded type and
/// declared inflated representation size. Construction also inflates exactly one bounded zlib
/// stream, rejects trailing bytes, and validates delta instructions before retaining the immutable
/// compressed allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidatedPackEntry {
    source_pack_checksum: ObjectId,
    source_offset: u64,
    kind: StoredPackEntryKind,
    declared_size: u64,
    result_size: u64,
    result_oid: ObjectId,
    compressed: Arc<[u8]>,
}

impl ValidatedPackEntry {
    /// Validate and bind retained zlib bytes to immutable pack and entry metadata.
    ///
    /// `source_header` starts at `source_offset` and excludes delta-base bytes. `compressed`
    /// contains exactly one zlib stream and excludes both the entry and delta-base headers.
    /// `declared_size` is the PACK header size: object bytes for direct entries and inflated delta
    /// instruction bytes for deltas. `result_size` is the decoded result object size.
    ///
    /// # Errors
    ///
    /// Returns a typed error for checksum width, source bounds, header type/size disagreement,
    /// malformed or over-budget compressed/delta data, or checked arithmetic failure.
    #[allow(dead_code)]
    pub(crate) fn from_prevalidated_pack(
        pack: &PackMetadata,
        hash_algo: HashAlgo,
        source_offset: u64,
        source_header: &[u8],
        delta_base_header_bytes: usize,
        kind: StoredPackEntryKind,
        declared_size: u64,
        result_size: u64,
        result_oid: ObjectId,
        compressed: Arc<[u8]>,
    ) -> Result<Self, PackEntrySourceError> {
        let trailer_len = hash_algo.len();
        if pack.pack_checksum.len() != trailer_len
            || pack.pack_checksum.iter().all(|byte| *byte == 0)
            || pack.object_count == 0
            || pack.size_bytes
                < 12_u64
                    .checked_add(
                        u64::try_from(trailer_len)
                            .map_err(|_| PackEntrySourceError::ByteOverflow)?,
                    )
                    .ok_or(PackEntrySourceError::ByteOverflow)?
        {
            return Err(PackEntrySourceError::InvalidPack);
        }
        if compressed.is_empty() {
            return Err(PackEntrySourceError::EmptyCompressedStream);
        }
        if result_oid.algo() != hash_algo || result_oid.is_zero() {
            return Err(PackEntrySourceError::TypeMismatch);
        }
        let (type_code, encoded_size, consumed) = decode_source_header(source_header)?;
        if consumed != source_header.len() {
            return Err(PackEntrySourceError::InvalidHeader);
        }
        if type_code != kind.type_code() {
            return Err(PackEntrySourceError::TypeMismatch);
        }
        if encoded_size != declared_size {
            return Err(PackEntrySourceError::SizeMismatch);
        }
        if matches!(kind, StoredPackEntryKind::Direct(_)) && declared_size != result_size {
            return Err(PackEntrySourceError::SizeMismatch);
        }
        let is_delta = !matches!(kind, StoredPackEntryKind::Direct(_));
        let delta_instructions = validate_zlib_stream(&compressed, declared_size, is_delta)?;
        match kind {
            StoredPackEntryKind::Direct(_) => {}
            StoredPackEntryKind::RefDelta { base_size, .. }
            | StoredPackEntryKind::OfsDelta { base_size, .. } => {
                validate_delta_instructions(&delta_instructions, base_size, result_size)?;
            }
        }
        let entry_span = source_header
            .len()
            .checked_add(delta_base_header_bytes)
            .and_then(|span| span.checked_add(compressed.len()))
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        let entry_end = source_offset
            .checked_add(u64::try_from(entry_span).map_err(|_| PackEntrySourceError::ByteOverflow)?)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        let body_end = pack
            .size_bytes
            .checked_sub(
                u64::try_from(trailer_len).map_err(|_| PackEntrySourceError::ByteOverflow)?,
            )
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        if source_offset < 12 || entry_end > body_end {
            return Err(PackEntrySourceError::OutOfBounds);
        }
        if match kind {
            StoredPackEntryKind::RefDelta { base, .. }
            | StoredPackEntryKind::OfsDelta { base, .. } => {
                base.algo() != hash_algo || base.is_zero()
            }
            StoredPackEntryKind::Direct(_) => false,
        } {
            return Err(PackEntrySourceError::TypeMismatch);
        }
        let expected_base_header = match kind {
            StoredPackEntryKind::Direct(_) => 0,
            StoredPackEntryKind::RefDelta { base, .. } => base.algo().len(),
            StoredPackEntryKind::OfsDelta { .. } => delta_base_header_bytes,
        };
        if delta_base_header_bytes != expected_base_header
            || matches!(kind, StoredPackEntryKind::OfsDelta { .. })
                && !(1..=10).contains(&delta_base_header_bytes)
        {
            return Err(PackEntrySourceError::InvalidHeader);
        }
        let source_pack_checksum = ObjectId::from_bytes(&pack.pack_checksum)
            .map_err(|_| PackEntrySourceError::InvalidPack)?;
        Ok(Self {
            source_pack_checksum,
            source_offset,
            kind,
            declared_size,
            result_size,
            result_oid,
            compressed,
        })
    }

    /// Checksum of the immutable source pack that established this evidence.
    #[must_use]
    pub fn source_pack_checksum(&self) -> &[u8] {
        self.source_pack_checksum.as_bytes()
    }

    /// Absolute source-pack entry offset.
    #[must_use]
    pub const fn source_offset(&self) -> u64 {
        self.source_offset
    }

    /// Return the validated source representation kind and resolved delta dependency.
    #[must_use]
    pub const fn kind(&self) -> StoredPackEntryKind {
        self.kind
    }

    pub(crate) fn matches_result(&self, oid: ObjectId, object: &StoredObject) -> bool {
        self.result_oid == oid
            && self.result_oid.algo() == oid.algo()
            && u64::try_from(object.data.len()).ok() == Some(self.result_size)
            && match self.kind {
                StoredPackEntryKind::Direct(kind) => kind == object.kind,
                StoredPackEntryKind::RefDelta { result_kind, .. }
                | StoredPackEntryKind::OfsDelta { result_kind, .. } => result_kind == object.kind,
            }
    }
}

/// Proven bases available to delta entries in deterministic output construction.
#[derive(Clone, Debug, Default)]
pub struct PackDependencySet {
    outgoing_offsets: HashMap<ObjectId, u64>,
}

impl PackDependencySet {
    /// Create an empty dependency contract.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record one object after its entry header has been emitted in the outgoing pack.
    pub(crate) fn record_outgoing(&mut self, oid: ObjectId, output_offset: u64) {
        self.outgoing_offsets.entry(oid).or_insert(output_offset);
    }

    fn outgoing_offset(&self, oid: &ObjectId) -> Option<u64> {
        self.outgoing_offsets.get(oid).copied()
    }

    pub(crate) fn outgoing_offset_for(&self, oid: &ObjectId) -> Option<u64> {
        self.outgoing_offset(oid)
    }
}

/// Byte-affecting delta representation negotiated for an outgoing pack.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PackReuseCapabilities {
    /// Receiver accepts OFS-delta entries.
    pub ofs_delta: bool,
}

/// Reason a retained representation safely routes to the existing recompression path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackRecompressReason {
    /// Source metadata or retained bytes failed structural validation.
    InvalidSource(PackEntrySourceError),
    /// Delta base is absent from both the earlier outgoing pack and negotiated receiver closure.
    UnprovenBase,
    /// Stored OFS-delta cannot be represented because OFS-delta was not negotiated.
    OfsDeltaUnsupported,
    /// Stored OFS-delta base is not earlier than the entry's new output offset.
    InvalidOutputDistance,
}

/// Complete deterministic action for one outgoing PACK entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PackEntryPlan {
    /// Emit a fresh direct header followed by retained zlib object bytes.
    Direct(ReusedDirectEntry),
    /// Emit a fresh REF-delta header/base ID followed by retained zlib instruction bytes.
    RefDelta(ReusedRefDeltaEntry),
    /// Emit a fresh OFS-delta header/new distance followed by retained zlib instruction bytes.
    OfsDelta(ReusedOfsDeltaEntry),
    /// Use the existing bounded streaming compressor for the decoded object.
    RecompressFallback(PackRecompressReason),
}

/// Validated retained direct entry payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReusedDirectEntry {
    pub(crate) kind: ObjectKind,
    pub(crate) declared_size: u64,
    pub(crate) compressed: Arc<[u8]>,
}

/// Validated retained REF-delta instruction payload and proven base.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReusedRefDeltaEntry {
    pub(crate) result_kind: ObjectKind,
    pub(crate) result_size: u64,
    pub(crate) declared_size: u64,
    pub(crate) base: ObjectId,
    pub(crate) compressed: Arc<[u8]>,
}

/// Validated retained OFS-delta instruction payload with a recomputed output distance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReusedOfsDeltaEntry {
    pub(crate) result_kind: ObjectKind,
    pub(crate) result_size: u64,
    pub(crate) declared_size: u64,
    pub(crate) base_output_offset: u64,
    pub(crate) compressed: Arc<[u8]>,
}

impl PackEntryPlan {
    /// Convert source validation directly into reuse or a safe recompression fallback.
    ///
    /// This is the preferred adapter entry point: malformed, truncated, or inconsistent source
    /// metadata cannot escape as a reusable representation.
    #[must_use]
    pub fn from_source_validation(
        source: Result<ValidatedPackEntry, PackEntrySourceError>,
        dependencies: &PackDependencySet,
        capabilities: PackReuseCapabilities,
        output_offset: u64,
    ) -> Self {
        match source {
            Ok(source) => {
                Self::from_validated_entry(&source, dependencies, capabilities, output_offset)
            }
            Err(error) => Self::RecompressFallback(PackRecompressReason::InvalidSource(error)),
        }
    }

    /// Build a reuse plan from source evidence and the exact current output dependency contract.
    ///
    /// Unsupported or unproven delta representations return a typed recompression plan. This
    /// function never guesses that a receiver owns a base and never retains a source OFS distance.
    #[must_use]
    pub fn from_validated_entry(
        source: &ValidatedPackEntry,
        dependencies: &PackDependencySet,
        capabilities: PackReuseCapabilities,
        output_offset: u64,
    ) -> Self {
        match source.kind {
            StoredPackEntryKind::Direct(kind) => Self::Direct(ReusedDirectEntry {
                kind,
                declared_size: source.declared_size,
                compressed: source.compressed.clone(),
            }),
            StoredPackEntryKind::RefDelta {
                base, result_kind, ..
            } => {
                let base_is_outgoing = dependencies
                    .outgoing_offset(&base)
                    .is_some_and(|offset| offset < output_offset);
                if !base_is_outgoing {
                    return Self::RecompressFallback(PackRecompressReason::UnprovenBase);
                }
                Self::RefDelta(ReusedRefDeltaEntry {
                    result_kind,
                    result_size: source.result_size,
                    declared_size: source.declared_size,
                    base,
                    compressed: source.compressed.clone(),
                })
            }
            StoredPackEntryKind::OfsDelta {
                base, result_kind, ..
            } => {
                if !capabilities.ofs_delta {
                    return Self::RecompressFallback(PackRecompressReason::OfsDeltaUnsupported);
                }
                let Some(base_offset) = dependencies.outgoing_offset(&base) else {
                    return Self::RecompressFallback(PackRecompressReason::UnprovenBase);
                };
                let Some(_distance) = output_offset
                    .checked_sub(base_offset)
                    .filter(|value| *value > 0)
                else {
                    return Self::RecompressFallback(PackRecompressReason::InvalidOutputDistance);
                };
                Self::OfsDelta(ReusedOfsDeltaEntry {
                    result_kind,
                    result_size: source.result_size,
                    declared_size: source.declared_size,
                    base_output_offset: base_offset,
                    compressed: source.compressed.clone(),
                })
            }
        }
    }

    /// Final object kind used for object metrics, if this plan reuses compressed bytes.
    #[must_use]
    pub const fn reused_kind(&self) -> Option<ObjectKind> {
        match self {
            Self::Direct(entry) => Some(entry.kind),
            Self::RefDelta(entry) => Some(entry.result_kind),
            Self::OfsDelta(entry) => Some(entry.result_kind),
            Self::RecompressFallback(_) => None,
        }
    }
}

fn validate_zlib_stream(
    compressed: &[u8],
    declared_size: u64,
    retain_output: bool,
) -> Result<Vec<u8>, PackEntrySourceError> {
    const OUTPUT_CHUNK_BYTES: usize = 8 * 1024;

    let compressed_len =
        u64::try_from(compressed.len()).map_err(|_| PackEntrySourceError::ByteOverflow)?;
    let retained_capacity = if retain_output {
        let size = usize::try_from(declared_size)
            .map_err(|_| PackEntrySourceError::DeltaInstructionsTooLarge)?;
        if size > MAX_REUSED_DELTA_INSTRUCTION_BYTES {
            return Err(PackEntrySourceError::DeltaInstructionsTooLarge);
        }
        size
    } else {
        0
    };
    let mut decoder = Decompress::new(true);
    let mut output = [0_u8; OUTPUT_CHUNK_BYTES];
    let mut retained = Vec::new();
    retained
        .try_reserve_exact(retained_capacity)
        .map_err(|_| PackEntrySourceError::ByteOverflow)?;
    loop {
        let input_offset =
            usize::try_from(decoder.total_in()).map_err(|_| PackEntrySourceError::ByteOverflow)?;
        let input = compressed
            .get(input_offset..)
            .ok_or(PackEntrySourceError::InvalidCompressedStream)?;
        let input_before = decoder.total_in();
        let output_before = decoder.total_out();
        let status = decoder
            .decompress(input, &mut output, FlushDecompress::Finish)
            .map_err(|_| PackEntrySourceError::InvalidCompressedStream)?;
        let consumed = decoder
            .total_in()
            .checked_sub(input_before)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        let produced = decoder
            .total_out()
            .checked_sub(output_before)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        if decoder.total_out() > declared_size {
            return Err(PackEntrySourceError::SizeMismatch);
        }
        let produced = usize::try_from(produced).map_err(|_| PackEntrySourceError::ByteOverflow)?;
        if retain_output {
            retained.extend_from_slice(&output[..produced]);
        }

        if status == Status::StreamEnd {
            if decoder.total_in() != compressed_len || decoder.total_out() != declared_size {
                return Err(PackEntrySourceError::InvalidCompressedStream);
            }
            return Ok(retained);
        }
        if consumed == 0 && produced == 0 {
            return Err(PackEntrySourceError::InvalidCompressedStream);
        }
    }
}

fn validate_delta_instructions(
    instructions: &[u8],
    expected_base_size: u64,
    expected_result_size: u64,
) -> Result<(), PackEntrySourceError> {
    let (base_size, mut cursor) = decode_delta_size(instructions, 0)?;
    let (result_size, next) = decode_delta_size(instructions, cursor)?;
    cursor = next;
    if base_size != expected_base_size || result_size != expected_result_size {
        return Err(PackEntrySourceError::InvalidDeltaHeader);
    }
    let mut produced = 0_u64;
    while cursor < instructions.len() {
        let opcode = instructions[cursor];
        cursor = cursor
            .checked_add(1)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        if opcode == 0 {
            return Err(PackEntrySourceError::InvalidDeltaInstructions);
        }
        if opcode & 0x80 == 0 {
            cursor = cursor
                .checked_add(usize::from(opcode))
                .filter(|end| *end <= instructions.len())
                .ok_or(PackEntrySourceError::InvalidDeltaInstructions)?;
            produced = produced
                .checked_add(u64::from(opcode))
                .ok_or(PackEntrySourceError::ByteOverflow)?;
            continue;
        }
        let mut copy_offset = 0_u64;
        let mut copy_size = 0_u64;
        for (mask, shift) in [(0x01, 0), (0x02, 8), (0x04, 16), (0x08, 24)] {
            if opcode & mask != 0 {
                copy_offset |= u64::from(read_delta_byte(instructions, &mut cursor)?) << shift;
            }
        }
        for (mask, shift) in [(0x10, 0), (0x20, 8), (0x40, 16)] {
            if opcode & mask != 0 {
                copy_size |= u64::from(read_delta_byte(instructions, &mut cursor)?) << shift;
            }
        }
        if copy_size == 0 {
            copy_size = 0x1_0000;
        }
        if copy_offset
            .checked_add(copy_size)
            .is_none_or(|end| end > base_size)
        {
            return Err(PackEntrySourceError::InvalidDeltaInstructions);
        }
        produced = produced
            .checked_add(copy_size)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
    }
    if produced != result_size {
        return Err(PackEntrySourceError::InvalidDeltaInstructions);
    }
    Ok(())
}

fn read_delta_byte(bytes: &[u8], cursor: &mut usize) -> Result<u8, PackEntrySourceError> {
    let byte = *bytes
        .get(*cursor)
        .ok_or(PackEntrySourceError::InvalidDeltaInstructions)?;
    *cursor = cursor
        .checked_add(1)
        .ok_or(PackEntrySourceError::ByteOverflow)?;
    Ok(byte)
}

fn decode_delta_size(bytes: &[u8], start: usize) -> Result<(u64, usize), PackEntrySourceError> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let mut cursor = start;
    loop {
        let byte = *bytes
            .get(cursor)
            .ok_or(PackEntrySourceError::InvalidDeltaHeader)?;
        let part = u64::from(byte & 0x7f)
            .checked_shl(shift)
            .ok_or(PackEntrySourceError::InvalidDeltaHeader)?;
        value = value
            .checked_add(part)
            .ok_or(PackEntrySourceError::InvalidDeltaHeader)?;
        cursor = cursor
            .checked_add(1)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        if byte & 0x80 == 0 {
            return Ok((value, cursor));
        }
        shift = shift
            .checked_add(7)
            .filter(|shift| *shift < 64)
            .ok_or(PackEntrySourceError::InvalidDeltaHeader)?;
    }
}

fn decode_source_header(bytes: &[u8]) -> Result<(u8, u64, usize), PackEntrySourceError> {
    if bytes.is_empty() || bytes.len() > MAX_REUSED_ENTRY_HEADER_BYTES {
        return Err(PackEntrySourceError::InvalidHeader);
    }
    let first = bytes[0];
    let type_code = (first >> 4) & 0x07;
    let mut size = u64::from(first & 0x0f);
    let mut shift = 4_u32;
    let mut consumed = 1_usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = *bytes
            .get(consumed)
            .ok_or(PackEntrySourceError::InvalidHeader)?;
        let value = u64::from(byte & 0x7f)
            .checked_shl(shift)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        size = size
            .checked_add(value)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        consumed = consumed
            .checked_add(1)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
        shift = shift
            .checked_add(7)
            .ok_or(PackEntrySourceError::ByteOverflow)?;
    }
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(MAX_REUSED_ENTRY_HEADER_BYTES)
        .map_err(|_| PackEntrySourceError::ByteOverflow)?;
    encode_header_u64(&mut canonical, type_code, size)
        .map_err(|_| PackEntrySourceError::ByteOverflow)?;
    if canonical != bytes {
        return Err(PackEntrySourceError::InvalidHeader);
    }
    Ok((type_code, size, consumed))
}

pub(crate) fn encode_header_u64(
    output: &mut Vec<u8>,
    type_code: u8,
    payload_len: u64,
) -> Result<(), PackWriterError> {
    let mut size = payload_len;
    let first = ((type_code & 0x07) << 4)
        | u8::try_from(size & 0x0f).map_err(|_| PackWriterError::ByteOverflow)?;
    size >>= 4;
    if size == 0 {
        output.push(first);
        return Ok(());
    }
    output.push(first | 0x80);
    while size > 0 {
        let mut byte = u8::try_from(size & 0x7f).map_err(|_| PackWriterError::ByteOverflow)?;
        size >>= 7;
        if size > 0 {
            byte |= 0x80;
        }
        output.push(byte);
    }
    Ok(())
}

pub(crate) fn encode_ofs_delta_distance(
    output: &mut Vec<u8>,
    distance: u64,
) -> Result<(), PackWriterError> {
    if distance == 0 {
        return Err(PackWriterError::ByteOverflow);
    }
    let mut buffer = [0_u8; 10];
    let mut index = buffer.len() - 1;
    buffer[index] = u8::try_from(distance & 0x7f).map_err(|_| PackWriterError::ByteOverflow)?;
    let mut remaining = distance >> 7;
    while remaining > 0 {
        remaining = remaining
            .checked_sub(1)
            .ok_or(PackWriterError::ByteOverflow)?;
        index = index.checked_sub(1).ok_or(PackWriterError::ByteOverflow)?;
        buffer[index] =
            u8::try_from(remaining & 0x7f).map_err(|_| PackWriterError::ByteOverflow)? | 0x80;
        remaining >>= 7;
    }
    output.extend_from_slice(&buffer[index..]);
    Ok(())
}
