//! Compact, versioned encoding for the direct children of one Git tree.
//!
//! Version 1 stores a fixed-size entry table followed by one contiguous name arena. Object IDs
//! remain raw digest bytes, and names are sorted bytewise so equal logical inputs always produce
//! equal encoded bytes.
//!
//! All integers use network byte order. The 16-byte header contains the four-byte magic, one-byte
//! version, one-byte object-ID width, two reserved zero bytes, a `u32` entry count, and a `u32`
//! name-arena length. Each entry then contains a `u32` name offset, `u32` name length, `u32` mode,
//! one-byte flags, three reserved zero bytes, the repository-width object ID, and a `u64` size.
//! The size is zero when its presence flag is clear. Entry name ranges must form a contiguous,
//! strictly bytewise-sorted partition of the trailing arena.

use grit_lib::objects::{HashAlgo, ObjectId};

const MAGIC: &[u8; 4] = b"GRTB";
const HEADER_LEN: usize = 16;
const FIXED_ENTRY_LEN: usize = 24;
const SIZE_PRESENT: u8 = 1;

/// Current compact tree-block format version.
pub const TREE_BLOCK_FORMAT_VERSION: u8 = 1;

/// One direct child stored in a compact tree block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeBlockEntry {
    /// Raw direct-child name, excluding any parent path or slash separator.
    pub name: Vec<u8>,
    /// Canonical Git tree-entry mode.
    pub mode: u32,
    /// Object identifier named by this entry.
    pub oid: ObjectId,
    /// Object payload size when it is known.
    pub size: Option<u64>,
}

/// Failure while encoding or decoding a compact tree block.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum TreeBlockError {
    /// The input does not start with the tree-block magic bytes.
    #[error("invalid tree-block magic")]
    InvalidMagic,
    /// The encoded version is not supported by this codec.
    #[error("unsupported tree-block version {0}")]
    UnsupportedVersion(u8),
    /// The encoded object-ID width is not supported or disagrees with the requested algorithm.
    #[error("invalid tree-block object-id width {0}")]
    InvalidObjectIdWidth(u8),
    /// The input ends before the declared structure is complete.
    #[error("truncated tree block")]
    Truncated,
    /// Bytes remain after the declared entry table and name arena.
    #[error("tree block has trailing data")]
    TrailingData,
    /// An entry name is empty, contains NUL, or contains a slash.
    #[error("invalid direct tree-entry name")]
    InvalidName,
    /// Entry names are duplicated or are not in strict bytewise order.
    #[error("tree-block names are not unique and strictly sorted")]
    NamesNotStrictlySorted,
    /// An entry uses an object identifier from a different hash algorithm.
    #[error("tree-block object ID does not match {0}")]
    ObjectIdAlgorithmMismatch(&'static str),
    /// The entry count or name arena cannot be represented by the format.
    #[error("tree block exceeds the {0} format limit")]
    FormatLimit(&'static str),
    /// Header or entry reserved bytes are nonzero.
    #[error("tree block has nonzero reserved bytes")]
    NonzeroReserved,
    /// An entry contains flags that version 1 does not define.
    #[error("invalid tree-block entry flags {0:#04x}")]
    InvalidFlags(u8),
    /// A name range is out of bounds, overlaps, or leaves unused arena bytes.
    #[error("invalid tree-block name arena")]
    InvalidNameArena,
    /// A size value is present when its presence flag is clear.
    #[error("tree-block entry has a noncanonical absent size")]
    NoncanonicalSize,
    /// Raw digest bytes could not be converted into an object identifier.
    #[error("invalid tree-block object ID")]
    InvalidObjectId,
    /// Memory for the requested operation could not be reserved.
    #[error("could not allocate memory for the tree-block {0}")]
    AllocationFailed(&'static str),
}

/// Encode direct tree entries into the current deterministic format.
///
/// `hash_algorithm` selects the raw object-ID width recorded in the header. Entries are sorted by
/// raw name bytes before encoding; duplicate or invalid direct-child names are rejected.
///
/// # Errors
///
/// Returns [`TreeBlockError`] when a name is invalid or duplicated, an object ID uses a different
/// hash algorithm, or the entry table/name arena exceeds version 1 limits.
pub fn encode_tree_block(
    hash_algorithm: HashAlgo,
    entries: &[TreeBlockEntry],
) -> Result<Vec<u8>, TreeBlockError> {
    let entry_count =
        u32::try_from(entries.len()).map_err(|_| TreeBlockError::FormatLimit("entry count"))?;
    let mut sorted = Vec::new();
    sorted
        .try_reserve_exact(entries.len())
        .map_err(|_| TreeBlockError::AllocationFailed("entry index"))?;
    sorted.extend(entries);
    sorted.sort_unstable_by(|left, right| left.name.cmp(&right.name));

    let mut name_arena_len = 0usize;
    let mut previous_name: Option<&[u8]> = None;
    for entry in &sorted {
        validate_name(&entry.name)?;
        if previous_name == Some(entry.name.as_slice()) {
            return Err(TreeBlockError::NamesNotStrictlySorted);
        }
        previous_name = Some(&entry.name);
        if entry.oid.algo() != hash_algorithm {
            return Err(TreeBlockError::ObjectIdAlgorithmMismatch(
                hash_algorithm.name(),
            ));
        }
        name_arena_len = name_arena_len
            .checked_add(entry.name.len())
            .ok_or(TreeBlockError::FormatLimit("name arena"))?;
    }
    let name_arena_len_u32 =
        u32::try_from(name_arena_len).map_err(|_| TreeBlockError::FormatLimit("name arena"))?;
    let record_len = FIXED_ENTRY_LEN
        .checked_add(hash_algorithm.len())
        .ok_or(TreeBlockError::FormatLimit("encoded size"))?;
    let encoded_len = entries
        .len()
        .checked_mul(record_len)
        .and_then(|len| len.checked_add(HEADER_LEN))
        .and_then(|len| len.checked_add(name_arena_len))
        .ok_or(TreeBlockError::FormatLimit("encoded size"))?;
    let mut output = Vec::new();
    output
        .try_reserve_exact(encoded_len)
        .map_err(|_| TreeBlockError::AllocationFailed("encoded data"))?;
    output.extend_from_slice(MAGIC);
    output.push(TREE_BLOCK_FORMAT_VERSION);
    output.push(hash_algorithm.len() as u8);
    output.extend_from_slice(&[0; 2]);
    output.extend_from_slice(&entry_count.to_be_bytes());
    output.extend_from_slice(&name_arena_len_u32.to_be_bytes());

    let mut name_offset = 0u32;
    for entry in &sorted {
        let name_len = u32::try_from(entry.name.len())
            .map_err(|_| TreeBlockError::FormatLimit("entry name"))?;
        output.extend_from_slice(&name_offset.to_be_bytes());
        output.extend_from_slice(&name_len.to_be_bytes());
        output.extend_from_slice(&entry.mode.to_be_bytes());
        output.push(u8::from(entry.size.is_some()));
        output.extend_from_slice(&[0; 3]);
        output.extend_from_slice(entry.oid.as_bytes());
        output.extend_from_slice(&entry.size.unwrap_or_default().to_be_bytes());
        name_offset = name_offset
            .checked_add(name_len)
            .ok_or(TreeBlockError::FormatLimit("name arena"))?;
    }
    for entry in sorted {
        output.extend_from_slice(&entry.name);
    }
    Ok(output)
}

/// Decode and strictly validate a compact tree block.
///
/// `hash_algorithm` is the repository algorithm expected by the caller. The decoder verifies the
/// version, exact length, reserved bytes, flags, contiguous arena layout, direct-child names,
/// strict name ordering, object-ID width, and canonical optional-size encoding.
///
/// # Errors
///
/// Returns [`TreeBlockError`] for malformed, noncanonical, unsupported, or algorithm-mismatched
/// data.
pub fn decode_tree_block(
    hash_algorithm: HashAlgo,
    data: &[u8],
) -> Result<Vec<TreeBlockEntry>, TreeBlockError> {
    if data.len() < HEADER_LEN {
        return Err(TreeBlockError::Truncated);
    }
    if &data[..4] != MAGIC {
        return Err(TreeBlockError::InvalidMagic);
    }
    if data[4] != TREE_BLOCK_FORMAT_VERSION {
        return Err(TreeBlockError::UnsupportedVersion(data[4]));
    }
    let oid_width = usize::from(data[5]);
    if oid_width != hash_algorithm.len() {
        return Err(TreeBlockError::InvalidObjectIdWidth(data[5]));
    }
    if data[6..8] != [0, 0] {
        return Err(TreeBlockError::NonzeroReserved);
    }
    let entry_count = read_u32(&data[8..12])? as usize;
    let name_arena_len = read_u32(&data[12..16])? as usize;
    let record_len = FIXED_ENTRY_LEN
        .checked_add(oid_width)
        .ok_or(TreeBlockError::FormatLimit("encoded size"))?;
    let table_len = entry_count
        .checked_mul(record_len)
        .ok_or(TreeBlockError::FormatLimit("encoded size"))?;
    let expected_len = HEADER_LEN
        .checked_add(table_len)
        .and_then(|len| len.checked_add(name_arena_len))
        .ok_or(TreeBlockError::FormatLimit("encoded size"))?;
    if data.len() < expected_len {
        return Err(TreeBlockError::Truncated);
    }
    if data.len() > expected_len {
        return Err(TreeBlockError::TrailingData);
    }
    let arena = &data[HEADER_LEN + table_len..];
    let mut entries = Vec::new();
    entries
        .try_reserve_exact(entry_count)
        .map_err(|_| TreeBlockError::AllocationFailed("decoded entries"))?;
    let mut expected_name_offset = 0usize;
    let mut previous_name: Option<&[u8]> = None;
    for index in 0..entry_count {
        let start = HEADER_LEN + index * record_len;
        let record = &data[start..start + record_len];
        let name_offset = read_u32(&record[..4])? as usize;
        let name_len = read_u32(&record[4..8])? as usize;
        if name_offset != expected_name_offset {
            return Err(TreeBlockError::InvalidNameArena);
        }
        let name_end = name_offset
            .checked_add(name_len)
            .ok_or(TreeBlockError::InvalidNameArena)?;
        let name = arena
            .get(name_offset..name_end)
            .ok_or(TreeBlockError::InvalidNameArena)?;
        validate_name(name)?;
        if previous_name.is_some_and(|previous| previous >= name) {
            return Err(TreeBlockError::NamesNotStrictlySorted);
        }
        let flags = record[12];
        if flags & !SIZE_PRESENT != 0 {
            return Err(TreeBlockError::InvalidFlags(flags));
        }
        if record[13..16] != [0, 0, 0] {
            return Err(TreeBlockError::NonzeroReserved);
        }
        let oid_end = 16 + oid_width;
        let oid = ObjectId::from_bytes(&record[16..oid_end])
            .map_err(|_| TreeBlockError::InvalidObjectId)?;
        let size_value = read_u64(&record[oid_end..oid_end + 8])?;
        let size = if flags & SIZE_PRESENT == 0 {
            if size_value != 0 {
                return Err(TreeBlockError::NoncanonicalSize);
            }
            None
        } else {
            Some(size_value)
        };
        let mode = read_u32(&record[8..12])?;
        let mut owned_name = Vec::new();
        owned_name
            .try_reserve_exact(name.len())
            .map_err(|_| TreeBlockError::AllocationFailed("entry name"))?;
        owned_name.extend_from_slice(name);
        expected_name_offset = name_end;
        previous_name = Some(name);
        entries.push(TreeBlockEntry {
            name: owned_name,
            mode,
            oid,
            size,
        });
    }
    if expected_name_offset != arena.len() {
        return Err(TreeBlockError::InvalidNameArena);
    }
    Ok(entries)
}

fn validate_name(name: &[u8]) -> Result<(), TreeBlockError> {
    if name.is_empty() || name.contains(&0) || name.contains(&b'/') {
        return Err(TreeBlockError::InvalidName);
    }
    Ok(())
}

fn read_u32(bytes: &[u8]) -> Result<u32, TreeBlockError> {
    let value: [u8; 4] = bytes.try_into().map_err(|_| TreeBlockError::Truncated)?;
    Ok(u32::from_be_bytes(value))
}

fn read_u64(bytes: &[u8]) -> Result<u64, TreeBlockError> {
    let value: [u8; 8] = bytes.try_into().map_err(|_| TreeBlockError::Truncated)?;
    Ok(u64::from_be_bytes(value))
}
