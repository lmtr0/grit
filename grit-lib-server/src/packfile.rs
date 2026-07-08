use std::io::{Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use grit_lib::objects::{HashAlgo, ObjectId, ObjectKind};
use sha1::{Digest as _, Sha1};
use sha2::Sha256;

use crate::error::{Error, Result};
use crate::storage::{PackMetadata, PackObjectIndex, StoredObject, StoredPack};

pub(crate) struct DecodedPackObject {
    pub(crate) index: PackObjectIndex,
    pub(crate) object: StoredObject,
}

pub(crate) struct DecodedPack {
    pub(crate) pack: StoredPack,
    pub(crate) objects: Vec<DecodedPackObject>,
}

pub(crate) fn decode_pack(pack: &[u8], hash_algo: HashAlgo) -> Result<DecodedPack> {
    if pack.is_empty() {
        return Err(Error::Protocol("pack stream is empty".to_owned()));
    }
    let trailer_len = hash_algo.len();
    if pack.len() < 12 + trailer_len {
        return Err(Error::Protocol("pack stream is too short".to_owned()));
    }
    let trailer_start = pack.len() - trailer_len;
    verify_pack_trailer(&pack[..trailer_start], &pack[trailer_start..], hash_algo)?;
    if &pack[..4] != b"PACK" {
        return Err(Error::Protocol(
            "pack stream has invalid signature".to_owned(),
        ));
    }
    let version = read_u32_be(pack, 4)?;
    if version != 2 && version != 3 {
        return Err(Error::Protocol(format!(
            "unsupported pack version {version}"
        )));
    }
    let count = read_u32_be(pack, 8)?;
    let mut cursor = 12usize;
    let mut objects = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let object = decode_pack_object_at(pack, &mut cursor, trailer_start, hash_algo)?;
        objects.push(object);
    }
    if cursor != trailer_start {
        return Err(Error::Protocol(
            "pack stream has trailing bytes before checksum".to_owned(),
        ));
    }

    let index = objects
        .iter()
        .map(|object| object.index.clone())
        .collect::<Vec<_>>();
    let metadata = PackMetadata {
        pack_checksum: pack[trailer_start..].to_vec(),
        index_checksum: pack_index_checksum(&pack[trailer_start..], &index, hash_algo),
        object_count: count,
        size_bytes: u64::try_from(pack.len())
            .map_err(|_| Error::Protocol("pack size exceeds u64".to_owned()))?,
        storage_order: 0,
    };
    Ok(DecodedPack {
        pack: StoredPack {
            metadata,
            data: pack.to_vec(),
            index,
        },
        objects,
    })
}

pub(crate) fn read_object_at_offset(
    pack: &[u8],
    offset: u64,
    hash_algo: HashAlgo,
) -> Result<StoredObject> {
    let trailer_len = hash_algo.len();
    if pack.len() < 12 + trailer_len {
        return Err(Error::Protocol("pack stream is too short".to_owned()));
    }
    let trailer_start = pack.len() - trailer_len;
    verify_pack_trailer(&pack[..trailer_start], &pack[trailer_start..], hash_algo)?;
    let mut cursor = usize::try_from(offset)
        .map_err(|_| Error::Protocol("pack object offset exceeds usize".to_owned()))?;
    if cursor < 12 || cursor >= trailer_start {
        return Err(Error::Protocol(
            "pack object offset is out of bounds".to_owned(),
        ));
    }
    Ok(decode_pack_object_at(pack, &mut cursor, trailer_start, hash_algo)?.object)
}

pub(crate) fn read_object_from_pack_range(
    range: &[u8],
    expected_oid: &ObjectId,
    absolute_offset: u64,
    hash_algo: HashAlgo,
) -> Result<StoredObject> {
    let mut cursor = 0usize;
    let decoded = decode_pack_object_at(range, &mut cursor, range.len(), hash_algo)?;
    if decoded.index.offset != 0 {
        return Err(Error::Protocol(
            "partial pack object decoder did not start at range boundary".to_owned(),
        ));
    }
    if decoded.index.oid != *expected_oid {
        return Err(Error::Protocol(format!(
            "pack object at offset {absolute_offset} decoded as {}, expected {}",
            decoded.index.oid.to_hex(),
            expected_oid.to_hex()
        )));
    }
    Ok(decoded.object)
}

pub(crate) fn serialize_pack(
    objects: &[(ObjectId, StoredObject)],
    hash_algo: HashAlgo,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(b"PACK");
    out.extend_from_slice(&2u32.to_be_bytes());
    let count = u32::try_from(objects.len())
        .map_err(|_| Error::Protocol("pack object count exceeds u32".to_owned()))?;
    out.extend_from_slice(&count.to_be_bytes());

    for (_, object) in objects {
        encode_pack_object_header(&mut out, pack_type_code(object.kind), object.data.len());
        write_zlib(&mut out, &object.data)?;
    }
    append_pack_trailer(&mut out, hash_algo);
    Ok(out)
}

fn decode_pack_object_at(
    pack: &[u8],
    cursor: &mut usize,
    trailer_start: usize,
    hash_algo: HashAlgo,
) -> Result<DecodedPackObject> {
    let offset = u64::try_from(*cursor)
        .map_err(|_| Error::Protocol("pack object offset exceeds u64".to_owned()))?;
    let (type_code, size) = read_type_and_size(pack, cursor, trailer_start)?;
    let kind = object_kind_from_pack_type(type_code)?;
    let mut decoder = ZlibDecoder::new(&pack[*cursor..trailer_start]);
    let mut data = Vec::with_capacity(size);
    decoder.read_to_end(&mut data)?;
    let consumed = decoder.total_in() as usize;
    if consumed == 0 {
        return Err(Error::Protocol(
            "pack object has empty zlib stream".to_owned(),
        ));
    }
    if data.len() != size {
        return Err(Error::Protocol(format!(
            "pack object size mismatch: expected {size}, got {}",
            data.len()
        )));
    }
    *cursor = cursor
        .checked_add(consumed)
        .ok_or_else(|| Error::Protocol("pack cursor overflow".to_owned()))?;
    let object = StoredObject::new(kind, data);
    let oid = object.object_id(hash_algo);
    Ok(DecodedPackObject {
        index: PackObjectIndex {
            oid,
            kind,
            offset,
            size: u64::try_from(size)
                .map_err(|_| Error::Protocol("pack object size exceeds u64".to_owned()))?,
            compressed_size: u64::try_from(consumed).map_err(|_| {
                Error::Protocol("pack object compressed size exceeds u64".to_owned())
            })?,
        },
        object,
    })
}

fn pack_index_checksum(
    pack_checksum: &[u8],
    index: &[PackObjectIndex],
    hash_algo: HashAlgo,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(pack_checksum);
    for row in index {
        bytes.extend_from_slice(row.oid.as_bytes());
        bytes.extend_from_slice(&row.offset.to_be_bytes());
        bytes.extend_from_slice(&row.size.to_be_bytes());
        bytes.extend_from_slice(&row.compressed_size.to_be_bytes());
        bytes.push(pack_type_code(row.kind));
    }
    digest(&bytes, hash_algo)
}

fn verify_pack_trailer(body: &[u8], trailer: &[u8], hash_algo: HashAlgo) -> Result<()> {
    if digest(body, hash_algo) != trailer {
        return Err(Error::Protocol(
            "pack trailing checksum mismatch".to_owned(),
        ));
    }
    Ok(())
}

fn digest(bytes: &[u8], hash_algo: HashAlgo) -> Vec<u8> {
    match hash_algo {
        HashAlgo::Sha1 => {
            let mut hasher = Sha1::new();
            hasher.update(bytes);
            hasher.finalize().to_vec()
        }
        HashAlgo::Sha256 => {
            let mut hasher = Sha256::new();
            hasher.update(bytes);
            hasher.finalize().to_vec()
        }
    }
}

fn read_u32_be(bytes: &[u8], offset: usize) -> Result<u32> {
    let slice = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| Error::Protocol("pack stream truncated".to_owned()))?;
    Ok(u32::from_be_bytes([slice[0], slice[1], slice[2], slice[3]]))
}

fn read_type_and_size(pack: &[u8], cursor: &mut usize, end: usize) -> Result<(u8, usize)> {
    let first = read_pack_byte(pack, cursor, end)?;
    let type_code = (first >> 4) & 0x07;
    let mut size = (first & 0x0f) as usize;
    let mut shift = 4usize;
    let mut byte = first;
    while byte & 0x80 != 0 {
        byte = read_pack_byte(pack, cursor, end)?;
        size |= ((byte & 0x7f) as usize)
            .checked_shl(shift as u32)
            .ok_or_else(|| Error::Protocol("pack object size overflow".to_owned()))?;
        shift = shift
            .checked_add(7)
            .ok_or_else(|| Error::Protocol("pack object size overflow".to_owned()))?;
    }
    Ok((type_code, size))
}

fn read_pack_byte(pack: &[u8], cursor: &mut usize, end: usize) -> Result<u8> {
    if *cursor >= end {
        return Err(Error::Protocol("pack stream truncated".to_owned()));
    }
    let byte = pack[*cursor];
    *cursor += 1;
    Ok(byte)
}

fn object_kind_from_pack_type(type_code: u8) -> Result<ObjectKind> {
    match type_code {
        1 => Ok(ObjectKind::Commit),
        2 => Ok(ObjectKind::Tree),
        3 => Ok(ObjectKind::Blob),
        4 => Ok(ObjectKind::Tag),
        6 | 7 => Err(Error::Protocol(
            "delta objects are not supported by pack storage yet".to_owned(),
        )),
        other => Err(Error::Protocol(format!(
            "unknown packed-object type {other}"
        ))),
    }
}

fn pack_type_code(kind: ObjectKind) -> u8 {
    match kind {
        ObjectKind::Commit => 1,
        ObjectKind::Tree => 2,
        ObjectKind::Blob => 3,
        ObjectKind::Tag => 4,
    }
}

fn encode_pack_object_header(buf: &mut Vec<u8>, type_code: u8, payload_len: usize) {
    let mut size = payload_len;
    let first = ((type_code & 0x7) << 4) | (size & 0x0f) as u8;
    size >>= 4;
    if size > 0 {
        buf.push(first | 0x80);
        while size > 0 {
            let byte = (size & 0x7f) as u8;
            size >>= 7;
            buf.push(if size > 0 { byte | 0x80 } else { byte });
        }
    } else {
        buf.push(first);
    }
}

fn write_zlib(buf: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data)?;
    buf.extend_from_slice(&encoder.finish()?);
    Ok(())
}

fn append_pack_trailer(buf: &mut Vec<u8>, hash_algo: HashAlgo) {
    let checksum = digest(buf, hash_algo);
    buf.extend_from_slice(&checksum);
}
