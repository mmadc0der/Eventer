//! Per-block zone maps for equality filters.
//!
//! The sparse index only stores a timestamp range, so a query whose window covers
//! every block still reads every payload. A zone map sits beside that index
//! (`seg-NNNNNN.zon`) and records, per column, enough information to reject a
//! block before the payload is read.
//!
//! Strings and text use the exact distinct set when it is small, and a bloom
//! filter otherwise. A bloom hit can be wrong; a miss is not. Numbers use an
//! inclusive min/max, which also rejects a range whose lower bound is above the
//! max or whose upper bound is below the min, including exclusive bounds.
//! Floats and JSON stay unpruned. Segments written before
//! zone maps existed are summarized once on open and the summary is kept.
//!
//! The bytes on disk are still that zone file: `EVZN` header, then each block's
//! length and column records, then one crc32 of the bytes before that trailer.
//! Version 1 files, which stored a crc32 in front of every record, still load.
//! The whole file is replaced by a plain zstd frame when the frame is strictly
//! smaller, and left raw otherwise. A batch append rewrites the segment's zone
//! file instead of appending a frame onto a raw prefix. Readers decompress a
//! zstd magic with a bounded output size, then parse the zone file. A raw file
//! still loads. A corrupt or truncated frame, or a trailer that does not match,
//! is rebuilt from payloads, the same path a corrupt raw file already takes.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::codec::{decode_block, ColumnPredicate, RangeFilter};
use crate::error::{Error, Result};
use crate::schema::{FieldType, Schema};
use crate::segment::{self, BlockMeta};
use crate::value::{Row, Scalar};

const ZONE_MAGIC: &[u8; 4] = b"EVZN";
/// A crc32 sits in front of every record. Still loaded; new files use version 2.
const ZONE_VERSION_V1: u16 = 1;
/// Each record is a length and a payload. One crc32 covers the image before the trailer.
const ZONE_VERSION: u16 = 2;
const ZONE_HEADER_LEN: usize = 16;
const ZONE_TRAILER_LEN: usize = 4;
/// Plain zstd frame magic. Distinct from [`ZONE_MAGIC`].
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
/// Level 1. The zone file is small and rewritten as a whole at the end of a batch.
const ZONE_ZSTD_LEVEL: i32 = 1;
/// Cap for a decompressed zone frame. A larger claim is treated as corrupt and
/// the segment is summarized again from its payloads.
const MAX_ZONE_UNCOMPRESSED: usize = 512 * 1024 * 1024;
/// 2048 bits. A block with a few hundred distinct strings still misses almost
/// every value that was not inserted.
const BLOOM_BYTES: usize = 256;
const BLOOM_BITS: u64 = (BLOOM_BYTES * 8) as u64;
const BLOOM_K: u8 = 4;
/// Distinct strings stored verbatim. Above this, the bloom filter is smaller
/// than the set and still rejects values the block does not contain.
const EXACT_MAX_VALUES: usize = 32;
const EXACT_MAX_BYTES: usize = 32 * 48;

const TAG_UNKNOWN: u8 = 0;
const TAG_ALL_NULL: u8 = 1;
const TAG_I64: u8 = 2;
const TAG_I128: u8 = 3;
const TAG_BOOL: u8 = 4;
const TAG_EXACT: u8 = 5;
const TAG_BLOOM: u8 = 6;

/// Statistics for one sealed block, aligned with schema field order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BlockZone {
    pub columns: Vec<ColumnZone>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ColumnZone {
    /// The block must be read. Used for floats, JSON, and unreadable stats.
    Unknown,
    AllNull,
    I64 {
        has_null: bool,
        min: i64,
        max: i64,
    },
    I128 {
        has_null: bool,
        min: i128,
        max: i128,
    },
    Bool {
        has_null: bool,
        has_false: bool,
        has_true: bool,
    },
    Exact {
        has_null: bool,
        values: Vec<String>,
    },
    Bloom {
        has_null: bool,
        bloom: Bloom,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Bloom {
    k: u8,
    bits: Vec<u8>,
}

impl Bloom {
    fn new() -> Self {
        Self {
            k: BLOOM_K,
            bits: vec![0; BLOOM_BYTES],
        }
    }

    fn insert(&mut self, bytes: &[u8]) {
        for bit in indexes(bytes, self.k) {
            set_bit(&mut self.bits, bit);
        }
    }

    fn contains(&self, bytes: &[u8]) -> bool {
        if self.bits.len() != BLOOM_BYTES || self.k == 0 {
            return true;
        }
        indexes(bytes, self.k).all(|bit| bit_is_set(&self.bits, bit))
    }
}

pub(crate) fn zone_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:06}.zon"))
}

pub(crate) fn from_rows(schema: &Schema, rows: &[Row]) -> BlockZone {
    let columns = schema
        .fields
        .iter()
        .enumerate()
        .map(|(index, field)| column_from_rows(field.ty, rows, index))
        .collect();
    BlockZone { columns }
}

/// True when every predicate might match this block.
///
/// A `false` result means the payload cannot contain a matching row. A `true`
/// result can still be a bloom false positive, so the block is decoded as usual.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn may_match(zone: &BlockZone, predicates: &[ColumnPredicate]) -> bool {
    may_match_ranges(zone, predicates, &[])
}

/// Equality predicates and numeric ranges. A false result means the payload
/// cannot contain a matching row.
///
/// An `i64` or `i128` zone whose max is below a lower bound, or whose min is
/// above an upper bound, does not match. Exclusive bounds use `<=` and `>=`.
/// [`ColumnZone::Unknown`] (float and JSON) is not a skip. [`ColumnZone::AllNull`]
/// fails every range, because nulls do not match one. A missing column is null.
pub(crate) fn may_match_ranges(
    zone: &BlockZone,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
) -> bool {
    for predicate in predicates {
        match zone.columns.get(predicate.index) {
            Some(column) if !column.may_contain(&predicate.allowed) => return false,
            Some(_) | None => {}
        }
    }
    for range in ranges {
        match zone.columns.get(range.index) {
            Some(column) if !column.may_contain_range(range) => return false,
            None => return false,
            Some(_) => {}
        }
    }
    true
}

/// Load the sidecar, or build it from block payloads when it is missing or stale.
pub(crate) fn load_segment_zones(
    dir: &Path,
    segment_id: u32,
    blocks: &[BlockMeta],
    dictionary: Option<&[u8]>,
    schema: &Schema,
) -> Result<Vec<BlockZone>> {
    if blocks.is_empty() {
        return Ok(Vec::new());
    }
    let path = zone_path(dir, segment_id);
    let schema_crc = schema_crc(schema);
    let field_count = field_count(schema)?;
    if let Some(zones) = read_zone_file(&path, schema_crc, field_count, blocks.len())? {
        return Ok(zones);
    }
    let zones = build_from_payloads(dir, blocks, dictionary, schema)?;
    write_zone_file(&path, schema_crc, field_count, &zones)?;
    Ok(zones)
}

/// Append zone records for blocks just committed to this segment.
///
/// The sidecar is rewritten as a whole so a compressed frame is never appended
/// onto a raw prefix. The returned delta is the change in on-disk size and can
/// be negative when recompression shrinks the file. Those bytes stay in the
/// index total.
pub(crate) fn append_zones(
    dir: &Path,
    segment_id: u32,
    schema_crc: u32,
    field_count: u16,
    zones: &[BlockZone],
    sync: bool,
) -> Result<i64> {
    if zones.is_empty() {
        return Ok(0);
    }
    let path = zone_path(dir, segment_id);
    let before = match fs::metadata(&path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => 0,
        Err(err) => return Err(err.into()),
    };
    let mut raw = match existing_plain(&path)? {
        Some(plain) => v2_prefix_for_append(&plain, schema_crc, field_count)
            .unwrap_or_else(|| header(schema_crc, field_count).to_vec()),
        None => header(schema_crc, field_count).to_vec(),
    };
    for zone in zones {
        write_record(&mut raw, zone)?;
    }
    seal_zone_image(&mut raw);
    let after = persist_zone_bytes(&path, &raw, sync)?;
    Ok(after as i64 - before as i64)
}

pub(crate) fn schema_crc(schema: &Schema) -> u32 {
    crc32fast::hash(schema.canonical().as_bytes())
}

pub(crate) fn field_count(schema: &Schema) -> Result<u16> {
    u16::try_from(schema.fields.len())
        .map_err(|_| Error::schema("schema has too many fields for a zone map"))
}

fn column_from_rows(ty: FieldType, rows: &[Row], index: usize) -> ColumnZone {
    match ty {
        FieldType::Int => i64_zone(rows, index, |value| match value {
            Scalar::Int(value) => Some(*value),
            _ => None,
        }),
        FieldType::Timestamp => i64_zone(rows, index, |value| match value {
            Scalar::Timestamp(value) => Some(*value),
            _ => None,
        }),
        FieldType::Decimal { .. } => i128_zone(rows, index),
        FieldType::Bool => bool_zone(rows, index),
        FieldType::String | FieldType::Text => string_zone(rows, index),
        FieldType::Float | FieldType::Json => ColumnZone::Unknown,
    }
}

fn i64_zone(rows: &[Row], index: usize, pick: impl Fn(&Scalar) -> Option<i64>) -> ColumnZone {
    let mut has_null = false;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut any = false;
    for row in rows {
        let Some(value) = row.values.get(index) else {
            return ColumnZone::Unknown;
        };
        if matches!(value, Scalar::Null) {
            has_null = true;
            continue;
        }
        let Some(value) = pick(value) else {
            return ColumnZone::Unknown;
        };
        any = true;
        min = min.min(value);
        max = max.max(value);
    }
    if !any {
        ColumnZone::AllNull
    } else {
        ColumnZone::I64 { has_null, min, max }
    }
}

fn i128_zone(rows: &[Row], index: usize) -> ColumnZone {
    let mut has_null = false;
    let mut min = i128::MAX;
    let mut max = i128::MIN;
    let mut any = false;
    for row in rows {
        match row.values.get(index) {
            Some(Scalar::Null) => has_null = true,
            Some(Scalar::Decimal(value)) => {
                any = true;
                min = min.min(*value);
                max = max.max(*value);
            }
            _ => return ColumnZone::Unknown,
        }
    }
    if !any {
        ColumnZone::AllNull
    } else {
        ColumnZone::I128 { has_null, min, max }
    }
}

fn bool_zone(rows: &[Row], index: usize) -> ColumnZone {
    let mut has_null = false;
    let mut has_false = false;
    let mut has_true = false;
    for row in rows {
        match row.values.get(index) {
            Some(Scalar::Null) => has_null = true,
            Some(Scalar::Bool(false)) => has_false = true,
            Some(Scalar::Bool(true)) => has_true = true,
            _ => return ColumnZone::Unknown,
        }
    }
    if !has_false && !has_true {
        ColumnZone::AllNull
    } else {
        ColumnZone::Bool {
            has_null,
            has_false,
            has_true,
        }
    }
}

fn string_zone(rows: &[Row], index: usize) -> ColumnZone {
    let mut has_null = false;
    let mut seen: HashSet<&str> = HashSet::new();
    let mut values: Vec<&str> = Vec::new();
    let mut bytes = 0usize;
    for row in rows {
        match row.values.get(index) {
            Some(Scalar::Null) => has_null = true,
            Some(Scalar::Str(text)) => {
                if seen.insert(text) {
                    bytes = bytes.saturating_add(text.len());
                    values.push(text.as_str());
                }
            }
            _ => return ColumnZone::Unknown,
        }
    }
    if values.is_empty() {
        return ColumnZone::AllNull;
    }
    if values.len() <= EXACT_MAX_VALUES && bytes <= EXACT_MAX_BYTES {
        return ColumnZone::Exact {
            has_null,
            values: values.into_iter().map(str::to_string).collect(),
        };
    }
    let mut bloom = Bloom::new();
    for text in values {
        bloom.insert(text.as_bytes());
    }
    ColumnZone::Bloom { has_null, bloom }
}

impl ColumnZone {
    fn may_contain(&self, allowed: &[Scalar]) -> bool {
        if allowed.is_empty() {
            return false;
        }
        allowed.iter().any(|value| self.may_contain_one(value))
    }

    fn may_contain_one(&self, value: &Scalar) -> bool {
        match self {
            ColumnZone::Unknown => true,
            ColumnZone::AllNull => matches!(value, Scalar::Null),
            ColumnZone::I64 { has_null, min, max } => match value {
                Scalar::Null => *has_null,
                Scalar::Int(value) | Scalar::Timestamp(value) => *value >= *min && *value <= *max,
                _ => true,
            },
            ColumnZone::I128 { has_null, min, max } => match value {
                Scalar::Null => *has_null,
                Scalar::Decimal(value) => *value >= *min && *value <= *max,
                _ => true,
            },
            ColumnZone::Bool {
                has_null,
                has_false,
                has_true,
            } => match value {
                Scalar::Null => *has_null,
                Scalar::Bool(false) => *has_false,
                Scalar::Bool(true) => *has_true,
                _ => true,
            },
            ColumnZone::Exact { has_null, values } => match value {
                Scalar::Null => *has_null,
                Scalar::Str(text) => values.iter().any(|candidate| candidate == text),
                _ => true,
            },
            ColumnZone::Bloom { has_null, bloom } => match value {
                Scalar::Null => *has_null,
                Scalar::Str(text) => bloom.contains(text.as_bytes()),
                _ => true,
            },
        }
    }

    fn may_contain_range(&self, range: &RangeFilter) -> bool {
        match self {
            ColumnZone::Unknown => true,
            ColumnZone::AllNull => false,
            ColumnZone::I64 { min, max, .. } => {
                ordered_range_overlaps(*min, *max, range, bound_i64)
            }
            ColumnZone::I128 { min, max, .. } => {
                ordered_range_overlaps(*min, *max, range, bound_i128)
            }
            ColumnZone::Bool { .. } | ColumnZone::Exact { .. } | ColumnZone::Bloom { .. } => true,
        }
    }
}

fn bound_i64(value: &Scalar) -> Option<i64> {
    match value {
        Scalar::Int(value) | Scalar::Timestamp(value) => Some(*value),
        _ => None,
    }
}

fn bound_i128(value: &Scalar) -> Option<i128> {
    match value {
        Scalar::Decimal(value) => Some(*value),
        _ => None,
    }
}

fn ordered_range_overlaps<T: Ord + Copy>(
    min: T,
    max: T,
    range: &RangeFilter,
    bound: impl Fn(&Scalar) -> Option<T>,
) -> bool {
    if let Some(lower) = &range.lower {
        let Some(cutoff) = bound(&lower.value) else {
            return true;
        };
        if lower.inclusive {
            if max < cutoff {
                return false;
            }
        } else if max <= cutoff {
            return false;
        }
    }
    if let Some(upper) = &range.upper {
        let Some(cutoff) = bound(&upper.value) else {
            return true;
        };
        if upper.inclusive {
            if min > cutoff {
                return false;
            }
        } else if min >= cutoff {
            return false;
        }
    }
    true
}

fn build_from_payloads(
    dir: &Path,
    blocks: &[BlockMeta],
    dictionary: Option<&[u8]>,
    schema: &Schema,
) -> Result<Vec<BlockZone>> {
    let mut zones = Vec::with_capacity(blocks.len());
    for block in blocks {
        let path = segment::data_path(dir, block.segment_id);
        let payload = segment::read_block_payload(&path, block, dictionary)?;
        let rows = decode_block(schema, &payload)?;
        zones.push(from_rows(schema, &rows));
    }
    Ok(zones)
}

fn read_zone_file(
    path: &Path,
    schema_crc: u32,
    field_count: u16,
    block_count: usize,
) -> Result<Option<Vec<BlockZone>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let plain = match plain_zone_bytes(&bytes) {
        Ok(plain) => plain,
        Err(_) => return Ok(None),
    };
    match parse_zone_file(&plain, schema_crc, field_count, block_count) {
        Ok(zones) => Ok(Some(zones)),
        Err(_) => Ok(None),
    }
}

fn existing_plain(path: &Path) -> Result<Option<Vec<u8>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) if !bytes.is_empty() => bytes,
        Ok(_) => return Ok(None),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    match plain_zone_bytes(&bytes) {
        Ok(plain) => Ok(Some(plain)),
        Err(_) => Ok(None),
    }
}

fn plain_zone_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if is_zstd_frame(bytes) {
        decompress_zone_frame(bytes)
    } else {
        Ok(bytes.to_vec())
    }
}

fn is_zstd_frame(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[..4] == ZSTD_MAGIC
}

fn compress_zone_if_smaller(raw: &[u8]) -> Vec<u8> {
    match zstd::bulk::compress(raw, ZONE_ZSTD_LEVEL) {
        Ok(frame) if frame.len() < raw.len() && is_zstd_frame(&frame) => frame,
        _ => raw.to_vec(),
    }
}

fn decompress_zone_frame(frame: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::Decoder::with_buffer(frame).map_err(Error::io)?;
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = match decoder.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => return Err(Error::corrupt("truncated zone map frame")),
        };
        if out.len().saturating_add(n) > MAX_ZONE_UNCOMPRESSED {
            return Err(Error::corrupt("zone map exceeds the decompress bound"));
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

fn read_zone_header(bytes: &[u8]) -> Result<(u16, u32, u16)> {
    if bytes.len() < ZONE_HEADER_LEN {
        return Err(Error::corrupt("truncated zone map"));
    }
    if &bytes[0..4] != ZONE_MAGIC {
        return Err(Error::corrupt("zone map magic mismatch"));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let stored_fields = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
    Ok((version, stored_crc, stored_fields))
}

fn require_known_zone(bytes: &[u8], expect_crc: u32, field_count: u16) -> Result<u16> {
    let (version, stored_crc, stored_fields) = read_zone_header(bytes)?;
    if version != ZONE_VERSION_V1 && version != ZONE_VERSION {
        return Err(Error::corrupt(format!(
            "unsupported zone map version {version}"
        )));
    }
    if stored_crc != expect_crc || stored_fields != field_count {
        return Err(Error::corrupt("zone map does not match the schema"));
    }
    Ok(version)
}

/// Version 2 body, with the trailer removed after it matches.
fn v2_body(bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < ZONE_HEADER_LEN + ZONE_TRAILER_LEN {
        return Err(Error::corrupt("truncated zone map"));
    }
    let split = bytes.len() - ZONE_TRAILER_LEN;
    let (body, trailer) = bytes.split_at(split);
    let expect = u32::from_le_bytes(trailer.try_into().unwrap());
    if crc32fast::hash(body) != expect {
        return Err(Error::corrupt("zone map checksum mismatch"));
    }
    Ok(body)
}

fn record_payloads(body: &[u8], version: u16) -> Result<Vec<&[u8]>> {
    let mut cursor = ZONE_HEADER_LEN;
    let mut payloads = Vec::new();
    while cursor < body.len() {
        let (payload, next) = if version == ZONE_VERSION_V1 {
            if body.len() - cursor < 8 {
                return Err(Error::corrupt("truncated zone map record"));
            }
            let len = u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(body[cursor + 4..cursor + 8].try_into().unwrap());
            let start = cursor + 8;
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::corrupt("zone map record length overflow"))?;
            if end > body.len() {
                return Err(Error::corrupt("truncated zone map record"));
            }
            let payload = &body[start..end];
            if crc32fast::hash(payload) != crc {
                return Err(Error::corrupt("zone map record checksum mismatch"));
            }
            (payload, end)
        } else {
            if body.len() - cursor < 4 {
                return Err(Error::corrupt("truncated zone map record"));
            }
            let len = u32::from_le_bytes(body[cursor..cursor + 4].try_into().unwrap()) as usize;
            let start = cursor + 4;
            let end = start
                .checked_add(len)
                .ok_or_else(|| Error::corrupt("zone map record length overflow"))?;
            if end > body.len() {
                return Err(Error::corrupt("truncated zone map record"));
            }
            (&body[start..end], end)
        };
        payloads.push(payload);
        cursor = next;
    }
    Ok(payloads)
}

fn parse_zone_file(
    bytes: &[u8],
    expect_crc: u32,
    field_count: u16,
    block_count: usize,
) -> Result<Vec<BlockZone>> {
    let version = require_known_zone(bytes, expect_crc, field_count)?;
    let body = if version == ZONE_VERSION {
        v2_body(bytes)?
    } else {
        bytes
    };
    let payloads = record_payloads(body, version)?;
    if payloads.len() != block_count {
        return Err(Error::corrupt(
            "zone map block count does not match the segment",
        ));
    }
    let mut zones = Vec::with_capacity(block_count);
    for payload in payloads {
        zones.push(decode_zone(payload, field_count)?);
    }
    Ok(zones)
}

/// Bytes of a version-2 image with the trailer not yet written.
///
/// A matching version-2 file keeps its body. A version-1 file is rewritten
/// without the per-record crc32s. A checksum failure returns `None` so the
/// caller starts a new image instead of appending onto a torn one.
fn v2_prefix_for_append(plain: &[u8], schema_crc: u32, field_count: u16) -> Option<Vec<u8>> {
    let version = require_known_zone(plain, schema_crc, field_count).ok()?;
    let body = if version == ZONE_VERSION {
        v2_body(plain).ok()?
    } else {
        plain
    };
    let payloads = record_payloads(body, version).ok()?;
    if version == ZONE_VERSION {
        return Some(body.to_vec());
    }
    let mut raw = header(schema_crc, field_count).to_vec();
    for payload in payloads {
        raw.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        raw.extend_from_slice(payload);
    }
    Some(raw)
}

/// Write zone records for the blocks that survive retention.
///
/// Copies the on-disk record bytes when the sidecar matches the segment.
/// Otherwise encodes the in-memory stats for those blocks. The caller renames
/// `tmp` into place.
pub(crate) fn stage_kept_zones(
    dir: &Path,
    segment_id: u32,
    schema_crc: u32,
    field_count: u16,
    zones: &[BlockZone],
    keep: &[bool],
    tmp: &Path,
) -> Result<()> {
    let path = zone_path(dir, segment_id);
    let raw = if zones.len() == keep.len() {
        fs::read(&path)
            .ok()
            .and_then(|bytes| plain_zone_bytes(&bytes).ok())
            .and_then(|plain| split_zone_records(&plain, schema_crc, field_count, zones.len()).ok())
            .map(|records| {
                let mut raw = header(schema_crc, field_count).to_vec();
                for (record, keep_block) in records.iter().zip(keep.iter()) {
                    if *keep_block {
                        raw.extend_from_slice(record);
                    }
                }
                raw
            })
    } else {
        None
    };
    let mut raw = match raw {
        Some(raw) => raw,
        None => {
            let mut raw = header(schema_crc, field_count).to_vec();
            for (zone, keep_block) in zones.iter().zip(keep.iter()) {
                if *keep_block {
                    write_record(&mut raw, zone)?;
                }
            }
            raw
        }
    };
    seal_zone_image(&mut raw);
    // The caller renames `tmp` onto the zone path, so the bytes here are the
    // stored form, including a plain zstd frame when that frame is smaller.
    let stored = compress_zone_if_smaller(&raw);
    let mut file = File::create(tmp)?;
    file.write_all(&stored)?;
    file.sync_all()?;
    Ok(())
}

fn write_zone_file(
    path: &Path,
    schema_crc: u32,
    field_count: u16,
    zones: &[BlockZone],
) -> Result<()> {
    let mut raw = header(schema_crc, field_count).to_vec();
    for zone in zones {
        write_record(&mut raw, zone)?;
    }
    seal_zone_image(&mut raw);
    persist_zone_bytes(path, &raw, true)?;
    Ok(())
}

fn persist_zone_bytes(path: &Path, raw: &[u8], sync: bool) -> Result<u64> {
    let stored = compress_zone_if_smaller(raw);
    let tmp = path.with_extension("zon.partial");
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&stored)?;
        file.flush()?;
        if sync {
            file.sync_all()?;
        }
    }
    fs::rename(&tmp, path)?;
    Ok(stored.len() as u64)
}

/// Each item is one version-2 record: a length prefix and the payload.
fn split_zone_records(
    bytes: &[u8],
    expect_crc: u32,
    field_count: u16,
    block_count: usize,
) -> Result<Vec<Vec<u8>>> {
    let version = require_known_zone(bytes, expect_crc, field_count)?;
    let body = if version == ZONE_VERSION {
        v2_body(bytes)?
    } else {
        bytes
    };
    let payloads = record_payloads(body, version)?;
    if payloads.len() != block_count {
        return Err(Error::corrupt(
            "zone map block count does not match the segment",
        ));
    }
    let mut records = Vec::with_capacity(block_count);
    for payload in payloads {
        let mut record = Vec::with_capacity(4 + payload.len());
        record.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        record.extend_from_slice(payload);
        records.push(record);
    }
    Ok(records)
}

fn header(schema_crc: u32, field_count: u16) -> [u8; ZONE_HEADER_LEN] {
    let mut out = [0u8; ZONE_HEADER_LEN];
    out[0..4].copy_from_slice(ZONE_MAGIC);
    out[4..6].copy_from_slice(&ZONE_VERSION.to_le_bytes());
    out[8..12].copy_from_slice(&schema_crc.to_le_bytes());
    out[12..14].copy_from_slice(&field_count.to_le_bytes());
    out
}

fn seal_zone_image(raw: &mut Vec<u8>) {
    let crc = crc32fast::hash(raw);
    raw.extend_from_slice(&crc.to_le_bytes());
}

fn write_record(out: &mut impl Write, zone: &BlockZone) -> Result<()> {
    let payload = encode_zone(zone);
    out.write_all(&(payload.len() as u32).to_le_bytes())?;
    out.write_all(&payload)?;
    Ok(())
}

fn encode_zone(zone: &BlockZone) -> Vec<u8> {
    let mut out = Vec::new();
    for column in &zone.columns {
        encode_column(&mut out, column);
    }
    out
}

fn encode_column(out: &mut Vec<u8>, column: &ColumnZone) {
    match column {
        ColumnZone::Unknown => out.push(TAG_UNKNOWN),
        ColumnZone::AllNull => out.push(TAG_ALL_NULL),
        ColumnZone::I64 { has_null, min, max } => {
            out.push(TAG_I64);
            out.push(u8::from(*has_null));
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&max.to_le_bytes());
        }
        ColumnZone::I128 { has_null, min, max } => {
            out.push(TAG_I128);
            out.push(u8::from(*has_null));
            out.extend_from_slice(&min.to_le_bytes());
            out.extend_from_slice(&max.to_le_bytes());
        }
        ColumnZone::Bool {
            has_null,
            has_false,
            has_true,
        } => {
            out.push(TAG_BOOL);
            let mut bits = 0u8;
            if *has_null {
                bits |= 1;
            }
            if *has_false {
                bits |= 2;
            }
            if *has_true {
                bits |= 4;
            }
            out.push(bits);
        }
        ColumnZone::Exact { has_null, values } => {
            out.push(TAG_EXACT);
            out.push(u8::from(*has_null));
            out.extend_from_slice(&(values.len() as u16).to_le_bytes());
            for value in values {
                let bytes = value.as_bytes();
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
        ColumnZone::Bloom { has_null, bloom } => {
            out.push(TAG_BLOOM);
            out.push(u8::from(*has_null));
            out.push(bloom.k);
            out.extend_from_slice(&bloom.bits);
        }
    }
}

fn decode_zone(mut bytes: &[u8], field_count: u16) -> Result<BlockZone> {
    let mut columns = Vec::with_capacity(field_count as usize);
    for _ in 0..field_count {
        columns.push(decode_column(&mut bytes)?);
    }
    if !bytes.is_empty() {
        return Err(Error::corrupt("zone map record has trailing bytes"));
    }
    Ok(BlockZone { columns })
}

fn decode_column(bytes: &mut &[u8]) -> Result<ColumnZone> {
    let tag = *bytes
        .first()
        .ok_or_else(|| Error::corrupt("truncated zone column"))?;
    *bytes = &bytes[1..];
    match tag {
        TAG_UNKNOWN => Ok(ColumnZone::Unknown),
        TAG_ALL_NULL => Ok(ColumnZone::AllNull),
        TAG_I64 => {
            let (has_null, rest) = take_bool(bytes)?;
            let (min, rest) = take_i64(rest)?;
            let (max, rest) = take_i64(rest)?;
            *bytes = rest;
            Ok(ColumnZone::I64 { has_null, min, max })
        }
        TAG_I128 => {
            let (has_null, rest) = take_bool(bytes)?;
            let (min, rest) = take_i128(rest)?;
            let (max, rest) = take_i128(rest)?;
            *bytes = rest;
            Ok(ColumnZone::I128 { has_null, min, max })
        }
        TAG_BOOL => {
            let bits = *bytes
                .first()
                .ok_or_else(|| Error::corrupt("truncated bool zone"))?;
            *bytes = &bytes[1..];
            Ok(ColumnZone::Bool {
                has_null: bits & 1 != 0,
                has_false: bits & 2 != 0,
                has_true: bits & 4 != 0,
            })
        }
        TAG_EXACT => {
            let (has_null, rest) = take_bool(bytes)?;
            if rest.len() < 2 {
                return Err(Error::corrupt("truncated exact zone"));
            }
            let count = u16::from_le_bytes(rest[0..2].try_into().unwrap()) as usize;
            if count > EXACT_MAX_VALUES {
                return Err(Error::corrupt("exact zone is too large"));
            }
            let mut rest = &rest[2..];
            let mut values = Vec::with_capacity(count);
            let mut total = 0usize;
            for _ in 0..count {
                if rest.len() < 4 {
                    return Err(Error::corrupt("truncated exact zone"));
                }
                let len = u32::from_le_bytes(rest[0..4].try_into().unwrap()) as usize;
                rest = &rest[4..];
                if len > EXACT_MAX_BYTES || rest.len() < len {
                    return Err(Error::corrupt("truncated exact zone value"));
                }
                total = total.saturating_add(len);
                if total > EXACT_MAX_BYTES {
                    return Err(Error::corrupt("exact zone is too large"));
                }
                let text = std::str::from_utf8(&rest[..len])
                    .map_err(|_| Error::corrupt("exact zone value is not utf-8"))?
                    .to_string();
                rest = &rest[len..];
                values.push(text);
            }
            *bytes = rest;
            Ok(ColumnZone::Exact { has_null, values })
        }
        TAG_BLOOM => {
            if bytes.len() < 2 + BLOOM_BYTES {
                return Err(Error::corrupt("truncated bloom zone"));
            }
            let has_null = bytes[0] != 0;
            let k = bytes[1];
            if k == 0 || k > 16 {
                return Err(Error::corrupt("bloom zone hash count is invalid"));
            }
            let bits = bytes[2..2 + BLOOM_BYTES].to_vec();
            *bytes = &bytes[2 + BLOOM_BYTES..];
            Ok(ColumnZone::Bloom {
                has_null,
                bloom: Bloom { k, bits },
            })
        }
        _ => Err(Error::corrupt("unknown zone column tag")),
    }
}

fn take_bool(bytes: &[u8]) -> Result<(bool, &[u8])> {
    let flag = *bytes
        .first()
        .ok_or_else(|| Error::corrupt("truncated zone flag"))?;
    Ok((flag != 0, &bytes[1..]))
}

fn take_i64(bytes: &[u8]) -> Result<(i64, &[u8])> {
    if bytes.len() < 8 {
        return Err(Error::corrupt("truncated i64 zone"));
    }
    let value = i64::from_le_bytes(bytes[0..8].try_into().unwrap());
    Ok((value, &bytes[8..]))
}

fn take_i128(bytes: &[u8]) -> Result<(i128, &[u8])> {
    if bytes.len() < 16 {
        return Err(Error::corrupt("truncated i128 zone"));
    }
    let value = i128::from_le_bytes(bytes[0..16].try_into().unwrap());
    Ok((value, &bytes[16..]))
}

fn indexes(bytes: &[u8], k: u8) -> impl Iterator<Item = u64> {
    let (h1, h2) = hash_pair(bytes);
    (0..u64::from(k)).map(move |index| h1.wrapping_add(index.wrapping_mul(h2)) % BLOOM_BITS)
}

fn hash_pair(bytes: &[u8]) -> (u64, u64) {
    let mut h1 = 0xcbf29ce484222325u64;
    let mut h2 = 0x84222325cbf29ce4u64;
    for &byte in bytes {
        h1 ^= u64::from(byte);
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 ^= u64::from(byte);
        h2 = h2.wrapping_mul(0xcbf29ce484222325);
    }
    if h2 % 2 == 0 {
        h2 |= 1;
    }
    (h1, h2)
}

fn set_bit(bits: &mut [u8], bit: u64) {
    let index = (bit / 8) as usize;
    if let Some(slot) = bits.get_mut(index) {
        *slot |= 1 << (bit % 8);
    }
}

fn bit_is_set(bits: &[u8], bit: u64) -> bool {
    let index = (bit / 8) as usize;
    bits.get(index)
        .is_some_and(|slot| slot & (1 << (bit % 8)) != 0)
}

/// Version-1 image of a version-2 plain file, for readers of the old layout.
#[cfg(test)]
pub(crate) fn legacy_v1_image(v2_plain: &[u8]) -> Result<Vec<u8>> {
    let (version, _, _) = read_zone_header(v2_plain)?;
    if version != ZONE_VERSION {
        return Err(Error::corrupt("expected a version-2 zone image"));
    }
    let body = v2_body(v2_plain)?;
    let payloads = record_payloads(body, ZONE_VERSION)?;
    let mut out = v2_plain[..ZONE_HEADER_LEN].to_vec();
    out[4..6].copy_from_slice(&ZONE_VERSION_V1.to_le_bytes());
    for payload in payloads {
        let crc = crc32fast::hash(payload);
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(payload);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;
    use crate::value::parse_event;

    fn schema() -> Schema {
        parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"},
                    {"name": "note", "type": "text"},
                    {"name": "amount", "type": "decimal", "scale": 2},
                    {"name": "score", "type": "float"}
                ]
            }"#,
        )
        .unwrap()
    }

    fn row(schema: &Schema, json: &str) -> Row {
        parse_event(schema, json.as_bytes()).unwrap()
    }

    fn pred(schema: &Schema, name: &str, value: Scalar) -> ColumnPredicate {
        ColumnPredicate {
            index: schema
                .fields
                .iter()
                .position(|field| field.name == name)
                .unwrap(),
            allowed: vec![value],
        }
    }

    #[test]
    fn exact_string_and_ranges_reject_absent_values() {
        let schema = schema();
        let rows = vec![
            row(
                &schema,
                r#"{"ts":10,"user_id":1,"ok":true,"action":"click","note":"a","amount":"1.00","score":1.5}"#,
            ),
            row(
                &schema,
                r#"{"ts":20,"user_id":2,"ok":false,"action":"view","note":null,"amount":"2.50","score":1.5}"#,
            ),
        ];
        let zone = from_rows(&schema, &rows);
        assert!(!may_match(&zone, &[pred(&schema, "action", "buy".into())]));
        assert!(may_match(&zone, &[pred(&schema, "action", "click".into())]));
        assert!(!may_match(
            &zone,
            &[pred(&schema, "user_id", Scalar::Int(9))]
        ));
        assert!(!may_match(&zone, &[pred(&schema, "user_id", Scalar::Null)]));
        assert!(may_match(&zone, &[pred(&schema, "note", Scalar::Null)]));
        assert!(!may_match(
            &zone,
            &[pred(&schema, "note", "missing".into())]
        ));
        assert!(!may_match(&zone, &[pred(&schema, "ok", Scalar::Null)]));
        assert!(may_match(
            &zone,
            &[pred(&schema, "amount", Scalar::Decimal(100))]
        ));
        assert!(!may_match(
            &zone,
            &[pred(&schema, "amount", Scalar::Decimal(9))]
        ));
        assert!(may_match(
            &zone,
            &[pred(&schema, "score", Scalar::Float(99.0))]
        ));
        let user = schema
            .fields
            .iter()
            .position(|field| field.name == "user_id")
            .unwrap();
        let amount = schema
            .fields
            .iter()
            .position(|field| field.name == "amount")
            .unwrap();
        let score = schema
            .fields
            .iter()
            .position(|field| field.name == "score")
            .unwrap();
        let above = |index, value| crate::codec::RangeFilter {
            index,
            lower: Some(crate::codec::RangeEnd {
                inclusive: false,
                value,
            }),
            upper: None,
        };
        assert!(!may_match_ranges(
            &zone,
            &[],
            &[above(user, Scalar::Int(2))]
        ));
        assert!(may_match_ranges(&zone, &[], &[above(user, Scalar::Int(1))]));
        assert!(!may_match_ranges(
            &zone,
            &[],
            &[above(amount, Scalar::Decimal(900))]
        ));
        assert!(may_match_ranges(
            &zone,
            &[],
            &[above(score, Scalar::Float(100.0))]
        ));
        let encoded = encode_zone(&zone);
        let decoded = decode_zone(&encoded, schema.fields.len() as u16).unwrap();
        assert_eq!(decoded, zone);
    }

    #[test]
    fn bloom_has_no_false_negatives_for_hundreds_of_ids() {
        let schema = schema();
        let mut rows = Vec::new();
        for id in 0..400 {
            let json = format!(
                r#"{{"ts":{id},"user_id":1,"ok":true,"action":"run-{id}","note":"n","amount":"1.00","score":1.0}}"#
            );
            rows.push(row(&schema, &json));
        }
        let zone = from_rows(&schema, &rows);
        assert!(
            matches!(zone.columns[3], ColumnZone::Bloom { .. }),
            "hundreds of distinct strings use the bloom filter"
        );
        for id in 0..400 {
            let value = format!("run-{id}");
            assert!(
                may_match(&zone, &[pred(&schema, "action", value.into())]),
                "missing {id}"
            );
        }
        let mut false_positives = 0usize;
        for id in 0..400 {
            let value = format!("other-{id}");
            if may_match(&zone, &[pred(&schema, "action", value.into())]) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 80,
            "bloom accepted {false_positives} absent values"
        );
    }

    #[test]
    fn plain_zstd_frame_round_trips_and_raw_file_still_loads() {
        let schema = schema();
        let crc = schema_crc(&schema);
        let fields = field_count(&schema).unwrap();
        let zone = from_rows(
            &schema,
            &[row(
                &schema,
                r#"{"ts":10,"user_id":1,"ok":true,"action":"click","note":"a","amount":"1.00","score":1.5}"#,
            )],
        );
        let zones: Vec<BlockZone> = (0..48).map(|_| zone.clone()).collect();
        let dir = std::env::temp_dir().join(format!(
            "eventer-zone-zstd-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = zone_path(&dir, 1);
        let first = append_zones(&dir, 1, crc, fields, &zones[..16], true).unwrap();
        let second = append_zones(&dir, 1, crc, fields, &zones[16..], true).unwrap();
        let stored = fs::read(&path).unwrap();
        assert!(is_zstd_frame(&stored), "repeated zone records compress");
        assert_eq!(first + second, stored.len() as i64);
        let loaded = read_zone_file(&path, crc, fields, zones.len())
            .unwrap()
            .expect("compressed zone file");
        assert_eq!(loaded, zones);

        let raw = decompress_zone_frame(&stored).unwrap();
        assert!(stored.len() < raw.len());
        assert_eq!(
            u16::from_le_bytes(raw[4..6].try_into().unwrap()),
            ZONE_VERSION
        );
        let split = raw.len() - ZONE_TRAILER_LEN;
        assert_eq!(
            crc32fast::hash(&raw[..split]),
            u32::from_le_bytes(raw[split..].try_into().unwrap())
        );
        fs::write(&path, &raw).unwrap();
        let from_raw = read_zone_file(&path, crc, fields, zones.len())
            .unwrap()
            .expect("raw zone file");
        assert_eq!(from_raw, zones);

        fs::write(&path, &stored[..8]).unwrap();
        assert!(read_zone_file(&path, crc, fields, zones.len())
            .unwrap()
            .is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn zone_file_stays_raw_when_the_frame_is_not_smaller() {
        let schema = schema();
        let zone = from_rows(
            &schema,
            &[row(
                &schema,
                r#"{"ts":10,"user_id":1,"ok":true,"action":"click","note":"a","amount":"1.00","score":1.5}"#,
            )],
        );
        let mut raw = header(schema_crc(&schema), field_count(&schema).unwrap()).to_vec();
        write_record(&mut raw, &zone).unwrap();
        seal_zone_image(&mut raw);
        let stored = compress_zone_if_smaller(&raw);
        assert!(stored.len() <= raw.len());
        if stored.len() == raw.len() {
            assert_eq!(stored, raw);
            assert!(!is_zstd_frame(&stored));
        }
        let tiny = b"not-repetitive-zone";
        let stored_tiny = compress_zone_if_smaller(tiny);
        assert!(stored_tiny.len() <= tiny.len());
        if stored_tiny.len() < tiny.len() {
            assert!(is_zstd_frame(&stored_tiny));
        } else {
            assert_eq!(stored_tiny, tiny);
        }
    }

    #[test]
    fn version1_zone_file_loads_and_a_bad_record_crc_still_fails() {
        let schema = schema();
        let crc = schema_crc(&schema);
        let fields = field_count(&schema).unwrap();
        let zone = from_rows(
            &schema,
            &[row(
                &schema,
                r#"{"ts":10,"user_id":1,"ok":true,"action":"click","note":"a","amount":"1.00","score":1.5}"#,
            )],
        );
        let zones = vec![zone.clone(), zone];
        let mut raw = header(crc, fields).to_vec();
        for zone in &zones {
            write_record(&mut raw, zone).unwrap();
        }
        seal_zone_image(&mut raw);
        let v1 = legacy_v1_image(&raw).unwrap();
        assert_eq!(
            u16::from_le_bytes(v1[4..6].try_into().unwrap()),
            ZONE_VERSION_V1
        );
        assert!(v1.len() > raw.len() - ZONE_TRAILER_LEN);

        let dir = std::env::temp_dir().join(format!(
            "eventer-zone-v1-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = zone_path(&dir, 1);
        fs::write(&path, &v1).unwrap();
        let loaded = read_zone_file(&path, crc, fields, zones.len())
            .unwrap()
            .expect("version-1 zone file");
        assert_eq!(loaded, zones);

        let mut bad = v1.clone();
        bad[ZONE_HEADER_LEN + 4] ^= 0xff;
        let err = parse_zone_file(&bad, crc, fields, zones.len()).unwrap_err();
        assert!(
            err.to_string()
                .contains("zone map record checksum mismatch"),
            "{err}"
        );
        fs::write(&path, &bad).unwrap();
        assert!(read_zone_file(&path, crc, fields, zones.len())
            .unwrap()
            .is_none());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn version2_trailer_mismatch_rejects_the_image() {
        let schema = schema();
        let crc = schema_crc(&schema);
        let fields = field_count(&schema).unwrap();
        let zone = from_rows(
            &schema,
            &[row(
                &schema,
                r#"{"ts":10,"user_id":1,"ok":true,"action":"click","note":"a","amount":"1.00","score":1.5}"#,
            )],
        );
        let mut raw = header(crc, fields).to_vec();
        write_record(&mut raw, &zone).unwrap();
        seal_zone_image(&mut raw);
        let loaded = parse_zone_file(&raw, crc, fields, 1).unwrap();
        assert_eq!(loaded, vec![zone.clone()]);
        assert!(matches!(loaded[0].columns[6], ColumnZone::Unknown));

        let mut flipped = raw.clone();
        let last = flipped.len() - 1;
        flipped[last] ^= 0xff;
        let err = parse_zone_file(&flipped, crc, fields, 1).unwrap_err();
        assert!(
            err.to_string().contains("zone map checksum mismatch"),
            "{err}"
        );

        let dir = std::env::temp_dir().join(format!(
            "eventer-zone-v2-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = zone_path(&dir, 1);
        fs::write(&path, &flipped).unwrap();
        assert!(read_zone_file(&path, crc, fields, 1).unwrap().is_none());

        append_zones(&dir, 1, crc, fields, &[zone.clone()], true).unwrap();
        let appended = read_zone_file(&path, crc, fields, 1)
            .unwrap()
            .expect("append replaces a torn image");
        assert_eq!(appended, vec![zone]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn appending_to_a_version1_file_keeps_the_old_records() {
        let schema = schema();
        let crc = schema_crc(&schema);
        let fields = field_count(&schema).unwrap();
        let first = from_rows(
            &schema,
            &[row(
                &schema,
                r#"{"ts":10,"user_id":1,"ok":true,"action":"click","note":"a","amount":"1.00","score":1.5}"#,
            )],
        );
        let second = from_rows(
            &schema,
            &[row(
                &schema,
                r#"{"ts":20,"user_id":2,"ok":false,"action":"view","note":"b","amount":"2.00","score":1.5}"#,
            )],
        );
        let mut image = header(crc, fields).to_vec();
        write_record(&mut image, &first).unwrap();
        seal_zone_image(&mut image);
        let v1 = legacy_v1_image(&image).unwrap();
        let dir = std::env::temp_dir().join(format!(
            "eventer-zone-append-v1-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        fs::write(zone_path(&dir, 1), &v1).unwrap();
        append_zones(&dir, 1, crc, fields, &[second.clone()], true).unwrap();
        let loaded = read_zone_file(&zone_path(&dir, 1), crc, fields, 2)
            .unwrap()
            .expect("version-1 records survive an append");
        assert_eq!(loaded, vec![first, second]);
        let _ = fs::remove_dir_all(&dir);
    }
}
