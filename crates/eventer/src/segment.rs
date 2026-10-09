use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::schema::Schema;
use crate::zone::{self, BlockZone};

pub const BLOCK_MAGIC: &[u8; 4] = b"EVBK";
/// Block payload is a zstd frame compressed with the segment dictionary.
pub const BLOCK_MAGIC_DICT: &[u8; 4] = b"EVBD";
pub const DICT_MAGIC: &[u8; 4] = b"EVZD";
pub const INDEX_MAGIC: &[u8; 4] = b"EVIX";
/// Header written for new frames: magic, lengths, row count, and payload crc.
/// Block min/max timestamps live in the sparse index (`INDEX_ENTRY_LEN` is 40).
pub const BLOCK_HEADER_LEN: usize = 20;
/// Older `EVBK` / `EVBD` header that also stores `min_ts` and `max_ts`.
pub const BLOCK_HEADER_LEN_V1: usize = 36;
pub const INDEX_HEADER_LEN: usize = 8;
pub const INDEX_ENTRY_LEN: usize = 40;
pub const DICT_HEADER_LEN: usize = 16;
pub const INDEX_VERSION: u16 = 1;
pub const DICT_VERSION: u16 = 1;
/// Trained dictionary cap. The sidecar frames this plus a 16-byte header.
pub const DICT_MAX_BYTES: usize = 4 * 1024;
/// Plain zstd level for the on-disk sidecar. Block frames keep the store level.
const DICT_SIDECAR_ZSTD_LEVEL: i32 = 3;
/// Plain zstd level for the sparse index. The segment dictionary is not used.
const INDEX_ZSTD_LEVEL: i32 = 1;
/// Cap for a decompressed index frame. A larger claim is corrupt and the index
/// is rebuilt from the segment.
const MAX_INDEX_UNCOMPRESSED: usize = 512 * 1024 * 1024;
/// Little-endian zstd frame magic (`0xFD2FB528`).
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];
/// Uncompressed sealed-block sample kept before training one dictionary.
pub const DICT_SAMPLE_MAX: usize = 256 * 1024;
/// Slice size for `zstd::dict::from_continuous`. Fast cover keeps a train/test
/// split and rejects a handful of ~40KB blocks, which is all that fits in
/// [`DICT_SAMPLE_MAX`]. The slices are still the same capped sample.
pub const DICT_SAMPLE_CHUNK: usize = 8 * 1024;

/// One compressed block inside a segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockMeta {
    pub segment_id: u32,
    pub offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
    pub row_count: u32,
    pub min_ts: i64,
    pub max_ts: i64,
}

#[derive(Debug, Clone)]
pub struct SegmentState {
    pub id: u32,
    pub blocks: Vec<BlockMeta>,
    pub data_len: u64,
    pub index_len: u64,
    /// Sidecar bytes already counted in [`Catalog::data_bytes`].
    pub dict_bytes: u64,
    /// Scan saw at least one `EVBD` frame. A bad sidecar is fatal only then.
    pub uses_dict: bool,
    /// Equality stats aligned with [`SegmentState::blocks`]. Empty means unknown.
    pub zones: Vec<BlockZone>,
}

#[derive(Debug, Clone)]
pub struct Catalog {
    pub segments: Vec<SegmentState>,
    pub rows: u64,
    pub blocks: u64,
    pub data_bytes: u64,
    pub index_bytes: u64,
}

impl Catalog {
    pub fn empty() -> Self {
        Self {
            segments: Vec::new(),
            rows: 0,
            blocks: 0,
            data_bytes: 0,
            index_bytes: 0,
        }
    }

    pub fn note_new_segment(&mut self, id: u32) {
        self.segments.push(SegmentState {
            id,
            blocks: Vec::new(),
            data_len: 0,
            index_len: INDEX_HEADER_LEN as u64,
            dict_bytes: 0,
            uses_dict: false,
            zones: Vec::new(),
        });
        self.index_bytes += INDEX_HEADER_LEN as u64;
    }

    /// Count a segment dictionary sidecar in the same total as block bytes.
    pub fn add_dictionary_bytes(&mut self, segment_id: u32, nbytes: u64) {
        if let Some(segment) = self
            .segments
            .iter_mut()
            .find(|segment| segment.id == segment_id)
        {
            segment.dict_bytes = segment.dict_bytes.saturating_add(nbytes);
        }
        self.data_bytes = self.data_bytes.saturating_add(nbytes);
    }

    /// `on_disk_delta` is applied after each new index entry is counted at its
    /// raw size. Zone files contribute their whole size change. A rewritten
    /// index contributes the rest, so `index_bytes` matches the files on disk.
    /// The delta can be negative.
    pub fn append_blocks(
        &mut self,
        metas: &[BlockMeta],
        zones: Vec<BlockZone>,
        on_disk_delta: i64,
    ) {
        let mut zones = zones.into_iter();
        for meta in metas {
            if self.segments.last().map(|segment| segment.id) != Some(meta.segment_id) {
                self.note_new_segment(meta.segment_id);
            }
            let segment = self.segments.last_mut().expect("segment just inserted");
            let add = BLOCK_HEADER_LEN as u64 + u64::from(meta.compressed_len);
            segment.data_len += add;
            segment.index_len += INDEX_ENTRY_LEN as u64;
            segment.blocks.push(meta.clone());
            segment.zones.push(zones.next().unwrap_or(BlockZone {
                columns: Vec::new(),
            }));
            self.rows += u64::from(meta.row_count);
            self.blocks += 1;
            self.data_bytes += add;
            self.index_bytes += INDEX_ENTRY_LEN as u64;
        }
        self.index_bytes = self.index_bytes.saturating_add_signed(on_disk_delta);
    }
}

pub fn data_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:06}.dat"))
}

pub fn index_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:06}.idx"))
}

pub fn dictionary_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:06}.dict"))
}

pub fn list_segment_ids(dir: &Path) -> Result<Vec<u32>> {
    let mut ids = Vec::new();
    if !dir.exists() {
        return Ok(ids);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(rest) = name.strip_prefix("seg-") {
            if let Some(num) = rest.strip_suffix(".dat") {
                if num.len() == 6 {
                    if let Ok(id) = num.parse::<u32>() {
                        ids.push(id);
                    }
                }
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Load every segment, drop a torn tail, and rebuild the sparse index when it disagrees.
pub fn load_catalog(dir: &Path, schema: &Schema) -> Result<Catalog> {
    let mut catalog = Catalog::empty();
    for id in list_segment_ids(dir)? {
        let data = data_path(dir, id);
        let index = index_path(dir, id);
        let (scanned, data_len, uses_dict) = scan_and_repair(&data, id)?;
        let stored = match read_dictionary(&dictionary_path(dir, id)) {
            Ok(stored) => stored,
            // A torn sidecar must not hide segments that never used a dictionary.
            // Dictionary frames still fail below, including a missing file.
            Err(Error::Corrupt(_)) if !uses_dict => None,
            Err(err) => return Err(err),
        };
        if uses_dict && stored.is_none() {
            return Err(Error::corrupt(format!(
                "segment {id} has dictionary-compressed blocks but the dictionary file is missing"
            )));
        }
        let dict_bytes = stored.as_ref().map(|dict| dict.file_len).unwrap_or(0);
        let dictionary = stored.as_ref().map(|dict| dict.bytes.as_slice());
        let (mut indexed, index_ok) = match read_index(&index) {
            Ok(blocks) => (blocks, true),
            Err(_) => (Vec::new(), false),
        };
        for block in &mut indexed {
            block.segment_id = id;
        }
        let mut blocks: Vec<BlockMeta> = scanned.iter().map(|block| block.meta.clone()).collect();
        if index_agrees(&indexed, &scanned) {
            for (block, indexed) in blocks.iter_mut().zip(&indexed) {
                block.min_ts = indexed.min_ts;
                block.max_ts = indexed.max_ts;
            }
        } else {
            // Timestamps are not written back into the segment. A 36-byte header
            // already has them; a 20-byte frame is recovered from its payload.
            for (block, scanned) in blocks.iter_mut().zip(&scanned) {
                if scanned.header_timestamps.is_none() {
                    let (min_ts, max_ts) = recover_timestamps(&data, block, dictionary, schema)?;
                    block.min_ts = min_ts;
                    block.max_ts = max_ts;
                }
            }
        }
        let index_len = INDEX_HEADER_LEN as u64 + blocks.len() as u64 * INDEX_ENTRY_LEN as u64;
        let mut index_on_disk = fs::metadata(&index).map(|meta| meta.len()).unwrap_or(0);
        // A shorter zstd frame is valid. Rebuild only when the file is missing,
        // truncated, or its entries disagree with the segment.
        if !index_ok || !index_matches(&indexed, &blocks) {
            write_index(&index, &blocks)?;
            index_on_disk = fs::metadata(&index).map(|meta| meta.len()).unwrap_or(0);
        }
        let zones = zone::load_segment_zones(dir, id, &blocks, dictionary, schema)?;
        let zone_len = fs::metadata(zone::zone_path(dir, id))
            .map(|meta| meta.len())
            .unwrap_or(0);
        catalog.data_bytes += data_len.saturating_add(dict_bytes);
        catalog.index_bytes += index_on_disk.saturating_add(zone_len);
        catalog.rows += blocks
            .iter()
            .map(|block| u64::from(block.row_count))
            .sum::<u64>();
        catalog.blocks += blocks.len() as u64;
        catalog.segments.push(SegmentState {
            id,
            blocks,
            data_len,
            index_len,
            dict_bytes,
            uses_dict,
            zones,
        });
    }
    Ok(catalog)
}

fn index_matches(indexed: &[BlockMeta], scanned: &[BlockMeta]) -> bool {
    indexed.len() == scanned.len()
        && indexed
            .iter()
            .zip(scanned.iter())
            .all(|(left, right)| left == right)
}

struct ScannedBlock {
    meta: BlockMeta,
    /// Set for a 36-byte header. A 20-byte frame leaves this empty.
    header_timestamps: Option<(i64, i64)>,
}

fn index_agrees(indexed: &[BlockMeta], scanned: &[ScannedBlock]) -> bool {
    indexed.len() == scanned.len()
        && indexed.iter().zip(scanned.iter()).all(|(left, right)| {
            left.segment_id == right.meta.segment_id
                && left.offset == right.meta.offset
                && left.compressed_len == right.meta.compressed_len
                && left.uncompressed_len == right.meta.uncompressed_len
                && left.row_count == right.meta.row_count
                && right
                    .header_timestamps
                    .is_none_or(|(min_ts, max_ts)| left.min_ts == min_ts && left.max_ts == max_ts)
        })
}

fn recover_timestamps(
    path: &Path,
    meta: &BlockMeta,
    dictionary: Option<&[u8]>,
    schema: &Schema,
) -> Result<(i64, i64)> {
    let bytes = read_block_payload(path, meta, dictionary)?;
    let rows = crate::codec::decode_block(schema, &bytes)?;
    if rows.is_empty() {
        return Err(Error::corrupt(
            "decoded block has no rows to recover timestamps from",
        ));
    }
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for row in &rows {
        min_ts = min_ts.min(row.ts);
        max_ts = max_ts.max(row.ts);
    }
    Ok((min_ts, max_ts))
}

pub fn frame_block(
    compressed: &[u8],
    uncompressed_len: u32,
    row_count: u32,
    dictionary: bool,
) -> Result<Vec<u8>> {
    if compressed.len() > u32::MAX as usize {
        return Err(Error::event("compressed block does not fit in u32"));
    }
    let mut out = Vec::with_capacity(BLOCK_HEADER_LEN + compressed.len());
    out.extend_from_slice(if dictionary {
        BLOCK_MAGIC_DICT
    } else {
        BLOCK_MAGIC
    });
    out.extend_from_slice(&uncompressed_len.to_le_bytes());
    out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    out.extend_from_slice(&row_count.to_le_bytes());
    let crc = crc32fast::hash(compressed);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(compressed);
    Ok(out)
}

/// 36-byte frame still written by older stores. New segments do not call this.
#[cfg(test)]
pub fn frame_block_v1(
    compressed: &[u8],
    uncompressed_len: u32,
    row_count: u32,
    min_ts: i64,
    max_ts: i64,
    dictionary: bool,
) -> Result<Vec<u8>> {
    if compressed.len() > u32::MAX as usize {
        return Err(Error::event("compressed block does not fit in u32"));
    }
    let mut out = Vec::with_capacity(BLOCK_HEADER_LEN_V1 + compressed.len());
    out.extend_from_slice(if dictionary {
        BLOCK_MAGIC_DICT
    } else {
        BLOCK_MAGIC
    });
    out.extend_from_slice(&uncompressed_len.to_le_bytes());
    out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    out.extend_from_slice(&row_count.to_le_bytes());
    out.extend_from_slice(&min_ts.to_le_bytes());
    out.extend_from_slice(&max_ts.to_le_bytes());
    let crc = crc32fast::hash(compressed);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(compressed);
    Ok(out)
}

struct ParsedFrame {
    header_len: usize,
    dictionary: bool,
    uncompressed_len: u32,
    compressed_len: u32,
    row_count: u32,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

fn try_frame(buf: &[u8], header_len: usize, with_timestamps: bool) -> Option<ParsedFrame> {
    if buf.len() < 16 || header_len < 20 {
        return None;
    }
    let dictionary = if buf[0..4] == BLOCK_MAGIC[..] {
        false
    } else if buf[0..4] == BLOCK_MAGIC_DICT[..] {
        true
    } else {
        return None;
    };
    let uncompressed_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
    let compressed_len = u32::from_le_bytes(buf[8..12].try_into().unwrap()) as usize;
    let row_count = u32::from_le_bytes(buf[12..16].try_into().unwrap());
    let total = header_len.checked_add(compressed_len)?;
    if buf.len() < total {
        return None;
    }
    let (crc_at, min_ts, max_ts) = if with_timestamps {
        if header_len != BLOCK_HEADER_LEN_V1 {
            return None;
        }
        (
            32,
            Some(i64::from_le_bytes(buf[16..24].try_into().unwrap())),
            Some(i64::from_le_bytes(buf[24..32].try_into().unwrap())),
        )
    } else {
        if header_len != BLOCK_HEADER_LEN {
            return None;
        }
        (16, None, None)
    };
    let crc = u32::from_le_bytes(buf[crc_at..crc_at + 4].try_into().unwrap());
    let payload = &buf[header_len..total];
    if payload.len() < 4 || payload[0..4] != ZSTD_FRAME_MAGIC || crc32fast::hash(payload) != crc {
        return None;
    }
    Some(ParsedFrame {
        header_len,
        dictionary,
        uncompressed_len,
        compressed_len: compressed_len as u32,
        row_count,
        min_ts,
        max_ts,
    })
}

/// Prefer a valid 20-byte frame. A buffer that only matches the 36-byte header stays readable.
fn parse_framed_block(buf: &[u8]) -> Option<ParsedFrame> {
    if let Some(frame) = try_frame(buf, BLOCK_HEADER_LEN, false) {
        return Some(frame);
    }
    try_frame(buf, BLOCK_HEADER_LEN_V1, true)
}

fn scan_and_repair(path: &Path, segment_id: u32) -> Result<(Vec<ScannedBlock>, u64, bool)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    let mut offset = 0u64;
    let mut blocks = Vec::new();
    let mut uses_dict = false;
    loop {
        if offset >= file_len || file_len - offset < BLOCK_HEADER_LEN as u64 {
            break;
        }
        let available = file_len - offset;
        file.seek(SeekFrom::Start(offset))?;
        let mut common = [0u8; 16];
        if file.read_exact(&mut common).is_err() {
            break;
        }
        if common[0..4] != BLOCK_MAGIC[..] && common[0..4] != BLOCK_MAGIC_DICT[..] {
            break;
        }
        let compressed_len = u32::from_le_bytes(common[8..12].try_into().unwrap());
        let Some(new_total) = (BLOCK_HEADER_LEN as u64).checked_add(u64::from(compressed_len))
        else {
            break;
        };
        let Some(old_total) = (BLOCK_HEADER_LEN_V1 as u64).checked_add(u64::from(compressed_len))
        else {
            break;
        };
        if available < new_total {
            break;
        }
        let want = if available >= old_total {
            old_total
        } else {
            new_total
        };
        let mut buf = vec![0u8; want as usize];
        buf[..16].copy_from_slice(&common);
        if file.read_exact(&mut buf[16..]).is_err() {
            break;
        }
        let Some(parsed) = parse_framed_block(&buf) else {
            break;
        };
        let header_timestamps = match (parsed.min_ts, parsed.max_ts) {
            (Some(min_ts), Some(max_ts)) => Some((min_ts, max_ts)),
            _ => None,
        };
        blocks.push(ScannedBlock {
            meta: BlockMeta {
                segment_id,
                offset,
                compressed_len: parsed.compressed_len,
                uncompressed_len: parsed.uncompressed_len,
                row_count: parsed.row_count,
                min_ts: header_timestamps.map(|(min_ts, _)| min_ts).unwrap_or(0),
                max_ts: header_timestamps.map(|(_, max_ts)| max_ts).unwrap_or(0),
            },
            header_timestamps,
        });
        uses_dict |= parsed.dictionary;
        let Some(next) = offset
            .checked_add(parsed.header_len as u64)
            .and_then(|pos| pos.checked_add(u64::from(parsed.compressed_len)))
        else {
            break;
        };
        offset = next;
    }
    if offset < file_len {
        file.set_len(offset)?;
        file.sync_all()?;
    }
    Ok((blocks, offset, uses_dict))
}

/// One complete frame inside a segment file, for tests that inspect the layout.
#[cfg(test)]
pub struct OnDiskFrame {
    pub magic: [u8; 4],
    pub header_len: usize,
    pub min_ts: Option<i64>,
    pub max_ts: Option<i64>,
}

#[cfg(test)]
pub fn frames_in(bytes: &[u8]) -> Vec<OnDiskFrame> {
    let mut offset = 0usize;
    let mut frames = Vec::new();
    while offset + BLOCK_HEADER_LEN <= bytes.len() {
        let available = bytes.len() - offset;
        let compressed_len = u32::from_le_bytes(bytes[offset + 8..offset + 12].try_into().unwrap());
        let Some(old_total) = BLOCK_HEADER_LEN_V1.checked_add(compressed_len as usize) else {
            break;
        };
        let want = old_total.min(available);
        let Some(parsed) = parse_framed_block(&bytes[offset..offset + want]) else {
            break;
        };
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[offset..offset + 4]);
        frames.push(OnDiskFrame {
            magic,
            header_len: parsed.header_len,
            min_ts: parsed.min_ts,
            max_ts: parsed.max_ts,
        });
        let Some(next) = offset
            .checked_add(parsed.header_len)
            .and_then(|pos| pos.checked_add(parsed.compressed_len as usize))
        else {
            break;
        };
        offset = next;
    }
    frames
}

/// Dictionary bytes plus the on-disk sidecar length, which includes the header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredDictionary {
    pub bytes: Vec<u8>,
    pub file_len: u64,
}

pub fn read_dictionary(path: &Path) -> Result<Option<StoredDictionary>> {
    let len = match fs::metadata(path) {
        Ok(meta) => meta.len(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let max = (DICT_HEADER_LEN + DICT_MAX_BYTES) as u64;
    if len > max {
        return Err(Error::corrupt("segment dictionary exceeds the size cap"));
    }
    let bytes = fs::read(path)?;
    if bytes.len() as u64 > max {
        return Err(Error::corrupt("segment dictionary exceeds the size cap"));
    }
    let sidecar = decode_dictionary_sidecar(&bytes)?;
    let dict = parse_dictionary_file(&sidecar)?;
    Ok(Some(StoredDictionary {
        bytes: dict,
        file_len: bytes.len() as u64,
    }))
}

pub fn write_dictionary(dir: &Path, id: u32, dict: &[u8]) -> Result<u64> {
    if dict.is_empty() || dict.len() > DICT_MAX_BYTES {
        return Err(Error::corrupt(
            "refusing to store an empty or oversized segment dictionary",
        ));
    }
    let path = dictionary_path(dir, id);
    // Publish via rename so a crash cannot leave a truncated sidecar in place
    // of a previous dictionary, or invent one before the bytes are durable.
    let tmp = dir.join(format!(".seg-{id:06}.dict.partial"));
    let sidecar = encode_dictionary_sidecar(dict);
    // Plain zstd only. The segment dictionary cannot decode its own sidecar.
    let compressed = zstd::bulk::compress(&sidecar, DICT_SIDECAR_ZSTD_LEVEL).map_err(Error::io)?;
    let stored = if compressed.len() < sidecar.len() {
        compressed
    } else {
        sidecar
    };
    let mut file = File::create(&tmp)?;
    file.write_all(&stored)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, &path)?;
    Ok(stored.len() as u64)
}

fn encode_dictionary_sidecar(dict: &[u8]) -> Vec<u8> {
    let mut sidecar = Vec::with_capacity(DICT_HEADER_LEN + dict.len());
    sidecar.extend_from_slice(DICT_MAGIC);
    sidecar.extend_from_slice(&DICT_VERSION.to_le_bytes());
    sidecar.extend_from_slice(&0u16.to_le_bytes());
    sidecar.extend_from_slice(&(dict.len() as u32).to_le_bytes());
    sidecar.extend_from_slice(&crc32fast::hash(dict).to_le_bytes());
    sidecar.extend_from_slice(dict);
    sidecar
}

/// Expand a plain zstd sidecar, or return a raw `EVZD` file unchanged.
fn decode_dictionary_sidecar(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() >= 4 && bytes[0..4] == ZSTD_FRAME_MAGIC {
        let cap = DICT_HEADER_LEN + DICT_MAX_BYTES;
        zstd::bulk::decompress(bytes, cap)
            .map_err(|_| Error::corrupt("truncated segment dictionary"))
    } else {
        Ok(bytes.to_vec())
    }
}

/// True when the block frame at `meta.offset` is dictionary-compressed (`EVBD`).
pub fn frame_uses_dictionary(path: &Path, meta: &BlockMeta) -> Result<bool> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(meta.offset))?;
    let mut magic = [0u8; 4];
    file.read_exact(&mut magic)?;
    if magic == *BLOCK_MAGIC {
        Ok(false)
    } else if magic == *BLOCK_MAGIC_DICT {
        Ok(true)
    } else {
        Err(Error::corrupt("block magic mismatch while reading"))
    }
}

fn parse_dictionary_file(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() < DICT_HEADER_LEN {
        return Err(Error::corrupt("truncated segment dictionary"));
    }
    if bytes[0..4] != DICT_MAGIC[..] {
        return Err(Error::corrupt("segment dictionary magic mismatch"));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    if version != DICT_VERSION {
        return Err(Error::corrupt(format!(
            "unsupported dictionary version {version}"
        )));
    }
    let dict_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
    let total = DICT_HEADER_LEN
        .checked_add(dict_len)
        .ok_or_else(|| Error::corrupt("segment dictionary length overflow"))?;
    if bytes.len() != total {
        return Err(Error::corrupt("truncated segment dictionary"));
    }
    let dict = &bytes[DICT_HEADER_LEN..];
    if dict.is_empty() || dict.len() > DICT_MAX_BYTES {
        return Err(Error::corrupt("segment dictionary size is invalid"));
    }
    if crc32fast::hash(dict) != crc {
        return Err(Error::corrupt("segment dictionary checksum mismatch"));
    }
    Ok(dict.to_vec())
}

pub(crate) fn read_index(path: &Path) -> Result<Vec<BlockMeta>> {
    let bytes = fs::read(path)?;
    let plain = plain_index_bytes(&bytes)?;
    parse_index(&plain)
}

fn plain_index_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    if is_zstd_frame(bytes) {
        decompress_index_frame(bytes, MAX_INDEX_UNCOMPRESSED)
    } else {
        Ok(bytes.to_vec())
    }
}

fn is_zstd_frame(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes[..4] == ZSTD_FRAME_MAGIC
}

/// Compress `raw` EVIX bytes with plain zstd level 1. The frame is used only
/// when it is strictly smaller than `raw`.
fn compress_index_if_smaller(raw: &[u8]) -> Vec<u8> {
    match zstd::bulk::compress(raw, INDEX_ZSTD_LEVEL) {
        Ok(frame) if frame.len() < raw.len() && is_zstd_frame(&frame) => frame,
        _ => raw.to_vec(),
    }
}

fn decompress_index_frame(frame: &[u8], cap: usize) -> Result<Vec<u8>> {
    let mut decoder = zstd::stream::Decoder::with_buffer(frame).map_err(Error::io)?;
    let mut out = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        let n = match decoder.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => n,
            Err(_) => return Err(Error::corrupt("truncated index frame")),
        };
        if out.len().saturating_add(n) > cap {
            return Err(Error::corrupt("index exceeds the decompress bound"));
        }
        out.extend_from_slice(&buf[..n]);
    }
    Ok(out)
}

fn parse_index(plain: &[u8]) -> Result<Vec<BlockMeta>> {
    if plain.len() < INDEX_HEADER_LEN || &plain[0..4] != INDEX_MAGIC {
        return Err(Error::corrupt("index magic mismatch"));
    }
    let version = u16::from_le_bytes(plain[4..6].try_into().unwrap());
    if version != INDEX_VERSION {
        return Err(Error::corrupt(format!(
            "unsupported index version {version}"
        )));
    }
    let body = &plain[INDEX_HEADER_LEN..];
    if body.len() % INDEX_ENTRY_LEN != 0 {
        return Err(Error::corrupt("truncated index"));
    }
    let mut blocks = Vec::with_capacity(body.len() / INDEX_ENTRY_LEN);
    for chunk in body.chunks_exact(INDEX_ENTRY_LEN) {
        let entry: [u8; INDEX_ENTRY_LEN] = chunk.try_into().unwrap();
        blocks.push(decode_index_entry(&entry)?);
    }
    Ok(blocks)
}

fn decode_index_entry(entry: &[u8; INDEX_ENTRY_LEN]) -> Result<BlockMeta> {
    Ok(BlockMeta {
        segment_id: 0,
        offset: u64::from_le_bytes(entry[0..8].try_into().unwrap()),
        compressed_len: u32::from_le_bytes(entry[8..12].try_into().unwrap()),
        uncompressed_len: u32::from_le_bytes(entry[12..16].try_into().unwrap()),
        row_count: u32::from_le_bytes(entry[16..20].try_into().unwrap()),
        min_ts: i64::from_le_bytes(entry[24..32].try_into().unwrap()),
        max_ts: i64::from_le_bytes(entry[32..40].try_into().unwrap()),
    })
}

pub fn index_entry_bytes(meta: &BlockMeta) -> [u8; INDEX_ENTRY_LEN] {
    let mut entry = [0u8; INDEX_ENTRY_LEN];
    entry[0..8].copy_from_slice(&meta.offset.to_le_bytes());
    entry[8..12].copy_from_slice(&meta.compressed_len.to_le_bytes());
    entry[12..16].copy_from_slice(&meta.uncompressed_len.to_le_bytes());
    entry[16..20].copy_from_slice(&meta.row_count.to_le_bytes());
    entry[24..32].copy_from_slice(&meta.min_ts.to_le_bytes());
    entry[32..40].copy_from_slice(&meta.max_ts.to_le_bytes());
    entry
}

fn encode_index(blocks: &[BlockMeta]) -> Vec<u8> {
    let mut raw = Vec::with_capacity(INDEX_HEADER_LEN + blocks.len() * INDEX_ENTRY_LEN);
    raw.extend_from_slice(INDEX_MAGIC);
    raw.extend_from_slice(&INDEX_VERSION.to_le_bytes());
    raw.extend_from_slice(&0u16.to_le_bytes());
    for block in blocks {
        // Segment id is not stored in the entry; stamp it so equality checks work.
        let mut owned = block.clone();
        owned.segment_id = 0;
        raw.extend_from_slice(&index_entry_bytes(&owned));
    }
    raw
}

fn write_index(path: &Path, blocks: &[BlockMeta]) -> Result<()> {
    let raw = encode_index(blocks);
    let stored = compress_index_if_smaller(&raw);
    write_index_file(path, &stored, true)
}

/// Replace the index file as a whole so a zstd frame is never appended onto a
/// raw prefix.
fn write_index_file(path: &Path, stored: &[u8], sync: bool) -> Result<()> {
    let tmp = index_temp_path(path);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(stored)?;
        file.flush()?;
        if sync {
            file.sync_all()?;
        }
    }
    if tmp != path {
        fs::rename(&tmp, path)?;
    }
    Ok(())
}

/// Retention writes `.seg-NNNNNN.idx.partial` and unlinks that name if publish
/// fails. Replacing the extension of that path nests `.idx.idx.partial`, which
/// the error path does not remove.
fn index_temp_path(path: &Path) -> PathBuf {
    let name = path.file_name().and_then(|name| name.to_str()).unwrap_or("");
    if name.ends_with(".idx.partial") {
        path.to_path_buf()
    } else {
        path.with_extension("idx.partial")
    }
}

/// Drop blocks whose `max_ts` is strictly less than `cutoff_ms`.
///
/// `None` means the segment file was removed. `Some` is the segment to keep,
/// unchanged when no block was eligible. A mixed segment is published by
/// renaming a finished temp file over the old data file. The index and zone
/// map are renamed first, so a crash before the data rename still has the old
/// `.dat`. Open then rebuilds the index from that file.
pub(crate) fn drop_eligible_blocks(
    dir: &Path,
    segment: &SegmentState,
    schema_crc: u32,
    field_count: u16,
    cutoff_ms: i64,
) -> Result<Option<SegmentState>> {
    if segment.blocks.is_empty() {
        return Ok(Some(segment.clone()));
    }
    let keep: Vec<bool> = segment
        .blocks
        .iter()
        .map(|block| block.max_ts >= cutoff_ms)
        .collect();
    if keep.iter().all(|keep_block| *keep_block) {
        return Ok(Some(segment.clone()));
    }
    if keep.iter().all(|keep_block| !*keep_block) {
        remove_segment_files(dir, segment.id)?;
        return Ok(None);
    }
    let (blocks, data_len, uses_dict) = publish_retained_segment(
        dir,
        segment.id,
        &segment.blocks,
        &segment.zones,
        schema_crc,
        field_count,
        &keep,
    )?;
    let zones = segment
        .zones
        .iter()
        .zip(keep.iter())
        .filter(|(_, keep_block)| **keep_block)
        .map(|(zone, _)| zone.clone())
        .collect();
    Ok(Some(SegmentState {
        id: segment.id,
        blocks,
        data_len,
        index_len: INDEX_HEADER_LEN as u64
            + keep.iter().filter(|keep_block| **keep_block).count() as u64 * INDEX_ENTRY_LEN as u64,
        dict_bytes: segment.dict_bytes,
        uses_dict,
        zones,
    }))
}

impl Catalog {
    pub(crate) fn recompute_totals(&mut self, dir: &Path) {
        self.rows = 0;
        self.blocks = 0;
        self.data_bytes = 0;
        self.index_bytes = 0;
        for segment in &self.segments {
            self.rows += segment
                .blocks
                .iter()
                .map(|block| u64::from(block.row_count))
                .sum::<u64>();
            self.blocks += segment.blocks.len() as u64;
            self.data_bytes = self
                .data_bytes
                .saturating_add(segment.data_len)
                .saturating_add(segment.dict_bytes);
            let zone_len = fs::metadata(zone::zone_path(dir, segment.id))
                .map(|meta| meta.len())
                .unwrap_or(0);
            // `index_len` stays the raw EVIX size. Append checks that against
            // the decompressed frame. The catalog counts the file on disk.
            let index_on_disk = fs::metadata(index_path(dir, segment.id))
                .map(|meta| meta.len())
                .unwrap_or(0);
            self.index_bytes = self
                .index_bytes
                .saturating_add(index_on_disk)
                .saturating_add(zone_len);
        }
    }
}

fn remove_segment_files(dir: &Path, id: u32) -> Result<()> {
    // Unlink the data file first. After that, open no longer lists the segment,
    // so a crash cannot resurrect its rows. Removing the dictionary first would
    // make an `EVBD` segment fail to open if the data file were still present.
    for path in [
        data_path(dir, id),
        index_path(dir, id),
        zone::zone_path(dir, id),
        dictionary_path(dir, id),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

fn publish_retained_segment(
    dir: &Path,
    id: u32,
    blocks: &[BlockMeta],
    zones: &[BlockZone],
    schema_crc: u32,
    field_count: u16,
    keep: &[bool],
) -> Result<(Vec<BlockMeta>, u64, bool)> {
    let dat_tmp = dir.join(format!(".seg-{id:06}.dat.partial"));
    let idx_tmp = dir.join(format!(".seg-{id:06}.idx.partial"));
    let zon_tmp = dir.join(format!(".seg-{id:06}.zon.partial"));
    let (kept, data_len, uses_dict) =
        match write_kept_frames(&dat_tmp, &data_path(dir, id), blocks, keep) {
            Ok(written) => written,
            Err(err) => {
                discard_file(&dat_tmp);
                return Err(err);
            }
        };
    if let Err(err) = write_index(&idx_tmp, &kept) {
        discard_file(&dat_tmp);
        discard_file(&idx_tmp);
        return Err(err);
    }
    if let Err(err) =
        zone::stage_kept_zones(dir, id, schema_crc, field_count, zones, keep, &zon_tmp)
    {
        discard_file(&dat_tmp);
        discard_file(&idx_tmp);
        discard_file(&zon_tmp);
        return Err(err);
    }
    if uses_dict && read_dictionary(&dictionary_path(dir, id))?.is_none() {
        discard_file(&dat_tmp);
        discard_file(&idx_tmp);
        discard_file(&zon_tmp);
        return Err(Error::corrupt(format!(
            "segment {id} keeps a dictionary frame but the dictionary file is missing"
        )));
    }
    // Rename the index and zone map before the data file. A crash in between
    // leaves the old `.dat`. The next open scans it and rebuilds a mismatched
    // index, so the partial temp is never the file `open` treats as the segment.
    if let Err(err) = fs::rename(&idx_tmp, index_path(dir, id)) {
        discard_file(&dat_tmp);
        discard_file(&idx_tmp);
        discard_file(&zon_tmp);
        return Err(err.into());
    }
    if let Err(err) = fs::rename(&zon_tmp, zone::zone_path(dir, id)) {
        discard_file(&dat_tmp);
        discard_file(&zon_tmp);
        return Err(err.into());
    }
    if let Err(err) = fs::rename(&dat_tmp, data_path(dir, id)) {
        discard_file(&dat_tmp);
        return Err(err.into());
    }
    Ok((kept, data_len, uses_dict))
}

fn write_kept_frames(
    tmp: &Path,
    src_path: &Path,
    blocks: &[BlockMeta],
    keep: &[bool],
) -> Result<(Vec<BlockMeta>, u64, bool)> {
    let mut src = File::open(src_path)?;
    let mut out = File::create(tmp)?;
    let mut kept = Vec::new();
    let mut offset = 0u64;
    let mut uses_dict = false;
    for (block, keep_block) in blocks.iter().zip(keep.iter()) {
        if !keep_block {
            continue;
        }
        let frame = read_kept_frame(&mut src, block).map_err(|err| {
            discard_file(tmp);
            err
        })?;
        if frame[0..4] == BLOCK_MAGIC_DICT[..] {
            uses_dict = true;
        }
        out.write_all(&frame)?;
        let mut meta = block.clone();
        meta.offset = offset;
        kept.push(meta);
        offset += frame.len() as u64;
    }
    out.sync_all()?;
    Ok((kept, offset, uses_dict))
}

/// Copy one on-disk frame. New frames are 20 bytes; a legacy frame is 36.
/// The CRC sits at offset 16 or 32 to match that header.
fn read_kept_frame(src: &mut File, block: &BlockMeta) -> Result<Vec<u8>> {
    let file_len = src.metadata()?.len();
    if block.offset >= file_len {
        return Err(Error::corrupt(
            "block offset is past the end of the segment while retaining",
        ));
    }
    let available = (file_len - block.offset) as usize;
    let compressed_len = block.compressed_len as usize;
    let new_total = BLOCK_HEADER_LEN
        .checked_add(compressed_len)
        .ok_or_else(|| Error::corrupt("block frame length overflow"))?;
    let old_total = BLOCK_HEADER_LEN_V1
        .checked_add(compressed_len)
        .ok_or_else(|| Error::corrupt("block frame length overflow"))?;
    if available < new_total {
        return Err(Error::corrupt("truncated block while retaining a segment"));
    }
    let want = old_total.min(available);
    src.seek(SeekFrom::Start(block.offset))?;
    let mut buf = vec![0u8; want];
    src.read_exact(&mut buf)?;
    if buf[0..4] != BLOCK_MAGIC[..] && buf[0..4] != BLOCK_MAGIC_DICT[..] {
        return Err(Error::corrupt(
            "block magic mismatch while retaining a segment",
        ));
    }
    let Some(parsed) = parse_framed_block(&buf) else {
        return Err(Error::corrupt("crc mismatch while retaining a segment"));
    };
    if parsed.compressed_len != block.compressed_len {
        return Err(Error::corrupt("crc mismatch while retaining a segment"));
    }
    let frame_len = parsed
        .header_len
        .checked_add(parsed.compressed_len as usize)
        .ok_or_else(|| Error::corrupt("block frame length overflow"))?;
    buf.truncate(frame_len);
    Ok(buf)
}

fn discard_file(path: &Path) {
    let _ = fs::remove_file(path);
}

pub fn read_block_payload(
    path: &Path,
    meta: &BlockMeta,
    dictionary: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if meta.offset >= file_len {
        return Err(Error::corrupt(format!(
            "block offset {} is past the end of segment {}",
            meta.offset, meta.segment_id
        )));
    }
    let available = file_len - meta.offset;
    let new_total = (BLOCK_HEADER_LEN as u64)
        .checked_add(u64::from(meta.compressed_len))
        .ok_or_else(|| Error::corrupt("block frame length overflow"))?;
    let old_total = (BLOCK_HEADER_LEN_V1 as u64)
        .checked_add(u64::from(meta.compressed_len))
        .ok_or_else(|| Error::corrupt("block frame length overflow"))?;
    if available < new_total {
        return Err(Error::corrupt(format!(
            "truncated block at offset {} in segment {}",
            meta.offset, meta.segment_id
        )));
    }
    let want = if available >= old_total {
        old_total
    } else {
        new_total
    };
    file.seek(SeekFrom::Start(meta.offset))?;
    let mut buf = vec![0u8; want as usize];
    file.read_exact(&mut buf)?;
    let Some(parsed) = parse_framed_block(&buf) else {
        return Err(Error::corrupt(format!(
            "crc mismatch at offset {} in segment {}",
            meta.offset, meta.segment_id
        )));
    };
    if parsed.compressed_len != meta.compressed_len {
        return Err(Error::corrupt(format!(
            "compressed length does not match the index at offset {} in segment {}",
            meta.offset, meta.segment_id
        )));
    }
    let dictionary_frame = parsed.dictionary;
    let compressed = &buf[parsed.header_len..parsed.header_len + parsed.compressed_len as usize];
    let uncompressed_len = parsed.uncompressed_len;
    if uncompressed_len != meta.uncompressed_len {
        return Err(Error::corrupt(
            "uncompressed length does not match the index",
        ));
    }
    if dictionary_frame {
        let Some(dictionary) = dictionary else {
            return Err(Error::corrupt(format!(
                "dictionary frame at offset {} in segment {} has no dictionary",
                meta.offset, meta.segment_id
            )));
        };
        let mut decompressor =
            zstd::bulk::Decompressor::with_dictionary(dictionary).map_err(|err| {
                Error::corrupt(format!(
                    "zstd dictionary decompressor failed at offset {} in segment {}: {err}",
                    meta.offset, meta.segment_id
                ))
            })?;
        decompressor
            .decompress(compressed, uncompressed_len as usize)
            .map_err(|err| {
                Error::corrupt(format!(
                    "zstd dictionary decompress failed at offset {} in segment {}: {err}",
                    meta.offset, meta.segment_id
                ))
            })
    } else {
        zstd::bulk::decompress(compressed, uncompressed_len as usize)
            .map_err(|err| Error::corrupt(format!("zstd decompress failed: {err}")))
    }
}

/// Append-only writer for the current segment.
///
/// Index entries are buffered and the whole index file is replaced at the end
/// of a batch. A plain zstd frame is written only when it is strictly smaller
/// than the raw `EVIX` bytes.
pub struct ActiveSegment {
    pub id: u32,
    data: std::io::BufWriter<File>,
    dir: PathBuf,
    pending_index: Vec<u8>,
    pub data_len: u64,
    pub index_len: u64,
}

impl ActiveSegment {
    pub fn create_new(dir: &Path, id: u32) -> Result<Self> {
        let data_path = data_path(dir, id);
        let index_path = index_path(dir, id);
        let data = File::create(&data_path)?;
        let mut index = File::create(&index_path)?;
        index.write_all(INDEX_MAGIC)?;
        index.write_all(&INDEX_VERSION.to_le_bytes())?;
        index.write_all(&0u16.to_le_bytes())?;
        index.flush()?;
        Ok(Self {
            id,
            data: std::io::BufWriter::new(data),
            dir: dir.to_path_buf(),
            pending_index: Vec::new(),
            data_len: 0,
            index_len: INDEX_HEADER_LEN as u64,
        })
    }

    pub fn open_existing(dir: &Path, state: &SegmentState) -> Result<Self> {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_path(dir, state.id))?;
        data.seek(SeekFrom::Start(state.data_len))?;
        Ok(Self {
            id: state.id,
            data: std::io::BufWriter::new(data),
            dir: dir.to_path_buf(),
            pending_index: Vec::new(),
            data_len: state.data_len,
            index_len: state.index_len,
        })
    }

    pub fn write_framed(&mut self, framed: &[u8], meta: &BlockMeta) -> Result<()> {
        self.data.write_all(framed)?;
        self.pending_index
            .extend_from_slice(&index_entry_bytes(meta));
        self.data_len += framed.len() as u64;
        self.index_len += INDEX_ENTRY_LEN as u64;
        Ok(())
    }

    /// Flush the data file and, when index entries are buffered, rewrite the
    /// index. The returned delta is `new_index_len - previous_index_len -
    /// raw_entries_just_appended`, so catalog `index_bytes` can stay equal to
    /// the file on disk after it has already counted those raw entries.
    pub fn flush_os(&mut self, sync: bool) -> Result<i64> {
        self.data.flush()?;
        if sync {
            self.data.get_ref().sync_data()?;
        }
        if self.pending_index.is_empty() {
            if sync {
                if let Ok(file) = File::open(index_path(&self.dir, self.id)) {
                    file.sync_all()?;
                }
            }
            return Ok(0);
        }
        self.persist_pending_index(sync)
    }

    fn persist_pending_index(&mut self, sync: bool) -> Result<i64> {
        let path = index_path(&self.dir, self.id);
        let before = fs::metadata(&path)?.len();
        let existing = fs::read(&path)?;
        let mut raw = plain_index_bytes(&existing)?;
        let added = self.pending_index.len() as u64;
        if raw.len() as u64 + added != self.index_len {
            return Err(Error::corrupt("index length does not match the segment"));
        }
        raw.extend_from_slice(&self.pending_index);
        let stored = compress_index_if_smaller(&raw);
        write_index_file(&path, &stored, sync)?;
        self.pending_index.clear();
        Ok(stored.len() as i64 - before as i64 - added as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "eventer-dict-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn write_index_on_retention_partial_does_not_nest_another_temp() {
        let dir = scratch_dir();
        let partial = dir.join(".seg-000001.idx.partial");
        let block = BlockMeta {
            segment_id: 1,
            offset: 0,
            compressed_len: 1,
            uncompressed_len: 1,
            row_count: 1,
            min_ts: 0,
            max_ts: 0,
        };
        write_index(&partial, &[block]).unwrap();
        assert!(partial.is_file());
        assert!(!dir.join(".seg-000001.idx.idx.partial").exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn plain_zstd_sidecar_round_trips_and_raw_evzd_still_loads() {
        let dir = scratch_dir();
        let dict: Vec<u8> = (0..1024).map(|i| (i % 17) as u8).collect();
        let file_len = write_dictionary(&dir, 1, &dict).unwrap();
        let path = dictionary_path(&dir, 1);
        let on_disk = fs::read(&path).unwrap();
        assert_eq!(file_len, on_disk.len() as u64);
        assert!(on_disk.len() < DICT_HEADER_LEN + dict.len());
        assert_eq!(&on_disk[..4], &ZSTD_FRAME_MAGIC);
        let stored = read_dictionary(&path).unwrap().unwrap();
        assert_eq!(stored.bytes, dict);
        assert_eq!(stored.file_len, file_len);

        let raw = encode_dictionary_sidecar(&dict);
        fs::write(&path, &raw).unwrap();
        let stored = read_dictionary(&path).unwrap().unwrap();
        assert_eq!(stored.bytes, dict);
        assert_eq!(stored.file_len, raw.len() as u64);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn incompressible_sidecar_stays_raw_evzd() {
        let dir = scratch_dir();
        // Already a zstd frame, so a second plain pass does not shrink the sidecar.
        let seed: Vec<u8> = (0..384u32)
            .map(|i| (i.wrapping_mul(17) ^ 0xA5) as u8)
            .collect();
        let dict = zstd::bulk::compress(&seed, 19).unwrap();
        assert!(dict.len() <= DICT_MAX_BYTES);
        let file_len = write_dictionary(&dir, 3, &dict).unwrap();
        let path = dictionary_path(&dir, 3);
        let on_disk = fs::read(&path).unwrap();
        let raw = encode_dictionary_sidecar(&dict);
        assert_eq!(
            on_disk, raw,
            "plain zstd grew or tied; raw EVZD must be kept"
        );
        assert_eq!(file_len, raw.len() as u64);
        assert_eq!(read_dictionary(&path).unwrap().unwrap().bytes, dict);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_zstd_sidecar_is_corrupt() {
        let dir = scratch_dir();
        let dict = vec![7u8; 256];
        write_dictionary(&dir, 1, &dict).unwrap();
        let path = dictionary_path(&dir, 1);
        let on_disk = fs::read(&path).unwrap();
        assert_eq!(&on_disk[..4], &ZSTD_FRAME_MAGIC);
        fs::write(&path, &on_disk[..on_disk.len() - 1]).unwrap();
        let err = read_dictionary(&path).unwrap_err();
        assert!(
            err.to_string().contains("truncated segment dictionary"),
            "{err}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn zstd_sidecar_above_the_dictionary_cap_is_corrupt() {
        let dir = scratch_dir();
        let path = dictionary_path(&dir, 1);
        let huge = vec![0u8; DICT_HEADER_LEN + DICT_MAX_BYTES + 64];
        let frame = zstd::bulk::compress(&huge, 1).unwrap();
        assert!(frame.len() <= DICT_HEADER_LEN + DICT_MAX_BYTES);
        fs::write(&path, &frame).unwrap();
        let err = read_dictionary(&path).unwrap_err();
        assert!(err.to_string().contains("corrupt"), "{err}");
        let _ = fs::remove_dir_all(&dir);
    }

    fn sample_meta(index: u32) -> BlockMeta {
        BlockMeta {
            segment_id: 1,
            offset: u64::from(index) * 128,
            compressed_len: 64,
            uncompressed_len: 256,
            row_count: 32,
            min_ts: i64::from(index) * 1_000,
            max_ts: i64::from(index) * 1_000 + 999,
        }
    }

    #[test]
    fn plain_zstd_index_round_trips_and_raw_evix_still_loads() {
        let dir = scratch_dir();
        let mut active = ActiveSegment::create_new(&dir, 1).unwrap();
        let metas: Vec<BlockMeta> = (0..40).map(sample_meta).collect();
        for meta in &metas {
            active.write_framed(&[0u8; 4], meta).unwrap();
        }
        let delta = active.flush_os(true).unwrap();
        let path = index_path(&dir, 1);
        let on_disk = fs::read(&path).unwrap();
        let raw = encode_index(&metas);
        assert!(is_zstd_frame(&on_disk), "repeated index entries compress");
        assert!(on_disk.len() < raw.len());
        assert_eq!(
            delta,
            on_disk.len() as i64 - raw.len() as i64,
            "delta is the savings versus the raw entries already counted"
        );
        let loaded = read_index(&path).unwrap();
        assert_eq!(loaded.len(), metas.len());
        for (got, expect) in loaded.iter().zip(&metas) {
            let mut expect = expect.clone();
            expect.segment_id = 0;
            assert_eq!(got, &expect);
        }

        fs::write(&path, &raw).unwrap();
        let loaded = read_index(&path).unwrap();
        assert_eq!(loaded.len(), metas.len());
        assert_eq!(
            fs::read(&path).unwrap(),
            raw,
            "a raw EVIX file is left in place"
        );

        let mut again = ActiveSegment::open_existing(
            &dir,
            &SegmentState {
                id: 1,
                blocks: Vec::new(),
                data_len: active.data_len,
                index_len: active.index_len,
                dict_bytes: 0,
                uses_dict: false,
                zones: Vec::new(),
            },
        )
        .unwrap();
        let extra = sample_meta(40);
        again.write_framed(&[1u8; 4], &extra).unwrap();
        again.flush_os(true).unwrap();
        let rewritten = fs::read(&path).unwrap();
        assert!(
            is_zstd_frame(&rewritten),
            "a later batch rewrites the whole file"
        );
        assert_ne!(&rewritten[..4], INDEX_MAGIC);
        let loaded = read_index(&path).unwrap();
        assert_eq!(loaded.len(), metas.len() + 1);
        assert_eq!(loaded.last().unwrap().offset, extra.offset);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn incompressible_index_stays_raw_evix() {
        let header = encode_index(&[]);
        assert_eq!(compress_index_if_smaller(&header), header);
        let already = zstd::bulk::compress(&vec![1u8; 256], 1).unwrap();
        assert!(
            already.len() >= 4 && already[..4] == ZSTD_FRAME_MAGIC,
            "fixture should already be a zstd frame"
        );
        assert_eq!(
            compress_index_if_smaller(&already),
            already,
            "plain zstd grew or tied; the original bytes must be kept"
        );
    }

    #[test]
    fn truncated_zstd_index_is_corrupt_and_oversize_frame_is_rejected() {
        let dir = scratch_dir();
        let metas: Vec<BlockMeta> = (0..40).map(sample_meta).collect();
        let frame = compress_index_if_smaller(&encode_index(&metas));
        assert!(is_zstd_frame(&frame));
        let path = index_path(&dir, 1);
        fs::write(&path, &frame[..frame.len() - 1]).unwrap();
        let err = read_index(&path).unwrap_err();
        assert!(err.to_string().contains("truncated index"), "{err}");

        let big = vec![7u8; 4096];
        let big_frame = zstd::bulk::compress(&big, 1).unwrap();
        let err = decompress_index_frame(&big_frame, 64).unwrap_err();
        assert!(err.to_string().contains("decompress bound"), "{err}");
        let _ = fs::remove_dir_all(&dir);
    }
}
