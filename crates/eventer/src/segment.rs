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
/// New frames: magic, `compressed_len`, crc.
pub const BLOCK_HEADER_LEN: usize = 12;
/// magic, `uncompressed_len`, `compressed_len`, `row_count`, crc.
pub const BLOCK_HEADER_LEN_V20: usize = 20;
/// Older frames also store `min_ts` and `max_ts` ahead of the crc.
pub const BLOCK_HEADER_LEN_V36: usize = 36;
pub const INDEX_HEADER_LEN: usize = 8;
pub const INDEX_ENTRY_LEN: usize = 40;
pub const DICT_HEADER_LEN: usize = 16;
pub const INDEX_VERSION: u16 = 1;
pub const DICT_VERSION: u16 = 1;
/// Trained dictionary cap. The sidecar frames this plus a 16-byte header.
pub const DICT_MAX_BYTES: usize = 4 * 1024;
/// Plain zstd level for the on-disk sidecar. Block frames keep the store level.
const DICT_SIDECAR_ZSTD_LEVEL: i32 = 3;
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
    /// On-disk header size. Not stored in the sparse index.
    pub header_len: u8,
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

    pub fn append_blocks(&mut self, metas: &[BlockMeta], zones: Vec<BlockZone>, zone_bytes: i64) {
        let mut zones = zones.into_iter();
        for meta in metas {
            if self.segments.last().map(|segment| segment.id) != Some(meta.segment_id) {
                self.note_new_segment(meta.segment_id);
            }
            let segment = self.segments.last_mut().expect("segment just inserted");
            let add = u64::from(meta.header_len) + u64::from(meta.compressed_len);
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
        self.index_bytes = self.index_bytes.saturating_add_signed(zone_bytes);
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
        let (frames, data_len, uses_dict) = scan_and_repair(&data, id)?;
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
        let mut indexed = read_index(&index).unwrap_or_default();
        for block in &mut indexed {
            block.segment_id = id;
        }
        let index_len = INDEX_HEADER_LEN as u64 + frames.len() as u64 * INDEX_ENTRY_LEN as u64;
        let index_on_disk = fs::metadata(&index).map(|meta| meta.len()).unwrap_or(0);
        let dictionary = stored.as_ref().map(|dict| dict.bytes.as_slice());
        let blocks = if index_on_disk == index_len && index_supplies(&indexed, &frames) {
            indexed
                .into_iter()
                .zip(frames.iter())
                .map(|(mut block, frame)| {
                    block.header_len = frame.header_len;
                    block
                })
                .collect()
        } else {
            let blocks = recover_block_stats(&data, id, &frames, dictionary, schema)?;
            write_index(&index, &blocks)?;
            blocks
        };
        let zones = zone::load_segment_zones(dir, id, &blocks, dictionary, schema)?;
        let zone_len = fs::metadata(zone::zone_path(dir, id))
            .map(|meta| meta.len())
            .unwrap_or(0);
        catalog.data_bytes += data_len.saturating_add(dict_bytes);
        catalog.index_bytes += index_len.saturating_add(zone_len);
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

struct ScannedFrame {
    offset: u64,
    header_len: u8,
    dictionary: bool,
    compressed_len: u32,
    uncompressed_len: Option<u32>,
    row_count: Option<u32>,
    min_ts: Option<i64>,
    max_ts: Option<i64>,
}

fn index_supplies(indexed: &[BlockMeta], frames: &[ScannedFrame]) -> bool {
    indexed.len() == frames.len()
        && indexed.iter().zip(frames.iter()).all(|(left, frame)| {
            left.offset == frame.offset
                && left.compressed_len == frame.compressed_len
                && frame
                    .uncompressed_len
                    .is_none_or(|value| value == left.uncompressed_len)
                && frame.row_count.is_none_or(|value| value == left.row_count)
                && frame.min_ts.is_none_or(|value| value == left.min_ts)
                && frame.max_ts.is_none_or(|value| value == left.max_ts)
        })
}

/// New block frame: 4-byte magic, `compressed_len`, crc, then the payload.
pub fn frame_block(compressed: &[u8], dictionary: bool) -> Result<Vec<u8>> {
    if compressed.len() > u32::MAX as usize {
        return Err(Error::event("compressed block does not fit in u32"));
    }
    let mut out = Vec::with_capacity(BLOCK_HEADER_LEN + compressed.len());
    out.extend_from_slice(if dictionary {
        BLOCK_MAGIC_DICT
    } else {
        BLOCK_MAGIC
    });
    out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(compressed).to_le_bytes());
    out.extend_from_slice(compressed);
    debug_assert_eq!(out.len(), BLOCK_HEADER_LEN + compressed.len());
    Ok(out)
}

/// A `20`-byte or `36`-byte frame. `min_ts` and `max_ts` are written only for `36`.
#[cfg(test)]
pub fn frame_legacy_block(
    compressed: &[u8],
    uncompressed_len: u32,
    row_count: u32,
    min_ts: i64,
    max_ts: i64,
    dictionary: bool,
    header_len: usize,
) -> Result<Vec<u8>> {
    if compressed.len() > u32::MAX as usize {
        return Err(Error::event("compressed block does not fit in u32"));
    }
    if header_len != BLOCK_HEADER_LEN_V20 && header_len != BLOCK_HEADER_LEN_V36 {
        return Err(Error::corrupt("unsupported legacy block header"));
    }
    let mut out = Vec::with_capacity(header_len + compressed.len());
    out.extend_from_slice(if dictionary {
        BLOCK_MAGIC_DICT
    } else {
        BLOCK_MAGIC
    });
    out.extend_from_slice(&uncompressed_len.to_le_bytes());
    out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    out.extend_from_slice(&row_count.to_le_bytes());
    if header_len == BLOCK_HEADER_LEN_V36 {
        out.extend_from_slice(&min_ts.to_le_bytes());
        out.extend_from_slice(&max_ts.to_le_bytes());
    }
    out.extend_from_slice(&crc32fast::hash(compressed).to_le_bytes());
    if out.len() != header_len {
        return Err(Error::corrupt("legacy block header length mismatch"));
    }
    out.extend_from_slice(compressed);
    Ok(out)
}

fn scan_and_repair(path: &Path, _segment_id: u32) -> Result<(Vec<ScannedFrame>, u64, bool)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    let (frames, offset, uses_dict) = scan_frames(&mut file, file_len)?;
    if offset < file_len {
        file.set_len(offset)?;
        file.sync_all()?;
    }
    Ok((frames, offset, uses_dict))
}

fn scan_frames(file: &mut File, file_len: u64) -> Result<(Vec<ScannedFrame>, u64, bool)> {
    let mut offset = 0u64;
    let mut frames = Vec::new();
    let mut uses_dict = false;
    loop {
        if offset >= file_len {
            break;
        }
        let Some(frame) = read_frame(file, offset, file_len)? else {
            break;
        };
        let next = offset
            .saturating_add(u64::from(frame.header_len))
            .saturating_add(u64::from(frame.compressed_len));
        uses_dict |= frame.dictionary;
        frames.push(frame);
        offset = next;
    }
    Ok((frames, offset, uses_dict))
}

fn read_frame(file: &mut File, offset: u64, file_len: u64) -> Result<Option<ScannedFrame>> {
    if file_len.saturating_sub(offset) < 4 {
        return Ok(None);
    }
    let mut magic = [0u8; 4];
    file.seek(SeekFrom::Start(offset))?;
    if file.read_exact(&mut magic).is_err() {
        return Ok(None);
    }
    let dictionary = if magic == *BLOCK_MAGIC {
        false
    } else if magic == *BLOCK_MAGIC_DICT {
        true
    } else {
        return Ok(None);
    };
    for header_len in [BLOCK_HEADER_LEN, BLOCK_HEADER_LEN_V20, BLOCK_HEADER_LEN_V36] {
        if let Some(frame) = try_frame(file, offset, file_len, header_len, dictionary)? {
            return Ok(Some(frame));
        }
    }
    Ok(None)
}

fn try_frame(
    file: &mut File,
    offset: u64,
    file_len: u64,
    header_len: usize,
    dictionary: bool,
) -> Result<Option<ScannedFrame>> {
    if file_len.saturating_sub(offset) < header_len as u64 {
        return Ok(None);
    }
    let mut header = vec![0u8; header_len];
    file.seek(SeekFrom::Start(offset))?;
    if file.read_exact(&mut header).is_err() {
        return Ok(None);
    }
    let (compressed_len, crc, uncompressed_len, row_count, min_ts, max_ts) = match header_len {
        BLOCK_HEADER_LEN => (
            u32::from_le_bytes(header[4..8].try_into().unwrap()),
            u32::from_le_bytes(header[8..12].try_into().unwrap()),
            None,
            None,
            None,
            None,
        ),
        BLOCK_HEADER_LEN_V20 => (
            u32::from_le_bytes(header[8..12].try_into().unwrap()),
            u32::from_le_bytes(header[16..20].try_into().unwrap()),
            Some(u32::from_le_bytes(header[4..8].try_into().unwrap())),
            Some(u32::from_le_bytes(header[12..16].try_into().unwrap())),
            None,
            None,
        ),
        BLOCK_HEADER_LEN_V36 => (
            u32::from_le_bytes(header[8..12].try_into().unwrap()),
            u32::from_le_bytes(header[32..36].try_into().unwrap()),
            Some(u32::from_le_bytes(header[4..8].try_into().unwrap())),
            Some(u32::from_le_bytes(header[12..16].try_into().unwrap())),
            Some(i64::from_le_bytes(header[16..24].try_into().unwrap())),
            Some(i64::from_le_bytes(header[24..32].try_into().unwrap())),
        ),
        _ => return Ok(None),
    };
    let payload_end = offset + header_len as u64 + u64::from(compressed_len);
    if payload_end > file_len {
        return Ok(None);
    }
    let mut payload = vec![0u8; compressed_len as usize];
    if file.read_exact(&mut payload).is_err() {
        return Ok(None);
    }
    if crc32fast::hash(&payload) != crc {
        return Ok(None);
    }
    let header_len = u8::try_from(header_len)
        .map_err(|_| Error::corrupt("block header length does not fit in u8"))?;
    Ok(Some(ScannedFrame {
        offset,
        header_len,
        dictionary,
        compressed_len,
        uncompressed_len,
        row_count,
        min_ts,
        max_ts,
    }))
}

fn recover_block_stats(
    path: &Path,
    segment_id: u32,
    frames: &[ScannedFrame],
    dictionary: Option<&[u8]>,
    schema: &Schema,
) -> Result<Vec<BlockMeta>> {
    let mut file = File::open(path)?;
    let mut blocks = Vec::with_capacity(frames.len());
    for frame in frames {
        file.seek(SeekFrom::Start(frame.offset + u64::from(frame.header_len)))?;
        let mut payload = vec![0u8; frame.compressed_len as usize];
        file.read_exact(&mut payload)?;
        let raw = if frame.dictionary {
            let Some(dictionary) = dictionary else {
                return Err(Error::corrupt(format!(
                    "dictionary frame at offset {} in segment {segment_id} has no dictionary",
                    frame.offset
                )));
            };
            decompress_payload(&payload, Some(dictionary))?
        } else {
            decompress_payload(&payload, None)?
        };
        let uncompressed_len = u32::try_from(raw.len())
            .map_err(|_| Error::corrupt("uncompressed block does not fit in u32"))?;
        let row_count = u32::try_from(crate::codec::block_row_count(&raw)?)
            .map_err(|_| Error::corrupt("block row count does not fit in u32"))?;
        let rows = crate::codec::decode_block(schema, &raw)?;
        if rows.len() != row_count as usize {
            return Err(Error::corrupt(
                "decoded block row count does not match its header",
            ));
        }
        let min_ts = rows
            .iter()
            .map(|row| row.ts)
            .min()
            .ok_or_else(|| Error::corrupt("decoded block has no rows"))?;
        let max_ts = rows
            .iter()
            .map(|row| row.ts)
            .max()
            .ok_or_else(|| Error::corrupt("decoded block has no rows"))?;
        blocks.push(BlockMeta {
            segment_id,
            offset: frame.offset,
            compressed_len: frame.compressed_len,
            uncompressed_len,
            row_count,
            min_ts,
            max_ts,
            header_len: frame.header_len,
        });
    }
    Ok(blocks)
}

#[cfg(test)]
pub(crate) fn compressed_payloads(path: &Path) -> Result<Vec<Vec<u8>>> {
    let mut file = File::open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    let (frames, _, _) = scan_frames(&mut file, file_len)?;
    let mut out = Vec::with_capacity(frames.len());
    for frame in frames {
        file.seek(SeekFrom::Start(frame.offset + u64::from(frame.header_len)))?;
        let mut payload = vec![0u8; frame.compressed_len as usize];
        file.read_exact(&mut payload)?;
        out.push(payload);
    }
    Ok(out)
}

fn decompress_payload(payload: &[u8], dictionary: Option<&[u8]>) -> Result<Vec<u8>> {
    if let Some(dictionary) = dictionary {
        let mut decoder =
            zstd::stream::read::Decoder::with_dictionary(std::io::Cursor::new(payload), dictionary)
                .map_err(|err| {
                    Error::corrupt(format!("zstd dictionary decompress failed: {err}"))
                })?;
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut out)
            .map_err(|err| Error::corrupt(format!("zstd dictionary decompress failed: {err}")))?;
        Ok(out)
    } else {
        zstd::stream::decode_all(payload)
            .map_err(|err| Error::corrupt(format!("zstd decompress failed: {err}")))
    }
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
    let mut file = File::open(path)?;
    let mut header = [0u8; INDEX_HEADER_LEN];
    file.read_exact(&mut header)?;
    if &header[0..4] != INDEX_MAGIC {
        return Err(Error::corrupt("index magic mismatch"));
    }
    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    if version != INDEX_VERSION {
        return Err(Error::corrupt(format!(
            "unsupported index version {version}"
        )));
    }
    let mut blocks = Vec::new();
    loop {
        let mut entry = [0u8; INDEX_ENTRY_LEN];
        match file.read_exact(&mut entry) {
            Ok(()) => blocks.push(decode_index_entry(&entry)?),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(err) => return Err(err.into()),
        }
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
        header_len: 0,
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

fn write_index(path: &Path, blocks: &[BlockMeta]) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(INDEX_MAGIC)?;
    file.write_all(&INDEX_VERSION.to_le_bytes())?;
    file.write_all(&0u16.to_le_bytes())?;
    for block in blocks {
        // Segment id is not stored in the entry; stamp it so equality checks work.
        let mut owned = block.clone();
        owned.segment_id = 0;
        file.write_all(&index_entry_bytes(&owned))?;
    }
    file.sync_all()?;
    Ok(())
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
            self.index_bytes = self
                .index_bytes
                .saturating_add(segment.index_len)
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
        let header_len = block.header_len as usize;
        if !matches!(
            header_len,
            BLOCK_HEADER_LEN | BLOCK_HEADER_LEN_V20 | BLOCK_HEADER_LEN_V36
        ) {
            discard_file(tmp);
            return Err(Error::corrupt("block header length is invalid"));
        }
        let frame_len = header_len
            .checked_add(block.compressed_len as usize)
            .ok_or_else(|| Error::corrupt("block frame length overflow"))?;
        src.seek(SeekFrom::Start(block.offset))?;
        let mut frame = vec![0u8; frame_len];
        src.read_exact(&mut frame)?;
        if frame[0..4] == BLOCK_MAGIC_DICT[..] {
            uses_dict = true;
        } else if frame[0..4] != BLOCK_MAGIC[..] {
            discard_file(tmp);
            return Err(Error::corrupt(
                "block magic mismatch while retaining a segment",
            ));
        }
        let compressed = &frame[header_len..];
        let crc = u32::from_le_bytes(frame[header_len - 4..header_len].try_into().unwrap());
        if crc32fast::hash(compressed) != crc {
            discard_file(tmp);
            return Err(Error::corrupt("crc mismatch while retaining a segment"));
        }
        out.write_all(&frame)?;
        let mut meta = block.clone();
        meta.offset = offset;
        kept.push(meta);
        offset += frame_len as u64;
    }
    out.sync_all()?;
    Ok((kept, offset, uses_dict))
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
    file.seek(SeekFrom::Start(meta.offset))?;
    let header_len = meta.header_len as usize;
    if !matches!(
        header_len,
        BLOCK_HEADER_LEN | BLOCK_HEADER_LEN_V20 | BLOCK_HEADER_LEN_V36
    ) {
        return Err(Error::corrupt("block header length is invalid"));
    }
    let total = header_len + meta.compressed_len as usize;
    let mut buf = vec![0u8; total];
    file.read_exact(&mut buf)?;
    let dictionary_frame = if buf[0..4] == BLOCK_MAGIC[..] {
        false
    } else if buf[0..4] == BLOCK_MAGIC_DICT[..] {
        true
    } else {
        return Err(Error::corrupt("block magic mismatch while reading"));
    };
    let compressed = &buf[header_len..];
    let crc = u32::from_le_bytes(buf[header_len - 4..header_len].try_into().unwrap());
    if crc32fast::hash(compressed) != crc {
        return Err(Error::corrupt(format!(
            "crc mismatch at offset {} in segment {}",
            meta.offset, meta.segment_id
        )));
    }
    let uncompressed_len = meta.uncompressed_len;
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
pub struct ActiveSegment {
    pub id: u32,
    data: std::io::BufWriter<File>,
    index: std::io::BufWriter<File>,
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
            index: std::io::BufWriter::new(index),
            data_len: 0,
            index_len: INDEX_HEADER_LEN as u64,
        })
    }

    pub fn open_existing(dir: &Path, state: &SegmentState) -> Result<Self> {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .open(data_path(dir, state.id))?;
        let mut index = OpenOptions::new()
            .read(true)
            .write(true)
            .open(index_path(dir, state.id))?;
        data.seek(SeekFrom::Start(state.data_len))?;
        index.seek(SeekFrom::Start(state.index_len))?;
        Ok(Self {
            id: state.id,
            data: std::io::BufWriter::new(data),
            index: std::io::BufWriter::new(index),
            data_len: state.data_len,
            index_len: state.index_len,
        })
    }

    pub fn write_framed(&mut self, framed: &[u8], meta: &BlockMeta) -> Result<()> {
        self.data.write_all(framed)?;
        self.index.write_all(&index_entry_bytes(meta))?;
        self.data_len += framed.len() as u64;
        self.index_len += INDEX_ENTRY_LEN as u64;
        Ok(())
    }

    pub fn flush_os(&mut self, sync: bool) -> Result<()> {
        self.data.flush()?;
        if sync {
            self.data.get_ref().sync_data()?;
        }
        self.index.flush()?;
        if sync {
            self.index.get_ref().sync_data()?;
        }
        Ok(())
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
}
