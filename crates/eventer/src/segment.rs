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
pub const BLOCK_HEADER_LEN: usize = 36;
pub const INDEX_HEADER_LEN: usize = 8;
pub const INDEX_ENTRY_LEN: usize = 40;
pub const DICT_HEADER_LEN: usize = 16;
pub const INDEX_VERSION: u16 = 1;
pub const DICT_VERSION: u16 = 1;
/// Trained dictionary cap. The sidecar stores this plus a 16-byte header.
pub const DICT_MAX_BYTES: usize = 4 * 1024;
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

    pub fn append_blocks(&mut self, metas: &[BlockMeta], zones: Vec<BlockZone>, zone_bytes: i64) {
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
        let (blocks, data_len, uses_dict) = scan_and_repair(&data, id)?;
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
        let index_len = INDEX_HEADER_LEN as u64 + blocks.len() as u64 * INDEX_ENTRY_LEN as u64;
        let index_on_disk = fs::metadata(&index).map(|meta| meta.len()).unwrap_or(0);
        if index_on_disk != index_len || !index_matches(&indexed, &blocks) {
            write_index(&index, &blocks)?;
        }
        let dictionary = stored.as_ref().map(|dict| dict.bytes.as_slice());
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

fn index_matches(indexed: &[BlockMeta], scanned: &[BlockMeta]) -> bool {
    indexed.len() == scanned.len()
        && indexed
            .iter()
            .zip(scanned.iter())
            .all(|(left, right)| left == right)
}

pub fn frame_block(
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
    let mut out = Vec::with_capacity(BLOCK_HEADER_LEN + compressed.len());
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

pub fn scan_and_repair(path: &Path, segment_id: u32) -> Result<(Vec<BlockMeta>, u64, bool)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;
    let mut offset = 0u64;
    let mut blocks = Vec::new();
    let mut uses_dict = false;
    loop {
        if offset >= file_len || file_len - offset < BLOCK_HEADER_LEN as u64 {
            break;
        }
        let mut header = [0u8; BLOCK_HEADER_LEN];
        file.seek(SeekFrom::Start(offset))?;
        if file.read_exact(&mut header).is_err() {
            break;
        }
        let dictionary_frame = if header[0..4] == BLOCK_MAGIC[..] {
            false
        } else if header[0..4] == BLOCK_MAGIC_DICT[..] {
            true
        } else {
            break;
        };
        let uncompressed_len = u32::from_le_bytes(header[4..8].try_into().unwrap());
        let compressed_len = u32::from_le_bytes(header[8..12].try_into().unwrap());
        let row_count = u32::from_le_bytes(header[12..16].try_into().unwrap());
        let min_ts = i64::from_le_bytes(header[16..24].try_into().unwrap());
        let max_ts = i64::from_le_bytes(header[24..32].try_into().unwrap());
        let crc = u32::from_le_bytes(header[32..36].try_into().unwrap());
        let payload_end = offset + BLOCK_HEADER_LEN as u64 + u64::from(compressed_len);
        if payload_end > file_len {
            break;
        }
        let mut payload = vec![0u8; compressed_len as usize];
        if file.read_exact(&mut payload).is_err() {
            break;
        }
        if crc32fast::hash(&payload) != crc {
            break;
        }
        blocks.push(BlockMeta {
            segment_id,
            offset,
            compressed_len,
            uncompressed_len,
            row_count,
            min_ts,
            max_ts,
        });
        uses_dict |= dictionary_frame;
        offset = payload_end;
    }
    if offset < file_len {
        file.set_len(offset)?;
        file.sync_all()?;
    }
    Ok((blocks, offset, uses_dict))
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
    let dict = parse_dictionary_file(&bytes)?;
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
    let mut header = [0u8; DICT_HEADER_LEN];
    header[0..4].copy_from_slice(DICT_MAGIC);
    header[4..6].copy_from_slice(&DICT_VERSION.to_le_bytes());
    header[8..12].copy_from_slice(&(dict.len() as u32).to_le_bytes());
    header[12..16].copy_from_slice(&crc32fast::hash(dict).to_le_bytes());
    let mut file = File::create(&tmp)?;
    file.write_all(&header)?;
    file.write_all(dict)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, &path)?;
    Ok((DICT_HEADER_LEN + dict.len()) as u64)
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

fn read_index(path: &Path) -> Result<Vec<BlockMeta>> {
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

pub fn read_block_payload(
    path: &Path,
    meta: &BlockMeta,
    dictionary: Option<&[u8]>,
) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(meta.offset))?;
    let total = BLOCK_HEADER_LEN + meta.compressed_len as usize;
    let mut buf = vec![0u8; total];
    file.read_exact(&mut buf)?;
    let dictionary_frame = if buf[0..4] == BLOCK_MAGIC[..] {
        false
    } else if buf[0..4] == BLOCK_MAGIC_DICT[..] {
        true
    } else {
        return Err(Error::corrupt("block magic mismatch while reading"));
    };
    let compressed = &buf[BLOCK_HEADER_LEN..];
    let crc = u32::from_le_bytes(buf[32..36].try_into().unwrap());
    if crc32fast::hash(compressed) != crc {
        return Err(Error::corrupt(format!(
            "crc mismatch at offset {} in segment {}",
            meta.offset, meta.segment_id
        )));
    }
    let uncompressed_len = u32::from_le_bytes(buf[4..8].try_into().unwrap());
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
