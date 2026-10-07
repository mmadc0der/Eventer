use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

pub const BLOCK_MAGIC: &[u8; 4] = b"EVBK";
pub const INDEX_MAGIC: &[u8; 4] = b"EVIX";
pub const BLOCK_HEADER_LEN: usize = 36;
pub const INDEX_HEADER_LEN: usize = 8;
pub const INDEX_ENTRY_LEN: usize = 40;
pub const INDEX_VERSION: u16 = 1;

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
        });
        self.index_bytes += INDEX_HEADER_LEN as u64;
    }

    pub fn append_blocks(&mut self, metas: &[BlockMeta]) {
        for meta in metas {
            if self.segments.last().map(|segment| segment.id) != Some(meta.segment_id) {
                self.note_new_segment(meta.segment_id);
            }
            let segment = self.segments.last_mut().expect("segment just inserted");
            let add = BLOCK_HEADER_LEN as u64 + u64::from(meta.compressed_len);
            segment.data_len += add;
            segment.index_len += INDEX_ENTRY_LEN as u64;
            segment.blocks.push(meta.clone());
            self.rows += u64::from(meta.row_count);
            self.blocks += 1;
            self.data_bytes += add;
            self.index_bytes += INDEX_ENTRY_LEN as u64;
        }
    }
}

pub fn data_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:06}.dat"))
}

pub fn index_path(dir: &Path, id: u32) -> PathBuf {
    dir.join(format!("seg-{id:06}.idx"))
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
pub fn load_catalog(dir: &Path) -> Result<Catalog> {
    let mut catalog = Catalog::empty();
    for id in list_segment_ids(dir)? {
        let data = data_path(dir, id);
        let index = index_path(dir, id);
        let (blocks, data_len) = scan_and_repair(&data, id)?;
        let mut indexed = read_index(&index).unwrap_or_default();
        for block in &mut indexed {
            block.segment_id = id;
        }
        let index_len = INDEX_HEADER_LEN as u64 + blocks.len() as u64 * INDEX_ENTRY_LEN as u64;
        let index_on_disk = fs::metadata(&index).map(|meta| meta.len()).unwrap_or(0);
        if index_on_disk != index_len || !index_matches(&indexed, &blocks) {
            write_index(&index, &blocks)?;
        }
        catalog.data_bytes += data_len;
        catalog.index_bytes += index_len;
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
) -> Result<Vec<u8>> {
    if compressed.len() > u32::MAX as usize {
        return Err(Error::event("compressed block does not fit in u32"));
    }
    let mut out = Vec::with_capacity(BLOCK_HEADER_LEN + compressed.len());
    out.extend_from_slice(BLOCK_MAGIC);
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

pub fn scan_and_repair(path: &Path, segment_id: u32) -> Result<(Vec<BlockMeta>, u64)> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(0))?;
    let mut offset = 0u64;
    let mut blocks = Vec::new();
    loop {
        if offset >= file_len || file_len - offset < BLOCK_HEADER_LEN as u64 {
            break;
        }
        let mut header = [0u8; BLOCK_HEADER_LEN];
        file.seek(SeekFrom::Start(offset))?;
        if file.read_exact(&mut header).is_err() {
            break;
        }
        if &header[0..4] != BLOCK_MAGIC {
            break;
        }
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
        offset = payload_end;
    }
    if offset < file_len {
        file.set_len(offset)?;
        file.sync_all()?;
    }
    Ok((blocks, offset))
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

pub fn read_block_payload(path: &Path, meta: &BlockMeta) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(meta.offset))?;
    let total = BLOCK_HEADER_LEN + meta.compressed_len as usize;
    let mut buf = vec![0u8; total];
    file.read_exact(&mut buf)?;
    if &buf[0..4] != BLOCK_MAGIC {
        return Err(Error::corrupt("block magic mismatch while reading"));
    }
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
    zstd::bulk::decompress(compressed, uncompressed_len as usize)
        .map_err(|err| Error::corrupt(format!("zstd decompress failed: {err}")))
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
