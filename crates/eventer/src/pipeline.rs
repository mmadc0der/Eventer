use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};

use crate::codec::encode_block;
use crate::error::{Error, Result};
use crate::schema::{FieldType, Schema};
use crate::segment::{
    self, frame_block, read_dictionary, ActiveSegment, BlockMeta, Catalog, BLOCK_HEADER_LEN,
    DICT_MAX_BYTES, DICT_SAMPLE_CHUNK, DICT_SAMPLE_MAX,
};
use crate::value::{parse_event, Row, Scalar};

const WRITE_BATCH_BLOCKS: usize = 8;

type Ack = mpsc::Sender<Result<()>>;

enum Cmd {
    Event { json: Vec<u8>, ack: Option<Ack> },
    Flush { ack: Ack },
    Shutdown { ack: mpsc::Sender<()> },
}

struct Job {
    seq: u64,
    json: Vec<u8>,
    ack: Option<Ack>,
}

enum EncoderMsg {
    Parsed {
        seq: u64,
        row: Result<Row>,
        ack: Option<Ack>,
    },
    Flush {
        target: u64,
        ack: Ack,
    },
    Shutdown {
        target: u64,
        ack: mpsc::Sender<()>,
    },
}

struct BlockIn {
    seq: u64,
    raw: Vec<u8>,
    min_ts: i64,
    max_ts: i64,
    row_count: u32,
    acks: Vec<Option<Ack>>,
    /// Compress with the dictionary published for `epoch`, when one exists.
    use_dict: bool,
    epoch: u32,
}

enum CompIn {
    Block(BlockIn),
    Flush { seq: u64, ack: Ack },
    Shutdown { seq: u64, ack: mpsc::Sender<()> },
}

struct BlockOut {
    seq: u64,
    framed: Vec<u8>,
    /// Uncompressed sealed block. The writer samples these and may recompress them.
    raw: Vec<u8>,
    /// Dictionary the frame was compressed with, when `framed` is an `EVBD` block.
    dict: Option<Arc<Vec<u8>>>,
    epoch: u32,
    uncompressed_len: u32,
    compressed_len: u32,
    row_count: u32,
    min_ts: i64,
    max_ts: i64,
    acks: Vec<Option<Ack>>,
}

struct DictPublish {
    epoch: AtomicU32,
    current: Mutex<Option<(u32, Arc<Vec<u8>>)>>,
}

impl DictPublish {
    fn new() -> Self {
        Self {
            epoch: AtomicU32::new(0),
            current: Mutex::new(None),
        }
    }

    fn epoch(&self) -> u32 {
        self.epoch.load(Ordering::Acquire)
    }

    fn lookup(&self, epoch: u32) -> Option<Arc<Vec<u8>>> {
        let guard = self.current.lock().unwrap_or_else(|err| err.into_inner());
        match guard.as_ref() {
            Some((stored, dict)) if *stored == epoch => Some(Arc::clone(dict)),
            _ => None,
        }
    }

    fn set(&self, epoch: u32, dict: Arc<Vec<u8>>) {
        let mut guard = self.current.lock().unwrap_or_else(|err| err.into_inner());
        if self.epoch.load(Ordering::Acquire) == epoch {
            *guard = Some((epoch, dict));
        }
    }

    fn bump_and_clear(&self) -> u32 {
        let mut guard = self.current.lock().unwrap_or_else(|err| err.into_inner());
        *guard = None;
        self.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }
}

struct DictSampler {
    epoch: u32,
    sample: Vec<u8>,
    sample_sizes: Vec<usize>,
    closed: bool,
}

impl DictSampler {
    fn new(epoch: u32) -> Self {
        Self {
            epoch,
            sample: Vec::new(),
            sample_sizes: Vec::new(),
            closed: false,
        }
    }

    fn reset(&mut self, epoch: u32) {
        *self = Self::new(epoch);
    }

    /// `true` when this block should be compressed with the segment dictionary.
    fn observe(&mut self, publish: &Arc<DictPublish>, raw: &[u8]) -> bool {
        let epoch = publish.epoch();
        if epoch != self.epoch {
            self.reset(epoch);
        }
        if publish.lookup(self.epoch).is_some() {
            self.closed = true;
            return true;
        }
        if self.closed || raw.is_empty() {
            return false;
        }
        let fits = self.sample.len().saturating_add(raw.len()) <= DICT_SAMPLE_MAX;
        if fits || self.sample.is_empty() {
            self.absorb(raw);
            if self.sample.len() >= DICT_SAMPLE_MAX || !fits {
                self.train(Arc::clone(publish));
            }
            false
        } else {
            self.train(Arc::clone(publish));
            publish.lookup(self.epoch).is_some()
        }
    }

    fn absorb(&mut self, raw: &[u8]) {
        if raw.is_empty() || self.sample.len() >= DICT_SAMPLE_MAX {
            return;
        }
        let room = DICT_SAMPLE_MAX - self.sample.len();
        let n = raw.len().min(room);
        let mut offset = 0;
        while offset < n {
            let take = (n - offset).min(DICT_SAMPLE_CHUNK);
            self.sample_sizes.push(take);
            offset += take;
        }
        self.sample.extend_from_slice(&raw[..n]);
    }

    fn train(&mut self, publish: Arc<DictPublish>) {
        if self.closed {
            return;
        }
        self.closed = true;
        let sample = std::mem::take(&mut self.sample);
        let sizes = std::mem::take(&mut self.sample_sizes);
        if sample.is_empty() || sizes.is_empty() {
            return;
        }
        let epoch = self.epoch;
        // Training is off the append path. Later blocks pick up the dictionary
        // once `publish` shows it; blocks already sealed stay plain.
        let _ = thread::Builder::new()
            .name("eventer-dict".into())
            .spawn(move || {
                let Ok(bytes) = zstd::dict::from_continuous(&sample, &sizes, DICT_MAX_BYTES) else {
                    return;
                };
                if bytes.is_empty() || bytes.len() > DICT_MAX_BYTES {
                    return;
                }
                publish.set(epoch, Arc::new(bytes));
            });
    }
}

enum CompOut {
    Block(BlockOut),
    Flush {
        seq: u64,
        ack: Ack,
    },
    Shutdown {
        seq: u64,
        ack: mpsc::Sender<()>,
    },
    Skip {
        seq: u64,
        acks: Vec<Option<Ack>>,
        error: Error,
    },
}

fn comp_out_seq(msg: &CompOut) -> u64 {
    match msg {
        CompOut::Block(block) => block.seq,
        CompOut::Flush { seq, .. } | CompOut::Shutdown { seq, .. } | CompOut::Skip { seq, .. } => {
            *seq
        }
    }
}

pub struct Pipeline {
    tx: Mutex<Option<Sender<Cmd>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    pub(crate) catalog: Arc<Mutex<Catalog>>,
    poison: Arc<Mutex<Option<Error>>>,
}

pub struct PipelineConfig {
    pub dir: PathBuf,
    pub schema: Arc<Schema>,
    pub catalog: Arc<Mutex<Catalog>>,
    pub block_rows: usize,
    pub zstd_level: i32,
    pub segment_bytes: u64,
    pub parser_threads: usize,
    pub compress_threads: usize,
    pub linger: Duration,
}

pub fn spawn(config: PipelineConfig) -> Result<Pipeline> {
    let poison = Arc::new(Mutex::new(None));
    let dict_publish = Arc::new(DictPublish::new());
    let (cmd_tx, cmd_rx) = bounded::<Cmd>(4096);
    let (parse_tx, parse_rx) = bounded::<Job>(4096);
    let (enc_tx, enc_rx) = bounded::<EncoderMsg>(4096);
    let (comp_tx, comp_rx) = bounded::<CompIn>(64);
    let (writer_tx, writer_rx) = bounded::<CompOut>(64);

    let mut threads = Vec::new();
    let disk = Disk::new(
        config.dir,
        config.segment_bytes,
        config.zstd_level,
        Arc::clone(&config.catalog),
        Arc::clone(&poison),
        Arc::clone(&dict_publish),
    )?;
    threads.push(named("eventer-writer", {
        let linger = config.linger;
        move || writer_loop(writer_rx, disk, linger)
    })?);

    for index in 0..config.compress_threads {
        let rx = comp_rx.clone();
        let tx = writer_tx.clone();
        let level = config.zstd_level;
        let dict_publish = Arc::clone(&dict_publish);
        threads.push(named(&format!("eventer-compress-{index}"), move || {
            compress_loop(rx, tx, level, dict_publish);
        })?);
    }
    drop(comp_rx);
    drop(writer_tx);

    let schema = Arc::clone(&config.schema);
    let block_rows = config.block_rows;
    let linger = config.linger;
    let dict_publish = Arc::clone(&dict_publish);
    threads.push(named("eventer-encode", move || {
        encode_loop(enc_rx, comp_tx, schema, block_rows, linger, dict_publish);
    })?);

    for index in 0..config.parser_threads {
        let rx = parse_rx.clone();
        let tx = enc_tx.clone();
        let schema = Arc::clone(&config.schema);
        threads.push(named(&format!("eventer-parse-{index}"), move || {
            parse_loop(rx, tx, schema);
        })?);
    }
    drop(parse_rx);
    threads.push(named("eventer-dispatch", move || {
        dispatch_loop(cmd_rx, parse_tx, enc_tx);
    })?);

    Ok(Pipeline {
        tx: Mutex::new(Some(cmd_tx)),
        threads: Mutex::new(threads),
        catalog: config.catalog,
        poison,
    })
}

fn named(name: &str, f: impl FnOnce() + Send + 'static) -> Result<JoinHandle<()>> {
    thread::Builder::new()
        .name(name.into())
        .spawn(f)
        .map_err(Error::from)
}

impl Pipeline {
    pub fn append(&self, json: &[u8], durable: bool) -> Result<()> {
        let tx = self.sender()?;
        if durable {
            let (ack_tx, ack_rx) = mpsc::channel();
            tx.send(Cmd::Event {
                json: json.to_vec(),
                ack: Some(ack_tx),
            })
            .map_err(|_| Error::Closed)?;
            ack_rx.recv().map_err(|_| Error::Closed)??;
            Ok(())
        } else {
            tx.send(Cmd::Event {
                json: json.to_vec(),
                ack: None,
            })
            .map_err(|_| Error::Closed)?;
            Ok(())
        }
    }

    pub fn flush(&self) -> Result<()> {
        self.fail_if_poisoned()?;
        let tx = self.sender()?;
        let (ack_tx, ack_rx) = mpsc::channel();
        tx.send(Cmd::Flush { ack: ack_tx })
            .map_err(|_| Error::Closed)?;
        ack_rx.recv().map_err(|_| Error::Closed)??;
        self.fail_if_poisoned()
    }

    pub fn shutdown(&self) -> Result<()> {
        let tx = self.tx.lock().unwrap_or_else(|err| err.into_inner()).take();
        let Some(tx) = tx else {
            return Ok(());
        };
        let (ack_tx, ack_rx) = mpsc::channel();
        if tx.send(Cmd::Shutdown { ack: ack_tx }).is_err() {
            return Err(Error::Closed);
        }
        let _ = ack_rx.recv();
        let threads =
            std::mem::take(&mut *self.threads.lock().unwrap_or_else(|err| err.into_inner()));
        for thread in threads {
            let _ = thread.join();
        }
        Ok(())
    }

    fn sender(&self) -> Result<Sender<Cmd>> {
        self.tx
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .as_ref()
            .cloned()
            .ok_or(Error::Closed)
    }

    fn fail_if_poisoned(&self) -> Result<()> {
        match self
            .poison
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

fn dispatch_loop(rx: Receiver<Cmd>, parse_tx: Sender<Job>, enc_tx: Sender<EncoderMsg>) {
    let mut seq = 0u64;
    while let Ok(cmd) = rx.recv() {
        match cmd {
            Cmd::Event { json, ack } => {
                seq += 1;
                if parse_tx.send(Job { seq, json, ack }).is_err() {
                    break;
                }
            }
            Cmd::Flush { ack } => {
                if enc_tx.send(EncoderMsg::Flush { target: seq, ack }).is_err() {
                    break;
                }
            }
            Cmd::Shutdown { ack } => {
                let _ = enc_tx.send(EncoderMsg::Shutdown { target: seq, ack });
                break;
            }
        }
    }
}

fn parse_loop(rx: Receiver<Job>, tx: Sender<EncoderMsg>, schema: Arc<Schema>) {
    while let Ok(job) = rx.recv() {
        let row = parse_event(&schema, &job.json);
        if tx
            .send(EncoderMsg::Parsed {
                seq: job.seq,
                row,
                ack: job.ack,
            })
            .is_err()
        {
            break;
        }
    }
}

fn encode_loop(
    rx: Receiver<EncoderMsg>,
    tx: Sender<CompIn>,
    schema: Arc<Schema>,
    block_rows: usize,
    linger: Duration,
    publish: Arc<DictPublish>,
) {
    let mut next = 1u64;
    let mut pending: BTreeMap<u64, (Result<Row>, Option<Ack>)> = BTreeMap::new();
    let mut rows: Vec<Row> = Vec::new();
    let mut acks: Vec<Option<Ack>> = Vec::new();
    let mut flushes: VecDeque<(u64, Ack)> = VecDeque::new();
    let mut shutdown: Option<(u64, mpsc::Sender<()>)> = None;
    let mut stage_seq = 1u64;
    let mut disconnected = false;
    let mut sampler = DictSampler::new(publish.epoch());

    loop {
        let mut idle = false;
        match rx.recv_timeout(linger) {
            Ok(msg) => accept_encoder_msg(msg, &mut pending, &mut flushes, &mut shutdown),
            Err(RecvTimeoutError::Timeout) => idle = true,
            Err(RecvTimeoutError::Disconnected) => disconnected = true,
        }
        while let Ok(msg) = rx.try_recv() {
            idle = false;
            accept_encoder_msg(msg, &mut pending, &mut flushes, &mut shutdown);
        }
        drain_parsed(&mut pending, &mut next, &mut rows, &mut acks);
        if disconnected && !pending.contains_key(&next) {
            if !rows.is_empty() {
                let n = rows.len();
                emit_block(
                    &mut rows,
                    &mut acks,
                    n,
                    &mut stage_seq,
                    &tx,
                    &schema,
                    &mut sampler,
                    &publish,
                );
            }
            while let Some((_, ack)) = flushes.pop_front() {
                let _ = ack.send(Err(Error::Closed));
            }
            if let Some((_, ack)) = shutdown.take() {
                let _ = tx.send(CompIn::Shutdown {
                    seq: stage_seq,
                    ack,
                });
            }
            break;
        }

        while rows.len() >= block_rows {
            emit_block(
                &mut rows,
                &mut acks,
                block_rows,
                &mut stage_seq,
                &tx,
                &schema,
                &mut sampler,
                &publish,
            );
        }

        while flushes
            .front()
            .map(|(target, _)| next == *target + 1)
            .unwrap_or(false)
        {
            if !rows.is_empty() {
                let n = rows.len();
                emit_block(
                    &mut rows,
                    &mut acks,
                    n,
                    &mut stage_seq,
                    &tx,
                    &schema,
                    &mut sampler,
                    &publish,
                );
            }
            let (_, ack) = flushes.pop_front().unwrap();
            if tx
                .send(CompIn::Flush {
                    seq: stage_seq,
                    ack,
                })
                .is_err()
            {
                return;
            }
            stage_seq += 1;
        }

        if idle
            && !rows.is_empty()
            && pending.is_empty()
            && flushes.is_empty()
            && shutdown.is_none()
        {
            let n = rows.len();
            emit_block(
                &mut rows,
                &mut acks,
                n,
                &mut stage_seq,
                &tx,
                &schema,
                &mut sampler,
                &publish,
            );
        }

        if let Some((target, _)) = &shutdown {
            if flushes.is_empty() && pending.is_empty() && next == *target + 1 {
                if !rows.is_empty() {
                    let n = rows.len();
                    emit_block(
                        &mut rows,
                        &mut acks,
                        n,
                        &mut stage_seq,
                        &tx,
                        &schema,
                        &mut sampler,
                        &publish,
                    );
                }
                let (_, ack) = shutdown.take().unwrap();
                let _ = tx.send(CompIn::Shutdown {
                    seq: stage_seq,
                    ack,
                });
                break;
            }
        }
    }
}

fn accept_encoder_msg(
    msg: EncoderMsg,
    pending: &mut BTreeMap<u64, (Result<Row>, Option<Ack>)>,
    flushes: &mut VecDeque<(u64, Ack)>,
    shutdown: &mut Option<(u64, mpsc::Sender<()>)>,
) {
    match msg {
        EncoderMsg::Parsed { seq, row, ack } => {
            pending.insert(seq, (row, ack));
        }
        EncoderMsg::Flush { target, ack } => flushes.push_back((target, ack)),
        EncoderMsg::Shutdown { target, ack } => {
            if shutdown.is_none() {
                *shutdown = Some((target, ack));
            }
        }
    }
}

fn drain_parsed(
    pending: &mut BTreeMap<u64, (Result<Row>, Option<Ack>)>,
    next: &mut u64,
    rows: &mut Vec<Row>,
    acks: &mut Vec<Option<Ack>>,
) {
    while let Some((row, ack)) = pending.remove(next) {
        match row {
            Ok(row) => {
                rows.push(row);
                acks.push(ack);
            }
            Err(err) => {
                if let Some(ack) = ack {
                    let _ = ack.send(Err(err));
                }
            }
        }
        *next += 1;
    }
}

fn emit_block(
    rows: &mut Vec<Row>,
    acks: &mut Vec<Option<Ack>>,
    n: usize,
    stage_seq: &mut u64,
    tx: &Sender<CompIn>,
    schema: &Schema,
    sampler: &mut DictSampler,
    publish: &Arc<DictPublish>,
) {
    let mut left = n.min(rows.len());
    while left > 0 {
        let take = sealed_prefix_len(schema, &rows[..left]);
        let chunk: Vec<Row> = rows.drain(..take).collect();
        let chunk_acks: Vec<_> = acks.drain(..take).collect();
        left -= take;
        match encode_block(schema, &chunk) {
            Ok(encoded) => {
                let use_dict = sampler.observe(publish, &encoded.bytes);
                let msg = CompIn::Block(BlockIn {
                    seq: *stage_seq,
                    raw: encoded.bytes,
                    min_ts: encoded.min_ts,
                    max_ts: encoded.max_ts,
                    row_count: encoded.row_count,
                    acks: chunk_acks,
                    use_dict,
                    epoch: sampler.epoch,
                });
                if let Err(err) = tx.send(msg) {
                    if let CompIn::Block(block) = err.into_inner() {
                        for ack in block.acks.into_iter().flatten() {
                            let _ = ack.send(Err(Error::Closed));
                        }
                    }
                    return;
                }
                *stage_seq += 1;
            }
            Err(err) => {
                for ack in chunk_acks.into_iter().flatten() {
                    let _ = ack.send(Err(err.clone()));
                }
            }
        }
    }
}

/// A sealed block is decompressed even when the query range only overlaps it.
/// Cut the batch before `encode_block` so each sealed block stays inside one
/// query response (`store::MAX_QUERY_BYTES`) and every accepted row is written.
const MAX_SEALED_BLOCK_UNCOMPRESSED: usize = 64 * 1024 * 1024;

fn sealed_prefix_len(schema: &Schema, rows: &[Row]) -> usize {
    if rows.is_empty() {
        return 0;
    }
    let mut used = 4usize;
    for _ in &schema.fields {
        used = used.saturating_add(16);
    }
    let mut count = 0usize;
    for row in rows {
        let mut add = 16usize;
        for (index, field) in schema.fields.iter().enumerate() {
            let scalar = row.values.get(index).unwrap_or(&Scalar::Null);
            add = add.saturating_add(scalar_encoded_upper_bound(field.ty, scalar));
        }
        if count > 0 && used.saturating_add(add) > MAX_SEALED_BLOCK_UNCOMPRESSED {
            break;
        }
        used = used.saturating_add(add);
        count += 1;
    }
    count.max(1)
}

fn scalar_encoded_upper_bound(ty: FieldType, scalar: &Scalar) -> usize {
    match (ty, scalar) {
        (_, Scalar::Null) => 1,
        (FieldType::Bool, _) => 1,
        (FieldType::Decimal { .. }, _) => 24,
        (FieldType::String | FieldType::Text, Scalar::Str(text)) => 10 + text.len(),
        (FieldType::Json, Scalar::Json(text)) => 10 + text.len(),
        (FieldType::String | FieldType::Text | FieldType::Json, _) => 10,
        _ => 16,
    }
}

fn compress_loop(rx: Receiver<CompIn>, tx: Sender<CompOut>, level: i32, publish: Arc<DictPublish>) {
    let mut plain = zstd::bulk::Compressor::new(level).ok();
    let mut with_dict: Option<(usize, zstd::bulk::Compressor<'static>)> = None;
    while let Ok(msg) = rx.recv() {
        let out = match msg {
            CompIn::Flush { seq, ack } => CompOut::Flush { seq, ack },
            CompIn::Shutdown { seq, ack } => CompOut::Shutdown { seq, ack },
            CompIn::Block(block) => {
                compress_block(&mut plain, &mut with_dict, &publish, level, block)
            }
        };
        if tx.send(out).is_err() {
            break;
        }
    }
}

fn compress_block(
    plain: &mut Option<zstd::bulk::Compressor<'static>>,
    with_dict: &mut Option<(usize, zstd::bulk::Compressor<'static>)>,
    publish: &DictPublish,
    level: i32,
    block: BlockIn,
) -> CompOut {
    let dict = if block.use_dict {
        publish.lookup(block.epoch)
    } else {
        None
    };
    let compressed = if let Some(dict) = dict.as_ref() {
        let key = Arc::as_ptr(dict) as usize;
        if with_dict.as_ref().map(|(existing, _)| *existing) != Some(key) {
            match zstd::bulk::Compressor::with_dictionary(level, dict) {
                Ok(compressor) => *with_dict = Some((key, compressor)),
                Err(err) => {
                    return CompOut::Skip {
                        seq: block.seq,
                        acks: block.acks,
                        error: Error::io(err),
                    };
                }
            }
        }
        match with_dict.as_mut().unwrap().1.compress(&block.raw) {
            Ok(compressed) => Ok((compressed, true)),
            Err(err) => Err(Error::io(err)),
        }
    } else {
        match plain.as_mut() {
            None => Err(Error::io("zstd compressor failed to initialize")),
            Some(compressor) => match compressor.compress(&block.raw) {
                Ok(compressed) => Ok((compressed, false)),
                Err(err) => Err(Error::io(err)),
            },
        }
    };
    match compressed {
        Err(error) => CompOut::Skip {
            seq: block.seq,
            acks: block.acks,
            error,
        },
        Ok((compressed, used_dict)) => match frame_block(
            &compressed,
            block.raw.len() as u32,
            block.row_count,
            block.min_ts,
            block.max_ts,
            used_dict,
        ) {
            Ok(framed) => CompOut::Block(BlockOut {
                seq: block.seq,
                compressed_len: compressed.len() as u32,
                uncompressed_len: block.raw.len() as u32,
                framed,
                raw: block.raw,
                dict: if used_dict { dict } else { None },
                epoch: block.epoch,
                row_count: block.row_count,
                min_ts: block.min_ts,
                max_ts: block.max_ts,
                acks: block.acks,
            }),
            Err(err) => CompOut::Skip {
                seq: block.seq,
                acks: block.acks,
                error: err,
            },
        },
    }
}

struct Disk {
    dir: PathBuf,
    rotate_at: u64,
    level: i32,
    active: Option<ActiveSegment>,
    next_id: u32,
    catalog: Arc<Mutex<Catalog>>,
    poison: Arc<Mutex<Option<Error>>>,
    publish: Arc<DictPublish>,
    plain: Option<zstd::bulk::Compressor<'static>>,
    dict_compressor: Option<(usize, zstd::bulk::Compressor<'static>)>,
    dict: Option<Arc<Vec<u8>>>,
    segment_epoch: u32,
}

impl Disk {
    fn new(
        dir: PathBuf,
        rotate_at: u64,
        level: i32,
        catalog: Arc<Mutex<Catalog>>,
        poison: Arc<Mutex<Option<Error>>>,
        publish: Arc<DictPublish>,
    ) -> Result<Self> {
        let last = catalog
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .segments
            .last()
            .cloned();
        let (active, next_id) = match last {
            Some(state) if state.data_len < rotate_at => {
                let next_id = state.id.saturating_add(1);
                (Some(ActiveSegment::open_existing(&dir, &state)?), next_id)
            }
            Some(state) => (None, state.id.saturating_add(1)),
            None => (None, 1),
        };
        let mut disk = Self {
            dir,
            rotate_at,
            level,
            active,
            next_id,
            catalog,
            poison,
            publish,
            plain: zstd::bulk::Compressor::new(level).ok(),
            dict_compressor: None,
            dict: None,
            segment_epoch: 0,
        };
        if let Some(segment) = disk.active.as_ref() {
            disk.load_existing_dictionary(segment.id)?;
        }
        Ok(disk)
    }

    fn load_existing_dictionary(&mut self, id: u32) -> Result<()> {
        match read_dictionary(&segment::dictionary_path(&self.dir, id))? {
            Some(stored) => {
                let dict = Arc::new(stored.bytes);
                self.dict = Some(Arc::clone(&dict));
                self.publish.set(self.segment_epoch, dict);
            }
            None => {
                self.dict = None;
            }
        }
        Ok(())
    }

    fn poison(&self, err: Error) {
        let mut slot = self.poison.lock().unwrap_or_else(|err| err.into_inner());
        if slot.is_none() {
            *slot = Some(err);
        }
    }

    fn health(&self) -> Result<()> {
        match self
            .poison
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
        {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn prepare(&mut self) -> Result<()> {
        let rotate = self
            .active
            .as_ref()
            .map(|segment| segment.data_len >= self.rotate_at)
            .unwrap_or(false);
        if rotate {
            if let Some(mut segment) = self.active.take() {
                segment.flush_os(true)?;
            }
        }
        if self.active.is_none() {
            let id = self.next_id;
            self.next_id = self.next_id.saturating_add(1);
            self.active = Some(ActiveSegment::create_new(&self.dir, id)?);
            self.catalog
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .note_new_segment(id);
            if rotate {
                self.dict = None;
                self.dict_compressor = None;
                self.segment_epoch = self.publish.bump_and_clear();
            }
        }
        Ok(())
    }

    fn commit(&mut self, batch: &mut Vec<BlockOut>, sync: bool) -> Result<()> {
        if batch.is_empty() {
            if sync {
                self.sync_only()?;
            }
            return Ok(());
        }
        let mut metas = Vec::with_capacity(batch.len());
        let mut acks = Vec::new();
        let result = (|| {
            for item in batch.iter_mut() {
                self.prepare()?;
                let (framed, compressed_len) = self.frame_for(item)?;
                let segment = self.active.as_mut().expect("segment prepared");
                if framed.len() != BLOCK_HEADER_LEN + compressed_len as usize {
                    return Err(Error::corrupt("framed block length does not match"));
                }
                let meta = BlockMeta {
                    segment_id: segment.id,
                    offset: segment.data_len,
                    compressed_len,
                    uncompressed_len: item.uncompressed_len,
                    row_count: item.row_count,
                    min_ts: item.min_ts,
                    max_ts: item.max_ts,
                };
                segment.write_framed(&framed, &meta)?;
                metas.push(meta);
            }
            self.active
                .as_mut()
                .expect("segment prepared")
                .flush_os(sync)?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.catalog
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .append_blocks(&metas);
                for item in batch.drain(..) {
                    acks.extend(item.acks);
                }
                for ack in acks.into_iter().flatten() {
                    let _ = ack.send(Ok(()));
                }
                Ok(())
            }
            Err(err) => {
                self.poison(err.clone());
                for item in batch.drain(..) {
                    for ack in item.acks.into_iter().flatten() {
                        let _ = ack.send(Err(err.clone()));
                    }
                }
                Err(err)
            }
        }
    }

    /// Plain frames stay plain. A dictionary frame is stored only when it was
    /// trained for this segment's epoch. The sidecar is fsynced before that frame.
    fn frame_for(&mut self, item: &mut BlockOut) -> Result<(Vec<u8>, u32)> {
        if item.raw.len() != item.uncompressed_len as usize {
            return Err(Error::corrupt(
                "uncompressed block length does not match its frame",
            ));
        }
        if item.epoch == self.segment_epoch {
            if let Some(dict) = item.dict.clone() {
                if self.dict.is_none() {
                    self.install_dictionary(dict)?;
                }
            }
        }
        if self.dict.is_some() {
            self.frame_with_active_dict(item)
        } else {
            self.plain_frame(item)
        }
    }

    fn install_dictionary(&mut self, dict: Arc<Vec<u8>>) -> Result<()> {
        let id = self.active.as_ref().expect("segment prepared").id;
        let file_len = segment::write_dictionary(&self.dir, id, &dict)?;
        self.catalog
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .add_dictionary_bytes(id, file_len);
        self.dict = Some(dict);
        Ok(())
    }

    fn frame_with_active_dict(&mut self, item: &mut BlockOut) -> Result<(Vec<u8>, u32)> {
        let Some(dict) = self.dict.clone() else {
            return self.plain_frame(item);
        };
        if item
            .dict
            .as_ref()
            .is_some_and(|existing| Arc::ptr_eq(existing, &dict))
        {
            let framed = std::mem::take(&mut item.framed);
            return Ok((framed, item.compressed_len));
        }
        self.compress_frame(item, Some(dict))
    }

    fn plain_frame(&mut self, item: &mut BlockOut) -> Result<(Vec<u8>, u32)> {
        if item.dict.is_none() {
            let framed = std::mem::take(&mut item.framed);
            if framed.len() != BLOCK_HEADER_LEN + item.compressed_len as usize {
                return Err(Error::corrupt("framed block length does not match"));
            }
            return Ok((framed, item.compressed_len));
        }
        self.compress_frame(item, None)
    }

    fn compress_frame(
        &mut self,
        item: &BlockOut,
        dict: Option<Arc<Vec<u8>>>,
    ) -> Result<(Vec<u8>, u32)> {
        let compressed = if let Some(dict) = dict.as_ref() {
            let key = Arc::as_ptr(dict) as usize;
            if self.dict_compressor.as_ref().map(|(existing, _)| *existing) != Some(key) {
                let compressor =
                    zstd::bulk::Compressor::with_dictionary(self.level, dict).map_err(Error::io)?;
                self.dict_compressor = Some((key, compressor));
            }
            self.dict_compressor
                .as_mut()
                .expect("dictionary compressor installed")
                .1
                .compress(&item.raw)
                .map_err(Error::io)?
        } else {
            let compressor = self
                .plain
                .as_mut()
                .ok_or_else(|| Error::io("zstd compressor failed to initialize"))?;
            compressor.compress(&item.raw).map_err(Error::io)?
        };
        if compressed.len() > u32::MAX as usize {
            return Err(Error::event("compressed block does not fit in u32"));
        }
        let framed = frame_block(
            &compressed,
            item.uncompressed_len,
            item.row_count,
            item.min_ts,
            item.max_ts,
            dict.is_some(),
        )?;
        Ok((framed, compressed.len() as u32))
    }

    fn sync_only(&mut self) -> Result<()> {
        if let Some(segment) = self.active.as_mut() {
            segment.flush_os(true)?;
        }
        Ok(())
    }
}

fn writer_loop(rx: Receiver<CompOut>, mut disk: Disk, linger: Duration) {
    let mut next = 1u64;
    let mut pending: BTreeMap<u64, CompOut> = BTreeMap::new();
    let mut batch: Vec<BlockOut> = Vec::new();
    let mut disconnected = false;
    loop {
        let received = rx.recv_timeout(linger);
        let mut idle = false;
        match received {
            Ok(msg) => {
                pending.insert(comp_out_seq(&msg), msg);
            }
            Err(RecvTimeoutError::Timeout) => idle = true,
            Err(RecvTimeoutError::Disconnected) => disconnected = true,
        }
        while let Ok(msg) = rx.try_recv() {
            idle = false;
            pending.insert(comp_out_seq(&msg), msg);
        }
        if disconnected && !pending.contains_key(&next) {
            let _ = disk.commit(&mut batch, true);
            for msg in pending.into_values() {
                match msg {
                    CompOut::Flush { ack, .. } => {
                        let _ = ack.send(Err(Error::Closed));
                    }
                    CompOut::Shutdown { ack, .. } => {
                        let _ = ack.send(());
                    }
                    CompOut::Skip { acks, error, .. } => {
                        for ack in acks.into_iter().flatten() {
                            let _ = ack.send(Err(error.clone()));
                        }
                    }
                    CompOut::Block(block) => {
                        for ack in block.acks.into_iter().flatten() {
                            let _ = ack.send(Err(Error::Closed));
                        }
                    }
                }
            }
            break;
        }

        loop {
            let mut control = None;
            while control.is_none() {
                match pending.remove(&next) {
                    Some(CompOut::Block(block)) => {
                        batch.push(block);
                        next += 1;
                    }
                    Some(other) => control = Some(other),
                    None => break,
                }
            }
            let durable = batch
                .iter()
                .any(|block| block.acks.iter().any(|ack| ack.is_some()));
            let force = control.is_some();
            if !batch.is_empty() && (force || durable || batch.len() >= WRITE_BATCH_BLOCKS || idle)
            {
                let _ = disk.commit(&mut batch, force || durable);
            }
            match control {
                Some(CompOut::Flush { ack, .. }) => {
                    let health = disk.sync_only().and_then(|_| disk.health());
                    let _ = ack.send(health);
                    next += 1;
                }
                Some(CompOut::Shutdown { ack, .. }) => {
                    let _ = disk.sync_only();
                    let _ = ack.send(());
                    return;
                }
                Some(CompOut::Skip { acks, error, .. }) => {
                    disk.poison(error.clone());
                    for ack in acks.into_iter().flatten() {
                        let _ = ack.send(Err(error.clone()));
                    }
                    next += 1;
                }
                Some(CompOut::Block(_)) | None => break,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;

    #[test]
    fn sealed_prefix_splits_before_encode_and_keeps_every_row() {
        let schema = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"props","type":"json"}]}"#,
        )
        .unwrap();
        let body = "x".repeat(890 * 1024);
        let rows: Vec<Row> = (1..=100)
            .map(|ts| {
                let json = format!(r#"{{"n":{ts},"body":"{body}"}}"#);
                Row {
                    ts,
                    values: vec![Scalar::Timestamp(ts), Scalar::Json(json)],
                }
            })
            .collect();
        let mut offset = 0;
        let mut blocks = 0usize;
        while offset < rows.len() {
            let take = sealed_prefix_len(&schema, &rows[offset..]);
            assert!(take >= 1);
            let encoded = encode_block(&schema, &rows[offset..offset + take]).unwrap();
            assert!(encoded.bytes.len() <= MAX_SEALED_BLOCK_UNCOMPRESSED);
            offset += take;
            blocks += 1;
        }
        assert!(blocks > 1);
        assert_eq!(offset, rows.len());
        assert_eq!(sealed_prefix_len(&schema, &rows[..1]), 1);
    }
}
