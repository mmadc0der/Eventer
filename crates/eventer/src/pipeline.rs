use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender};

use crate::codec::encode_block;
use crate::error::{Error, Result};
use crate::schema::Schema;
use crate::segment::{frame_block, ActiveSegment, BlockMeta, Catalog, BLOCK_HEADER_LEN};
use crate::value::{parse_event, Row};

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
}

enum CompIn {
    Block(BlockIn),
    Flush { seq: u64, ack: Ack },
    Shutdown { seq: u64, ack: mpsc::Sender<()> },
}

struct BlockOut {
    seq: u64,
    framed: Vec<u8>,
    uncompressed_len: u32,
    compressed_len: u32,
    row_count: u32,
    min_ts: i64,
    max_ts: i64,
    acks: Vec<Option<Ack>>,
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
    let (cmd_tx, cmd_rx) = bounded::<Cmd>(4096);
    let (parse_tx, parse_rx) = bounded::<Job>(4096);
    let (enc_tx, enc_rx) = bounded::<EncoderMsg>(4096);
    let (comp_tx, comp_rx) = bounded::<CompIn>(64);
    let (writer_tx, writer_rx) = bounded::<CompOut>(64);

    let mut threads = Vec::new();
    let disk = Disk::new(
        config.dir,
        config.segment_bytes,
        Arc::clone(&config.catalog),
        Arc::clone(&poison),
    )?;
    threads.push(named("eventer-writer", {
        let linger = config.linger;
        move || writer_loop(writer_rx, disk, linger)
    })?);

    for index in 0..config.compress_threads {
        let rx = comp_rx.clone();
        let tx = writer_tx.clone();
        let level = config.zstd_level;
        threads.push(named(&format!("eventer-compress-{index}"), move || {
            compress_loop(rx, tx, level);
        })?);
    }
    drop(comp_rx);
    drop(writer_tx);

    let schema = Arc::clone(&config.schema);
    let block_rows = config.block_rows;
    let linger = config.linger;
    threads.push(named("eventer-encode", move || {
        encode_loop(enc_rx, comp_tx, schema, block_rows, linger);
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
) {
    let mut next = 1u64;
    let mut pending: BTreeMap<u64, (Result<Row>, Option<Ack>)> = BTreeMap::new();
    let mut rows: Vec<Row> = Vec::new();
    let mut acks: Vec<Option<Ack>> = Vec::new();
    let mut flushes: VecDeque<(u64, Ack)> = VecDeque::new();
    let mut shutdown: Option<(u64, mpsc::Sender<()>)> = None;
    let mut stage_seq = 1u64;
    let mut disconnected = false;

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
                emit_block(&mut rows, &mut acks, n, &mut stage_seq, &tx, &schema);
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
            );
        }

        while flushes
            .front()
            .map(|(target, _)| next == *target + 1)
            .unwrap_or(false)
        {
            if !rows.is_empty() {
                let n = rows.len();
                emit_block(&mut rows, &mut acks, n, &mut stage_seq, &tx, &schema);
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
            emit_block(&mut rows, &mut acks, n, &mut stage_seq, &tx, &schema);
        }

        if let Some((target, _)) = &shutdown {
            if flushes.is_empty() && pending.is_empty() && next == *target + 1 {
                if !rows.is_empty() {
                    let n = rows.len();
                    emit_block(&mut rows, &mut acks, n, &mut stage_seq, &tx, &schema);
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
) {
    let chunk: Vec<Row> = rows.drain(..n).collect();
    let chunk_acks: Vec<_> = acks.drain(..n).collect();
    match encode_block(schema, &chunk) {
        Ok(encoded) => {
            let msg = CompIn::Block(BlockIn {
                seq: *stage_seq,
                raw: encoded.bytes,
                min_ts: encoded.min_ts,
                max_ts: encoded.max_ts,
                row_count: encoded.row_count,
                acks: chunk_acks,
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

/// A sealed block is decompressed even when the query range only overlaps it.
/// Keep that expansion inside one query response (`store::MAX_QUERY_BYTES`).
const MAX_SEALED_BLOCK_UNCOMPRESSED: usize = 64 * 1024 * 1024;

fn compress_loop(rx: Receiver<CompIn>, tx: Sender<CompOut>, level: i32) {
    let mut compressor = zstd::bulk::Compressor::new(level).ok();
    while let Ok(msg) = rx.recv() {
        let out = match msg {
            CompIn::Flush { seq, ack } => CompOut::Flush { seq, ack },
            CompIn::Shutdown { seq, ack } => CompOut::Shutdown { seq, ack },
            CompIn::Block(block) if block.raw.len() > MAX_SEALED_BLOCK_UNCOMPRESSED => {
                CompOut::Skip {
                    seq: block.seq,
                    acks: block.acks,
                    error: Error::event("query response size limit exceeded"),
                }
            }
            CompIn::Block(block) => match compressor.as_mut() {
                None => CompOut::Skip {
                    seq: block.seq,
                    acks: block.acks,
                    error: Error::io("zstd compressor failed to initialize"),
                },
                Some(compressor) => match compressor.compress(&block.raw) {
                    Ok(compressed) => match frame_block(
                        &compressed,
                        block.raw.len() as u32,
                        block.row_count,
                        block.min_ts,
                        block.max_ts,
                    ) {
                        Ok(framed) => CompOut::Block(BlockOut {
                            seq: block.seq,
                            compressed_len: compressed.len() as u32,
                            uncompressed_len: block.raw.len() as u32,
                            framed,
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
                    Err(err) => CompOut::Skip {
                        seq: block.seq,
                        acks: block.acks,
                        error: Error::io(err),
                    },
                },
            },
        };
        if tx.send(out).is_err() {
            break;
        }
    }
}

struct Disk {
    dir: PathBuf,
    rotate_at: u64,
    active: Option<ActiveSegment>,
    next_id: u32,
    catalog: Arc<Mutex<Catalog>>,
    poison: Arc<Mutex<Option<Error>>>,
}

impl Disk {
    fn new(
        dir: PathBuf,
        rotate_at: u64,
        catalog: Arc<Mutex<Catalog>>,
        poison: Arc<Mutex<Option<Error>>>,
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
        Ok(Self {
            dir,
            rotate_at,
            active,
            next_id,
            catalog,
            poison,
        })
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
            for item in batch.iter() {
                self.prepare()?;
                let segment = self.active.as_mut().expect("segment prepared");
                if item.framed.len() != BLOCK_HEADER_LEN + item.compressed_len as usize {
                    return Err(Error::corrupt("framed block length does not match"));
                }
                let meta = BlockMeta {
                    segment_id: segment.id,
                    offset: segment.data_len,
                    compressed_len: item.compressed_len,
                    uncompressed_len: item.uncompressed_len,
                    row_count: item.row_count,
                    min_ts: item.min_ts,
                    max_ts: item.max_ts,
                };
                segment.write_framed(&item.framed, &meta)?;
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
