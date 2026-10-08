use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::json_scan::validate_json_structure;
use crate::schema::{FieldType, Schema};
use crate::value::{Row, Scalar};

/// Uncompressed columnar block. `min_ts` / `max_ts` cover every row.
pub struct EncodedBlock {
    pub bytes: Vec<u8>,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: u32,
}

pub fn encode_block(schema: &Schema, rows: &[Row]) -> Result<EncodedBlock> {
    if rows.is_empty() {
        return Err(Error::event("refusing to encode an empty block"));
    }
    if rows.len() > 1_000_000 {
        return Err(Error::event("block exceeds 1_000_000 rows"));
    }
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for row in rows {
        min_ts = min_ts.min(row.ts);
        max_ts = max_ts.max(row.ts);
        if row.values.len() != schema.fields.len() {
            return Err(Error::event("row width does not match schema"));
        }
    }
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
    for (index, field) in schema.fields.iter().enumerate() {
        encode_column(&mut bytes, field.ty, rows, index)?;
    }
    Ok(EncodedBlock {
        bytes,
        min_ts,
        max_ts,
        row_count: rows.len() as u32,
    })
}

pub fn block_row_count(bytes: &[u8]) -> Result<usize> {
    if bytes.len() < 4 {
        return Err(Error::corrupt("block is shorter than its row count"));
    }
    let nrows = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if nrows == 0 || nrows > 1_000_000 {
        return Err(Error::corrupt(format!(
            "block row count {nrows} is invalid"
        )));
    }
    Ok(nrows)
}

pub fn decode_block(schema: &Schema, bytes: &[u8]) -> Result<Vec<Row>> {
    decode_rows(schema, bytes, None, usize::MAX)
}

/// Decode rows whose timestamp is inside `[from_ms, to_ms]`.
///
/// `max_string_bytes` is the remaining query budget. String and JSON columns
/// are charged for the bytes that would be replicated into matching rows, and
/// the decode stops before that replication when the budget cannot hold them.
pub fn decode_rows_in_range(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    max_string_bytes: usize,
) -> Result<Vec<Row>> {
    decode_rows(schema, bytes, Some((from_ms, to_ms)), max_string_bytes)
}

fn decode_rows(
    schema: &Schema,
    bytes: &[u8],
    range: Option<(i64, i64)>,
    max_string_bytes: usize,
) -> Result<Vec<Row>> {
    let nrows = block_row_count(bytes)?;
    let keep = match range {
        None => vec![true; nrows],
        Some((from_ms, to_ms)) => {
            let timestamps = read_timestamp_column(schema, bytes)?;
            timestamps
                .into_iter()
                .map(|ts| ts >= from_ms && ts <= to_ms)
                .collect()
        }
    };
    let mut budget = max_string_bytes;
    let mut cursor = 4;
    let mut columns = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        columns.push(decode_column(
            field.ty,
            bytes,
            &mut cursor,
            nrows,
            &keep,
            &mut budget,
        )?);
    }
    if cursor != bytes.len() {
        return Err(Error::corrupt("block has trailing bytes"));
    }
    let mut rows = Vec::new();
    for row_idx in 0..nrows {
        if !keep[row_idx] {
            continue;
        }
        let mut values = Vec::with_capacity(columns.len());
        for column in &mut columns {
            values.push(std::mem::replace(&mut column[row_idx], Scalar::Null));
        }
        let ts = match &values[schema.timestamp_index] {
            Scalar::Timestamp(ts) => *ts,
            _ => return Err(Error::corrupt("timestamp column is null or the wrong type")),
        };
        rows.push(Row { values, ts });
    }
    Ok(rows)
}

fn encode_column(out: &mut Vec<u8>, ty: FieldType, rows: &[Row], index: usize) -> Result<()> {
    let mut nulls = Vec::with_capacity(rows.len());
    for row in rows {
        nulls.push(matches!(row.values[index], Scalar::Null));
    }
    write_nulls(out, &nulls);
    match ty {
        FieldType::Int => {
            let values = ints(rows, index)?;
            out.extend_from_slice(&encode_i64s(&values));
        }
        FieldType::Timestamp => {
            let values = timestamps(rows, index)?;
            out.extend_from_slice(&encode_i64s(&values));
        }
        FieldType::Float => {
            let values = floats(rows, index)?;
            out.extend_from_slice(&encode_f64s(&values));
        }
        FieldType::Bool => {
            let values = bools(rows, index)?;
            out.extend_from_slice(&encode_bools(&values));
        }
        FieldType::Decimal { .. } => {
            let values = decimals(rows, index)?;
            out.extend_from_slice(&encode_i128s(&values));
        }
        FieldType::String => {
            let values = strings(rows, index)?;
            out.extend_from_slice(&encode_strings(&values, true));
        }
        FieldType::Text => {
            let values = strings(rows, index)?;
            out.extend_from_slice(&encode_strings(&values, false));
        }
        FieldType::Json => {
            let values = json_texts(rows, index)?;
            out.extend_from_slice(&encode_strings(&values, true));
        }
    }
    Ok(())
}

fn write_nulls(out: &mut Vec<u8>, nulls: &[bool]) {
    if nulls.iter().all(|is_null| !is_null) {
        out.push(0);
        return;
    }
    out.push(1);
    let mut bitmap = vec![0u8; nulls.len().div_ceil(8)];
    for (index, is_null) in nulls.iter().enumerate() {
        if !is_null {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    out.extend_from_slice(&bitmap);
}

/// Integer kind bytes. Kinds 0–6 are the original empty, constant, and
/// byte-width frame-of-reference encodings. Kinds 7 and 8 are additive.
const KIND_EMPTY: u8 = 0;
const KIND_CONSTANT: u8 = 1;
const KIND_STRIDE: u8 = 7;
const KIND_BITPACK: u8 = 8;

fn encode_i64s(values: &[Option<i64>]) -> Vec<u8> {
    let present: Vec<i64> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![KIND_EMPTY];
    }
    let min = present.iter().copied().min().unwrap();
    let max = present.iter().copied().max().unwrap();
    if min == max {
        let mut out = vec![KIND_CONSTANT];
        out.extend_from_slice(&min.to_le_bytes());
        return out;
    }
    let mut best = encode_i64s_frame(min, &present);
    if let Some((base, stride)) = constant_stride_i64(&present) {
        let encoded = encode_stride_i64(base, stride);
        if encoded.len() < best.len() {
            best = encoded;
        }
    }
    let span = (max as u64).wrapping_sub(min as u64);
    let bits = bit_width_for_span(u128::from(span));
    if bitpack_len(8, bits, present.len()) < best.len() {
        let packed = encode_bitpack(
            &min.to_le_bytes(),
            bits,
            present
                .iter()
                .map(|value| u128::from((*value as u64).wrapping_sub(min as u64))),
        );
        if packed.len() < best.len() {
            best = packed;
        }
    }
    best
}

fn encode_i128s(values: &[Option<i128>]) -> Vec<u8> {
    let present: Vec<i128> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![KIND_EMPTY];
    }
    let min = present.iter().copied().min().unwrap();
    let max = present.iter().copied().max().unwrap();
    if min == max {
        let mut out = vec![KIND_CONSTANT];
        out.extend_from_slice(&min.to_le_bytes());
        return out;
    }
    let mut best = encode_i128s_frame(min, &present);
    if let Some((base, stride)) = constant_stride_i128(&present) {
        let encoded = encode_stride_i128(base, stride);
        if encoded.len() < best.len() {
            best = encoded;
        }
    }
    let span = (max as u128).wrapping_sub(min as u128);
    let bits = bit_width_for_span(span);
    if bitpack_len(16, bits, present.len()) < best.len() {
        let packed = encode_bitpack(
            &min.to_le_bytes(),
            bits,
            present
                .iter()
                .map(|value| (*value as u128).wrapping_sub(min as u128)),
        );
        if packed.len() < best.len() {
            best = packed;
        }
    }
    best
}

fn encode_i64s_frame(min: i64, present: &[i64]) -> Vec<u8> {
    let span = (present.iter().copied().max().unwrap() as u64).wrapping_sub(min as u64);
    let width = width_of(u128::from(span));
    let mut out = vec![kind_for_width(width)];
    out.extend_from_slice(&min.to_le_bytes());
    for value in present {
        let delta = (*value as u64).wrapping_sub(min as u64);
        write_uint(&mut out, u128::from(delta), width);
    }
    out
}

fn encode_i128s_frame(min: i128, present: &[i128]) -> Vec<u8> {
    let span = (present.iter().copied().max().unwrap() as u128).wrapping_sub(min as u128);
    let width = width_of(span);
    let mut out = vec![kind_for_width(width)];
    out.extend_from_slice(&min.to_le_bytes());
    for value in present {
        let delta = (*value as u128).wrapping_sub(min as u128);
        write_uint(&mut out, delta, width);
    }
    out
}

/// `base + index * stride` over non-null values in order. A zero stride is the
/// constant kind, so it is not reported here.
fn constant_stride_i64(present: &[i64]) -> Option<(i64, i64)> {
    if present.len() < 2 {
        return None;
    }
    let base = present[0];
    let stride = present[1].wrapping_sub(base);
    if stride == 0 {
        return None;
    }
    for (index, value) in present.iter().enumerate().skip(2) {
        let expected = base.wrapping_add((index as i64).wrapping_mul(stride));
        if *value != expected {
            return None;
        }
    }
    Some((base, stride))
}

fn constant_stride_i128(present: &[i128]) -> Option<(i128, i128)> {
    if present.len() < 2 {
        return None;
    }
    let base = present[0];
    let stride = present[1].wrapping_sub(base);
    if stride == 0 {
        return None;
    }
    for (index, value) in present.iter().enumerate().skip(2) {
        let expected = base.wrapping_add((index as i128).wrapping_mul(stride));
        if *value != expected {
            return None;
        }
    }
    Some((base, stride))
}

fn encode_stride_i64(base: i64, stride: i64) -> Vec<u8> {
    let mut out = vec![KIND_STRIDE];
    out.extend_from_slice(&base.to_le_bytes());
    out.extend_from_slice(&stride.to_le_bytes());
    out
}

fn encode_stride_i128(base: i128, stride: i128) -> Vec<u8> {
    let mut out = vec![KIND_STRIDE];
    out.extend_from_slice(&base.to_le_bytes());
    out.extend_from_slice(&stride.to_le_bytes());
    out
}

/// Exact-width frame of reference. Deltas are packed least-significant-bit
/// first. A FastLanes 1024-lane transpose is unnecessary for a correct
/// round trip; the bit cursor below is the scalar layout the decoder mirrors.
fn bitpack_len(base_len: usize, bits: u32, count: usize) -> usize {
    2 + base_len + (count * bits as usize).div_ceil(8)
}

fn encode_bitpack(base: &[u8], bits: u32, deltas: impl Iterator<Item = u128>) -> Vec<u8> {
    let mut out = vec![KIND_BITPACK, bits as u8];
    out.extend_from_slice(base);
    let mut packer = BitPacker::default();
    for delta in deltas {
        packer.push(delta, bits);
    }
    out.extend_from_slice(&packer.finish());
    out
}

/// Uncompressed size of each column in one block, including its null bitmap.
pub fn uncompressed_column_sizes(schema: &Schema, rows: &[Row]) -> Result<Vec<(String, usize)>> {
    let mut sizes = Vec::with_capacity(schema.fields.len());
    for (index, field) in schema.fields.iter().enumerate() {
        let mut column = Vec::new();
        encode_column(&mut column, field.ty, rows, index)?;
        sizes.push((field.name.clone(), column.len()));
    }
    Ok(sizes)
}

fn encode_f64s(values: &[Option<f64>]) -> Vec<u8> {
    let present: Vec<f64> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![0];
    }
    let first = present[0].to_bits();
    if present.iter().all(|value| value.to_bits() == first) {
        let mut out = vec![1];
        out.extend_from_slice(&present[0].to_le_bytes());
        return out;
    }
    let mut out = Vec::with_capacity(1 + present.len() * 8);
    out.push(2);
    for value in present {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn encode_bools(values: &[Option<bool>]) -> Vec<u8> {
    let present: Vec<bool> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![0];
    }
    if present.iter().all(|value| *value == present[0]) {
        return vec![1, u8::from(present[0])];
    }
    let mut out = vec![2];
    let mut bitmap = vec![0u8; present.len().div_ceil(8)];
    for (index, value) in present.iter().enumerate() {
        if *value {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    out.extend_from_slice(&bitmap);
    out
}

fn encode_strings(values: &[Option<String>], allow_dict: bool) -> Vec<u8> {
    let present: Vec<&str> = values.iter().filter_map(|value| value.as_deref()).collect();
    if present.is_empty() {
        return vec![0];
    }
    if present.iter().all(|text| *text == present[0]) {
        let mut out = vec![3];
        write_varint(&mut out, present[0].len() as u64);
        out.extend_from_slice(present[0].as_bytes());
        return out;
    }
    let raw = encode_raw_strings(&present);
    if !allow_dict || !string_dict_might_compress(&present) {
        return raw;
    }
    let dict = encode_dict_strings(&present);
    if dict.len() < raw.len() {
        dict
    } else {
        raw
    }
}

/// Dictionary encoding only wins when some values repeat; unique strings pay extra
/// for a code table, so skip the second pass unless a cheap check says it can win.
fn string_dict_might_compress(present: &[&str]) -> bool {
    if present.len() < 2 {
        return false;
    }
    const LARGE: usize = 4096;
    let total_bytes: u64 = present.iter().map(|text| text.len() as u64).sum();
    let mut small = HashSet::with_capacity(present.len());
    let mut large_fp = HashSet::with_capacity(present.len());
    let mut duplicate_bytes: u64 = 0;
    let mut seen_bytes: u64 = 0;
    for text in present {
        let tail_bytes = total_bytes - seen_bytes;
        let unique_count = small.len() + large_fp.len();
        let max_duplicate = duplicate_bytes.saturating_add(tail_bytes);
        let duplicate_can_win = max_duplicate.saturating_mul(2) >= total_bytes;
        let cardinality_can_win = unique_count.saturating_mul(2) <= present.len();
        if !duplicate_can_win && !cardinality_can_win {
            return false;
        }
        let len = text.len() as u64;
        seen_bytes += len;
        if text.len() > LARGE {
            let fp = (text.len(), string_fingerprint(text));
            if !large_fp.insert(fp) {
                duplicate_bytes += len;
            }
        } else if !small.insert(text) {
            duplicate_bytes += len;
        }
    }
    if duplicate_bytes == 0 {
        return false;
    }
    let unique_count = small.len() + large_fp.len();
    duplicate_bytes * 2 >= total_bytes || unique_count * 2 <= present.len()
}

fn string_fingerprint(text: &str) -> u64 {
    let mut hash = 0u64;
    for byte in text.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
    }
    hash
}

fn encode_raw_strings(present: &[&str]) -> Vec<u8> {
    let mut out = vec![2];
    for text in present {
        write_varint(&mut out, text.len() as u64);
        out.extend_from_slice(text.as_bytes());
    }
    out
}

fn encode_dict_strings(present: &[&str]) -> Vec<u8> {
    let mut lookup: HashMap<&str, u32> = HashMap::new();
    let mut dict: Vec<&str> = Vec::new();
    let mut codes = Vec::with_capacity(present.len());
    for text in present {
        let code = if let Some(code) = lookup.get(text) {
            *code
        } else {
            let code = dict.len() as u32;
            dict.push(text);
            lookup.insert(text, code);
            code
        };
        codes.push(code);
    }
    let width = if dict.len() <= 256 {
        1
    } else if dict.len() <= 65_536 {
        2
    } else {
        4
    };
    let mut out = vec![1];
    write_varint(&mut out, dict.len() as u64);
    for text in dict {
        write_varint(&mut out, text.len() as u64);
        out.extend_from_slice(text.as_bytes());
    }
    out.push(width as u8);
    for code in codes {
        write_uint(&mut out, code as u128, width);
    }
    out
}

fn kind_for_width(width: usize) -> u8 {
    match width {
        1 => 2,
        2 => 3,
        4 => 4,
        8 => 5,
        16 => 6,
        _ => 5,
    }
}

fn width_of(span: u128) -> usize {
    if span <= u128::from(u8::MAX) {
        1
    } else if span <= u128::from(u16::MAX) {
        2
    } else if span <= u128::from(u32::MAX) {
        4
    } else if span <= u128::from(u64::MAX) {
        8
    } else {
        16
    }
}

fn write_uint(out: &mut Vec<u8>, value: u128, width: usize) {
    let bytes = value.to_le_bytes();
    out.extend_from_slice(&bytes[..width]);
}

/// `ceil(log2(span + 1))`, the bits needed to store every delta in `0..=span`.
fn bit_width_for_span(span: u128) -> u32 {
    if span == u128::MAX {
        return 128;
    }
    let distinct = span + 1;
    u128::BITS - distinct.leading_zeros()
}

#[derive(Default)]
struct BitPacker {
    out: Vec<u8>,
    acc: u128,
    acc_bits: u32,
}

impl BitPacker {
    fn push(&mut self, value: u128, width: u32) {
        let value = if width >= 128 {
            value
        } else {
            value & ((1u128 << width) - 1)
        };
        let space = 128 - self.acc_bits;
        if width <= space {
            self.acc |= value << self.acc_bits;
            self.acc_bits += width;
            if self.acc_bits == 128 {
                self.flush_full();
            }
        } else {
            self.acc |= value << self.acc_bits;
            let rest = width - space;
            self.flush_full();
            self.acc = value >> space;
            self.acc_bits = rest;
        }
    }

    fn flush_full(&mut self) {
        self.out.extend_from_slice(&self.acc.to_le_bytes());
        self.acc = 0;
        self.acc_bits = 0;
    }

    fn finish(mut self) -> Vec<u8> {
        if self.acc_bits > 0 {
            let nbytes = (self.acc_bits as usize).div_ceil(8);
            let bytes = self.acc.to_le_bytes();
            self.out.extend_from_slice(&bytes[..nbytes]);
        }
        self.out
    }
}

struct BitUnpacker<'a> {
    data: &'a [u8],
    pos: usize,
    acc: u128,
    acc_bits: u32,
}

impl<'a> BitUnpacker<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            acc: 0,
            acc_bits: 0,
        }
    }

    fn pull(&mut self, width: u32) -> Result<u128> {
        let mut got = 0u32;
        let mut value = 0u128;
        while got < width {
            if self.acc_bits == 0 {
                let byte = *self
                    .data
                    .get(self.pos)
                    .ok_or_else(|| Error::corrupt("truncated bit-packed integer column"))?;
                self.pos += 1;
                self.acc = u128::from(byte);
                self.acc_bits = 8;
            }
            let take = (width - got).min(self.acc_bits);
            let mask = (1u128 << take) - 1;
            value |= (self.acc & mask) << got;
            self.acc >>= take;
            self.acc_bits -= take;
            got += take;
        }
        Ok(value)
    }
}

fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn ints(rows: &[Row], index: usize) -> Result<Vec<Option<i64>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Int(value) => Ok(Some(*value)),
            _ => Err(Error::event("int column contains a non-int value")),
        })
        .collect()
}

fn timestamps(rows: &[Row], index: usize) -> Result<Vec<Option<i64>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Timestamp(value) => Ok(Some(*value)),
            _ => Err(Error::event(
                "timestamp column contains a non-timestamp value",
            )),
        })
        .collect()
}

fn floats(rows: &[Row], index: usize) -> Result<Vec<Option<f64>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Float(value) => Ok(Some(*value)),
            _ => Err(Error::event("float column contains a non-float value")),
        })
        .collect()
}

fn bools(rows: &[Row], index: usize) -> Result<Vec<Option<bool>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Bool(value) => Ok(Some(*value)),
            _ => Err(Error::event("bool column contains a non-bool value")),
        })
        .collect()
}

fn decimals(rows: &[Row], index: usize) -> Result<Vec<Option<i128>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Decimal(value) => Ok(Some(*value)),
            _ => Err(Error::event("decimal column contains a non-decimal value")),
        })
        .collect()
}

fn strings(rows: &[Row], index: usize) -> Result<Vec<Option<String>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Str(value) => Ok(Some(value.clone())),
            _ => Err(Error::event("string column contains a non-string value")),
        })
        .collect()
}

fn json_texts(rows: &[Row], index: usize) -> Result<Vec<Option<String>>> {
    rows.iter()
        .map(|row| match &row.values[index] {
            Scalar::Null => Ok(None),
            Scalar::Json(text) => Ok(Some(text.clone())),
            _ => Err(Error::event("json column contains a non-json value")),
        })
        .collect()
}

fn decode_column(
    ty: FieldType,
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    keep: &[bool],
    budget: &mut usize,
) -> Result<Vec<Scalar>> {
    let present = read_present(bytes, cursor, nrows)?;
    let present_count = match &present {
        None => nrows,
        Some(flags) => flags.iter().filter(|flag| **flag).count(),
    };
    let present_keep = present_keep_flags(&present, keep, nrows);
    let mut values: Vec<Scalar> = match ty {
        FieldType::Int | FieldType::Timestamp => {
            let nums = decode_i64s(bytes, cursor, present_count)?;
            let wrap = |value: i64| {
                if ty == FieldType::Timestamp {
                    Scalar::Timestamp(value)
                } else {
                    Scalar::Int(value)
                }
            };
            nums.into_iter().map(wrap).collect()
        }
        FieldType::Float => decode_f64s(bytes, cursor, present_count)?
            .into_iter()
            .map(Scalar::Float)
            .collect(),
        FieldType::Bool => decode_bools(bytes, cursor, present_count)?
            .into_iter()
            .map(Scalar::Bool)
            .collect(),
        FieldType::Decimal { .. } => decode_i128s(bytes, cursor, present_count)?
            .into_iter()
            .map(Scalar::Decimal)
            .collect(),
        FieldType::String | FieldType::Text => {
            decode_strings(bytes, cursor, present_count, &present_keep, budget, false)?
                .into_iter()
                .map(Scalar::Str)
                .collect()
        }
        FieldType::Json => decode_json(bytes, cursor, present_count, &present_keep, budget)?,
    };
    if values.len() != present_count {
        return Err(Error::corrupt(
            "column value count does not match the null bitmap",
        ));
    }
    let mut column = Vec::with_capacity(nrows);
    let mut next = 0;
    for row in 0..nrows {
        let is_present = match &present {
            None => true,
            Some(flags) => flags[row],
        };
        if is_present {
            column.push(std::mem::replace(&mut values[next], Scalar::Null));
            next += 1;
        } else {
            column.push(Scalar::Null);
        }
    }
    Ok(column)
}

fn read_timestamp_column(schema: &Schema, bytes: &[u8]) -> Result<Vec<i64>> {
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4;
    for (index, field) in schema.fields.iter().enumerate() {
        if index == schema.timestamp_index {
            let present = read_present(bytes, &mut cursor, nrows)?;
            let present_count = match &present {
                None => nrows,
                Some(flags) => flags.iter().filter(|flag| **flag).count(),
            };
            if field.ty != FieldType::Timestamp {
                return Err(Error::corrupt("timestamp column has the wrong type"));
            }
            let nums = decode_i64s(bytes, &mut cursor, present_count)?;
            if nums.len() != present_count {
                return Err(Error::corrupt(
                    "column value count does not match the null bitmap",
                ));
            }
            let mut out = Vec::with_capacity(nrows);
            let mut next = 0;
            for row in 0..nrows {
                let is_present = match &present {
                    None => true,
                    Some(flags) => flags[row],
                };
                if is_present {
                    out.push(nums[next]);
                    next += 1;
                } else {
                    return Err(Error::corrupt("timestamp column is null or the wrong type"));
                }
            }
            return Ok(out);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows)?;
    }
    Err(Error::corrupt("timestamp column is missing"))
}

fn skip_column(ty: FieldType, bytes: &[u8], cursor: &mut usize, nrows: usize) -> Result<()> {
    let present = read_present(bytes, cursor, nrows)?;
    let present_count = match &present {
        None => nrows,
        Some(flags) => flags.iter().filter(|flag| **flag).count(),
    };
    match ty {
        FieldType::Int | FieldType::Timestamp => skip_i64s(bytes, cursor, present_count),
        FieldType::Float => skip_f64s(bytes, cursor, present_count),
        FieldType::Bool => skip_bools(bytes, cursor, present_count),
        FieldType::Decimal { .. } => skip_i128s(bytes, cursor, present_count),
        FieldType::String | FieldType::Text | FieldType::Json => {
            skip_strings(bytes, cursor, present_count)
        }
    }
}

fn skip_i64s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, KIND_EMPTY);
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let _ = read_i64(bytes, cursor)?;
        return Ok(());
    }
    if kind == KIND_STRIDE {
        let _ = read_i64(bytes, cursor)?;
        let _ = read_i64(bytes, cursor)?;
        return Ok(());
    }
    if kind == KIND_BITPACK {
        let bits = read_bit_width(bytes, cursor, 64)?;
        let _ = read_i64(bytes, cursor)?;
        skip_packed(bytes, cursor, count, bits, "int")?;
        return Ok(());
    }
    let width = width_for_kind(kind)?;
    if width > 8 {
        return Err(Error::corrupt("int column uses a 16-byte delta"));
    }
    let _ = read_i64(bytes, cursor)?;
    let nbytes = width
        .checked_mul(count)
        .ok_or_else(|| Error::corrupt("int column length overflow"))?;
    let _ = read_exact(bytes, cursor, nbytes)?;
    Ok(())
}

fn skip_i128s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, KIND_EMPTY);
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let _ = read_i128(bytes, cursor)?;
        return Ok(());
    }
    if kind == KIND_STRIDE {
        let _ = read_i128(bytes, cursor)?;
        let _ = read_i128(bytes, cursor)?;
        return Ok(());
    }
    if kind == KIND_BITPACK {
        let bits = read_bit_width(bytes, cursor, 128)?;
        let _ = read_i128(bytes, cursor)?;
        skip_packed(bytes, cursor, count, bits, "decimal")?;
        return Ok(());
    }
    let width = width_for_kind(kind)?;
    let _ = read_i128(bytes, cursor)?;
    let nbytes = width
        .checked_mul(count)
        .ok_or_else(|| Error::corrupt("decimal column length overflow"))?;
    let _ = read_exact(bytes, cursor, nbytes)?;
    Ok(())
}

fn read_bit_width(bytes: &[u8], cursor: &mut usize, max_bits: u32) -> Result<u32> {
    let bits = u32::from(read_u8(bytes, cursor)?);
    if bits == 0 || bits > max_bits {
        return Err(Error::corrupt("integer bit width is invalid"));
    }
    Ok(bits)
}

fn skip_packed(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    bits: u32,
    what: &str,
) -> Result<()> {
    let total_bits = count
        .checked_mul(bits as usize)
        .ok_or_else(|| Error::corrupt(format!("{what} column length overflow")))?;
    let _ = read_exact(bytes, cursor, total_bits.div_ceil(8))?;
    Ok(())
}

fn skip_f64s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, 0);
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        1 => {
            let _ = read_f64(bytes, cursor)?;
            Ok(())
        }
        2 => {
            let _ = read_exact(bytes, cursor, count.saturating_mul(8))?;
            Ok(())
        }
        _ => Err(Error::corrupt(format!("unknown float encoding {kind}"))),
    }
}

fn skip_bools(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, 0);
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        1 => {
            let _ = read_u8(bytes, cursor)?;
            Ok(())
        }
        2 => {
            let _ = read_exact(bytes, cursor, count.div_ceil(8))?;
            Ok(())
        }
        _ => Err(Error::corrupt(format!("unknown bool encoding {kind}"))),
    }
}

fn skip_strings(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, 0);
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => skip_lp_string(bytes, cursor),
        2 => {
            for _ in 0..count {
                skip_lp_string(bytes, cursor)?;
            }
            Ok(())
        }
        1 => {
            let dict_len = read_varint(bytes, cursor)? as usize;
            if dict_len == 0 {
                return Err(Error::corrupt("string dictionary is empty"));
            }
            for _ in 0..dict_len {
                skip_lp_string(bytes, cursor)?;
            }
            let width = read_u8(bytes, cursor)? as usize;
            if !matches!(width, 1 | 2 | 4) {
                return Err(Error::corrupt("string dictionary code width is invalid"));
            }
            let nbytes = width
                .checked_mul(count)
                .ok_or_else(|| Error::corrupt("string dictionary length overflow"))?;
            let _ = read_exact(bytes, cursor, nbytes)?;
            Ok(())
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn read_present(bytes: &[u8], cursor: &mut usize, nrows: usize) -> Result<Option<Vec<bool>>> {
    let flags = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::corrupt("truncated null flag"))?;
    *cursor += 1;
    match flags {
        0 => Ok(None),
        1 => {
            let nbytes = nrows.div_ceil(8);
            let end = *cursor + nbytes;
            let bitmap = bytes
                .get(*cursor..end)
                .ok_or_else(|| Error::corrupt("truncated null bitmap"))?;
            *cursor = end;
            let mut present = vec![false; nrows];
            for row in 0..nrows {
                present[row] = bitmap[row / 8] & (1 << (row % 8)) != 0;
            }
            Ok(Some(present))
        }
        _ => Err(Error::corrupt(format!("unknown null flag {flags}"))),
    }
}

fn decode_i64s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<i64>> {
    if count == 0 {
        expect_kind(bytes, cursor, KIND_EMPTY)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let base = read_i64(bytes, cursor)?;
        return Ok(vec![base; count]);
    }
    if kind == KIND_STRIDE {
        let base = read_i64(bytes, cursor)?;
        let stride = read_i64(bytes, cursor)?;
        let mut out = Vec::with_capacity(count);
        for index in 0..count {
            out.push(base.wrapping_add((index as i64).wrapping_mul(stride)));
        }
        return Ok(out);
    }
    if kind == KIND_BITPACK {
        let bits = read_bit_width(bytes, cursor, 64)?;
        let base = read_i64(bytes, cursor)?;
        let packed = read_packed(bytes, cursor, count, bits, "int")?;
        let mut unpacker = BitUnpacker::new(packed);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let delta = unpacker.pull(bits)? as u64;
            out.push((base as u64).wrapping_add(delta) as i64);
        }
        return Ok(out);
    }
    let width = width_for_kind(kind)?;
    if width > 8 {
        return Err(Error::corrupt("int column uses a 16-byte delta"));
    }
    let base = read_i64(bytes, cursor)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let delta = read_uint(bytes, cursor, width)? as u64;
        out.push((base as u64).wrapping_add(delta) as i64);
    }
    Ok(out)
}

fn decode_i128s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<i128>> {
    if count == 0 {
        expect_kind(bytes, cursor, KIND_EMPTY)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let base = read_i128(bytes, cursor)?;
        return Ok(vec![base; count]);
    }
    if kind == KIND_STRIDE {
        let base = read_i128(bytes, cursor)?;
        let stride = read_i128(bytes, cursor)?;
        let mut out = Vec::with_capacity(count);
        for index in 0..count {
            out.push(base.wrapping_add((index as i128).wrapping_mul(stride)));
        }
        return Ok(out);
    }
    if kind == KIND_BITPACK {
        let bits = read_bit_width(bytes, cursor, 128)?;
        let base = read_i128(bytes, cursor)?;
        let packed = read_packed(bytes, cursor, count, bits, "decimal")?;
        let mut unpacker = BitUnpacker::new(packed);
        let mut out = Vec::with_capacity(count);
        for _ in 0..count {
            let delta = unpacker.pull(bits)?;
            out.push((base as u128).wrapping_add(delta) as i128);
        }
        return Ok(out);
    }
    let width = width_for_kind(kind)?;
    let base = read_i128(bytes, cursor)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let delta = read_uint(bytes, cursor, width)?;
        out.push((base as u128).wrapping_add(delta) as i128);
    }
    Ok(out)
}

fn read_packed<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    count: usize,
    bits: u32,
    what: &str,
) -> Result<&'a [u8]> {
    let total_bits = count
        .checked_mul(bits as usize)
        .ok_or_else(|| Error::corrupt(format!("{what} column length overflow")))?;
    read_exact(bytes, cursor, total_bits.div_ceil(8))
}

fn decode_f64s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<f64>> {
    if count == 0 {
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        1 => {
            let value = read_f64(bytes, cursor)?;
            Ok(vec![value; count])
        }
        2 => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(read_f64(bytes, cursor)?);
            }
            Ok(out)
        }
        _ => Err(Error::corrupt(format!("unknown float encoding {kind}"))),
    }
}

fn decode_bools(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<bool>> {
    if count == 0 {
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        1 => {
            let value = read_u8(bytes, cursor)?;
            if value > 1 {
                return Err(Error::corrupt("bool constant is not 0 or 1"));
            }
            Ok(vec![value == 1; count])
        }
        2 => {
            let nbytes = count.div_ceil(8);
            let end = *cursor + nbytes;
            let bitmap = bytes
                .get(*cursor..end)
                .ok_or_else(|| Error::corrupt("truncated bool bitmap"))?;
            *cursor = end;
            let mut out = Vec::with_capacity(count);
            for index in 0..count {
                out.push(bitmap[index / 8] & (1 << (index % 8)) != 0);
            }
            Ok(out)
        }
        _ => Err(Error::corrupt(format!("unknown bool encoding {kind}"))),
    }
}

fn present_keep_flags(present: &Option<Vec<bool>>, keep: &[bool], nrows: usize) -> Vec<bool> {
    let mut out = Vec::new();
    for row in 0..nrows {
        let is_present = match present {
            None => true,
            Some(flags) => flags[row],
        };
        if is_present {
            out.push(keep[row]);
        }
    }
    out
}

fn charge_string_bytes(budget: &mut usize, n: usize) -> Result<()> {
    if n > *budget {
        return Err(Error::event("query response size limit exceeded"));
    }
    *budget -= n;
    Ok(())
}

fn decode_strings(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    keep: &[bool],
    budget: &mut usize,
    validate_json: bool,
) -> Result<Vec<String>> {
    if keep.len() != count {
        return Err(Error::corrupt("string keep mask does not match the column"));
    }
    if count == 0 {
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => {
            let text = read_lp_string(bytes, cursor)?;
            if validate_json {
                validate_json_column_text(&text)?;
            }
            let copies = keep.iter().filter(|flag| **flag).count();
            let expanded = text
                .len()
                .checked_mul(copies)
                .ok_or_else(|| Error::event("query response size limit exceeded"))?;
            charge_string_bytes(budget, expanded)?;
            let mut out = Vec::with_capacity(count);
            for flag in keep {
                if *flag {
                    out.push(text.clone());
                } else {
                    out.push(String::new());
                }
            }
            Ok(out)
        }
        2 => {
            let mut out = Vec::with_capacity(count);
            for flag in keep {
                if *flag {
                    let text = read_lp_string(bytes, cursor)?;
                    if validate_json {
                        validate_json_column_text(&text)?;
                    }
                    charge_string_bytes(budget, text.len())?;
                    out.push(text);
                } else {
                    skip_lp_string(bytes, cursor)?;
                    out.push(String::new());
                }
            }
            Ok(out)
        }
        1 => {
            let dict_len = read_varint(bytes, cursor)? as usize;
            if dict_len == 0 {
                return Err(Error::corrupt("string dictionary is empty"));
            }
            let mut dict = Vec::with_capacity(dict_len);
            for _ in 0..dict_len {
                let text = read_lp_string(bytes, cursor)?;
                if validate_json {
                    validate_json_column_text(&text)?;
                }
                dict.push(text);
            }
            let width = read_u8(bytes, cursor)? as usize;
            if !matches!(width, 1 | 2 | 4) {
                return Err(Error::corrupt("string dictionary code width is invalid"));
            }
            let mut codes = Vec::with_capacity(count);
            for _ in 0..count {
                codes.push(read_uint(bytes, cursor, width)? as usize);
            }
            let mut expanded = 0usize;
            for (code, flag) in codes.iter().zip(keep.iter()) {
                if !*flag {
                    continue;
                }
                let text = dict
                    .get(*code)
                    .ok_or_else(|| Error::corrupt("string dictionary code is out of range"))?;
                expanded = expanded
                    .checked_add(text.len())
                    .ok_or_else(|| Error::event("query response size limit exceeded"))?;
            }
            charge_string_bytes(budget, expanded)?;
            let mut out = Vec::with_capacity(count);
            for (code, flag) in codes.iter().zip(keep.iter()) {
                if *flag {
                    let text = dict
                        .get(*code)
                        .ok_or_else(|| Error::corrupt("string dictionary code is out of range"))?;
                    out.push(text.clone());
                } else {
                    out.push(String::new());
                }
            }
            Ok(out)
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn decode_json(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    keep: &[bool],
    budget: &mut usize,
) -> Result<Vec<Scalar>> {
    Ok(decode_strings(bytes, cursor, count, keep, budget, true)?
        .into_iter()
        .map(Scalar::Json)
        .collect())
}

fn validate_json_column_text(text: &str) -> Result<()> {
    validate_json_structure(text).map_err(|_| Error::corrupt("json column value is not valid JSON"))
}

fn read_lp_string(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    let slice = read_lp_bytes(bytes, cursor)?;
    String::from_utf8(slice.to_vec()).map_err(|_| Error::corrupt("string is not utf-8"))
}

fn skip_lp_string(bytes: &[u8], cursor: &mut usize) -> Result<()> {
    let _ = read_lp_bytes(bytes, cursor)?;
    Ok(())
}

fn read_lp_bytes<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a [u8]> {
    let len = read_varint(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::corrupt("string length overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::corrupt("truncated string"))?;
    *cursor = end;
    Ok(slice)
}

fn expect_kind(bytes: &[u8], cursor: &mut usize, expected: u8) -> Result<()> {
    let kind = read_u8(bytes, cursor)?;
    if kind != expected {
        return Err(Error::corrupt(format!(
            "expected encoding kind {expected}, found {kind}"
        )));
    }
    Ok(())
}

fn width_for_kind(kind: u8) -> Result<usize> {
    match kind {
        2 => Ok(1),
        3 => Ok(2),
        4 => Ok(4),
        5 => Ok(8),
        6 => Ok(16),
        _ => Err(Error::corrupt(format!("unknown integer encoding {kind}"))),
    }
}

fn read_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let byte = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::corrupt("truncated block"))?;
    *cursor += 1;
    Ok(byte)
}

fn read_exact<'a>(bytes: &'a [u8], cursor: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::corrupt("length overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::corrupt("truncated block"))?;
    *cursor = end;
    Ok(slice)
}

fn read_i64(bytes: &[u8], cursor: &mut usize) -> Result<i64> {
    let slice = read_exact(bytes, cursor, 8)?;
    Ok(i64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_i128(bytes: &[u8], cursor: &mut usize) -> Result<i128> {
    let slice = read_exact(bytes, cursor, 16)?;
    Ok(i128::from_le_bytes(slice.try_into().unwrap()))
}

fn read_f64(bytes: &[u8], cursor: &mut usize) -> Result<f64> {
    let slice = read_exact(bytes, cursor, 8)?;
    Ok(f64::from_le_bytes(slice.try_into().unwrap()))
}

fn read_uint(bytes: &[u8], cursor: &mut usize, width: usize) -> Result<u128> {
    let slice = read_exact(bytes, cursor, width)?;
    let mut buf = [0u8; 16];
    buf[..width].copy_from_slice(slice);
    Ok(u128::from_le_bytes(buf))
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = read_u8(bytes, cursor)?;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift > 63 {
            return Err(Error::corrupt("varint is too long"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;
    use crate::value::{parse_event, row_to_json, Scalar};

    fn schema() -> Schema {
        parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "score", "type": "float"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"},
                    {"name": "note", "type": "text"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn string_dictionary_wins_on_low_cardinality() {
        let values: Vec<Option<String>> = (0..8)
            .map(|i| Some(["click", "view", "buy"][i % 3].to_string()))
            .collect();
        let present: Vec<&str> = values.iter().filter_map(|v| v.as_deref()).collect();
        assert!(string_dict_might_compress(&present));
        let distinct: Vec<String> = (0..64)
            .map(|i| format!("doc-{i:04}-{}", "x".repeat(64)))
            .collect();
        let distinct_refs: Vec<&str> = distinct.iter().map(String::as_str).collect();
        assert!(!string_dict_might_compress(&distinct_refs));
        let raw = encode_raw_strings(&present);
        let dict = encode_dict_strings(&present);
        assert!(dict.len() < raw.len());
        let encoded = encode_strings(&values, true);
        assert_eq!(encoded[0], 1, "expected dictionary encoding kind 1");
    }

    #[test]
    fn string_fingerprint_does_not_overflow_on_large_strings() {
        let a = "x".repeat(5000);
        let b = "y".repeat(5000);
        let _ = string_fingerprint(&a);
        let _ = string_fingerprint(&b);
        let values = vec![Some(a), Some(b)];
        let _ = encode_strings(&values, true);
    }

    #[test]
    fn roundtrip_covers_nulls_constants_and_dictionaries() {
        let schema = schema();
        let mut rows = Vec::new();
        for i in 0..32i64 {
            let action = ["click", "view", "buy"][i as usize % 3];
            let mut obj = serde_json::json!({
                "ts": 1_700_000_000_000i64 + i * 10,
                "score": if i % 6 == 0 { 1.5 } else { i as f64 / 4.0 },
                "ok": i % 2 == 0,
                "action": action,
                "amount": format!("{}.{:02}", i / 2, (i * 3) % 100),
            });
            if i % 4 != 0 {
                obj["user_id"] = serde_json::json!(i % 3);
            }
            if i % 5 != 0 {
                obj["note"] = serde_json::json!(format!("note-{i}"));
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let decoded = decode_block(&schema, &encoded.bytes).unwrap();
        assert_eq!(decoded.len(), rows.len());
        assert_eq!(encoded.min_ts, rows[0].ts);
        assert_eq!(encoded.max_ts, rows.last().unwrap().ts);
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.ts, right.ts);
            let l = row_to_json(&schema, left).unwrap();
            let r = row_to_json(&schema, right).unwrap();
            assert_eq!(l, r);
        }
        assert!(matches!(decoded[0].values[1], Scalar::Null));
        assert!(matches!(decoded[0].values[5], Scalar::Null));
    }

    #[test]
    fn json_column_roundtrips_mixed_shapes() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let payloads = [
            serde_json::json!({"plan": "pro", "flags": ["a", 1]}),
            serde_json::json!({"plan": "pro", "flags": ["a", 1]}),
            serde_json::json!([1, "x", {"k": false}]),
            serde_json::json!("plain"),
            serde_json::json!(42),
            serde_json::json!(true),
            serde_json::Value::Null,
        ];
        let mut rows = Vec::new();
        for (i, props) in payloads.iter().enumerate() {
            let mut obj = serde_json::json!({"ts": 1_000 + i as i64});
            if !props.is_null() {
                obj["props"] = props.clone();
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let decoded = decode_block(&schema, &encoded.bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values[1], right.values[1]);
        }
        assert!(matches!(decoded.last().unwrap().values[1], Scalar::Null));
        assert!(matches!(decoded[0].values[1], Scalar::Json(_)));
    }

    #[test]
    fn constant_json_is_rejected_before_it_exceeds_the_budget() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let body = "a".repeat(64);
        let mut rows = Vec::new();
        for ts in 1..=4 {
            let raw = format!(r#"{{"ts":{ts},"props":"{body}"}}"#);
            rows.push(parse_event(&schema, raw.as_bytes()).unwrap());
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let one = match &rows[0].values[1] {
            Scalar::Json(text) => text.len(),
            _ => panic!("expected json"),
        };
        let err = decode_rows_in_range(&schema, &encoded.bytes, 1, 4, one * 4 - 1).unwrap_err();
        assert!(err
            .to_string()
            .contains("query response size limit exceeded"));
        let decoded = decode_rows_in_range(&schema, &encoded.bytes, 1, 4, one * 4).unwrap();
        assert_eq!(decoded.len(), 4);
        let partial = decode_rows_in_range(&schema, &encoded.bytes, 4, 4, one).unwrap();
        assert_eq!(partial.len(), 1);
        assert_eq!(partial[0].ts, 4);
        let too_small = decode_rows_in_range(&schema, &encoded.bytes, 4, 4, one - 1).unwrap_err();
        assert!(too_small
            .to_string()
            .contains("query response size limit exceeded"));
    }

    #[test]
    fn constant_stride_with_nulls_in_the_middle_roundtrips() {
        let mut values = Vec::new();
        let mut present = Vec::new();
        for index in 0..20 {
            if index % 2 == 0 {
                values.push(None);
            } else {
                let value = 100 + (index / 2) * 10;
                present.push(value);
                values.push(Some(value));
            }
        }
        let encoded = encode_i64s(&values);
        assert_eq!(encoded[0], KIND_STRIDE);
        let mut cursor = 0;
        let decoded = decode_i64s(&encoded, &mut cursor, present.len()).unwrap();
        assert_eq!(decoded, present);
        assert_eq!(cursor, encoded.len());

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "user_id", "type": "int"},
                    {"name": "ts", "type": "timestamp"}
                ]
            }"#,
        )
        .unwrap();
        let mut rows = Vec::new();
        for (index, user_id) in values.iter().enumerate() {
            let mut obj = serde_json::json!({
                "ts": 1_000 + index as i64 * 10,
            });
            if let Some(user_id) = user_id {
                obj["user_id"] = serde_json::json!(user_id);
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let block = encode_block(&schema, &rows).unwrap();
        let decoded = decode_block(&schema, &block.bytes).unwrap();
        assert_eq!(decoded.len(), rows.len());
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values, right.values);
        }
        let ranged = decode_rows_in_range(&schema, &block.bytes, 1_000, 1_020, usize::MAX).unwrap();
        assert_eq!(ranged.len(), 3);
        assert!(matches!(ranged[0].values[0], Scalar::Null));
        assert_eq!(ranged[1].values[0], Scalar::Int(100));
        assert!(matches!(ranged[2].values[0], Scalar::Null));
    }

    #[test]
    fn negative_i64_roundtrips_for_stride_and_bit_width() {
        let stride: Vec<Option<i64>> = (0..12).map(|i| Some(-80 + i * 7)).collect();
        let encoded = encode_i64s(&stride);
        assert_eq!(encoded[0], KIND_STRIDE);
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, stride.len()).unwrap(),
            stride.into_iter().flatten().collect::<Vec<_>>()
        );

        let packed: Vec<Option<i64>> = [-1000, -1, 40, -7, 12].into_iter().map(Some).collect();
        let encoded = encode_i64s(&packed);
        assert_eq!(encoded[0], KIND_BITPACK);
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, packed.len()).unwrap(),
            vec![-1000, -1, 40, -7, 12]
        );

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let mut rows = Vec::new();
        for (index, user_id) in packed.iter().enumerate() {
            let obj = serde_json::json!({
                "ts": 50 + index as i64,
                "user_id": user_id,
            });
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let block = encode_block(&schema, &rows).unwrap();
        let decoded = decode_block(&schema, &block.bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values, right.values);
        }
    }

    #[test]
    fn exact_bit_width_packs_span_999() {
        let values: Vec<Option<i64>> = (0..2048).map(|i| Some(i % 1000)).collect();
        let encoded = encode_i64s(&values);
        assert_eq!(encoded[0], KIND_BITPACK);
        assert_eq!(encoded[1], 10, "999 fits in 10 bits");
        let mut cursor = 0;
        let decoded = decode_i64s(&encoded, &mut cursor, values.len()).unwrap();
        assert_eq!(cursor, encoded.len());
        for (index, value) in decoded.iter().enumerate() {
            assert_eq!(*value, (index % 1000) as i64);
        }
        let mut skip = 0;
        skip_i64s(&encoded, &mut skip, values.len()).unwrap();
        assert_eq!(skip, encoded.len());
    }

    #[test]
    fn decimal_i128_stride_and_exact_width_roundtrip() {
        let stride: Vec<Option<i128>> = (0..32).map(|i| Some(5_000 + i * 25)).collect();
        let encoded = encode_i128s(&stride);
        assert_eq!(encoded[0], KIND_STRIDE);
        let mut cursor = 0;
        assert_eq!(
            decode_i128s(&encoded, &mut cursor, stride.len()).unwrap(),
            stride.into_iter().flatten().collect::<Vec<_>>()
        );

        let mut packed = vec![Some(0i128), Some(999), Some(1), Some(500)];
        packed.extend((0..64).map(|i| Some(i128::from((i * 3) % 1000))));
        let encoded = encode_i128s(&packed);
        assert_eq!(encoded[0], KIND_BITPACK);
        assert_eq!(encoded[1], 10);
        let mut cursor = 0;
        assert_eq!(
            decode_i128s(&encoded, &mut cursor, packed.len()).unwrap(),
            packed.iter().copied().flatten().collect::<Vec<_>>()
        );
        let mut skip = 0;
        skip_i128s(&encoded, &mut skip, packed.len()).unwrap();
        assert_eq!(skip, encoded.len());

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let mut rows = Vec::new();
        for i in 0..16i64 {
            let cents = if i % 2 == 0 { i * 25 } else { 999 };
            let obj = serde_json::json!({
                "ts": 10_000 + i,
                "amount": format!("{}.{:02}", cents / 100, cents % 100),
            });
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let block = encode_block(&schema, &rows).unwrap();
        let decoded = decode_block(&schema, &block.bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values, right.values);
        }
    }

    #[test]
    fn constant_timestamp_keeps_the_constant_kind() {
        let values = vec![Some(1_700_000_000_000i64); 32];
        let encoded = encode_i64s(&values);
        assert_eq!(encoded[0], KIND_CONSTANT);
        assert_ne!(encoded[0], KIND_STRIDE);

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let mut rows = Vec::new();
        for i in 0..8i64 {
            let obj = serde_json::json!({
                "ts": 1_700_000_000_000i64,
                "user_id": i,
            });
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let block = encode_block(&schema, &rows).unwrap();
        assert_eq!(block.bytes[5], KIND_CONSTANT);
        let decoded = decode_block(&schema, &block.bytes).unwrap();
        assert!(decoded.iter().all(|row| row.ts == 1_700_000_000_000));
    }

    #[test]
    fn legacy_byte_width_blocks_still_decode() {
        let mut payload = vec![3u8];
        payload.extend_from_slice(&10i64.to_le_bytes());
        payload.extend_from_slice(&0u16.to_le_bytes());
        payload.extend_from_slice(&5u16.to_le_bytes());
        let mut cursor = 0;
        assert_eq!(decode_i64s(&payload, &mut cursor, 2).unwrap(), vec![10, 15]);
        assert_eq!(cursor, payload.len());

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.push(0);
        bytes.push(KIND_CONSTANT);
        bytes.extend_from_slice(&1000i64.to_le_bytes());
        bytes.push(0);
        bytes.push(3);
        bytes.extend_from_slice(&0i64.to_le_bytes());
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&2u16.to_le_bytes());
        let rows = decode_block(&schema, &bytes).unwrap();
        assert_eq!(rows[0].ts, 1000);
        assert_eq!(rows[1].ts, 1000);
        assert_eq!(rows[0].values[1], Scalar::Int(1));
        assert_eq!(rows[1].values[1], Scalar::Int(2));
    }
}
