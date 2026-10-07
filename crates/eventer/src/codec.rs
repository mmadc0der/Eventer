use std::collections::{HashMap, HashSet};

use crate::json_scan::validate_json_structure;
use crate::error::{Error, Result};
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
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4;
    let mut columns = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        columns.push(decode_column(field.ty, bytes, &mut cursor, nrows)?);
    }
    if cursor != bytes.len() {
        return Err(Error::corrupt("block has trailing bytes"));
    }
    let mut rows = Vec::with_capacity(nrows);
    for row_idx in 0..nrows {
        let mut values = Vec::with_capacity(columns.len());
        for column in &columns {
            values.push(column[row_idx].clone());
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

fn encode_i64s(values: &[Option<i64>]) -> Vec<u8> {
    let present: Vec<i64> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![0];
    }
    let min = present.iter().copied().min().unwrap();
    let max = present.iter().copied().max().unwrap();
    if min == max {
        let mut out = vec![1];
        out.extend_from_slice(&min.to_le_bytes());
        return out;
    }
    let span = (max as u64).wrapping_sub(min as u64);
    let width = width_of(span as u128);
    let mut out = vec![kind_for_width(width)];
    out.extend_from_slice(&min.to_le_bytes());
    for value in present {
        let delta = (value as u64).wrapping_sub(min as u64);
        write_uint(&mut out, delta as u128, width);
    }
    out
}

fn encode_i128s(values: &[Option<i128>]) -> Vec<u8> {
    let present: Vec<i128> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![0];
    }
    let min = present.iter().copied().min().unwrap();
    let max = present.iter().copied().max().unwrap();
    if min == max {
        let mut out = vec![1];
        out.extend_from_slice(&min.to_le_bytes());
        return out;
    }
    let span = (max as u128).wrapping_sub(min as u128);
    let width = width_of(span);
    let mut out = vec![kind_for_width(width)];
    out.extend_from_slice(&min.to_le_bytes());
    for value in present {
        let delta = (value as u128).wrapping_sub(min as u128);
        write_uint(&mut out, delta, width);
    }
    out
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
    for text in present {
        if text.len() > LARGE {
            let fp = (text.len(), string_fingerprint(text));
            if !large_fp.insert(fp) {
                duplicate_bytes += text.len() as u64;
            }
        } else if !small.insert(text) {
            duplicate_bytes += text.len() as u64;
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
) -> Result<Vec<Scalar>> {
    let present = read_present(bytes, cursor, nrows)?;
    let present_count = match &present {
        None => nrows,
        Some(flags) => flags.iter().filter(|flag| **flag).count(),
    };
    let values: Vec<Scalar> = match ty {
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
        FieldType::String | FieldType::Text => decode_strings(bytes, cursor, present_count)?
            .into_iter()
            .map(Scalar::Str)
            .collect(),
        FieldType::Json => decode_json(bytes, cursor, present_count)?,
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
            column.push(values[next].clone());
            next += 1;
        } else {
            column.push(Scalar::Null);
        }
    }
    Ok(column)
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
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == 1 {
        let base = read_i64(bytes, cursor)?;
        return Ok(vec![base; count]);
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
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == 1 {
        let base = read_i128(bytes, cursor)?;
        return Ok(vec![base; count]);
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

fn decode_strings(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<String>> {
    if count == 0 {
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => {
            let text = read_lp_string(bytes, cursor)?;
            Ok(vec![text; count])
        }
        2 => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(read_lp_string(bytes, cursor)?);
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
                dict.push(read_lp_string(bytes, cursor)?);
            }
            let width = read_u8(bytes, cursor)? as usize;
            if !matches!(width, 1 | 2 | 4) {
                return Err(Error::corrupt("string dictionary code width is invalid"));
            }
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                let code = read_uint(bytes, cursor, width)? as usize;
                let text = dict
                    .get(code)
                    .ok_or_else(|| Error::corrupt("string dictionary code is out of range"))?;
                out.push(text.clone());
            }
            Ok(out)
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn decode_json(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<Scalar>> {
    decode_strings(bytes, cursor, count)?
        .into_iter()
        .map(|text| {
            validate_json_column_text(&text)?;
            Ok(Scalar::Json(text))
        })
        .collect()
}

fn validate_json_column_text(text: &str) -> Result<()> {
    validate_json_structure(text).map_err(|_| Error::corrupt("json column value is not valid JSON"))
}

fn read_lp_string(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    let len = read_varint(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::corrupt("string length overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::corrupt("truncated string"))?;
    *cursor = end;
    String::from_utf8(slice.to_vec()).map_err(|_| Error::corrupt("string is not utf-8"))
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
}
