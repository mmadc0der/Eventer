use std::collections::HashMap;

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

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_block(schema: &Schema, bytes: &[u8]) -> Result<Vec<Row>> {
    if bytes.len() < 4 {
        return Err(Error::corrupt("block is shorter than its row count"));
    }
    let nrows = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if nrows == 0 || nrows > 1_000_000 {
        return Err(Error::corrupt(format!(
            "block row count {nrows} is invalid"
        )));
    }
    let mut cursor = 4;
    let mut columns = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        columns.push(decode_column(field.ty, bytes, &mut cursor, nrows, None)?);
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

/// One column equality set. Several predicates on the same column are already intersected.
#[derive(Debug, Clone)]
pub(crate) struct ColumnPredicate {
    pub index: usize,
    pub allowed: Vec<Scalar>,
}

/// Decode rows whose timestamp is inside `from_ms..=to_ms` and that match every predicate.
///
/// Filter columns are read first. A constant or dictionary that cannot contain the
/// requested value returns no rows without decoding later columns. Other columns are
/// skipped until the match mask is known, then only matching rows are materialized.
pub(crate) fn decode_rows_in_range(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
) -> Result<Vec<Row>> {
    if from_ms > to_ms || predicates.iter().any(|pred| pred.allowed.is_empty()) {
        return Ok(Vec::new());
    }
    if bytes.len() < 4 {
        return Err(Error::corrupt("block is shorter than its row count"));
    }
    let nrows = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if nrows == 0 || nrows > 1_000_000 {
        return Err(Error::corrupt(format!(
            "block row count {nrows} is invalid"
        )));
    }
    let mut cursor = 4;
    let mut mask = vec![true; nrows];
    let mut columns: Vec<Option<Vec<Scalar>>> = (0..schema.fields.len()).map(|_| None).collect();
    let mut skipped: Vec<(usize, usize, usize)> = Vec::new();

    for (index, field) in schema.fields.iter().enumerate() {
        let predicate = predicates.iter().find(|pred| pred.index == index);
        let is_timestamp = index == schema.timestamp_index;
        let is_filter = is_timestamp || predicate.is_some();
        let filters_remain = (index + 1..schema.fields.len()).any(|later| {
            later == schema.timestamp_index || predicates.iter().any(|pred| pred.index == later)
        });

        if is_filter {
            if is_timestamp && timestamp_range_misses(bytes, cursor, nrows, from_ms, to_ms)? {
                return finish_empty(schema, bytes, &mut cursor, index);
            }
            if let Some(predicate) = predicate {
                if matches!(field.ty, FieldType::String | FieldType::Text) {
                    match read_string_filter(
                        bytes,
                        &mut cursor,
                        nrows,
                        &mut mask,
                        &predicate.allowed,
                    )? {
                        StringFilter::Miss => {
                            return finish_empty(schema, bytes, &mut cursor, index + 1);
                        }
                        StringFilter::Values(values) => {
                            apply_eq(&mut mask, &values, &predicate.allowed);
                            columns[index] = Some(values);
                            if !mask.iter().any(|keep| *keep) {
                                return finish_empty(schema, bytes, &mut cursor, index + 1);
                            }
                            continue;
                        }
                    }
                } else if column_misses(field.ty, bytes, cursor, nrows, &predicate.allowed)? {
                    return finish_empty(schema, bytes, &mut cursor, index);
                }
            }
            let values = decode_column(field.ty, bytes, &mut cursor, nrows, Some(&mask))?;
            if is_timestamp {
                for (row, value) in values.iter().enumerate() {
                    if !mask[row] {
                        continue;
                    }
                    match value {
                        Scalar::Timestamp(ts) => {
                            if *ts < from_ms || *ts > to_ms {
                                mask[row] = false;
                            }
                        }
                        _ => {
                            return Err(Error::corrupt(
                                "timestamp column is null or the wrong type",
                            ))
                        }
                    }
                }
            }
            if let Some(predicate) = predicate {
                for (row, value) in values.iter().enumerate() {
                    if mask[row] && !predicate.allowed.iter().any(|allowed| allowed == value) {
                        mask[row] = false;
                    }
                }
            }
            columns[index] = Some(values);
            if !mask.iter().any(|keep| *keep) {
                return finish_empty(schema, bytes, &mut cursor, index + 1);
            }
        } else if filters_remain {
            let start = cursor;
            skip_column(field.ty, bytes, &mut cursor, nrows)?;
            skipped.push((index, start, cursor));
        } else {
            columns[index] = Some(decode_column(
                field.ty,
                bytes,
                &mut cursor,
                nrows,
                Some(&mask),
            )?);
        }
    }
    if cursor != bytes.len() {
        return Err(Error::corrupt("block has trailing bytes"));
    }
    for (index, start, end) in skipped {
        let mut replay = start;
        let values = decode_column(
            schema.fields[index].ty,
            bytes,
            &mut replay,
            nrows,
            Some(&mask),
        )?;
        if replay != end {
            return Err(Error::corrupt("column skip and decode disagree"));
        }
        columns[index] = Some(values);
    }

    let mut rows = Vec::new();
    for row_idx in 0..nrows {
        if !mask[row_idx] {
            continue;
        }
        let mut values = Vec::with_capacity(columns.len());
        for column in &columns {
            let column = column
                .as_ref()
                .ok_or_else(|| Error::corrupt("decoded block is missing a column"))?;
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
    if !allow_dict {
        return raw;
    }
    let dict = encode_dict_strings(&present);
    if dict.len() < raw.len() {
        dict
    } else {
        raw
    }
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

fn decode_column(
    ty: FieldType,
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    keep: Option<&[bool]>,
) -> Result<Vec<Scalar>> {
    let present = read_present(bytes, cursor, nrows)?;
    let present_count = match &present {
        None => nrows,
        Some(flags) => flags.iter().filter(|flag| **flag).count(),
    };
    let materialize = keep.map(|keep| {
        let mut flags = Vec::with_capacity(present_count);
        for row in 0..nrows {
            let is_present = match &present {
                None => true,
                Some(flags) => flags[row],
            };
            if is_present {
                flags.push(keep[row]);
            }
        }
        flags
    });
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
            decode_strings(bytes, cursor, present_count, materialize.as_deref())?
                .into_iter()
                .map(Scalar::Str)
                .collect()
        }
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
            let value = std::mem::replace(&mut values[next], Scalar::Null);
            next += 1;
            if keep.map(|flags| flags[row]).unwrap_or(true) {
                column.push(value);
            } else {
                column.push(Scalar::Null);
            }
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

fn decode_strings(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    materialize: Option<&[bool]>,
) -> Result<Vec<String>> {
    if count == 0 {
        expect_kind(bytes, cursor, 0)?;
        return Ok(Vec::new());
    }
    let keep = |index: usize| materialize.map(|flags| flags[index]).unwrap_or(true);
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => {
            let text = read_lp_string(bytes, cursor)?;
            let mut out = Vec::with_capacity(count);
            for index in 0..count {
                if keep(index) {
                    out.push(text.clone());
                } else {
                    out.push(String::new());
                }
            }
            Ok(out)
        }
        2 => {
            let mut out = Vec::with_capacity(count);
            for index in 0..count {
                if keep(index) {
                    out.push(read_lp_string(bytes, cursor)?);
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
                dict.push(read_lp_string(bytes, cursor)?);
            }
            let width = read_u8(bytes, cursor)? as usize;
            if !matches!(width, 1 | 2 | 4) {
                return Err(Error::corrupt("string dictionary code width is invalid"));
            }
            let mut out = Vec::with_capacity(count);
            for index in 0..count {
                let code = read_uint(bytes, cursor, width)? as usize;
                if keep(index) {
                    let text = dict
                        .get(code)
                        .ok_or_else(|| Error::corrupt("string dictionary code is out of range"))?;
                    out.push(text.clone());
                } else {
                    if code >= dict.len() {
                        return Err(Error::corrupt("string dictionary code is out of range"));
                    }
                    out.push(String::new());
                }
            }
            Ok(out)
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn read_lp_str<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a str> {
    let len = read_varint(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| Error::corrupt("string length overflow"))?;
    let slice = bytes
        .get(*cursor..end)
        .ok_or_else(|| Error::corrupt("truncated string"))?;
    *cursor = end;
    std::str::from_utf8(slice).map_err(|_| Error::corrupt("string is not utf-8"))
}

fn read_lp_string(bytes: &[u8], cursor: &mut usize) -> Result<String> {
    read_lp_str(bytes, cursor).map(str::to_owned)
}

fn skip_lp_string(bytes: &[u8], cursor: &mut usize) -> Result<()> {
    let _ = read_lp_str(bytes, cursor)?;
    Ok(())
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
        FieldType::String | FieldType::Text => skip_strings(bytes, cursor, present_count),
    }
}

fn skip_i64s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, 0);
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == 1 {
        let _ = read_i64(bytes, cursor)?;
        return Ok(());
    }
    let width = width_for_kind(kind)?;
    if width > 8 {
        return Err(Error::corrupt("int column uses a 16-byte delta"));
    }
    let _ = read_i64(bytes, cursor)?;
    let nbytes = count
        .checked_mul(width)
        .ok_or_else(|| Error::corrupt("length overflow"))?;
    let _ = read_exact(bytes, cursor, nbytes)?;
    Ok(())
}

fn skip_i128s(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, 0);
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == 1 {
        let _ = read_i128(bytes, cursor)?;
        return Ok(());
    }
    let width = width_for_kind(kind)?;
    let _ = read_i128(bytes, cursor)?;
    let nbytes = count
        .checked_mul(width)
        .ok_or_else(|| Error::corrupt("length overflow"))?;
    let _ = read_exact(bytes, cursor, nbytes)?;
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
            let nbytes = count
                .checked_mul(8)
                .ok_or_else(|| Error::corrupt("length overflow"))?;
            let _ = read_exact(bytes, cursor, nbytes)?;
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
            let nbytes = count.div_ceil(8);
            let _ = read_exact(bytes, cursor, nbytes)?;
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
            for _ in 0..count {
                let code = read_uint(bytes, cursor, width)?;
                if code >= dict_len as u128 {
                    return Err(Error::corrupt("string dictionary code is out of range"));
                }
            }
            Ok(())
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn finish_empty(
    schema: &Schema,
    bytes: &[u8],
    cursor: &mut usize,
    from_index: usize,
) -> Result<Vec<Row>> {
    let nrows = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    for field in &schema.fields[from_index..] {
        skip_column(field.ty, bytes, cursor, nrows)?;
    }
    if *cursor != bytes.len() {
        return Err(Error::corrupt("block has trailing bytes"));
    }
    Ok(Vec::new())
}

fn apply_eq(mask: &mut [bool], values: &[Scalar], allowed: &[Scalar]) {
    for (row, value) in values.iter().enumerate() {
        if mask[row] && !allowed.iter().any(|candidate| candidate == value) {
            mask[row] = false;
        }
    }
}

enum StringFilter {
    Miss,
    Values(Vec<Scalar>),
}

fn scalar_text_hit(allowed: &[Scalar], text: &str) -> bool {
    allowed
        .iter()
        .any(|value| matches!(value, Scalar::Str(expected) if expected == text))
}

/// One pass over a string or text filter column.
///
/// A miss still checks dictionary code bounds. Matching raw values are the only
/// ones copied into `String`s.
fn read_string_filter(
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    mask: &mut [bool],
    allowed: &[Scalar],
) -> Result<StringFilter> {
    let present = read_present(bytes, cursor, nrows)?;
    let (present_count, _) = present_stats(&present, nrows);
    let null_ok = allowed.iter().any(|value| matches!(value, Scalar::Null));
    let row_present = |row: usize, present: &Option<Vec<bool>>| match present {
        None => true,
        Some(flags) => flags[row],
    };
    let null_hit = null_ok && (0..nrows).any(|row| mask[row] && !row_present(row, &present));
    if present_count == 0 {
        expect_kind(bytes, cursor, 0)?;
        if !null_hit {
            return Ok(StringFilter::Miss);
        }
        return Ok(StringFilter::Values(vec![Scalar::Null; nrows]));
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => {
            let text = read_lp_str(bytes, cursor)?;
            let hit = scalar_text_hit(allowed, text);
            if !hit && !null_hit {
                return Ok(StringFilter::Miss);
            }
            let owned = text.to_owned();
            let mut column = Vec::with_capacity(nrows);
            for (row, keep) in mask.iter_mut().enumerate() {
                if row_present(row, &present) {
                    if *keep && hit {
                        column.push(Scalar::Str(owned.clone()));
                    } else {
                        *keep = false;
                        column.push(Scalar::Null);
                    }
                } else if *keep && null_ok {
                    column.push(Scalar::Null);
                } else {
                    *keep = false;
                    column.push(Scalar::Null);
                }
            }
            Ok(StringFilter::Values(column))
        }
        2 => {
            let mut column = vec![Scalar::Null; nrows];
            let mut saw = null_hit;
            for row in 0..nrows {
                if !row_present(row, &present) {
                    if !(mask[row] && null_ok) {
                        mask[row] = false;
                    }
                    continue;
                }
                let text = read_lp_str(bytes, cursor)?;
                if mask[row] && scalar_text_hit(allowed, text) {
                    saw = true;
                    column[row] = Scalar::Str(text.to_owned());
                } else {
                    mask[row] = false;
                }
            }
            Ok(if saw {
                StringFilter::Values(column)
            } else {
                StringFilter::Miss
            })
        }
        1 => {
            let dict_len = read_varint(bytes, cursor)? as usize;
            if dict_len == 0 {
                return Err(Error::corrupt("string dictionary is empty"));
            }
            let mut dict = Vec::with_capacity(dict_len);
            let mut entry_hit = false;
            for _ in 0..dict_len {
                let text = read_lp_str(bytes, cursor)?;
                entry_hit |= scalar_text_hit(allowed, text);
                dict.push(text);
            }
            let width = read_u8(bytes, cursor)? as usize;
            if !matches!(width, 1 | 2 | 4) {
                return Err(Error::corrupt("string dictionary code width is invalid"));
            }
            let mut codes = Vec::with_capacity(present_count);
            for _ in 0..present_count {
                let code = read_uint(bytes, cursor, width)?;
                if code >= dict_len as u128 {
                    return Err(Error::corrupt("string dictionary code is out of range"));
                }
                codes.push(code as usize);
            }
            if !entry_hit && !null_hit {
                return Ok(StringFilter::Miss);
            }
            let mut column = vec![Scalar::Null; nrows];
            let mut next = 0;
            for row in 0..nrows {
                if !row_present(row, &present) {
                    if !(mask[row] && null_ok) {
                        mask[row] = false;
                    }
                    continue;
                }
                let text = dict[codes[next]];
                next += 1;
                if mask[row] && scalar_text_hit(allowed, text) {
                    column[row] = Scalar::Str(text.to_owned());
                } else {
                    mask[row] = false;
                }
            }
            Ok(StringFilter::Values(column))
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn present_stats(present: &Option<Vec<bool>>, nrows: usize) -> (usize, bool) {
    match present {
        None => (nrows, false),
        Some(flags) => (
            flags.iter().filter(|flag| **flag).count(),
            flags.iter().any(|flag| !flag),
        ),
    }
}

fn column_misses(
    ty: FieldType,
    bytes: &[u8],
    start: usize,
    nrows: usize,
    allowed: &[Scalar],
) -> Result<bool> {
    if allowed.is_empty() {
        return Ok(true);
    }
    let mut cursor = start;
    let present = read_present(bytes, &mut cursor, nrows)?;
    let (present_count, has_null) = present_stats(&present, nrows);
    let null_ok = allowed.iter().any(|value| matches!(value, Scalar::Null));
    if present_count == 0 {
        return Ok(!null_ok);
    }
    match ty {
        FieldType::String | FieldType::Text => Ok(false),
        FieldType::Int => int_const_misses(bytes, &mut cursor, allowed, has_null, null_ok, false),
        FieldType::Timestamp => {
            int_const_misses(bytes, &mut cursor, allowed, has_null, null_ok, true)
        }
        FieldType::Float => float_const_misses(bytes, &mut cursor, allowed, has_null, null_ok),
        FieldType::Bool => bool_const_misses(bytes, &mut cursor, allowed, has_null, null_ok),
        FieldType::Decimal { .. } => {
            decimal_const_misses(bytes, &mut cursor, allowed, has_null, null_ok)
        }
    }
}

fn int_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    allowed: &[Scalar],
    has_null: bool,
    null_ok: bool,
    timestamp: bool,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_i64(bytes, cursor)?;
    let hit = allowed.iter().any(|value| match value {
        Scalar::Int(number) if !timestamp => *number == base,
        Scalar::Timestamp(number) if timestamp => *number == base,
        _ => false,
    });
    Ok(!hit && !(has_null && null_ok))
}

fn float_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    allowed: &[Scalar],
    has_null: bool,
    null_ok: bool,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_f64(bytes, cursor)?;
    let hit = allowed
        .iter()
        .any(|value| matches!(value, Scalar::Float(number) if *number == base));
    Ok(!hit && !(has_null && null_ok))
}

fn bool_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    allowed: &[Scalar],
    has_null: bool,
    null_ok: bool,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_u8(bytes, cursor)?;
    if base > 1 {
        return Err(Error::corrupt("bool constant is not 0 or 1"));
    }
    let flag = base == 1;
    let hit = allowed
        .iter()
        .any(|value| matches!(value, Scalar::Bool(expected) if *expected == flag));
    Ok(!hit && !(has_null && null_ok))
}

fn decimal_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    allowed: &[Scalar],
    has_null: bool,
    null_ok: bool,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_i128(bytes, cursor)?;
    let hit = allowed
        .iter()
        .any(|value| matches!(value, Scalar::Decimal(number) if *number == base));
    Ok(!hit && !(has_null && null_ok))
}

fn timestamp_range_misses(
    bytes: &[u8],
    start: usize,
    nrows: usize,
    from_ms: i64,
    to_ms: i64,
) -> Result<bool> {
    let mut cursor = start;
    let present = read_present(bytes, &mut cursor, nrows)?;
    let (present_count, has_null) = present_stats(&present, nrows);
    if has_null || present_count == 0 {
        return Ok(false);
    }
    let kind = read_u8(bytes, &mut cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_i64(bytes, &mut cursor)?;
    Ok(base < from_ms || base > to_ms)
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
    fn filtered_decode_matches_rows_and_skips_a_column_miss() {
        let schema = schema();
        let mut rows = Vec::new();
        for i in 0..16i64 {
            let action = ["click", "view", "buy"][i as usize % 3];
            let mut obj = serde_json::json!({
                "ts": 1_000 + i * 1_000,
                "score": 1.5,
                "ok": true,
                "action": action,
                "amount": "1.00",
                "note": format!("payload-{i}-{}", "x".repeat(64)),
            });
            if i % 4 != 0 {
                obj["user_id"] = serde_json::json!(i);
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let click = ColumnPredicate {
            index: 4,
            allowed: vec![Scalar::Str("click".into())],
        };
        let filtered = decode_rows_in_range(
            &schema,
            &encoded.bytes,
            2_000,
            12_000,
            std::slice::from_ref(&click),
        )
        .unwrap();
        let expected: Vec<_> = decode_block(&schema, &encoded.bytes)
            .unwrap()
            .into_iter()
            .filter(|row| (2_000..=12_000).contains(&row.ts))
            .filter(|row| matches!(&row.values[4], Scalar::Str(action) if action == "click"))
            .collect();
        assert_eq!(filtered.len(), expected.len());
        assert!(filtered.len() < rows.len());
        for (left, right) in expected.iter().zip(filtered.iter()) {
            assert_eq!(
                row_to_json(&schema, left).unwrap(),
                row_to_json(&schema, right).unwrap()
            );
        }

        let note = ColumnPredicate {
            index: 5,
            allowed: vec![Scalar::Str("payload-1-".to_string() + &"x".repeat(64))],
        };
        let by_note =
            decode_rows_in_range(&schema, &encoded.bytes, i64::MIN, i64::MAX, &[note]).unwrap();
        assert_eq!(by_note.len(), 1);
        assert_eq!(by_note[0].ts, 2_000);

        let miss = ColumnPredicate {
            index: 4,
            allowed: vec![Scalar::Str("missing".into())],
        };
        let mut truncated = encoded.bytes.clone();
        truncated.pop();
        assert!(decode_rows_in_range(&schema, &truncated, i64::MIN, i64::MAX, &[miss]).is_err());
        assert!(decode_rows_in_range(
            &schema,
            &truncated,
            i64::MIN,
            i64::MAX,
            std::slice::from_ref(&click)
        )
        .is_err());
    }

    #[test]
    fn filtered_decode_rejects_invalid_utf8_on_a_dropped_row() {
        let schema = schema();
        let click = parse_event(
            &schema,
            br#"{"ts":1000,"user_id":1,"score":1.0,"ok":true,"action":"click","note":"alpha","amount":"1.00"}"#,
        )
        .unwrap();
        let view = parse_event(
            &schema,
            br#"{"ts":2000,"user_id":2,"score":1.0,"ok":true,"action":"view","note":"beta","amount":"1.00"}"#,
        )
        .unwrap();
        let encoded = encode_block(&schema, &[click, view]).unwrap();
        let mut bytes = encoded.bytes;
        let pos = bytes
            .windows(4)
            .position(|window| window == b"beta")
            .expect("note payload");
        bytes[pos] = 0xff;
        assert!(decode_block(&schema, &bytes)
            .unwrap_err()
            .to_string()
            .contains("utf-8"));
        let filter = ColumnPredicate {
            index: 4,
            allowed: vec![Scalar::Str("click".into())],
        };
        assert!(
            decode_rows_in_range(&schema, &bytes, i64::MIN, i64::MAX, &[filter])
                .unwrap_err()
                .to_string()
                .contains("utf-8")
        );
    }
}
