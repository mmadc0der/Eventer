use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::json_scan::validate_json_structure;
use crate::schema::{FieldType, Schema};
use crate::value::{Row, Scalar};

/// Uncompressed columnar block. `min_ts` / `max_ts` cover every row.
///
/// `slices` names ranges of `bytes` after the 4-byte row count. A json column
/// is its own slice so a filter on an earlier column does not have to
/// decompress it. The writer still stores one frame when that is smaller.
pub struct EncodedBlock {
    pub bytes: Vec<u8>,
    pub min_ts: i64,
    pub max_ts: i64,
    pub row_count: u32,
    pub slices: Vec<SliceSpan>,
}

/// One compression unit inside [`EncodedBlock::bytes`], not including the
/// leading row count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SliceSpan {
    pub start: u32,
    pub end: u32,
    pub first_column: u16,
    pub last_column: u16,
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
    let mut column_ends = Vec::with_capacity(schema.fields.len());
    for (index, field) in schema.fields.iter().enumerate() {
        encode_column(&mut bytes, field.ty, rows, index)?;
        column_ends.push(bytes.len() as u32);
    }
    let slices = plan_slices(&column_ends);
    Ok(EncodedBlock {
        bytes,
        min_ts,
        max_ts,
        row_count: rows.len() as u32,
        slices,
    })
}

/// Each column is its own slice so a filter stops after the predicate.
///
/// The writer falls back to one frame when the container grows too much.
fn plan_slices(column_ends: &[u32]) -> Vec<SliceSpan> {
    column_ends
        .iter()
        .enumerate()
        .map(|(index, end)| SliceSpan {
            start: if index == 0 {
                4
            } else {
                column_ends[index - 1]
            },
            end: *end,
            first_column: index as u16,
            last_column: index as u16,
        })
        .collect()
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
    decode_rows_in_range_filtered(schema, bytes, from_ms, to_ms, max_string_bytes, &[])
}

pub(crate) struct ColumnPredicate {
    pub index: usize,
    pub allowed: Vec<Scalar>,
}

/// Byte prefix of one string or text column. Several prefixes on the same
/// column are already reduced to the longest one that implies the others.
/// A null does not match. An empty prefix is not stored.
pub(crate) struct PrefixFilter {
    pub index: usize,
    pub prefix: String,
}

fn prefix_at<'a>(prefixes: &'a [PrefixFilter], index: usize) -> Option<&'a str> {
    prefixes
        .iter()
        .find(|prefix| prefix.index == index)
        .map(|prefix| prefix.prefix.as_str())
}

/// One end of an ordered comparison. `inclusive` is false for `>` and `<`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RangeEnd {
    pub inclusive: bool,
    pub value: Scalar,
}

/// Inclusive or exclusive numeric bounds on one column. Missing ends are open.
///
/// A null never satisfies a range. Several bounds on the same column are already
/// tightened to one lower end and one upper end.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RangeFilter {
    pub index: usize,
    pub lower: Option<RangeEnd>,
    pub upper: Option<RangeEnd>,
}

pub(crate) fn cmp_numeric(left: &Scalar, right: &Scalar) -> Option<Ordering> {
    match (left, right) {
        (Scalar::Int(left), Scalar::Int(right)) => Some(left.cmp(right)),
        (Scalar::Timestamp(left), Scalar::Timestamp(right)) => Some(left.cmp(right)),
        (Scalar::Decimal(left), Scalar::Decimal(right)) => Some(left.cmp(right)),
        (Scalar::Float(left), Scalar::Float(right)) => left.partial_cmp(right),
        _ => None,
    }
}

pub(crate) fn tighter_lower(left: RangeEnd, right: RangeEnd) -> RangeEnd {
    tighter_end(left, right, true)
}

pub(crate) fn tighter_upper(left: RangeEnd, right: RangeEnd) -> RangeEnd {
    tighter_end(left, right, false)
}

fn tighter_end(left: RangeEnd, right: RangeEnd, lower: bool) -> RangeEnd {
    match cmp_numeric(&left.value, &right.value) {
        Some(Ordering::Equal) => RangeEnd {
            inclusive: left.inclusive && right.inclusive,
            value: left.value,
        },
        Some(order) if (order == Ordering::Greater) == lower => left,
        Some(_) => right,
        None => left,
    }
}

/// True when the two ends cannot contain a value, including equal exclusive ends.
pub(crate) fn range_impossible(range: &RangeFilter) -> bool {
    let (Some(lower), Some(upper)) = (&range.lower, &range.upper) else {
        return false;
    };
    match cmp_numeric(&lower.value, &upper.value) {
        Some(Ordering::Greater) => true,
        Some(Ordering::Equal) => !(lower.inclusive && upper.inclusive),
        Some(Ordering::Less) => false,
        None => true,
    }
}

pub(crate) fn range_matches(value: &Scalar, range: &RangeFilter) -> bool {
    if matches!(value, Scalar::Null) {
        return false;
    }
    if let Some(lower) = &range.lower {
        match cmp_numeric(value, &lower.value) {
            Some(Ordering::Greater) => {}
            Some(Ordering::Equal) if lower.inclusive => {}
            _ => return false,
        }
    }
    if let Some(upper) = &range.upper {
        match cmp_numeric(value, &upper.value) {
            Some(Ordering::Less) => {}
            Some(Ordering::Equal) if upper.inclusive => {}
            _ => return false,
        }
    }
    true
}

/// Decode rows in `from_ms..=to_ms` that match every predicate.
///
/// An empty predicate list uses one pass and the string budget. A constant or
/// dictionary that cannot match still walks later column framing so a corrupt
/// block returns [`Error::Corrupt`](crate::Error).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn decode_rows_in_range_filtered(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    max_string_bytes: usize,
    predicates: &[ColumnPredicate],
) -> Result<Vec<Row>> {
    decode_rows_filtered(
        schema,
        bytes,
        from_ms,
        to_ms,
        max_string_bytes,
        predicates,
        &[],
    )
}

pub(crate) fn decode_rows_filtered(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    max_string_bytes: usize,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
) -> Result<Vec<Row>> {
    decode_rows_filtered_with(
        schema,
        bytes,
        from_ms,
        to_ms,
        max_string_bytes,
        predicates,
        ranges,
        &[],
    )
}

pub(crate) fn decode_rows_filtered_with(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    max_string_bytes: usize,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    prefixes: &[PrefixFilter],
) -> Result<Vec<Row>> {
    decode_rows_included(
        schema,
        bytes,
        from_ms,
        to_ms,
        max_string_bytes,
        predicates,
        ranges,
        prefixes,
        None,
    )
}

/// Decode like [`decode_rows_filtered`].
///
/// `included` is the column set a sliced read actually decompressed. A false
/// column is a hole: it is null, and its bytes are not in `bytes`. `None`
/// means the bytes are a normal block image. Columns the caller does not want
/// are still decoded when they are present; the caller drops them afterwards.
pub(crate) fn decode_rows_included(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    max_string_bytes: usize,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    prefixes: &[PrefixFilter],
    included: Option<&[bool]>,
) -> Result<Vec<Row>> {
    if included.is_none() && predicates.is_empty() && ranges.is_empty() && prefixes.is_empty() {
        return decode_rows(schema, bytes, Some((from_ms, to_ms)), max_string_bytes);
    }
    if from_ms > to_ms
        || predicates.iter().any(|pred| pred.allowed.is_empty())
        || ranges.iter().any(range_impossible)
    {
        return Ok(Vec::new());
    }
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4;
    let mut mask = vec![true; nrows];
    let mut columns: Vec<Option<Vec<Scalar>>> = (0..schema.fields.len()).map(|_| None).collect();
    let mut skipped: Vec<(usize, usize, usize)> = Vec::new();
    let mut deferred: Vec<(usize, bool, Vec<Option<(usize, usize)>>)> = Vec::new();
    let mut budget = max_string_bytes;

    for (index, field) in schema.fields.iter().enumerate() {
        if included.is_some_and(|included| !included.get(index).copied().unwrap_or(false)) {
            let nulls = vec![Scalar::Null; nrows];
            if let Some(predicate) = predicates.iter().find(|pred| pred.index == index) {
                apply_eq(&mut mask, &nulls, &predicate.allowed);
            }
            if ranges.iter().any(|range| range.index == index)
                || prefix_at(prefixes, index).is_some()
            {
                mask.fill(false);
            }
            columns[index] = Some(nulls);
            if !mask.iter().any(|keep| *keep) {
                return finish_empty(schema, bytes, &mut cursor, index + 1, included);
            }
            continue;
        }
        if column_was_not_stored(bytes, cursor) {
            if index <= schema.timestamp_index {
                return Err(Error::corrupt("timestamp column is missing"));
            }
            for rest in index..schema.fields.len() {
                let nulls = vec![Scalar::Null; nrows];
                if let Some(predicate) = predicates.iter().find(|pred| pred.index == rest) {
                    apply_eq(&mut mask, &nulls, &predicate.allowed);
                }
                if ranges.iter().any(|range| range.index == rest)
                    || prefix_at(prefixes, rest).is_some()
                {
                    mask.fill(false);
                }
                columns[rest] = Some(nulls);
            }
            break;
        }
        let predicate = predicates.iter().find(|pred| pred.index == index);
        let range = ranges.iter().find(|range| range.index == index);
        let prefix = prefix_at(prefixes, index);
        let is_timestamp = index == schema.timestamp_index;
        let is_filter = is_timestamp || predicate.is_some() || range.is_some() || prefix.is_some();
        let filters_remain = (index + 1..schema.fields.len()).any(|later| {
            later == schema.timestamp_index
                || predicates.iter().any(|pred| pred.index == later)
                || ranges.iter().any(|range| range.index == later)
                || prefix_at(prefixes, later).is_some()
        });

        if is_filter {
            if is_timestamp && timestamp_range_misses(bytes, cursor, nrows, from_ms, to_ms)? {
                return finish_empty(schema, bytes, &mut cursor, index, included);
            }
            if predicate.is_some() || prefix.is_some() {
                if matches!(
                    field.ty,
                    FieldType::String | FieldType::Text | FieldType::Json
                ) {
                    let spans =
                        read_text_spans(bytes, &mut cursor, nrows, field.ty == FieldType::Json)?;
                    if let Some(predicate) = predicate {
                        mask_text_spans(bytes, &spans, &mut mask, &predicate.allowed);
                    }
                    if let Some(prefix) = prefix {
                        if field.ty == FieldType::Json {
                            mask.fill(false);
                        } else {
                            mask_text_prefix(bytes, &spans, &mut mask, prefix);
                        }
                    }
                    if range.is_some() {
                        mask.fill(false);
                    }
                    if !mask.iter().any(|keep| *keep) {
                        return finish_empty(schema, bytes, &mut cursor, index + 1, included);
                    }
                    // Clone only after every later predicate has updated `mask`.
                    deferred.push((index, field.ty == FieldType::Json, spans));
                    continue;
                }
            }
            if (predicate.is_some() || range.is_some())
                && column_misses(field.ty, bytes, cursor, nrows, predicate, range)?
            {
                return finish_empty(schema, bytes, &mut cursor, index, included);
            }
            let values = decode_column(field.ty, bytes, &mut cursor, nrows, &mask, &mut budget)?;
            if is_timestamp {
                for (row, value) in values.iter().enumerate() {
                    match value {
                        Scalar::Timestamp(ts) => {
                            if mask[row] && (*ts < from_ms || *ts > to_ms) {
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
            apply_constraints(&mut mask, &values, predicate, range);
            if let Some(prefix) = prefix {
                apply_prefix(&mut mask, &values, prefix);
            }
            columns[index] = Some(values);
            if !mask.iter().any(|keep| *keep) {
                return finish_empty(schema, bytes, &mut cursor, index + 1, included);
            }
        } else if filters_remain {
            let start = cursor;
            // The event timestamp is always a filter, so this skip is another column.
            skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
            skipped.push((index, start, cursor));
        } else {
            columns[index] = Some(decode_column(
                field.ty,
                bytes,
                &mut cursor,
                nrows,
                &mask,
                &mut budget,
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
            &mask,
            &mut budget,
        )?;
        if replay != end {
            return Err(Error::corrupt("column skip and decode disagree"));
        }
        columns[index] = Some(values);
    }
    for (index, json, spans) in deferred {
        columns[index] = Some(materialize_text_spans(
            bytes,
            &spans,
            &mask,
            &mut budget,
            json,
        )?);
    }

    let mut rows = Vec::new();
    for row_idx in 0..nrows {
        if !mask[row_idx] {
            continue;
        }
        let mut values = Vec::with_capacity(columns.len());
        for column in &mut columns {
            let column = column
                .as_mut()
                .ok_or_else(|| Error::corrupt("decoded block is missing a column"))?;
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
    for (index, field) in schema.fields.iter().enumerate() {
        if column_was_not_stored(bytes, cursor) {
            // Columns appended after this block was written are null.
            // The timestamp is part of every locked schema, so it is never absent.
            if index <= schema.timestamp_index {
                return Err(Error::corrupt("timestamp column is missing"));
            }
            columns.push(vec![Scalar::Null; nrows]);
            continue;
        }
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

/// Count rows in `from_ms..=to_ms` that match every predicate.
///
/// An empty predicate list reads the timestamp column and nothing after it.
/// When every payload timestamp is inside the range, that count is the block's
/// row count. A timestamp outside the range is left out even if the sparse
/// index min/max sit inside the range. A null or undecodable timestamp returns
/// the same [`Error::Corrupt`](crate::Error) as a row decode.
///
/// Predicates decode the timestamp column and the predicate columns. Columns
/// after the last of those are not read. Columns between them are skipped, not
/// turned into values.
///
/// One equality on a string or text column, with no other predicate and no
/// numeric range, is counted from the column header when every payload
/// timestamp sits in the inclusive range. A constant string contributes the
/// row count or zero. One period of dictionary codes contributes the matches
/// in that period, repeated across the rows, and the codes are not expanded.
/// A null, a full per-row code vector, raw strings, or a timestamp outside
/// the range still walks rows. The sparse index is not that check. A constant
/// or a positive non-wrapping timestamp stride may prove it. Any other
/// timestamp encoding still expands.
///
/// One equality on a bool column uses the same rule. A constant bool
/// contributes the row count or zero. One bitmap period contributes the
/// matches in that period, repeated across the rows, and a bool is not
/// expanded per row. A null, a full per-row bitmap, or a timestamp outside
/// the range still walks rows.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn count_rows_in_range_filtered(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
) -> Result<u64> {
    count_rows_filtered(schema, bytes, from_ms, to_ms, predicates, &[])
}

pub(crate) fn count_rows_filtered(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
) -> Result<u64> {
    count_rows_filtered_with(schema, bytes, from_ms, to_ms, predicates, ranges, &[])
}

pub(crate) fn count_rows_filtered_with(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    prefixes: &[PrefixFilter],
) -> Result<u64> {
    if prefixes.is_empty() {
        if let Some(total) =
            count_one_string_equality(schema, bytes, from_ms, to_ms, predicates, ranges, None)?
        {
            return Ok(total);
        }
        if let Some(total) =
            count_one_bool_equality(schema, bytes, from_ms, to_ms, predicates, ranges, None)?
        {
            return Ok(total);
        }
    }
    let mut total = 0u64;
    visit_matching_timestamps_with(
        schema,
        bytes,
        from_ms,
        to_ms,
        predicates,
        ranges,
        prefixes,
        &mut [],
        |_ts, _row, _captured| {
            total = total
                .checked_add(1)
                .ok_or_else(|| Error::event("event count overflow"))?;
            Ok(())
        },
    )?;
    Ok(total)
}

/// Add each matching timestamp to the epoch-aligned bucket that contains it.
///
/// `counts[i]` is the bucket starting at `first_start + i * bucket_ms`. A timestamp
/// outside `from_ms..=to_ms` is not visited. `bucket_ms` is a positive integer.
///
/// With no predicates, a timestamp column that is a constant or a positive
/// non-wrapping stride, with no nulls, and whose encoded endpoints both sit in
/// the inclusive range, adds each whole run to those buckets. The column is not
/// expanded. Anything else walks the timestamps.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn accumulate_histogram(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    bucket_ms: i64,
    first_start: i128,
    counts: &mut [u64],
) -> Result<()> {
    accumulate_histogram_filtered(
        schema,
        bytes,
        from_ms,
        to_ms,
        predicates,
        &[],
        bucket_ms,
        first_start,
        counts,
    )
}

pub(crate) fn accumulate_histogram_filtered(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    bucket_ms: i64,
    first_start: i128,
    counts: &mut [u64],
) -> Result<()> {
    accumulate_histogram_with(
        schema,
        bytes,
        from_ms,
        to_ms,
        predicates,
        ranges,
        &[],
        bucket_ms,
        first_start,
        counts,
    )
}

pub(crate) fn accumulate_histogram_with(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    prefixes: &[PrefixFilter],
    bucket_ms: i64,
    first_start: i128,
    counts: &mut [u64],
) -> Result<()> {
    if predicates.is_empty() && ranges.is_empty() && prefixes.is_empty() {
        if let Some(run) = plain_timestamp_run(schema, bytes, from_ms, to_ms)? {
            return add_timestamp_run(&run, bucket_ms, first_start, counts);
        }
    }
    visit_matching_timestamps_with(
        schema,
        bytes,
        from_ms,
        to_ms,
        predicates,
        ranges,
        prefixes,
        &mut [],
        |ts, _row, _captured| {
            let start = (ts as i128).div_euclid(i128::from(bucket_ms)) * i128::from(bucket_ms);
            let index = (start - first_start) / i128::from(bucket_ms);
            let index =
                usize::try_from(index).map_err(|_| Error::corrupt("histogram bucket index"))?;
            let bucket = counts
                .get_mut(index)
                .ok_or_else(|| Error::corrupt("histogram bucket index"))?;
            *bucket = bucket
                .checked_add(1)
                .ok_or_else(|| Error::event("event count overflow"))?;
            Ok(())
        },
    )
}

/// A constant (`stride == 0`) or a positive stride over every row.
struct TimestampRun {
    base: i64,
    stride: i64,
    count: usize,
}

/// Header of a null-free constant or positive non-wrapping stride whose first
/// and last values both sit in `from_ms..=to_ms`.
///
/// The sparse index is not read. A negative stride, wrapping arithmetic,
/// stride pieces, bit packing, a null, or an endpoint outside the range is
/// `None`, and the caller walks rows. A truncated header is corrupt.
fn plain_timestamp_run(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
) -> Result<Option<TimestampRun>> {
    plain_timestamp_run_included(schema, bytes, from_ms, to_ms, None)
}

fn plain_timestamp_run_included(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    included: Option<&[bool]>,
) -> Result<Option<TimestampRun>> {
    if from_ms > to_ms {
        return Ok(None);
    }
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if column_is_hole(included, index) {
            if index == schema.timestamp_index {
                return Err(Error::corrupt("timestamp column is missing"));
            }
            continue;
        }
        if column_was_not_stored(bytes, cursor) {
            return Err(Error::corrupt("timestamp column is missing"));
        }
        if index == schema.timestamp_index {
            if field.ty != FieldType::Timestamp {
                return Err(Error::corrupt("timestamp column has the wrong type"));
            }
            return plain_i64_run(bytes, &mut cursor, nrows, from_ms, to_ms);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Err(Error::corrupt("timestamp column is missing"))
}

fn plain_i64_run(
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    from_ms: i64,
    to_ms: i64,
) -> Result<Option<TimestampRun>> {
    let flag = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::corrupt("truncated null flag"))?;
    if flag != 0 {
        return Ok(None);
    }
    *cursor += 1;
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let base = read_i64(bytes, cursor)?;
        if base < from_ms || base > to_ms {
            return Ok(None);
        }
        return Ok(Some(TimestampRun {
            base,
            stride: 0,
            count: nrows,
        }));
    }
    if kind == KIND_STRIDE {
        let base = read_i64(bytes, cursor)?;
        let stride = read_i64(bytes, cursor)?;
        if stride <= 0 {
            return Ok(None);
        }
        let Some(last) = stride_end(base, stride, nrows) else {
            return Ok(None);
        };
        if base < from_ms || last > to_ms {
            return Ok(None);
        }
        return Ok(Some(TimestampRun {
            base,
            stride,
            count: nrows,
        }));
    }
    Ok(None)
}

/// One string, text, or bool equality, counted from a constant or one period.
///
/// The caller has already dropped numeric ranges a zone proved. `included`
/// names the columns whose encodings are in `bytes`. `None` means a normal
/// block image. A false column is a hole and contributes no bytes. `Ok(None)`
/// means this header cannot count the block: a second predicate, a value that
/// is not one string or one bool, a null, a full code vector or bitmap, raw
/// strings, or a timestamp column this header cannot prove sits inside the
/// range. The row walk still has to run.
pub(crate) fn count_header_equality(
    schema: &Schema,
    bytes: &[u8],
    included: Option<&[bool]>,
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
) -> Result<Option<u64>> {
    if let Some(total) =
        count_one_string_equality(schema, bytes, from_ms, to_ms, predicates, &[], included)?
    {
        return Ok(Some(total));
    }
    count_one_bool_equality(schema, bytes, from_ms, to_ms, predicates, &[], included)
}

/// One string or text equality, and nothing else, counted from a constant or
/// one dictionary-code period.
///
/// `Ok(None)` means the row walk still has to run: a second predicate, a
/// numeric range, a value that is not one string, a null in the column, a
/// full code vector, raw strings, or a timestamp column this header cannot
/// prove sits entirely inside the range.
fn count_one_string_equality(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    included: Option<&[bool]>,
) -> Result<Option<u64>> {
    if !ranges.is_empty() || predicates.len() != 1 {
        return Ok(None);
    }
    let predicate = &predicates[0];
    let [Scalar::Str(needle)] = predicate.allowed.as_slice() else {
        return Ok(None);
    };
    let Some(field) = schema.fields.get(predicate.index) else {
        return Ok(None);
    };
    if !matches!(field.ty, FieldType::String | FieldType::Text) {
        return Ok(None);
    }
    if plain_timestamp_run_included(schema, bytes, from_ms, to_ms, included)?.is_none() {
        return Ok(None);
    }
    count_constant_or_code_period(schema, bytes, predicate.index, needle, included)
}

/// Constant string (kind 3) or one period of dictionary codes (kind 4).
///
/// A null flag other than `0`, and every other string kind, returns `Ok(None)`
/// so the caller walks rows. Kind 4 counts matches in the stored period and
/// repeats that pattern. It does not allocate a code per row.
fn count_constant_or_code_period(
    schema: &Schema,
    bytes: &[u8],
    column: usize,
    needle: &str,
    included: Option<&[bool]>,
) -> Result<Option<u64>> {
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if column_is_hole(included, index) {
            if index == column {
                return Ok(None);
            }
            continue;
        }
        if column_was_not_stored(bytes, cursor) {
            return Ok(None);
        }
        if index == column {
            if !matches!(field.ty, FieldType::String | FieldType::Text) {
                return Ok(None);
            }
            let flag = *bytes
                .get(cursor)
                .ok_or_else(|| Error::corrupt("truncated null flag"))?;
            if flag != 0 {
                return Ok(None);
            }
            cursor += 1;
            let kind = read_u8(bytes, &mut cursor)?;
            // Kind 3 is one string shared by every present row.
            if kind == 3 {
                let hit = length_prefixed_equals(bytes, &mut cursor, needle)?;
                return Ok(Some(if hit { nrows as u64 } else { 0 }));
            }
            if kind == STRING_DICT_PERIOD {
                return Ok(Some(dictionary_period_matches(
                    bytes,
                    &mut cursor,
                    nrows,
                    needle,
                )?));
            }
            return Ok(None);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Ok(None)
}

fn dictionary_period_matches(
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    needle: &str,
) -> Result<u64> {
    let dict_len = read_string_dict_len(bytes, cursor)?;
    let mut hits = Vec::with_capacity(dict_len);
    for _ in 0..dict_len {
        hits.push(length_prefixed_equals(bytes, cursor, needle)?);
    }
    let width = read_dict_code_width(bytes, cursor)?;
    let period = dictionary_period(bytes, cursor, nrows)?;
    let mut pattern = Vec::with_capacity(period);
    let mut in_period = 0u64;
    for _ in 0..period {
        let code = read_one_dict_code(bytes, cursor, width, dict_len)?;
        let hit = hits[code];
        pattern.push(hit);
        if hit {
            in_period += 1;
        }
    }
    let cycles = (nrows / period) as u64;
    let remainder = nrows % period;
    let mut total = in_period
        .checked_mul(cycles)
        .ok_or_else(|| Error::event("event count overflow"))?;
    for hit in &pattern[..remainder] {
        if *hit {
            total = total
                .checked_add(1)
                .ok_or_else(|| Error::event("event count overflow"))?;
        }
    }
    Ok(total)
}

fn length_prefixed_equals(bytes: &[u8], cursor: &mut usize, needle: &str) -> Result<bool> {
    let slice = read_lp_bytes(bytes, cursor)?;
    let text = std::str::from_utf8(slice).map_err(|_| Error::corrupt("string is not utf-8"))?;
    Ok(text == needle)
}

/// One bool equality, and nothing else, counted from a constant or one bitmap period.
///
/// `Ok(None)` means the row walk still has to run: a second predicate, a
/// numeric range, a value that is not one bool, a null in the column, a full
/// per-row bitmap, or a timestamp column this header cannot prove sits
/// entirely inside the range.
fn count_one_bool_equality(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    included: Option<&[bool]>,
) -> Result<Option<u64>> {
    if !ranges.is_empty() || predicates.len() != 1 {
        return Ok(None);
    }
    let predicate = &predicates[0];
    let [Scalar::Bool(want)] = predicate.allowed.as_slice() else {
        return Ok(None);
    };
    let Some(field) = schema.fields.get(predicate.index) else {
        return Ok(None);
    };
    if field.ty != FieldType::Bool {
        return Ok(None);
    }
    if plain_timestamp_run_included(schema, bytes, from_ms, to_ms, included)?.is_none() {
        return Ok(None);
    }
    count_constant_or_bool_period(schema, bytes, predicate.index, *want, included)
}

/// Constant bool (kind 1) or one period of packed bits (kind 4).
///
/// A null flag other than `0`, and every other bool kind, returns `Ok(None)`
/// so the caller walks rows. Kind 4 counts matches in the stored period and
/// repeats that pattern. It does not allocate a bool per row.
fn count_constant_or_bool_period(
    schema: &Schema,
    bytes: &[u8],
    column: usize,
    want: bool,
    included: Option<&[bool]>,
) -> Result<Option<u64>> {
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if column_is_hole(included, index) {
            if index == column {
                return Ok(None);
            }
            continue;
        }
        if column_was_not_stored(bytes, cursor) {
            return Ok(None);
        }
        if index == column {
            if field.ty != FieldType::Bool {
                return Ok(None);
            }
            let flag = *bytes
                .get(cursor)
                .ok_or_else(|| Error::corrupt("truncated null flag"))?;
            if flag != 0 {
                return Ok(None);
            }
            cursor += 1;
            let kind = read_u8(bytes, &mut cursor)?;
            if kind == 1 {
                let value = read_u8(bytes, &mut cursor)?;
                if value > 1 {
                    return Err(Error::corrupt("bool constant is not 0 or 1"));
                }
                let hit = (value == 1) == want;
                return Ok(Some(if hit { nrows as u64 } else { 0 }));
            }
            if kind == BOOL_PERIOD {
                let (period, pattern) = read_bitmap_period(bytes, &mut cursor)?;
                return Ok(Some(bitmap_period_matches(pattern, period, nrows, want)?));
            }
            return Ok(None);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Ok(None)
}

fn bitmap_period_matches(pattern: &[u8], period: usize, nrows: usize, want: bool) -> Result<u64> {
    let mut hits = Vec::with_capacity(period);
    let mut in_period = 0u64;
    for bit in 0..period {
        let byte = *pattern
            .get(bit / 8)
            .ok_or_else(|| Error::corrupt("truncated bool bitmap"))?;
        let set = byte & (1 << (bit % 8)) != 0;
        let hit = set == want;
        hits.push(hit);
        if hit {
            in_period += 1;
        }
    }
    let cycles = (nrows / period) as u64;
    let remainder = nrows % period;
    let mut total = in_period
        .checked_mul(cycles)
        .ok_or_else(|| Error::event("event count overflow"))?;
    for hit in &hits[..remainder] {
        if *hit {
            total = total
                .checked_add(1)
                .ok_or_else(|| Error::event("event count overflow"))?;
        }
    }
    Ok(total)
}

/// `base + (count - 1) * stride` when that product does not wrap in `i64`.
fn stride_end(base: i64, stride: i64, count: usize) -> Option<i64> {
    let steps = i64::try_from(count.checked_sub(1)?).ok()?;
    base.checked_add(steps.checked_mul(stride)?)
}

fn add_timestamp_run(
    run: &TimestampRun,
    bucket_ms: i64,
    first_start: i128,
    counts: &mut [u64],
) -> Result<()> {
    let width = i128::from(bucket_ms);
    if width <= 0 {
        return Err(Error::event("bucket_ms must be a positive integer"));
    }
    let base = i128::from(run.base);
    let stride = i128::from(run.stride);
    let count = u64::try_from(run.count).map_err(|_| Error::corrupt("histogram bucket index"))?;
    let mut index = 0u64;
    while index < count {
        let ts = base + i128::from(index) * stride;
        let start = ts.div_euclid(width) * width;
        let bucket_index = (start - first_start) / width;
        let bucket_index =
            usize::try_from(bucket_index).map_err(|_| Error::corrupt("histogram bucket index"))?;
        let bucket = counts
            .get_mut(bucket_index)
            .ok_or_else(|| Error::corrupt("histogram bucket index"))?;
        let remain = count - index;
        let n = if stride == 0 {
            remain
        } else {
            let room = start + width - ts;
            if room <= 0 {
                return Err(Error::corrupt("histogram bucket index"));
            }
            let steps = (room - 1) / stride;
            u64::try_from(steps + 1).unwrap_or(u64::MAX).min(remain)
        };
        if n == 0 {
            return Err(Error::corrupt("histogram bucket index"));
        }
        *bucket = bucket
            .checked_add(n)
            .ok_or_else(|| Error::event("event count overflow"))?;
        index += n;
    }
    Ok(())
}

/// One decoded column kept for rows that survive the filters.
///
/// `values` stays empty when the column was not decoded. The timestamp fast
/// path leaves it empty and passes the timestamp to `visit` instead.
pub(crate) struct CapturedColumn {
    pub index: usize,
    pub values: Vec<Scalar>,
}

/// Call `visit` once for each payload timestamp in `from_ms..=to_ms` that matches
/// every predicate.
///
/// An empty predicate list, and no captured column other than the timestamp,
/// reads the timestamp column and nothing after it. A timestamp outside the
/// range is left out even if the sparse index min/max sit inside the range. A
/// null or undecodable timestamp returns the same
/// [`Error::Corrupt`](crate::Error) as a row decode.
///
/// Predicates decode the timestamp column, the predicate columns, and any
/// captured columns. Columns after the last of those are not read. Columns
/// between them are skipped, not turned into values. A captured float that is
/// not finite is corrupt. `visit` receives the row index and the captured
/// columns for that block.
pub(crate) fn visit_matching_timestamps(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    captures: &mut [CapturedColumn],
    visit: impl FnMut(i64, usize, &[CapturedColumn]) -> Result<()>,
) -> Result<()> {
    visit_matching_timestamps_with(
        schema,
        bytes,
        from_ms,
        to_ms,
        predicates,
        ranges,
        &[],
        captures,
        visit,
    )
}

pub(crate) fn visit_matching_timestamps_with(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
    predicates: &[ColumnPredicate],
    ranges: &[RangeFilter],
    prefixes: &[PrefixFilter],
    captures: &mut [CapturedColumn],
    mut visit: impl FnMut(i64, usize, &[CapturedColumn]) -> Result<()>,
) -> Result<()> {
    if from_ms > to_ms
        || predicates.iter().any(|pred| pred.allowed.is_empty())
        || ranges.iter().any(range_impossible)
    {
        return Ok(());
    }
    let capture_other = captures
        .iter()
        .any(|column| column.index != schema.timestamp_index);
    if predicates.is_empty() && ranges.is_empty() && prefixes.is_empty() && !capture_other {
        let timestamps = read_timestamp_column(schema, bytes)?;
        for (row, ts) in timestamps.into_iter().enumerate() {
            if ts >= from_ms && ts <= to_ms {
                visit(ts, row, captures)?;
            }
        }
        return Ok(());
    }
    let nrows = block_row_count(bytes)?;
    let last_needed = predicates
        .iter()
        .map(|pred| pred.index)
        .chain(ranges.iter().map(|range| range.index))
        .chain(prefixes.iter().map(|prefix| prefix.index))
        .chain(captures.iter().map(|column| column.index))
        .fold(schema.timestamp_index, usize::max);
    let mut cursor = 4usize;
    let mut mask = vec![true; nrows];
    let mut timestamps: Option<Vec<i64>> = None;
    let mut budget = usize::MAX;
    // Captured string and text values are owned until the visitor returns.
    // That budget is the query ceiling, shared by every such column in the block.
    let mut string_budget = crate::store::MAX_QUERY_BYTES;
    for index in 0..=last_needed {
        let field = &schema.fields[index];
        if column_was_not_stored(bytes, cursor) {
            if index <= schema.timestamp_index {
                return Err(Error::corrupt("timestamp column is missing"));
            }
            for rest in index..=last_needed {
                if let Some(predicate) = predicates.iter().find(|pred| pred.index == rest) {
                    let nulls = vec![Scalar::Null; nrows];
                    apply_eq(&mut mask, &nulls, &predicate.allowed);
                }
                if ranges.iter().any(|range| range.index == rest)
                    || prefix_at(prefixes, rest).is_some()
                {
                    mask.fill(false);
                }
            }
            for slot in captures.iter_mut() {
                if slot.index >= index && slot.values.is_empty() {
                    slot.values = vec![Scalar::Null; nrows];
                }
            }
            break;
        }
        let predicate = predicates.iter().find(|pred| pred.index == index);
        let range = ranges.iter().find(|range| range.index == index);
        let prefix = prefix_at(prefixes, index);
        let is_timestamp = index == schema.timestamp_index;
        let capture_here = captures.iter().any(|column| column.index == index);
        if is_timestamp
            || predicate.is_some()
            || range.is_some()
            || capture_here
            || prefix.is_some()
        {
            if is_timestamp && timestamp_range_misses(bytes, cursor, nrows, from_ms, to_ms)? {
                skip_column(field.ty, bytes, &mut cursor, nrows, true)?;
                mask.fill(false);
                continue;
            }
            if matches!(field.ty, FieldType::String | FieldType::Text)
                && (capture_here || predicate.is_some() || prefix.is_some())
            {
                let spans = read_text_spans(bytes, &mut cursor, nrows, false)?;
                if let Some(predicate) = predicate {
                    mask_text_spans(bytes, &spans, &mut mask, &predicate.allowed);
                }
                if let Some(prefix) = prefix {
                    mask_text_prefix(bytes, &spans, &mut mask, prefix);
                }
                if range.is_some() {
                    mask.fill(false);
                }
                if capture_here {
                    let values =
                        materialize_text_spans(bytes, &spans, &mask, &mut string_budget, false)?;
                    if let Some(slot) = captures.iter_mut().find(|column| column.index == index) {
                        slot.values = values;
                    }
                }
                continue;
            }
            if let Some(predicate) = predicate {
                if !capture_here && field.ty == FieldType::Json {
                    let spans = read_text_spans(bytes, &mut cursor, nrows, true)?;
                    mask_text_spans(bytes, &spans, &mut mask, &predicate.allowed);
                    if range.is_some() {
                        mask.fill(false);
                    }
                    continue;
                }
            }
            if (predicate.is_some() || range.is_some())
                && column_misses(field.ty, bytes, cursor, nrows, predicate, range)?
            {
                skip_column(field.ty, bytes, &mut cursor, nrows, is_timestamp)?;
                mask.fill(false);
                continue;
            }
            let values = decode_column(field.ty, bytes, &mut cursor, nrows, &mask, &mut budget)?;
            if capture_here {
                if field.ty == FieldType::Float {
                    for value in &values {
                        if let Scalar::Float(number) = value {
                            if !number.is_finite() {
                                return Err(Error::corrupt("float column is not finite"));
                            }
                        }
                    }
                }
                if let Some(slot) = captures.iter_mut().find(|column| column.index == index) {
                    slot.values = values.clone();
                }
            }
            if is_timestamp {
                let mut column = Vec::with_capacity(nrows);
                for (row, value) in values.iter().enumerate() {
                    match value {
                        Scalar::Timestamp(ts) => {
                            if mask[row] && (*ts < from_ms || *ts > to_ms) {
                                mask[row] = false;
                            }
                            column.push(*ts);
                        }
                        _ => {
                            return Err(Error::corrupt(
                                "timestamp column is null or the wrong type",
                            ))
                        }
                    }
                }
                timestamps = Some(column);
            }
            apply_constraints(&mut mask, &values, predicate, range);
            if let Some(prefix) = prefix {
                apply_prefix(&mut mask, &values, prefix);
            }
        } else {
            skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
        }
    }
    let Some(timestamps) = timestamps else {
        if mask.iter().any(|keep| *keep) {
            return Err(Error::corrupt("timestamp column is missing"));
        }
        return Ok(());
    };
    for slot in captures.iter() {
        if slot.values.is_empty() {
            if mask.iter().any(|keep| *keep) && slot.index != schema.timestamp_index {
                return Err(Error::corrupt("stat column is missing"));
            }
        } else if slot.values.len() != nrows {
            return Err(Error::corrupt("stat column width does not match the block"));
        }
    }
    for (row, ts) in timestamps.into_iter().enumerate() {
        if mask[row] {
            visit(ts, row, captures)?;
        }
    }
    Ok(())
}

/// Add one float column into `total` when every payload timestamp is inside
/// `from_ms..=to_ms` and the column stores no nulls.
///
/// The sum is sequential IEEE-754 addition of a constant, a raw f64 frame, or a
/// repeated decimal stride, in row order, continuing from the value already in
/// `total`. `Ok(Some(rows))` means those rows were added and the other columns
/// were not visited. `Ok(None)` leaves `total` unchanged: a timestamp falls
/// outside the range, the column has a null or was not stored, a value is not
/// finite, or the encoding is not one of those three. The caller keeps the row
/// visitor for that block. The sparse index is not the containment check.
pub(crate) fn try_add_dense_float(
    schema: &Schema,
    bytes: &[u8],
    column: usize,
    from_ms: i64,
    to_ms: i64,
    total: &mut f64,
) -> Result<Option<usize>> {
    if schema.fields.get(column).map(|field| field.ty) != Some(FieldType::Float) {
        return Ok(None);
    }
    let timestamps = read_timestamp_column(schema, bytes)?;
    if timestamps.iter().any(|ts| *ts < from_ms || *ts > to_ms) {
        return Ok(None);
    }
    let nrows = timestamps.len();
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if index == column {
            // A column appended after this block was written is all nulls.
            // Summing it would set `contributed`; the row visitor must apply them.
            // Bytes that start the column and then stop stay corrupt.
            if column_was_not_stored(bytes, cursor) {
                return Ok(None);
            }
            let added = add_dense_f64_values(bytes, &mut cursor, nrows, total)?;
            return Ok(added.then_some(nrows));
        }
        if column_was_not_stored(bytes, cursor) {
            return Ok(None);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Ok(None)
}

/// `true` when `count` finite values were added. `false` when this column is
/// not a dense constant, raw frame, or repeated stride.
fn add_dense_f64_values(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    total: &mut f64,
) -> Result<bool> {
    let flags = *bytes
        .get(*cursor)
        .ok_or_else(|| Error::corrupt("truncated null flag"))?;
    if flags != 0 {
        return Ok(false);
    }
    *cursor += 1;
    if count == 0 {
        expect_kind(bytes, cursor, KIND_FLOAT_EMPTY)?;
        return Ok(true);
    }
    let kind = read_u8(bytes, cursor)?;
    let mut local = *total;
    match kind {
        KIND_FLOAT_CONSTANT => {
            let value = read_f64(bytes, cursor)?;
            if !value.is_finite() {
                return Ok(false);
            }
            for _ in 0..count {
                local += value;
            }
        }
        KIND_FLOAT_RAW => {
            for _ in 0..count {
                let value = read_f64(bytes, cursor)?;
                if !value.is_finite() {
                    return Ok(false);
                }
                local += value;
            }
        }
        KIND_FLOAT_REPEATED => {
            let repeated = read_float_repeated(bytes, cursor, count)?;
            let scale = pow10(repeated.exp);
            for index in 0..count {
                let scaled = repeated
                    .base
                    .wrapping_add(((index % repeated.period) as i64).wrapping_mul(repeated.stride));
                let value = (scaled as f64) / scale;
                if !value.is_finite() {
                    return Ok(false);
                }
                local += value;
            }
        }
        _ => return Ok(false),
    }
    *total = local;
    Ok(true)
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
            out.extend_from_slice(&encode_strings(&values, true));
        }
        FieldType::Json => {
            let values = json_texts(rows, index)?;
            out.extend_from_slice(&encode_strings(&values, true));
        }
    }
    Ok(())
}

/// Null flags. `0` is no nulls. `1` is the full present bitmap. `3` is one
/// period of that bitmap: a period byte, then `ceil(period / 8)` pattern
/// bytes in the same bit order. Row `i` uses bit `i % period`.
const NULL_PERIOD: u8 = 3;
/// Bool kinds. `0` is empty, `1` is a constant, `2` is the full bitmap.
/// `4` is one period of the present values, same layout as a null period.
const BOOL_PERIOD: u8 = 4;
const MAX_BITMAP_PERIOD: usize = 32;

fn write_nulls(out: &mut Vec<u8>, nulls: &[bool]) {
    if nulls.iter().all(|is_null| !is_null) {
        out.push(0);
        return;
    }
    // Flag 1 stores a present bit, so the period is taken over those bits.
    let present: Vec<bool> = nulls.iter().map(|is_null| !is_null).collect();
    if let Some(period) = repeating_bit_period(&present) {
        out.push(NULL_PERIOD);
        out.push(period as u8);
        out.extend_from_slice(&pack_present_bits(&present[..period]));
        return;
    }
    out.push(1);
    out.extend_from_slice(&pack_present_bits(&present));
}

fn pack_present_bits(present: &[bool]) -> Vec<u8> {
    let mut bitmap = vec![0u8; present.len().div_ceil(8)];
    for (index, is_present) in present.iter().enumerate() {
        if *is_present {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    bitmap
}

/// Smallest `period` in `1..=32` such that bit `i` equals bit `i % period`,
/// and only when `2 + ceil(period / 8)` is strictly shorter than the full
/// bitmap framing `1 + ceil(len / 8)`. A longer multiple cannot be smaller.
fn repeating_bit_period(bits: &[bool]) -> Option<usize> {
    let count = bits.len();
    let full_len = 1 + count.div_ceil(8);
    if full_len <= 3 || count < 2 {
        return None;
    }
    let max_period = MAX_BITMAP_PERIOD.min(count - 1);
    for period in 1..=max_period {
        if (period..count).all(|index| bits[index] == bits[index % period]) {
            let periodic_len = 2 + period.div_ceil(8);
            if periodic_len < full_len {
                return Some(period);
            }
            return None;
        }
    }
    None
}

/// Integer kind bytes. Kinds 0–6 are the original empty, constant, and
/// byte-width frame-of-reference encodings. Kinds 7 and 8 are additive.
/// Kind 9 is a handful of constant-stride pieces (a wrapped sawtooth).
const KIND_EMPTY: u8 = 0;
const KIND_CONSTANT: u8 = 1;
const KIND_STRIDE: u8 = 7;
const KIND_BITPACK: u8 = 8;
const KIND_PIECES: u8 = 9;
const MAX_STRIDE_PIECES: usize = 8;

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
    let span = (max as u64).wrapping_sub(min as u64);
    let width = width_of(u128::from(span));
    let count = present.len();
    let mut best_len = frame_len(8, width, count);
    let mut choice = IntEncoding::Frame;
    if let Some((base, stride)) = constant_stride_i64(&present) {
        let stride_len = 1 + 8 + 8;
        if stride_len < best_len {
            best_len = stride_len;
            choice = IntEncoding::Stride {
                base: i128::from(base),
                stride: i128::from(stride),
            };
        }
    }
    let bits = bit_width_for_span(u128::from(span));
    let packed_len = bitpack_len(8, bits, count);
    if packed_len < best_len {
        best_len = packed_len;
        choice = IntEncoding::Bitpack { bits };
    }
    if let Some(pieces) = stride_pieces_i64(&present) {
        if stride_pieces_len(8, pieces.len()) < best_len {
            choice = IntEncoding::Pieces(pieces);
        }
    }
    match choice {
        IntEncoding::Frame => encode_i64s_frame(min, width, &present),
        IntEncoding::Stride { base, stride } => encode_stride_i64(base as i64, stride as i64),
        IntEncoding::Bitpack { bits } => encode_bitpack(
            &min.to_le_bytes(),
            bits,
            present
                .iter()
                .map(|value| u128::from((*value as u64).wrapping_sub(min as u64))),
        ),
        IntEncoding::Pieces(pieces) => encode_stride_pieces(&pieces, 8),
    }
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
    let span = (max as u128).wrapping_sub(min as u128);
    let width = width_of(span);
    let count = present.len();
    let mut best_len = frame_len(16, width, count);
    let mut choice = IntEncoding::Frame;
    if let Some((base, stride)) = constant_stride_i128(&present) {
        let stride_len = 1 + 16 + 16;
        if stride_len < best_len {
            best_len = stride_len;
            choice = IntEncoding::Stride { base, stride };
        }
    }
    let bits = bit_width_for_span(span);
    let packed_len = bitpack_len(16, bits, count);
    if packed_len < best_len {
        best_len = packed_len;
        choice = IntEncoding::Bitpack { bits };
    }
    if let Some(pieces) = stride_pieces_i128(&present) {
        if stride_pieces_len(16, pieces.len()) < best_len {
            choice = IntEncoding::Pieces(pieces);
        }
    }
    match choice {
        IntEncoding::Frame => encode_i128s_frame(min, width, &present),
        IntEncoding::Stride { base, stride } => encode_stride_i128(base, stride),
        IntEncoding::Bitpack { bits } => encode_bitpack(
            &min.to_le_bytes(),
            bits,
            present
                .iter()
                .map(|value| (*value as u128).wrapping_sub(min as u128)),
        ),
        IntEncoding::Pieces(pieces) => encode_stride_pieces(&pieces, 16),
    }
}

enum IntEncoding {
    Frame,
    Stride { base: i128, stride: i128 },
    Bitpack { bits: u32 },
    Pieces(Vec<StridePiece>),
}

struct StridePiece {
    start: u32,
    base: i128,
    stride: i128,
}

fn frame_len(base_len: usize, width: usize, count: usize) -> usize {
    1 + base_len + width * count
}

fn encode_i64s_frame(min: i64, width: usize, present: &[i64]) -> Vec<u8> {
    let mut out = vec![kind_for_width(width)];
    out.extend_from_slice(&min.to_le_bytes());
    for value in present {
        let delta = (*value as u64).wrapping_sub(min as u64);
        write_uint(&mut out, u128::from(delta), width);
    }
    out
}

fn encode_i128s_frame(min: i128, width: usize, present: &[i128]) -> Vec<u8> {
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

/// Partition present values into at most eight constant-stride runs.
/// One run is kind 7, and a zero stride is the constant kind, so neither is
/// returned here. A run of equal values becomes single-value pieces; more
/// than eight pieces is not this encoding.
fn stride_pieces_i64(present: &[i64]) -> Option<Vec<StridePiece>> {
    stride_pieces(
        present,
        |value| i128::from(value),
        |base, offset, stride| {
            let base = base as i64;
            let stride = stride as i64;
            let offset = offset as i64;
            i128::from(base.wrapping_add(offset.wrapping_mul(stride)))
        },
    )
}

fn stride_pieces_i128(present: &[i128]) -> Option<Vec<StridePiece>> {
    stride_pieces(
        present,
        |value| value,
        |base, offset, stride| base.wrapping_add(offset.wrapping_mul(stride)),
    )
}

fn stride_pieces<T: Copy>(
    present: &[T],
    widen: impl Fn(T) -> i128,
    step: impl Fn(i128, i128, i128) -> i128,
) -> Option<Vec<StridePiece>> {
    if present.len() < 2 {
        return None;
    }
    let mut pieces = Vec::new();
    let mut index = 0usize;
    while index < present.len() {
        if pieces.len() == MAX_STRIDE_PIECES {
            return None;
        }
        let start = u32::try_from(index).ok()?;
        let base = widen(present[index]);
        if index + 1 == present.len() {
            pieces.push(StridePiece {
                start,
                base,
                stride: 1,
            });
            break;
        }
        let delta = widen(present[index + 1]).wrapping_sub(base);
        if delta == 0 {
            pieces.push(StridePiece {
                start,
                base,
                stride: 1,
            });
            index += 1;
            continue;
        }
        index += 2;
        while index < present.len() {
            let offset = i128::try_from(index - start as usize).ok()?;
            if widen(present[index]) != step(base, offset, delta) {
                break;
            }
            index += 1;
        }
        pieces.push(StridePiece {
            start,
            base,
            stride: delta,
        });
    }
    if pieces.len() < 2 {
        None
    } else {
        Some(pieces)
    }
}

fn stride_pieces_len(value_len: usize, count: usize) -> usize {
    1 + 2 + count * (4 + value_len * 2)
}

fn encode_stride_pieces(pieces: &[StridePiece], value_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(stride_pieces_len(value_len, pieces.len()));
    out.push(KIND_PIECES);
    out.extend_from_slice(&(pieces.len() as u16).to_le_bytes());
    for piece in pieces {
        out.extend_from_slice(&piece.start.to_le_bytes());
        if value_len == 8 {
            out.extend_from_slice(&(piece.base as i64).to_le_bytes());
            out.extend_from_slice(&(piece.stride as i64).to_le_bytes());
        } else {
            out.extend_from_slice(&piece.base.to_le_bytes());
            out.extend_from_slice(&piece.stride.to_le_bytes());
        }
    }
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

/// Float kinds. 0–2 are the original empty, constant, and raw f64 frame.
/// Kind 3 is one decimal exponent whose scaled integers repeat
/// `base + (i % period) * stride`.
const KIND_FLOAT_EMPTY: u8 = 0;
const KIND_FLOAT_CONSTANT: u8 = 1;
const KIND_FLOAT_RAW: u8 = 2;
const KIND_FLOAT_REPEATED: u8 = 3;
const FLOAT_REPEATED_LEN: usize = 1 + 1 + 8 + 8 + 4;

fn encode_f64s(values: &[Option<f64>]) -> Vec<u8> {
    let present: Vec<f64> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![KIND_FLOAT_EMPTY];
    }
    let first = present[0].to_bits();
    if present.iter().all(|value| value.to_bits() == first) {
        let mut out = vec![KIND_FLOAT_CONSTANT];
        out.extend_from_slice(&present[0].to_le_bytes());
        return out;
    }
    if let Some(repeated) = encode_repeated_decimal_stride(&present) {
        return repeated;
    }
    let mut out = Vec::with_capacity(1 + present.len() * 8);
    out.push(KIND_FLOAT_RAW);
    for value in present {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

/// `10^exp` for `exp` in `0..=18`. Each step is exact in f64.
fn pow10(exp: u8) -> f64 {
    let mut value = 1.0f64;
    for _ in 0..exp {
        value *= 10.0;
    }
    value
}

struct FloatRepeated {
    exp: u8,
    base: i64,
    stride: i64,
    period: usize,
}

/// One arithmetic period, repeated. The first delta is the stride. The period
/// is the first index that breaks `base + i * stride`. Later values must be
/// `base + (i % period) * stride`, and every present value must round-trip
/// through that exponent bit-exactly. A constant stays kind 1.
fn encode_repeated_decimal_stride(present: &[f64]) -> Option<Vec<u8>> {
    let count = present.len();
    if count < 3 || FLOAT_REPEATED_LEN >= 1 + count * 8 {
        return None;
    }
    if present.iter().any(|value| !value.is_finite()) {
        return None;
    }
    for exp in 0..=18u8 {
        let scale = pow10(exp);
        let mut scaled = Vec::with_capacity(count);
        let mut fits = true;
        for value in present {
            let n = (*value * scale).round() as i64;
            let back = (n as f64) / scale;
            if back.to_bits() != value.to_bits() {
                fits = false;
                break;
            }
            scaled.push(n);
        }
        if !fits {
            continue;
        }
        let base = scaled[0];
        let stride = scaled[1].wrapping_sub(base);
        let mut period = count;
        for index in 2..count {
            let expected = base.wrapping_add((index as i64).wrapping_mul(stride));
            if scaled[index] != expected {
                period = index;
                break;
            }
        }
        if period < 2 || period >= count {
            continue;
        }
        let mut matches = true;
        for index in period..count {
            let expected = base.wrapping_add(((index % period) as i64).wrapping_mul(stride));
            if scaled[index] != expected {
                matches = false;
                break;
            }
        }
        if !matches {
            continue;
        }
        let mut out = Vec::with_capacity(FLOAT_REPEATED_LEN);
        out.push(KIND_FLOAT_REPEATED);
        out.push(exp);
        out.extend_from_slice(&base.to_le_bytes());
        out.extend_from_slice(&stride.to_le_bytes());
        out.extend_from_slice(&(period as u32).to_le_bytes());
        return Some(out);
    }
    None
}

fn encode_bools(values: &[Option<bool>]) -> Vec<u8> {
    let present: Vec<bool> = values.iter().copied().flatten().collect();
    if present.is_empty() {
        return vec![0];
    }
    if present.iter().all(|value| *value == present[0]) {
        return vec![1, u8::from(present[0])];
    }
    if let Some(period) = repeating_bit_period(&present) {
        let mut out = vec![BOOL_PERIOD, period as u8];
        out.extend_from_slice(&pack_present_bits(&present[..period]));
        return out;
    }
    let mut out = vec![2];
    out.extend_from_slice(&pack_present_bits(&present));
    out
}

/// String bodies. Kind 1 is a dictionary plus one code per present value.
/// Kind 4 is that same dictionary plus one period of codes, expanded with
/// `code[i] = code[i % p]` on read. Kinds 0, 2, and 3 are unchanged.
const STRING_DICT: u8 = 1;
const STRING_DICT_PERIOD: u8 = 4;

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
    let (dict, codes, width) = build_string_dictionary(&present);
    let full = write_string_dictionary(STRING_DICT, &dict, width, &codes);
    // The dictionary has to beat raw strings on its own. A periodic code
    // vector never rescues a dictionary that lost that comparison.
    if full.len() >= raw.len() {
        return raw;
    }
    if let Some(period) = repeating_code_period(&codes) {
        let periodic = write_string_dictionary(STRING_DICT_PERIOD, &dict, width, &codes[..period]);
        if periodic.len() < full.len() {
            return periodic;
        }
    }
    full
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

#[cfg(test)]
fn encode_dict_strings(present: &[&str]) -> Vec<u8> {
    let (dict, codes, width) = build_string_dictionary(present);
    write_string_dictionary(STRING_DICT, &dict, width, &codes)
}

fn build_string_dictionary<'a>(present: &[&'a str]) -> (Vec<&'a str>, Vec<u32>, usize) {
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
    (dict, codes, width)
}

fn write_string_dictionary(kind: u8, dict: &[&str], width: usize, codes: &[u32]) -> Vec<u8> {
    let mut out = vec![kind];
    write_varint(&mut out, dict.len() as u64);
    for text in dict {
        write_varint(&mut out, text.len() as u64);
        out.extend_from_slice(text.as_bytes());
    }
    out.push(width as u8);
    if kind == STRING_DICT_PERIOD {
        write_varint(&mut out, codes.len() as u64);
    }
    for code in codes {
        write_uint(&mut out, u128::from(*code), width);
    }
    out
}

/// Smallest `p` in `2..codes.len()` such that `codes[i] == codes[i % p]` for
/// every index. The scan follows the sequence against its prefix and shortens
/// that prefix when they disagree. `n - border` is the candidate period, and
/// it is accepted only when the rest of the column matches.
fn repeating_code_period(codes: &[u32]) -> Option<usize> {
    let n = codes.len();
    if n < 3 {
        return None;
    }
    let mut border = vec![0usize; n];
    let mut matched = 0usize;
    for i in 1..n {
        while matched > 0 && codes[i] != codes[matched] {
            matched = border[matched - 1];
        }
        if codes[i] == codes[matched] {
            matched += 1;
        }
        border[i] = matched;
    }
    let period = n - border[n - 1];
    if period < 2 || period >= n {
        return None;
    }
    if (period..n).any(|index| codes[index] != codes[index % period]) {
        return None;
    }
    Some(period)
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

/// Bit length of `span`, which is enough for every delta in `0..=span`.
/// `u128::MAX` is already 128. A zero span is the constant kind and is not packed.
fn bit_width_for_span(span: u128) -> u32 {
    u128::BITS - span.leading_zeros()
}

#[derive(Default)]
struct BitPacker {
    out: Vec<u8>,
    acc: u128,
    acc_bits: u32,
}

impl BitPacker {
    fn push(&mut self, value: u128, width: u32) {
        debug_assert!(
            width >= 128 || value >> width == 0,
            "delta {value} does not fit in {width} bits"
        );
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

pub(crate) fn read_timestamp_column(schema: &Schema, bytes: &[u8]) -> Result<Vec<i64>> {
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
        // Columns before the event timestamp may themselves be nullable timestamps.
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Err(Error::corrupt("timestamp column is missing"))
}

/// Minimum, maximum, and whether every timestamp sits in `from_ms..=to_ms`.
///
/// `None` is an empty block. A constant, a non-wrapping stride, or non-wrapping
/// stride pieces answer from the encoded endpoints. Every other encoding
/// expands the column. A null timestamp is the same corrupt error as a row decode.
pub(crate) struct TimestampSpan {
    pub min: i64,
    pub max: i64,
    pub inside: bool,
}

pub(crate) fn timestamp_column_span(
    schema: &Schema,
    bytes: &[u8],
    from_ms: i64,
    to_ms: i64,
) -> Result<Option<TimestampSpan>> {
    let nrows = block_row_count(bytes)?;
    if nrows == 0 {
        return Ok(None);
    }
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if index == schema.timestamp_index {
            if field.ty != FieldType::Timestamp {
                return Err(Error::corrupt("timestamp column has the wrong type"));
            }
            let flags = *bytes
                .get(cursor)
                .ok_or_else(|| Error::corrupt("truncated null flag"))?;
            if flags != 0 {
                let timestamps = read_timestamp_column(schema, bytes)?;
                return Ok(Some(span_from_timestamps(&timestamps, from_ms, to_ms)?));
            }
            cursor += 1;
            let span = i64_encoded_span(bytes, &mut cursor, nrows)?;
            return Ok(Some(TimestampSpan {
                inside: span.min >= from_ms && span.max <= to_ms,
                min: span.min,
                max: span.max,
            }));
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Err(Error::corrupt("timestamp column is missing"))
}

/// Add one null-free int, timestamp, or decimal column into `total`.
///
/// `Ok(None)` leaves `total` unchanged: the column is missing, has a null, or
/// is bit-packed or a frame of reference. `Ok(Some(true))` means one or more
/// values were added. A constant whose product fits is one add of `value * n`.
/// When that product does not fit, the same value is added once per row. A
/// stride or a stride piece is the wrapped steps in row order, the same
/// arithmetic [`decode_i64s`] uses. Overflow is the event error from adding
/// those values one by one.
pub(crate) fn try_accumulate_exact_sum(
    schema: &Schema,
    bytes: &[u8],
    column: usize,
    name: &str,
    total: &mut i128,
) -> Result<Option<bool>> {
    let nrows = block_row_count(bytes)?;
    if nrows == 0 {
        return Ok(Some(false));
    }
    let Some(field) = schema.fields.get(column) else {
        return Err(Error::corrupt("stat column is missing"));
    };
    let width = match field.ty {
        FieldType::Int | FieldType::Timestamp => 8,
        FieldType::Decimal { .. } => 16,
        _ => return Ok(None),
    };
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if column_was_not_stored(bytes, cursor) {
            return Ok(None);
        }
        if index == column {
            let flags = *bytes
                .get(cursor)
                .ok_or_else(|| Error::corrupt("truncated null flag"))?;
            if flags != 0 {
                return Ok(None);
            }
            cursor += 1;
            return accumulate_integer_body(bytes, &mut cursor, nrows, width, name, total);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Ok(None)
}

/// One inclusive run of integers, each repeated `mult` times.
///
/// A constant is a single integer whose multiplicity is the row count. A unit
/// stride is one integer per row (`mult` is 1) from `start` through `end`.
#[derive(Clone, Copy)]
pub(crate) struct UnitRun {
    pub start: i128,
    pub end: i128,
    pub mult: u64,
}

/// Null-free constant, stride, or stride pieces whose stride is -1, 0, or 1
/// and whose values do not wrap.
///
/// `Ok(None)` leaves the caller on the row path: a null, a missing column,
/// bit packing, a frame of reference, any other stride, or wrapping arithmetic.
/// `Ok(Some(runs))` is every present value, in pieces, without expanding a row.
/// A truncated body is corrupt. The timestamp containment check is the caller's.
/// Distinct count uses each run as an inclusive interval and ignores `mult`.
/// A stride of 2 from 0 to 2 is not a unit run.
pub(crate) fn try_unit_stride_runs(
    schema: &Schema,
    bytes: &[u8],
    column: usize,
) -> Result<Option<Vec<UnitRun>>> {
    let nrows = block_row_count(bytes)?;
    if nrows == 0 {
        return Ok(Some(Vec::new()));
    }
    let Some(field) = schema.fields.get(column) else {
        return Err(Error::corrupt("quantile column is missing"));
    };
    let narrow = match field.ty {
        FieldType::Int | FieldType::Timestamp => true,
        FieldType::Decimal { .. } => false,
        _ => return Ok(None),
    };
    let width = if narrow { 8 } else { 16 };
    let mut cursor = 4usize;
    for (index, field) in schema.fields.iter().enumerate() {
        if column_was_not_stored(bytes, cursor) {
            return Ok(None);
        }
        if index == column {
            let flags = *bytes
                .get(cursor)
                .ok_or_else(|| Error::corrupt("truncated null flag"))?;
            if flags != 0 {
                return Ok(None);
            }
            cursor += 1;
            return read_unit_runs(bytes, &mut cursor, nrows, width, narrow);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Ok(None)
}

fn read_unit_runs(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    width: usize,
    narrow: bool,
) -> Result<Option<Vec<UnitRun>>> {
    if count == 0 {
        expect_kind(bytes, cursor, KIND_EMPTY)?;
        return Ok(Some(Vec::new()));
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let base = if width == 8 {
            i128::from(read_i64(bytes, cursor)?)
        } else {
            read_i128(bytes, cursor)?
        };
        let mut runs = Vec::new();
        if push_unit_run(&mut runs, base, 0, count, narrow).is_none() {
            return Ok(None);
        }
        return Ok(Some(runs));
    }
    if kind == KIND_STRIDE {
        let (base, stride) = if width == 8 {
            (
                i128::from(read_i64(bytes, cursor)?),
                i128::from(read_i64(bytes, cursor)?),
            )
        } else {
            (read_i128(bytes, cursor)?, read_i128(bytes, cursor)?)
        };
        let mut runs = Vec::new();
        if push_unit_run(&mut runs, base, stride, count, narrow).is_none() {
            return Ok(None);
        }
        return Ok(Some(runs));
    }
    if kind == KIND_PIECES {
        let pieces = read_stride_pieces(bytes, cursor, count, width)?;
        let mut runs = Vec::with_capacity(pieces.len());
        for (index, piece) in pieces.iter().enumerate() {
            let end = pieces
                .get(index + 1)
                .map(|next| next.start as usize)
                .unwrap_or(count);
            let len = end - piece.start as usize;
            if push_unit_run(&mut runs, piece.base, piece.stride, len, narrow).is_none() {
                return Ok(None);
            }
        }
        return Ok(Some(runs));
    }
    Ok(None)
}

/// `None` when `stride` is not -1, 0, or 1, or `base + (len - 1) * stride` wraps.
fn push_unit_run(
    runs: &mut Vec<UnitRun>,
    base: i128,
    stride: i128,
    len: usize,
    narrow: bool,
) -> Option<()> {
    if len == 0 || (stride != -1 && stride != 0 && stride != 1) {
        return None;
    }
    if narrow && (i64::try_from(base).is_err() || i64::try_from(stride).is_err()) {
        return None;
    }
    let (start, end, mult) = if stride == 0 || len == 1 {
        (base, base, u64::try_from(len).ok()?)
    } else {
        let steps = i128::try_from(len - 1).ok()?;
        let end = base.checked_add(stride.checked_mul(steps)?)?;
        if narrow && i64::try_from(end).is_err() {
            return None;
        }
        if stride > 0 {
            (base, end, 1)
        } else {
            (end, base, 1)
        }
    };
    runs.push(UnitRun { start, end, mult });
    Some(())
}

fn sum_overflow(name: &str) -> Error {
    Error::event(format!("sum of `{name}` overflowed"))
}

fn accumulate_integer_body(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    width: usize,
    name: &str,
    total: &mut i128,
) -> Result<Option<bool>> {
    if count == 0 {
        expect_kind(bytes, cursor, KIND_EMPTY)?;
        return Ok(Some(false));
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let value = if width == 8 {
            i128::from(read_i64(bytes, cursor)?)
        } else {
            read_i128(bytes, cursor)?
        };
        let n = i128::try_from(count).map_err(|_| Error::corrupt("column is too long to sum"))?;
        if let Some(product) = value.checked_mul(n) {
            *total = total
                .checked_add(product)
                .ok_or_else(|| sum_overflow(name))?;
        } else {
            for _ in 0..count {
                *total = total.checked_add(value).ok_or_else(|| sum_overflow(name))?;
            }
        }
        return Ok(Some(true));
    }
    if kind == KIND_STRIDE {
        if width == 8 {
            let base = read_i64(bytes, cursor)?;
            let stride = read_i64(bytes, cursor)?;
            add_i64_run(total, base, stride, count, name)?;
        } else {
            let base = read_i128(bytes, cursor)?;
            let stride = read_i128(bytes, cursor)?;
            add_i128_run(total, base, stride, count, name)?;
        }
        return Ok(Some(true));
    }
    if kind == KIND_PIECES {
        let pieces = read_stride_pieces(bytes, cursor, count, width)?;
        for (index, piece) in pieces.iter().enumerate() {
            let end = pieces
                .get(index + 1)
                .map(|next| next.start as usize)
                .unwrap_or(count);
            let len = end - piece.start as usize;
            if width == 8 {
                add_i64_run(total, piece.base as i64, piece.stride as i64, len, name)?;
            } else {
                add_i128_run(total, piece.base, piece.stride, len, name)?;
            }
        }
        return Ok(Some(count > 0));
    }
    Ok(None)
}

/// Sum of a non-wrapping i64 run. `None` when the steps wrap, so the caller
/// adds the wrapped values instead.
fn i64_series_sum(base: i64, stride: i64, count: usize) -> Option<i128> {
    if count == 0 {
        return Some(0);
    }
    stride_span(base, stride, count)?;
    let n = i128::from(u32::try_from(count).ok()?);
    let pairs = n * (n - 1) / 2;
    Some(i128::from(base) * n + i128::from(stride) * pairs)
}

/// True when every partial sum of `count` i64 values stays in range if the
/// final sum does. The slack is the widest those values can swing.
fn i64_sum_headroom(total: i128, count: usize) -> bool {
    let Ok(count) = u32::try_from(count) else {
        return false;
    };
    let slack = i128::from(count) * i128::from(i64::MAX);
    total.checked_add(slack).is_some() && total.checked_sub(slack).is_some()
}

fn add_i64_run(total: &mut i128, base: i64, stride: i64, count: usize, name: &str) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    if i64_sum_headroom(*total, count) {
        if let Some(sum) = i64_series_sum(base, stride, count) {
            *total = total.checked_add(sum).ok_or_else(|| sum_overflow(name))?;
            return Ok(());
        }
    }
    for index in 0..count {
        let value = i128::from(base.wrapping_add((index as i64).wrapping_mul(stride)));
        *total = total.checked_add(value).ok_or_else(|| sum_overflow(name))?;
    }
    Ok(())
}

fn add_i128_run(
    total: &mut i128,
    base: i128,
    stride: i128,
    count: usize,
    name: &str,
) -> Result<()> {
    for index in 0..count {
        let value = base.wrapping_add((index as i128).wrapping_mul(stride));
        *total = total.checked_add(value).ok_or_else(|| sum_overflow(name))?;
    }
    Ok(())
}

struct I64Span {
    min: i64,
    max: i64,
}

fn span_from_timestamps(timestamps: &[i64], from_ms: i64, to_ms: i64) -> Result<TimestampSpan> {
    let (min, max) = i64_minmax(timestamps)
        .ok_or_else(|| Error::corrupt("timestamp column is missing a row"))?;
    Ok(TimestampSpan {
        inside: min >= from_ms && max <= to_ms,
        min,
        max,
    })
}

fn i64_minmax(values: &[i64]) -> Option<(i64, i64)> {
    let mut iter = values.iter().copied();
    let first = iter.next()?;
    let mut min = first;
    let mut max = first;
    for value in iter {
        min = min.min(value);
        max = max.max(value);
    }
    Some((min, max))
}

/// Extremes of an i64 column body. A wrapping stride expands, so the result
/// matches [`decode_i64s`].
fn i64_encoded_span(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<I64Span> {
    if count == 0 {
        expect_kind(bytes, cursor, KIND_EMPTY)?;
        return Err(Error::corrupt("timestamp column is missing a row"));
    }
    let kind = read_u8(bytes, cursor)?;
    if kind == KIND_CONSTANT {
        let base = read_i64(bytes, cursor)?;
        return Ok(I64Span {
            min: base,
            max: base,
        });
    }
    if kind == KIND_STRIDE {
        let base = read_i64(bytes, cursor)?;
        let stride = read_i64(bytes, cursor)?;
        if let Some(span) = stride_span(base, stride, count) {
            return Ok(span);
        }
        let values = materialize_wrapping_stride(base, stride, count);
        return span_from_values(&values);
    }
    if kind == KIND_PIECES {
        let pieces = read_stride_pieces(bytes, cursor, count, 8)?;
        if let Some(span) = piece_span(&pieces, count) {
            return Ok(span);
        }
        return span_from_values(&materialize_i64_pieces(&pieces, count));
    }
    *cursor -= 1;
    let values = decode_i64s(bytes, cursor, count)?;
    span_from_values(&values)
}

fn span_from_values(values: &[i64]) -> Result<I64Span> {
    let (min, max) =
        i64_minmax(values).ok_or_else(|| Error::corrupt("timestamp column is missing a row"))?;
    Ok(I64Span { min, max })
}

fn stride_span(base: i64, stride: i64, count: usize) -> Option<I64Span> {
    if count == 0 {
        return None;
    }
    if count == 1 {
        return Some(I64Span {
            min: base,
            max: base,
        });
    }
    let offset = (count as i64 - 1).checked_mul(stride)?;
    let end = base.checked_add(offset)?;
    Some(I64Span {
        min: base.min(end),
        max: base.max(end),
    })
}

fn piece_span(pieces: &[StridePiece], count: usize) -> Option<I64Span> {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for (index, piece) in pieces.iter().enumerate() {
        let end = pieces
            .get(index + 1)
            .map(|next| next.start as usize)
            .unwrap_or(count);
        let len = end - piece.start as usize;
        let base = i64::try_from(piece.base).ok()?;
        let stride = i64::try_from(piece.stride).ok()?;
        let span = stride_span(base, stride, len)?;
        min = min.min(span.min);
        max = max.max(span.max);
    }
    if min > max {
        return None;
    }
    Some(I64Span { min, max })
}

fn materialize_wrapping_stride(base: i64, stride: i64, count: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        out.push(base.wrapping_add((index as i64).wrapping_mul(stride)));
    }
    out
}

/// [`read_timestamp_column`] for a sliced payload that left holes.
///
/// A false `included` column has no bytes. The timestamp itself has to be set.
pub(crate) fn read_timestamp_column_included(
    schema: &Schema,
    bytes: &[u8],
    included: &[bool],
) -> Result<Vec<i64>> {
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4;
    for (index, field) in schema.fields.iter().enumerate() {
        if !included.get(index).copied().unwrap_or(false) {
            if index == schema.timestamp_index {
                return Err(Error::corrupt("timestamp column is missing"));
            }
            continue;
        }
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
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Err(Error::corrupt("timestamp column is missing"))
}

/// True when `cursor` is at the end of a block, so this column was not stored.
fn column_was_not_stored(bytes: &[u8], cursor: usize) -> bool {
    cursor == bytes.len()
}

/// A sliced read left this column out. Its encoding is not in the buffer.
fn column_is_hole(included: Option<&[bool]>, index: usize) -> bool {
    included.is_some_and(|included| !included.get(index).copied().unwrap_or(false))
}

/// Whether `bytes` still holds column `index`.
///
/// A timestamp prefix of a sliced block ends on a column boundary. A column
/// past that boundary is absent. A single frame holds every column, so a later
/// field is present even when the caller asked only for the timestamp.
pub(crate) fn image_contains_column(schema: &Schema, bytes: &[u8], index: usize) -> Result<bool> {
    let nrows = block_row_count(bytes)?;
    let mut cursor = 4usize;
    for (field_index, field) in schema.fields.iter().enumerate() {
        if column_was_not_stored(bytes, cursor) {
            return Ok(false);
        }
        if field_index == index {
            return Ok(true);
        }
        skip_column(field.ty, bytes, &mut cursor, nrows, false)?;
    }
    Ok(false)
}

fn skip_column(
    ty: FieldType,
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    reject_null_timestamp: bool,
) -> Result<()> {
    if column_was_not_stored(bytes, *cursor) {
        if reject_null_timestamp {
            return Err(Error::corrupt("timestamp column is missing"));
        }
        return Ok(());
    }
    let present = read_present(bytes, cursor, nrows)?;
    let present_count = match &present {
        None => nrows,
        Some(flags) => flags.iter().filter(|flag| **flag).count(),
    };
    match ty {
        FieldType::Int => skip_i64s(bytes, cursor, present_count),
        FieldType::Timestamp => {
            if reject_null_timestamp
                && present
                    .as_ref()
                    .is_some_and(|flags| flags.iter().any(|flag| !*flag))
            {
                return Err(Error::corrupt("timestamp column is null or the wrong type"));
            }
            skip_i64s(bytes, cursor, present_count)
        }
        FieldType::Float => skip_f64s(bytes, cursor, present_count),
        FieldType::Bool => skip_bools(bytes, cursor, present_count),
        FieldType::Decimal { .. } => skip_i128s(bytes, cursor, present_count),
        FieldType::String | FieldType::Text => skip_strings(bytes, cursor, present_count, false),
        FieldType::Json => skip_strings(bytes, cursor, present_count, true),
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
    if kind == KIND_PIECES {
        let _ = read_stride_pieces(bytes, cursor, count, 8)?;
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
    if kind == KIND_PIECES {
        let _ = read_stride_pieces(bytes, cursor, count, 16)?;
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
        return expect_kind(bytes, cursor, KIND_FLOAT_EMPTY);
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        KIND_FLOAT_CONSTANT => {
            let _ = read_f64(bytes, cursor)?;
            Ok(())
        }
        KIND_FLOAT_RAW => {
            let _ = read_exact(bytes, cursor, count.saturating_mul(8))?;
            Ok(())
        }
        KIND_FLOAT_REPEATED => {
            let _ = read_float_repeated(bytes, cursor, count)?;
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
            let value = read_u8(bytes, cursor)?;
            if value > 1 {
                return Err(Error::corrupt("bool constant is not 0 or 1"));
            }
            Ok(())
        }
        2 => {
            let _ = read_exact(bytes, cursor, count.div_ceil(8))?;
            Ok(())
        }
        BOOL_PERIOD => {
            let _ = read_bitmap_period(bytes, cursor)?;
            Ok(())
        }
        _ => Err(Error::corrupt(format!("unknown bool encoding {kind}"))),
    }
}

fn read_string_dict_len(bytes: &[u8], cursor: &mut usize) -> Result<usize> {
    let dict_len = read_varint(bytes, cursor)? as usize;
    if dict_len == 0 {
        return Err(Error::corrupt("string dictionary is empty"));
    }
    Ok(dict_len)
}

fn read_dict_code_width(bytes: &[u8], cursor: &mut usize) -> Result<usize> {
    let width = read_u8(bytes, cursor)? as usize;
    if !matches!(width, 1 | 2 | 4) {
        return Err(Error::corrupt("string dictionary code width is invalid"));
    }
    Ok(width)
}

fn read_one_dict_code(
    bytes: &[u8],
    cursor: &mut usize,
    width: usize,
    dict_len: usize,
) -> Result<usize> {
    let code = read_uint(bytes, cursor, width)? as usize;
    if code >= dict_len {
        return Err(Error::corrupt("string dictionary code is out of range"));
    }
    Ok(code)
}

fn skip_dict_codes(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    dict_len: usize,
    periodic: bool,
) -> Result<()> {
    let width = read_dict_code_width(bytes, cursor)?;
    let stored = if periodic {
        dictionary_period(bytes, cursor, count)?
    } else {
        count
    };
    for _ in 0..stored {
        let _ = read_one_dict_code(bytes, cursor, width, dict_len)?;
    }
    Ok(())
}

fn read_dict_codes(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    dict_len: usize,
    periodic: bool,
) -> Result<Vec<usize>> {
    let width = read_dict_code_width(bytes, cursor)?;
    if !periodic {
        let mut codes = Vec::with_capacity(count);
        for _ in 0..count {
            codes.push(read_one_dict_code(bytes, cursor, width, dict_len)?);
        }
        return Ok(codes);
    }
    let period = dictionary_period(bytes, cursor, count)?;
    let mut prefix = Vec::with_capacity(period);
    for _ in 0..period {
        prefix.push(read_one_dict_code(bytes, cursor, width, dict_len)?);
    }
    let mut codes = Vec::with_capacity(count);
    for index in 0..count {
        codes.push(prefix[index % period]);
    }
    Ok(codes)
}

fn dictionary_period(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<usize> {
    let period = read_varint(bytes, cursor)?;
    if period < 2 || period >= count as u64 {
        return Err(Error::corrupt("string dictionary period is invalid"));
    }
    Ok(period as usize)
}

fn skip_strings(bytes: &[u8], cursor: &mut usize, count: usize, validate_json: bool) -> Result<()> {
    if count == 0 {
        return expect_kind(bytes, cursor, 0);
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => skip_lp_string_value(bytes, cursor, validate_json),
        2 => {
            for _ in 0..count {
                skip_lp_string_value(bytes, cursor, validate_json)?;
            }
            Ok(())
        }
        1 | 4 => {
            let dict_len = read_string_dict_len(bytes, cursor)?;
            for _ in 0..dict_len {
                skip_lp_string_value(bytes, cursor, validate_json)?;
            }
            skip_dict_codes(bytes, cursor, count, dict_len, kind == STRING_DICT_PERIOD)?;
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
        NULL_PERIOD => Ok(Some(expand_bitmap_period(bytes, cursor, nrows)?)),
        _ => Err(Error::corrupt(format!("unknown null flag {flags}"))),
    }
}

fn read_bitmap_period<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<(usize, &'a [u8])> {
    let period = read_u8(bytes, cursor)? as usize;
    if period == 0 || period > MAX_BITMAP_PERIOD {
        return Err(Error::corrupt("bitmap period is invalid"));
    }
    let pattern = read_exact(bytes, cursor, period.div_ceil(8))?;
    Ok((period, pattern))
}

fn expand_bitmap_period(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<Vec<bool>> {
    let (period, pattern) = read_bitmap_period(bytes, cursor)?;
    let mut out = Vec::with_capacity(count);
    for index in 0..count {
        let bit = index % period;
        out.push(pattern[bit / 8] & (1 << (bit % 8)) != 0);
    }
    Ok(out)
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
    if kind == KIND_PIECES {
        let pieces = read_stride_pieces(bytes, cursor, count, 8)?;
        return Ok(materialize_i64_pieces(&pieces, count));
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
    if kind == KIND_PIECES {
        let pieces = read_stride_pieces(bytes, cursor, count, 16)?;
        return Ok(materialize_i128_pieces(&pieces, count));
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
        expect_kind(bytes, cursor, KIND_FLOAT_EMPTY)?;
        return Ok(Vec::new());
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        KIND_FLOAT_CONSTANT => {
            let value = read_f64(bytes, cursor)?;
            Ok(vec![value; count])
        }
        KIND_FLOAT_RAW => {
            let mut out = Vec::with_capacity(count);
            for _ in 0..count {
                out.push(read_f64(bytes, cursor)?);
            }
            Ok(out)
        }
        KIND_FLOAT_REPEATED => {
            let repeated = read_float_repeated(bytes, cursor, count)?;
            let scale = pow10(repeated.exp);
            let mut out = Vec::with_capacity(count);
            for index in 0..count {
                let scaled = repeated
                    .base
                    .wrapping_add(((index % repeated.period) as i64).wrapping_mul(repeated.stride));
                out.push((scaled as f64) / scale);
            }
            Ok(out)
        }
        _ => Err(Error::corrupt(format!("unknown float encoding {kind}"))),
    }
}

fn read_float_repeated(bytes: &[u8], cursor: &mut usize, count: usize) -> Result<FloatRepeated> {
    let exp = read_u8(bytes, cursor)?;
    if exp > 18 {
        return Err(Error::corrupt("float stride exponent is invalid"));
    }
    let base = read_i64(bytes, cursor)?;
    let stride = read_i64(bytes, cursor)?;
    let raw = read_exact(bytes, cursor, 4)?;
    let period = u32::from_le_bytes(raw.try_into().unwrap()) as usize;
    if period < 2 || period >= count {
        return Err(Error::corrupt("float stride period is invalid"));
    }
    Ok(FloatRepeated {
        exp,
        base,
        stride,
        period,
    })
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
        BOOL_PERIOD => expand_bitmap_period(bytes, cursor, count),
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
                    skip_lp_string_value(bytes, cursor, validate_json)?;
                    out.push(String::new());
                }
            }
            Ok(out)
        }
        1 | 4 => {
            let dict_len = read_string_dict_len(bytes, cursor)?;
            let mut dict = Vec::with_capacity(dict_len);
            for _ in 0..dict_len {
                let text = read_lp_string(bytes, cursor)?;
                if validate_json {
                    validate_json_column_text(&text)?;
                }
                dict.push(text);
            }
            let codes =
                read_dict_codes(bytes, cursor, count, dict.len(), kind == STRING_DICT_PERIOD)?;
            let mut expanded = 0usize;
            for (code, flag) in codes.iter().zip(keep.iter()) {
                if !*flag {
                    continue;
                }
                expanded = expanded
                    .checked_add(dict[*code].len())
                    .ok_or_else(|| Error::event("query response size limit exceeded"))?;
            }
            charge_string_bytes(budget, expanded)?;
            let mut out = Vec::with_capacity(count);
            for (code, flag) in codes.iter().zip(keep.iter()) {
                if *flag {
                    out.push(dict[*code].clone());
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

fn skip_lp_string_value(bytes: &[u8], cursor: &mut usize, validate_json: bool) -> Result<()> {
    let slice = read_lp_bytes(bytes, cursor)?;
    let text = std::str::from_utf8(slice).map_err(|_| Error::corrupt("string is not utf-8"))?;
    if validate_json {
        validate_json_column_text(text)?;
    }
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

fn read_stride_pieces(
    bytes: &[u8],
    cursor: &mut usize,
    count: usize,
    value_len: usize,
) -> Result<Vec<StridePiece>> {
    let raw = read_exact(bytes, cursor, 2)?;
    let piece_count = u16::from_le_bytes(raw.try_into().unwrap()) as usize;
    if piece_count == 0 || piece_count > MAX_STRIDE_PIECES {
        return Err(Error::corrupt("integer stride piece count is invalid"));
    }
    let mut pieces = Vec::with_capacity(piece_count);
    for _ in 0..piece_count {
        let raw = read_exact(bytes, cursor, 4)?;
        let start = u32::from_le_bytes(raw.try_into().unwrap());
        let (base, stride) = if value_len == 8 {
            (
                i128::from(read_i64(bytes, cursor)?),
                i128::from(read_i64(bytes, cursor)?),
            )
        } else {
            (read_i128(bytes, cursor)?, read_i128(bytes, cursor)?)
        };
        if stride == 0 {
            return Err(Error::corrupt("integer stride piece has a zero stride"));
        }
        pieces.push(StridePiece {
            start,
            base,
            stride,
        });
    }
    if pieces[0].start != 0 {
        return Err(Error::corrupt("integer stride pieces do not start at 0"));
    }
    for pair in pieces.windows(2) {
        if pair[1].start <= pair[0].start {
            return Err(Error::corrupt(
                "integer stride pieces are not strictly increasing",
            ));
        }
        if pair[1].start as usize >= count {
            return Err(Error::corrupt(
                "integer stride piece starts past the column",
            ));
        }
    }
    if pieces.last().unwrap().start as usize >= count {
        return Err(Error::corrupt(
            "integer stride piece starts past the column",
        ));
    }
    Ok(pieces)
}

fn materialize_i64_pieces(pieces: &[StridePiece], count: usize) -> Vec<i64> {
    let mut out = Vec::with_capacity(count);
    for (index, piece) in pieces.iter().enumerate() {
        let end = pieces
            .get(index + 1)
            .map(|next| next.start as usize)
            .unwrap_or(count);
        let base = piece.base as i64;
        let stride = piece.stride as i64;
        for offset in 0..(end - piece.start as usize) {
            out.push(base.wrapping_add((offset as i64).wrapping_mul(stride)));
        }
    }
    out
}

fn materialize_i128_pieces(pieces: &[StridePiece], count: usize) -> Vec<i128> {
    let mut out = Vec::with_capacity(count);
    for (index, piece) in pieces.iter().enumerate() {
        let end = pieces
            .get(index + 1)
            .map(|next| next.start as usize)
            .unwrap_or(count);
        for offset in 0..(end - piece.start as usize) {
            out.push(
                piece
                    .base
                    .wrapping_add((offset as i128).wrapping_mul(piece.stride)),
            );
        }
    }
    out
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

fn finish_empty(
    schema: &Schema,
    bytes: &[u8],
    cursor: &mut usize,
    from_index: usize,
    included: Option<&[bool]>,
) -> Result<Vec<Row>> {
    let nrows = block_row_count(bytes)?;
    for (offset, field) in schema.fields[from_index..].iter().enumerate() {
        let index = from_index + offset;
        if included.is_some_and(|included| !included.get(index).copied().unwrap_or(false)) {
            continue;
        }
        skip_column(
            field.ty,
            bytes,
            cursor,
            nrows,
            index == schema.timestamp_index,
        )?;
    }
    if *cursor != bytes.len() {
        return Err(Error::corrupt("block has trailing bytes"));
    }
    Ok(Vec::new())
}

fn apply_prefix(mask: &mut [bool], values: &[Scalar], prefix: &str) {
    let needle = prefix.as_bytes();
    for (row, value) in values.iter().enumerate() {
        if !mask[row] {
            continue;
        }
        let keep = match value {
            Scalar::Str(text) => text.as_bytes().starts_with(needle),
            _ => false,
        };
        if !keep {
            mask[row] = false;
        }
    }
}

fn mask_text_prefix(
    bytes: &[u8],
    spans: &[Option<(usize, usize)>],
    mask: &mut [bool],
    prefix: &str,
) {
    let needle = prefix.as_bytes();
    for (row, span) in spans.iter().enumerate() {
        if !mask[row] {
            continue;
        }
        let keep = match span {
            Some((start, end)) => bytes[*start..*end].starts_with(needle),
            None => false,
        };
        if !keep {
            mask[row] = false;
        }
    }
}

fn apply_eq(mask: &mut [bool], values: &[Scalar], allowed: &[Scalar]) {
    for (row, value) in values.iter().enumerate() {
        if mask[row] && !allowed.iter().any(|candidate| candidate == value) {
            mask[row] = false;
        }
    }
}

fn apply_constraints(
    mask: &mut [bool],
    values: &[Scalar],
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
) {
    if let Some(eq) = eq {
        apply_eq(mask, values, &eq.allowed);
    }
    if let Some(range) = range {
        for (row, value) in values.iter().enumerate() {
            if mask[row] && !range_matches(value, range) {
                mask[row] = false;
            }
        }
    }
}

/// A constant column misses when neither its value nor a null can satisfy the
/// filters. Nulls satisfy equality only, and only when the range is absent.
fn constant_misses(
    value: &Scalar,
    has_null: bool,
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
) -> bool {
    let value_hit = eq.is_none_or(|eq| eq.allowed.iter().any(|candidate| candidate == value))
        && range.is_none_or(|range| range_matches(value, range));
    let null_hit = has_null
        && range.is_none()
        && eq.is_some_and(|eq| {
            eq.allowed
                .iter()
                .any(|candidate| matches!(candidate, Scalar::Null))
        });
    !value_hit && !null_hit
}

fn scalar_text_hit(allowed: &[Scalar], text: &str, json: bool) -> bool {
    allowed.iter().any(|value| match value {
        Scalar::Str(expected) if !json => expected == text,
        Scalar::Json(expected) if json => expected == text,
        _ => false,
    })
}

fn read_lp_span(bytes: &[u8], cursor: &mut usize, validate_json: bool) -> Result<(usize, usize)> {
    let len = read_varint(bytes, cursor)? as usize;
    let start = *cursor;
    let slice = read_exact(bytes, cursor, len)?;
    let text = std::str::from_utf8(slice).map_err(|_| Error::corrupt("string is not utf-8"))?;
    if validate_json {
        validate_json_column_text(text)?;
    }
    Ok((start, start + len))
}

fn row_is_present(present: &Option<Vec<bool>>, row: usize) -> bool {
    match present {
        None => true,
        Some(flags) => flags[row],
    }
}

/// Walk a string or JSON column and remember payload spans. No `String` is built.
fn read_text_spans(
    bytes: &[u8],
    cursor: &mut usize,
    nrows: usize,
    validate_json: bool,
) -> Result<Vec<Option<(usize, usize)>>> {
    let present = read_present(bytes, cursor, nrows)?;
    let present_count = match &present {
        None => nrows,
        Some(flags) => flags.iter().filter(|flag| **flag).count(),
    };
    let mut spans = vec![None; nrows];
    if present_count == 0 {
        expect_kind(bytes, cursor, 0)?;
        return Ok(spans);
    }
    let kind = read_u8(bytes, cursor)?;
    match kind {
        3 => {
            let span = read_lp_span(bytes, cursor, validate_json)?;
            for row in 0..nrows {
                if row_is_present(&present, row) {
                    spans[row] = Some(span);
                }
            }
            Ok(spans)
        }
        2 => {
            for row in 0..nrows {
                if row_is_present(&present, row) {
                    spans[row] = Some(read_lp_span(bytes, cursor, validate_json)?);
                }
            }
            Ok(spans)
        }
        1 | 4 => {
            let dict_len = read_string_dict_len(bytes, cursor)?;
            let mut dict = Vec::with_capacity(dict_len);
            for _ in 0..dict_len {
                dict.push(read_lp_span(bytes, cursor, validate_json)?);
            }
            let codes = read_dict_codes(
                bytes,
                cursor,
                present_count,
                dict.len(),
                kind == STRING_DICT_PERIOD,
            )?;
            let mut present_index = 0usize;
            for row in 0..nrows {
                if !row_is_present(&present, row) {
                    continue;
                }
                spans[row] = Some(dict[codes[present_index]]);
                present_index += 1;
            }
            Ok(spans)
        }
        _ => Err(Error::corrupt(format!("unknown string encoding {kind}"))),
    }
}

fn mask_text_spans(
    bytes: &[u8],
    spans: &[Option<(usize, usize)>],
    mask: &mut [bool],
    allowed: &[Scalar],
) {
    let json = allowed.iter().any(|value| matches!(value, Scalar::Json(_)));
    let null_ok = allowed.iter().any(|value| matches!(value, Scalar::Null));
    for (row, span) in spans.iter().enumerate() {
        if !mask[row] {
            continue;
        }
        let keep = match span {
            None => null_ok,
            Some((start, end)) => {
                let text = std::str::from_utf8(&bytes[*start..*end]).unwrap_or("");
                scalar_text_hit(allowed, text, json)
            }
        };
        if !keep {
            mask[row] = false;
        }
    }
}

fn materialize_text_spans(
    bytes: &[u8],
    spans: &[Option<(usize, usize)>],
    mask: &[bool],
    budget: &mut usize,
    json: bool,
) -> Result<Vec<Scalar>> {
    let mut expanded = 0usize;
    for (row, span) in spans.iter().enumerate() {
        if mask[row] {
            if let Some((start, end)) = span {
                expanded = expanded
                    .checked_add(end - start)
                    .ok_or_else(|| Error::event("query response size limit exceeded"))?;
            }
        }
    }
    charge_string_bytes(budget, expanded)?;
    let mut column = Vec::with_capacity(spans.len());
    for (row, span) in spans.iter().enumerate() {
        if !mask[row] {
            column.push(Scalar::Null);
            continue;
        }
        match span {
            None => column.push(Scalar::Null),
            Some((start, end)) => {
                let text = std::str::from_utf8(&bytes[*start..*end])
                    .map_err(|_| Error::corrupt("string is not utf-8"))?
                    .to_owned();
                column.push(if json {
                    Scalar::Json(text)
                } else {
                    Scalar::Str(text)
                });
            }
        }
    }
    Ok(column)
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
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
) -> Result<bool> {
    if eq.is_some_and(|eq| eq.allowed.is_empty()) || range.is_some_and(range_impossible) {
        return Ok(true);
    }
    let mut cursor = start;
    let present = read_present(bytes, &mut cursor, nrows)?;
    let (present_count, has_null) = present_stats(&present, nrows);
    if present_count == 0 {
        let null_hit = range.is_none()
            && eq.is_some_and(|eq| {
                eq.allowed
                    .iter()
                    .any(|candidate| matches!(candidate, Scalar::Null))
            });
        return Ok(!null_hit);
    }
    match ty {
        FieldType::String | FieldType::Text | FieldType::Json => Ok(false),
        FieldType::Int => int_const_misses(bytes, &mut cursor, has_null, eq, range, false),
        FieldType::Timestamp => int_const_misses(bytes, &mut cursor, has_null, eq, range, true),
        FieldType::Float => float_const_misses(bytes, &mut cursor, has_null, eq, range),
        FieldType::Bool => bool_const_misses(bytes, &mut cursor, has_null, eq, range),
        FieldType::Decimal { .. } => decimal_const_misses(bytes, &mut cursor, has_null, eq, range),
    }
}

fn int_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    has_null: bool,
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
    timestamp: bool,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_i64(bytes, cursor)?;
    let value = if timestamp {
        Scalar::Timestamp(base)
    } else {
        Scalar::Int(base)
    };
    Ok(constant_misses(&value, has_null, eq, range))
}

fn float_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    has_null: bool,
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_f64(bytes, cursor)?;
    Ok(constant_misses(&Scalar::Float(base), has_null, eq, range))
}

fn bool_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    has_null: bool,
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_u8(bytes, cursor)?;
    if base > 1 {
        return Err(Error::corrupt("bool constant is not 0 or 1"));
    }
    Ok(constant_misses(
        &Scalar::Bool(base == 1),
        has_null,
        eq,
        range,
    ))
}

fn decimal_const_misses(
    bytes: &[u8],
    cursor: &mut usize,
    has_null: bool,
    eq: Option<&ColumnPredicate>,
    range: Option<&RangeFilter>,
) -> Result<bool> {
    let kind = read_u8(bytes, cursor)?;
    if kind != 1 {
        return Ok(false);
    }
    let base = read_i128(bytes, cursor)?;
    Ok(constant_misses(&Scalar::Decimal(base), has_null, eq, range))
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

    fn text_schema() -> Schema {
        parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "note", "type": "text"}
                ]
            }"#,
        )
        .unwrap()
    }

    fn rows_with_notes(notes: &[Option<&str>]) -> Vec<Row> {
        let schema = text_schema();
        notes
            .iter()
            .enumerate()
            .map(|(i, note)| {
                let mut obj = serde_json::json!({"ts": 1_700_000_000_000i64 + i as i64});
                if let Some(text) = note {
                    obj["note"] = serde_json::json!(text);
                }
                parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap()
            })
            .collect()
    }

    fn text_encoding_kind(bytes: &[u8]) -> u8 {
        let nrows = block_row_count(bytes).unwrap();
        let mut cursor = 4;
        skip_column(FieldType::Timestamp, bytes, &mut cursor, nrows, true).unwrap();
        let _ = read_present(bytes, &mut cursor, nrows).unwrap();
        bytes[cursor]
    }

    fn assert_text_roundtrip(rows: &[Row]) {
        let schema = text_schema();
        let encoded = encode_block(&schema, rows).unwrap();
        let decoded = decode_block(&schema, &encoded.bytes).unwrap();
        assert_eq!(decoded.len(), rows.len());
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values[1], right.values[1]);
        }
    }

    /// A block whose text column was written with the dictionary disabled.
    fn encode_legacy_raw_text_block(rows: &[Row]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        encode_column(&mut bytes, FieldType::Timestamp, rows, 0).unwrap();
        let values = strings(rows, 1).unwrap();
        let nulls: Vec<bool> = values.iter().map(|value| value.is_none()).collect();
        write_nulls(&mut bytes, &nulls);
        bytes.extend_from_slice(&encode_strings(&values, false));
        bytes
    }

    #[test]
    fn appended_columns_decode_as_null_and_a_torn_tail_does_not() {
        let narrow = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let wide = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "region", "type": "string"},
                    {"name": "extra", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let row = parse_event(&narrow, br#"{"ts":10,"user_id":7}"#).unwrap();
        let encoded = encode_block(&narrow, &[row]).unwrap();
        let decoded = decode_block(&wide, &encoded.bytes).unwrap();
        assert_eq!(decoded[0].values[1], Scalar::Int(7));
        assert_eq!(decoded[0].values[2], Scalar::Null);
        assert_eq!(decoded[0].values[3], Scalar::Null);
        assert_eq!(decoded[0].ts, 10);

        let null_region = decode_rows_in_range_filtered(
            &wide,
            &encoded.bytes,
            0,
            100,
            usize::MAX,
            &[ColumnPredicate {
                index: 2,
                allowed: vec![Scalar::Null],
            }],
        )
        .unwrap();
        assert_eq!(null_region.len(), 1);
        let west = decode_rows_in_range_filtered(
            &wide,
            &encoded.bytes,
            0,
            100,
            usize::MAX,
            &[ColumnPredicate {
                index: 2,
                allowed: vec![Scalar::Str("west".into())],
            }],
        )
        .unwrap();
        assert!(west.is_empty());

        let wide_row =
            parse_event(&wide, br#"{"ts":11,"user_id":8,"region":"west","extra":3}"#).unwrap();
        let wide_block = encode_block(&wide, &[wide_row]).unwrap();
        let decoded = decode_block(&wide, &wide_block.bytes).unwrap();
        assert_eq!(decoded[0].values[2], Scalar::Str("west".into()));
        assert_eq!(decoded[0].values[3], Scalar::Int(3));

        let mut torn = encoded.bytes.clone();
        torn.push(0xff);
        assert!(decode_block(&wide, &torn).is_err());
    }

    #[test]
    fn text_dictionary_roundtrips_low_cardinality_notes() {
        let notes = ["landing", "checkout", "search"];
        let rows = rows_with_notes(
            &(0..12)
                .map(|i| if i % 4 == 0 { None } else { Some(notes[i % 3]) })
                .collect::<Vec<_>>(),
        );
        let encoded = encode_block(&text_schema(), &rows).unwrap();
        assert_eq!(
            text_encoding_kind(&encoded.bytes),
            STRING_DICT_PERIOD,
            "repeated notes should store one period of dictionary codes"
        );
        assert_text_roundtrip(&rows);
    }

    #[test]
    fn unique_text_stays_raw() {
        let owned: Vec<String> = (0..8).map(|i| format!("unique-note-{i:04}")).collect();
        let notes: Vec<Option<&str>> = owned.iter().map(|text| Some(text.as_str())).collect();
        let rows = rows_with_notes(&notes);
        let encoded = encode_block(&text_schema(), &rows).unwrap();
        assert_eq!(text_encoding_kind(&encoded.bytes), 2);
        assert_text_roundtrip(&rows);
    }

    #[test]
    fn constant_text_stays_constant() {
        let rows = rows_with_notes(&[Some("landing"); 6]);
        let encoded = encode_block(&text_schema(), &rows).unwrap();
        assert_eq!(text_encoding_kind(&encoded.bytes), 3);
        assert_text_roundtrip(&rows);
    }

    #[test]
    fn legacy_raw_text_block_still_decodes() {
        let notes = ["landing", "checkout", "search"];
        let rows = rows_with_notes(&(0..12).map(|i| Some(notes[i % 3])).collect::<Vec<_>>());
        let bytes = encode_legacy_raw_text_block(&rows);
        assert_eq!(
            text_encoding_kind(&bytes),
            2,
            "legacy writer left the dictionary disabled"
        );
        let decoded = decode_block(&text_schema(), &bytes).unwrap();
        assert_eq!(decoded.len(), rows.len());
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values[1], right.values[1]);
        }
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
        assert_eq!(
            encoded[0], STRING_DICT_PERIOD,
            "a repeated code cycle should store one period"
        );
    }

    #[test]
    fn dictionary_without_a_repeated_period_keeps_the_full_code_vector() {
        let values: Vec<Option<String>> = ["click", "view", "click", "view", "click", "buy"]
            .into_iter()
            .map(|text| Some(text.to_string()))
            .collect();
        let encoded = encode_strings(&values, true);
        assert_eq!(encoded[0], STRING_DICT);
        let present: Vec<&str> = values.iter().filter_map(|value| value.as_deref()).collect();
        let (_, codes, _) = build_string_dictionary(&present);
        assert_eq!(repeating_code_period(&codes), None);
    }

    #[test]
    fn period_body_is_kept_only_when_it_is_strictly_smaller() {
        // Period is count - 1, so the extra period length costs the one code it saves.
        let repeated = "click".repeat(20);
        let values = [
            repeated.as_str(),
            "v",
            repeated.as_str(),
            "b",
            "s",
            "w",
            repeated.as_str(),
        ];
        let values: Vec<Option<String>> = values
            .into_iter()
            .map(|text| Some(text.to_string()))
            .collect();
        let present: Vec<&str> = values.iter().filter_map(|value| value.as_deref()).collect();
        let (dict, codes, width) = build_string_dictionary(&present);
        let period = repeating_code_period(&codes).expect("period");
        let full = write_string_dictionary(STRING_DICT, &dict, width, &codes);
        let periodic = write_string_dictionary(STRING_DICT_PERIOD, &dict, width, &codes[..period]);
        assert_eq!(full.len(), periodic.len());
        assert!(full.len() < encode_raw_strings(&present).len());
        assert_eq!(encode_strings(&values, true)[0], STRING_DICT);
    }

    #[test]
    fn stock_action_and_note_store_one_period() {
        let actions = ["click", "view", "buy", "scroll"];
        let notes = ["landing", "checkout", "search", ""];
        let schema = schema();
        let mut rows = Vec::new();
        for i in 0..16 {
            let mut obj = serde_json::json!({
                "ts": 1_700_000_000_000i64 + i * 10,
                "user_id": i,
                "score": 1.0,
                "ok": true,
                "action": actions[i as usize % actions.len()],
                "amount": "1.00",
            });
            let note = notes[i as usize % notes.len()];
            if !note.is_empty() {
                obj["note"] = serde_json::json!(note);
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let kinds = string_column_kinds(&schema, &encoded.bytes);
        assert_eq!(kinds["action"], STRING_DICT_PERIOD);
        assert_eq!(kinds["note"], STRING_DICT_PERIOD);
        let decoded = decode_block(&schema, &encoded.bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values, right.values);
        }
        let ranged = decode_rows_in_range(
            &schema,
            &encoded.bytes,
            1_700_000_000_000,
            1_700_000_000_000 + 15 * 10,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(ranged.len(), rows.len());
        let filtered = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[ColumnPredicate {
                index: 4,
                allowed: vec![Scalar::Str("click".into())],
            }],
        )
        .unwrap();
        assert_eq!(filtered.len(), 4);
        assert!(filtered
            .iter()
            .all(|row| row.values[4] == Scalar::Str("click".into())));
    }

    #[test]
    fn count_reads_one_dictionary_period_from_the_header() {
        let schema = schema();
        let actions = ["click", "view", "buy", "scroll"];
        let from = 1_700_000_000_000i64;
        let rows = (0..18)
            .map(|i| {
                action_row(
                    &schema,
                    from + i * 10,
                    actions[i as usize % actions.len()],
                    true,
                    Some("note"),
                )
            })
            .collect::<Vec<_>>();
        let encoded = encode_block(&schema, &rows).unwrap();
        assert_eq!(
            string_column_kinds(&schema, &encoded.bytes)["action"],
            STRING_DICT_PERIOD
        );
        let click = str_eq(4, "click");
        assert_eq!(
            count_rows_filtered(&schema, &encoded.bytes, from, from + 170, &click, &[]).unwrap(),
            5,
            "eighteen rows of a four-code period contain five clicks"
        );
        assert_eq!(
            count_rows_filtered(
                &schema,
                &encoded.bytes,
                from,
                from + 170,
                &str_eq(4, "missing"),
                &[]
            )
            .unwrap(),
            0
        );
        assert_eq!(
            count_rows_filtered(&schema, &encoded.bytes, from + 30, from + 50, &click, &[])
                .unwrap(),
            1,
            "a cut window still walks and keeps the one click inside it"
        );
        assert_eq!(
            count_rows_filtered(
                &schema,
                &encoded.bytes,
                from,
                from + 170,
                &click,
                &[RangeFilter {
                    index: 1,
                    lower: Some(RangeEnd {
                        inclusive: true,
                        value: Scalar::Int(0),
                    }),
                    upper: Some(RangeEnd {
                        inclusive: false,
                        value: Scalar::Int(1),
                    }),
                }],
            )
            .unwrap(),
            0,
            "a numeric range still walks, and none of these rows sit below 1"
        );
        assert_eq!(
            count_rows_filtered(
                &schema,
                &encoded.bytes,
                from,
                from + 170,
                &[
                    ColumnPredicate {
                        index: 4,
                        allowed: vec![Scalar::Str("click".into())],
                    },
                    ColumnPredicate {
                        index: 3,
                        allowed: vec![Scalar::Bool(false)],
                    },
                ],
                &[],
            )
            .unwrap(),
            0,
            "a second predicate still walks"
        );

        let constant = (0..7)
            .map(|i| action_row(&schema, from + i * 10, "click", true, Some("note")))
            .collect::<Vec<_>>();
        let constant_block = encode_block(&schema, &constant).unwrap();
        assert_eq!(
            string_column_kinds(&schema, &constant_block.bytes)["action"],
            3
        );
        assert_eq!(
            count_rows_filtered(&schema, &constant_block.bytes, from, from + 60, &click, &[])
                .unwrap(),
            7
        );

        let notes = ["landing", "checkout", "search", ""];
        let with_nulls = (0..7)
            .map(|i| {
                let note = notes[i as usize % notes.len()];
                action_row(
                    &schema,
                    from + i * 10,
                    actions[i as usize % actions.len()],
                    true,
                    if note.is_empty() { None } else { Some(note) },
                )
            })
            .collect::<Vec<_>>();
        let null_block = encode_block(&schema, &with_nulls).unwrap();
        assert_eq!(
            count_rows_filtered(
                &schema,
                &null_block.bytes,
                from,
                from + 60,
                &str_eq(5, "landing"),
                &[]
            )
            .unwrap(),
            2,
            "a null in the text column still walks"
        );

        let full = ["click", "view", "click", "view", "click", "buy"];
        let full_rows = (0..full.len())
            .map(|i| action_row(&schema, from + i as i64 * 10, full[i], true, Some("note")))
            .collect::<Vec<_>>();
        let full_block = encode_block(&schema, &full_rows).unwrap();
        assert_eq!(
            string_column_kinds(&schema, &full_block.bytes)["action"],
            STRING_DICT
        );
        assert_eq!(
            count_rows_filtered(&schema, &full_block.bytes, from, from + 50, &click, &[]).unwrap(),
            3
        );

        let unique: Vec<String> = (0..8).map(|i| format!("unique-action-{i:04}")).collect();
        let raw_rows = unique
            .iter()
            .enumerate()
            .map(|(i, action)| {
                action_row(&schema, from + i as i64 * 10, action, true, Some("note"))
            })
            .collect::<Vec<_>>();
        let raw_block = encode_block(&schema, &raw_rows).unwrap();
        assert_eq!(string_column_kinds(&schema, &raw_block.bytes)["action"], 2);
        assert_eq!(
            count_rows_filtered(
                &schema,
                &raw_block.bytes,
                from,
                from + 70,
                &str_eq(4, "unique-action-0003"),
                &[]
            )
            .unwrap(),
            1
        );

        let decreasing = (0..8)
            .map(|i| {
                action_row(
                    &schema,
                    1_000 - i * 10,
                    actions[i as usize % actions.len()],
                    true,
                    Some("note"),
                )
            })
            .collect::<Vec<_>>();
        let decreasing_block = encode_block(&schema, &decreasing).unwrap();
        assert_eq!(
            count_rows_filtered(&schema, &decreasing_block.bytes, 900, 1_000, &click, &[]).unwrap(),
            2
        );
        assert_eq!(
            count_rows_filtered(&schema, &decreasing_block.bytes, 940, 980, &click, &[]).unwrap(),
            1,
            "a negative timestamp stride still walks a cut window"
        );

        let base = i64::MAX - 5;
        let wrapped = [base, base.wrapping_add(10), base.wrapping_add(20)];
        let wrapped_rows = wrapped
            .iter()
            .map(|ts| action_row(&schema, *ts, "click", true, Some("note")))
            .collect::<Vec<_>>();
        let wrapped_block = encode_block(&schema, &wrapped_rows).unwrap();
        assert_eq!(
            count_rows_filtered(&schema, &wrapped_block.bytes, base, base, &click, &[]).unwrap(),
            1,
            "a wrapping timestamp stride still walks"
        );
    }

    #[test]
    fn count_reads_one_bool_period_from_the_header() {
        let schema = schema();
        let from = 1_700_000_000_000i64;
        let rows = (0..18)
            .map(|i| action_row(&schema, from + i * 10, "click", i % 2 == 0, Some("note")))
            .collect::<Vec<_>>();
        let encoded = encode_block(&schema, &rows).unwrap();
        assert_eq!(bool_encoding_kind(&schema, &encoded.bytes), BOOL_PERIOD);
        let yes = bool_eq(3, true);
        let no = bool_eq(3, false);
        assert_eq!(
            count_rows_filtered(&schema, &encoded.bytes, from, from + 170, &yes, &[]).unwrap(),
            9
        );
        assert_eq!(
            count_rows_filtered(&schema, &encoded.bytes, from, from + 170, &no, &[]).unwrap(),
            9
        );
        assert_eq!(
            count_rows_filtered(&schema, &encoded.bytes, from + 30, from + 50, &yes, &[]).unwrap(),
            1,
            "a cut window still walks and keeps the one true inside it"
        );
        assert_eq!(
            count_rows_filtered(
                &schema,
                &encoded.bytes,
                from,
                from + 170,
                &yes,
                &[RangeFilter {
                    index: 1,
                    lower: Some(RangeEnd {
                        inclusive: true,
                        value: Scalar::Int(0),
                    }),
                    upper: Some(RangeEnd {
                        inclusive: false,
                        value: Scalar::Int(1),
                    }),
                }],
            )
            .unwrap(),
            0,
            "a numeric range still walks, and none of these rows sit below 1"
        );
        assert_eq!(
            count_rows_filtered(
                &schema,
                &encoded.bytes,
                from,
                from + 170,
                &[
                    ColumnPredicate {
                        index: 3,
                        allowed: vec![Scalar::Bool(true)],
                    },
                    ColumnPredicate {
                        index: 4,
                        allowed: vec![Scalar::Str("missing".into())],
                    },
                ],
                &[],
            )
            .unwrap(),
            0,
            "a second predicate still walks"
        );

        let wide: Vec<Row> = (0..40)
            .map(|i| action_row(&schema, from + i * 10, "click", i % 9 == 0, Some("note")))
            .collect();
        let wide_block = encode_block(&schema, &wide).unwrap();
        assert_eq!(bool_encoding_kind(&schema, &wide_block.bytes), BOOL_PERIOD);
        assert_eq!(
            count_rows_filtered(&schema, &wide_block.bytes, from, from + 390, &yes, &[]).unwrap(),
            5,
            "a period that crosses a pattern byte is counted from those bits"
        );

        let constant = (0..7)
            .map(|i| action_row(&schema, from + i * 10, "click", true, Some("note")))
            .collect::<Vec<_>>();
        let constant_block = encode_block(&schema, &constant).unwrap();
        assert_eq!(bool_encoding_kind(&schema, &constant_block.bytes), 1);
        assert_eq!(
            count_rows_filtered(&schema, &constant_block.bytes, from, from + 60, &yes, &[])
                .unwrap(),
            7
        );
        assert_eq!(
            count_rows_filtered(&schema, &constant_block.bytes, from, from + 60, &no, &[]).unwrap(),
            0
        );

        let with_nulls = (0..8)
            .map(|i| {
                let ok = if i % 4 == 0 { None } else { Some(i % 2 == 0) };
                ok_row(&schema, from + i * 10, ok)
            })
            .collect::<Vec<_>>();
        let null_block = encode_block(&schema, &with_nulls).unwrap();
        assert_eq!(
            count_rows_filtered(&schema, &null_block.bytes, from, from + 70, &yes, &[]).unwrap(),
            2,
            "a null in the bool column still walks"
        );
        assert_eq!(
            count_rows_filtered(
                &schema,
                &null_block.bytes,
                from,
                from + 70,
                &[ColumnPredicate {
                    index: 3,
                    allowed: vec![Scalar::Null],
                }],
                &[]
            )
            .unwrap(),
            2
        );

        let tied: Vec<Row> = (0..16)
            .map(|i| action_row(&schema, from + i * 10, "click", i % 2 == 0, Some("note")))
            .collect();
        let tied_block = encode_block(&schema, &tied).unwrap();
        assert_eq!(bool_encoding_kind(&schema, &tied_block.bytes), 2);
        assert_eq!(
            count_rows_filtered(&schema, &tied_block.bytes, from, from + 150, &yes, &[]).unwrap(),
            8,
            "a full per-row bitmap still walks"
        );

        let decreasing = (0..8)
            .map(|i| action_row(&schema, 1_000 - i * 10, "click", i % 2 == 0, Some("note")))
            .collect::<Vec<_>>();
        let decreasing_block = encode_block(&schema, &decreasing).unwrap();
        assert_eq!(
            count_rows_filtered(&schema, &decreasing_block.bytes, 900, 1_000, &yes, &[]).unwrap(),
            4
        );
        assert_eq!(
            count_rows_filtered(&schema, &decreasing_block.bytes, 940, 980, &yes, &[]).unwrap(),
            3,
            "a negative timestamp stride still walks a cut window"
        );

        let base = i64::MAX - 5;
        let wrapped = [base, base.wrapping_add(10), base.wrapping_add(20)];
        let wrapped_rows = wrapped
            .iter()
            .enumerate()
            .map(|(i, ts)| action_row(&schema, *ts, "click", i % 2 == 0, Some("note")))
            .collect::<Vec<_>>();
        let wrapped_block = encode_block(&schema, &wrapped_rows).unwrap();
        assert_eq!(
            count_rows_filtered(&schema, &wrapped_block.bytes, base, base, &yes, &[]).unwrap(),
            1,
            "a wrapping timestamp stride still walks"
        );
    }

    fn bool_eq(index: usize, value: bool) -> Vec<ColumnPredicate> {
        vec![ColumnPredicate {
            index,
            allowed: vec![Scalar::Bool(value)],
        }]
    }

    fn bool_encoding_kind(schema: &Schema, bytes: &[u8]) -> u8 {
        let nrows = block_row_count(bytes).unwrap();
        let mut cursor = 4usize;
        for (index, field) in schema.fields.iter().enumerate() {
            if index == 3 {
                assert_eq!(
                    bytes[cursor], 0,
                    "the bool column in this fixture has no nulls"
                );
                cursor += 1;
                return bytes[cursor];
            }
            skip_column(field.ty, bytes, &mut cursor, nrows, false).unwrap();
        }
        panic!("bool column is missing");
    }

    fn ok_row(schema: &Schema, ts: i64, ok: Option<bool>) -> Row {
        let mut obj = serde_json::json!({
            "ts": ts,
            "user_id": 1,
            "score": 1.0,
            "action": "click",
            "amount": "1.00",
            "note": "note",
        });
        if let Some(ok) = ok {
            obj["ok"] = serde_json::json!(ok);
        }
        parse_event(schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap()
    }

    fn str_eq(index: usize, text: &str) -> Vec<ColumnPredicate> {
        vec![ColumnPredicate {
            index,
            allowed: vec![Scalar::Str(text.into())],
        }]
    }

    fn action_row(schema: &Schema, ts: i64, action: &str, ok: bool, note: Option<&str>) -> Row {
        let mut obj = serde_json::json!({
            "ts": ts,
            "user_id": 1,
            "score": 1.0,
            "ok": ok,
            "action": action,
            "amount": "1.00",
        });
        if let Some(note) = note {
            obj["note"] = serde_json::json!(note);
        }
        parse_event(schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap()
    }

    #[test]
    fn legacy_dictionary_block_still_decodes() {
        let notes = ["landing", "checkout", "search"];
        let rows = rows_with_notes(&(0..12).map(|i| Some(notes[i % 3])).collect::<Vec<_>>());
        let bytes = encode_legacy_dict_text_block(&rows);
        assert_eq!(text_encoding_kind(&bytes), STRING_DICT);
        let decoded = decode_block(&text_schema(), &bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values[1], right.values[1]);
        }
    }

    fn encode_legacy_dict_text_block(rows: &[Row]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(rows.len() as u32).to_le_bytes());
        encode_column(&mut bytes, FieldType::Timestamp, rows, 0).unwrap();
        let values = strings(rows, 1).unwrap();
        let nulls: Vec<bool> = values.iter().map(|value| value.is_none()).collect();
        write_nulls(&mut bytes, &nulls);
        let present: Vec<&str> = values.iter().filter_map(|value| value.as_deref()).collect();
        bytes.extend_from_slice(&encode_dict_strings(&present));
        bytes
    }

    fn string_column_kinds(schema: &Schema, bytes: &[u8]) -> std::collections::HashMap<String, u8> {
        let nrows = block_row_count(bytes).unwrap();
        let mut cursor = 4;
        let mut kinds = std::collections::HashMap::new();
        for field in &schema.fields {
            let start = cursor;
            skip_column(field.ty, bytes, &mut cursor, nrows, false).unwrap();
            if matches!(
                field.ty,
                FieldType::String | FieldType::Text | FieldType::Json
            ) {
                let mut at = start;
                let present = read_present(bytes, &mut at, nrows).unwrap();
                let present_count = match present {
                    None => nrows,
                    Some(flags) => flags.iter().filter(|flag| **flag).count(),
                };
                if present_count > 0 {
                    kinds.insert(field.name.clone(), bytes[at]);
                }
            }
        }
        kinds
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
    fn exact_bit_width_uses_span_on_power_of_two_boundaries() {
        fn roundtrip(values: &[Option<i64>], bits: u8) {
            let encoded = encode_i64s(values);
            assert_eq!(encoded[0], KIND_BITPACK);
            assert_eq!(encoded[1], bits);
            let mut cursor = 0;
            let decoded = decode_i64s(&encoded, &mut cursor, values.len()).unwrap();
            assert_eq!(cursor, encoded.len());
            assert_eq!(
                decoded,
                values.iter().copied().flatten().collect::<Vec<_>>()
            );
            let mut skip = 0;
            skip_i64s(&encoded, &mut skip, values.len()).unwrap();
            assert_eq!(skip, encoded.len());
        }

        // Not an arithmetic progression, so kind 7 cannot steal the column.
        roundtrip(&[Some(0), Some(1), Some(0)], 1);

        let mut seven_bits = vec![Some(0i64), Some(127)];
        seven_bits.extend(std::iter::repeat(Some(3)).take(14));
        roundtrip(&seven_bits, 7);

        let mut fifteen_bits = vec![Some(0i64), Some(32_767)];
        fifteen_bits.extend(std::iter::repeat(Some(1)).take(14));
        roundtrip(&fifteen_bits, 15);
    }

    #[test]
    fn exact_bit_width_packs_span_999() {
        // Three values are not one stride, and two stride pieces are larger than 10-bit packing.
        let values = [Some(0i64), Some(999), Some(1)];
        let encoded = encode_i64s(&values);
        assert_eq!(encoded[0], KIND_BITPACK);
        assert_eq!(encoded[1], 10, "999 fits in 10 bits");
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, values.len()).unwrap(),
            vec![0, 999, 1]
        );
        assert_eq!(cursor, encoded.len());
    }

    #[test]
    fn wrapped_modulo_uses_a_few_stride_pieces() {
        let values: Vec<Option<i64>> = (0..2048).map(|i| Some(i % 1000)).collect();
        let encoded = encode_i64s(&values);
        assert_eq!(encoded[0], KIND_PIECES);
        assert_eq!(u16::from_le_bytes(encoded[1..3].try_into().unwrap()), 3);
        assert!(
            encoded.len() < 100,
            "three i64 pieces are a few dozen bytes"
        );
        let mut cursor = 0;
        let decoded = decode_i64s(&encoded, &mut cursor, values.len()).unwrap();
        assert_eq!(cursor, encoded.len());
        for (index, value) in decoded.iter().enumerate() {
            assert_eq!(*value, (index % 1000) as i64);
        }
        let mut skip = 0;
        skip_i64s(&encoded, &mut skip, values.len()).unwrap();
        assert_eq!(skip, encoded.len());

        let mut with_nulls = values.clone();
        with_nulls[10] = None;
        with_nulls[1500] = None;
        let encoded = encode_i64s(&with_nulls);
        assert_eq!(encoded[0], KIND_PIECES);
        let present: Vec<i64> = with_nulls.iter().copied().flatten().collect();
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, present.len()).unwrap(),
            present
        );
    }

    #[test]
    fn eight_stride_pieces_roundtrip_and_a_ninth_does_not() {
        let mut eight = Vec::new();
        for _ in 0..8 {
            for value in 0..100i64 {
                eight.push(Some(value));
            }
        }
        let encoded = encode_i64s(&eight);
        assert_eq!(encoded[0], KIND_PIECES);
        assert_eq!(u16::from_le_bytes(encoded[1..3].try_into().unwrap()), 8);
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, eight.len()).unwrap(),
            eight.iter().copied().flatten().collect::<Vec<_>>()
        );
        let mut skip = 0;
        skip_i64s(&encoded, &mut skip, eight.len()).unwrap();
        assert_eq!(skip, encoded.len());

        let mut nine = Vec::new();
        for piece in 0..9 {
            nine.push(Some(piece * 10));
            nine.push(Some(piece * 10 + 1));
        }
        let encoded = encode_i64s(&nine);
        assert_ne!(encoded[0], KIND_PIECES);
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, nine.len()).unwrap(),
            nine.iter().copied().flatten().collect::<Vec<_>>()
        );
    }

    #[test]
    fn unit_stride_runs_keep_a_sawtooth_and_reject_a_wrap() {
        let schema = schema();
        let saw: Vec<Row> = (0..2048)
            .map(|i| {
                parse_event(
                    &schema,
                    format!(
                        r#"{{"ts":{i},"user_id":{},"score":1.0,"ok":true,"action":"click","amount":"1.00"}}"#,
                        i % 1000
                    )
                    .as_bytes(),
                )
                .unwrap()
            })
            .collect();
        let encoded = encode_block(&schema, &saw).unwrap();
        let runs = try_unit_stride_runs(&schema, &encoded.bytes, 1)
            .unwrap()
            .expect("user_id is unit-stride pieces");
        let rows: u128 = runs
            .iter()
            .map(|run| {
                let start = (run.start as u128) ^ (1u128 << 127);
                let end = (run.end as u128) ^ (1u128 << 127);
                (end - start + 1) * u128::from(run.mult)
            })
            .sum();
        assert_eq!(rows, 2048);
        assert!(runs.iter().all(|run| run.mult == 1 && run.end >= run.start));

        let wrapped = [
            parse_event(
                &schema,
                format!(
                    r#"{{"ts":1,"user_id":{},"score":1.0,"ok":true,"action":"click","amount":"1.00"}}"#,
                    i64::MAX
                )
                .as_bytes(),
            )
            .unwrap(),
            parse_event(
                &schema,
                format!(
                    r#"{{"ts":2,"user_id":{},"score":1.0,"ok":true,"action":"click","amount":"1.00"}}"#,
                    i64::MIN
                )
                .as_bytes(),
            )
            .unwrap(),
            parse_event(
                &schema,
                format!(
                    r#"{{"ts":3,"user_id":{},"score":1.0,"ok":true,"action":"click","amount":"1.00"}}"#,
                    i64::MIN + 1
                )
                .as_bytes(),
            )
            .unwrap(),
        ];
        let wrapped = encode_block(&schema, &wrapped).unwrap();
        assert!(try_unit_stride_runs(&schema, &wrapped.bytes, 1)
            .unwrap()
            .is_none());

        let wide: Vec<Row> = (0..8)
            .map(|i| {
                parse_event(
                    &schema,
                    format!(
                        r#"{{"ts":{i},"user_id":{},"score":1.0,"ok":true,"action":"click","amount":"1.00"}}"#,
                        i * 2
                    )
                    .as_bytes(),
                )
                .unwrap()
            })
            .collect();
        let wide = encode_block(&schema, &wide).unwrap();
        assert!(try_unit_stride_runs(&schema, &wide.bytes, 1)
            .unwrap()
            .is_none());
    }

    #[test]
    fn wrapped_i64_across_the_sign_boundary_uses_kind_9() {
        let values = [
            Some(i64::MAX),
            Some(i64::MIN),
            Some(i64::MIN + 1),
            Some(0),
            Some(1),
        ];
        let encoded = encode_i64s(&values);
        assert_eq!(encoded[0], KIND_PIECES);
        let mut cursor = 0;
        assert_eq!(
            decode_i64s(&encoded, &mut cursor, values.len()).unwrap(),
            vec![i64::MAX, i64::MIN, i64::MIN + 1, 0, 1]
        );
    }

    #[test]
    fn wrapped_i128_modulo_uses_kind_9() {
        let values: Vec<Option<i128>> = (0..2048).map(|i| Some(i128::from(i % 1000))).collect();
        let encoded = encode_i128s(&values);
        assert_eq!(encoded[0], KIND_PIECES);
        assert_eq!(u16::from_le_bytes(encoded[1..3].try_into().unwrap()), 3);
        let mut cursor = 0;
        let decoded = decode_i128s(&encoded, &mut cursor, values.len()).unwrap();
        assert_eq!(cursor, encoded.len());
        for (index, value) in decoded.iter().enumerate() {
            assert_eq!(*value, i128::from((index % 1000) as u32));
        }
        let mut skip = 0;
        skip_i128s(&encoded, &mut skip, values.len()).unwrap();
        assert_eq!(skip, encoded.len());
    }

    #[test]
    fn legacy_kind_8_block_still_decodes() {
        let values = [0i64, 5, 7, 1];
        let mut payload = vec![KIND_BITPACK, 3];
        payload.extend_from_slice(&0i64.to_le_bytes());
        let mut packer = BitPacker::default();
        for value in values {
            packer.push(u128::from(value as u64), 3);
        }
        payload.extend_from_slice(&packer.finish());
        let mut cursor = 0;
        assert_eq!(decode_i64s(&payload, &mut cursor, 4).unwrap(), values);
        assert_eq!(cursor, payload.len());
        let mut skip = 0;
        skip_i64s(&payload, &mut skip, 4).unwrap();
        assert_eq!(skip, payload.len());

        let mut column = vec![KIND_BITPACK, 3];
        column.extend_from_slice(&0i64.to_le_bytes());
        let mut packer = BitPacker::default();
        packer.push(0, 3);
        packer.push(5, 3);
        column.extend_from_slice(&packer.finish());

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
        bytes.extend_from_slice(&column);
        let rows = decode_block(&schema, &bytes).unwrap();
        assert_eq!(rows[0].values[1], Scalar::Int(0));
        assert_eq!(rows[1].values[1], Scalar::Int(5));
        assert_eq!(rows[0].ts, 1000);
        assert_eq!(rows[1].ts, 1000);
    }

    #[test]
    fn kind_9_rejects_a_zero_stride_and_a_bad_piece_count() {
        let mut zero_stride = vec![KIND_PIECES];
        zero_stride.extend_from_slice(&1u16.to_le_bytes());
        zero_stride.extend_from_slice(&0u32.to_le_bytes());
        zero_stride.extend_from_slice(&4i64.to_le_bytes());
        zero_stride.extend_from_slice(&0i64.to_le_bytes());
        assert!(decode_i64s(&zero_stride, &mut 0, 2).is_err());
        assert!(skip_i64s(&zero_stride, &mut 0, 2).is_err());

        let mut too_many = vec![KIND_PIECES];
        too_many.extend_from_slice(&9u16.to_le_bytes());
        assert!(decode_i64s(&too_many, &mut 0, 4).is_err());

        let mut not_from_zero = vec![KIND_PIECES];
        not_from_zero.extend_from_slice(&1u16.to_le_bytes());
        not_from_zero.extend_from_slice(&1u32.to_le_bytes());
        not_from_zero.extend_from_slice(&0i64.to_le_bytes());
        not_from_zero.extend_from_slice(&1i64.to_le_bytes());
        assert!(decode_i64s(&not_from_zero, &mut 0, 4)
            .unwrap_err()
            .to_string()
            .contains("do not start at 0"));
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
        let filtered = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            2_000,
            12_000,
            usize::MAX,
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
        let by_note = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[note],
        )
        .unwrap();
        assert_eq!(by_note.len(), 1);
        assert_eq!(by_note[0].ts, 2_000);

        let miss = ColumnPredicate {
            index: 4,
            allowed: vec![Scalar::Str("missing".into())],
        };
        let mut truncated = encoded.bytes.clone();
        truncated.pop();
        assert!(decode_rows_in_range_filtered(
            &schema,
            &truncated,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[miss]
        )
        .is_err());
        assert!(decode_rows_in_range_filtered(
            &schema,
            &truncated,
            i64::MIN,
            i64::MAX,
            usize::MAX,
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
        assert!(decode_rows_in_range_filtered(
            &schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[filter]
        )
        .unwrap_err()
        .to_string()
        .contains("utf-8"));
    }

    #[test]
    fn filtered_decode_rejects_dictionary_code_on_a_dropped_row() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"},
                    {"name": "note", "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        let rows = [
            br#"{"ts":1000,"action":"click","note":"alpha"}"#.as_slice(),
            br#"{"ts":2000,"action":"click","note":"alpha"}"#,
            br#"{"ts":3000,"action":"view","note":"beta"}"#,
            br#"{"ts":4000,"action":"view","note":"beta"}"#,
        ];
        let parsed: Vec<_> = rows
            .iter()
            .map(|raw| parse_event(&schema, raw).unwrap())
            .collect();
        let encoded = encode_block(&schema, &parsed).unwrap();
        let mut bytes = encoded.bytes;
        let last = bytes.len() - 1;
        bytes[last] = 0xff;
        assert!(decode_block(&schema, &bytes)
            .unwrap_err()
            .to_string()
            .contains("string dictionary code is out of range"));
        let filter = ColumnPredicate {
            index: 1,
            allowed: vec![Scalar::Str("click".into())],
        };
        assert!(decode_rows_in_range_filtered(
            &schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[filter]
        )
        .unwrap_err()
        .to_string()
        .contains("string dictionary code is out of range"));
    }

    #[test]
    fn filtered_decode_rejects_invalid_json_on_a_dropped_row() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let click =
            parse_event(&schema, br#"{"ts":1000,"action":"click","props":{"a":1}}"#).unwrap();
        let view = parse_event(&schema, br#"{"ts":2000,"action":"view","props":{"b":2}}"#).unwrap();
        let view_json = match &view.values[2] {
            Scalar::Json(text) => text.clone(),
            other => panic!("expected json, got {other:?}"),
        };
        let encoded = encode_block(&schema, &[click, view]).unwrap();
        let mut bytes = encoded.bytes;
        let needle = view_json.as_bytes();
        let pos = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("view json");
        bytes[pos + 1] = b'x';
        assert!(decode_block(&schema, &bytes)
            .unwrap_err()
            .to_string()
            .contains("json column value is not valid JSON"));
        let filter = ColumnPredicate {
            index: 1,
            allowed: vec![Scalar::Str("click".into())],
        };
        assert!(decode_rows_in_range_filtered(
            &schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[filter]
        )
        .unwrap_err()
        .to_string()
        .contains("json column value is not valid JSON"));
    }

    #[test]
    fn filtered_decode_rejects_bad_bool_and_json_on_the_skip_path() {
        let bool_schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "ok", "type": "bool"},
                    {"name": "action", "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        let row = parse_event(&bool_schema, br#"{"ts":1000,"ok":true,"action":"click"}"#).unwrap();
        let encoded = encode_block(&bool_schema, &[row]).unwrap();
        let mut bytes = encoded.bytes;
        let marker = bytes
            .windows(2)
            .rposition(|window| window == [1, 1])
            .expect("constant bool");
        bytes[marker + 1] = 2;
        let full = decode_block(&bool_schema, &bytes).unwrap_err().to_string();
        assert!(full.contains("bool constant is not 0 or 1"));
        let miss = ColumnPredicate {
            index: 2,
            allowed: vec![Scalar::Str("missing".into())],
        };
        let filtered = decode_rows_in_range_filtered(
            &bool_schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[miss],
        )
        .unwrap_err()
        .to_string();
        assert!(filtered.contains("bool constant is not 0 or 1"));

        let json_schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let row = parse_event(
            &json_schema,
            br#"{"ts":1000,"action":"click","props":{"a":1}}"#,
        )
        .unwrap();
        let json_text = match &row.values[2] {
            Scalar::Json(text) => text.clone(),
            other => panic!("expected json, got {other:?}"),
        };
        assert!(json_text.starts_with('{'));
        let encoded = encode_block(&json_schema, &[row]).unwrap();
        let mut bytes = encoded.bytes;
        let needle = json_text.as_bytes();
        let pos = bytes
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("json payload");
        bytes[pos + 1] = b'x';
        let full = decode_block(&json_schema, &bytes).unwrap_err().to_string();
        assert!(full.contains("json column value is not valid JSON"));
        let miss = ColumnPredicate {
            index: 1,
            allowed: vec![Scalar::Str("missing".into())],
        };
        let filtered = decode_rows_in_range_filtered(
            &json_schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[miss],
        )
        .unwrap_err()
        .to_string();
        assert!(filtered.contains("json column value is not valid JSON"));
    }

    #[test]
    fn filtered_constant_string_charges_before_replication() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "action", "type": "string"}
                ]
            }"#,
        )
        .unwrap();
        let rows: Vec<_> = (0..4)
            .map(|ts| {
                parse_event(
                    &schema,
                    format!(r#"{{"ts":{ts},"action":"click"}}"#).as_bytes(),
                )
                .unwrap()
            })
            .collect();
        let encoded = encode_block(&schema, &rows).unwrap();
        let filter = || ColumnPredicate {
            index: 1,
            allowed: vec![Scalar::Str("click".into())],
        };
        let err = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            5 * 3,
            &[filter()],
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("query response size limit exceeded"));
        let decoded = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            5 * 4,
            &[filter()],
        )
        .unwrap();
        assert_eq!(decoded.len(), 4);
    }

    fn null_timestamp_block(action_first: bool) -> (Schema, Vec<u8>) {
        let schema = if action_first {
            parse_schema(
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "action", "type": "string"},
                        {"name": "ts", "type": "timestamp"}
                    ]
                }"#,
            )
            .unwrap()
        } else {
            parse_schema(
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "action", "type": "string"}
                    ]
                }"#,
            )
            .unwrap()
        };
        let (click_ts, view_ts) = if action_first {
            (
                vec![Scalar::Str("click".into()), Scalar::Timestamp(1000)],
                vec![Scalar::Str("view".into()), Scalar::Null],
            )
        } else {
            (
                vec![Scalar::Timestamp(1000), Scalar::Str("click".into())],
                vec![Scalar::Null, Scalar::Str("view".into())],
            )
        };
        let rows = vec![
            Row {
                values: click_ts,
                ts: 1000,
            },
            Row {
                values: view_ts,
                ts: 1000,
            },
        ];
        let encoded = encode_block(&schema, &rows).unwrap();
        (schema, encoded.bytes)
    }

    #[test]
    fn filtered_query_rejects_null_timestamp_on_constant_miss() {
        let (schema, bytes) = null_timestamp_block(false);
        let full = decode_block(&schema, &bytes).unwrap_err().to_string();
        assert!(full.contains("timestamp column is null or the wrong type"));
        let filter = ColumnPredicate {
            index: 0,
            allowed: vec![Scalar::Timestamp(999)],
        };
        let filtered = decode_rows_in_range_filtered(
            &schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[filter],
        )
        .unwrap_err()
        .to_string();
        assert!(filtered.contains("timestamp column is null or the wrong type"));
    }

    #[test]
    fn filtered_query_rejects_null_timestamp_on_a_dropped_row() {
        let (schema, bytes) = null_timestamp_block(true);
        let full = decode_block(&schema, &bytes).unwrap_err().to_string();
        assert!(full.contains("timestamp column is null or the wrong type"));
        let filter = ColumnPredicate {
            index: 0,
            allowed: vec![Scalar::Str("click".into())],
        };
        let filtered = decode_rows_in_range_filtered(
            &schema,
            &bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[filter],
        )
        .unwrap_err()
        .to_string();
        assert!(filtered.contains("timestamp column is null or the wrong type"));
    }

    fn extra_timestamp_event(seen_at_first: bool) -> (Schema, Vec<u8>) {
        let schema = if seen_at_first {
            parse_schema(
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "seen_at", "type": "timestamp"},
                        {"name": "ts", "type": "timestamp"},
                        {"name": "action", "type": "string"}
                    ]
                }"#,
            )
            .unwrap()
        } else {
            parse_schema(
                r#"{
                    "timestamp_field": "ts",
                    "fields": [
                        {"name": "ts", "type": "timestamp"},
                        {"name": "seen_at", "type": "timestamp"},
                        {"name": "action", "type": "string"}
                    ]
                }"#,
            )
            .unwrap()
        };
        let row = parse_event(&schema, br#"{"ts":1000,"action":"click"}"#).unwrap();
        let encoded = encode_block(&schema, &[row]).unwrap();
        (schema, encoded.bytes)
    }

    #[test]
    fn nullable_timestamp_column_is_not_the_event_timestamp() {
        let (schema, bytes) = extra_timestamp_event(true);
        let decoded = decode_block(&schema, &bytes).unwrap();
        assert!(matches!(decoded[0].values[0], Scalar::Null));
        let ranged = decode_rows_in_range(&schema, &bytes, i64::MIN, i64::MAX, usize::MAX).unwrap();
        assert_eq!(ranged.len(), 1);
        assert!(matches!(ranged[0].values[0], Scalar::Null));

        let (schema, bytes) = extra_timestamp_event(false);
        let ranged = decode_rows_in_range(&schema, &bytes, i64::MIN, i64::MAX, usize::MAX).unwrap();
        assert_eq!(ranged.len(), 1);
        assert!(matches!(ranged[0].values[1], Scalar::Null));
        let hit = ColumnPredicate {
            index: 2,
            allowed: vec![Scalar::Str("click".into())],
        };
        let rows =
            decode_rows_in_range_filtered(&schema, &bytes, i64::MIN, i64::MAX, usize::MAX, &[hit])
                .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(matches!(rows[0].values[1], Scalar::Null));
        let miss = ColumnPredicate {
            index: 2,
            allowed: vec![Scalar::Str("missing".into())],
        };
        let rows =
            decode_rows_in_range_filtered(&schema, &bytes, i64::MIN, i64::MAX, usize::MAX, &[miss])
                .unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn string_copies_are_charged_after_later_predicates() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "note", "type": "text"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let note = "n".repeat(32);
        let rows: Vec<_> = (0..4)
            .map(|id| {
                parse_event(
                    &schema,
                    format!(r#"{{"ts":1000,"note":"{note}","user_id":{id}}}"#).as_bytes(),
                )
                .unwrap()
            })
            .collect();
        let encoded = encode_block(&schema, &rows).unwrap();
        let predicates = || {
            vec![
                ColumnPredicate {
                    index: 1,
                    allowed: vec![Scalar::Str(note.clone())],
                },
                ColumnPredicate {
                    index: 2,
                    allowed: vec![Scalar::Int(1)],
                },
            ]
        };
        let err = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            note.len() - 1,
            &predicates(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("query response size limit exceeded"));
        let decoded = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            note.len(),
            &predicates(),
        )
        .unwrap();
        assert_eq!(decoded.len(), 1);
        assert!(matches!(&decoded[0].values[2], Scalar::Int(1)));
        assert!(matches!(&decoded[0].values[1], Scalar::Str(text) if text == &note));
    }

    fn assert_f64_bits(actual: &[f64], expected: &[f64]) {
        assert_eq!(actual.len(), expected.len());
        for (index, (left, right)) in actual.iter().zip(expected.iter()).enumerate() {
            assert_eq!(
                left.to_bits(),
                right.to_bits(),
                "bit mismatch at {index}: {left} vs {right}"
            );
        }
    }

    fn score_cycle(count: usize) -> Vec<f64> {
        (0..count)
            .map(|index| (index % 100) as f64 / 10.0)
            .collect()
    }

    #[test]
    fn repeated_decimal_stride_encodes_the_stock_score_cycle() {
        let scores = score_cycle(2048);
        let values: Vec<Option<f64>> = scores.iter().copied().map(Some).collect();
        let encoded = encode_f64s(&values);
        assert_eq!(encoded[0], KIND_FLOAT_REPEATED);
        assert_eq!(encoded[1], 1, "score tenths use exponent 1");
        assert_eq!(i64::from_le_bytes(encoded[2..10].try_into().unwrap()), 0);
        assert_eq!(i64::from_le_bytes(encoded[10..18].try_into().unwrap()), 1);
        assert_eq!(u32::from_le_bytes(encoded[18..22].try_into().unwrap()), 100);
        assert!(encoded.len() < 1 + scores.len() * 8);
        let mut cursor = 0;
        let decoded = decode_f64s(&encoded, &mut cursor, scores.len()).unwrap();
        assert_eq!(cursor, encoded.len());
        assert_f64_bits(&decoded, &scores);
        let mut skip = 0;
        skip_f64s(&encoded, &mut skip, scores.len()).unwrap();
        assert_eq!(skip, encoded.len());

        let short_tail = score_cycle(250);
        let values: Vec<Option<f64>> = short_tail.iter().copied().map(Some).collect();
        let encoded = encode_f64s(&values);
        assert_eq!(encoded[0], KIND_FLOAT_REPEATED);
        let mut cursor = 0;
        assert_f64_bits(
            &decode_f64s(&encoded, &mut cursor, short_tail.len()).unwrap(),
            &short_tail,
        );
    }

    #[test]
    fn constant_float_stays_constant_and_irregular_stays_raw() {
        let constant = vec![Some(1.5f64); 32];
        let encoded = encode_f64s(&constant);
        assert_eq!(encoded[0], KIND_FLOAT_CONSTANT);
        let mut cursor = 0;
        assert_f64_bits(
            &decode_f64s(&encoded, &mut cursor, constant.len()).unwrap(),
            &[1.5; 32],
        );

        let pure = (0..8)
            .map(|index| Some(index as f64 / 10.0))
            .collect::<Vec<_>>();
        let encoded = encode_f64s(&pure);
        assert_eq!(
            encoded[0], KIND_FLOAT_RAW,
            "one unrepeated stride stays the raw frame"
        );

        let irregular = vec![
            Some(0.1f64),
            Some(0.2),
            Some(0.4),
            Some(0.1),
            Some(0.2),
            Some(0.4),
        ];
        let encoded = encode_f64s(&irregular);
        assert_eq!(encoded[0], KIND_FLOAT_RAW);
        let mut cursor = 0;
        assert_f64_bits(
            &decode_f64s(&encoded, &mut cursor, irregular.len()).unwrap(),
            &[0.1, 0.2, 0.4, 0.1, 0.2, 0.4],
        );

        let non_finite = vec![
            Some(0.0f64),
            Some(f64::INFINITY),
            Some(0.0),
            Some(f64::INFINITY),
        ];
        assert_eq!(encode_f64s(&non_finite)[0], KIND_FLOAT_RAW);
    }

    #[test]
    fn repeated_decimal_stride_roundtrips_with_nulls_and_a_negative_cycle() {
        let mut values = Vec::new();
        let mut present_index = 0usize;
        for index in 0..30 {
            if index % 5 == 0 {
                values.push(None);
            } else {
                values.push(Some((present_index % 6) as f64 / 10.0));
                present_index += 1;
            }
        }
        let encoded = encode_f64s(&values);
        assert_eq!(encoded[0], KIND_FLOAT_REPEATED);
        let present: Vec<f64> = values.iter().copied().flatten().collect();
        let mut cursor = 0;
        assert_f64_bits(
            &decode_f64s(&encoded, &mut cursor, present.len()).unwrap(),
            &present,
        );

        let negative: Vec<f64> = (0..40)
            .map(|index| -1.5 + ((index % 4) as f64) * 0.25)
            .collect();
        let wrapped: Vec<Option<f64>> = negative.iter().copied().map(Some).collect();
        let encoded = encode_f64s(&wrapped);
        assert_eq!(encoded[0], KIND_FLOAT_REPEATED);
        assert_eq!(encoded[1], 2);
        let mut cursor = 0;
        assert_f64_bits(
            &decode_f64s(&encoded, &mut cursor, negative.len()).unwrap(),
            &negative,
        );

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "score", "type": "float"}
                ]
            }"#,
        )
        .unwrap();
        let mut rows = Vec::new();
        for (index, score) in values.iter().enumerate() {
            let mut obj = serde_json::json!({"ts": 1_000 + index as i64});
            if let Some(score) = score {
                obj["score"] = serde_json::json!(score);
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let block = encode_block(&schema, &rows).unwrap();
        let decoded = decode_block(&schema, &block.bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            match (&left.values[1], &right.values[1]) {
                (Scalar::Null, Scalar::Null) => {}
                (Scalar::Float(expected), Scalar::Float(actual)) => {
                    assert_eq!(expected.to_bits(), actual.to_bits());
                }
                _ => panic!("score column changed type"),
            }
        }
    }

    #[test]
    fn legacy_raw_f64_block_still_decodes() {
        let scores = [0.1f64, 1.25, -3.5, 0.0];
        let mut payload = vec![KIND_FLOAT_RAW];
        for score in scores {
            payload.extend_from_slice(&score.to_le_bytes());
        }
        let mut cursor = 0;
        assert_f64_bits(
            &decode_f64s(&payload, &mut cursor, scores.len()).unwrap(),
            &scores,
        );
        assert_eq!(cursor, payload.len());
        let mut skip = 0;
        skip_f64s(&payload, &mut skip, scores.len()).unwrap();
        assert_eq!(skip, payload.len());

        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "score", "type": "float"}
                ]
            }"#,
        )
        .unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.push(0);
        bytes.push(KIND_CONSTANT);
        bytes.extend_from_slice(&1000i64.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&payload);
        let rows = decode_block(&schema, &bytes).unwrap();
        for (row, score) in rows.iter().zip(scores.iter()) {
            match row.values[1] {
                Scalar::Float(value) => assert_eq!(value.to_bits(), score.to_bits()),
                _ => panic!("expected a float"),
            }
        }
    }

    #[test]
    fn repeated_float_stride_rejects_a_bad_exponent_and_period() {
        let mut bad_exp = vec![KIND_FLOAT_REPEATED, 19];
        bad_exp.extend_from_slice(&0i64.to_le_bytes());
        bad_exp.extend_from_slice(&1i64.to_le_bytes());
        bad_exp.extend_from_slice(&2u32.to_le_bytes());
        assert!(decode_f64s(&bad_exp, &mut 0, 6).is_err());
        assert!(skip_f64s(&bad_exp, &mut 0, 6).is_err());

        let mut bad_period = vec![KIND_FLOAT_REPEATED, 1];
        bad_period.extend_from_slice(&0i64.to_le_bytes());
        bad_period.extend_from_slice(&1i64.to_le_bytes());
        bad_period.extend_from_slice(&0u32.to_le_bytes());
        assert!(decode_f64s(&bad_period, &mut 0, 6)
            .unwrap_err()
            .to_string()
            .contains("period"));
        assert!(skip_f64s(&bad_period, &mut 0, 6).is_err());
    }

    #[test]
    fn repeating_null_bitmap_stores_one_period() {
        let nulls: Vec<bool> = (0..64).map(|index| index % 4 == 0).collect();
        let mut out = Vec::new();
        write_nulls(&mut out, &nulls);
        // Present bits of one period: row 0 null, then three present rows.
        assert_eq!(out, vec![NULL_PERIOD, 4, 0b0000_1110]);
        let mut cursor = 0;
        let present = read_present(&out, &mut cursor, nulls.len())
            .unwrap()
            .unwrap();
        assert_eq!(cursor, out.len());
        for (index, is_null) in nulls.iter().enumerate() {
            assert_eq!(present[index], !is_null);
        }

        // Period 9 crosses into a second pattern byte.
        let wide: Vec<bool> = (0..40).map(|index| index % 9 == 8).collect();
        let mut out = Vec::new();
        write_nulls(&mut out, &wide);
        assert_eq!(out[0], NULL_PERIOD);
        assert_eq!(out[1], 9);
        assert_eq!(out.len(), 4);
        let mut cursor = 0;
        let present = read_present(&out, &mut cursor, wide.len())
            .unwrap()
            .unwrap();
        assert_eq!(cursor, out.len());
        for (index, is_null) in wide.iter().enumerate() {
            assert_eq!(present[index], !is_null);
        }

        let all_null = vec![true; 40];
        let mut out = Vec::new();
        write_nulls(&mut out, &all_null);
        assert_eq!(out, vec![NULL_PERIOD, 1, 0]);
        let mut cursor = 0;
        let present = read_present(&out, &mut cursor, all_null.len())
            .unwrap()
            .unwrap();
        assert!(present.iter().all(|is_present| !is_present));
    }

    #[test]
    fn null_period_stays_full_when_it_does_not_shrink() {
        let tied: Vec<bool> = (0..16).map(|index| index % 2 == 0).collect();
        let mut out = Vec::new();
        write_nulls(&mut out, &tied);
        assert_eq!(out[0], 1);
        assert_eq!(out.len(), 1 + 16_usize.div_ceil(8));

        let mut irregular = vec![false; 40];
        for index in [0, 1, 3, 8, 17, 39] {
            irregular[index] = true;
        }
        let mut out = Vec::new();
        write_nulls(&mut out, &irregular);
        assert_eq!(out[0], 1);

        // Smallest cycle is 33, past the period cap, so the full bitmap stays.
        let capped: Vec<bool> = (0..80).map(|index| index % 33 == 0).collect();
        let mut out = Vec::new();
        write_nulls(&mut out, &capped);
        assert_eq!(out[0], 1);
        assert_eq!(out.len(), 1 + 80_usize.div_ceil(8));
        let mut cursor = 0;
        let present = read_present(&out, &mut cursor, capped.len())
            .unwrap()
            .unwrap();
        for (index, is_null) in capped.iter().enumerate() {
            assert_eq!(present[index], !is_null);
        }

        let mut none = Vec::new();
        write_nulls(&mut none, &[false; 40]);
        assert_eq!(none, vec![0]);
    }

    #[test]
    fn repeating_bool_bitmap_stores_one_period() {
        let values: Vec<Option<bool>> = (0..64).map(|index| Some(index % 2 == 0)).collect();
        let encoded = encode_bools(&values);
        assert_eq!(encoded, vec![BOOL_PERIOD, 2, 0b0000_0001]);
        let mut cursor = 0;
        let decoded = decode_bools(&encoded, &mut cursor, values.len()).unwrap();
        assert_eq!(cursor, encoded.len());
        assert_eq!(
            decoded,
            values.iter().copied().flatten().collect::<Vec<_>>()
        );
        let mut skip = 0;
        skip_bools(&encoded, &mut skip, values.len()).unwrap();
        assert_eq!(skip, encoded.len());

        let constant = vec![Some(true); 64];
        assert_eq!(encode_bools(&constant), vec![1, 1]);
        assert_eq!(encode_bools(&[]), vec![0]);

        // 16 alternating values tie the full bitmap, so kind 2 stays.
        let tied: Vec<Option<bool>> = (0..16).map(|index| Some(index % 2 == 0)).collect();
        let encoded = encode_bools(&tied);
        assert_eq!(encoded[0], 2);
        assert_eq!(encoded.len(), 1 + 16_usize.div_ceil(8));

        let noisy: Vec<Option<bool>> = (0..40)
            .map(|index| Some(index % 7 == 0 || index % 5 == 0))
            .collect();
        assert_eq!(encode_bools(&noisy)[0], 2);
    }

    #[test]
    fn legacy_null_bitmap_and_bool_bitmap_still_decode() {
        let nulls: Vec<bool> = (0..64).map(|index| index % 4 == 0).collect();
        let present_bits: Vec<bool> = nulls.iter().map(|is_null| !is_null).collect();
        let mut legacy = vec![1];
        legacy.extend_from_slice(&pack_present_bits(&present_bits));
        let mut cursor = 0;
        let present = read_present(&legacy, &mut cursor, nulls.len())
            .unwrap()
            .unwrap();
        assert_eq!(present, present_bits);

        let values: Vec<bool> = (0..64).map(|index| index % 2 == 0).collect();
        let mut legacy = vec![2];
        legacy.extend_from_slice(&pack_present_bits(&values));
        let mut cursor = 0;
        assert_eq!(
            decode_bools(&legacy, &mut cursor, values.len()).unwrap(),
            values
        );
        let mut skip = 0;
        skip_bools(&legacy, &mut skip, values.len()).unwrap();
        assert_eq!(skip, legacy.len());
    }

    #[test]
    fn bitmap_period_rejects_a_bad_period() {
        for bad in [0u8, 33, 255] {
            let nulls = vec![NULL_PERIOD, bad, 0];
            assert!(read_present(&nulls, &mut 0, 64)
                .unwrap_err()
                .to_string()
                .contains("period"));
            let bools = vec![BOOL_PERIOD, bad, 0];
            assert!(decode_bools(&bools, &mut 0, 64).is_err());
            assert!(skip_bools(&bools, &mut 0, 64).is_err());
        }
        let truncated = vec![NULL_PERIOD, 32];
        assert!(read_present(&truncated, &mut 0, 64).is_err());
        assert!(decode_bools(&truncated, &mut 0, 64).is_err());
    }

    #[test]
    fn periodic_null_and_bool_columns_reopen() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "ok", "type": "bool"},
                    {"name": "note", "type": "text"}
                ]
            }"#,
        )
        .unwrap();
        let mut rows = Vec::new();
        for index in 0..64i64 {
            let mut obj = serde_json::json!({
                "ts": 1_000 + index,
                "ok": index % 2 == 0,
            });
            if index % 4 != 0 {
                obj["note"] = serde_json::json!("n");
            }
            rows.push(parse_event(&schema, serde_json::to_vec(&obj).unwrap().as_slice()).unwrap());
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let mut cursor = 4;
        skip_column(
            FieldType::Timestamp,
            &encoded.bytes,
            &mut cursor,
            rows.len(),
            true,
        )
        .unwrap();
        let ok_at = cursor;
        assert_eq!(encoded.bytes[cursor], 0, "ok has no nulls");
        let _ = read_present(&encoded.bytes, &mut cursor, rows.len()).unwrap();
        assert_eq!(encoded.bytes[cursor], BOOL_PERIOD);
        cursor = ok_at;
        skip_column(
            FieldType::Bool,
            &encoded.bytes,
            &mut cursor,
            rows.len(),
            false,
        )
        .unwrap();
        assert_eq!(encoded.bytes[cursor], NULL_PERIOD);
        assert_eq!(encoded.bytes[cursor + 1], 4);

        let decoded = decode_block(&schema, &encoded.bytes).unwrap();
        for (left, right) in rows.iter().zip(decoded.iter()) {
            assert_eq!(left.values, right.values);
        }
        let trues = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[ColumnPredicate {
                index: 1,
                allowed: vec![Scalar::Bool(true)],
            }],
        )
        .unwrap();
        assert_eq!(trues.len(), 32);
        assert!(trues.iter().all(|row| row.values[1] == Scalar::Bool(true)));
        let missing = decode_rows_in_range_filtered(
            &schema,
            &encoded.bytes,
            i64::MIN,
            i64::MAX,
            usize::MAX,
            &[ColumnPredicate {
                index: 2,
                allowed: vec![Scalar::Null],
            }],
        )
        .unwrap();
        assert_eq!(missing.len(), 16);
        assert!(missing.iter().all(|row| row.values[2] == Scalar::Null));
    }

    #[test]
    fn count_uses_timestamps_and_leaves_later_columns_unread() {
        let schema = schema();
        let rows: Vec<Row> = [10i64, 20, 30]
            .into_iter()
            .enumerate()
            .map(|(index, ts)| {
                parse_event(
                    &schema,
                    format!(
                        r#"{{"ts":{ts},"user_id":{index},"score":1.5,"ok":true,"action":"click","note":"n","amount":"1.00"}}"#
                    )
                    .as_bytes(),
                )
                .unwrap()
            })
            .collect();
        let encoded = encode_block(&schema, &rows).unwrap();
        let nrows = block_row_count(&encoded.bytes).unwrap();
        let mut cursor = 4;
        skip_column(
            FieldType::Timestamp,
            &encoded.bytes,
            &mut cursor,
            nrows,
            true,
        )
        .unwrap();
        let mut torn = encoded.bytes[..cursor].to_vec();
        torn.extend_from_slice(&[0xff, 0xff, 0xff]);
        assert!(decode_block(&schema, &torn).is_err());
        assert_eq!(
            count_rows_in_range_filtered(&schema, &torn, 10, 30, &[]).unwrap(),
            3
        );
        assert_eq!(
            count_rows_in_range_filtered(&schema, &torn, 15, 25, &[]).unwrap(),
            1
        );
        let mut buckets = [0u64; 3];
        accumulate_histogram(&schema, &torn, 10, 30, &[], 10, 10, &mut buckets).unwrap();
        assert_eq!(buckets, [1, 1, 1]);

        let mut cursor = 4;
        skip_column(
            FieldType::Timestamp,
            &encoded.bytes,
            &mut cursor,
            nrows,
            true,
        )
        .unwrap();
        skip_column(FieldType::Int, &encoded.bytes, &mut cursor, nrows, false).unwrap();
        let mut pred_torn = encoded.bytes[..cursor].to_vec();
        pred_torn.extend_from_slice(&[0xff, 0xff]);
        let user = [ColumnPredicate {
            index: 1,
            allowed: vec![Scalar::Int(1)],
        }];
        assert!(
            decode_rows_in_range_filtered(&schema, &pred_torn, 0, 100, usize::MAX, &user).is_err()
        );
        assert_eq!(
            count_rows_in_range_filtered(&schema, &pred_torn, 0, 100, &user).unwrap(),
            1
        );

        let mut null_row = rows[0].clone();
        null_row.values[0] = Scalar::Null;
        let null_block = encode_block(&schema, &[null_row]).unwrap();
        let row_err = decode_block(&schema, &null_block.bytes).unwrap_err();
        let count_err =
            count_rows_in_range_filtered(&schema, &null_block.bytes, 0, 100, &[]).unwrap_err();
        assert_eq!(row_err.to_string(), count_err.to_string());
        assert!(count_err.to_string().contains("timestamp column is null"));
    }

    #[test]
    fn a_non_finite_float_stat_is_corrupt() {
        let schema = schema();
        let mut row = parse_event(
            &schema,
            br#"{"ts":1000,"user_id":1,"score":1.0,"ok":true,"action":"click","note":"n","amount":"1.00"}"#,
        )
        .unwrap();
        row.values[2] = Scalar::Float(f64::NAN);
        let encoded = encode_block(&schema, &[row]).unwrap();
        let mut captured = [CapturedColumn {
            index: 2,
            values: Vec::new(),
        }];
        let err = visit_matching_timestamps(
            &schema,
            &encoded.bytes,
            0,
            5000,
            &[],
            &[],
            &mut captured,
            |_ts, _row, _captured| Ok(()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not finite"), "{err}");
        let mut total = 0.0;
        assert_eq!(
            try_add_dense_float(&schema, &encoded.bytes, 2, 0, 5000, &mut total).unwrap(),
            None
        );
        assert_eq!(total.to_bits(), 0.0f64.to_bits());
        let err = visit_matching_timestamps(
            &schema,
            &encoded.bytes,
            0,
            5000,
            &[],
            &[],
            &mut captured,
            |_ts, _row, _captured| Ok(()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("not finite"), "{err}");
    }

    #[test]
    fn dense_float_sum_adds_in_row_order_and_leaves_a_cut_block() {
        let schema = schema();
        let mut rows = Vec::new();
        for i in 0..300 {
            let score = (i % 100) as f64 / 10.0;
            rows.push(float_row(&schema, i as i64, score, "click"));
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let mut total = 0.0;
        let added = try_add_dense_float(&schema, &encoded.bytes, 2, 0, 299, &mut total).unwrap();
        assert_eq!(added, Some(300));
        let mut expected = 0.0;
        for i in 0..300 {
            expected += (i % 100) as f64 / 10.0;
        }
        assert_eq!(total.to_bits(), expected.to_bits());

        let mut untouched = 3.5;
        assert_eq!(
            try_add_dense_float(&schema, &encoded.bytes, 2, 1, 299, &mut untouched).unwrap(),
            None
        );
        assert_eq!(untouched.to_bits(), 3.5f64.to_bits());

        let mut constant_rows = Vec::new();
        for i in 0..32 {
            constant_rows.push(float_row(&schema, i, 1.5, "click"));
        }
        let constant = encode_block(&schema, &constant_rows).unwrap();
        let mut constant_sum = 0.0;
        assert_eq!(
            try_add_dense_float(&schema, &constant.bytes, 2, 0, 31, &mut constant_sum).unwrap(),
            Some(32)
        );
        let mut constant_expected = 0.0f64;
        for _ in 0..32 {
            constant_expected += 1.5;
        }
        assert_eq!(constant_sum.to_bits(), constant_expected.to_bits());

        let mut raw_rows = Vec::new();
        for (index, score) in [0.1, 0.2, 0.4, 0.8].into_iter().enumerate() {
            raw_rows.push(float_row(&schema, index as i64, score, "click"));
        }
        let raw = encode_block(&schema, &raw_rows).unwrap();
        let mut raw_sum = 0.0;
        assert_eq!(
            try_add_dense_float(&schema, &raw.bytes, 2, 0, 3, &mut raw_sum).unwrap(),
            Some(4)
        );
        assert_eq!(raw_sum.to_bits(), (0.1 + 0.2 + 0.4 + 0.8f64).to_bits());

        let mut with_null = rows[0].clone();
        with_null.values[2] = Scalar::Null;
        let nullable = encode_block(&schema, &[with_null, rows[1].clone()]).unwrap();
        let mut null_sum = 4.0;
        assert_eq!(
            try_add_dense_float(&schema, &nullable.bytes, 2, 0, 10, &mut null_sum).unwrap(),
            None
        );
        assert_eq!(null_sum.to_bits(), 4.0f64.to_bits());

        let narrow = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "amount", "type": "decimal", "scale": 2}
                ]
            }"#,
        )
        .unwrap();
        let wide = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "amount", "type": "decimal", "scale": 2},
                    {"name": "score", "type": "float"}
                ]
            }"#,
        )
        .unwrap();
        let old = parse_event(&narrow, br#"{"ts":1,"user_id":1,"amount":"1.00"}"#).unwrap();
        let old_block = encode_block(&narrow, &[old]).unwrap();
        let mut missing = 7.0;
        assert_eq!(
            try_add_dense_float(&wide, &old_block.bytes, 3, 0, 10, &mut missing).unwrap(),
            None
        );
        assert_eq!(missing.to_bits(), 7.0f64.to_bits());
        let mut torn = old_block.bytes.clone();
        torn.push(0);
        let err = try_add_dense_float(&wide, &torn, 3, 0, 10, &mut missing).unwrap_err();
        assert!(err.to_string().contains("truncated"), "{err}");
    }

    fn float_row(schema: &Schema, ts: i64, score: f64, action: &str) -> Row {
        parse_event(
            schema,
            format!(
                r#"{{"ts":{ts},"user_id":1,"score":{score},"ok":true,"action":"{action}","note":"n","amount":"1.00"}}"#
            )
            .as_bytes(),
        )
        .unwrap()
    }

    #[test]
    fn captured_text_omits_rows_the_predicate_already_dropped() {
        let schema = schema();
        let mut rows = Vec::new();
        for (ts, action) in [(1000, "click"), (2000, "view"), (3000, "click")] {
            rows.push(
                parse_event(
                    &schema,
                    format!(
                        r#"{{"ts":{ts},"user_id":1,"score":1.0,"ok":true,"action":"{action}","note":"n","amount":"1.00"}}"#
                    )
                    .as_bytes(),
                )
                .unwrap(),
            );
        }
        let encoded = encode_block(&schema, &rows).unwrap();
        let predicate = [ColumnPredicate {
            index: 4,
            allowed: vec![Scalar::Str("click".into())],
        }];
        let mut captured = [CapturedColumn {
            index: 4,
            values: Vec::new(),
        }];
        visit_matching_timestamps(
            &schema,
            &encoded.bytes,
            0,
            5000,
            &predicate,
            &[],
            &mut captured,
            |_ts, _row, _captured| Ok(()),
        )
        .unwrap();
        assert_eq!(
            captured[0].values,
            vec![
                Scalar::Str("click".into()),
                Scalar::Null,
                Scalar::Str("click".into()),
            ]
        );
    }

    fn timestamp_only() -> Schema {
        parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [{"name": "ts", "type": "timestamp"}]
            }"#,
        )
        .unwrap()
    }

    fn rows_at(stamps: &[i64]) -> Vec<Row> {
        let schema = timestamp_only();
        stamps
            .iter()
            .map(|ts| parse_event(&schema, format!(r#"{{"ts":{ts}}}"#).as_bytes()).unwrap())
            .collect()
    }

    fn timestamp_kind(schema: &Schema, bytes: &[u8]) -> u8 {
        let nrows = block_row_count(bytes).unwrap();
        let mut cursor = 4usize;
        for (index, field) in schema.fields.iter().enumerate() {
            if index == schema.timestamp_index {
                assert_eq!(bytes[cursor], 0);
                return bytes[cursor + 1];
            }
            skip_column(field.ty, bytes, &mut cursor, nrows, false).unwrap();
        }
        panic!("timestamp column is missing");
    }

    fn walked_buckets(
        schema: &Schema,
        bytes: &[u8],
        from: i64,
        to: i64,
        bucket_ms: i64,
    ) -> Vec<u64> {
        let width = i128::from(bucket_ms);
        let first = i128::from(from).div_euclid(width) * width;
        let last = i128::from(to).div_euclid(width) * width;
        let n = ((last - first) / width + 1) as usize;
        let mut counts = vec![0u64; n];
        for ts in read_timestamp_column(schema, bytes).unwrap() {
            if ts < from || ts > to {
                continue;
            }
            let start = i128::from(ts).div_euclid(width) * width;
            counts[((start - first) / width) as usize] += 1;
        }
        counts
    }

    fn assert_histogram_matches_walk(
        schema: &Schema,
        bytes: &[u8],
        from: i64,
        to: i64,
        bucket_ms: i64,
    ) -> Vec<u64> {
        let expected = walked_buckets(schema, bytes, from, to, bucket_ms);
        let width = i128::from(bucket_ms);
        let first = i128::from(from).div_euclid(width) * width;
        let mut counts = vec![0u64; expected.len()];
        accumulate_histogram(schema, bytes, from, to, &[], bucket_ms, first, &mut counts).unwrap();
        assert_eq!(counts, expected);
        counts
    }

    #[test]
    fn histogram_fills_a_positive_stride_without_expanding_it() {
        let schema = timestamp_only();
        let base = 1_700_000_000_000i64;
        let stamps: Vec<i64> = (0..2048).map(|i| base + i * 10).collect();
        let block = encode_block(&schema, &rows_at(&stamps)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_STRIDE);
        let last = base + 2047 * 10;
        assert!(plain_timestamp_run(&schema, &block.bytes, base, last)
            .unwrap()
            .is_some());
        let counts = assert_histogram_matches_walk(&schema, &block.bytes, base, last, 1000);
        assert_eq!(counts.len(), 21);
        assert_eq!(&counts[..20], &[100u64; 20]);
        assert_eq!(counts[20], 48);
        assert_eq!(counts.iter().sum::<u64>(), 2048);

        let uneven: Vec<i64> = (0..10).map(|i| 5 + i * 3).collect();
        let block = encode_block(&schema, &rows_at(&uneven)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_STRIDE);
        assert!(plain_timestamp_run(&schema, &block.bytes, 5, 32)
            .unwrap()
            .is_some());
        assert_eq!(
            assert_histogram_matches_walk(&schema, &block.bytes, 5, 32, 10),
            vec![2, 3, 4, 1]
        );

        let crossed: Vec<i64> = (0..32).map(|i| -25 + i * 10).collect();
        let block = encode_block(&schema, &rows_at(&crossed)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_STRIDE);
        let crossed_last = -25 + 31 * 10;
        assert!(
            plain_timestamp_run(&schema, &block.bytes, -25, crossed_last)
                .unwrap()
                .is_some()
        );
        let counts = assert_histogram_matches_walk(&schema, &block.bytes, -25, crossed_last, 10);
        assert_eq!(counts.len(), 32);
        assert!(counts.iter().all(|count| *count == 1));

        let constant = vec![base; 7];
        let block = encode_block(&schema, &rows_at(&constant)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_CONSTANT);
        assert!(plain_timestamp_run(&schema, &block.bytes, base, base)
            .unwrap()
            .is_some());
        assert_eq!(
            assert_histogram_matches_walk(&schema, &block.bytes, base, base, 1000),
            vec![7]
        );

        let early = vec![-1500i64; 4];
        let block = encode_block(&schema, &rows_at(&early)).unwrap();
        assert!(plain_timestamp_run(&schema, &block.bytes, -1500, -100)
            .unwrap()
            .is_some());
        assert_eq!(
            assert_histogram_matches_walk(&schema, &block.bytes, -1500, -100, 1000),
            vec![4, 0]
        );
    }

    #[test]
    fn histogram_walks_when_the_stride_is_not_a_plain_run() {
        let schema = timestamp_only();
        let positive: Vec<i64> = (0..12).map(|i| 100 + i * 10).collect();
        let block = encode_block(&schema, &rows_at(&positive)).unwrap();
        assert!(plain_timestamp_run(&schema, &block.bytes, 150, 180)
            .unwrap()
            .is_none());
        let cut = assert_histogram_matches_walk(&schema, &block.bytes, 150, 180, 10);
        assert_eq!(cut.iter().sum::<u64>(), 4);

        let decreasing: Vec<i64> = (0..30).map(|i| 5_000 - i * 15).collect();
        let block = encode_block(&schema, &rows_at(&decreasing)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_STRIDE);
        assert!(
            plain_timestamp_run(&schema, &block.bytes, decreasing[29], decreasing[0])
                .unwrap()
                .is_none()
        );
        let counts = assert_histogram_matches_walk(
            &schema,
            &block.bytes,
            decreasing[29],
            decreasing[0],
            100,
        );
        assert_eq!(counts.iter().sum::<u64>(), 30);

        let base = i64::MAX - 40;
        let wrapped: Vec<i64> = (0..6).map(|i| base.wrapping_add(i * 30)).collect();
        let block = encode_block(&schema, &rows_at(&wrapped)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_STRIDE);
        let from = *wrapped.iter().min().unwrap();
        let to = *wrapped.iter().max().unwrap();
        assert!(plain_timestamp_run(&schema, &block.bytes, from, to)
            .unwrap()
            .is_none());
        assert_eq!(
            assert_histogram_matches_walk(&schema, &block.bytes, from, to, 1 << 62),
            vec![4, 0, 0, 2]
        );

        let pieces: Vec<i64> = (0..200).map(|i| (i % 50) as i64).collect();
        let block = encode_block(&schema, &rows_at(&pieces)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_PIECES);
        assert!(plain_timestamp_run(&schema, &block.bytes, 0, 49)
            .unwrap()
            .is_none());
        let counts = assert_histogram_matches_walk(&schema, &block.bytes, 0, 49, 10);
        assert_eq!(counts.iter().sum::<u64>(), 200);

        let packed = [0i64, 999, 1];
        let block = encode_block(&schema, &rows_at(&packed)).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_BITPACK);
        assert!(plain_timestamp_run(&schema, &block.bytes, 0, 999)
            .unwrap()
            .is_none());
        assert_eq!(
            assert_histogram_matches_walk(&schema, &block.bytes, 0, 999, 500),
            vec![2, 1]
        );

        let mut null_row = rows_at(&[10])[0].clone();
        null_row.values[0] = Scalar::Null;
        let null_block = encode_block(&schema, &[null_row]).unwrap();
        assert!(plain_timestamp_run(&schema, &null_block.bytes, 0, 100)
            .unwrap()
            .is_none());
        let err = accumulate_histogram(&schema, &null_block.bytes, 0, 100, &[], 10, 0, &mut [0])
            .unwrap_err();
        assert!(
            err.to_string().contains("timestamp column is null"),
            "{err}"
        );
    }

    #[test]
    fn histogram_predicates_and_an_earlier_column_still_agree_with_the_walk() {
        let schema = schema();
        let mut rows = Vec::new();
        for index in 0..16 {
            let action = if index % 2 == 0 { "click" } else { "view" };
            rows.push(
                parse_event(
                    &schema,
                    format!(
                        r#"{{"ts":{},"user_id":{index},"score":1.0,"ok":true,"action":"{action}","note":"n","amount":"1.00"}}"#,
                        1_000 + index * 25
                    )
                    .as_bytes(),
                )
                .unwrap(),
            );
        }
        let block = encode_block(&schema, &rows).unwrap();
        assert_eq!(timestamp_kind(&schema, &block.bytes), KIND_STRIDE);
        let from = 1_000i64;
        let to = 1_000 + 15 * 25;
        assert!(plain_timestamp_run(&schema, &block.bytes, from, to)
            .unwrap()
            .is_some());
        let predicate = [ColumnPredicate {
            index: 4,
            allowed: vec![Scalar::Str("click".into())],
        }];
        let range = [RangeFilter {
            index: 1,
            lower: Some(RangeEnd {
                inclusive: true,
                value: Scalar::Int(8),
            }),
            upper: None,
        }];
        for (predicates, ranges) in [(&predicate[..], &[][..]), (&[][..], &range[..])] {
            let rows = decode_rows_filtered(
                &schema,
                &block.bytes,
                from,
                to,
                usize::MAX,
                predicates,
                ranges,
            )
            .unwrap();
            let width = 50i128;
            let first = i128::from(from).div_euclid(width) * width;
            let last = i128::from(to).div_euclid(width) * width;
            let n = ((last - first) / width + 1) as usize;
            let mut expected = vec![0u64; n];
            for row in &rows {
                let start = i128::from(row.ts).div_euclid(width) * width;
                expected[((start - first) / width) as usize] += 1;
            }
            let mut counts = vec![0u64; n];
            accumulate_histogram_filtered(
                &schema,
                &block.bytes,
                from,
                to,
                predicates,
                ranges,
                50,
                first,
                &mut counts,
            )
            .unwrap();
            assert_eq!(counts, expected);
            assert!(counts.iter().sum::<u64>() < 16);
        }

        let later = parse_schema(
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
        for index in 0..12 {
            rows.push(
                parse_event(
                    &later,
                    format!(r#"{{"user_id":{index},"ts":{}}}"#, 40 + index * 7).as_bytes(),
                )
                .unwrap(),
            );
        }
        let block = encode_block(&later, &rows).unwrap();
        assert_eq!(timestamp_kind(&later, &block.bytes), KIND_STRIDE);
        assert!(plain_timestamp_run(&later, &block.bytes, 40, 40 + 11 * 7)
            .unwrap()
            .is_some());
        assert_eq!(
            assert_histogram_matches_walk(&later, &block.bytes, 40, 40 + 11 * 7, 20)
                .iter()
                .sum::<u64>(),
            12
        );
    }
}
