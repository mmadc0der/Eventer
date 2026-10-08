//! Per-block presence summaries stored in a version-2 sparse index.
//!
//! Numeric columns keep a non-null min/max. String columns keep an exact set
//! when the block has at most 16 distinct values, and a 32-byte split-block
//! bloom otherwise. A summary miss is final. A hit still runs the exact
//! predicate after the block is decompressed.

use xxhash_rust::xxh64::xxh64;

use crate::schema::{FieldType, Schema};
use crate::value::{Row, Scalar};

const MAX_EXACT_STRINGS: usize = 16;
const BLOOM_BYTES: usize = 32;

/// `read_index` refuses a larger blob. Writers must stay at or under this so a
/// sealed block can be opened again without rebuilding the segment.
pub(crate) const MAX_SUMMARY_LEN: usize = 8 * 1024 * 1024;
const HAS_NULL: u8 = 0b0000_0001;

const KIND_ALL_NULL: u8 = 0;
const KIND_I64: u8 = 1;
const KIND_I128: u8 = 2;
const KIND_EXACT: u8 = 3;
const KIND_BLOOM: u8 = 4;

/// Odd salts from the Parquet split-block bloom filter.
const SALT: [u32; 8] = [
    0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31,
];

/// One equality or `In` predicate, already checked against the schema.
pub(crate) struct SummaryPredicate<'a> {
    pub field_index: usize,
    pub allowed: &'a [Scalar],
}

#[derive(Clone)]
enum Body {
    AllNull,
    I64 {
        has_null: bool,
        min: i64,
        max: i64,
    },
    I128 {
        has_null: bool,
        min: i128,
        max: i128,
    },
    Exact {
        has_null: bool,
        values: Vec<String>,
    },
    Bloom {
        has_null: bool,
        words: [u32; 8],
    },
}

struct Column {
    field_index: usize,
    body: Body,
}

/// Bytes appended to one version-2 index entry.
pub(crate) fn summarize(schema: &Schema, rows: &[Row]) -> Vec<u8> {
    let mut encoded = Vec::new();
    let mut count = 0u16;
    for (index, field) in schema.fields.iter().enumerate() {
        if index > u16::MAX as usize {
            continue;
        }
        let used = 2 + encoded.len();
        if used >= MAX_SUMMARY_LEN {
            break;
        }
        let max_body = MAX_SUMMARY_LEN - used - 2;
        let Some(body) = summarize_column(field.ty, rows, index, max_body) else {
            continue;
        };
        count = count.saturating_add(1);
        encoded.extend_from_slice(&(index as u16).to_le_bytes());
        encoded.extend_from_slice(&body);
    }
    let mut out = Vec::with_capacity(2 + encoded.len());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&encoded);
    out
}

/// `true` when every predicate might match this summary.
///
/// An empty summary, a column with no summary, or a summary that does not
/// parse returns `true`. Callers then decompress and run the exact predicate.
pub(crate) fn might_match(summary: &[u8], predicates: &[SummaryPredicate<'_>]) -> bool {
    if summary.is_empty() || predicates.is_empty() {
        return true;
    }
    if predicates.iter().any(|predicate| predicate.allowed.is_empty()) {
        return false;
    }
    // A summary that does not parse is not a miss. Only columns named by a
    // predicate are decoded; other exact sets are skipped without copying them.
    match column_misses(summary, predicates) {
        Some(miss) => !miss,
        None => true,
    }
}

/// `Some` when `field_index` is a string bloom. Used to force a false positive.
#[cfg(test)]
pub(crate) fn bloom_may_contain(summary: &[u8], field_index: usize, text: &str) -> Option<bool> {
    let columns = parse(summary)?;
    let column = columns
        .iter()
        .find(|column| column.field_index == field_index)?;
    match &column.body {
        Body::Bloom { words, .. } => Some(bloom_contains(words, text.as_bytes())),
        _ => None,
    }
}

fn summarize_column(ty: FieldType, rows: &[Row], index: usize, max_body: usize) -> Option<Vec<u8>> {
    let body = match ty {
        FieldType::Int => summarize_i64(rows, index, false)?,
        FieldType::Timestamp => summarize_i64(rows, index, true)?,
        FieldType::Decimal { .. } => summarize_i128(rows, index)?,
        FieldType::String => return summarize_string(rows, index, max_body),
        FieldType::Float | FieldType::Bool | FieldType::Text | FieldType::Json => return None,
    };
    if body.len() <= max_body {
        Some(body)
    } else {
        None
    }
}

fn summarize_i64(rows: &[Row], index: usize, timestamp: bool) -> Option<Vec<u8>> {
    let mut has_null = false;
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut any = false;
    for row in rows {
        let value = row.values.get(index)?;
        match value {
            Scalar::Null => has_null = true,
            Scalar::Int(number) if !timestamp => {
                any = true;
                min = min.min(*number);
                max = max.max(*number);
            }
            Scalar::Timestamp(number) if timestamp => {
                any = true;
                min = min.min(*number);
                max = max.max(*number);
            }
            _ => return None,
        }
    }
    Some(encode_i64(has_null, any, min, max))
}

fn encode_i64(has_null: bool, any: bool, min: i64, max: i64) -> Vec<u8> {
    if !any {
        return vec![KIND_ALL_NULL];
    }
    let mut out = Vec::with_capacity(1 + 1 + 16);
    out.push(KIND_I64);
    out.push(u8::from(has_null));
    out.extend_from_slice(&min.to_le_bytes());
    out.extend_from_slice(&max.to_le_bytes());
    out
}

fn summarize_i128(rows: &[Row], index: usize) -> Option<Vec<u8>> {
    let mut has_null = false;
    let mut min = i128::MAX;
    let mut max = i128::MIN;
    let mut any = false;
    for row in rows {
        match row.values.get(index)? {
            Scalar::Null => has_null = true,
            Scalar::Decimal(number) => {
                any = true;
                min = min.min(*number);
                max = max.max(*number);
            }
            _ => return None,
        }
    }
    if !any {
        return Some(vec![KIND_ALL_NULL]);
    }
    let mut out = Vec::with_capacity(1 + 1 + 32);
    out.push(KIND_I128);
    out.push(u8::from(has_null));
    out.extend_from_slice(&min.to_le_bytes());
    out.extend_from_slice(&max.to_le_bytes());
    Some(out)
}

fn summarize_string(rows: &[Row], index: usize, max_body: usize) -> Option<Vec<u8>> {
    let mut has_null = false;
    // Borrow the row text. Sixteen values are compared linearly; past that the
    // block switches to a bloom and the borrowed set is dropped.
    let mut distinct: Vec<&str> = Vec::new();
    let mut bloom: Option<[u32; 8]> = None;
    for row in rows {
        match row.values.get(index)? {
            Scalar::Null => has_null = true,
            Scalar::Str(text) => {
                if let Some(words) = bloom.as_mut() {
                    bloom_insert(words, text.as_bytes());
                    continue;
                }
                if distinct.iter().any(|value| *value == text) {
                    continue;
                }
                if distinct.len() == MAX_EXACT_STRINGS {
                    let mut words = [0u32; 8];
                    for value in &distinct {
                        bloom_insert(&mut words, value.as_bytes());
                    }
                    bloom_insert(&mut words, text.as_bytes());
                    bloom = Some(words);
                    distinct.clear();
                } else {
                    distinct.push(text.as_str());
                }
            }
            _ => return None,
        }
    }
    if let Some(words) = bloom {
        return encode_bloom(has_null, words, max_body);
    }
    if distinct.is_empty() {
        return fits(vec![KIND_ALL_NULL], max_body);
    }
    let exact_len = 3 + distinct.iter().map(|value| 4 + value.len()).sum::<usize>();
    if exact_len > max_body {
        let mut words = [0u32; 8];
        for value in &distinct {
            bloom_insert(&mut words, value.as_bytes());
        }
        return encode_bloom(has_null, words, max_body);
    }
    let mut values: Vec<&str> = distinct;
    values.sort_unstable();
    let mut out = Vec::with_capacity(exact_len);
    out.push(KIND_EXACT);
    out.push(u8::from(has_null));
    out.push(values.len() as u8);
    for value in values {
        let bytes = value.as_bytes();
        let len = u32::try_from(bytes.len()).ok()?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(bytes);
    }
    debug_assert!(out.len() <= max_body);
    Some(out)
}

fn encode_bloom(has_null: bool, words: [u32; 8], max_body: usize) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(2 + BLOOM_BYTES);
    out.push(KIND_BLOOM);
    out.push(u8::from(has_null));
    out.extend_from_slice(&bloom_bytes(&words));
    fits(out, max_body)
}

fn fits(body: Vec<u8>, max_body: usize) -> Option<Vec<u8>> {
    if body.len() <= max_body {
        Some(body)
    } else {
        None
    }
}

/// `Some(true)` when a named column cannot contain its predicate.
/// `None` when the summary bytes are not a valid column list.
fn column_misses(summary: &[u8], predicates: &[SummaryPredicate<'_>]) -> Option<bool> {
    if summary.len() < 2 {
        return None;
    }
    let count = u16::from_le_bytes(summary[0..2].try_into().ok()?) as usize;
    let mut cursor = 2;
    let mut matched = vec![false; predicates.len()];
    for _ in 0..count {
        if cursor + 3 > summary.len() {
            return None;
        }
        let field_index = u16::from_le_bytes(summary[cursor..cursor + 2].try_into().ok()?) as usize;
        cursor += 2;
        let kind = summary[cursor];
        cursor += 1;
        let needed: Vec<usize> = predicates
            .iter()
            .enumerate()
            .filter(|(index, predicate)| {
                !matched[*index] && predicate.field_index == field_index
            })
            .map(|(index, _)| index)
            .collect();
        if needed.is_empty() {
            cursor = skip_body(summary, cursor, kind)?;
            continue;
        }
        let (miss, next) = body_misses(summary, cursor, kind, predicates, &needed)?;
        cursor = next;
        if miss {
            return Some(true);
        }
        for index in needed {
            matched[index] = true;
        }
    }
    if cursor != summary.len() {
        return None;
    }
    Some(false)
}

fn skip_body(summary: &[u8], cursor: usize, kind: u8) -> Option<usize> {
    match kind {
        KIND_ALL_NULL => Some(cursor),
        KIND_I64 => read_i64_zone(summary, cursor).map(|(_, _, _, next)| next),
        KIND_I128 => read_i128_zone(summary, cursor).map(|(_, _, _, next)| next),
        KIND_EXACT => skip_exact(summary, cursor),
        KIND_BLOOM => read_bloom(summary, cursor).map(|(_, _, next)| next),
        _ => None,
    }
}

fn body_misses(
    summary: &[u8],
    cursor: usize,
    kind: u8,
    predicates: &[SummaryPredicate<'_>],
    needed: &[usize],
) -> Option<(bool, usize)> {
    match kind {
        KIND_ALL_NULL => {
            let miss = needed.iter().any(|index| {
                !predicates[*index]
                    .allowed
                    .iter()
                    .any(|value| matches!(value, Scalar::Null))
            });
            Some((miss, cursor))
        }
        KIND_I64 => {
            let (has_null, min, max, next) = read_i64_zone(summary, cursor)?;
            let body = Body::I64 { has_null, min, max };
            Some((predicate_miss(&body, predicates, needed), next))
        }
        KIND_I128 => {
            let (has_null, min, max, next) = read_i128_zone(summary, cursor)?;
            let body = Body::I128 { has_null, min, max };
            Some((predicate_miss(&body, predicates, needed), next))
        }
        KIND_EXACT => exact_misses(summary, cursor, predicates, needed),
        KIND_BLOOM => {
            let (has_null, words, next) = read_bloom(summary, cursor)?;
            let body = Body::Bloom { has_null, words };
            Some((predicate_miss(&body, predicates, needed), next))
        }
        _ => None,
    }
}

fn predicate_miss(body: &Body, predicates: &[SummaryPredicate<'_>], needed: &[usize]) -> bool {
    needed
        .iter()
        .any(|index| !column_may_contain(body, predicates[*index].allowed))
}

fn skip_exact(summary: &[u8], cursor: usize) -> Option<usize> {
    let (_, next) = walk_exact(summary, cursor, None)?;
    Some(next)
}

fn exact_misses(
    summary: &[u8],
    cursor: usize,
    predicates: &[SummaryPredicate<'_>],
    needed: &[usize],
) -> Option<(bool, usize)> {
    let (hit, next) = walk_exact(summary, cursor, Some((predicates, needed)))?;
    Some((!hit, next))
}

/// Walk one exact set. When `needed` is set, `hit` is whether every named
/// predicate contains a value in the set. Otherwise `hit` is unused.
fn walk_exact(
    summary: &[u8],
    cursor: usize,
    needed: Option<(&[SummaryPredicate<'_>], &[usize])>,
) -> Option<(bool, usize)> {
    if cursor + 2 > summary.len() {
        return None;
    }
    let has_null = summary[cursor] & HAS_NULL != 0;
    let count = summary[cursor + 1] as usize;
    if count == 0 || count > MAX_EXACT_STRINGS {
        return None;
    }
    let mut cursor = cursor + 2;
    let mut satisfied = vec![false; needed.map(|(_, indexes)| indexes.len()).unwrap_or(0)];
    for _ in 0..count {
        if cursor + 4 > summary.len() {
            return None;
        }
        let len = u32::from_le_bytes(summary[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        if cursor + len > summary.len() {
            return None;
        }
        let text = std::str::from_utf8(&summary[cursor..cursor + len]).ok()?;
        if let Some((predicates, indexes)) = needed {
            for (slot, index) in indexes.iter().enumerate() {
                if predicates[*index].allowed.iter().any(|value| match value {
                    Scalar::Str(candidate) => candidate == text,
                    _ => false,
                }) {
                    satisfied[slot] = true;
                }
            }
        }
        cursor += len;
    }
    let hit = match needed {
        None => true,
        Some((predicates, indexes)) => indexes.iter().enumerate().all(|(slot, index)| {
            satisfied[slot]
                || predicates[*index].allowed.iter().any(|value| match value {
                    Scalar::Null => has_null,
                    Scalar::Str(_) => false,
                    _ => true,
                })
        }),
    };
    Some((hit, cursor))
}

fn column_may_contain(body: &Body, allowed: &[Scalar]) -> bool {
    match body {
        Body::AllNull => allowed.iter().any(|value| matches!(value, Scalar::Null)),
        Body::I64 { has_null, min, max } => allowed.iter().any(|value| match value {
            Scalar::Null => *has_null,
            Scalar::Int(number) | Scalar::Timestamp(number) => *number >= *min && *number <= *max,
            _ => true,
        }),
        Body::I128 { has_null, min, max } => allowed.iter().any(|value| match value {
            Scalar::Null => *has_null,
            Scalar::Decimal(number) => *number >= *min && *number <= *max,
            _ => true,
        }),
        Body::Exact { has_null, values } => allowed.iter().any(|value| match value {
            Scalar::Null => *has_null,
            Scalar::Str(text) => values.iter().any(|candidate| candidate == text),
            _ => true,
        }),
        Body::Bloom { has_null, words } => allowed.iter().any(|value| match value {
            Scalar::Null => *has_null,
            Scalar::Str(text) => bloom_contains(words, text.as_bytes()),
            _ => true,
        }),
    }
}

fn parse(summary: &[u8]) -> Option<Vec<Column>> {
    if summary.len() < 2 {
        return None;
    }
    let count = u16::from_le_bytes(summary[0..2].try_into().ok()?) as usize;
    let mut cursor = 2;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor + 3 > summary.len() {
            return None;
        }
        let field_index = u16::from_le_bytes(summary[cursor..cursor + 2].try_into().ok()?) as usize;
        cursor += 2;
        let kind = summary[cursor];
        cursor += 1;
        let body = match kind {
            KIND_ALL_NULL => Body::AllNull,
            KIND_I64 => {
                let (has_null, min, max, next) = read_i64_zone(summary, cursor)?;
                cursor = next;
                Body::I64 { has_null, min, max }
            }
            KIND_I128 => {
                let (has_null, min, max, next) = read_i128_zone(summary, cursor)?;
                cursor = next;
                Body::I128 { has_null, min, max }
            }
            KIND_EXACT => {
                let (has_null, values, next) = read_exact(summary, cursor)?;
                cursor = next;
                Body::Exact { has_null, values }
            }
            KIND_BLOOM => {
                let (has_null, words, next) = read_bloom(summary, cursor)?;
                cursor = next;
                Body::Bloom { has_null, words }
            }
            _ => return None,
        };
        columns.push(Column { field_index, body });
    }
    if cursor != summary.len() {
        return None;
    }
    Some(columns)
}

fn read_i64_zone(summary: &[u8], cursor: usize) -> Option<(bool, i64, i64, usize)> {
    if cursor + 1 + 16 > summary.len() {
        return None;
    }
    let has_null = summary[cursor] & HAS_NULL != 0;
    let min = i64::from_le_bytes(summary[cursor + 1..cursor + 9].try_into().ok()?);
    let max = i64::from_le_bytes(summary[cursor + 9..cursor + 17].try_into().ok()?);
    if min > max {
        return None;
    }
    Some((has_null, min, max, cursor + 17))
}

fn read_i128_zone(summary: &[u8], cursor: usize) -> Option<(bool, i128, i128, usize)> {
    if cursor + 1 + 32 > summary.len() {
        return None;
    }
    let has_null = summary[cursor] & HAS_NULL != 0;
    let min = i128::from_le_bytes(summary[cursor + 1..cursor + 17].try_into().ok()?);
    let max = i128::from_le_bytes(summary[cursor + 17..cursor + 33].try_into().ok()?);
    if min > max {
        return None;
    }
    Some((has_null, min, max, cursor + 33))
}

fn read_exact(summary: &[u8], cursor: usize) -> Option<(bool, Vec<String>, usize)> {
    if cursor + 2 > summary.len() {
        return None;
    }
    let has_null = summary[cursor] & HAS_NULL != 0;
    let count = summary[cursor + 1] as usize;
    if count == 0 || count > MAX_EXACT_STRINGS {
        return None;
    }
    let mut cursor = cursor + 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor + 4 > summary.len() {
            return None;
        }
        let len = u32::from_le_bytes(summary[cursor..cursor + 4].try_into().ok()?) as usize;
        cursor += 4;
        if cursor + len > summary.len() {
            return None;
        }
        let text = std::str::from_utf8(&summary[cursor..cursor + len]).ok()?;
        values.push(text.to_string());
        cursor += len;
    }
    Some((has_null, values, cursor))
}

fn read_bloom(summary: &[u8], cursor: usize) -> Option<(bool, [u32; 8], usize)> {
    if cursor + 1 + BLOOM_BYTES > summary.len() {
        return None;
    }
    let has_null = summary[cursor] & HAS_NULL != 0;
    let mut words = [0u32; 8];
    for (index, word) in words.iter_mut().enumerate() {
        let start = cursor + 1 + index * 4;
        *word = u32::from_le_bytes(summary[start..start + 4].try_into().ok()?);
    }
    Some((has_null, words, cursor + 1 + BLOOM_BYTES))
}

fn bloom_bytes(words: &[u32; 8]) -> [u8; BLOOM_BYTES] {
    let mut out = [0u8; BLOOM_BYTES];
    for (index, word) in words.iter().enumerate() {
        out[index * 4..index * 4 + 4].copy_from_slice(&word.to_le_bytes());
    }
    out
}

fn bloom_insert(words: &mut [u32; 8], bytes: &[u8]) {
    let masks = bloom_masks(bytes);
    for lane in 0..8 {
        words[lane] |= masks[lane];
    }
}

fn bloom_contains(words: &[u32; 8], bytes: &[u8]) -> bool {
    let masks = bloom_masks(bytes);
    (0..8).all(|lane| words[lane] & masks[lane] != 0)
}

fn bloom_masks(bytes: &[u8]) -> [u32; 8] {
    let hash = xxh64(bytes, 0);
    // One 32-byte block, so the high half always selects block 0.
    let key = hash as u32;
    let mut masks = [0u32; 8];
    for lane in 0..8 {
        let bit = key.wrapping_mul(SALT[lane]) >> 27;
        masks[lane] = 1u32 << bit;
    }
    masks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;

    fn row(ts: i64, user: Scalar, action: Scalar) -> Row {
        Row {
            ts,
            values: vec![Scalar::Timestamp(ts), user, action],
        }
    }

    fn schema() -> Schema {
        parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"},
                    {"name": "action", "type": "string"}
                ]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn zone_map_rejects_values_outside_the_non_null_range() {
        let rows = vec![
            row(10, Scalar::Int(5), Scalar::Str("click".into())),
            row(12, Scalar::Null, Scalar::Str("click".into())),
        ];
        let summary = summarize(&schema(), &rows);
        let miss = [Scalar::Int(4)];
        assert!(!might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 1,
                allowed: &miss,
            }]
        ));
        let hit = [Scalar::Int(5)];
        assert!(might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 1,
                allowed: &hit,
            }]
        ));
        let null_only = [Scalar::Null];
        assert!(might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 1,
                allowed: &null_only,
            }]
        ));
    }

    #[test]
    fn null_equality_misses_a_block_with_no_nulls() {
        let rows = vec![row(1, Scalar::Int(3), Scalar::Str("view".into()))];
        let summary = summarize(&schema(), &rows);
        let null_only = [Scalar::Null];
        assert!(!might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 1,
                allowed: &null_only,
            }]
        ));
    }

    #[test]
    fn exact_string_set_rejects_an_absent_value() {
        let rows = vec![
            row(1, Scalar::Int(1), Scalar::Str("click".into())),
            row(2, Scalar::Int(1), Scalar::Str("view".into())),
        ];
        let summary = summarize(&schema(), &rows);
        let miss = [Scalar::Str("buy".into())];
        assert!(!might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 2,
                allowed: &miss,
            }]
        ));
        assert!(bloom_may_contain(&summary, 2, "click").is_none());
    }

    #[test]
    fn bloom_has_no_false_negative_and_can_false_positive() {
        let mut rows = Vec::new();
        for index in 0..64 {
            rows.push(row(
                index,
                Scalar::Int(index),
                Scalar::Str(format!("action-{index}")),
            ));
        }
        let summary = summarize(&schema(), &rows);
        for index in 0..64 {
            let text = format!("action-{index}");
            assert_eq!(bloom_may_contain(&summary, 2, &text), Some(true), "{text}");
        }
        let mut found = None;
        for index in 0..20_000 {
            let text = format!("missing-{index}");
            if bloom_may_contain(&summary, 2, &text) == Some(true) {
                found = Some(text);
                break;
            }
        }
        let text = found.expect("32-byte bloom should false-positive");
        let allowed = [Scalar::Str(text)];
        assert!(might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 2,
                allowed: &allowed,
            }]
        ));
    }

    #[test]
    fn unsummarized_columns_do_not_reject_the_block() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "note", "type": "text"}
                ]
            }"#,
        )
        .unwrap();
        let rows = vec![Row {
            ts: 1,
            values: vec![Scalar::Timestamp(1), Scalar::Str("hello".into())],
        }];
        let summary = summarize(&schema, &rows);
        let note = [Scalar::Str("other".into())];
        assert!(might_match(
            &summary,
            &[SummaryPredicate {
                field_index: 1,
                allowed: &note,
            }]
        ));
    }

    #[test]
    fn exact_strings_past_the_reader_limit_are_stored_as_a_bloom() {
        let huge = "x".repeat(MAX_SUMMARY_LEN);
        let rows = vec![row(1, Scalar::Int(1), Scalar::Str(huge.clone()))];
        let summary = summarize(&schema(), &rows);
        assert!(summary.len() <= MAX_SUMMARY_LEN);
        assert!(summary.len() < 256);
        assert_eq!(bloom_may_contain(&summary, 2, &huge), Some(true));
    }
}
