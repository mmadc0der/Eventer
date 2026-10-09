use std::fs;
use std::path::Path;

use serde_json::Value;

use crate::error::{Error, Result};

/// Column types stored in each block. Decimal scale lives on the schema, not per value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    Int,
    Float,
    Bool,
    /// Short, often repeated text. Dictionary-encoded when that is smaller.
    String,
    /// Long text. Dictionary-encoded when that body is smaller than raw bytes.
    Text,
    Decimal {
        scale: u32,
    },
    Timestamp,
    /// Any JSON value except null: object, array, string, number, or bool.
    /// Each row may use a different shape. JSON null and a missing field are column nulls.
    Json,
}

impl FieldType {
    pub fn name(self) -> &'static str {
        match self {
            FieldType::Int => "int",
            FieldType::Float => "float",
            FieldType::Bool => "bool",
            FieldType::String => "string",
            FieldType::Text => "text",
            FieldType::Decimal { .. } => "decimal",
            FieldType::Timestamp => "timestamp",
            FieldType::Json => "json",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
}

/// User schema. Field order is the on-disk column order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    pub timestamp_field: String,
    pub timestamp_index: usize,
    pub fields: Vec<Field>,
}

impl Schema {
    /// Stable text written to `schema.lock`.
    ///
    /// An open compares this with the locked schema. An identical field list matches.
    /// Fields appended after that list are accepted and the lock is rewritten.
    pub fn canonical(&self) -> String {
        let fields: Vec<Value> = self
            .fields
            .iter()
            .map(|field| {
                let mut obj = serde_json::Map::new();
                obj.insert("name".into(), Value::String(field.name.clone()));
                obj.insert("type".into(), Value::String(field.ty.name().to_string()));
                if let FieldType::Decimal { scale } = field.ty {
                    obj.insert("scale".into(), Value::from(scale));
                }
                Value::Object(obj)
            })
            .collect();
        let value = serde_json::json!({
            "timestamp_field": self.timestamp_field,
            "fields": fields,
        });
        serde_json::to_string_pretty(&value).expect("schema json")
    }
}

pub fn load_schema(path: &Path) -> Result<Schema> {
    let text = fs::read_to_string(path)
        .map_err(|err| Error::schema(format!("failed to read schema {}: {err}", path.display())))?;
    parse_schema(&text)
}

pub fn parse_schema(text: &str) -> Result<Schema> {
    let value: Value = serde_json::from_str(text)?;
    let obj = value
        .as_object()
        .ok_or_else(|| Error::schema("schema must be a JSON object"))?;
    let timestamp_field = obj
        .get("timestamp_field")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::schema("timestamp_field must be a string"))?
        .to_string();
    let fields_v = obj
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::schema("fields must be an array"))?;
    if fields_v.is_empty() {
        return Err(Error::schema("schema needs at least one field"));
    }
    let mut fields = Vec::with_capacity(fields_v.len());
    for field_v in fields_v {
        let field_obj = field_v
            .as_object()
            .ok_or_else(|| Error::schema("each field must be an object"))?;
        let name = field_obj
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::schema("field name must be a string"))?
            .to_string();
        if name.is_empty() {
            return Err(Error::schema("field name must not be empty"));
        }
        if fields.iter().any(|field: &Field| field.name == name) {
            return Err(Error::schema(format!("duplicate field `{name}`")));
        }
        let ty_name = field_obj
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::schema(format!("field `{name}` needs a type")))?;
        let ty = match ty_name {
            "int" => FieldType::Int,
            "float" => FieldType::Float,
            "bool" => FieldType::Bool,
            "string" => FieldType::String,
            "text" => FieldType::Text,
            "timestamp" => FieldType::Timestamp,
            "json" => FieldType::Json,
            "decimal" => {
                let scale = field_obj
                    .get("scale")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        Error::schema(format!("decimal `{name}` needs an integer scale"))
                    })?;
                if scale > 38 {
                    return Err(Error::schema(format!(
                        "decimal `{name}` scale {scale} exceeds 38"
                    )));
                }
                FieldType::Decimal {
                    scale: scale as u32,
                }
            }
            other => {
                return Err(Error::schema(format!(
                    "field `{name}` has unknown type `{other}`"
                )))
            }
        };
        if ty_name != "decimal" && field_obj.contains_key("scale") {
            return Err(Error::schema(format!("field `{name}` does not use scale")));
        }
        fields.push(Field { name, ty });
    }
    let timestamp_index = fields
        .iter()
        .position(|field| field.name == timestamp_field)
        .ok_or_else(|| {
            Error::schema(format!(
                "timestamp_field `{timestamp_field}` is not a field"
            ))
        })?;
    if fields[timestamp_index].ty != FieldType::Timestamp {
        return Err(Error::schema(format!(
            "timestamp_field `{timestamp_field}` must have type timestamp"
        )));
    }
    Ok(Schema {
        timestamp_field,
        timestamp_index,
        fields,
    })
}

/// How the schema used to open a directory relates to the one stored in `schema.lock`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaEvolution {
    /// Timestamp field, names, types, and decimal scales all match.
    Unchanged,
    /// `added` fields were appended after the locked field list.
    Appended { added: usize },
}

/// Accept an unchanged schema or one that only appends fields.
///
/// A different timestamp field, a type or decimal-scale change, a removed field,
/// a renamed field, or a reordered field is rejected. Comparison uses parsed
/// fields, so key order inside a field object does not matter.
pub fn schema_evolution(locked: &Schema, opened: &Schema) -> Result<SchemaEvolution> {
    if locked.timestamp_field != opened.timestamp_field
        || locked.timestamp_index != opened.timestamp_index
    {
        return Err(schema_lock_mismatch("timestamp field differs"));
    }
    if opened.fields.len() < locked.fields.len() {
        return Err(schema_lock_mismatch("a field was removed"));
    }
    for (locked_field, opened_field) in locked.fields.iter().zip(opened.fields.iter()) {
        if locked_field.name != opened_field.name {
            return Err(schema_lock_mismatch("a field was renamed or reordered"));
        }
        if locked_field.ty != opened_field.ty {
            return Err(schema_lock_mismatch(
                "a field type or decimal scale changed",
            ));
        }
    }
    let added = opened.fields.len() - locked.fields.len();
    if added == 0 {
        Ok(SchemaEvolution::Unchanged)
    } else {
        Ok(SchemaEvolution::Appended { added })
    }
}

fn schema_lock_mismatch(reason: &str) -> Error {
    Error::schema(format!(
        "schema.lock does not match the supplied schema; this directory was created with a different schema ({reason})"
    ))
}

/// Unix milliseconds. Accepts an integer, a digit string, or a UTC RFC3339 timestamp.
pub fn parse_timestamp_value(value: &Value) -> Result<i64> {
    match value {
        Value::Number(number) => number.as_i64().ok_or_else(|| {
            Error::event(format!(
                "timestamp {number} is not an i64 millisecond value"
            ))
        }),
        Value::String(text) => parse_timestamp_str(text),
        other => Err(Error::event(format!(
            "timestamp must be a number or string, got {other}"
        ))),
    }
}

pub fn parse_timestamp_str(text: &str) -> Result<i64> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(Error::event("timestamp string is empty"));
    }
    let digit_body = trimmed.strip_prefix('-').unwrap_or(trimmed);
    if !digit_body.is_empty() && digit_body.bytes().all(|b| b.is_ascii_digit()) {
        return trimmed
            .parse::<i64>()
            .map_err(|_| Error::event(format!("timestamp `{trimmed}` does not fit in i64")));
    }
    parse_rfc3339(trimmed)
}

fn parse_rfc3339(text: &str) -> Result<i64> {
    let bytes = text.as_bytes();
    if bytes.len() < 20 {
        return Err(Error::event(format!("timestamp `{text}` is not RFC3339")));
    }
    let year = take_i32(text, 0, 4)?;
    expect_char(text, 4, b'-')?;
    let month = take_u32(text, 5, 2)?;
    expect_char(text, 7, b'-')?;
    let day = take_u32(text, 8, 2)?;
    expect_char(text, 10, b'T')?;
    let hour = take_u32(text, 11, 2)?;
    expect_char(text, 13, b':')?;
    let minute = take_u32(text, 14, 2)?;
    expect_char(text, 16, b':')?;
    let second = take_u32(text, 17, 2)?;
    if !(1..=12).contains(&month) || hour > 23 || minute > 59 || second > 59 {
        return Err(Error::event(format!(
            "timestamp `{text}` has an invalid clock field"
        )));
    }
    let max_day = days_in_month(year, month)?;
    if day == 0 || day > max_day {
        return Err(Error::event(format!(
            "timestamp `{text}` has an invalid date"
        )));
    }
    let mut idx = 19;
    let mut millis: i64 = 0;
    if idx < bytes.len() && bytes[idx] == b'.' {
        idx += 1;
        let start = idx;
        while idx < bytes.len() && bytes[idx].is_ascii_digit() {
            idx += 1;
        }
        if idx == start {
            return Err(Error::event(format!(
                "timestamp `{text}` has an empty fraction"
            )));
        }
        let mut frac = text[start..idx].to_string();
        if frac.len() > 3 {
            frac.truncate(3);
        }
        while frac.len() < 3 {
            frac.push('0');
        }
        millis = frac.parse::<i64>().unwrap_or(0);
    }
    if idx >= bytes.len() {
        return Err(Error::event(format!(
            "timestamp `{text}` is missing a timezone"
        )));
    }
    let offset_secs: i64 = if bytes[idx] == b'Z' {
        if idx + 1 != bytes.len() {
            return Err(Error::event(format!(
                "timestamp `{text}` has trailing junk"
            )));
        }
        0
    } else if bytes[idx] == b'+' || bytes[idx] == b'-' {
        let sign: i64 = if bytes[idx] == b'+' { 1 } else { -1 };
        if idx + 6 != bytes.len() || bytes.get(idx + 3) != Some(&b':') {
            return Err(Error::event(format!(
                "timestamp `{text}` offset must look like +HH:MM"
            )));
        }
        let off_h = take_u32(text, idx + 1, 2)?;
        let off_m = take_u32(text, idx + 4, 2)?;
        if off_h > 23 || off_m > 59 {
            return Err(Error::event(format!(
                "timestamp `{text}` has an invalid offset"
            )));
        }
        sign * (off_h as i64 * 3600 + off_m as i64 * 60)
    } else {
        return Err(Error::event(format!(
            "timestamp `{text}` is missing a timezone"
        )));
    };

    let days = days_from_civil(year, month, day);
    let secs = days
        .checked_mul(86_400)
        .and_then(|v| v.checked_add(hour as i64 * 3600))
        .and_then(|v| v.checked_add(minute as i64 * 60))
        .and_then(|v| v.checked_add(second as i64))
        .and_then(|v| v.checked_sub(offset_secs))
        .ok_or_else(|| Error::event(format!("timestamp `{text}` overflowed")))?;
    secs.checked_mul(1000)
        .and_then(|v| v.checked_add(millis))
        .ok_or_else(|| Error::event(format!("timestamp `{text}` overflowed")))
}

fn expect_char(text: &str, index: usize, expected: u8) -> Result<()> {
    match text.as_bytes().get(index) {
        Some(byte) if *byte == expected => Ok(()),
        _ => Err(Error::event(format!("timestamp `{text}` is not RFC3339"))),
    }
}

fn take_u32(text: &str, index: usize, width: usize) -> Result<u32> {
    let end = index + width;
    let slice = text
        .get(index..end)
        .ok_or_else(|| Error::event(format!("timestamp `{text}` is truncated")))?;
    if !slice.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::event(format!("timestamp `{text}` is not RFC3339")));
    }
    slice
        .parse::<u32>()
        .map_err(|_| Error::event(format!("timestamp `{text}` is not RFC3339")))
}

fn take_i32(text: &str, index: usize, width: usize) -> Result<i32> {
    Ok(take_u32(text, index, width)? as i32)
}

fn days_in_month(year: i32, month: u32) -> Result<u32> {
    let days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    if !(1..=12).contains(&month) {
        return Err(Error::event("invalid month"));
    }
    let mut n = days[month as usize];
    if month == 2 && is_leap(year) {
        n = 29;
    }
    Ok(n)
}

fn is_leap(year: i32) -> bool {
    let y = year;
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// Howard Hinnant's `days_from_civil`. Days since 1970-01-01.
fn days_from_civil(mut year: i32, month: u32, day: u32) -> i64 {
    year -= i32::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let yoe = (year - era * 400) as u64;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy as u64;
    era as i64 * 146097 + doe as i64 - 719468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_schema_and_rejects_a_bad_timestamp_field() {
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
        assert_eq!(schema.timestamp_index, 0);
        assert_eq!(schema.fields[1].ty, FieldType::Decimal { scale: 2 });

        let err = parse_schema(r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"int"}]}"#)
            .unwrap_err();
        assert!(err.to_string().contains("timestamp"));
    }

    #[test]
    fn parses_a_json_field_and_rejects_scale() {
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
        assert_eq!(schema.fields[1].ty, FieldType::Json);
        assert!(schema.canonical().contains("\"type\": \"json\""));

        let err = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"props","type":"json","scale":1}]}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("scale"));
    }

    #[test]
    fn civil_dates_match_unix_milliseconds() {
        assert_eq!(parse_timestamp_str("1970-01-01T00:00:00Z").unwrap(), 0);
        assert_eq!(
            parse_timestamp_str("1970-01-02T00:00:00Z").unwrap(),
            86_400_000
        );
        assert_eq!(
            parse_timestamp_str("2000-01-01T00:00:00Z").unwrap(),
            946_684_800_000
        );
        assert_eq!(
            parse_timestamp_str("2024-01-02T03:04:05.123Z").unwrap(),
            parse_timestamp_str("2024-01-02T08:34:05.123+05:30").unwrap()
        );
        assert!(parse_timestamp_str("2023-02-29T00:00:00Z").is_err());
        assert!(parse_timestamp_str("2024-02-29T00:00:00Z").is_ok());
    }

    fn two_field_schema() -> Schema {
        parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap()
    }

    #[test]
    fn appended_fields_are_compatible_and_other_edits_are_not() {
        let locked = two_field_schema();
        let appended = parse_schema(
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
        assert_eq!(
            schema_evolution(&locked, &appended).unwrap(),
            SchemaEvolution::Appended { added: 2 }
        );
        assert_eq!(
            schema_evolution(&locked, &locked).unwrap(),
            SchemaEvolution::Unchanged
        );

        let type_change = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"user_id","type":"float"}]}"#,
        )
        .unwrap();
        let removed =
            parse_schema(r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"}]}"#)
                .unwrap();
        let reordered = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"user_id","type":"int"},{"name":"ts","type":"timestamp"}]}"#,
        )
        .unwrap();
        let renamed = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"uid","type":"int"}]}"#,
        )
        .unwrap();
        let scale = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"user_id","type":"decimal","scale":2}]}"#,
        )
        .unwrap();
        let other_clock = parse_schema(
            r#"{
                "timestamp_field": "event_time",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "event_time", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();
        let locked_two_clocks = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "event_time", "type": "timestamp"},
                    {"name": "user_id", "type": "int"}
                ]
            }"#,
        )
        .unwrap();

        for (opened, reason) in [
            (&type_change, "type"),
            (&removed, "removed"),
            (&reordered, "reordered"),
            (&renamed, "renamed"),
            (&scale, "scale"),
        ] {
            let err = schema_evolution(&locked, opened).unwrap_err();
            assert!(err.to_string().contains("schema.lock"), "{reason}: {err}");
        }
        let err = schema_evolution(&locked_two_clocks, &other_clock).unwrap_err();
        assert!(err.to_string().contains("timestamp"), "{err}");
    }
}
