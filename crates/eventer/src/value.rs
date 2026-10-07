use serde_json::{Map, Number, Value};

use crate::error::{Error, Result};
use crate::schema::{parse_timestamp_value, Field, FieldType, Schema};

#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    Decimal(i128),
    Timestamp(i64),
    /// UTF-8 JSON text for one value, preserved from ingest.
    Json(String),
}

#[derive(Debug, Clone)]
pub struct Row {
    pub values: Vec<Scalar>,
    pub ts: i64,
}

pub fn parse_event(schema: &Schema, json: &[u8]) -> Result<Row> {
    if json.len() > 1024 * 1024 {
        return Err(Error::event("event JSON exceeds 1 MiB"));
    }
    let value: Value = serde_json::from_slice(json)?;
    let obj = value
        .as_object()
        .ok_or_else(|| Error::event("event must be a JSON object"))?;
    let mut values = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let raw = obj.get(&field.name);
        let scalar = match raw {
            None | Some(Value::Null) => {
                if field.ty == FieldType::Timestamp && field.name == schema.timestamp_field {
                    return Err(Error::event(format!(
                        "missing timestamp field `{}`",
                        field.name
                    )));
                }
                Scalar::Null
            }
            Some(item) => {
                if field.ty == FieldType::Json {
                    let raw = extract_object_field_raw(json, &field.name)?
                        .ok_or_else(|| Error::event(format!("missing json field `{}`", field.name)))?;
                    if raw.as_bytes() == b"null" {
                        return Err(Error::event(format!(
                            "json field `{}` must not be null",
                            field.name
                        )));
                    }
                    validate_json_text(&raw)?;
                    Scalar::Json(raw)
                } else {
                    parse_field(&field.name, field.ty, item)?
                }
            }
        };
        values.push(scalar);
    }
    let ts = match &values[schema.timestamp_index] {
        Scalar::Timestamp(ts) => *ts,
        _ => {
            return Err(Error::event(format!(
                "missing timestamp field `{}`",
                schema.timestamp_field
            )))
        }
    };
    Ok(Row { values, ts })
}

fn parse_field(name: &str, ty: FieldType, value: &Value) -> Result<Scalar> {
    match ty {
        FieldType::Int => {
            let number = value
                .as_i64()
                .ok_or_else(|| Error::event(format!("field `{name}` must be an integer")))?;
            Ok(Scalar::Int(number))
        }
        FieldType::Float => {
            let number = value
                .as_f64()
                .ok_or_else(|| Error::event(format!("field `{name}` must be a number")))?;
            if !number.is_finite() {
                return Err(Error::event(format!("field `{name}` must be finite")));
            }
            Ok(Scalar::Float(number))
        }
        FieldType::Bool => {
            let flag = value
                .as_bool()
                .ok_or_else(|| Error::event(format!("field `{name}` must be a boolean")))?;
            Ok(Scalar::Bool(flag))
        }
        FieldType::String | FieldType::Text => {
            let text = value
                .as_str()
                .ok_or_else(|| Error::event(format!("field `{name}` must be a string")))?;
            Ok(Scalar::Str(text.to_string()))
        }
        FieldType::Decimal { scale } => {
            let text = value
                .as_str()
                .ok_or_else(|| Error::event(format!("field `{name}` decimal must be a string")))?;
            Ok(Scalar::Decimal(parse_decimal(text, scale).map_err(
                |err| Error::event(format!("field `{name}`: {err}")),
            )?))
        }
        FieldType::Timestamp => Ok(Scalar::Timestamp(
            parse_timestamp_value(value)
                .map_err(|err| Error::event(format!("field `{name}`: {err}")))?,
        )),
        FieldType::Json => Err(Error::event("json fields are parsed from raw event bytes")),
    }
}

fn validate_json_text(text: &str) -> Result<()> {
    let value: Value = serde_json::from_str(text)?;
    if value.is_null() {
        return Err(Error::event("json field must not be null"));
    }
    Ok(())
}

/// Returns the raw UTF-8 JSON text of a top-level object field, if present.
fn extract_object_field_raw(document: &[u8], key: &str) -> Result<Option<String>> {
    let mut cursor = skip_json_whitespace(document, 0);
    if document.get(cursor) != Some(&b'{') {
        return Err(Error::event("event must be a JSON object"));
    }
    cursor += 1;
    cursor = skip_json_whitespace(document, cursor);
    if document.get(cursor) == Some(&b'}') {
        return Ok(None);
    }
    while cursor < document.len() {
        let (field_key, next) = parse_json_string_token(document, cursor)?;
        cursor = skip_json_whitespace(document, next);
        if document.get(cursor) != Some(&b':') {
            return Err(Error::event("malformed JSON object"));
        }
        cursor = skip_json_whitespace(document, cursor + 1);
        let value_start = cursor;
        let value_end = end_of_json_value(document, value_start)?;
        if field_key == key {
            let raw = document
                .get(value_start..value_end)
                .ok_or_else(|| Error::event("malformed JSON object"))?;
            return Ok(Some(String::from_utf8(raw.to_vec()).map_err(|_| {
                Error::event("json field is not valid UTF-8")
            })?));
        }
        cursor = skip_json_whitespace(document, value_end);
        if document.get(cursor) == Some(&b',') {
            cursor = skip_json_whitespace(document, cursor + 1);
            continue;
        }
        if document.get(cursor) == Some(&b'}') {
            return Ok(None);
        }
        return Err(Error::event("malformed JSON object"));
    }
    Err(Error::event("malformed JSON object"))
}

fn skip_json_whitespace(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

fn parse_json_string_token(bytes: &[u8], start: usize) -> Result<(String, usize)> {
    if bytes.get(start) != Some(&b'"') {
        return Err(Error::event("JSON object key must be a string"));
    }
    let mut i = start + 1;
    let mut out = String::new();
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'"' {
            return Ok((out, i + 1));
        }
        if byte == b'\\' {
            i += 1;
            let esc = bytes
                .get(i)
                .ok_or_else(|| Error::event("truncated JSON string"))?;
            let ch = match esc {
                b'"' => '"',
                b'\\' => '\\',
                b'/' => '/',
                b'b' => '\u{0008}',
                b'f' => '\u{000c}',
                b'n' => '\n',
                b'r' => '\r',
                b't' => '\t',
                b'u' => {
                    let hex = bytes
                        .get(i + 1..i + 5)
                        .ok_or_else(|| Error::event("truncated JSON unicode escape"))?;
                    let code = u16::from_str_radix(
                        std::str::from_utf8(hex)
                            .map_err(|_| Error::event("invalid JSON unicode escape"))?,
                        16,
                    )
                    .map_err(|_| Error::event("invalid JSON unicode escape"))?;
                    i += 4;
                    char::from_u32(code as u32)
                        .ok_or_else(|| Error::event("invalid JSON unicode escape"))?
                }
                _ => return Err(Error::event("invalid JSON string escape")),
            };
            out.push(ch);
            i += 1;
            continue;
        }
        if byte.is_ascii() {
            out.push(byte as char);
            i += 1;
            continue;
        }
        return Err(Error::event("JSON object key must be UTF-8"));
    }
    Err(Error::event("truncated JSON string"))
}

fn end_of_json_value(bytes: &[u8], start: usize) -> Result<usize> {
    let byte = *bytes
        .get(start)
        .ok_or_else(|| Error::event("truncated JSON value"))?;
    match byte {
        b'"' => Ok(parse_json_string_token(bytes, start)?.1),
        b'{' => end_of_json_container(bytes, start, b'{', b'}'),
        b'[' => end_of_json_container(bytes, start, b'[', b']'),
        b't' => {
            if bytes.get(start..start + 4) == Some(b"true") {
                Ok(start + 4)
            } else {
                Err(Error::event("malformed JSON literal"))
            }
        }
        b'f' => {
            if bytes.get(start..start + 5) == Some(b"false") {
                Ok(start + 5)
            } else {
                Err(Error::event("malformed JSON literal"))
            }
        }
        b'n' => {
            if bytes.get(start..start + 4) == Some(b"null") {
                Ok(start + 4)
            } else {
                Err(Error::event("malformed JSON literal"))
            }
        }
        b'-' | b'0'..=b'9' => end_of_json_number(bytes, start),
        _ => Err(Error::event("malformed JSON value")),
    }
}

fn end_of_json_container(bytes: &[u8], start: usize, open: u8, close: u8) -> Result<usize> {
    let mut depth = 0usize;
    let mut i = start;
    let mut in_string = false;
    while i < bytes.len() {
        let byte = bytes[i];
        if in_string {
            if byte == b'\\' {
                i += 2;
                continue;
            }
            if byte == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b if b == open => depth += 1,
            b if b == close => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(Error::event("truncated JSON value"))
}

fn end_of_json_number(bytes: &[u8], start: usize) -> Result<usize> {
    let mut i = start;
    if bytes.get(i) == Some(&b'-') {
        i += 1;
    }
    i = consume_json_digits(bytes, i, false)?;
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        i = consume_json_digits(bytes, i, true)?;
    }
    if bytes.get(i) == Some(&b'e') || bytes.get(i) == Some(&b'E') {
        i += 1;
        if bytes.get(i) == Some(&b'+') || bytes.get(i) == Some(&b'-') {
            i += 1;
        }
        i = consume_json_digits(bytes, i, true)?;
    }
    Ok(i)
}

fn consume_json_digits(bytes: &[u8], start: usize, allow_empty: bool) -> Result<usize> {
    let mut i = start;
    let mut saw = false;
    while bytes.get(i).is_some_and(|b| b.is_ascii_digit()) {
        saw = true;
        i += 1;
    }
    if !saw && !allow_empty {
        return Err(Error::event("malformed JSON number"));
    }
    Ok(i)
}

pub fn parse_decimal(text: &str, scale: u32) -> Result<i128> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(Error::event("decimal is empty"));
    }
    let (neg, rest) = if let Some(rest) = trimmed.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = trimmed.strip_prefix('+') {
        (false, rest)
    } else {
        (false, trimmed)
    };
    if rest.is_empty() || rest.bytes().any(|b| !(b.is_ascii_digit() || b == b'.')) {
        return Err(Error::event(format!(
            "decimal `{trimmed}` is not a plain decimal"
        )));
    }
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((int_part, frac_part)) => (int_part, frac_part),
        None => (rest, ""),
    };
    if frac_part.contains('.') || int_part.is_empty() && frac_part.is_empty() {
        return Err(Error::event(format!(
            "decimal `{trimmed}` is not a plain decimal"
        )));
    }
    if int_part.bytes().any(|b| !b.is_ascii_digit())
        || frac_part.bytes().any(|b| !b.is_ascii_digit())
    {
        return Err(Error::event(format!(
            "decimal `{trimmed}` is not a plain decimal"
        )));
    }
    if frac_part.len() > scale as usize {
        return Err(Error::event(format!(
            "decimal `{trimmed}` has more than {scale} fractional digits"
        )));
    }
    let mut digits = String::new();
    digits.push_str(if int_part.is_empty() { "0" } else { int_part });
    digits.push_str(frac_part);
    for _ in frac_part.len()..scale as usize {
        digits.push('0');
    }
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let mut magnitude: i128 = 0;
    for byte in digits.bytes() {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|v| v.checked_add((byte - b'0') as i128))
            .ok_or_else(|| Error::event(format!("decimal `{trimmed}` does not fit in i128")))?;
    }
    if neg {
        magnitude = magnitude
            .checked_neg()
            .ok_or_else(|| Error::event(format!("decimal `{trimmed}` does not fit in i128")))?;
    }
    Ok(magnitude)
}

pub fn format_decimal(value: i128, scale: u32) -> String {
    let neg = value < 0;
    let digits = value.unsigned_abs().to_string();
    let body = if scale == 0 {
        digits
    } else {
        let scale = scale as usize;
        let (int_part, frac) = if digits.len() <= scale {
            ("0".to_string(), format!("{digits:0>scale$}"))
        } else {
            let split = digits.len() - scale;
            (digits[..split].to_string(), digits[split..].to_string())
        };
        format!("{int_part}.{frac}")
    };
    if neg {
        format!("-{body}")
    } else {
        body
    }
}

pub fn row_to_json(schema: &Schema, row: &Row) -> Result<Value> {
    let mut obj = Map::new();
    for (field, scalar) in schema.fields.iter().zip(row.values.iter()) {
        let value = match (field.ty, scalar) {
            (_, Scalar::Null) => Value::Null,
            (FieldType::Int, Scalar::Int(v)) => Value::Number((*v).into()),
            (FieldType::Float, Scalar::Float(v)) => {
                Number::from_f64(*v).map(Value::Number).ok_or_else(|| {
                    Error::corrupt(format!("field `{}` is not a finite float", field.name))
                })?
            }
            (FieldType::Bool, Scalar::Bool(v)) => Value::Bool(*v),
            (FieldType::String | FieldType::Text, Scalar::Str(v)) => Value::String(v.clone()),
            (FieldType::Decimal { scale }, Scalar::Decimal(v)) => {
                Value::String(format_decimal(*v, scale))
            }
            (FieldType::Timestamp, Scalar::Timestamp(v)) => Value::Number((*v).into()),
            (FieldType::Json, Scalar::Json(v)) => serde_json::from_str(v).map_err(|err| {
                Error::corrupt(format!("field `{}` is not valid JSON: {err}", field.name))
            })?,
            _ => {
                return Err(Error::corrupt(format!(
                    "field `{}` has a value that does not match its type",
                    field.name
                )))
            }
        };
        obj.insert(field.name.clone(), value);
    }
    Ok(Value::Object(obj))
}

/// Serializes a row to JSON bytes, preserving raw `json` column text from ingest.
pub fn row_to_json_bytes(schema: &Schema, row: &Row) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.push(b'{');
    for (index, (field, scalar)) in schema.fields.iter().zip(row.values.iter()).enumerate() {
        if index > 0 {
            out.push(b',');
        }
        serde_json::to_writer(&mut out, &field.name)
            .map_err(|err| Error::corrupt(format!("failed to encode field name: {err}")))?;
        out.push(b':');
        match (field.ty, scalar) {
            (FieldType::Json, Scalar::Json(raw)) => {
                out.extend_from_slice(raw.as_bytes());
            }
            _ => {
                let value = scalar_to_json_value(field, scalar)?;
                serde_json::to_writer(&mut out, &value)
                    .map_err(|err| Error::corrupt(format!("failed to encode field: {err}")))?;
            }
        }
    }
    out.push(b'}');
    Ok(out)
}

fn scalar_to_json_value(field: &Field, scalar: &Scalar) -> Result<Value> {
    match (field.ty, scalar) {
        (_, Scalar::Null) => Ok(Value::Null),
        (FieldType::Int, Scalar::Int(v)) => Ok(Value::Number((*v).into())),
        (FieldType::Float, Scalar::Float(v)) => {
            Number::from_f64(*v)
                .map(Value::Number)
                .ok_or_else(|| Error::corrupt(format!("field `{}` is not a finite float", field.name)))
        }
        (FieldType::Bool, Scalar::Bool(v)) => Ok(Value::Bool(*v)),
        (FieldType::String | FieldType::Text, Scalar::Str(v)) => Ok(Value::String(v.clone())),
        (FieldType::Decimal { scale }, Scalar::Decimal(v)) => {
            Ok(Value::String(format_decimal(*v, scale)))
        }
        (FieldType::Timestamp, Scalar::Timestamp(v)) => Ok(Value::Number((*v).into())),
        _ => Err(Error::corrupt(format!(
            "field `{}` has a value that does not match its type",
            field.name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parse_schema;

    fn sample_schema() -> Schema {
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
    fn decimal_scale_and_roundtrip() {
        assert_eq!(parse_decimal("12.3", 2).unwrap(), 1230);
        assert_eq!(format_decimal(1230, 2), "12.30");
        assert_eq!(parse_decimal("-0.01", 2).unwrap(), -1);
        assert_eq!(format_decimal(-1, 2), "-0.01");
        assert_eq!(parse_decimal(".5", 2).unwrap(), 50);
        assert!(parse_decimal("1.234", 2).is_err());
        assert!(parse_decimal("1e2", 2).is_err());
    }

    #[test]
    fn parses_each_field_type() {
        let schema = sample_schema();
        let row = parse_event(
            &schema,
            br#"{"ts":"2024-01-02T03:04:05.123Z","user_id":7,"score":1.5,"ok":true,"action":"buy","note":"hi","amount":"19.99"}"#,
        )
        .unwrap();
        assert_eq!(row.ts, 1_704_164_645_123);
        match &row.values[6] {
            Scalar::Decimal(v) => assert_eq!(*v, 1999),
            other => panic!("unexpected {other:?}"),
        }
        let json = row_to_json(&schema, &row).unwrap();
        let again = parse_event(&schema, serde_json::to_vec(&json).unwrap().as_slice()).unwrap();
        assert_eq!(again.ts, row.ts);
        assert!(parse_event(&schema, br#"{"user_id":1}"#).is_err());
    }

    #[test]
    fn json_field_keeps_each_row_shape() {
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
        let nested = parse_event(
            &schema,
            br#"{"ts":10,"props":{"b":1,"a":{"tags":["x",2]},"ok":true}}"#,
        )
        .unwrap();
        let json = row_to_json(&schema, &nested).unwrap();
        assert_eq!(json["props"]["a"]["tags"][1], 2);
        assert_eq!(json["props"]["ok"], true);
        let again = parse_event(&schema, serde_json::to_vec(&json).unwrap().as_slice()).unwrap();
        assert_eq!(row_to_json(&schema, &again).unwrap(), json);

        let list = parse_event(&schema, br#"{"ts":11,"props":[1,"z",false]}"#).unwrap();
        assert_eq!(
            row_to_json(&schema, &list).unwrap()["props"],
            serde_json::json!([1, "z", false])
        );
        let text = parse_event(&schema, br#"{"ts":12,"props":"plain"}"#).unwrap();
        assert_eq!(row_to_json(&schema, &text).unwrap()["props"], "plain");
        let number = parse_event(&schema, br#"{"ts":13,"props":42}"#).unwrap();
        assert_eq!(row_to_json(&schema, &number).unwrap()["props"], 42);
        let missing = parse_event(&schema, br#"{"ts":14}"#).unwrap();
        assert!(row_to_json(&schema, &missing).unwrap()["props"].is_null());
        let explicit_null = parse_event(&schema, br#"{"ts":15,"props":null}"#).unwrap();
        assert!(row_to_json(&schema, &explicit_null).unwrap()["props"].is_null());

        let strings = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"action","type":"string"}]}"#,
        )
        .unwrap();
        assert!(parse_event(&strings, br#"{"ts":1,"action":{"nested":true}}"#).is_err());
    }

    #[test]
    fn json_field_preserves_raw_lexemes() {
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
        let input = br#"{"ts":3,"props":{"a":1,"a":2,"b":{"z":1,"z":3}}}"#;
        let row = parse_event(&schema, input).unwrap();
        match &row.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, r#"{"a":1,"a":2,"b":{"z":1,"z":3}}"#),
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(row_to_json_bytes(&schema, &row).unwrap(), input);
    }
}
