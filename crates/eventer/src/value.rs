use serde_json::{Map, Number, Value};

use crate::error::{Error, Result};
use crate::schema::{parse_timestamp_value, FieldType, Schema};

#[derive(Debug, Clone, PartialEq)]
pub enum Scalar {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Str(String),
    Decimal(i128),
    Timestamp(i64),
}

impl From<&str> for Scalar {
    fn from(value: &str) -> Self {
        Scalar::Str(value.to_string())
    }
}

impl From<String> for Scalar {
    fn from(value: String) -> Self {
        Scalar::Str(value)
    }
}

impl From<i64> for Scalar {
    fn from(value: i64) -> Self {
        Scalar::Int(value)
    }
}

impl From<bool> for Scalar {
    fn from(value: bool) -> Self {
        Scalar::Bool(value)
    }
}

impl From<f64> for Scalar {
    fn from(value: f64) -> Self {
        Scalar::Float(value)
    }
}

/// Parse a filter literal the way a query string would spell the value.
pub fn scalar_from_literal(ty: FieldType, text: &str) -> Result<Scalar> {
    match ty {
        FieldType::String | FieldType::Text => Ok(Scalar::Str(text.to_string())),
        FieldType::Int => text
            .parse::<i64>()
            .map(Scalar::Int)
            .map_err(|_| Error::event(format!("`{text}` is not an integer"))),
        FieldType::Timestamp => text
            .parse::<i64>()
            .map(Scalar::Timestamp)
            .map_err(|_| Error::event(format!("`{text}` is not a unix millisecond timestamp"))),
        FieldType::Float => {
            let number = text
                .parse::<f64>()
                .map_err(|_| Error::event(format!("`{text}` is not a number")))?;
            if !number.is_finite() {
                return Err(Error::event(format!("`{text}` is not a finite number")));
            }
            Ok(Scalar::Float(number))
        }
        FieldType::Bool => match text {
            "true" => Ok(Scalar::Bool(true)),
            "false" => Ok(Scalar::Bool(false)),
            _ => Err(Error::event(format!("`{text}` is not true or false"))),
        },
        FieldType::Decimal { scale } => Ok(Scalar::Decimal(parse_decimal(text, scale)?)),
    }
}

pub(crate) fn scalar_matches_field(value: &Scalar, ty: FieldType) -> bool {
    matches!(
        (value, ty),
        (Scalar::Null, _)
            | (Scalar::Int(_), FieldType::Int)
            | (Scalar::Float(_), FieldType::Float)
            | (Scalar::Bool(_), FieldType::Bool)
            | (Scalar::Str(_), FieldType::String | FieldType::Text)
            | (Scalar::Decimal(_), FieldType::Decimal { .. })
            | (Scalar::Timestamp(_), FieldType::Timestamp)
    )
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
            Some(item) => parse_field(&field.name, field.ty, item)?,
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
    }
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
}
