use std::borrow::Cow;

use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::json_scan::{
    decode_json_string, end_of_json_string, skip_json_whitespace, validate_json_structure,
};
use crate::schema::{parse_timestamp_str, Field, FieldType, Schema};

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
        FieldType::Json => Err(Error::event(
            "json columns are not supported by equality filters",
        )),
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

/// Scan one JSON object straight into a [`Row`].
///
/// Unknown top-level keys are ignored. A repeated key keeps the last value.
/// A missing key and JSON null are column nulls. The schema timestamp is required.
pub fn parse_event(schema: &Schema, json: &[u8]) -> Result<Row> {
    if json.len() > 1024 * 1024 {
        return Err(Error::event("event JSON exceeds 1 MiB"));
    }
    let mut parser = EventParser {
        bytes: json,
        i: 0,
        depth: 0,
    };
    parser.skip_ws();
    if parser.peek() != Some(b'{') {
        if parser.i >= parser.bytes.len() {
            return Err(Error::json("EOF while parsing a JSON value"));
        }
        parser.skip_value().map_err(ScanError::into_error)?;
        parser.skip_ws();
        if parser.i != parser.bytes.len() {
            return Err(Error::json("trailing characters after a JSON value"));
        }
        return Err(Error::event("event must be a JSON object"));
    }

    let n = schema.fields.len();
    let mut values = vec![Scalar::Null; n];
    let mut errors: Vec<Option<Error>> = Vec::new();
    parser
        .parse_object(schema, &mut values, &mut errors)
        .map_err(ScanError::into_error)?;
    parser.skip_ws();
    if parser.i != parser.bytes.len() {
        return Err(Error::json("trailing characters after a JSON value"));
    }

    for (index, field) in schema.fields.iter().enumerate() {
        if let Some(err) = errors.get_mut(index).and_then(Option::take) {
            return Err(err);
        }
        if field.ty == FieldType::Timestamp && field.name == schema.timestamp_field {
            if !matches!(values[index], Scalar::Timestamp(_)) {
                return Err(Error::event(format!(
                    "missing timestamp field `{}`",
                    field.name
                )));
            }
        }
    }
    let ts = match values.get(schema.timestamp_index) {
        Some(Scalar::Timestamp(ts)) => *ts,
        _ => {
            return Err(Error::event(format!(
                "missing timestamp field `{}`",
                schema.timestamp_field
            )))
        }
    };
    Ok(Row { values, ts })
}

/// serde_json starts a depth counter at 128 and rejects a container that would
/// drive it to 0, so 127 nested containers are the deepest document it accepts.
const MAX_JSON_DEPTH: u32 = 127;

enum ScanError {
    /// The document is not JSON. A later key cannot repair it.
    Syntax(Error),
    /// The document is JSON, but the last value does not match the field.
    Type(Error),
}

impl ScanError {
    fn into_error(self) -> Error {
        match self {
            ScanError::Syntax(err) | ScanError::Type(err) => err,
        }
    }

    fn syntax(message: impl Into<String>) -> Self {
        ScanError::Syntax(Error::json(message.into()))
    }
}

struct EventParser<'a> {
    bytes: &'a [u8],
    i: usize,
    depth: u32,
}

impl<'a> EventParser<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.i).copied()
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.peek() == Some(byte) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.i += 1;
        }
    }

    fn enter(&mut self) -> std::result::Result<(), ScanError> {
        if self.depth >= MAX_JSON_DEPTH {
            return Err(ScanError::syntax("JSON recursion limit exceeded"));
        }
        self.depth += 1;
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    fn parse_object(
        &mut self,
        schema: &Schema,
        values: &mut [Scalar],
        errors: &mut Vec<Option<Error>>,
    ) -> std::result::Result<(), ScanError> {
        if !self.eat(b'{') {
            return Err(ScanError::syntax("event must be a JSON object"));
        }
        self.enter()?;
        self.skip_ws();
        if self.eat(b'}') {
            self.leave();
            return Ok(());
        }
        loop {
            let index = self.match_field(schema)?;
            self.skip_ws();
            if !self.eat(b':') {
                return Err(ScanError::syntax("malformed JSON object"));
            }
            self.skip_ws();
            if let Some(index) = index {
                match self.parse_typed(&schema.fields[index]) {
                    Ok(scalar) => {
                        values[index] = scalar;
                        if let Some(slot) = errors.get_mut(index) {
                            *slot = None;
                        }
                    }
                    Err(ScanError::Type(err)) => {
                        if errors.is_empty() {
                            errors.resize_with(values.len(), || None);
                        }
                        errors[index] = Some(err);
                    }
                    Err(err) => return Err(err),
                }
            } else {
                self.skip_value()?;
            }
            self.skip_ws();
            if self.eat(b',') {
                self.skip_ws();
                continue;
            }
            if self.eat(b'}') {
                self.leave();
                return Ok(());
            }
            return Err(ScanError::syntax("malformed JSON object"));
        }
    }

    fn match_field(&mut self, schema: &Schema) -> std::result::Result<Option<usize>, ScanError> {
        if self.peek() != Some(b'"') {
            return Err(ScanError::syntax("JSON object key must be a string"));
        }
        let (inner, escaped) = self.scan_string()?;
        if escaped {
            let owned = decode_json_string(inner).map_err(ScanError::Syntax)?;
            Ok(schema.fields.iter().position(|field| field.name == owned))
        } else if std::str::from_utf8(inner).is_err() {
            Err(ScanError::syntax("JSON object key is not valid UTF-8"))
        } else {
            Ok(schema
                .fields
                .iter()
                .position(|field| field.name.as_bytes() == inner))
        }
    }

    fn skip_key(&mut self) -> std::result::Result<(), ScanError> {
        if self.peek() != Some(b'"') {
            return Err(ScanError::syntax("JSON object key must be a string"));
        }
        self.skip_string()
    }

    fn parse_typed(&mut self, field: &Field) -> std::result::Result<Scalar, ScanError> {
        if self.consume_null()? {
            return Ok(Scalar::Null);
        }
        match field.ty {
            FieldType::Int => self.parse_int(&field.name),
            FieldType::Float => self.parse_float(&field.name),
            FieldType::Bool => self.parse_bool(&field.name),
            FieldType::String | FieldType::Text => self.parse_text(&field.name),
            FieldType::Decimal { scale } => self.parse_decimal_field(&field.name, scale),
            FieldType::Timestamp => self.parse_timestamp_field(&field.name),
            FieldType::Json => self.parse_json_field(),
        }
    }

    fn consume_null(&mut self) -> std::result::Result<bool, ScanError> {
        if self.peek() != Some(b'n') {
            return Ok(false);
        }
        self.consume_literal(b"null")?;
        Ok(true)
    }

    fn parse_int(&mut self, name: &str) -> std::result::Result<Scalar, ScanError> {
        if !self.starts_number() {
            self.skip_value()?;
            return Err(self.type_error(format!("field `{name}` must be an integer")));
        }
        let (start, end, integer) = self.scan_number()?;
        if !integer {
            return Err(self.type_error(format!("field `{name}` must be an integer")));
        }
        match i64_from_lexeme(&self.bytes[start..end]) {
            Some(value) => Ok(Scalar::Int(value)),
            None => Err(self.type_error(format!("field `{name}` must be an integer"))),
        }
    }

    fn parse_float(&mut self, name: &str) -> std::result::Result<Scalar, ScanError> {
        if !self.starts_number() {
            self.skip_value()?;
            return Err(self.type_error(format!("field `{name}` must be a number")));
        }
        let (start, end, integer) = self.scan_number()?;
        let raw = &self.bytes[start..end];
        // An integer that fits in i64 follows `Number::as_f64` under
        // `arbitrary_precision`: the lexeme is parsed as i64 first, so `-0`
        // becomes `+0.0` rather than a signed zero from `f64` parsing.
        if integer {
            if let Some(value) = i64_from_lexeme(raw) {
                return Ok(Scalar::Float(value as f64));
            }
        }
        let lexeme = std::str::from_utf8(raw)
            .map_err(|_| ScanError::syntax("JSON number is not valid UTF-8"))?;
        match lexeme.parse::<f64>() {
            Ok(value) if value.is_finite() => Ok(Scalar::Float(value)),
            Ok(_) => Err(self.type_error(format!("field `{name}` must be finite"))),
            Err(_) => Err(self.type_error(format!("field `{name}` must be a number"))),
        }
    }

    fn parse_bool(&mut self, name: &str) -> std::result::Result<Scalar, ScanError> {
        match self.peek() {
            Some(b't') => {
                self.consume_literal(b"true")?;
                Ok(Scalar::Bool(true))
            }
            Some(b'f') => {
                self.consume_literal(b"false")?;
                Ok(Scalar::Bool(false))
            }
            Some(_) => {
                self.skip_value()?;
                Err(self.type_error(format!("field `{name}` must be a boolean")))
            }
            None => Err(ScanError::syntax("truncated JSON value")),
        }
    }

    fn parse_text(&mut self, name: &str) -> std::result::Result<Scalar, ScanError> {
        if self.peek() != Some(b'"') {
            self.skip_value()?;
            return Err(self.type_error(format!("field `{name}` must be a string")));
        }
        Ok(Scalar::Str(self.take_string()?.into_owned()))
    }

    fn parse_decimal_field(
        &mut self,
        name: &str,
        scale: u32,
    ) -> std::result::Result<Scalar, ScanError> {
        if self.peek() != Some(b'"') {
            self.skip_value()?;
            return Err(self.type_error(format!("field `{name}` decimal must be a string")));
        }
        let text = self.take_string()?;
        match parse_decimal(&text, scale) {
            Ok(value) => Ok(Scalar::Decimal(value)),
            Err(err) => Err(self.type_error(format!("field `{name}`: {err}"))),
        }
    }

    fn parse_timestamp_field(&mut self, name: &str) -> std::result::Result<Scalar, ScanError> {
        if self.starts_number() {
            let (start, end, integer) = self.scan_number()?;
            let lexeme = std::str::from_utf8(&self.bytes[start..end])
                .map_err(|_| ScanError::syntax("JSON number is not valid UTF-8"))?;
            if integer {
                if let Some(value) = i64_from_lexeme(&self.bytes[start..end]) {
                    return Ok(Scalar::Timestamp(value));
                }
            }
            return Err(self.type_error(format!(
                "field `{name}`: event error: timestamp {lexeme} is not an i64 millisecond value"
            )));
        }
        if self.peek() == Some(b'"') {
            let text = self.take_string()?;
            return match parse_timestamp_str(&text) {
                Ok(value) => Ok(Scalar::Timestamp(value)),
                Err(err) => Err(self.type_error(format!("field `{name}`: {err}"))),
            };
        }
        let start = self.i;
        self.skip_value()?;
        let raw = self.lexeme(start);
        Err(self.type_error(format!(
            "field `{name}`: event error: timestamp must be a number or string, got {raw}"
        )))
    }

    fn parse_json_field(&mut self) -> std::result::Result<Scalar, ScanError> {
        let start = self.i;
        self.skip_value()?;
        let raw = &self.bytes[start..self.i];
        let text = std::str::from_utf8(raw)
            .map_err(|_| ScanError::syntax("json field is not valid UTF-8"))?;
        Ok(Scalar::Json(text.to_owned()))
    }

    fn take_string(&mut self) -> std::result::Result<Cow<'a, str>, ScanError> {
        let (inner, escaped) = self.scan_string()?;
        if escaped {
            let owned = decode_json_string(inner).map_err(ScanError::Syntax)?;
            Ok(Cow::Owned(owned))
        } else {
            let text = std::str::from_utf8(inner)
                .map_err(|_| ScanError::syntax("JSON string is not valid UTF-8"))?;
            Ok(Cow::Borrowed(text))
        }
    }

    fn type_error(&self, message: String) -> ScanError {
        ScanError::Type(Error::event(message))
    }

    fn starts_number(&self) -> bool {
        matches!(self.peek(), Some(b'-' | b'0'..=b'9'))
    }

    fn lexeme(&self, start: usize) -> Cow<'a, str> {
        let raw = self.bytes.get(start..self.i).unwrap_or(&[]);
        match std::str::from_utf8(raw) {
            Ok(text) => Cow::Borrowed(text),
            Err(_) => Cow::Owned(String::from_utf8_lossy(raw).into_owned()),
        }
    }

    fn skip_value(&mut self) -> std::result::Result<(), ScanError> {
        match self.peek() {
            Some(b'"') => self.skip_string(),
            Some(b'{') => self.skip_object(),
            Some(b'[') => self.skip_array(),
            Some(b't') => self.consume_literal(b"true"),
            Some(b'f') => self.consume_literal(b"false"),
            Some(b'n') => self.consume_literal(b"null"),
            Some(b'-' | b'0'..=b'9') => {
                self.scan_number()?;
                Ok(())
            }
            Some(_) => Err(ScanError::syntax("malformed JSON value")),
            None => Err(ScanError::syntax("truncated JSON value")),
        }
    }

    fn skip_object(&mut self) -> std::result::Result<(), ScanError> {
        if !self.eat(b'{') {
            return Err(ScanError::syntax("malformed JSON object"));
        }
        self.enter()?;
        self.skip_ws();
        if self.eat(b'}') {
            self.leave();
            return Ok(());
        }
        loop {
            self.skip_key()?;
            self.skip_ws();
            if !self.eat(b':') {
                return Err(ScanError::syntax("malformed JSON object"));
            }
            self.skip_ws();
            self.skip_value()?;
            self.skip_ws();
            if self.eat(b',') {
                self.skip_ws();
                continue;
            }
            if self.eat(b'}') {
                self.leave();
                return Ok(());
            }
            return Err(ScanError::syntax("malformed JSON object"));
        }
    }

    fn skip_array(&mut self) -> std::result::Result<(), ScanError> {
        if !self.eat(b'[') {
            return Err(ScanError::syntax("malformed JSON array"));
        }
        self.enter()?;
        self.skip_ws();
        if self.eat(b']') {
            self.leave();
            return Ok(());
        }
        loop {
            self.skip_value()?;
            self.skip_ws();
            if self.eat(b',') {
                self.skip_ws();
                continue;
            }
            if self.eat(b']') {
                self.leave();
                return Ok(());
            }
            return Err(ScanError::syntax("malformed JSON array"));
        }
    }

    fn skip_string(&mut self) -> std::result::Result<(), ScanError> {
        let (inner, escaped) = self.scan_string()?;
        if escaped {
            decode_json_string(inner).map_err(ScanError::Syntax)?;
        } else if std::str::from_utf8(inner).is_err() {
            return Err(ScanError::syntax("JSON string is not valid UTF-8"));
        }
        Ok(())
    }

    /// `i` sits on the opening quote. Returns the raw interior and whether it has an escape.
    fn scan_string(&mut self) -> std::result::Result<(&'a [u8], bool), ScanError> {
        if !self.eat(b'"') {
            return Err(ScanError::syntax("expected JSON string"));
        }
        let start = self.i;
        let mut escaped = false;
        while let Some(byte) = self.peek() {
            if byte == b'"' {
                let inner = &self.bytes[start..self.i];
                self.i += 1;
                return Ok((inner, escaped));
            }
            if byte < 0x20 {
                return Err(ScanError::syntax("control character in JSON string"));
            }
            if byte == b'\\' {
                escaped = true;
                self.i += 1;
                let esc = self
                    .peek()
                    .ok_or_else(|| ScanError::syntax("truncated JSON string"))?;
                self.i += 1;
                match esc {
                    b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => {}
                    b'u' => {
                        for _ in 0..4 {
                            let hex = self.peek().ok_or_else(|| {
                                ScanError::syntax("truncated JSON unicode escape")
                            })?;
                            if !hex.is_ascii_hexdigit() {
                                return Err(ScanError::syntax("invalid JSON unicode escape"));
                            }
                            self.i += 1;
                        }
                    }
                    _ => return Err(ScanError::syntax("invalid JSON string escape")),
                }
                continue;
            }
            self.i += 1;
        }
        Err(ScanError::syntax("truncated JSON string"))
    }

    fn consume_literal(&mut self, literal: &[u8]) -> std::result::Result<(), ScanError> {
        let end = self.i + literal.len();
        if self.bytes.get(self.i..end) != Some(literal) {
            return Err(ScanError::syntax("malformed JSON literal"));
        }
        if self
            .bytes
            .get(end)
            .is_some_and(|byte| !is_value_boundary(*byte))
        {
            return Err(ScanError::syntax("malformed JSON literal"));
        }
        self.i = end;
        Ok(())
    }

    /// Returns `(start, end, integer lexeme)`.
    fn scan_number(&mut self) -> std::result::Result<(usize, usize, bool), ScanError> {
        let start = self.i;
        if self.peek() == Some(b'-') {
            self.i += 1;
        }
        let first = self
            .peek()
            .ok_or_else(|| ScanError::syntax("malformed JSON number"))?;
        if !first.is_ascii_digit() {
            return Err(ScanError::syntax("malformed JSON number"));
        }
        self.i += 1;
        if first == b'0' {
            if self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                return Err(ScanError::syntax("malformed JSON number"));
            }
        } else {
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.i += 1;
            }
        }
        let mut integer = true;
        if self.peek() == Some(b'.') {
            integer = false;
            self.i += 1;
            let mark = self.i;
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.i += 1;
            }
            if self.i == mark {
                return Err(ScanError::syntax("malformed JSON number"));
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            integer = false;
            self.i += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.i += 1;
            }
            let mark = self.i;
            while self.peek().is_some_and(|byte| byte.is_ascii_digit()) {
                self.i += 1;
            }
            if self.i == mark {
                return Err(ScanError::syntax("malformed JSON number"));
            }
        }
        Ok((start, self.i, integer))
    }
}

fn is_value_boundary(byte: u8) -> bool {
    matches!(
        byte,
        b' ' | b'\n' | b'\r' | b'\t' | b',' | b'}' | b']' | b':'
    )
}

/// Integer lexeme, including a leading `-`. `i64::MIN` is accumulated as a negative.
fn i64_from_lexeme(lexeme: &[u8]) -> Option<i64> {
    let (negative, digits) = if lexeme.first() == Some(&b'-') {
        (true, &lexeme[1..])
    } else {
        (false, lexeme)
    };
    if digits.is_empty() || !digits.iter().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut value: i64 = 0;
    for &byte in digits {
        value = value.checked_mul(10)?.checked_sub(i64::from(byte - b'0'))?;
    }
    if negative {
        Some(value)
    } else {
        value.checked_neg()
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
    let mut out = Vec::new();
    write_decimal_lexeme(&mut out, value, scale);
    String::from_utf8(out).expect("decimal lexeme is ASCII")
}

/// Decimal digits `format_decimal` would return. No sign other than a leading `-`, and no exponent.
fn write_decimal_lexeme(out: &mut Vec<u8>, value: i128, scale: u32) {
    if value < 0 {
        out.push(b'-');
    }
    let mut buf = itoa::Buffer::new();
    let digits = buf.format(value.unsigned_abs()).as_bytes();
    let scale = scale as usize;
    if scale == 0 {
        out.extend_from_slice(digits);
        return;
    }
    if digits.len() <= scale {
        out.push(b'0');
        out.push(b'.');
        out.resize(out.len() + (scale - digits.len()), b'0');
        out.extend_from_slice(digits);
    } else {
        let split = digits.len() - scale;
        out.extend_from_slice(&digits[..split]);
        out.push(b'.');
        out.extend_from_slice(&digits[split..]);
    }
}

/// Serializes a row to JSON while preserving raw `json` column lexemes (including duplicate keys).
pub struct RowSerializable<'a> {
    pub schema: &'a Schema,
    pub row: &'a Row,
}

impl Serialize for RowSerializable<'_> {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = row_to_json_bytes(self.schema, self.row).map_err(serde::ser::Error::custom)?;
        let raw = serde_json::value::RawValue::from_string(
            String::from_utf8(bytes).map_err(serde::ser::Error::custom)?,
        )
        .map_err(serde::ser::Error::custom)?;
        raw.serialize(serializer)
    }
}

/// Builds a `serde_json::Value` for a row.
///
/// Object values are maps, so a `json` column cannot keep duplicate keys.
/// This re-parses that column and keeps the last key. It is not a check that
/// the stored lexeme survived. Use [`row_to_json_bytes`] or [`RowSerializable`]
/// when the original text, including duplicate keys, has to round-trip.
pub fn row_to_json(schema: &Schema, row: &Row) -> Result<Value> {
    let bytes = serde_json::to_vec(&RowSerializable { schema, row })
        .map_err(|err| Error::corrupt(format!("failed to encode row JSON: {err}")))?;
    value_from_row_json_bytes(&bytes, schema)
}

/// Builds a [`Value`] from canonical row bytes, re-parsing only non-`json` columns.
fn value_from_row_json_bytes(bytes: &[u8], schema: &Schema) -> Result<Value> {
    let mut cursor = skip_json_whitespace(bytes, 0);
    if bytes.get(cursor) != Some(&b'{') {
        return Err(Error::corrupt("row JSON must be an object"));
    }
    cursor += 1;
    cursor = skip_json_whitespace(bytes, cursor);
    let mut obj = Map::new();
    if bytes.get(cursor) == Some(&b'}') {
        return Ok(Value::Object(obj));
    }
    while cursor < bytes.len() {
        let (field_key, next) = parse_row_object_key(bytes, cursor)?;
        cursor = skip_json_whitespace(bytes, next);
        if bytes.get(cursor) != Some(&b':') {
            return Err(Error::corrupt("malformed row JSON object"));
        }
        cursor = skip_json_whitespace(bytes, cursor + 1);
        let value_start = cursor;
        let value_end = crate::json_scan::end_of_json_value(bytes, value_start)?;
        let raw = bytes
            .get(value_start..value_end)
            .ok_or_else(|| Error::corrupt("malformed row JSON object"))?;
        let field = schema
            .fields
            .iter()
            .find(|field| field.name == field_key)
            .ok_or_else(|| Error::corrupt(format!("unknown field `{field_key}` in row")))?;
        let value = match field.ty {
            FieldType::Json if raw == b"null" => Value::Null,
            FieldType::Json => json_lexeme_to_value(raw)?,
            _ => serde_json::from_slice(raw).map_err(|err| {
                Error::corrupt(format!("field `{field_key}` is not valid JSON: {err}"))
            })?,
        };
        obj.insert(field_key, value);
        cursor = skip_json_whitespace(bytes, value_end);
        if bytes.get(cursor) == Some(&b',') {
            cursor = skip_json_whitespace(bytes, cursor + 1);
            continue;
        }
        if bytes.get(cursor) == Some(&b'}') {
            return Ok(Value::Object(obj));
        }
        return Err(Error::corrupt("malformed row JSON object"));
    }
    Err(Error::corrupt("malformed row JSON object"))
}

fn parse_row_object_key(bytes: &[u8], start: usize) -> Result<(String, usize)> {
    if bytes.get(start) != Some(&b'"') {
        return Err(Error::corrupt("row object key must be a string"));
    }
    let end = end_of_json_string(bytes, start)?;
    let inner = bytes
        .get(start + 1..end - 1)
        .ok_or_else(|| Error::corrupt("truncated row object key"))?;
    let key = decode_json_string(inner)
        .map_err(|err| Error::corrupt(format!("row object key is not valid UTF-8: {err}")))?;
    Ok((key, end))
}

fn json_lexeme_to_value(raw: &[u8]) -> Result<Value> {
    let text =
        std::str::from_utf8(raw).map_err(|_| Error::corrupt("json column is not valid UTF-8"))?;
    validate_json_structure(text)?;
    serde_json::from_str(text)
        .map_err(|err| Error::corrupt(format!("json column is invalid: {err}")))
}

/// Serializes a row to JSON bytes, preserving raw `json` column text from ingest.
pub fn row_to_json_bytes(schema: &Schema, row: &Row) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    append_row_json(&mut out, schema, row, None)?;
    Ok(out)
}

/// Same bytes as [`row_to_json_bytes`] for the fields `keep` marks.
///
/// A false field is absent. A true field that is null is JSON `null`. Field
/// order is the schema order. `keep` aligned with every column matches
/// [`row_to_json_bytes`].
pub fn row_to_json_bytes_selected(schema: &Schema, row: &Row, keep: &[bool]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    append_row_json(&mut out, schema, row, Some(keep))?;
    Ok(out)
}

/// Appends one row object. A `json` column is copied as stored. Every other scalar is written
/// as the same lexeme `serde_json` would emit for that value, without building a `Value`.
pub(crate) fn append_row_json(
    out: &mut Vec<u8>,
    schema: &Schema,
    row: &Row,
    keep: Option<&[bool]>,
) -> Result<()> {
    out.push(b'{');
    let mut written = 0usize;
    for (index, (field, scalar)) in schema.fields.iter().zip(row.values.iter()).enumerate() {
        if keep.is_some_and(|keep| !keep.get(index).copied().unwrap_or(false)) {
            continue;
        }
        if written > 0 {
            out.push(b',');
        }
        written += 1;
        write_json_string(out, &field.name);
        out.push(b':');
        write_scalar_json(out, field, scalar)?;
    }
    out.push(b'}');
    Ok(())
}

fn write_scalar_json(out: &mut Vec<u8>, field: &Field, scalar: &Scalar) -> Result<()> {
    match (field.ty, scalar) {
        (_, Scalar::Null) => out.extend_from_slice(b"null"),
        (FieldType::Bool, Scalar::Bool(true)) => out.extend_from_slice(b"true"),
        (FieldType::Bool, Scalar::Bool(false)) => out.extend_from_slice(b"false"),
        (FieldType::Int, Scalar::Int(value)) | (FieldType::Timestamp, Scalar::Timestamp(value)) => {
            let mut buf = itoa::Buffer::new();
            out.extend_from_slice(buf.format(*value).as_bytes());
        }
        (FieldType::Float, Scalar::Float(value)) => {
            if !value.is_finite() {
                return Err(Error::corrupt(format!(
                    "field `{}` is not a finite float",
                    field.name
                )));
            }
            // `Number::from_f64` stores this text. `+0.0`, which is what an integer lexeme
            // `-0` becomes, is `0.0`. A stored signed zero is `-0.0`.
            let mut buf = zmij::Buffer::new();
            out.extend_from_slice(buf.format_finite(*value).as_bytes());
        }
        (FieldType::String | FieldType::Text, Scalar::Str(value)) => {
            write_json_string(out, value);
        }
        (FieldType::Decimal { scale }, Scalar::Decimal(value)) => {
            out.push(b'"');
            write_decimal_lexeme(out, *value, scale);
            out.push(b'"');
        }
        (FieldType::Json, Scalar::Json(raw)) => out.extend_from_slice(raw.as_bytes()),
        _ => {
            return Err(Error::corrupt(format!(
                "field `{}` has a value that does not match its type",
                field.name
            )));
        }
    }
    Ok(())
}

/// JSON string escaping matches `serde_json`: quotes, backslashes, and ASCII controls.
fn write_json_string(out: &mut Vec<u8>, value: &str) {
    out.push(b'"');
    let bytes = value.as_bytes();
    let mut start = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        let escape = JSON_ESCAPE[byte as usize];
        if escape == 0 {
            continue;
        }
        out.extend_from_slice(&bytes[start..index]);
        if escape == b'u' {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(byte >> 4) as usize],
                HEX[(byte & 0x0F) as usize],
            ]);
        } else {
            out.push(b'\\');
            out.push(escape);
        }
        start = index + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

const JSON_ESCAPE: [u8; 256] = [
    b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'b', b't', b'n', b'u', b'f', b'r', b'u',
    b'u', // 0
    b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u', b'u',
    b'u', // 1
    0, 0, b'"', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 2
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 3
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 4
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, b'\\', 0, 0, 0, // 5
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 6
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 7
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 8
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // 9
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // A
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // B
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // C
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // D
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // E
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, // F
];

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
        assert_eq!(format_decimal(0, 0), "0");
        assert_eq!(format_decimal(0, 4), "0.0000");
        assert_eq!(format_decimal(i128::MIN, 0), i128::MIN.to_string());
        assert_eq!(format_decimal(i128::MAX, 2), {
            let digits = i128::MAX.to_string();
            let split = digits.len() - 2;
            format!("{}.{}", &digits[..split], &digits[split..])
        });
    }

    /// The previous writer: one `serde_json::Value` per scalar, then `to_writer`.
    fn row_json_via_value(schema: &Schema, row: &Row, keep: Option<&[bool]>) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(b'{');
        let mut written = 0usize;
        for (index, (field, scalar)) in schema.fields.iter().zip(row.values.iter()).enumerate() {
            if keep.is_some_and(|keep| !keep.get(index).copied().unwrap_or(false)) {
                continue;
            }
            if written > 0 {
                out.push(b',');
            }
            written += 1;
            serde_json::to_writer(&mut out, &field.name).unwrap();
            out.push(b':');
            let value = match (field.ty, scalar) {
                (_, Scalar::Null) => Value::Null,
                (FieldType::Int, Scalar::Int(v)) => Value::Number((*v).into()),
                (FieldType::Float, Scalar::Float(v)) => {
                    Value::Number(serde_json::Number::from_f64(*v).unwrap())
                }
                (FieldType::Bool, Scalar::Bool(v)) => Value::Bool(*v),
                (FieldType::String | FieldType::Text, Scalar::Str(v)) => Value::String(v.clone()),
                (FieldType::Decimal { scale }, Scalar::Decimal(v)) => {
                    Value::String(format_decimal(*v, scale))
                }
                (FieldType::Timestamp, Scalar::Timestamp(v)) => Value::Number((*v).into()),
                (FieldType::Json, Scalar::Json(raw)) => {
                    out.extend_from_slice(raw.as_bytes());
                    continue;
                }
                other => panic!("unexpected {other:?}"),
            };
            serde_json::to_writer(&mut out, &value).unwrap();
        }
        out.push(b'}');
        out
    }

    #[test]
    fn scalar_lexemes_match_the_value_writer() {
        let schema = sample_schema();
        let samples = vec![
            br#"{"ts":1}"#.to_vec(),
            br#"{"ts":1,"user_id":null,"score":null,"ok":null,"action":null,"note":null,"amount":null}"#.to_vec(),
            br#"{"ts":"2024-01-02T03:04:05.123Z","user_id":7,"score":1.5,"ok":true,"action":"buy","note":"hi","amount":"19.99"}"#.to_vec(),
            br#"{"ts":-0,"score":-0,"ok":false,"action":"a\"b\\c","note":"line\n\t\u0000","amount":"-0.01"}"#.to_vec(),
            br#"{"ts":1,"score":-0.0,"note":"caf\u00e9","amount":"0.00"}"#.to_vec(),
            br#"{"ts":1,"score":0.1,"user_id":0,"ok":true}"#.to_vec(),
            format!(r#"{{"ts":{},"user_id":{}}}"#, i64::MIN, i64::MAX).into_bytes(),
        ];
        for sample in &samples {
            let row = parse_event(&schema, sample).unwrap();
            assert_eq!(
                row_to_json_bytes(&schema, &row).unwrap(),
                row_json_via_value(&schema, &row, None),
                "{}",
                String::from_utf8_lossy(sample)
            );
            let keep = [true, false, true, false, true, false, true];
            assert_eq!(
                row_to_json_bytes_selected(&schema, &row, &keep).unwrap(),
                row_json_via_value(&schema, &row, Some(&keep))
            );
        }

        let quoted = Schema {
            timestamp_field: "ts".into(),
            timestamp_index: 0,
            fields: vec![
                Field {
                    name: "ts".into(),
                    ty: FieldType::Timestamp,
                },
                Field {
                    name: "say \"hi\"".into(),
                    ty: FieldType::String,
                },
            ],
        };
        let row = Row {
            ts: 1,
            values: vec![Scalar::Timestamp(1), Scalar::Str("x/y".into())],
        };
        assert_eq!(
            row_to_json_bytes(&quoted, &row).unwrap(),
            row_json_via_value(&quoted, &row, None)
        );

        let floats = Schema {
            timestamp_field: "ts".into(),
            timestamp_index: 0,
            fields: vec![
                Field {
                    name: "ts".into(),
                    ty: FieldType::Timestamp,
                },
                Field {
                    name: "score".into(),
                    ty: FieldType::Float,
                },
            ],
        };
        let positive_zero = Row {
            ts: 1,
            values: vec![Scalar::Timestamp(1), Scalar::Float(0.0)],
        };
        let bytes = row_to_json_bytes(&floats, &positive_zero).unwrap();
        assert_eq!(bytes, row_json_via_value(&floats, &positive_zero, None));
        assert_eq!(bytes, br#"{"ts":1,"score":0.0}"#);
        let negative_zero = Row {
            ts: 1,
            values: vec![Scalar::Timestamp(1), Scalar::Float(-0.0)],
        };
        assert_eq!(
            row_to_json_bytes(&floats, &negative_zero).unwrap(),
            row_json_via_value(&floats, &negative_zero, None)
        );
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
    fn json_repeated_top_level_field_last_wins() {
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
        let row = parse_event(&schema, br#"{"ts":1,"props":{"a":1},"props":{"a":2}}"#).unwrap();
        match &row.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, r#"{"a":2}"#),
            other => panic!("unexpected {other:?}"),
        }
        let null_last = parse_event(&schema, br#"{"ts":1,"props":{"a":1},"props":null}"#).unwrap();
        assert!(matches!(null_last.values[1], Scalar::Null));
        let object_last =
            parse_event(&schema, br#"{"ts":1,"props":null,"props":{"a":1}}"#).unwrap();
        match &object_last.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, r#"{"a":1}"#),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn json_utf8_object_key_matches_schema_field() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "café", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let raw_key = br#"{"ts":1,"caf\u00e9":{"ok":true}}"#;
        let row = parse_event(&schema, raw_key).unwrap();
        match &row.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, r#"{"ok":true}"#),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn json_surrogate_key_suffix_does_not_alias_field_name() {
        let schema = parse_schema(
            r#"{
                "timestamp_field": "ts",
                "fields": [
                    {"name": "ts", "type": "timestamp"},
                    {"name": "😀x", "type": "json"},
                    {"name": "props", "type": "json"}
                ]
            }"#,
        )
        .unwrap();
        let row = parse_event(&schema, br#"{"ts":1,"\uD83D\uDE00x":1,"props":2}"#).unwrap();
        match &row.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, "1"),
            other => panic!("unexpected {other:?}"),
        }
        match &row.values[2] {
            Scalar::Json(raw) => assert_eq!(raw, "2"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn json_ignored_surrogate_key_does_not_break_parse() {
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
        let row = parse_event(&schema, br#"{"ts":1,"\uD83D\uDE00":9,"props":2}"#).unwrap();
        match &row.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, "2"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn json_utf8_and_surrogate_strings() {
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
        let cafe = parse_event(&schema, br#"{"ts":1,"props":"caf\u00e9"}"#).unwrap();
        match &cafe.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, "\"caf\\u00e9\""),
            other => panic!("unexpected {other:?}"),
        }
        let emoji = parse_event(&schema, br#"{"ts":1,"props":"\uD83D\uDE00"}"#).unwrap();
        match &emoji.values[1] {
            Scalar::Json(raw) => assert_eq!(raw, r#""\uD83D\uDE00""#),
            other => panic!("unexpected {other:?}"),
        }
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

    /// The previous parser: `serde_json::from_slice` into a DOM, then field walks.
    fn parse_event_dom(schema: &Schema, json: &[u8]) -> Result<Row> {
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
                        match crate::json_scan::extract_object_field_raw_last(json, &field.name)? {
                            None => Scalar::Null,
                            Some(raw) if raw.as_bytes() == b"null" => Scalar::Null,
                            Some(raw) => {
                                validate_json_structure(&raw)?;
                                Scalar::Json(raw)
                            }
                        }
                    } else {
                        dom_field(&field.name, field.ty, item)?
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

    fn dom_field(name: &str, ty: FieldType, value: &Value) -> Result<Scalar> {
        match ty {
            FieldType::Int => value
                .as_i64()
                .map(Scalar::Int)
                .ok_or_else(|| Error::event(format!("field `{name}` must be an integer"))),
            FieldType::Float => {
                let number = value
                    .as_f64()
                    .ok_or_else(|| Error::event(format!("field `{name}` must be a number")))?;
                if !number.is_finite() {
                    return Err(Error::event(format!("field `{name}` must be finite")));
                }
                Ok(Scalar::Float(number))
            }
            FieldType::Bool => value
                .as_bool()
                .map(Scalar::Bool)
                .ok_or_else(|| Error::event(format!("field `{name}` must be a boolean"))),
            FieldType::String | FieldType::Text => value
                .as_str()
                .map(|text| Scalar::Str(text.to_string()))
                .ok_or_else(|| Error::event(format!("field `{name}` must be a string"))),
            FieldType::Decimal { scale } => {
                let text = value.as_str().ok_or_else(|| {
                    Error::event(format!("field `{name}` decimal must be a string"))
                })?;
                Ok(Scalar::Decimal(parse_decimal(text, scale).map_err(
                    |err| Error::event(format!("field `{name}`: {err}")),
                )?))
            }
            FieldType::Timestamp => Ok(Scalar::Timestamp(
                crate::schema::parse_timestamp_value(value)
                    .map_err(|err| Error::event(format!("field `{name}`: {err}")))?,
            )),
            FieldType::Json => Err(Error::event("json fields are parsed from raw event bytes")),
        }
    }

    fn scalars_match(left: &Scalar, right: &Scalar) -> bool {
        match (left, right) {
            (Scalar::Float(a), Scalar::Float(b)) => a.to_bits() == b.to_bits(),
            _ => left == right,
        }
    }

    fn assert_same_parse(schema: &Schema, json: &[u8]) {
        let dom = parse_event_dom(schema, json);
        let scan = parse_event(schema, json);
        match (dom, scan) {
            (Ok(dom), Ok(scan)) => {
                assert_eq!(dom.ts, scan.ts, "{}", String::from_utf8_lossy(json));
                assert_eq!(dom.values.len(), scan.values.len());
                for (index, (left, right)) in dom.values.iter().zip(scan.values.iter()).enumerate()
                {
                    assert!(
                        scalars_match(left, right),
                        "field {index} {json:?}: {left:?} vs {right:?}"
                    );
                }
            }
            (Err(_), Err(_)) => {}
            (Ok(_), Err(err)) => panic!("scan rejected {}: {err}", String::from_utf8_lossy(json)),
            (Err(err), Ok(_)) => panic!("scan accepted {}: {err}", String::from_utf8_lossy(json)),
        }
    }

    #[test]
    fn scan_accepts_and_rejects_the_same_events_as_the_dom_parser() {
        let schema = sample_schema();
        let json_schema = parse_schema(
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"props","type":"json"},{"name":"café","type":"string"}]}"#,
        )
        .unwrap();
        let mut samples: Vec<Vec<u8>> = vec![
            br#"{"ts":1}"#.to_vec(),
            br#"{"ts":1,"user_id":null}"#.to_vec(),
            br#"{"user_id":"x","user_id":2,"ts":1}"#.to_vec(),
            br#"{"ts":1,"user_id":2,"user_id":"x"}"#.to_vec(),
            br#"{"ts":"2024-01-02T03:04:05.123Z","user_id":7,"score":1.5,"ok":true,"action":"buy","note":"hi","amount":"19.99"}"#.to_vec(),
            br#"{"ts":1,"score":1e2}"#.to_vec(),
            br#"{"ts":1,"score":1e309}"#.to_vec(),
            br#"{"ts":1,"score":1e10000}"#.to_vec(),
            br#"{"ts":1,"user_id":1.5}"#.to_vec(),
            br#"{"ts":1,"user_id":9223372036854775807}"#.to_vec(),
            br#"{"ts":1,"user_id":9223372036854775808}"#.to_vec(),
            format!(r#"{{"ts":{}}}"#, i64::MIN).into_bytes(),
            format!(r#"{{"ts":{}}}"#, i64::MAX).into_bytes(),
            br#"{"ts":-0,"score":-0.0,"ok":false}"#.to_vec(),
            br#"{"ts":1,"score":-0}"#.to_vec(),
            br#"{"ts":01}"#.to_vec(),
            br#"{"ts":1,}"#.to_vec(),
            br#"{"ts":1}{}"#.to_vec(),
            br#"[]"#.to_vec(),
            br#"null"#.to_vec(),
            br#""#.to_vec(),
            br#"{"ts":1,"note":"a\nb"}"#.to_vec(),
            br#"{"ts":1,"note":"\u0000"}"#.to_vec(),
            br#"{"ts":1,"ok":false,"extra":{"a":1},"amount":"1.00"}"#.to_vec(),
            br#"{"ts": "1700"}"#.to_vec(),
            br#"{"ts":1,"action":{"nested":true}}"#.to_vec(),
            br#"{"ts":1,"amount":1}"#.to_vec(),
            br#"{"ts":1,"amount":".5"}"#.to_vec(),
            br#"{"ts":1,"amount":"1e2"}"#.to_vec(),
            br#"{"ts":true}"#.to_vec(),
            br#"{"ts":1,"score":"1"}"#.to_vec(),
            br#"{"a":1}"#.to_vec(),
            br#"  {"ts":1}  "#.to_vec(),
            br#"{"ts":1,"note":"caf\u00e9"}"#.to_vec(),
            br#"{"ts":1, "props": {"b":1, "a":[true, null]}, "props": [1, "z"]}"#.to_vec(),
            br#"{"ts":1,"props":null,"props":{"a":1}}"#.to_vec(),
            br#"{"ts":1,"caf\u00e9":"ok","props":1}"#.to_vec(),
            br#"{"ts":1,"\uD83D\uDE00":9,"props":2}"#.to_vec(),
            br#"{"ts":1,"props":"\uD83D\uDE00"}"#.to_vec(),
            br#"{"ts":1,"props":{"a":1,}}"#.to_vec(),
            b"{\"ts\":1,\"note\":\"\x0c\"}".to_vec(),
            b"{\x0c\"ts\":1}".to_vec(),
            br#"{"ts":1,"user_id":-}"#.to_vec(),
            br#"{"ts":+1}"#.to_vec(),
            br#"{"ts":1e2}"#.to_vec(),
            br#"{"ts":"1e2"}"#.to_vec(),
            br#"{"ts":1,"amount":" 12.30 "}"#.to_vec(),
            br#"{"ts":1,"ok":truee}"#.to_vec(),
            br#"{"ts":1,"note":"\uD800"}"#.to_vec(),
            br#"{"ts":1,"score":0e0}"#.to_vec(),
            br#"{"ts":1,"user_id":0e0}"#.to_vec(),
        ];
        for depth in [1usize, 127, 128, 129, 200] {
            let mut nested = "[".repeat(depth);
            nested.push('1');
            nested.push_str(&"]".repeat(depth));
            samples.push(format!(r#"{{"ts":1,"extra":{nested}}}"#).into_bytes());
            samples.push(format!(r#"{{"ts":1,"props":{nested}}}"#).into_bytes());
        }
        let big = vec![b' '; 1024 * 1024 + 1];
        samples.push(big);
        for sample in &samples {
            assert_same_parse(&schema, sample);
            assert_same_parse(&json_schema, sample);
        }
    }
}
