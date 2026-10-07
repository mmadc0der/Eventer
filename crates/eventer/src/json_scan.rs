use crate::error::{Error, Result};

/// Validates that `text` is a single non-null JSON value without building a DOM.
pub fn validate_json_structure(text: &str) -> Result<()> {
    let bytes = text.as_bytes();
    let end = end_of_json_value(bytes, 0)?;
    if skip_json_whitespace(bytes, end) != bytes.len() {
        return Err(Error::event("json field has trailing data"));
    }
    if text.trim() == "null" {
        return Err(Error::event("json field must not be null"));
    }
    Ok(())
}

pub fn skip_json_whitespace(bytes: &[u8], start: usize) -> usize {
    let mut i = start;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    i
}

/// Returns the raw UTF-8 JSON text of the last top-level object field named `key`, if any.
pub fn extract_object_field_raw_last(document: &[u8], key: &str) -> Result<Option<String>> {
    let mut cursor = skip_json_whitespace(document, 0);
    if document.get(cursor) != Some(&b'{') {
        return Err(Error::event("event must be a JSON object"));
    }
    cursor += 1;
    cursor = skip_json_whitespace(document, cursor);
    if document.get(cursor) == Some(&b'}') {
        return Ok(None);
    }
    let mut last: Option<String> = None;
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
            last = Some(
                String::from_utf8(raw.to_vec())
                    .map_err(|_| Error::event("json field is not valid UTF-8"))?,
            );
        }
        cursor = skip_json_whitespace(document, value_end);
        if document.get(cursor) == Some(&b',') {
            cursor = skip_json_whitespace(document, cursor + 1);
            continue;
        }
        if document.get(cursor) == Some(&b'}') {
            return Ok(last);
        }
        return Err(Error::event("malformed JSON object"));
    }
    Err(Error::event("malformed JSON object"))
}

pub fn end_of_json_string(bytes: &[u8], start: usize) -> Result<usize> {
    if bytes.get(start) != Some(&b'"') {
        return Err(Error::event("expected JSON string"));
    }
    let mut i = start + 1;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'"' {
            return Ok(i + 1);
        }
        if byte == b'\\' {
            i += 1;
            let esc = bytes
                .get(i)
                .ok_or_else(|| Error::event("truncated JSON string"))?;
            if *esc == b'u' {
                i += 5;
            } else {
                i += 1;
            }
            continue;
        }
        i += 1;
    }
    Err(Error::event("truncated JSON string"))
}

pub fn decode_json_string(bytes: &[u8]) -> Result<String> {
    let mut out = String::new();
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'\\' {
            i += 1;
            let esc = bytes
                .get(i)
                .ok_or_else(|| Error::event("truncated JSON string"))?;
            match esc {
                b'"' => out.push('"'),
                b'\\' => out.push('\\'),
                b'/' => out.push('/'),
                b'b' => out.push('\u{0008}'),
                b'f' => out.push('\u{000c}'),
                b'n' => out.push('\n'),
                b'r' => out.push('\r'),
                b't' => out.push('\t'),
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
                    if (0xD800..=0xDBFF).contains(&code) {
                        let low_hex = bytes
                            .get(i + 2..i + 6)
                            .ok_or_else(|| Error::event("truncated JSON unicode escape"))?;
                        if bytes.get(i + 1) != Some(&b'\\') || bytes.get(i + 2) != Some(&b'u') {
                            return Err(Error::event("invalid JSON unicode escape"));
                        }
                        let low = u16::from_str_radix(
                            std::str::from_utf8(low_hex)
                                .map_err(|_| Error::event("invalid JSON unicode escape"))?,
                            16,
                        )
                        .map_err(|_| Error::event("invalid JSON unicode escape"))?;
                        if !(0xDC00..=0xDFFF).contains(&low) {
                            return Err(Error::event("invalid JSON unicode escape"));
                        }
                        let combined =
                            0x10000 + (((code - 0xD800) as u32) << 10) + (low - 0xDC00) as u32;
                        let ch = char::from_u32(combined)
                            .ok_or_else(|| Error::event("invalid JSON unicode escape"))?;
                        out.push(ch);
                        i += 6;
                    } else if (0xDC00..=0xDFFF).contains(&code) {
                        return Err(Error::event("invalid JSON unicode escape"));
                    } else {
                        let ch = char::from_u32(code as u32)
                            .ok_or_else(|| Error::event("invalid JSON unicode escape"))?;
                        out.push(ch);
                    }
                }
                _ => return Err(Error::event("invalid JSON string escape")),
            }
            i += 1;
            continue;
        }
        out.push(char::from_u32(byte as u32).ok_or_else(|| {
            Error::event("JSON string is not valid UTF-8")
        })?);
        i += 1;
    }
    Ok(out)
}

fn parse_json_string_token(bytes: &[u8], start: usize) -> Result<(String, usize)> {
    if bytes.get(start) != Some(&b'"') {
        return Err(Error::event("JSON object key must be a string"));
    }
    let end = end_of_json_string(bytes, start)?;
    let inner = bytes
        .get(start + 1..end - 1)
        .ok_or_else(|| Error::event("truncated JSON string"))?;
    let decoded = decode_json_string(inner)?;
    Ok((decoded, end))
}

pub fn end_of_json_value(bytes: &[u8], start: usize) -> Result<usize> {
    let byte = *bytes
        .get(start)
        .ok_or_else(|| Error::event("truncated JSON value"))?;
    match byte {
        b'"' => end_of_json_string(bytes, start),
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
