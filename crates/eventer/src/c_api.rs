use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::ptr;
use std::sync::Mutex;

use crate::error::Error;
use crate::store::{Predicate, Store};
use crate::value::Scalar;

/// Opaque handle returned by [`eventer_open`].
#[repr(C)]
pub struct EventerStore {
    _private: [u8; 0],
}

struct FfiStore {
    store: Store,
    err: Mutex<CString>,
}

fn empty_cstring() -> CString {
    CString::new("").unwrap()
}

fn set_err(handle: &FfiStore, err: &Error) {
    let text = CString::new(err.to_string()).unwrap_or_else(|_| empty_cstring());
    *handle
        .err
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = text;
}

fn clear_err(handle: &FfiStore) {
    *handle
        .err
        .lock()
        .unwrap_or_else(|poison| poison.into_inner()) = empty_cstring();
}

fn map_err(err: &Error) -> c_int {
    match err {
        Error::Closed => -5,
        Error::Event(_) | Error::Json(_) | Error::Schema(_) => -3,
        Error::Corrupt(_) | Error::Io(_) => -2,
    }
}

unsafe fn borrow<'a>(store: *mut EventerStore) -> Option<&'a FfiStore> {
    if store.is_null() {
        None
    } else {
        Some(&*(store as *const FfiStore))
    }
}

/// Open a store in `dir` using the JSON schema at `schema_path`. Returns null on failure.
///
/// # Safety
/// Both pointers must be null-terminated and valid for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn eventer_open(
    dir: *const c_char,
    schema_path: *const c_char,
) -> *mut EventerStore {
    if dir.is_null() || schema_path.is_null() {
        return ptr::null_mut();
    }
    let dir = match CStr::from_ptr(dir).to_str() {
        Ok(dir) => dir,
        Err(_) => return ptr::null_mut(),
    };
    let schema_path = match CStr::from_ptr(schema_path).to_str() {
        Ok(path) => path,
        Err(_) => return ptr::null_mut(),
    };
    match Store::open(dir, schema_path) {
        Ok(store) => Box::into_raw(Box::new(FfiStore {
            store,
            err: Mutex::new(empty_cstring()),
        })) as *mut EventerStore,
        Err(_) => ptr::null_mut(),
    }
}

/// Queue one JSON event. The bytes do not need to be null-terminated.
/// Durability happens on [`eventer_flush`], [`eventer_query`], or [`eventer_close`].
///
/// # Safety
/// `store` must come from [`eventer_open`]. `json` must point at `len` readable bytes.
#[no_mangle]
pub unsafe extern "C" fn eventer_append(
    store: *mut EventerStore,
    json: *const u8,
    len: usize,
) -> c_int {
    let Some(handle) = borrow(store) else {
        return -1;
    };
    if json.is_null() && len != 0 {
        set_err(handle, &Error::event("json pointer is null"));
        return -1;
    }
    let bytes = if len == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(json, len)
    };
    match handle.store.append_json(bytes) {
        Ok(()) => {
            clear_err(handle);
            0
        }
        Err(err) => {
            let code = map_err(&err);
            set_err(handle, &err);
            code
        }
    }
}

/// Encode, write, and fsync every queued event.
///
/// # Safety
/// `store` must come from [`eventer_open`] and must not be used after [`eventer_close`].
#[no_mangle]
pub unsafe extern "C" fn eventer_flush(store: *mut EventerStore) -> c_int {
    let Some(handle) = borrow(store) else {
        return -1;
    };
    match handle.store.flush() {
        Ok(()) => {
            clear_err(handle);
            0
        }
        Err(err) => {
            let code = map_err(&err);
            set_err(handle, &err);
            code
        }
    }
}

/// Write events with timestamps in `from_ms..=to_ms` as a JSON array.
/// On success returns 0 and sets `*out_len` to the byte count.
/// When `out_cap` is too small, returns -4 and sets `*out_len` to the required size.
///
/// # Safety
/// `out` may be null only when `out_cap` is 0. `out_len` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn eventer_query(
    store: *mut EventerStore,
    from_ms: i64,
    to_ms: i64,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> c_int {
    query_into(store, from_ms, to_ms, &[], out, out_cap, out_len)
}

/// Query a time range and keep rows whose string or text column equals `filter_val`.
///
/// `filter_col` is a schema field name, for example `"type"` or `"action"`.
/// `filter_val` is the exact UTF-8 value. The match is pushed into block decoding.
///
/// # Safety
/// `filter_col` and `filter_val` must be null-terminated. `out` may be null only when
/// `out_cap` is 0. `out_len` must be non-null.
#[no_mangle]
pub unsafe extern "C" fn eventer_query_filtered(
    store: *mut EventerStore,
    from_ms: i64,
    to_ms: i64,
    filter_col: *const c_char,
    filter_val: *const c_char,
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> c_int {
    let Some(handle) = borrow(store) else {
        return -1;
    };
    if filter_col.is_null() || filter_val.is_null() {
        set_err(
            handle,
            &Error::event("filter column and value are required"),
        );
        return -1;
    }
    let column = match CStr::from_ptr(filter_col).to_str() {
        Ok(column) => column,
        Err(_) => {
            set_err(handle, &Error::event("filter column is not utf-8"));
            return -1;
        }
    };
    let value = match CStr::from_ptr(filter_val).to_str() {
        Ok(value) => value,
        Err(_) => {
            set_err(handle, &Error::event("filter value is not utf-8"));
            return -1;
        }
    };
    let predicate = Predicate::Eq(column.to_string(), Scalar::Str(value.to_string()));
    query_into(store, from_ms, to_ms, &[predicate], out, out_cap, out_len)
}

unsafe fn query_into(
    store: *mut EventerStore,
    from_ms: i64,
    to_ms: i64,
    predicates: &[Predicate],
    out: *mut u8,
    out_cap: usize,
    out_len: *mut usize,
) -> c_int {
    let Some(handle) = borrow(store) else {
        return -1;
    };
    if out_len.is_null() || (out.is_null() && out_cap != 0) {
        set_err(handle, &Error::event("invalid query buffer"));
        return -1;
    }
    match handle
        .store
        .query_json_with_filter(from_ms, to_ms, predicates)
    {
        Ok(bytes) => {
            *out_len = bytes.len();
            if out_cap < bytes.len() {
                set_err(handle, &Error::event("query buffer is too small"));
                return -4;
            }
            if !bytes.is_empty() {
                ptr::copy_nonoverlapping(bytes.as_ptr(), out, bytes.len());
            }
            clear_err(handle);
            0
        }
        Err(err) => {
            let code = map_err(&err);
            set_err(handle, &err);
            code
        }
    }
}

/// Last error for this store, or an empty string. The pointer is owned by the store.
///
/// # Safety
/// `store` must come from [`eventer_open`]. The returned pointer is invalidated by the next call.
#[no_mangle]
pub unsafe extern "C" fn eventer_last_error(store: *mut EventerStore) -> *const c_char {
    let Some(handle) = borrow(store) else {
        return ptr::null();
    };
    let guard = handle
        .err
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    guard.as_ptr()
}

/// Flush and free the store. The pointer must not be used again.
///
/// # Safety
/// `store` must come from [`eventer_open`] and must be closed at most once.
#[no_mangle]
pub unsafe extern "C" fn eventer_close(store: *mut EventerStore) {
    if store.is_null() {
        return;
    }
    let handle = Box::from_raw(store as *mut FfiStore);
    let _ = handle.store.close();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn c_abi_appends_and_queries() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("eventer-c-{}-{}", std::process::id(), nanos));
        fs::create_dir_all(&dir).unwrap();
        let schema = dir.join("schema.json");
        fs::write(
            &schema,
            r#"{"timestamp_field":"ts","fields":[{"name":"ts","type":"timestamp"},{"name":"action","type":"string"}]}"#,
        )
        .unwrap();
        let data = dir.join("data");
        let dir_c = CString::new(data.to_str().unwrap()).unwrap();
        let schema_c = CString::new(schema.to_str().unwrap()).unwrap();
        let store = unsafe { eventer_open(dir_c.as_ptr(), schema_c.as_ptr()) };
        assert!(!store.is_null());
        let json = br#"{"ts":42,"action":"click"}"#;
        assert_eq!(
            unsafe { eventer_append(store, json.as_ptr(), json.len()) },
            0
        );
        let mut needed = 0usize;
        assert_eq!(
            unsafe { eventer_query(store, 0, 100, std::ptr::null_mut(), 0, &mut needed) },
            -4
        );
        assert!(needed > 2);
        let mut buf = vec![0u8; needed];
        let mut got = 0usize;
        assert_eq!(
            unsafe { eventer_query(store, 0, 100, buf.as_mut_ptr(), buf.len(), &mut got) },
            0
        );
        let text = std::str::from_utf8(&buf[..got]).unwrap();
        assert!(text.contains("click"));
        assert!(text.contains("42"));

        let col = CString::new("action").unwrap();
        let val = CString::new("click").unwrap();
        let mut filtered_needed = 0usize;
        assert_eq!(
            unsafe {
                eventer_query_filtered(
                    store,
                    0,
                    100,
                    col.as_ptr(),
                    val.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                    &mut filtered_needed,
                )
            },
            -4
        );
        let mut filtered = vec![0u8; filtered_needed];
        let mut filtered_got = 0usize;
        assert_eq!(
            unsafe {
                eventer_query_filtered(
                    store,
                    0,
                    100,
                    col.as_ptr(),
                    val.as_ptr(),
                    filtered.as_mut_ptr(),
                    filtered.len(),
                    &mut filtered_got,
                )
            },
            0
        );
        let filtered_text = std::str::from_utf8(&filtered[..filtered_got]).unwrap();
        assert!(filtered_text.contains("click"));

        let other = CString::new("view").unwrap();
        let mut missed = 0usize;
        assert_eq!(
            unsafe {
                eventer_query_filtered(
                    store,
                    0,
                    100,
                    col.as_ptr(),
                    other.as_ptr(),
                    std::ptr::null_mut(),
                    0,
                    &mut missed,
                )
            },
            -4
        );
        let mut empty_buf = vec![0u8; missed];
        let mut empty_got = 0usize;
        assert_eq!(
            unsafe {
                eventer_query_filtered(
                    store,
                    0,
                    100,
                    col.as_ptr(),
                    other.as_ptr(),
                    empty_buf.as_mut_ptr(),
                    empty_buf.len(),
                    &mut empty_got,
                )
            },
            0
        );
        assert_eq!(std::str::from_utf8(&empty_buf[..empty_got]).unwrap(), "[]");

        let missing_col = CString::new("nope").unwrap();
        assert_eq!(
            unsafe {
                eventer_query_filtered(
                    store,
                    0,
                    100,
                    missing_col.as_ptr(),
                    val.as_ptr(),
                    empty_buf.as_mut_ptr(),
                    empty_buf.len(),
                    &mut empty_got,
                )
            },
            -3
        );
        unsafe { eventer_close(store) };
        let _ = fs::remove_dir_all(&dir);
    }
}
