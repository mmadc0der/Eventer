// Minimal cgo caller for the Eventer cdylib.
//
// Build the library first, from the repo root:
//
//	cargo build -p eventer --release
//
// Then, from this directory:
//
//	CGO_ENABLED=1 go run .
//
// Linux: cargo writes target/release/libeventer.so. The LDFLAGS below pass
// -leventer and an rpath so the dynamic loader finds that .so.
//
// Windows (MinGW/cgo): cargo writes target/release/eventer.dll and an import
// library libeventer.dll.a. -leventer links the import library. At runtime the
// loader looks for eventer.dll next to the exe or on PATH. An MSVC build emits
// eventer.dll.lib instead; cgo's gcc driver will not pick that up with -leventer.
package main

/*
#cgo CFLAGS: -I${SRCDIR}/../../include
#cgo linux LDFLAGS: -L${SRCDIR}/../../target/release -Wl,-rpath,${SRCDIR}/../../target/release -leventer
#cgo windows LDFLAGS: -L${SRCDIR}/../../target/release -leventer

#include "eventer.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"os"
	"path/filepath"
	"unsafe"
)

func main() {
	root, err := os.MkdirTemp("", "eventer-go-")
	if err != nil {
		fatal(err)
	}
	defer os.RemoveAll(root)

	schema := filepath.Join(root, "schema.json")
	if err := os.WriteFile(schema, []byte(schemaJSON), 0o644); err != nil {
		fatal(err)
	}
	data := filepath.Join(root, "data")

	store := openStore(data, schema)
	defer C.eventer_close(store)

	event := []byte(`{"ts":1700000000000,"user_id":7,"score":1.5,"ok":true,"action":"click","note":"from go","amount":"19.99"}`)
	if rc := C.eventer_append(store, (*C.uint8_t)(unsafe.Pointer(&event[0])), C.size_t(len(event))); rc != 0 {
		fatalf("append: %d: %s", rc, lastError(store))
	}

	// Query flushes queued events. from/to are inclusive unix milliseconds.
	body := query(store, 1700000000000, 1700000000000)
	fmt.Printf("%s\n", body)
}

func openStore(dir, schema string) *C.EventerStore {
	cDir := C.CString(dir)
	cSchema := C.CString(schema)
	// eventer_open only borrows these C strings for the call.
	defer C.free(unsafe.Pointer(cDir))
	defer C.free(unsafe.Pointer(cSchema))

	store := C.eventer_open(cDir, cSchema)
	if store == nil {
		fatalf("open failed (eventer_open returns NULL and has no last_error)")
	}
	return store
}

func query(store *C.EventerStore, from, to int64) []byte {
	var needed C.size_t
	// out == NULL and out_cap == 0 asks for the size. -4 means "too small", which is expected here.
	rc := C.eventer_query(store, C.int64_t(from), C.int64_t(to), nil, 0, &needed)
	if rc != -4 {
		fatalf("query size: %d: %s", rc, lastError(store))
	}
	if needed == 0 {
		return nil
	}
	buf := make([]byte, needed)
	rc = C.eventer_query(
		store,
		C.int64_t(from),
		C.int64_t(to),
		(*C.uint8_t)(unsafe.Pointer(&buf[0])),
		C.size_t(len(buf)),
		&needed,
	)
	if rc != 0 {
		fatalf("query: %d: %s", rc, lastError(store))
	}
	// The library writes raw JSON bytes. It does not allocate this buffer and does not NUL-terminate it.
	return buf[:needed]
}

// lastError copies the store-owned C string into Go memory.
// Do not C.free the pointer: the next API call on this store reuses it.
func lastError(store *C.EventerStore) string {
	msg := C.eventer_last_error(store)
	if msg == nil {
		return ""
	}
	return C.GoString(msg)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

const schemaJSON = `{
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
}
`
