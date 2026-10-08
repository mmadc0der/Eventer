#ifndef EVENTER_H
#define EVENTER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct EventerStore EventerStore;

/* 0 success, -1 bad argument, -2 io/corrupt, -3 event or schema, -4 buffer too small, -5 closed. */

EventerStore *eventer_open(const char *dir, const char *schema_path);

/* Queue one JSON object. Bytes are copied. Call eventer_flush before assuming durability. */
int32_t eventer_append(EventerStore *store, const uint8_t *json, size_t len);

int32_t eventer_flush(EventerStore *store);

/* Inclusive unix-millisecond range. Writes a JSON array. If out_cap is too small,
   returns -4 and sets *out_len to the size required. out may be NULL when out_cap is 0. */
int32_t eventer_query(EventerStore *store, int64_t from_ms, int64_t to_ms,
                      uint8_t *out, size_t out_cap, size_t *out_len);

/* Same as eventer_query, keeping rows whose string or text column equals filter_val.
   filter_col and filter_val are NUL-terminated. Example: column "type", value
   "dev.genesis.run.assistant". */
int32_t eventer_query_filtered(EventerStore *store, int64_t from_ms, int64_t to_ms,
                               const char *filter_col, const char *filter_val,
                               uint8_t *out, size_t out_cap, size_t *out_len);

const char *eventer_last_error(EventerStore *store);

void eventer_close(EventerStore *store);

#ifdef __cplusplus
}
#endif

#endif
