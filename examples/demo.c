#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/eventer.h"

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "usage: %s <data-dir> <schema.json>\n", argv[0]);
        return 1;
    }
    EventerStore *store = eventer_open(argv[1], argv[2]);
    if (store == NULL) {
        fprintf(stderr, "open failed\n");
        return 1;
    }
    const char *json = "{\"ts\":1700000000000,\"user_id\":7,\"score\":1.5,\"ok\":true,"
                       "\"action\":\"click\",\"note\":\"demo\",\"amount\":\"19.99\"}";
    if (eventer_append(store, (const uint8_t *)json, strlen(json)) != 0) {
        fprintf(stderr, "append failed: %s\n", eventer_last_error(store));
        eventer_close(store);
        return 1;
    }
    if (eventer_flush(store) != 0) {
        fprintf(stderr, "flush failed: %s\n", eventer_last_error(store));
        eventer_close(store);
        return 1;
    }
    size_t needed = 0;
    int rc = eventer_query(store, 1700000000000LL, 1700000000000LL, NULL, 0, &needed);
    if (rc != -4) {
        fprintf(stderr, "size query returned %d (%s)\n", rc, eventer_last_error(store));
        eventer_close(store);
        return 1;
    }
    uint8_t *buf = malloc(needed);
    if (buf == NULL) {
        eventer_close(store);
        return 1;
    }
    size_t got = 0;
    if (eventer_query(store, 1700000000000LL, 1700000000000LL, buf, needed, &got) != 0) {
        fprintf(stderr, "query failed: %s\n", eventer_last_error(store));
        free(buf);
        eventer_close(store);
        return 1;
    }
    fwrite(buf, 1, got, stdout);
    fputc('\n', stdout);
    free(buf);
    eventer_close(store);
    return 0;
}
