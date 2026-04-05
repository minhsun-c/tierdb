#include "engine.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "lsm_iter.h"
#include "memtable.h"

int engine_open(struct engine *e, struct engine_options *opts)
{
    if (!e || !opts)
        return -1;

    e->memtable = malloc(sizeof(struct memtable));
    if (!e->memtable)
        return -1;
    if (memtable_init(e->memtable, opts->max_level, 0) < 0) {
        free(e->memtable);
        return -1;
    }
    e->imm_memtables = calloc(opts->imm_cap, sizeof(struct memtable *));
    if (!e->imm_memtables) {
        memtable_destroy(e->memtable);
        free(e->memtable);
        return -1;
    }
    e->imm_count = 0;
    e->next_id = 1;
    memcpy(&e->opts, opts, sizeof(struct engine_options));
    return 0;
}

void engine_close(struct engine *e)
{
    if (!e)
        return;
    memtable_destroy(e->memtable);
    free(e->memtable);
    for (uint32_t i = 0; i < e->imm_count; i++) {
        memtable_destroy(e->imm_memtables[i]);
        free(e->imm_memtables[i]);
    }
    free(e->imm_memtables);
}

int engine_put(struct engine *e,
               const uint8_t *key,
               size_t key_len,
               const uint8_t *value,
               size_t value_len)
{
    if (!e || !key || key_len == 0)
        return -1;
    if (memtable_put(e->memtable, key, key_len, value, value_len) < 0)
        return -1;
    if (memtable_is_full(e->memtable, e->opts.threshold))
        return engine_freeze_memtable(e);
    return 0;
}

struct memtable_entry *engine_get(struct engine *e,
                                  const uint8_t *key,
                                  size_t key_len)
{
    if (!e || !key || key_len == 0)
        return NULL;
    struct memtable_entry *entry = memtable_get(e->memtable, key, key_len);
    if (entry)
        return entry;
    for (int i = (int) e->imm_count - 1; i >= 0; i--) {
        entry = memtable_get(e->imm_memtables[i], key, key_len);
        if (entry)
            return entry;
    }
    return NULL;
}

int engine_delete(struct engine *e, const uint8_t *key, size_t key_len)
{
    return engine_put(e, key, key_len, NULL, 0);
}

int engine_scan(struct engine *e,
                const uint8_t *lower,
                size_t lower_len,
                const uint8_t *upper,
                size_t upper_len,
                struct lsm_iter *iter)
{
    if (!e || !iter)
        return -1;

    struct memtable **src =
        malloc(sizeof(struct memtable *) * (1 + e->imm_count));
    if (!src)
        return -1;

    src[0] = e->memtable;
    for (uint32_t i = 1; i <= e->imm_count; i++)
        src[i] = e->imm_memtables[e->imm_count - i];
    int ret = lsm_iter_init(iter, src, e->imm_count + 1, lower, lower_len,
                            upper, upper_len);
    free(src);
    return ret;
}

int engine_freeze_memtable(struct engine *e)
{
    if (e->imm_count >= e->opts.imm_cap)
        return -1;

    struct memtable *new_mt = malloc(sizeof(struct memtable));
    if (!new_mt)
        return -1;
    if (memtable_init(new_mt, e->opts.max_level, e->next_id) < 0) {
        free(new_mt);
        return -1;
    }
    e->next_id++;
    e->imm_memtables[e->imm_count] = e->memtable;
    e->imm_count++;
    e->memtable = new_mt;
    return 0;
}