#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "engine.h"
#include "lsm_iter.h"
#include "memtable.h"
#include "mt_iter.h"
#include "sst.h"
#include "sst_builder.h"
#include "sst_iter.h"

int engine_open(struct engine *e, struct engine_options *opts, const char *path)
{
    if (!e || !opts)
        return -1;

    /* mutable memtable */
    e->memtable = malloc(sizeof(struct memtable));
    if (!e->memtable)
        return -1;
    if (memtable_init(e->memtable, opts->max_level, 0) < 0) {
        free(e->memtable);
        return -1;
    }

    /* immutable memtable */
    e->imm_memtables = calloc(opts->imm_cap, sizeof(struct memtable *));
    if (!e->imm_memtables)
        goto DESTROY_MT;
    e->imm_count = 0;

    /* sstable */
    e->sst_count = 0;
    e->sst_cap = 16;
    e->ssts = calloc(e->sst_cap, sizeof(struct sst));
    if (!e->ssts)
        goto DESTROY_IMM_MT;

    /* path */
    if (mkdir(path, 0755) < 0 && errno != EEXIST)
        goto DESTROY_SST;
    e->db_path = strdup(path);
    if (!e->db_path)
        goto DESTROY_SST;

    e->next_id = 1;
    memcpy(&e->opts, opts, sizeof(struct engine_options));

    if (e->opts.block_size == 0)
        e->opts.block_size = BLOCK_SIZE;
    return 0;

DESTROY_SST:
    free(e->ssts);
DESTROY_IMM_MT:
    free(e->imm_memtables);
DESTROY_MT:
    memtable_destroy(e->memtable);
    free(e->memtable);
    memset(e, 0, sizeof(struct engine));
    return -1;
}

void engine_close(struct engine *e)
{
    if (!e)
        return;

    /* memtable */
    memtable_destroy(e->memtable);
    free(e->memtable);
    for (uint32_t i = 0; i < e->imm_count; i++) {
        memtable_destroy(e->imm_memtables[i]);
        free(e->imm_memtables[i]);
    }
    free(e->imm_memtables);

    /* sstable */
    for (uint32_t i = 0; i < e->sst_count; i++)
        sst_close(&e->ssts[i]);
    free(e->ssts);

    /* path */
    free(e->db_path);
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

int engine_get(struct engine *e,
               const uint8_t *key,
               size_t key_len,
               uint8_t *value,
               size_t value_cap,
               size_t *value_len)
{
    if (!e || !key || key_len == 0)
        return -1;

    /* search in memtable */
    struct memtable_entry *entry = memtable_get(e->memtable, key, key_len);
    if (entry)
        goto FOUND_IN_MT;
    for (int i = (int) e->imm_count - 1; i >= 0; i--) {
        entry = memtable_get(e->imm_memtables[i], key, key_len);
        if (entry)
            goto FOUND_IN_MT;
    }

    /* search in sstable */
    struct sst_iter si;
    for (int i = (int) e->sst_count - 1; i >= 0; i--) {
        if (sst_iter_seek_key(&si, &e->ssts[i], key, key_len) < 0)
            continue;
        if (!sst_iter_is_valid(&si)) {
            sst_iter_destroy(&si);
            continue;
        }
        if (key_cmp(key, key_len, sst_iter_key(&si), sst_iter_key_len(&si)) ==
            0) {
            goto FOUND_IN_SST;
        }
        sst_iter_destroy(&si);
    }

    return -1;

FOUND_IN_MT:
    *value_len = entry->value_len;
    if (*value_len > 0) {
        if (*value_len > value_cap)
            return -1;
        memcpy(value, entry->value, *value_len);
    }
    return 0;
FOUND_IN_SST:
    *value_len = sst_iter_value_len(&si);
    if (*value_len > 0) {
        if (*value_len > value_cap) {
            sst_iter_destroy(&si);
            return -1;
        }
        memcpy(value, sst_iter_value(&si), *value_len);
    }
    sst_iter_destroy(&si);
    return 0;
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

    uint32_t count = 1 + e->imm_count + e->sst_count;

    void *buf = malloc((1 + e->imm_count) * sizeof(struct mt_iter) +
                       e->sst_count * sizeof(struct sst_iter) +
                       count * sizeof(struct iter));
    if (!buf)
        return -1;

    struct iter *iters = buf;
    struct mt_iter *mis =
        (struct mt_iter *) ((char *) buf + count * sizeof(struct iter));
    struct sst_iter *sis =
        (struct sst_iter *) ((char *) mis +
                             (1 + e->imm_count) * sizeof(struct mt_iter));

    /* mutable memtable */
    if (lower)
        mt_iter_seek_key(&mis[0], e->memtable, lower, (uint16_t) lower_len);
    else
        mt_iter_seek_first(&mis[0], e->memtable);
    mt_iter_to_iter(&mis[0], &iters[0]);

    /* immutable memtables */
    for (uint32_t i = 0; i < e->imm_count; i++) {
        struct memtable *mt = e->imm_memtables[e->imm_count - 1 - i];
        if (lower)
            mt_iter_seek_key(&mis[i + 1], mt, lower, (uint16_t) lower_len);
        else
            mt_iter_seek_first(&mis[i + 1], mt);
        mt_iter_to_iter(&mis[i + 1], &iters[i + 1]);
    }

    /* sstables */
    for (uint32_t i = 0; i < e->sst_count; i++) {
        struct sst *sst = &e->ssts[e->sst_count - 1 - i];
        if (lower)
            sst_iter_seek_key(&sis[i], sst, lower, (uint16_t) lower_len);
        else
            sst_iter_seek_first(&sis[i], sst);
        sst_iter_to_iter(&sis[i], &iters[i + 1 + e->imm_count]);
    }

    /* init the fields in lsm_iter */
    if (lsm_iter_init(iter, iters, count, upper, (uint16_t) upper_len) < 0) {
        free(buf);
        return -1;
    }

    /* store the buffer into lsm_iter */
    iter->iter_buffer = buf;
    iter->sis = sis;
    iter->sst_count = e->sst_count;

    return 0;
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

int engine_flush(struct engine *e)
{
    if (!e || e->imm_count == 0)
        return -1;

    /* extract the oldest immutable memtable -> sst */
    struct memtable *old_mt = e->imm_memtables[0];
    if (old_mt->size == 0)
        return -1;

    /* get sst file path */
    char path[256];
    snprintf(path, 256, "%s/%llu.sst", e->db_path, e->next_id);

    /* write memtable to sst_builder */
    struct sst_builder sb;
    if (sst_builder_init(&sb, e->opts.block_size, path) < 0)
        return -1;

    struct mt_iter mi;
    if (mt_iter_seek_first(&mi, old_mt) < 0) {
        sst_builder_destroy(&sb);
        return -1;
    }

    while (mt_iter_is_valid(&mi)) {
        if (sst_builder_add(&sb, mt_iter_key(&mi), mt_iter_key_len(&mi),
                            mt_iter_value(&mi), mt_iter_value_len(&mi)) < 0) {
            sst_builder_destroy(&sb);
            return -1;
        }
        mt_iter_next(&mi);
    }

    /* sst_builder -> sst */
    if (e->sst_count >= e->sst_cap) {
        uint32_t new_cap = e->sst_cap * 2;
        struct sst *tmp = realloc(e->ssts, new_cap * sizeof(struct sst));
        if (!tmp) {
            sst_builder_destroy(&sb);
            return -1;
        }
        e->ssts = tmp;
        e->sst_cap = new_cap;
    }
    if (sst_builder_build(&sb, path, e->next_id, &e->ssts[e->sst_count]) < 0) {
        sst_builder_destroy(&sb);
        return -1;
    }

    e->sst_count++;
    e->next_id++;

    /* remove from imm_memtable */
    for (uint32_t i = 1; i < e->imm_count; i++)
        e->imm_memtables[i - 1] = e->imm_memtables[i];
    e->imm_count--;

    memtable_destroy(old_mt);
    free(old_mt);

    return 0;
}