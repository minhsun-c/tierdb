#include "memtable.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "skiplist.h"
#include "util.h"

int memtable_init(struct memtable *mt, uint32_t max_level, uint64_t id)
{
    if (skiplist_init(&mt->sentinel, max_level) < 0)
        return -1;
    mt->size = 0;
    mt->approx_size = 0;
    mt->id = id;
    return 0;
}

void memtable_destroy(struct memtable *mt)
{
    struct skiplist *cur, *next;
    skiplist_for_each_safe (cur, next, &mt->sentinel) {
        struct memtable_entry *e = memtable_entry(cur);
        free(e->key);
        free(e->value);
        skiplist_free(&e->link);
        free(e);
    }
    skiplist_free(&mt->sentinel);
}

static int memtable_cmp(struct skiplist *a, struct skiplist *b)
{
    struct memtable_entry *ea = memtable_entry(a);
    struct memtable_entry *eb = memtable_entry(b);
    size_t min_len = ea->key_len < eb->key_len ? ea->key_len : eb->key_len;
    int ret = memcmp(ea->key, eb->key, min_len);
    if (ret != 0)
        return ret;
    if (ea->key_len < eb->key_len)
        return -1;
    if (ea->key_len > eb->key_len)
        return 1;
    return 0;
}

static struct memtable_entry *memtable_entry_init(const uint8_t *key,
                                                  size_t key_len,
                                                  const uint8_t *value,
                                                  size_t value_len)
{
    struct memtable_entry *entry = malloc(sizeof(struct memtable_entry));
    if (!entry)
        return NULL;
    entry->key = malloc(sizeof(uint8_t) * key_len);
    if (!entry->key) {
        free(entry);
        return NULL;
    }
    if (value_len > 0) {
        entry->value = malloc(sizeof(uint8_t) * value_len);
        if (!entry->value) {
            free(entry->key);
            free(entry);
            return NULL;
        }
        memcpy(entry->value, value, sizeof(uint8_t) * value_len);
    } else {
        entry->value = NULL;
    }
    memcpy(entry->key, key, sizeof(uint8_t) * key_len);
    entry->key_len = key_len;
    entry->value_len = value_len;
    return entry;
}

int memtable_put(struct memtable *mt,
                 const uint8_t *key,
                 size_t key_len,
                 const uint8_t *value,
                 size_t value_len)
{
    if (!key || key_len == 0)
        return -1;
    struct memtable_entry *entry = memtable_get(mt, key, key_len);
    if (entry) {
        if (value_len > 0) {
            uint8_t *new_val = malloc(sizeof(uint8_t) * value_len);
            if (!new_val)
                return -1;
            free(entry->value);
            entry->value = new_val;
            memcpy(entry->value, value, sizeof(uint8_t) * value_len);
        } else {
            free(entry->value);
            entry->value = NULL;
        }
        entry->value_len = value_len;
    } else {
        entry = memtable_entry_init(key, key_len, value, value_len);
        if (!entry)
            return -1;
        if (skiplist_init(&entry->link, mt->sentinel.level) < 0) {
            free(entry->key);
            free(entry->value);
            free(entry);
            return -1;
        }
        skiplist_insert(&mt->sentinel, &entry->link, memtable_cmp);
        mt->size++;
    }
    mt->approx_size += key_len + value_len;
    return 0;
}

struct memtable_entry *memtable_get(struct memtable *mt,
                                    const uint8_t *key,
                                    size_t key_len)
{
    struct memtable_entry tmp = {
        .key = (uint8_t *) key,
        .key_len = key_len,
    };

    struct skiplist *cur;
    skiplist_for_each (cur, &mt->sentinel) {
        int cmp = memtable_cmp(cur, &tmp.link);
        if (cmp == 0)
            return memtable_entry(cur);
        if (cmp > 0)
            return NULL;
    }
    return NULL;
}
