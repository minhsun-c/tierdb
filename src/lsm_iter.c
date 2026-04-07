#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "lsm_iter.h"
#include "memtable.h"
#include "util.h"

static void find_current(struct lsm_iter *iter)
{
    iter->current_cursor = -1;
    for (uint32_t i = 0; i < iter->count; i++) {
        if (!iter->cursors[i])
            continue;

        /* check upper bound */
        if (iter->upper &&
            key_cmp(iter->cursors[i]->key, iter->cursors[i]->key_len,
                    iter->upper, iter->upper_len) > 0) {
            iter->cursors[i] = NULL;
            continue;
        }

        if (iter->current_cursor == -1 ||
            key_cmp(iter->cursors[i]->key, iter->cursors[i]->key_len,
                    iter->cursors[iter->current_cursor]->key,
                    iter->cursors[iter->current_cursor]->key_len) < 0) {
            iter->current_cursor = (int) i;
        }
    }
}


int lsm_iter_init(struct lsm_iter *iter,
                  struct memtable **sources,
                  uint32_t count,
                  const uint8_t *lower,
                  size_t lower_len,
                  const uint8_t *upper,
                  size_t upper_len)
{
    if (!iter || !sources)
        return -1;
    iter->cursors = malloc(sizeof(struct memtable_entry *) * count);
    if (!iter->cursors)
        return -1;
    iter->count = count;
    iter->upper = upper;
    iter->upper_len = upper_len;

    for (uint32_t i = 0; i < count; i++) {
        struct memtable_entry *entry = memtable_iter_first(sources[i]);
        if (lower) {
            while (entry &&
                   key_cmp(entry->key, entry->key_len, lower, lower_len) < 0) {
                entry = memtable_iter_next(entry);
            }
        }
        while (entry && entry->value_len == 0) {
            entry = memtable_iter_next(entry);
        }
        iter->cursors[i] = entry;
    }
    find_current(iter);
    return 0;
}

void lsm_iter_destroy(struct lsm_iter *iter)
{
    if (!iter)
        return;
    free(iter->cursors);
    iter->cursors = NULL;
}


void lsm_iter_next(struct lsm_iter *iter)
{
    if (!iter || iter->current_cursor == -1)
        return;

    struct memtable_entry *prev = iter->cursors[iter->current_cursor];

    for (uint32_t i = 0; i < iter->count; i++) {
        while (iter->cursors[i] &&
               key_cmp(iter->cursors[i]->key, iter->cursors[i]->key_len,
                       prev->key, prev->key_len) <= 0) {
            iter->cursors[i] = memtable_iter_next(iter->cursors[i]);
        }
        while (iter->cursors[i] && iter->cursors[i]->value_len == 0) {
            iter->cursors[i] = memtable_iter_next(iter->cursors[i]);
        }
    }

    find_current(iter);
}