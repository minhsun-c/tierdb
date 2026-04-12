#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "memtable.h"
#include "mt_iter.h"
#include "skiplist.h"
#include "util.h"

int mt_iter_seek_first(struct mt_iter *iter, const struct memtable *mt)
{
    if (!iter || !mt)
        return -1;

    iter->mt = mt;
    struct skiplist *link = skiplist_next(&mt->sentinel);
    iter->entry = link ? memtable_entry(link) : NULL;
    return 0;
}

int mt_iter_seek_key(struct mt_iter *iter,
                     const struct memtable *mt,
                     const uint8_t *target,
                     uint16_t target_len)
{
    if (!iter || !mt || !target || target_len == 0)
        return -1;

    struct skiplist *cur;
    skiplist_for_each (cur, &mt->sentinel) {
        uint8_t *key = memtable_entry(cur)->key;
        uint16_t key_len = memtable_entry(cur)->key_len;
        if (key_cmp(key, key_len, target, target_len) >= 0)
            break;
    }

    iter->mt = mt;
    iter->entry = cur ? memtable_entry(cur) : NULL;

    return 0;
}

int mt_iter_next(struct mt_iter *iter)
{
    if (!iter)
        return -1;

    struct skiplist *link = skiplist_next(&iter->entry->link);
    iter->entry = link ? memtable_entry(link) : NULL;
    return 0;
}

void mt_iter_destroy(struct mt_iter *iter)
{
    if (!iter)
        return;

    memset(iter, 0, sizeof(struct mt_iter));
}

void mt_iter_to_iter(struct mt_iter *mi, struct iter *it)
{
    if (!mi || !it)
        return;

    it->ctx = mi;
    it->valid = (int (*)(void *)) mt_iter_is_valid;
    it->key = (const uint8_t *(*) (void *) ) mt_iter_key;
    it->key_len = (uint16_t (*)(void *)) mt_iter_key_len;
    it->value = (const uint8_t *(*) (void *) ) mt_iter_value;
    it->value_len = (uint16_t (*)(void *)) mt_iter_value_len;
    it->next = (int (*)(void *)) mt_iter_next;
}
