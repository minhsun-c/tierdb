#pragma once

#include <stddef.h>
#include <stdint.h>

#include "iter.h"
#include "memtable.h"

/**
 * struct mt_iter - iterator over a single memtable
 *
 * Provides sequential and seek-based access to the key-value entries
 * in a memtable. Wraps the memtable's intrusive linked list traversal
 * behind the same interface as sst_iter.
 *
 * The iterator does not own the memtable — the memtable must remain
 * valid for the entire lifetime of the iterator.
 *
 * @mt:    pointer to the memtable being iterated; not owned
 * @entry: pointer to the current entry; NULL when exhausted
 */
struct mt_iter {
    const struct memtable *mt;
    struct memtable_entry *entry;
};

/**
 * mt_iter_seek_first - position the iterator at the first entry
 *
 * @param iter: iterator to initialize
 * @param mt:   memtable to iterate over; must remain valid
 * @return:     0 on success, -1 on failure
 */
int mt_iter_seek_first(struct mt_iter *iter, const struct memtable *mt);

/**
 * mt_iter_seek_key - position the iterator at the first key >= target
 *
 * @param iter:       iterator to initialize
 * @param mt:         memtable to iterate over; must remain valid
 * @param target:     target key bytes to seek to
 * @param target_len: length of target key in bytes
 * @return:           0 on success, -1 on failure
 */
int mt_iter_seek_key(struct mt_iter *iter,
                     const struct memtable *mt,
                     const uint8_t *target,
                     uint16_t target_len);

/**
 * mt_iter_is_valid - check whether the iterator points to a valid entry
 *
 * @param iter: iterator to check
 * @return:     1 if valid, 0 otherwise
 */
static inline int mt_iter_is_valid(const struct mt_iter *iter)
{
    if (!iter || !iter->mt)
        return 0;
    return iter->entry != NULL;
}

/**
 * mt_iter_key - return the key of the current entry
 *
 * Behaviour is undefined if mt_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current key bytes
 */
static inline const uint8_t *mt_iter_key(const struct mt_iter *iter)
{
    return iter->entry->key;
}

/**
 * mt_iter_key_len - return the length of the current key
 *
 * Behaviour is undefined if mt_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current key length in bytes
 */
static inline uint16_t mt_iter_key_len(const struct mt_iter *iter)
{
    return (uint16_t) iter->entry->key_len;
}

/**
 * mt_iter_value - return the value of the current entry
 *
 * Behaviour is undefined if mt_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current value bytes
 */
static inline const uint8_t *mt_iter_value(const struct mt_iter *iter)
{
    return iter->entry->value;
}

/**
 * mt_iter_value_len - return the length of the current value
 *
 * A length of 0 indicates a tombstone (deleted key).
 * Behaviour is undefined if mt_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current value length in bytes
 */
static inline uint16_t mt_iter_value_len(const struct mt_iter *iter)
{
    return (uint16_t) iter->entry->value_len;
}

/**
 * mt_iter_next - advance the iterator to the next entry
 *
 * Behaviour is undefined if mt_iter_is_valid() returns 0.
 *
 * @param iter: iterator to advance
 * @return:     0 on success, -1 on failure
 */
int mt_iter_next(struct mt_iter *iter);

/**
 * mt_iter_destroy - release resources held by the iterator
 *
 * Does not free the memtable or its entries.
 *
 * @param iter: iterator to destroy
 */
void mt_iter_destroy(struct mt_iter *iter);

/**
 * mt_iter_to_iter - wrap a memtable iterator in the unified interface
 *
 * The mt_iter must already be seeked to its starting position.
 * The mt_iter must remain valid for the lifetime of the iter.
 *
 * @param mi: source memtable iterator
 * @param it: output unified iterator to populate
 */
void mt_iter_to_iter(struct mt_iter *mi, struct iter *it);