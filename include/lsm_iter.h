#pragma once

#include <stddef.h>
#include <stdint.h>
#include "memtable.h"

/**
 * struct lsm_iter - sorted iterator over multiple memtables
 *
 * Implements a k-way merge over an ordered set of memtable cursors.
 * On each call to lsm_iter_next(), the cursor holding the globally
 * smallest key is advanced. When two cursors hold the same key, the
 * one with the lower index (newest source) wins and the duplicate is
 * skipped. Tombstones (value_len == 0) are skipped automatically.
 *
 * The iterator is a snapshot: changes to the underlying memtables
 * after lsm_iter_init() may or may not be visible.
 *
 * @cursors:        one cursor per source memtable, NULL when exhausted
 * @current_cursor: index into cursors[] pointing to the entry with the
 *                  globally smallest key; -1 when the iterator is invalid
 * @count:          number of cursors (equals number of sources)
 * @upper:          inclusive upper bound key; NULL means unbounded
 * @upper_len:      length of upper bound key in bytes
 */
struct lsm_iter {
    struct memtable_entry **cursors;
    int current_cursor;
    uint32_t count;
    const uint8_t *upper;
    size_t upper_len;
};

/**
 * lsm_iter_init - initialize an lsm iterator
 *
 * Allocates the cursors array and positions each cursor at the first
 * entry >= lower in its source memtable. Sources must be ordered
 * newest first (index 0 = mutable memtable, index 1 = most recently
 * frozen immutable memtable, and so on).
 *
 * After init, lsm_iter_is_valid() must be checked before calling
 * lsm_iter_key(), lsm_iter_value(), or lsm_iter_next().
 *
 * @iter:      iterator to initialize
 * @sources:   memtable array ordered newest first
 * @count:     number of source memtables
 * @lower:     inclusive lower bound key; NULL means start from beginning
 * @lower_len: length of lower bound key in bytes
 * @upper:     inclusive upper bound key; NULL means no upper bound
 * @upper_len: length of upper bound key in bytes
 * @return:    0 on success, -1 on allocation failure
 */
int lsm_iter_init(struct lsm_iter *iter,
                  struct memtable **sources,
                  uint32_t count,
                  const uint8_t *lower,
                  size_t lower_len,
                  const uint8_t *upper,
                  size_t upper_len);

/**
 * lsm_iter_destroy - release resources held by the iterator
 *
 * Frees the internal cursors array. Does not free the source
 * memtables or their entries — the caller owns those.
 *
 * @iter: iterator to destroy
 */
void lsm_iter_destroy(struct lsm_iter *iter);

/**
 * lsm_iter_is_valid - check whether the iterator has a current entry
 *
 * Returns 0 when all cursors are exhausted or the remaining entries
 * exceed the upper bound. Must be checked before calling key, value,
 * or next.
 *
 * @iter:   iterator to check
 * @return: 1 if the iterator points to a valid entry, 0 otherwise
 */
static inline int lsm_iter_is_valid(struct lsm_iter *iter)
{
    if (!iter)
        return 0;
    return iter->current_cursor != -1;
}

/**
 * lsm_iter_key - return the key of the current entry
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @iter:   iterator
 * @return: pointer to the current key bytes; valid until lsm_iter_next()
 */
static inline const uint8_t *lsm_iter_key(struct lsm_iter *iter)
{
    return iter->cursors[iter->current_cursor]->key;
}

/**
 * lsm_iter_key_len - return the length of the current key
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @iter:   iterator
 * @return: current key length in bytes
 */
static inline size_t lsm_iter_key_len(struct lsm_iter *iter)
{
    return iter->cursors[iter->current_cursor]->key_len;
}

/**
 * lsm_iter_value - return the value of the current entry
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @iter:   iterator
 * @return: pointer to the current value bytes; valid until lsm_iter_next()
 */
static inline const uint8_t *lsm_iter_value(struct lsm_iter *iter)
{
    return iter->cursors[iter->current_cursor]->value;
}

/**
 * lsm_iter_value_len - return the length of the current value
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @iter:   iterator
 * @return: current value length in bytes
 */
static inline size_t lsm_iter_value_len(struct lsm_iter *iter)
{
    return iter->cursors[iter->current_cursor]->value_len;
}

/**
 * lsm_iter_next - advance to the next valid entry
 *
 * Advances the cursor holding the current smallest key, then skips
 * any duplicate keys (keeping the newest version) and any tombstones.
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @iter: iterator to advance
 */
void lsm_iter_next(struct lsm_iter *iter);