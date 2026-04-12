#pragma once

#include <stddef.h>
#include <stdint.h>
#include "iter.h"

/**
 * struct merge_iter - k-way merge iterator over multiple sorted sources
 *
 * Merges an ordered array of child iterators (struct iter) into a single
 * sorted stream. On each advance, the child holding the globally smallest
 * key is selected. When multiple children hold the same key, the one with
 * the lowest index wins (newest data) and the duplicates are skipped.
 *
 * Tombstones are NOT skipped — the caller decides how to handle them.
 * Upper/lower bounds are NOT enforced — each child iterator should be
 * seeked to the correct position before being passed in.
 *
 * The merge_iter does not own the child iterators — the caller must
 * destroy them after destroying the merge_iter.
 *
 * @iters:   array of child iterators; not owned
 * @count:   number of child iterators
 * @current: index into iters[] pointing to the child with the globally
 *           smallest key; -1 when the iterator is exhausted
 */
struct merge_iter {
    struct iter *iters;
    uint32_t count;
    int current;
};

/**
 * merge_iter_init - initialize a merge iterator
 *
 * Sets up the merge iterator over the given child iterators and finds
 * the initial smallest key. Each child iterator must already be seeked
 * to its starting position.
 *
 * @param iter:  merge iterator to initialize
 * @param iters: array of child iterators, ordered newest first (index 0
 *               is the newest source); must remain valid for the lifetime
 *               of the merge iterator
 * @param count: number of child iterators
 * @return:      0 on success, -1 on failure
 */
int merge_iter_init(struct merge_iter *iter,
                    struct iter *iters,
                    uint32_t count);

/**
 * merge_iter_is_valid - check whether the iterator points to a valid entry
 *
 * @param iter: iterator to check
 * @return:     1 if valid, 0 otherwise
 */
static inline int merge_iter_is_valid(const struct merge_iter *iter)
{
    if (!iter)
        return 0;
    return iter->current != -1;
}

/**
 * merge_iter_key - return the key of the current entry
 *
 * Behaviour is undefined if merge_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current key bytes
 */
static inline const uint8_t *merge_iter_key(const struct merge_iter *iter)
{
    return iter_key(&iter->iters[iter->current]);
}

/**
 * merge_iter_key_len - return the length of the current key
 *
 * Behaviour is undefined if merge_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current key length in bytes
 */
static inline uint16_t merge_iter_key_len(const struct merge_iter *iter)
{
    return iter_key_len(&iter->iters[iter->current]);
}

/**
 * merge_iter_value - return the value of the current entry
 *
 * Behaviour is undefined if merge_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current value bytes
 */
static inline const uint8_t *merge_iter_value(const struct merge_iter *iter)
{
    return iter_value(&iter->iters[iter->current]);
}

/**
 * merge_iter_value_len - return the length of the current value
 *
 * A length of 0 indicates a tombstone (deleted key).
 * Behaviour is undefined if merge_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current value length in bytes
 */
static inline uint16_t merge_iter_value_len(const struct merge_iter *iter)
{
    return iter_value_len(&iter->iters[iter->current]);
}

/**
 * merge_iter_next - advance to the next unique key
 *
 * Advances all children that hold the current smallest key, then
 * finds the next globally smallest key. When multiple children hold
 * the same key, the one with the lowest index wins (newest source).
 *
 * Behaviour is undefined if merge_iter_is_valid() returns 0.
 *
 * @param iter: iterator to advance
 * @return:     0 on success, -1 on failure
 */
int merge_iter_next(struct merge_iter *iter);

/**
 * merge_iter_destroy - release resources held by the merge iterator
 *
 * Does not destroy the child iterators — the caller owns them.
 *
 * @param iter: iterator to destroy
 */
void merge_iter_destroy(struct merge_iter *iter);