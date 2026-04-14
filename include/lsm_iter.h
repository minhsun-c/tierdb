#pragma once

#include <stddef.h>
#include <stdint.h>
#include "iter.h"
#include "merge_iter.h"

/**
 * struct lsm_iter - filtered iterator over the entire LSM tree
 *
 * Wraps a merge_iter and adds two filters on top:
 *   - tombstone skipping (value_len == 0 entries are hidden)
 *   - upper bound enforcement (entries beyond upper are hidden)
 *
 * The child iterators (struct iter array) and their backing storage
 * must remain valid for the lifetime of this iterator.
 *
 * @merge:        underlying merge iterator
 * @upper:        inclusive upper bound key; NULL means unbounded
 * @upper_len:    length of upper bound key in bytes
 * @exhausted:    1 if the iterator has exceeded the upper bound and
 *                the entire scan is finished; 0 otherwise
 * @iter_buffer:  heap-allocated backing memory for child iterators;
 *                NULL if caller manages lifetime; freed by
 *                lsm_iter_destroy()
 * @sis:          pointer to the sst_iter array within iter_buffer;
 *                used by lsm_iter_destroy to free block memory held
 *                by each sst_iter; NULL if no SSTs
 * @sst_count:    number of sst_iters pointed to by sis
 */
struct lsm_iter {
    struct merge_iter merge;
    const uint8_t *upper;
    uint16_t upper_len;
    int exhausted;
    void *iter_buffer;
    struct sst_iter *sis;
    uint32_t sst_count;
};

/**
 * lsm_iter_init - initialize the LSM iterator
 *
 * Initializes the underlying merge iterator over the given child
 * iterators and applies tombstone/upper-bound filtering. Each child
 * iterator must already be seeked to its starting position and
 * ordered newest first (index 0 = newest source).
 *
 * @param iter:      iterator to initialize
 * @param iters:     array of child iterators; must remain valid for
 *                   the lifetime of this iterator
 * @param count:     number of child iterators
 * @param upper:     inclusive upper bound key; NULL means unbounded
 * @param upper_len: length of upper bound key in bytes
 * @return:          0 on success, -1 on failure
 */
int lsm_iter_init(struct lsm_iter *iter,
                  struct iter *iters,
                  uint32_t count,
                  const uint8_t *upper,
                  uint16_t upper_len);

/**
 * lsm_iter_is_valid - check whether the iterator has a current entry
 *
 * @param iter: iterator to check
 * @return:     1 if valid, 0 otherwise
 */
static inline int lsm_iter_is_valid(const struct lsm_iter *iter)
{
    if (!iter || iter->exhausted)
        return 0;
    return merge_iter_is_valid(&iter->merge);
}

/**
 * lsm_iter_key - return the key of the current entry
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current key bytes
 */
static inline const uint8_t *lsm_iter_key(const struct lsm_iter *iter)
{
    return merge_iter_key(&iter->merge);
}

/**
 * lsm_iter_key_len - return the length of the current key
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current key length in bytes
 */
static inline uint16_t lsm_iter_key_len(const struct lsm_iter *iter)
{
    return merge_iter_key_len(&iter->merge);
}

/**
 * lsm_iter_value - return the value of the current entry
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current value bytes
 */
static inline const uint8_t *lsm_iter_value(const struct lsm_iter *iter)
{
    return merge_iter_value(&iter->merge);
}

/**
 * lsm_iter_value_len - return the length of the current value
 *
 * Behaviour is undefined if lsm_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current value length in bytes
 */
static inline uint16_t lsm_iter_value_len(const struct lsm_iter *iter)
{
    return merge_iter_value_len(&iter->merge);
}

/**
 * lsm_iter_next - advance to the next visible entry
 *
 * Skips tombstones and entries beyond the upper bound.
 *
 * @param iter: iterator to advance
 * @return:     0 on success, -1 on failure
 */
int lsm_iter_next(struct lsm_iter *iter);

/**
 * lsm_iter_destroy - release resources held by the iterator
 *
 * Destroys the underlying merge_iter. Does not destroy the child
 * iterators — the caller owns them.
 *
 * @param iter: iterator to destroy
 */
void lsm_iter_destroy(struct lsm_iter *iter);