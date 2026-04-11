#pragma once

#include <stddef.h>
#include <stdint.h>

#include "block.h"
#include "block_iter.h"
#include "sst.h"

/**
 * struct sst_iter - iterator over an entire SST file
 *
 * Provides sequential and seek-based access to all key-value entries
 * across all blocks in an SST. Internally maintains a block_iter over
 * the currently loaded block. When the block_iter is exhausted, the
 * next block is automatically loaded from disk.
 *
 * The iterator does not own the SST — the SST must remain valid and
 * open for the entire lifetime of the iterator.
 *
 * @sst:       pointer to the SST being iterated; not owned
 * @blk:       currently loaded block; heap-allocated, owned by iterator
 * @bi:        block iterator over @blk
 * @block_idx: index of the currently loaded block within the SST;
 *             equal to sst->n_blocks when exhausted
 */
struct sst_iter {
    const struct sst *sst;
    struct block *blk;
    struct block_iter bi;
    uint32_t block_idx;
};

/**
 * sst_iter_seek_first - position the iterator at the first entry
 *
 * Loads the first block from the SST and seeks to its first entry.
 * If the SST has no blocks, the iterator is left invalid.
 *
 * @param iter: iterator to initialize
 * @param sst:  SST to iterate over; must remain valid and open
 * @return:     0 on success, -1 on failure
 */
int sst_iter_seek_first(struct sst_iter *iter, const struct sst *sst);

/**
 * sst_iter_seek_key - position the iterator at the first key >= target
 *
 * Uses the SST block metadata to binary search for the block that may
 * contain the target key, then seeks within that block. If no entry
 * with key >= target exists in the SST, the iterator is left invalid.
 *
 * @param iter:       iterator to initialize
 * @param sst:        SST to iterate over; must remain valid and open
 * @param target:     target key bytes to seek to
 * @param target_len: length of target key in bytes
 * @return:           0 on success, -1 on failure
 */
int sst_iter_seek_key(struct sst_iter *iter,
                      const struct sst *sst,
                      const uint8_t *target,
                      uint16_t target_len);

/**
 * sst_iter_is_valid - check whether the iterator points to a valid entry
 *
 * @param iter: iterator to check
 * @return:     1 if the iterator points to a valid entry, 0 otherwise
 */
static inline int sst_iter_is_valid(const struct sst_iter *iter)
{
    if (!iter || !iter->sst)
        return 0;
    return block_iter_is_valid(&iter->bi);
}

/**
 * sst_iter_key - return the key of the current entry
 *
 * Behaviour is undefined if sst_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current key bytes
 */
static inline const uint8_t *sst_iter_key(const struct sst_iter *iter)
{
    return block_iter_key(&iter->bi);
}

/**
 * sst_iter_key_len - return the length of the current key
 *
 * Behaviour is undefined if sst_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current key length in bytes
 */
static inline uint16_t sst_iter_key_len(const struct sst_iter *iter)
{
    return block_iter_key_len(&iter->bi);
}

/**
 * sst_iter_value - return the value of the current entry
 *
 * Behaviour is undefined if sst_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     pointer to the current value bytes
 */
static inline const uint8_t *sst_iter_value(const struct sst_iter *iter)
{
    return block_iter_value(&iter->bi);
}

/**
 * sst_iter_value_len - return the length of the current value
 *
 * A length of 0 indicates a tombstone (deleted key).
 * Behaviour is undefined if sst_iter_is_valid() returns 0.
 *
 * @param iter: iterator
 * @return:     current value length in bytes
 */
static inline uint16_t sst_iter_value_len(const struct sst_iter *iter)
{
    return block_iter_value_len(&iter->bi);
}

/**
 * sst_iter_next - advance the iterator to the next entry
 *
 * Advances the underlying block iterator. If the current block is
 * exhausted, automatically loads the next block from the SST and
 * seeks to its first entry. If no more blocks remain, the iterator
 * becomes invalid.
 *
 * Behaviour is undefined if sst_iter_is_valid() returns 0.
 *
 * @param iter: iterator to advance
 * @return:     0 on success, -1 on failure
 */
int sst_iter_next(struct sst_iter *iter);

/**
 * sst_iter_destroy - release resources held by the iterator
 *
 * Frees the currently loaded block and destroys the block iterator.
 * Does not close the SST.
 *
 * @param iter: iterator to destroy
 */
void sst_iter_destroy(struct sst_iter *iter);