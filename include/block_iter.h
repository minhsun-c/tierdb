#pragma once

#include <stddef.h>
#include <stdint.h>
#include "block.h"

/**
 * struct block_iter - iterator over a single decoded block
 *
 * Provides sequential and seek-based access to the key-value entries
 * stored in a block. The key is copied into the iterator on each
 * seek or advance so that it remains valid independently of the block
 * layout. The value is not copied; instead, only its offset and length
 * within blk->data are stored.
 *
 * The iterator does not own the block — the block must remain valid
 * for the entire lifetime of the iterator.
 *
 * @blk:     pointer to the block being iterated; not owned by the iterator
 * @idx:     index of the current entry within blk->offsets[]; equal to
 *           blk->n when the iterator is exhausted
 * @key:     heap-allocated copy of the current entry's key; NULL when invalid
 * @key_len: length of the current key in bytes
 * @value_off: byte offset of the current value within blk->data
 * @value_len: length of the current value in bytes; 0 means tombstone
 */
struct block_iter {
    const struct block *blk;
    uint16_t idx;
    uint8_t *key;
    uint16_t key_len;
    uint16_t value_off;
    uint16_t value_len;
};

/**
 * block_iter_seek_first - position the iterator at the first entry
 *
 * Initializes the iterator and seeks to the first key-value pair in
 * the block. If the block is empty, the iterator is left invalid.
 *
 * @param iter: iterator to initialize
 * @param blk:  block to iterate over; must remain valid for the
 *              lifetime of the iterator
 * @return:     0 on success, -1 on allocation failure or empty block
 */
int block_iter_seek_first(struct block_iter *iter, const struct block *blk);

/**
 * block_iter_seek_key - position the iterator at the first key >= target
 *
 * Seeks to the first entry whose key is greater than or equal to the
 * given target key. If no such entry exists, the iterator is left invalid.
 * Uses linear scan from the beginning of the block.
 *
 * @param iter:       iterator to initialize
 * @param blk:        block to iterate over; must remain valid for the
 *                    lifetime of the iterator
 * @param target:     target key bytes to seek to
 * @param target_len: length of target key in bytes
 * @return:           0 on success, -1 on allocation failure
 */
int block_iter_seek_key(struct block_iter *iter,
                        const struct block *blk,
                        const uint8_t *target,
                        uint16_t target_len);

/**
 * block_iter_is_valid - check whether the iterator points to a valid entry
 *
 * @param iter: iterator to check
 * @return:     1 if the iterator points to a valid entry, 0 otherwise
 */
static inline int block_iter_is_valid(const struct block_iter *iter)
{
    if (!iter || !iter->blk)
        return 0;
    return iter->idx < iter->blk->n;
}

/**
 * block_iter_key - return the key of the current entry
 *
 * The returned pointer is valid until the next call to block_iter_next(),
 * block_iter_seek_first(), block_iter_seek_key(), or block_iter_destroy().
 * Behaviour is undefined if block_iter_is_valid() returns 0.
 *
 * @param iter:   iterator
 * @return:       pointer to the current key bytes
 */
static inline const uint8_t *block_iter_key(const struct block_iter *iter)
{
    return iter->key;
}

/**
 * block_iter_key_len - return the length of the current key
 *
 * Behaviour is undefined if block_iter_is_valid() returns 0.
 *
 * @param iter:   iterator
 * @return:       current key length in bytes
 */
static inline uint16_t block_iter_key_len(const struct block_iter *iter)
{
    return iter->key_len;
}

/**
 * block_iter_value - return the value of the current entry
 *
 * The returned pointer points directly into blk->data and is valid as
 * long as the block itself is valid. Behaviour is undefined if
 * block_iter_is_valid() returns 0.
 *
 * @param iter:   iterator
 * @return:       pointer to the current value bytes
 */
static inline const uint8_t *block_iter_value(const struct block_iter *iter)
{
    return iter->blk->data + iter->value_off;
}

/**
 * block_iter_value_len - return the length of the current value
 *
 * A length of 0 indicates a tombstone (deleted key).
 * Behaviour is undefined if block_iter_is_valid() returns 0.
 *
 * @param iter:   iterator
 * @return:       current value length in bytes
 */
static inline uint16_t block_iter_value_len(const struct block_iter *iter)
{
    return iter->value_len;
}

/**
 * block_iter_next - advance the iterator to the next entry
 *
 * Moves the cursor forward by one entry and updates the cached key.
 * If the end of the block is reached, the iterator becomes invalid.
 * Behaviour is undefined if block_iter_is_valid() returns 0.
 *
 * @param iter: iterator to advance
 * @return:     0 on success, -1 on allocation failure
 */
int block_iter_next(struct block_iter *iter);

/**
 * block_iter_destroy - release resources held by the iterator
 *
 * Frees the copied key buffer. Does not free the block.
 *
 * @param iter: iterator to destroy
 */
void block_iter_destroy(struct block_iter *iter);