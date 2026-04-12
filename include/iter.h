#pragma once

#include <stddef.h>
#include <stdint.h>

/**
 * struct iter - unified iterator interface
 *
 * A generic cursor over sorted key-value entries. Each concrete
 * iterator (block_iter, sst_iter, merge_iter, …) wraps its own
 * state in @ctx and provides callbacks through the function
 * pointers below.
 *
 * The interface does not own @ctx — the caller is responsible for
 * creating and destroying the concrete iterator.
 *
 * @ctx:       opaque pointer to the concrete iterator state
 * @valid:     return 1 if the iterator points to a valid entry
 * @key:       return pointer to the current key bytes
 * @key_len:   return length of the current key in bytes
 * @value:     return pointer to the current value bytes
 * @value_len: return length of the current value in bytes;
 *             0 indicates a tombstone
 * @next:      advance to the next entry; return 0 on success, -1 on failure
 */
struct iter {
    void *ctx;
    int (*valid)(void *ctx);
    const uint8_t *(*key)(void *ctx);
    uint16_t (*key_len)(void *ctx);
    const uint8_t *(*value)(void *ctx);
    uint16_t (*value_len)(void *ctx);
    int (*next)(void *ctx);
};

static inline int iter_is_valid(const struct iter *it)
{
    return it && it->ctx && it->valid(it->ctx);
}

static inline const uint8_t *iter_key(const struct iter *it)
{
    return it->key(it->ctx);
}

static inline uint16_t iter_key_len(const struct iter *it)
{
    return it->key_len(it->ctx);
}

static inline const uint8_t *iter_value(const struct iter *it)
{
    return it->value(it->ctx);
}

static inline uint16_t iter_value_len(const struct iter *it)
{
    return it->value_len(it->ctx);
}

static inline int iter_next(const struct iter *it)
{
    return it->next(it->ctx);
}