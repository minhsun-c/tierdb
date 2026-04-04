#pragma once

#include <stddef.h>
#include <stdint.h>
#include "skiplist.h"
#include "util.h"

/**
 * struct memtable_entry - a single key-value pair stored in the memtable
 *
 * Embedded in the skiplist via the link field. Use memtable_entry()
 * to recover this struct from a skiplist pointer.
 *
 * @key:        pointer to the key bytes, owned by this entry
 * @key_len:    length of the key in bytes
 * @value:      pointer to the value bytes, owned by this entry
 * @value_len:  length of the value in bytes; 0 means tombstone (deleted)
 * @link:       intrusive skiplist node
 */
struct memtable_entry {
    uint8_t *key;
    size_t key_len;
    uint8_t *value;
    size_t value_len;
    struct skiplist link;
};

/**
 * memtable_entry - recover a memtable_entry from a skiplist node pointer
 * @ptr: pointer to the embedded struct skiplist link field
 */
#define memtable_entry(ptr) container_of(ptr, struct memtable_entry, link)

/**
 * struct memtable - in-memory write buffer backed by a skiplist
 *
 * @sentinel:    skiplist sentinel (head) node; sentinel.level holds the
 *               maximum skiplist level for this memtable
 * @size:        number of entries currently stored
 * @approx_size: estimated total bytes of keys and values written
 * @id:          monotonically increasing identifier
 */
struct memtable {
    struct skiplist sentinel;
    uint32_t size;
    size_t approx_size;
    uint64_t id;
};

/**
 * memtable_init - initialize a memtable
 *
 * Initializes the skiplist sentinel with the given max_level and zeroes
 * all remaining fields. The memtable struct itself is owned by the caller.
 *
 * @mt:        memtable to initialize
 * @max_level: maximum skiplist level; must be between 1 and SKIPLIST_MAX_LEVEL
 * @id:        identifier assigned by the engine
 * @return:    0 on success, -1 if max_level is out of range or allocation fails
 */
int memtable_init(struct memtable *mt, uint32_t max_level, uint64_t id);

/**
 * memtable_destroy - destroy a memtable and free all entries
 *
 * Walks all entries and frees their key, value, and the entry itself.
 * Does not free the memtable struct — the caller owns it.
 *
 * @mt: memtable to destroy
 */
void memtable_destroy(struct memtable *mt);

/**
 * memtable_put - insert or overwrite a key-value pair
 *
 * Copies key and value into a newly allocated entry. If the key already
 * exists, the old entry's value is replaced in place. approx_size
 * is incremented by key_len + value_len on every call, even for overwrites.
 * A value_len of 0 represents a tombstone (logical deletion).
 *
 * @mt:        target memtable
 * @key:       key bytes, copied internally
 * @key_len:   length of key in bytes
 * @value:     value bytes, copied internally
 * @value_len: length of value in bytes; 0 = tombstone
 * @return:    0 on success, -1 on allocation failure
 */
int memtable_put(struct memtable *mt,
                 const uint8_t *key,
                 size_t key_len,
                 const uint8_t *value,
                 size_t value_len);

/**
 * memtable_get - look up a key
 *
 * Walks level 0 of the skiplist to find the first entry whose key matches.
 * Returns the entry regardless of whether it is a tombstone — the caller
 * must check value_len == 0 to detect deletion.
 *
 * @mt:      target memtable
 * @key:     key bytes to search for
 * @key_len: length of key in bytes
 * @return:  pointer to the matching entry, or NULL if not found
 */
struct memtable_entry *memtable_get(struct memtable *mt,
                                    const uint8_t *key,
                                    size_t key_len);

/**
 * memtable_is_full - check whether the memtable has exceeded its size limit
 *
 * Compares approx_size against the given threshold. When this returns
 * true, the engine should freeze this memtable and open a new one.
 *
 * @mt:        target memtable
 * @threshold: size limit in bytes
 * @return:    1 if full, 0 otherwise
 */
static inline int memtable_is_full(struct memtable *mt, size_t threshold)
{
    return mt->approx_size >= threshold;
}

/**
 * memtable_iter_first - get the first entry in sorted order
 *
 * @mt:     target memtable
 * @return: first entry, or NULL if the memtable is empty
 */
static inline struct memtable_entry *memtable_iter_first(struct memtable *mt)
{
    struct skiplist *link = skiplist_next(&mt->sentinel);
    if (!link)
        return NULL;
    return memtable_entry(link);
}

/**
 * memtable_iter_next - advance to the next entry
 *
 * @entry:  current entry
 * @return: next entry in sorted order, or NULL if this is the last one
 */
static inline struct memtable_entry *memtable_iter_next(
    struct memtable_entry *entry)
{
    struct skiplist *link = skiplist_next(&entry->link);
    if (!link)
        return NULL;
    return memtable_entry(link);
}