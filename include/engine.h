#pragma once

#include <stddef.h>
#include <stdint.h>

struct memtable;

/**
 * struct engine_options - configuration passed to engine_open
 *
 * @threshold:  soft size limit in bytes; when a memtable's approx_size
 *              exceeds this, it is frozen and a new one is created
 * @imm_cap:    maximum number of immutable memtables; when full,
 *              the oldest one is flushed to disk before freezing
 * @max_level:  maximum skiplist level for all memtables
 */
struct engine_options {
    size_t threshold;
    uint32_t imm_cap;
    uint32_t max_level;
};

/**
 * struct engine - top-level LSM storage engine
 *
 * @memtable:      current mutable memtable; all writes land here first
 * @imm_memtables: array of frozen immutable memtables, ordered oldest first;
 *                 imm_memtables[imm_count-1] is the most recently frozen
 * @imm_count:     number of immutable memtables currently held
 * @next_id:       monotonically increasing id assigned to each new memtable
 * @opts:          engine configuration
 */
struct engine {
    struct memtable *memtable;
    struct memtable **imm_memtables;
    uint32_t imm_count;
    uint64_t next_id;
    struct engine_options opts;
};

/**
 * engine_open - initialize and open the engine
 *
 * Allocates the first mutable memtable with id 0.
 *
 * @e:    pointer to an uninitialized engine struct
 * @opts: configuration options
 * @return: 0 on success, -1 on allocation failure
 */
int engine_open(struct engine *e, struct engine_options *opts);

/**
 * engine_close - destroy the engine and free all resources
 *
 * Destroys all memtables (mutable and immutable).
 * Does not free the engine struct itself — the caller owns it.
 *
 * @e: engine to close
 */
void engine_close(struct engine *e);

/**
 * engine_put - insert or overwrite a key-value pair
 *
 * Writes to the current mutable memtable. If the memtable's approx_size
 * exceeds opts.threshold after the write, it is frozen automatically.
 *
 * @e:         target engine
 * @key:       key bytes
 * @key_len:   length of key
 * @value:     value bytes
 * @value_len: length of value
 * @return:    0 on success, -1 on failure
 */
int engine_put(struct engine *e,
               const uint8_t *key,
               size_t key_len,
               const uint8_t *value,
               size_t value_len);

/**
 * engine_get - look up a key
 *
 * Searches the mutable memtable first, then immutable memtables
 * from newest to oldest. Returns the first match.
 * Caller must check entry->value_len == 0 for tombstone.
 *
 * @e:       target engine
 * @key:     key bytes
 * @key_len: length of key
 * @return:  pointer to the entry, or NULL if not found
 */
struct memtable_entry *engine_get(struct engine *e,
                                  const uint8_t *key,
                                  size_t key_len);

/**
 * engine_delete - delete a key by inserting a tombstone
 *
 * Equivalent to engine_put(e, key, key_len, NULL, 0).
 *
 * @e:       target engine
 * @key:     key bytes
 * @key_len: length of key
 * @return:  0 on success, -1 on failure
 */
int engine_delete(struct engine *e, const uint8_t *key, size_t key_len);

/**
 * engine_freeze_memtable - freeze the current mutable memtable
 *
 * Appends the current memtable to imm_memtables and creates
 * a new mutable memtable with the next available id.
 * imm_memtables[imm_count-1] is always the most recently frozen.
 *
 * @e:      target engine
 * @return: 0 on success, -1 on allocation failure or imm array full
 */
int engine_freeze_memtable(struct engine *e);