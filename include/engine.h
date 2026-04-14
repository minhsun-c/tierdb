#pragma once

#include <stddef.h>
#include <stdint.h>
#include "lsm_iter.h"

struct memtable;

/**
 * struct engine_options - configuration passed to engine_open
 *
 * @threshold:   soft size limit in bytes; when a memtable's approx_size
 *               exceeds this, it is frozen and a new one is created
 * @imm_cap:     maximum number of immutable memtables; when full,
 *               the oldest one is flushed to disk before freezing
 * @max_level:   maximum skiplist level for all memtables
 * @block_size:  target block size in bytes for SST files; defaults to
 *               BLOCK_SIZE (4096) if set to 0
 */
struct engine_options {
    size_t threshold;
    uint32_t imm_cap;
    uint32_t max_level;
    size_t block_size;
};

/**
 * struct engine - top-level LSM storage engine
 *
 * @memtable:      current mutable memtable; all writes land here first
 * @imm_memtables: array of frozen immutable memtables, ordered oldest first;
 *                 imm_memtables[imm_count-1] is the most recently frozen
 * @imm_count:     number of immutable memtables currently held
 * @ssts:          array of L0 SSTs, ordered newest first
 * @sst_count:     number of SSTs currently held
 * @sst_cap:       allocated capacity of the ssts array
 * @next_id:       monotonically increasing id assigned to each new
 *                 memtable and SST
 * @db_path:       directory path for SST files on disk
 * @opts:          engine configuration
 */
struct engine {
    /* memtable */
    struct memtable *memtable;
    struct memtable **imm_memtables;
    uint32_t imm_count;

    /* sstable */
    struct sst *ssts;
    uint32_t sst_count;
    uint32_t sst_cap;

    uint64_t next_id;
    char *db_path;
    struct engine_options opts;
};

/**
 * engine_open - initialize and open the engine
 *
 * Creates the database directory if it does not exist, allocates
 * the first mutable memtable with id 0, and initializes the L0
 * SST array.
 *
 * @param e:    pointer to an uninitialized engine struct
 * @param opts: configuration options
 * @param path: directory path for SST files on disk
 * @return:     0 on success, -1 on allocation failure
 */
int engine_open(struct engine *e,
                struct engine_options *opts,
                const char *path);

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
 * engine_scan - create a sorted iterator over a key range
 *
 * Builds mt_iter for the mutable and all immutable memtables,
 * wraps them in a unified iter array, and initializes an lsm_iter
 * with tombstone skipping and upper bound enforcement. All backing
 * memory is allocated in a single block and freed by lsm_iter_destroy().
 *
 * @e:         target engine
 * @lower:     inclusive lower bound key; NULL means unbounded
 * @lower_len: length of lower bound key in bytes
 * @upper:     inclusive upper bound key; NULL means unbounded
 * @upper_len: length of upper bound key in bytes
 * @iter:      iterator to initialize
 * @return:    0 on success, -1 on allocation failure
 */
int engine_scan(struct engine *e,
                const uint8_t *lower,
                size_t lower_len,
                const uint8_t *upper,
                size_t upper_len,
                struct lsm_iter *iter);

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

/**
 * engine_flush - flush the oldest immutable memtable to an L0 SST
 *
 * Takes the oldest immutable memtable (imm_memtables[0]), writes
 * all its entries to a new SST file on disk, adds the SST to the
 * L0 array, and removes the memtable from the immutable list.
 *
 * @e:       target engine
 * @return:  0 on success, -1 on failure or no immutable memtable to flush
 */
int engine_flush(struct engine *e);