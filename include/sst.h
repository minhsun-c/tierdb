#pragma once

#include <stddef.h>
#include <stdint.h>
#include "block.h"

/**
 * struct sst - an immutable on-disk sorted string table
 *
 * On-disk layout:
 *
 *   ┌──────────────────────────────────────────────────────┐ ← 0
 *   │  Block #0 (block_size bytes)                         │
 *   │  Block #1 (block_size bytes)                         │
 *   │  ...                                                 │
 *   │  Block #N (block_size bytes)                         │
 *   ├──────────────────────────────────────────────────────┤ ← meta_offset
 *   │  [ offset(4B) | fk_len(2B) | fk | lk_len(2B) | lk ]  │
 *   │  × n_blocks                                          │
 *   ├──────────────────────────────────────────────────────┤ ← file_size - 4
 *   │  meta_offset (4B)                                    │
 *   └──────────────────────────────────────────────────────┘ ← file_size
 *
 * Once built and written to disk, an SST is never modified. All block
 * data is loaded on demand via sst_read_block(). The block metadata
 * (metas) is always kept in memory for binary search during seek.
 *
 * @fd:        file descriptor opened for reading
 * @metas:     array of block metadata, one per data block
 * @n_blocks:  number of data blocks
 * @id:        monotonically increasing identifier assigned by the engine
 * @file_size: total size of the SST file in bytes
 * @meta_offset: byte offset of the meta section within the SST file;
 *               also marks the end of the last data block
 */
struct sst {
    int fd;
    struct block_meta *metas;
    uint32_t n_blocks;
    uint64_t id;
    size_t file_size;
    uint32_t meta_offset;
};

/**
 * sst_open - open an existing SST file and load its metadata
 *
 * Reads the meta section from disk and populates the sst struct.
 * Block data is not loaded until sst_read_block() is called.
 *
 * @param sst:  SST struct to populate
 * @param path: path to the SST file on disk
 * @param id:   unique identifier to assign to this SST
 * @return:     0 on success, -1 on failure
 */
int sst_open(struct sst *sst, const char *path, uint64_t id);

/**
 * sst_close - close an SST and free all resources
 *
 * Closes the file descriptor and frees all block metadata.
 * Does not free the sst struct itself — the caller owns it.
 *
 * @param sst: SST to close
 */
void sst_close(struct sst *sst);

/**
 * sst_read_block - read and decode a single block from disk
 *
 * Allocates and returns a decoded block. The caller must call
 * block_destroy() and free() on the returned block when done.
 *
 * @param sst:       source SST
 * @param block_idx: index of the block to read (0-based)
 * @return:          heap-allocated decoded block, or NULL on failure
 */
struct block *sst_read_block(const struct sst *sst, uint32_t block_idx);
