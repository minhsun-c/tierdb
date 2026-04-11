#pragma once

#include <stddef.h>
#include <stdint.h>
#include "block.h"

/**
 * struct sst_builder - accumulates key-value pairs and writes an SST file
 *
 * Keys must be added in sorted order. The builder automatically splits
 * entries into blocks when the block size limit is reached. Each finalized
 * block is immediately written to disk via fd.
 *
 * @bb:            current block builder
 * @metas:         array of block metadata built so far
 * @n_blocks:      number of blocks finalized so far
 * @metas_cap:     allocated capacity of the metas array
 * @data_size:     total bytes written to the data section so far;
 *                 used as the offset for the next block's meta entry
 * @block_size:    target size per block in bytes
 * @first_key:     first key of the current (not yet finalized) block;
 *                 NULL if the current block is empty
 * @first_key_len: length of first_key in bytes
 * @last_key:      most recently added key; becomes last_key of the current
 *                 block when it is finalized; NULL if no key added yet
 * @last_key_len:  length of last_key in bytes
 * @fd:            file descriptor opened for writing; owned by the builder
 *                 until sst_builder_build() transfers it to the SST
 */
struct sst_builder {
    struct block_builder bb;
    struct block_meta *metas;
    uint32_t n_blocks;
    uint32_t metas_cap;
    size_t data_size;
    size_t block_size;
    uint8_t *first_key;
    uint16_t first_key_len;
    uint8_t *last_key;
    uint16_t last_key_len;
    int fd;
};

/**
 * sst_builder_init - initialize an SST builder
 *
 * @param sb:         builder to initialize
 * @param block_size: target block size in bytes (e.g. BLOCK_SIZE)
 * @param path:       path to the SST file on disk
 * @return:           0 on success, -1 on allocation failure
 */
int sst_builder_init(struct sst_builder *sb,
                     size_t block_size,
                     const char *path);

/**
 * sst_builder_destroy - release all resources held by the builder
 *
 * Should not be called after sst_builder_build() as ownership is
 * transferred to the SST file and the sst struct.
 *
 * @param sb: builder to destroy
 */
void sst_builder_destroy(struct sst_builder *sb);

/**
 * sst_builder_add - add a key-value pair to the SST
 *
 * Keys must be added in strictly ascending sorted order. Automatically
 * finalizes the current block and starts a new one when the block size
 * limit is reached.
 *
 * @param sb:        target builder
 * @param key:       key bytes
 * @param key_len:   length of key in bytes
 * @param value:     value bytes; NULL or value_len == 0 means tombstone
 * @param value_len: length of value in bytes
 * @return:          0 on success, -1 on failure
 */
int sst_builder_add(struct sst_builder *sb,
                    const uint8_t *key,
                    uint16_t key_len,
                    const uint8_t *value,
                    uint16_t value_len);

/**
 * sst_builder_estimated_size - estimate the current SST data size in bytes
 *
 * Returns only the data section size as an approximation, since data
 * blocks dominate the total SST size.
 *
 * @param sb: target builder
 * @return:   estimated size in bytes
 */
static inline size_t sst_builder_estimated_size(const struct sst_builder *sb)
{
    return sb->data_size;
}

/**
 * sst_builder_build - finalize and write the SST to disk
 *
 * Flushes the remaining block, encodes the meta section, writes the
 * complete SST file to path, and populates the sst struct. The builder
 * must not be used after this call.
 *
 * @param sb:   source builder
 * @param path: file path to write the SST file to
 * @param id:   unique identifier to assign to this SST
 * @param sst:  output SST struct to populate
 * @return:     0 on success, -1 on failure
 */
int sst_builder_build(struct sst_builder *sb,
                      const char *path,
                      uint64_t id,
                      struct sst *sst);