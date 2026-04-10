#pragma once

#include <stddef.h>
#include <stdint.h>

#define BLOCK_SIZE 4096 /* default block size in bytes */

/**
 * struct block_meta - metadata describing one data block within an SST
 *
 * Stored in the meta section of the SST file and loaded into memory
 * when the SST is opened. Used as a lightweight index to locate which
 * block may contain a given key without reading block data from disk.
 *
 * @offset:        byte offset of the block within the SST file
 * @first_key:     heap-allocated copy of the first key in the block
 * @first_key_len: length of first_key in bytes
 * @last_key:      heap-allocated copy of the last key in the block
 * @last_key_len:  length of last_key in bytes
 */
struct block_meta {
    uint32_t offset;
    uint8_t *first_key;
    uint16_t first_key_len;
    uint8_t *last_key;
    uint16_t last_key_len;
};

/**
 * block_meta_destroy - free internal resources of a block_meta
 *
 * Frees first_key and last_key. Does not free the struct itself.
 *
 * @param meta: block_meta to destroy
 */
void block_meta_destroy(struct block_meta *meta);

/**
 * struct block - a decoded block ready for iteration
 *
 * On-disk layout (after encoding):
 *
 *   [ data section | offset section | num_of_elements (2B) ]
 *
 * data section:    raw key-value entries, each encoded as:
 *                  [ key_len(2B) | key | value_len(2B) | value ]
 * offset section:  one uint16_t offset per entry, relative to
 *                  the start of the data section
 * num_of_elements: total number of entries, stored as uint16_t
 *
 * @data:     raw bytes of the data section
 * @data_len: length of the data section in bytes
 * @offsets:  array of entry offsets into data[]
 * @n:        number of entries in this block
 */
struct block {
    uint8_t *data;
    size_t data_len;
    uint16_t *offsets;
    uint16_t n;
};

/**
 * struct block_builder - accumulates key-value entries before encoding
 *
 * @data:        data section buffer (grows as entries are added)
 * @data_len:    current length of data section in bytes
 * @offsets:     offset array (one entry per key-value pair)
 * @n:           number of entries added so far
 * @target_size: maximum encoded block size in bytes; block_builder_add()
 *               refuses new entries once this limit would be exceeded
 */
struct block_builder {
    uint8_t *data;
    size_t data_len;
    uint16_t *offsets;
    uint16_t n;
    size_t target_size;
};

/**
 * block_builder_init - initialize a block builder
 *
 * Allocates internal buffers. The builder must be destroyed with
 * block_builder_destroy() or consumed by block_builder_build().
 *
 * @param bb:          builder to initialize
 * @param target_size: maximum encoded block size in bytes
 * @return:            0 on success, -1 on allocation failure
 */
int block_builder_init(struct block_builder *bb, size_t target_size);

/**
 * block_builder_destroy - release a block builder's internal resources
 *
 * Should not be called after block_builder_build() has been called,
 * as ownership of the buffers is transferred to the block.
 *
 * @param bb: builder to destroy
 */
void block_builder_destroy(struct block_builder *bb);

/**
 * block_builder_add - add a key-value pair to the block
 *
 * Refuses to add the entry if doing so would cause the encoded block
 * size to exceed target_size, unless the block is currently empty
 * (the first entry is always accepted regardless of size).
 *
 * @param bb:        target builder
 * @param key:       key bytes
 * @param key_len:   length of key in bytes (max 65535)
 * @param value:     value bytes; NULL is treated as empty value (tombstone)
 * @param value_len: length of value in bytes (max 65535)
 * @return:          0 on success, 1 if the block is full, -1 on error
 */
int block_builder_add(struct block_builder *bb,
                      const uint8_t *key,
                      uint16_t key_len,
                      const uint8_t *value,
                      uint16_t value_len);

/**
 * block_builder_is_empty - check whether any entries have been added
 *
 * @param bb: target builder
 * @return:   1 if no entries have been added, 0 otherwise
 */
static inline int block_builder_is_empty(struct block_builder *bb)
{
    return bb->n == 0;
}

/**
 * block_builder_build - finalize the builder and produce a block
 *
 * Transfers ownership of the internal data and offsets buffers to blk.
 * The builder must not be used after this call.
 *
 * @param bb:  source builder
 * @param blk: output block to populate
 * @return:    0 on success, -1 if the builder is empty or on error
 */
int block_builder_build(struct block_builder *bb, struct block *blk);

/**
 * block_encode - serialize a block into raw bytes
 *
 * The returned buffer is heap-allocated and must be freed by the caller.
 * The encoding is:
 *   [ data section | offset section | num_of_elements (2B, little-endian) ]
 *
 * @param blk:     block to encode
 * @param out_len: set to the length of the returned buffer on success
 * @return:        heap-allocated encoded bytes, or NULL on failure
 */
uint8_t *block_encode(const struct block *blk, size_t *out_len);

/**
 * block_decode - deserialize raw bytes into a block
 *
 * Allocates internal buffers for the block. The block must be
 * destroyed with block_destroy() when no longer needed.
 *
 * @param blk: output block to populate
 * @param raw: raw encoded bytes produced by block_encode()
 * @param len: length of raw bytes
 * @return:    0 on success, -1 on malformed data or allocation failure
 */
int block_decode(struct block *blk, const uint8_t *raw, size_t len);

/**
 * block_destroy - free a block's internal buffers
 *
 * Does not free the block struct itself — the caller owns it.
 *
 * @param blk: block to destroy
 */
void block_destroy(struct block *blk);