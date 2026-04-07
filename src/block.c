#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "block.h"

int block_builder_init(struct block_builder *bb, size_t target_size)
{
    if (!bb)
        return -1;
    bb->data = malloc(sizeof(uint8_t) * target_size);
    if (!bb->data)
        return -1;
    bb->offsets = malloc(sizeof(uint16_t) * target_size / 2);
    if (!bb->offsets) {
        free(bb->data);
        return -1;
    }
    bb->data_len = 0;
    bb->n = 0;
    bb->target_size = target_size;
    return 0;
}

void block_builder_destroy(struct block_builder *bb)
{
    if (!bb)
        return;
    free(bb->data);
    free(bb->offsets);
    bb->data = NULL;
    bb->offsets = NULL;
}

int block_builder_add(struct block_builder *bb,
                      const uint8_t *key,
                      uint16_t key_len,
                      const uint8_t *value,
                      uint16_t value_len)
{
    if (!bb || !key || key_len == 0)
        return -1;

    size_t entry_size = 2 + key_len + 2 + value_len;
    size_t new_size = bb->data_len + entry_size +
                      (bb->n + 1) * sizeof(*bb->offsets) + sizeof(bb->n);
    if (new_size > bb->target_size && !block_builder_is_empty(bb))
        return 1;

    /* record offset */
    bb->offsets[bb->n] = (uint16_t) bb->data_len;

    /* write entry */
    uint8_t *p = &bb->data[bb->data_len];
    memcpy(p, &key_len, 2);
    p += 2;
    memcpy(p, key, key_len);
    p += key_len;
    memcpy(p, &value_len, 2);
    p += 2;
    if (value && value_len > 0)
        memcpy(p, value, value_len);

    bb->data_len += entry_size;
    bb->n++;
    return 0;
}

int block_builder_build(struct block_builder *bb, struct block *blk)
{
    if (!bb || !blk || block_builder_is_empty(bb))
        return -1;
    blk->data = bb->data;
    blk->data_len = bb->data_len;
    blk->offsets = bb->offsets;
    blk->n = bb->n;
    memset(bb, 0, sizeof(*bb));
    return 0;
}

uint8_t *block_encode(const struct block *blk, size_t *out_len)
{
    if (!blk || !out_len)
        return NULL;

    size_t size = blk->data_len * sizeof(*blk->data) +
                  blk->n * sizeof(*blk->offsets) + sizeof(blk->n);
    uint8_t *raw = malloc(size);
    if (!raw)
        return NULL;

    uint8_t *p = raw;
    memcpy(p, blk->data, blk->data_len);
    p += blk->data_len;
    memcpy(p, blk->offsets, blk->n * sizeof(*blk->offsets));
    p += blk->n * sizeof(*blk->offsets);
    memcpy(p, &blk->n, sizeof(blk->n));

    *out_len = size;
    return raw;
}

int block_decode(struct block *blk, const uint8_t *raw, size_t len)
{
    if (!blk || !raw || len == 0)
        return -1;
    const uint8_t *p = raw + len;
    memcpy(&blk->n, p - sizeof(blk->n), sizeof(blk->n));
    p -= sizeof(blk->n);

    size_t offsets_size = blk->n * sizeof(*blk->offsets);
    blk->offsets = malloc(offsets_size);
    if (!blk->offsets) {
        memset(blk, 0, sizeof(*blk));
        return -1;
    }
    memcpy(blk->offsets, p - offsets_size, offsets_size);

    size_t data_len = p - offsets_size - raw;
    blk->data = malloc(data_len);
    if (!blk->data) {
        free(blk->offsets);
        memset(blk, 0, sizeof(*blk));
        return -1;
    }
    memcpy(blk->data, raw, data_len * sizeof(*blk->data));
    blk->data_len = data_len;
    return 0;
}

void block_destroy(struct block *blk)
{
    if (!blk)
        return;
    free(blk->data);
    free(blk->offsets);
    memset(blk, 0, sizeof(*blk));
}