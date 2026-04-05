#include "block_iter.h"
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "block.h"

/* load entry at iter->idx into iter's key/value fields */
static int load_entry(struct block_iter *iter)
{
    uint16_t off = iter->blk->offsets[iter->idx];
    const uint8_t *p = iter->blk->data + off;

    uint16_t new_key_len;
    memcpy(&new_key_len, p, sizeof(new_key_len));
    p += sizeof(new_key_len);

    uint8_t *new_key = malloc(new_key_len);
    if (!new_key)
        return -1;
    memcpy(new_key, p, new_key_len);
    p += new_key_len;

    free(iter->key);
    iter->key = new_key;
    iter->key_len = new_key_len;

    memcpy(&iter->value_len, p, sizeof(iter->value_len));
    p += sizeof(iter->value_len);
    iter->value_off = (uint16_t) (p - iter->blk->data);

    return 0;
}

static int key_cmp(const uint8_t *k1,
                   uint16_t k1_len,
                   const uint8_t *k2,
                   uint16_t k2_len)
{
    uint16_t min_len = k1_len < k2_len ? k1_len : k2_len;
    int ret = memcmp(k1, k2, min_len);
    if (ret != 0)
        return ret;
    if (k1_len < k2_len)
        return -1;
    if (k1_len > k2_len)
        return 1;
    return 0;
}

int block_iter_seek_first(struct block_iter *iter, const struct block *blk)
{
    if (!iter || !blk || blk->n == 0)
        return -1;

    iter->blk = blk;
    iter->idx = 0;
    iter->key = NULL;

    if (load_entry(iter) < 0) {
        memset(iter, 0, sizeof(struct block_iter));
        return -1;
    }
    return 0;
}

int block_iter_seek_key(struct block_iter *iter,
                        const struct block *blk,
                        const uint8_t *target,
                        uint16_t target_len)
{
    if (!iter || !blk || !target || target_len == 0)
        return -1;
    if (block_iter_seek_first(iter, blk) < 0)
        return -1;

    while (block_iter_is_valid(iter) &&
           key_cmp(target, target_len, iter->key, iter->key_len) > 0) {
        block_iter_next(iter);
    }
    return 0;
}

int block_iter_next(struct block_iter *iter)
{
    if (!iter)
        return -1;

    iter->idx++;
    if (!block_iter_is_valid(iter)) {
        free(iter->key);
        iter->key = NULL;
        return -1;
    }
    return load_entry(iter);
}

void block_iter_destroy(struct block_iter *iter)
{
    if (!iter)
        return;
    free(iter->key);
    memset(iter, 0, sizeof(struct block_iter));
}