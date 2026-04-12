#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "block.h"
#include "block_iter.h"
#include "sst.h"
#include "sst_iter.h"
#include "util.h"

int sst_iter_seek_first(struct sst_iter *iter, const struct sst *sst)
{
    if (!iter || !sst)
        return -1;

    iter->blk = sst_read_block(sst, 0);
    if (!iter->blk)
        return -1;

    int ret = block_iter_seek_first(&iter->bi, iter->blk);
    if (ret < 0) {
        block_destroy(iter->blk);
        free(iter->blk);
        iter->blk = NULL;
        return -1;
    }

    iter->sst = sst;
    iter->block_idx = 0;
    return 0;
}

int sst_iter_seek_key(struct sst_iter *iter,
                      const struct sst *sst,
                      const uint8_t *target,
                      uint16_t target_len)
{
    if (!iter || !sst || !target || target_len == 0)
        return -1;

    uint32_t i;
    const uint8_t *last_key;
    uint16_t last_key_len;
    for (i = 0; i < sst->n_blocks; i++) {
        last_key = sst->metas[i].last_key;
        last_key_len = sst->metas[i].last_key_len;
        if (key_cmp(last_key, last_key_len, target, target_len) >= 0)
            break;
    }

    /* target at block <i> */
    if (i >= sst->n_blocks)
        return -1;

    iter->blk = sst_read_block(sst, i);
    if (!iter->blk)
        return -1;

    if (block_iter_seek_key(&iter->bi, iter->blk, target, target_len) < 0) {
        block_destroy(iter->blk);
        free(iter->blk);
        iter->blk = NULL;
        return -1;
    }

    iter->sst = sst;
    iter->block_idx = i;
    return 0;
}

int sst_iter_next(struct sst_iter *iter)
{
    if (!iter)
        return -1;

    /* get the next block in the same sst */
    block_iter_next(&iter->bi);

    if (block_iter_is_valid(&iter->bi))
        return 0;

    /* the end of the sst */
    if (iter->block_idx + 1 >= iter->sst->n_blocks)
        return 0;

    /* switch to the next block */
    struct block *blk = sst_read_block(iter->sst, iter->block_idx + 1);
    if (!blk)
        return -1;
    block_destroy(iter->blk);
    free(iter->blk);
    iter->blk = blk;

    /* reset block iterator */
    block_iter_destroy(&iter->bi);
    int ret = block_iter_seek_first(&iter->bi, iter->blk);
    if (ret < 0) {
        block_destroy(iter->blk);
        free(iter->blk);
        iter->blk = NULL;
        return -1;
    }

    iter->block_idx++;
    return 0;
}

void sst_iter_destroy(struct sst_iter *iter)
{
    if (!iter)
        return;
    if (iter->blk) {
        block_destroy(iter->blk);
        free(iter->blk);
    }
    block_iter_destroy(&iter->bi);
    memset(iter, 0, sizeof(struct sst_iter));
}

void sst_iter_to_iter(struct sst_iter *si, struct iter *it)
{
    if (!si || !it)
        return;

    it->ctx = si;
    it->valid = (int (*)(void *)) sst_iter_is_valid;
    it->key = (const uint8_t *(*) (void *) ) sst_iter_key;
    it->key_len = (uint16_t (*)(void *)) sst_iter_key_len;
    it->value = (const uint8_t *(*) (void *) ) sst_iter_value;
    it->value_len = (uint16_t (*)(void *)) sst_iter_value_len;
    it->next = (int (*)(void *)) sst_iter_next;
}