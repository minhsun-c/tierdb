#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "iter.h"
#include "lsm_iter.h"
#include "merge_iter.h"
#include "sst_iter.h"
#include "util.h"

static void __lsm_iter_skip_invalid(struct lsm_iter *iter)
{
    while (merge_iter_is_valid(&iter->merge)) {
        /* reach upper bound */
        if (iter->upper && key_cmp(merge_iter_key(&iter->merge),
                                   merge_iter_key_len(&iter->merge),
                                   iter->upper, iter->upper_len) > 0) {
            iter->exhausted = 1;
            return;
        }

        /* skip tombstone */
        if (merge_iter_value_len(&iter->merge) == 0) {
            merge_iter_next(&iter->merge);
            continue;
        }

        return;
    }
}

int lsm_iter_init(struct lsm_iter *iter,
                  struct iter *iters,
                  uint32_t count,
                  const uint8_t *upper,
                  uint16_t upper_len)
{
    if (!iter || !iters || count == 0)
        return -1;

    if (merge_iter_init(&iter->merge, iters, count) < 0)
        return -1;

    iter->upper = upper;
    iter->upper_len = upper_len;
    iter->exhausted = 0;
    iter->iter_buffer = NULL;

    __lsm_iter_skip_invalid(iter);
    return 0;
}

int lsm_iter_next(struct lsm_iter *iter)
{
    if (!iter)
        return -1;

    if (merge_iter_next(&iter->merge) < 0)
        return -1;

    __lsm_iter_skip_invalid(iter);
    return 0;
}

void lsm_iter_destroy(struct lsm_iter *iter)
{
    if (!iter)
        return;

    if (iter->sis)
        for (uint32_t i = 0; i < iter->sst_count; i++)
            sst_iter_destroy(&iter->sis[i]);

    merge_iter_destroy(&iter->merge);
    free(iter->iter_buffer);
    memset(iter, 0, sizeof(struct lsm_iter));
}