#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "iter.h"
#include "merge_iter.h"
#include "util.h"

static void __merge_iter_find_current(struct merge_iter *iter)
{
    iter->current = -1;

    struct iter *it;
    for (uint32_t i = 0; i < iter->count; i++) {
        it = &iter->iters[i];
        if (!iter_is_valid(it))
            continue;

        if (iter->current == -1 ||
            key_cmp(iter_key(it), iter_key_len(it),
                    iter_key(&iter->iters[iter->current]),
                    iter_key_len(&iter->iters[iter->current])) < 0)
            iter->current = (int) i;
    }
}

static void __merge_iter_skip_dup(struct merge_iter *iter,
                                  uint8_t *prev_key,
                                  uint16_t prev_key_len)
{
    struct iter *it;
    for (uint32_t i = 0; i < iter->count; i++) {
        it = &iter->iters[i];
        while (iter_is_valid(it) && key_cmp(iter_key(it), iter_key_len(it),
                                            prev_key, prev_key_len) == 0)
            iter_next(it);
    }
}

int merge_iter_init(struct merge_iter *iter, struct iter *iters, uint32_t count)
{
    if (!iter || !iters || count == 0)
        return -1;

    iter->iters = iters;
    iter->count = count;

    /* set current to the smallest key */
    __merge_iter_find_current(iter);

    return 0;
}

int merge_iter_next(struct merge_iter *iter)
{
    if (!iter)
        return -1;

    uint16_t prev_key_len = iter_key_len(&iter->iters[iter->current]);
    uint8_t *prev_key = malloc(prev_key_len);
    if (!prev_key)
        return -1;
    memcpy(prev_key, iter_key(&iter->iters[iter->current]), prev_key_len);

    __merge_iter_skip_dup(iter, prev_key, prev_key_len);
    __merge_iter_find_current(iter);

    free(prev_key);
    return 0;
}

void merge_iter_destroy(struct merge_iter *iter)
{
    if (!iter)
        return;

    memset(iter, 0, sizeof(struct merge_iter));
}