#include <stdlib.h>
#include <string.h>

#include "skiplist.h"

#define SKIPLIST_PROB_NUMERATOR 512
#define SKIPLIST_PROB_DENOMINATOR 1024

static uint32_t random_level(uint32_t max_level)
{
    uint32_t level = 1;
    while ((rand() & (SKIPLIST_PROB_DENOMINATOR - 1)) <
               SKIPLIST_PROB_NUMERATOR &&
           level < max_level) {
        level++;
    }
    return level;
}

int skiplist_init(struct skiplist *sl, uint32_t max_level)
{
    if (max_level == 0 || max_level > SKIPLIST_MAX_LEVEL)
        return -1;
    sl->forward = calloc(max_level, sizeof(struct skiplist *));
    if (sl->forward == NULL)
        return -1;
    sl->level = max_level;
    return 0;
}

void skiplist_free(struct skiplist *sl)
{
    free(sl->forward);
}

void skiplist_insert(struct skiplist *sentinel,
                     struct skiplist *node,
                     skiplist_cmp_fn cmp)
{
    struct skiplist *current = sentinel;
    uint32_t level = random_level(sentinel->level);

    for (int i = sentinel->level - 1; i >= 0; i--) {
        while (current->forward[i] != NULL &&
               cmp(current->forward[i], node) < 0) {
            current = current->forward[i];
        }
        if (i < (int) level) {
            node->forward[i] = current->forward[i];
            current->forward[i] = node;
        }
    }
}

int skiplist_remove(struct skiplist *sentinel,
                    struct skiplist *node,
                    skiplist_cmp_fn cmp)
{
    int found = -1;
    struct skiplist *current = sentinel;

    for (int i = sentinel->level - 1; i >= 0; i--) {
        while (current->forward[i] != NULL &&
               cmp(current->forward[i], node) < 0) {
            current = current->forward[i];
        }
        if (current->forward[i] != NULL &&
            cmp(current->forward[i], node) == 0) {
            current->forward[i] = node->forward[i];
            node->forward[i] = NULL;
            found = 0;
        }
    }
    return found;
}