#pragma once

#include <stddef.h>
#include <stdint.h>

#define SKIPLIST_MAX_LEVEL 16

/**
 * struct skiplist - intrusive link node, carries no data
 *
 * Embed this struct inside your own data structure and use
 * container_of to recover the enclosing struct from a node pointer.
 * Use as a sentinel (root) or as a node — same struct, same philosophy
 * as Linux list_head.
 *
 * When used as a sentinel, level holds the maximum level for the entire
 * skiplist and governs both traversal depth and random_level() upper bound.
 *
 * When used as a regular node, level holds the number of levels this
 * node participates in, i.e. the valid range of forward[].
 *
 * @forward: dynamically allocated array of pointers to the next node at
 *           each level; length equals level
 * @level:   number of valid entries in forward[]
 */
struct skiplist {
    struct skiplist **forward;
    uint32_t level;
};

/**
 * skiplist_cmp_fn - comparison function type
 *
 * The skiplist has no knowledge of the stored data. All comparison
 * logic is delegated to the caller, who recovers the key via container_of.
 *
 * @a:      left node
 * @b:      right node
 * @return: negative if a < b, zero if a == b, positive if a > b
 */
typedef int (*skiplist_cmp_fn)(struct skiplist *a, struct skiplist *b);

/**
 * skiplist_next - get the next node at level 0
 *
 * @node:   current node
 * @return: next node, or NULL if this is the last node
 */
static inline struct skiplist *skiplist_next(struct skiplist *node)
{
    return node->forward[0];
}

/**
 * skiplist_next_at - get the next node at a specific level
 *
 * @node:      current node
 * @level:     the level to follow (0 = bottom, max_level-1 = top)
 * @return:    next node at the given level, or NULL if out of range
 */
static inline struct skiplist *skiplist_next_at(struct skiplist *node,
                                                uint32_t level)
{
    if (level >= node->level)
        return NULL;
    return node->forward[level];
}

/**
 * skiplist_init - initialize a sentinel node
 *
 * Allocates the forward array of size max_level and zeroes all pointers.
 * max_level must be between 1 and SKIPLIST_MAX_LEVEL inclusive.
 * The sentinel itself is owned by the caller.
 *
 * @sl:        sentinel node to initialize
 * @max_level: maximum number of levels for this skiplist
 * @return:    0 on success, -1 on allocation failure or invalid max_level
 */
int skiplist_init(struct skiplist *sl, uint32_t max_level);

/**
 * skiplist_free - free the forward array of a node
 *
 * Frees only the forward array, not the node itself.
 * The node struct is owned by the caller (typically embedded in an outer
 * struct).
 *
 * @sl: node whose forward array should be freed
 */
void skiplist_free(struct skiplist *sl);

/**
 * skiplist_insert - insert a node into the skiplist
 *
 * Assigns a random level to the node (between 1 and max_level) and links
 * it into all levels up to that value. Traversal starts from max_level - 1
 * to skip empty upper levels efficiently.
 *
 * @sentinel:  root sentinel node
 * @node:      node to insert, embedded in the caller's data structure;
 *             forward array must already be allocated for max_level
 * @cmp:       comparison function
 */
void skiplist_insert(struct skiplist *sentinel,
                     struct skiplist *node,
                     skiplist_cmp_fn cmp);

/**
 * skiplist_remove - remove a node from the skiplist
 *
 * Unlinks the node from all levels it participates in.
 * Does not free the node or its forward array — the caller is responsible.
 *
 * @sentinel:  root sentinel node
 * @node:      node to remove
 * @cmp:       comparison function
 * @return:    0 on success, -1 if not found
 */
int skiplist_remove(struct skiplist *sentinel,
                    struct skiplist *node,
                    skiplist_cmp_fn cmp);

/**
 * skiplist_for_each_safe - iterate over all nodes at level 0, safe for deletion
 *
 * Stores the next pointer before the loop body executes, allowing the
 * caller to free or unlink the current node safely.
 *
 * @cur:      loop cursor (struct skiplist *)
 * @next:     temporary storage for next pointer (struct skiplist *)
 * @sentinel: root sentinel node
 */
#define skiplist_for_each_safe(cur, next, sentinel) \
    for ((cur) = skiplist_next(sentinel);           \
         (cur) != NULL && ((next) = skiplist_next(cur), 1); (cur) = (next))

/**
 * skiplist_for_each - iterate over all nodes at level 0
 *
 * @cur:      loop cursor (struct skiplist *)
 * @sentinel: root sentinel node
 */
#define skiplist_for_each(cur, sentinel)                 \
    for ((cur) = skiplist_next(sentinel); (cur) != NULL; \
         (cur) = skiplist_next(cur))