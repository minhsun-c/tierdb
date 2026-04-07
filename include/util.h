#pragma once

#include <stddef.h>
#include <stdlib.h>

/**
 * container_of - retrieve the enclosing struct from a member pointer
 * @ptr:    pointer to the member
 * @type:   type of the enclosing struct
 * @member: name of the member field within the struct
 */
#define container_of(ptr, type, member) \
    ((type *) ((char *) (ptr) - offsetof(type, member)))

/**
 * key_cmp - compare two raw keys lexicographically
 *
 * Compares byte-by-byte up to the length of the shorter key. If the
 * common prefix is equal, the shorter key is considered smaller.
 *
 * @param k1:     left key bytes
 * @param k1_len: length of left key in bytes
 * @param k2:     right key bytes
 * @param k2_len: length of right key in bytes
 * @return:       negative if k1 < k2, zero if k1 == k2, positive if k1 > k2
 */
int key_cmp(const uint8_t *k1, size_t k1_len, const uint8_t *k2, size_t k2_len);