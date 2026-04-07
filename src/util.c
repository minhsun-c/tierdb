#include <stdlib.h>
#include <string.h>

#include "util.h"

int key_cmp(const uint8_t *k1, size_t k1_len, const uint8_t *k2, size_t k2_len)
{
    size_t min_len = k1_len < k2_len ? k1_len : k2_len;
    int ret = memcmp(k1, k2, min_len);
    if (ret != 0)
        return ret;
    if (k1_len < k2_len)
        return -1;
    if (k1_len > k2_len)
        return 1;
    return 0;
}