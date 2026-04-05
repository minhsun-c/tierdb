#include <stdlib.h>
#include <string.h>
#include "block.h"
#include "checker.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const uint8_t *k(const char *s)
{
    return (const uint8_t *) s;
}
static const uint8_t *v(const char *s)
{
    return (const uint8_t *) s;
}

/*
 * ========================================
 * helpers
 * ========================================
 */

/* read key at offset i from block */
static void block_entry_at(const struct block *blk,
                           uint16_t i,
                           const uint8_t **key,
                           uint16_t *key_len,
                           const uint8_t **value,
                           uint16_t *value_len)
{
    uint16_t off = blk->offsets[i];
    const uint8_t *p = blk->data + off;
    memcpy(key_len, p, 2);
    p += 2;
    *key = p;
    p += *key_len;
    memcpy(value_len, p, 2);
    p += 2;
    *value = p;
}

/*
 * ========================================
 * tests
 * ========================================
 */
static void test_builder_init(void)
{
    printf(COLOR_BLUE "\n--- Builder Init ---\n" COLOR_RESET);
    struct block_builder bb;
    int ret = block_builder_init(&bb, BLOCK_SIZE);

    EXPECT_EQ(ret, 0, "init returns 0");
    EXPECT_COND(block_builder_is_empty(&bb), "empty after init");
    EXPECT_EQ(bb.data_len, 0, "data_len is 0");
    EXPECT_EQ(bb.n, 0, "n is 0");

    block_builder_destroy(&bb);
}

static void test_builder_add_single(void)
{
    printf(COLOR_BLUE "\n--- Builder Add Single ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);

    int ret = block_builder_add(&bb, k("apple"), 5, v("1"), 1);
    EXPECT_EQ(ret, 0, "add returns 0");
    EXPECT_COND(!block_builder_is_empty(&bb), "not empty after add");
    EXPECT_EQ(bb.n, 1, "n is 1");
    EXPECT_EQ(bb.offsets[0], 0, "first offset is 0");

    block_builder_destroy(&bb);
}

static void test_builder_add_multiple(void)
{
    printf(COLOR_BLUE "\n--- Builder Add Multiple ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);

    block_builder_add(&bb, k("apple"), 5, v("1"), 1);
    block_builder_add(&bb, k("banana"), 6, v("2"), 1);
    block_builder_add(&bb, k("cherry"), 6, v("3"), 1);

    EXPECT_EQ(bb.n, 3, "n is 3");
    EXPECT_EQ(bb.offsets[0], 0, "offset[0] is 0");
    /* apple: 2+5+2+1 = 10 bytes */
    EXPECT_EQ(bb.offsets[1], 10, "offset[1] is 10");
    /* banana: 2+6+2+1 = 11 bytes */
    EXPECT_EQ(bb.offsets[2], 21, "offset[2] is 21");

    block_builder_destroy(&bb);
}

static void test_builder_full(void)
{
    printf(COLOR_BLUE "\n--- Builder Full ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, 20); /* very small block */

    /* first entry always accepted */
    int ret = block_builder_add(&bb, k("apple"), 5, v("1"), 1);
    EXPECT_EQ(ret, 0, "first entry accepted");

    /* second entry should be rejected (block full) */
    ret = block_builder_add(&bb, k("banana"), 6, v("2"), 1);
    EXPECT_EQ(ret, 1, "second entry rejected (full)");

    block_builder_destroy(&bb);
}

static void test_builder_invalid(void)
{
    printf(COLOR_BLUE "\n--- Builder Invalid Args ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);

    EXPECT_EQ(block_builder_add(&bb, NULL, 5, v("1"), 1), -1,
              "NULL key returns -1");
    EXPECT_EQ(block_builder_add(&bb, k("a"), 0, v("1"), 1), -1,
              "zero key_len returns -1");

    block_builder_destroy(&bb);
}

static void test_build(void)
{
    printf(COLOR_BLUE "\n--- Build ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);

    block_builder_add(&bb, k("apple"), 5, v("1"), 1);
    block_builder_add(&bb, k("banana"), 6, v("2"), 1);

    struct block blk;
    int ret = block_builder_build(&bb, &blk);
    EXPECT_EQ(ret, 0, "build returns 0");
    EXPECT_EQ(blk.n, 2, "block has 2 entries");
    EXPECT_COND(bb.data == NULL, "builder data is NULL after build");

    block_destroy(&blk);
}

static void test_encode_decode(void)
{
    printf(COLOR_BLUE "\n--- Encode and Decode ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);

    block_builder_add(&bb, k("apple"), 5, v("1"), 1);
    block_builder_add(&bb, k("banana"), 6, v("2"), 1);
    block_builder_add(&bb, k("cherry"), 6, v("3"), 1);

    struct block blk;
    block_builder_build(&bb, &blk);

    /* encode */
    size_t raw_len;
    uint8_t *raw = block_encode(&blk, &raw_len);
    EXPECT_COND(raw != NULL, "encode returns non-NULL");
    EXPECT_COND(raw_len > 0, "encoded length > 0");

    /* decode */
    struct block blk2;
    int ret = block_decode(&blk2, raw, raw_len);
    EXPECT_EQ(ret, 0, "decode returns 0");
    EXPECT_EQ(blk2.n, 3, "decoded block has 3 entries");
    EXPECT_EQ(blk2.data_len, blk.data_len, "data_len matches");

    /* verify entries */
    const uint8_t *key, *val;
    uint16_t key_len, val_len;

    block_entry_at(&blk2, 0, &key, &key_len, &val, &val_len);
    EXPECT_EQ(key_len, 5, "entry[0] key_len");
    EXPECT_COND(memcmp(key, "apple", 5) == 0, "entry[0] key is apple");

    block_entry_at(&blk2, 1, &key, &key_len, &val, &val_len);
    EXPECT_EQ(key_len, 6, "entry[1] key_len");
    EXPECT_COND(memcmp(key, "banana", 6) == 0, "entry[1] key is banana");

    block_entry_at(&blk2, 2, &key, &key_len, &val, &val_len);
    EXPECT_EQ(key_len, 6, "entry[2] key_len");
    EXPECT_COND(memcmp(key, "cherry", 6) == 0, "entry[2] key is cherry");

    free(raw);
    block_destroy(&blk);
    block_destroy(&blk2);
}

static void test_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Tombstone (empty value) ---\n" COLOR_RESET);
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);

    block_builder_add(&bb, k("apple"), 5, NULL, 0);

    struct block blk;
    block_builder_build(&bb, &blk);

    size_t raw_len;
    uint8_t *raw = block_encode(&blk, &raw_len);

    struct block blk2;
    block_decode(&blk2, raw, raw_len);

    const uint8_t *key, *val;
    uint16_t key_len, val_len;
    block_entry_at(&blk2, 0, &key, &key_len, &val, &val_len);

    EXPECT_EQ(key_len, 5, "key_len is 5");
    EXPECT_EQ(val_len, 0, "value_len is 0 (tombstone)");

    free(raw);
    block_destroy(&blk);
    block_destroy(&blk2);
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    test_builder_init();
    test_builder_add_single();
    test_builder_add_multiple();
    test_builder_full();
    test_builder_invalid();
    test_build();
    test_encode_decode();
    test_tombstone();

    TEST_SUMMARY();
}