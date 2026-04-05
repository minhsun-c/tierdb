#include <stdlib.h>
#include <string.h>
#include "block.h"
#include "block_iter.h"
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

/* build a block with given entries */
static void make_block(struct block *blk,
                       const char **keys,
                       const char **vals,
                       int n)
{
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);
    for (int i = 0; i < n; i++)
        block_builder_add(&bb, k(keys[i]), (uint16_t) strlen(keys[i]),
                          v(vals[i]), (uint16_t) strlen(vals[i]));
    block_builder_build(&bb, blk);
}

/* ─────────────────────────────────────────
 * tests
 * ───────────────────────────────────────── */
static void test_seek_first(void)
{
    printf(COLOR_BLUE "\n--- Seek First ---\n" COLOR_RESET);
    struct block blk;
    const char *keys[] = {"apple", "banana", "cherry"};
    const char *vals[] = {"1", "2", "3"};
    make_block(&blk, keys, vals, 3);

    struct block_iter iter;
    int ret = block_iter_seek_first(&iter, &blk);

    EXPECT_EQ(ret, 0, "seek_first returns 0");
    EXPECT_COND(block_iter_is_valid(&iter), "iter is valid");
    EXPECT_EQ(block_iter_key_len(&iter), 5, "key_len is 5");
    EXPECT_COND(memcmp(block_iter_key(&iter), "apple", 5) == 0, "key is apple");
    EXPECT_EQ(block_iter_value_len(&iter), 1, "value_len is 1");
    EXPECT_COND(memcmp(block_iter_value(&iter), "1", 1) == 0, "value is 1");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

static void test_next(void)
{
    printf(COLOR_BLUE "\n--- Next ---\n" COLOR_RESET);
    struct block blk;
    const char *keys[] = {"apple", "banana", "cherry"};
    const char *vals[] = {"1", "2", "3"};
    make_block(&blk, keys, vals, 3);

    struct block_iter iter;
    block_iter_seek_first(&iter, &blk);

    /* advance to banana */
    block_iter_next(&iter);
    EXPECT_COND(block_iter_is_valid(&iter), "valid at banana");
    EXPECT_COND(memcmp(block_iter_key(&iter), "banana", 6) == 0,
                "key is banana");
    EXPECT_COND(memcmp(block_iter_value(&iter), "2", 1) == 0, "value is 2");

    /* advance to cherry */
    block_iter_next(&iter);
    EXPECT_COND(block_iter_is_valid(&iter), "valid at cherry");
    EXPECT_COND(memcmp(block_iter_key(&iter), "cherry", 6) == 0,
                "key is cherry");

    /* advance past end */
    block_iter_next(&iter);
    EXPECT_COND(!block_iter_is_valid(&iter), "invalid after end");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

static void test_seek_key_exact(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Exact ---\n" COLOR_RESET);
    struct block blk;
    const char *keys[] = {"apple", "banana", "cherry"};
    const char *vals[] = {"1", "2", "3"};
    make_block(&blk, keys, vals, 3);

    struct block_iter iter;
    block_iter_seek_key(&iter, &blk, k("banana"), 6);

    EXPECT_COND(block_iter_is_valid(&iter), "valid at banana");
    EXPECT_COND(memcmp(block_iter_key(&iter), "banana", 6) == 0,
                "key is banana");
    EXPECT_COND(memcmp(block_iter_value(&iter), "2", 1) == 0, "value is 2");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

static void test_seek_key_nonexistent(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Non-existent ---\n" COLOR_RESET);
    struct block blk;
    const char *keys[] = {"apple", "cherry", "mango"};
    const char *vals[] = {"1", "3", "4"};
    make_block(&blk, keys, vals, 3);

    struct block_iter iter;
    /* seek "banana" — not in block, should land on "cherry" */
    block_iter_seek_key(&iter, &blk, k("banana"), 6);

    EXPECT_COND(block_iter_is_valid(&iter), "valid at cherry");
    EXPECT_COND(memcmp(block_iter_key(&iter), "cherry", 6) == 0,
                "landed on cherry (first key >= banana)");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

static void test_seek_key_past_end(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Past End ---\n" COLOR_RESET);
    struct block blk;
    const char *keys[] = {"apple", "banana"};
    const char *vals[] = {"1", "2"};
    make_block(&blk, keys, vals, 2);

    struct block_iter iter;
    /* seek "zzz" — past all keys */
    block_iter_seek_key(&iter, &blk, k("zzz"), 3);

    EXPECT_COND(!block_iter_is_valid(&iter), "invalid when key past end");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

static void test_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Tombstone ---\n" COLOR_RESET);
    struct block blk;
    struct block_builder bb;
    block_builder_init(&bb, BLOCK_SIZE);
    block_builder_add(&bb, k("apple"), 5, NULL, 0);
    block_builder_build(&bb, &blk);

    struct block_iter iter;
    block_iter_seek_first(&iter, &blk);

    EXPECT_COND(block_iter_is_valid(&iter), "valid at tombstone");
    EXPECT_EQ(block_iter_value_len(&iter), 0, "value_len is 0 (tombstone)");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

static void test_full_scan(void)
{
    printf(COLOR_BLUE "\n--- Full Scan ---\n" COLOR_RESET);
    struct block blk;
    const char *keys[] = {"apple", "banana", "cherry", "mango"};
    const char *vals[] = {"1", "2", "3", "4"};
    make_block(&blk, keys, vals, 4);

    struct block_iter iter;
    block_iter_seek_first(&iter, &blk);

    int i = 0;
    while (block_iter_is_valid(&iter)) {
        EXPECT_COND(
            memcmp(block_iter_key(&iter), keys[i], strlen(keys[i])) == 0,
            keys[i]);
        i++;
        block_iter_next(&iter);
    }
    EXPECT_EQ(i, 4, "scanned 4 entries");

    block_iter_destroy(&iter);
    block_destroy(&blk);
}

int main(void)
{
    test_seek_first();
    test_next();
    test_seek_key_exact();
    test_seek_key_nonexistent();
    test_seek_key_past_end();
    test_tombstone();
    test_full_scan();

    TEST_SUMMARY();
}