#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "block.h"
#include "checker.h"
#include "sst.h"
#include "sst_builder.h"
#include "sst_iter.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const char *SST_PATH = "/tmp/test_sst_iter.sst";

static const uint8_t *k(const char *s)
{
    return (const uint8_t *) s;
}
static const uint8_t *v(const char *s)
{
    return (const uint8_t *) s;
}

static void cleanup(void)
{
    unlink(SST_PATH);
}

/* helper: build an SST file on disk, then reopen it into *sst */
static void build_and_open(size_t block_size,
                           const char **keys,
                           const char **vals,
                           uint32_t n,
                           struct sst *sst,
                           uint64_t id)
{
    struct sst_builder sb;
    sst_builder_init(&sb, block_size, SST_PATH);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t kl = (uint16_t) strlen(keys[i]);
        uint16_t vl = vals[i] ? (uint16_t) strlen(vals[i]) : 0;
        sst_builder_add(&sb, k(keys[i]), kl, vals[i] ? v(vals[i]) : NULL, vl);
    }
    struct sst tmp;
    sst_builder_build(&sb, SST_PATH, id, &tmp);
    sst_close(&tmp);
    sst_open(sst, SST_PATH, id);
}

/*
 * ========================================
 * seek_first tests
 * ========================================
 */
static void test_seek_first(void)
{
    printf(COLOR_BLUE "\n--- Seek First ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"apple", "banana", "cherry"};
    const char *vals[] = {"red", "yellow", "dark"};
    struct sst sst;
    build_and_open(BLOCK_SIZE, keys, vals, 3, &sst, 1);

    struct sst_iter iter;
    int ret = sst_iter_seek_first(&iter, &sst);
    EXPECT_EQ(ret, 0, "seek_first returns 0");
    EXPECT_COND(sst_iter_is_valid(&iter), "iterator is valid");
    EXPECT_EQ(sst_iter_key_len(&iter), 5, "key_len is 5");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "apple", 5) == 0,
                "first key is apple");
    EXPECT_EQ(sst_iter_value_len(&iter), 3, "value_len is 3");
    EXPECT_COND(memcmp(sst_iter_value(&iter), "red", 3) == 0,
                "first value is red");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_seek_first_invalid(void)
{
    printf(COLOR_BLUE "\n--- Seek First Invalid ---\n" COLOR_RESET);

    struct sst_iter iter;
    EXPECT_EQ(sst_iter_seek_first(NULL, NULL), -1, "NULL args returns -1");
    EXPECT_EQ(sst_iter_seek_first(&iter, NULL), -1, "NULL sst returns -1");
}

/*
 * ========================================
 * full scan tests
 * ========================================
 */
static void test_full_scan_single_block(void)
{
    printf(COLOR_BLUE "\n--- Full Scan Single Block ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc", "ddd"};
    const char *vals[] = {"111", "222", "333", "444"};
    struct sst sst;
    build_and_open(BLOCK_SIZE, keys, vals, 4, &sst, 1);

    struct sst_iter iter;
    sst_iter_seek_first(&iter, &sst);

    uint32_t count = 0;
    while (sst_iter_is_valid(&iter)) {
        EXPECT_EQ(sst_iter_key_len(&iter), 3, "key_len is 3");
        EXPECT_EQ(sst_iter_value_len(&iter), 3, "value_len is 3");
        count++;
        sst_iter_next(&iter);
    }
    EXPECT_EQ(count, 4, "scanned 4 entries");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_full_scan_multiple_blocks(void)
{
    printf(COLOR_BLUE "\n--- Full Scan Multiple Blocks ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc", "ddd", "eee", "fff"};
    const char *vals[] = {"111", "222", "333", "444", "555", "666"};
    struct sst sst;
    build_and_open(48, keys, vals, 6, &sst, 1);

    EXPECT_COND(sst.n_blocks > 1, "more than 1 block");

    struct sst_iter iter;
    sst_iter_seek_first(&iter, &sst);

    uint32_t count = 0;
    while (sst_iter_is_valid(&iter)) {
        count++;
        sst_iter_next(&iter);
    }
    EXPECT_EQ(count, 6, "scanned 6 entries across blocks");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_full_scan_order(void)
{
    printf(COLOR_BLUE "\n--- Full Scan Order ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc", "ddd", "eee"};
    const char *vals[] = {"111", "222", "333", "444", "555"};
    struct sst sst;
    build_and_open(48, keys, vals, 5, &sst, 1);

    struct sst_iter iter;
    sst_iter_seek_first(&iter, &sst);

    for (uint32_t i = 0; i < 5; i++) {
        EXPECT_COND(sst_iter_is_valid(&iter), "iterator valid");
        EXPECT_EQ(sst_iter_key_len(&iter), 3, "key_len is 3");
        EXPECT_COND(memcmp(sst_iter_key(&iter), keys[i], 3) == 0,
                    "key in order");
        EXPECT_COND(memcmp(sst_iter_value(&iter), vals[i], 3) == 0,
                    "value matches");
        sst_iter_next(&iter);
    }
    EXPECT_COND(!sst_iter_is_valid(&iter), "exhausted after 5 entries");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * seek_key tests
 * ========================================
 */
static void test_seek_key_exact(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Exact ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"apple", "banana", "cherry", "date", "elderberry"};
    const char *vals[] = {"11", "22", "33", "44", "55"};
    struct sst sst;
    build_and_open(48, keys, vals, 5, &sst, 1);

    struct sst_iter iter;
    int ret = sst_iter_seek_key(&iter, &sst, k("cherry"), 6);
    EXPECT_EQ(ret, 0, "seek_key returns 0");
    EXPECT_COND(sst_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "cherry", 6) == 0, "found cherry");
    EXPECT_COND(memcmp(sst_iter_value(&iter), "33", 2) == 0, "value is 33");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_seek_key_between(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Between ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "ccc", "eee", "ggg"};
    const char *vals[] = {"111", "333", "555", "777"};
    struct sst sst;
    build_and_open(48, keys, vals, 4, &sst, 1);

    /* seek to "bbb" should land on "ccc" */
    struct sst_iter iter;
    int ret = sst_iter_seek_key(&iter, &sst, k("bbb"), 3);
    EXPECT_EQ(ret, 0, "seek_key returns 0");
    EXPECT_COND(sst_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "ccc", 3) == 0, "landed on ccc");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_seek_key_first(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Before First ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"bbb", "ccc", "ddd"};
    const char *vals[] = {"222", "333", "444"};
    struct sst sst;
    build_and_open(BLOCK_SIZE, keys, vals, 3, &sst, 1);

    /* seek to "aaa" should land on "bbb" */
    struct sst_iter iter;
    sst_iter_seek_key(&iter, &sst, k("aaa"), 3);
    EXPECT_COND(sst_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "bbb", 3) == 0, "landed on bbb");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_seek_key_past_end(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Past End ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc"};
    const char *vals[] = {"111", "222", "333"};
    struct sst sst;
    build_and_open(BLOCK_SIZE, keys, vals, 3, &sst, 1);

    struct sst_iter iter;
    int ret = sst_iter_seek_key(&iter, &sst, k("zzz"), 3);
    EXPECT_EQ(ret, -1, "seek past end returns -1");
    EXPECT_COND(!sst_iter_is_valid(&iter), "iterator invalid");

    sst_close(&sst);
    cleanup();
}

static void test_seek_key_then_scan(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Then Scan ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc", "ddd", "eee", "fff"};
    const char *vals[] = {"111", "222", "333", "444", "555", "666"};
    struct sst sst;
    build_and_open(48, keys, vals, 6, &sst, 1);

    /* seek to "ccc" then scan remaining */
    struct sst_iter iter;
    sst_iter_seek_key(&iter, &sst, k("ccc"), 3);
    EXPECT_COND(sst_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "ccc", 3) == 0, "starts at ccc");

    uint32_t count = 0;
    while (sst_iter_is_valid(&iter)) {
        count++;
        sst_iter_next(&iter);
    }
    EXPECT_EQ(count, 4, "scanned ccc ddd eee fff");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

static void test_seek_key_invalid(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Invalid ---\n" COLOR_RESET);

    struct sst_iter iter;
    EXPECT_EQ(sst_iter_seek_key(NULL, NULL, k("a"), 1), -1,
              "NULL args returns -1");
    EXPECT_EQ(sst_iter_seek_key(&iter, NULL, k("a"), 1), -1,
              "NULL sst returns -1");
}

/*
 * ========================================
 * tombstone tests
 * ========================================
 */
static void test_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Tombstone ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"alive", "dead", "zombie"};
    const char *vals[] = {"yes", NULL, "maybe"};
    struct sst sst;
    build_and_open(BLOCK_SIZE, keys, vals, 3, &sst, 1);

    struct sst_iter iter;
    sst_iter_seek_first(&iter, &sst);

    /* alive */
    EXPECT_COND(sst_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "alive", 5) == 0, "key is alive");
    EXPECT_EQ(sst_iter_value_len(&iter), 3, "has value");
    sst_iter_next(&iter);

    /* dead (tombstone) */
    EXPECT_COND(sst_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "dead", 4) == 0, "key is dead");
    EXPECT_EQ(sst_iter_value_len(&iter), 0, "tombstone");
    sst_iter_next(&iter);

    /* zombie */
    EXPECT_COND(sst_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(sst_iter_key(&iter), "zombie", 6) == 0, "key is zombie");
    EXPECT_EQ(sst_iter_value_len(&iter), 5, "has value");
    sst_iter_next(&iter);

    EXPECT_COND(!sst_iter_is_valid(&iter), "exhausted");

    sst_iter_destroy(&iter);
    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    /* seek_first */
    test_seek_first();
    test_seek_first_invalid();

    /* full scan */
    test_full_scan_single_block();
    test_full_scan_multiple_blocks();
    test_full_scan_order();

    /* seek_key */
    test_seek_key_exact();
    test_seek_key_between();
    test_seek_key_first();
    test_seek_key_past_end();
    test_seek_key_then_scan();
    test_seek_key_invalid();

    /* tombstone */
    test_tombstone();

    TEST_SUMMARY();
}