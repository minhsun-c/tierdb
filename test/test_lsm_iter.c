#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "block.h"
#include "checker.h"
#include "iter.h"
#include "lsm_iter.h"
#include "memtable.h"
#include "mt_iter.h"
#include "sst.h"
#include "sst_builder.h"
#include "sst_iter.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const char *SST_PATH_0 = "/tmp/test_lsm_iter_0.sst";
static const char *SST_PATH_1 = "/tmp/test_lsm_iter_1.sst";

static const uint8_t *k(const char *s)
{
    return (const uint8_t *) s;
}
static const uint8_t *v(const char *s)
{
    return (const uint8_t *) s;
}

#define MAX_LEVEL 16

static void cleanup(void)
{
    unlink(SST_PATH_0);
    unlink(SST_PATH_1);
}

static void build_and_open(const char *path,
                           size_t block_size,
                           const char **keys,
                           const char **vals,
                           uint32_t n,
                           struct sst *sst,
                           uint64_t id)
{
    struct sst_builder sb;
    sst_builder_init(&sb, block_size, path);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t kl = (uint16_t) strlen(keys[i]);
        uint16_t vl = vals[i] ? (uint16_t) strlen(vals[i]) : 0;
        sst_builder_add(&sb, k(keys[i]), kl, vals[i] ? v(vals[i]) : NULL, vl);
    }
    struct sst tmp;
    sst_builder_build(&sb, path, id, &tmp);
    sst_close(&tmp);
    sst_open(sst, path, id);
}

/*
 * ========================================
 * basic scan tests
 * ========================================
 */
static void test_basic_scan(void)
{
    printf(COLOR_BLUE "\n--- Basic Scan ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v("222"), 3);
    memtable_put(&mt, k("cherry"), 6, v("333"), 3);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, NULL, 0);

    const char *expected[] = {"apple", "banana", "cherry"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count],
                           strlen(expected[count])) == 0,
                    "key in order");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 3, "3 entries");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

/*
 * ========================================
 * upper bound tests
 * ========================================
 */
static void test_upper_bound(void)
{
    printf(COLOR_BLUE "\n--- Upper Bound ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v("222"), 3);
    memtable_put(&mt, k("cherry"), 6, v("333"), 3);
    memtable_put(&mt, k("mango"), 5, v("444"), 3);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, k("cherry"), 6);

    const char *expected[] = {"apple", "banana", "cherry"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count],
                           strlen(expected[count])) == 0,
                    "key in order");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 3, "3 entries within bound");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

static void test_upper_bound_excludes(void)
{
    printf(COLOR_BLUE "\n--- Upper Bound Excludes ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt, k("bbb"), 3, v("222"), 3);
    memtable_put(&mt, k("ccc"), 3, v("333"), 3);
    memtable_put(&mt, k("ddd"), 3, v("444"), 3);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    /* upper = "bbc", so "ccc" and "ddd" excluded */
    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, k("bbc"), 3);

    const char *expected[] = {"aaa", "bbb"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count], 3) == 0,
                    "key in order");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 2, "2 entries within bound");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

static void test_lower_and_upper_bound(void)
{
    printf(COLOR_BLUE "\n--- Lower and Upper Bound ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v("222"), 3);
    memtable_put(&mt, k("cherry"), 6, v("333"), 3);
    memtable_put(&mt, k("mango"), 5, v("444"), 3);

    struct mt_iter mi;
    mt_iter_seek_key(&mi, &mt, k("banana"), 6);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, k("cherry"), 6);

    const char *expected[] = {"banana", "cherry"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count],
                           strlen(expected[count])) == 0,
                    "key in order");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 2, "2 entries within bounds");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

/*
 * ========================================
 * tombstone tests
 * ========================================
 */
static void test_skip_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Skip Tombstone ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v(""), 0);
    memtable_put(&mt, k("cherry"), 6, v("333"), 3);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, NULL, 0);

    const char *expected[] = {"apple", "cherry"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count],
                           strlen(expected[count])) == 0,
                    "key in order");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 2, "tombstone skipped");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

static void test_tombstone_hides_sst(void)
{
    printf(COLOR_BLUE "\n--- Tombstone Hides SST ---\n" COLOR_RESET);
    cleanup();

    /* memtable deletes apple */
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v(""), 0);
    memtable_put(&mt, k("cherry"), 6, v("new"), 3);

    /* SST has apple and banana */
    const char *sst_keys[] = {"apple", "banana"};
    const char *sst_vals[] = {"red", "yellow"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 2, &sst, 1);

    struct mt_iter mi;
    struct sst_iter si;
    mt_iter_seek_first(&mi, &mt);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[2];
    mt_iter_to_iter(&mi, &iters[0]);
    sst_iter_to_iter(&si, &iters[1]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 2, NULL, 0);

    /* apple is tombstoned, should see banana and cherry */
    const char *expected[] = {"banana", "cherry"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count],
                           strlen(expected[count])) == 0,
                    "key in order");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 2, "tombstone hides SST apple");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    sst_iter_destroy(&si);
    memtable_destroy(&mt);
    sst_close(&sst);
    cleanup();
}

static void test_all_tombstones(void)
{
    printf(COLOR_BLUE "\n--- All Tombstones ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("aaa"), 3, v(""), 0);
    memtable_put(&mt, k("bbb"), 3, v(""), 0);
    memtable_put(&mt, k("ccc"), 3, v(""), 0);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, NULL, 0);

    EXPECT_COND(!lsm_iter_is_valid(&iter), "all tombstones: invalid");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

/*
 * ========================================
 * dedup tests
 * ========================================
 */
static void test_dedup_newest_wins(void)
{
    printf(COLOR_BLUE "\n--- Dedup Newest Wins ---\n" COLOR_RESET);

    struct memtable mt0, mt1;
    memtable_init(&mt0, MAX_LEVEL, 2); /* newer */
    memtable_init(&mt1, MAX_LEVEL, 1); /* older */

    memtable_put(&mt0, k("key"), 3, v("new"), 3);
    memtable_put(&mt1, k("key"), 3, v("old"), 3);

    struct mt_iter mi[2];
    mt_iter_seek_first(&mi[0], &mt0);
    mt_iter_seek_first(&mi[1], &mt1);

    struct iter iters[2];
    mt_iter_to_iter(&mi[0], &iters[0]);
    mt_iter_to_iter(&mi[1], &iters[1]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 2, NULL, 0);

    EXPECT_COND(lsm_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(lsm_iter_key(&iter), "key", 3) == 0, "key is key");
    EXPECT_COND(memcmp(lsm_iter_value(&iter), "new", 3) == 0,
                "newer value wins");

    lsm_iter_next(&iter);
    EXPECT_COND(!lsm_iter_is_valid(&iter), "only one entry");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi[0]);
    mt_iter_destroy(&mi[1]);
    memtable_destroy(&mt0);
    memtable_destroy(&mt1);
}

/*
 * ========================================
 * mixed memtable + SST tests
 * ========================================
 */
static void test_mixed_scan(void)
{
    printf(COLOR_BLUE "\n--- Mixed Scan ---\n" COLOR_RESET);
    cleanup();

    /* mt0 (newest): apple, cherry */
    struct memtable mt0;
    memtable_init(&mt0, MAX_LEVEL, 3);
    memtable_put(&mt0, k("apple"), 5, v("mt0"), 3);
    memtable_put(&mt0, k("cherry"), 6, v("mt0"), 3);

    /* mt1: banana */
    struct memtable mt1;
    memtable_init(&mt1, MAX_LEVEL, 2);
    memtable_put(&mt1, k("banana"), 6, v("mt1"), 3);

    /* sst: apple, date */
    const char *sst_keys[] = {"apple", "date"};
    const char *sst_vals[] = {"sst", "sst"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 2, &sst, 1);

    struct mt_iter mi[2];
    struct sst_iter si;
    mt_iter_seek_first(&mi[0], &mt0);
    mt_iter_seek_first(&mi[1], &mt1);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[3];
    mt_iter_to_iter(&mi[0], &iters[0]);
    mt_iter_to_iter(&mi[1], &iters[1]);
    sst_iter_to_iter(&si, &iters[2]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 3, NULL, 0);

    const char *exp_keys[] = {"apple", "banana", "cherry", "date"};
    const char *exp_vals[] = {"mt0", "mt1", "mt0", "sst"};
    const uint16_t exp_klens[] = {5, 6, 6, 4};

    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(
            memcmp(lsm_iter_key(&iter), exp_keys[count], exp_klens[count]) == 0,
            "key correct");
        EXPECT_COND(memcmp(lsm_iter_value(&iter), exp_vals[count], 3) == 0,
                    "value from correct source");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 4, "4 unique keys");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi[0]);
    mt_iter_destroy(&mi[1]);
    sst_iter_destroy(&si);
    memtable_destroy(&mt0);
    memtable_destroy(&mt1);
    sst_close(&sst);
    cleanup();
}

static void test_mixed_with_upper_bound_and_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Mixed Upper Bound + Tombstone ---\n" COLOR_RESET);
    cleanup();

    /* memtable: delete banana, has cherry */
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("banana"), 6, v(""), 0);
    memtable_put(&mt, k("cherry"), 6, v("new"), 3);

    /* SST: apple, banana, date */
    const char *sst_keys[] = {"apple", "banana", "date"};
    const char *sst_vals[] = {"aaa", "bbb", "ddd"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 3, &sst, 1);

    struct mt_iter mi;
    struct sst_iter si;
    mt_iter_seek_first(&mi, &mt);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[2];
    mt_iter_to_iter(&mi, &iters[0]);
    sst_iter_to_iter(&si, &iters[1]);

    /* upper = "cherry" */
    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 2, k("cherry"), 6);

    /* apple (from SST), banana tombstoned, cherry (from mt), date excluded */
    const char *expected[] = {"apple", "cherry"};
    uint32_t count = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(lsm_iter_key(&iter), expected[count],
                           strlen(expected[count])) == 0,
                    "key correct");
        count++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(count, 2, "2 entries after filter");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    sst_iter_destroy(&si);
    memtable_destroy(&mt);
    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * empty and invalid tests
 * ========================================
 */
static void test_empty(void)
{
    printf(COLOR_BLUE "\n--- Empty ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct lsm_iter iter;
    lsm_iter_init(&iter, iters, 1, NULL, 0);

    EXPECT_COND(!lsm_iter_is_valid(&iter), "empty: invalid");

    lsm_iter_destroy(&iter);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

static void test_invalid(void)
{
    printf(COLOR_BLUE "\n--- Invalid Args ---\n" COLOR_RESET);

    struct lsm_iter iter;
    EXPECT_EQ(lsm_iter_init(NULL, NULL, 0, NULL, 0), -1,
              "NULL args returns -1");
    EXPECT_EQ(lsm_iter_init(&iter, NULL, 0, NULL, 0), -1,
              "NULL iters returns -1");
    EXPECT_COND(!lsm_iter_is_valid(NULL), "NULL iter invalid");
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    /* basic */
    test_basic_scan();

    /* upper bound */
    test_upper_bound();
    test_upper_bound_excludes();
    test_lower_and_upper_bound();

    /* tombstone */
    test_skip_tombstone();
    test_tombstone_hides_sst();
    test_all_tombstones();

    /* dedup */
    test_dedup_newest_wins();

    /* mixed */
    test_mixed_scan();
    test_mixed_with_upper_bound_and_tombstone();

    /* empty and invalid */
    test_empty();
    test_invalid();

    TEST_SUMMARY();
}