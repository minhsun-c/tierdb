#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "block.h"
#include "checker.h"
#include "iter.h"
#include "memtable.h"
#include "merge_iter.h"
#include "mt_iter.h"
#include "sst.h"
#include "sst_builder.h"
#include "sst_iter.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const char *SST_PATH_0 = "/tmp/test_merge_iter_0.sst";
static const char *SST_PATH_1 = "/tmp/test_merge_iter_1.sst";

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
 * single source tests
 * ========================================
 */
static void test_single_memtable(void)
{
    printf(COLOR_BLUE "\n--- Single Memtable ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v("222"), 3);
    memtable_put(&mt, k("cherry"), 6, v("333"), 3);

    struct mt_iter mi;
    mt_iter_seek_first(&mi, &mt);

    struct iter iters[1];
    mt_iter_to_iter(&mi, &iters[0]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 1);

    const char *expected[] = {"apple", "banana", "cherry"};
    uint32_t count = 0;
    while (merge_iter_is_valid(&merge)) {
        EXPECT_COND(memcmp(merge_iter_key(&merge), expected[count],
                           strlen(expected[count])) == 0,
                    "key in order");
        count++;
        merge_iter_next(&merge);
    }
    EXPECT_EQ(count, 3, "3 entries");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi);
    memtable_destroy(&mt);
}

static void test_single_sst(void)
{
    printf(COLOR_BLUE "\n--- Single SST ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc"};
    const char *vals[] = {"111", "222", "333"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, keys, vals, 3, &sst, 1);

    struct sst_iter si;
    sst_iter_seek_first(&si, &sst);

    struct iter iters[1];
    sst_iter_to_iter(&si, &iters[0]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 1);

    uint32_t count = 0;
    while (merge_iter_is_valid(&merge)) {
        EXPECT_COND(memcmp(merge_iter_key(&merge), keys[count], 3) == 0,
                    "key in order");
        count++;
        merge_iter_next(&merge);
    }
    EXPECT_EQ(count, 3, "3 entries");

    merge_iter_destroy(&merge);
    sst_iter_destroy(&si);
    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * multiple memtable tests
 * ========================================
 */
static void test_two_memtables_no_overlap(void)
{
    printf(COLOR_BLUE "\n--- Two Memtables No Overlap ---\n" COLOR_RESET);

    struct memtable mt0, mt1;
    memtable_init(&mt0, MAX_LEVEL, 2);
    memtable_init(&mt1, MAX_LEVEL, 1);

    memtable_put(&mt0, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt0, k("ccc"), 3, v("333"), 3);
    memtable_put(&mt1, k("bbb"), 3, v("222"), 3);
    memtable_put(&mt1, k("ddd"), 3, v("444"), 3);

    struct mt_iter mi[2];
    mt_iter_seek_first(&mi[0], &mt0);
    mt_iter_seek_first(&mi[1], &mt1);

    struct iter iters[2];
    mt_iter_to_iter(&mi[0], &iters[0]);
    mt_iter_to_iter(&mi[1], &iters[1]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 2);

    const char *expected[] = {"aaa", "bbb", "ccc", "ddd"};
    uint32_t count = 0;
    while (merge_iter_is_valid(&merge)) {
        EXPECT_COND(memcmp(merge_iter_key(&merge), expected[count], 3) == 0,
                    "key in order");
        count++;
        merge_iter_next(&merge);
    }
    EXPECT_EQ(count, 4, "4 entries");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi[0]);
    mt_iter_destroy(&mi[1]);
    memtable_destroy(&mt0);
    memtable_destroy(&mt1);
}

static void test_two_memtables_duplicate_key(void)
{
    printf(COLOR_BLUE "\n--- Two Memtables Duplicate Key ---\n" COLOR_RESET);

    struct memtable mt0, mt1;
    memtable_init(&mt0, MAX_LEVEL, 2); /* newer */
    memtable_init(&mt1, MAX_LEVEL, 1); /* older */

    memtable_put(&mt0, k("key"), 3, v("new"), 3);
    memtable_put(&mt1, k("key"), 3, v("old"), 3);

    struct mt_iter mi[2];
    mt_iter_seek_first(&mi[0], &mt0); /* index 0 = newer */
    mt_iter_seek_first(&mi[1], &mt1);

    struct iter iters[2];
    mt_iter_to_iter(&mi[0], &iters[0]);
    mt_iter_to_iter(&mi[1], &iters[1]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 2);

    EXPECT_COND(merge_iter_is_valid(&merge), "valid");
    EXPECT_COND(memcmp(merge_iter_key(&merge), "key", 3) == 0, "key is key");
    EXPECT_COND(memcmp(merge_iter_value(&merge), "new", 3) == 0,
                "newer value wins");

    merge_iter_next(&merge);
    EXPECT_COND(!merge_iter_is_valid(&merge), "only one entry (dedup)");

    merge_iter_destroy(&merge);
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
static void test_mixed_no_overlap(void)
{
    printf(COLOR_BLUE "\n--- Mixed No Overlap ---\n" COLOR_RESET);
    cleanup();

    /* memtable */
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt, k("ccc"), 3, v("333"), 3);

    /* SST */
    const char *sst_keys[] = {"bbb", "ddd"};
    const char *sst_vals[] = {"222", "444"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 2, &sst, 1);

    struct mt_iter mi;
    struct sst_iter si;
    mt_iter_seek_first(&mi, &mt);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[2];
    mt_iter_to_iter(&mi, &iters[0]);
    sst_iter_to_iter(&si, &iters[1]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 2);

    const char *expected[] = {"aaa", "bbb", "ccc", "ddd"};
    uint32_t count = 0;
    while (merge_iter_is_valid(&merge)) {
        EXPECT_COND(memcmp(merge_iter_key(&merge), expected[count], 3) == 0,
                    "key in order");
        count++;
        merge_iter_next(&merge);
    }
    EXPECT_EQ(count, 4, "4 entries");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi);
    sst_iter_destroy(&si);
    memtable_destroy(&mt);
    sst_close(&sst);
    cleanup();
}

static void test_mixed_duplicate_key(void)
{
    printf(COLOR_BLUE
           "\n--- Mixed Duplicate Key (memtable wins) ---\n" COLOR_RESET);
    cleanup();

    /* memtable has newer version */
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v("new"), 3);

    /* SST has older version */
    const char *sst_keys[] = {"apple"};
    const char *sst_vals[] = {"old"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 1, &sst, 1);

    struct mt_iter mi;
    struct sst_iter si;
    mt_iter_seek_first(&mi, &mt);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[2];
    mt_iter_to_iter(&mi, &iters[0]); /* index 0 = memtable = newer */
    sst_iter_to_iter(&si, &iters[1]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 2);

    EXPECT_COND(merge_iter_is_valid(&merge), "valid");
    EXPECT_COND(memcmp(merge_iter_value(&merge), "new", 3) == 0,
                "memtable value wins");

    merge_iter_next(&merge);
    EXPECT_COND(!merge_iter_is_valid(&merge), "dedup: one entry only");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi);
    sst_iter_destroy(&si);
    memtable_destroy(&mt);
    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * tombstone tests
 * ========================================
 */
static void test_tombstone_visible(void)
{
    printf(COLOR_BLUE "\n--- Tombstone Visible ---\n" COLOR_RESET);
    cleanup();

    /* memtable deletes "apple" */
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v(""), 0);

    /* SST has "apple" = "red" */
    const char *sst_keys[] = {"apple"};
    const char *sst_vals[] = {"red"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 1, &sst, 1);

    struct mt_iter mi;
    struct sst_iter si;
    mt_iter_seek_first(&mi, &mt);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[2];
    mt_iter_to_iter(&mi, &iters[0]);
    sst_iter_to_iter(&si, &iters[1]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 2);

    /* merge_iter should show tombstone, not skip it */
    EXPECT_COND(merge_iter_is_valid(&merge), "valid");
    EXPECT_COND(memcmp(merge_iter_key(&merge), "apple", 5) == 0,
                "key is apple");
    EXPECT_EQ(merge_iter_value_len(&merge), 0, "tombstone visible");

    merge_iter_next(&merge);
    EXPECT_COND(!merge_iter_is_valid(&merge), "SST apple deduped, not visible");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi);
    sst_iter_destroy(&si);
    memtable_destroy(&mt);
    sst_close(&sst);
    cleanup();
}

static void test_tombstone_with_other_keys(void)
{
    printf(COLOR_BLUE "\n--- Tombstone With Other Keys ---\n" COLOR_RESET);
    cleanup();

    /* memtable: delete apple, has banana and cherry */
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);
    memtable_put(&mt, k("apple"), 5, v(""), 0);
    memtable_put(&mt, k("banana"), 6, v("yellow"), 6);
    memtable_put(&mt, k("cherry"), 6, v("dark"), 4);

    /* SST: has apple */
    const char *sst_keys[] = {"apple"};
    const char *sst_vals[] = {"red"};
    struct sst sst;
    build_and_open(SST_PATH_0, BLOCK_SIZE, sst_keys, sst_vals, 1, &sst, 1);

    struct mt_iter mi;
    struct sst_iter si;
    mt_iter_seek_first(&mi, &mt);
    sst_iter_seek_first(&si, &sst);

    struct iter iters[2];
    mt_iter_to_iter(&mi, &iters[0]);
    sst_iter_to_iter(&si, &iters[1]);

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 2);

    /* apple (tombstone) */
    EXPECT_COND(merge_iter_is_valid(&merge), "valid");
    EXPECT_COND(memcmp(merge_iter_key(&merge), "apple", 5) == 0,
                "key is apple");
    EXPECT_EQ(merge_iter_value_len(&merge), 0, "tombstone");
    merge_iter_next(&merge);

    /* banana */
    EXPECT_COND(merge_iter_is_valid(&merge), "valid");
    EXPECT_COND(memcmp(merge_iter_key(&merge), "banana", 6) == 0,
                "key is banana");
    merge_iter_next(&merge);

    /* cherry */
    EXPECT_COND(merge_iter_is_valid(&merge), "valid");
    EXPECT_COND(memcmp(merge_iter_key(&merge), "cherry", 6) == 0,
                "key is cherry");
    merge_iter_next(&merge);

    EXPECT_COND(!merge_iter_is_valid(&merge), "exhausted");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi);
    sst_iter_destroy(&si);
    memtable_destroy(&mt);
    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * many sources test
 * ========================================
 */
static void test_many_sources(void)
{
    printf(COLOR_BLUE "\n--- Many Sources ---\n" COLOR_RESET);
    cleanup();

    /* mt0 (newest): apple, cherry */
    struct memtable mt0;
    memtable_init(&mt0, MAX_LEVEL, 3);
    memtable_put(&mt0, k("apple"), 5, v("mt0"), 3);
    memtable_put(&mt0, k("cherry"), 6, v("mt0"), 3);

    /* mt1: banana, cherry */
    struct memtable mt1;
    memtable_init(&mt1, MAX_LEVEL, 2);
    memtable_put(&mt1, k("banana"), 6, v("mt1"), 3);
    memtable_put(&mt1, k("cherry"), 6, v("mt1"), 3);

    /* sst0: apple, date */
    const char *s0_keys[] = {"apple", "date"};
    const char *s0_vals[] = {"ss0", "ss0"};
    struct sst sst0;
    build_and_open(SST_PATH_0, BLOCK_SIZE, s0_keys, s0_vals, 2, &sst0, 1);

    /* sst1: banana, elderberry */
    const char *s1_keys[] = {"banana", "elderberry"};
    const char *s1_vals[] = {"ss1", "ss1"};
    struct sst sst1;
    build_and_open(SST_PATH_1, BLOCK_SIZE, s1_keys, s1_vals, 2, &sst1, 2);

    struct mt_iter mi[2];
    struct sst_iter si[2];
    mt_iter_seek_first(&mi[0], &mt0);
    mt_iter_seek_first(&mi[1], &mt1);
    sst_iter_seek_first(&si[0], &sst0);
    sst_iter_seek_first(&si[1], &sst1);

    struct iter iters[4];
    mt_iter_to_iter(&mi[0], &iters[0]); /* newest */
    mt_iter_to_iter(&mi[1], &iters[1]);
    sst_iter_to_iter(&si[0], &iters[2]);
    sst_iter_to_iter(&si[1], &iters[3]); /* oldest */

    struct merge_iter merge;
    merge_iter_init(&merge, iters, 4);

    /* expected: apple(mt0), banana(mt1), cherry(mt0), date(ss0),
     * elderberry(ss1) */
    const char *exp_keys[] = {"apple", "banana", "cherry", "date",
                              "elderberry"};
    const char *exp_vals[] = {"mt0", "mt1", "mt0", "ss0", "ss1"};
    const uint16_t exp_klens[] = {5, 6, 6, 4, 10};

    uint32_t count = 0;
    while (merge_iter_is_valid(&merge)) {
        EXPECT_COND(memcmp(merge_iter_key(&merge), exp_keys[count],
                           exp_klens[count]) == 0,
                    "key correct");
        EXPECT_COND(memcmp(merge_iter_value(&merge), exp_vals[count], 3) == 0,
                    "value from correct source");
        count++;
        merge_iter_next(&merge);
    }
    EXPECT_EQ(count, 5, "5 unique keys");

    merge_iter_destroy(&merge);
    mt_iter_destroy(&mi[0]);
    mt_iter_destroy(&mi[1]);
    sst_iter_destroy(&si[0]);
    sst_iter_destroy(&si[1]);
    memtable_destroy(&mt0);
    memtable_destroy(&mt1);
    sst_close(&sst0);
    sst_close(&sst1);
    cleanup();
}

/*
 * ========================================
 * invalid args
 * ========================================
 */
static void test_invalid(void)
{
    printf(COLOR_BLUE "\n--- Invalid Args ---\n" COLOR_RESET);

    struct merge_iter merge;
    EXPECT_EQ(merge_iter_init(NULL, NULL, 0), -1, "NULL args returns -1");
    EXPECT_EQ(merge_iter_init(&merge, NULL, 0), -1, "NULL iters returns -1");
    EXPECT_COND(!merge_iter_is_valid(NULL), "NULL merge invalid");
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    /* single source */
    test_single_memtable();
    test_single_sst();

    /* multiple memtables */
    test_two_memtables_no_overlap();
    test_two_memtables_duplicate_key();

    /* mixed */
    test_mixed_no_overlap();
    test_mixed_duplicate_key();

    /* tombstone */
    test_tombstone_visible();
    test_tombstone_with_other_keys();

    /* many sources */
    test_many_sources();

    /* invalid */
    test_invalid();

    TEST_SUMMARY();
}