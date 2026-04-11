#include <stdlib.h>
#include <string.h>

#include "checker.h"
#include "memtable.h"
#include "mt_iter.h"

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

#define MAX_LEVEL 16

/*
 * ========================================
 * seek_first tests
 * ========================================
 */
static void test_seek_first(void)
{
    printf(COLOR_BLUE "\n--- Seek First ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("cherry"), 6, v("333"), 3);
    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v("222"), 3);

    struct mt_iter iter;
    int ret = mt_iter_seek_first(&iter, &mt);
    EXPECT_EQ(ret, 0, "seek_first returns 0");
    EXPECT_COND(mt_iter_is_valid(&iter), "iterator is valid");
    EXPECT_EQ(mt_iter_key_len(&iter), 5, "key_len is 5");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "apple", 5) == 0,
                "first key is apple");
    EXPECT_EQ(mt_iter_value_len(&iter), 3, "value_len is 3");
    EXPECT_COND(memcmp(mt_iter_value(&iter), "111", 3) == 0,
                "first value is 111");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_first_empty(void)
{
    printf(COLOR_BLUE "\n--- Seek First Empty ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    struct mt_iter iter;
    int ret = mt_iter_seek_first(&iter, &mt);
    EXPECT_EQ(ret, 0, "seek_first returns 0");
    EXPECT_COND(!mt_iter_is_valid(&iter), "iterator invalid on empty memtable");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_first_invalid(void)
{
    printf(COLOR_BLUE "\n--- Seek First Invalid ---\n" COLOR_RESET);

    struct mt_iter iter;
    EXPECT_EQ(mt_iter_seek_first(NULL, NULL), -1, "NULL args returns -1");
    EXPECT_EQ(mt_iter_seek_first(&iter, NULL), -1, "NULL mt returns -1");
}

/*
 * ========================================
 * full scan tests
 * ========================================
 */
static void test_full_scan(void)
{
    printf(COLOR_BLUE "\n--- Full Scan ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("ddd"), 3, v("444"), 3);
    memtable_put(&mt, k("bbb"), 3, v("222"), 3);
    memtable_put(&mt, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt, k("ccc"), 3, v("333"), 3);

    const char *expected_keys[] = {"aaa", "bbb", "ccc", "ddd"};
    const char *expected_vals[] = {"111", "222", "333", "444"};

    struct mt_iter iter;
    mt_iter_seek_first(&iter, &mt);

    uint32_t count = 0;
    while (mt_iter_is_valid(&iter)) {
        EXPECT_COND(memcmp(mt_iter_key(&iter), expected_keys[count], 3) == 0,
                    "key in order");
        EXPECT_COND(memcmp(mt_iter_value(&iter), expected_vals[count], 3) == 0,
                    "value matches");
        count++;
        mt_iter_next(&iter);
    }
    EXPECT_EQ(count, 4, "scanned 4 entries");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_full_scan_single(void)
{
    printf(COLOR_BLUE "\n--- Full Scan Single Entry ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("only"), 4, v("one"), 3);

    struct mt_iter iter;
    mt_iter_seek_first(&iter, &mt);

    EXPECT_COND(mt_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "only", 4) == 0, "key is only");
    EXPECT_COND(memcmp(mt_iter_value(&iter), "one", 3) == 0, "value is one");

    mt_iter_next(&iter);
    EXPECT_COND(!mt_iter_is_valid(&iter), "exhausted after 1 entry");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

/*
 * ========================================
 * seek_key tests
 * ========================================
 */
static void test_seek_key_exact(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Exact ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("apple"), 5, v("111"), 3);
    memtable_put(&mt, k("banana"), 6, v("222"), 3);
    memtable_put(&mt, k("cherry"), 6, v("333"), 3);

    struct mt_iter iter;
    int ret = mt_iter_seek_key(&iter, &mt, k("banana"), 6);
    EXPECT_EQ(ret, 0, "seek_key returns 0");
    EXPECT_COND(mt_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "banana", 6) == 0, "found banana");
    EXPECT_COND(memcmp(mt_iter_value(&iter), "222", 3) == 0, "value is 222");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_key_between(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Between ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt, k("ccc"), 3, v("333"), 3);
    memtable_put(&mt, k("eee"), 3, v("555"), 3);

    /* seek to "bbb" should land on "ccc" */
    struct mt_iter iter;
    mt_iter_seek_key(&iter, &mt, k("bbb"), 3);
    EXPECT_COND(mt_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "ccc", 3) == 0, "landed on ccc");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_key_before_first(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Before First ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("bbb"), 3, v("222"), 3);
    memtable_put(&mt, k("ccc"), 3, v("333"), 3);

    /* seek to "aaa" should land on "bbb" */
    struct mt_iter iter;
    mt_iter_seek_key(&iter, &mt, k("aaa"), 3);
    EXPECT_COND(mt_iter_is_valid(&iter), "iterator valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "bbb", 3) == 0, "landed on bbb");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_key_past_end(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Past End ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt, k("bbb"), 3, v("222"), 3);

    struct mt_iter iter;
    mt_iter_seek_key(&iter, &mt, k("zzz"), 3);
    EXPECT_COND(!mt_iter_is_valid(&iter), "iterator invalid past end");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_key_then_scan(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Then Scan ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("aaa"), 3, v("111"), 3);
    memtable_put(&mt, k("bbb"), 3, v("222"), 3);
    memtable_put(&mt, k("ccc"), 3, v("333"), 3);
    memtable_put(&mt, k("ddd"), 3, v("444"), 3);
    memtable_put(&mt, k("eee"), 3, v("555"), 3);

    struct mt_iter iter;
    mt_iter_seek_key(&iter, &mt, k("ccc"), 3);
    EXPECT_COND(memcmp(mt_iter_key(&iter), "ccc", 3) == 0, "starts at ccc");

    uint32_t count = 0;
    while (mt_iter_is_valid(&iter)) {
        count++;
        mt_iter_next(&iter);
    }
    EXPECT_EQ(count, 3, "scanned ccc ddd eee");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
}

static void test_seek_key_invalid(void)
{
    printf(COLOR_BLUE "\n--- Seek Key Invalid ---\n" COLOR_RESET);

    struct mt_iter iter;
    EXPECT_EQ(mt_iter_seek_key(NULL, NULL, k("a"), 1), -1,
              "NULL args returns -1");
    EXPECT_EQ(mt_iter_seek_key(&iter, NULL, k("a"), 1), -1,
              "NULL mt returns -1");
}

/*
 * ========================================
 * tombstone tests
 * ========================================
 */
static void test_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Tombstone ---\n" COLOR_RESET);

    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("alive"), 5, v("yes"), 3);
    memtable_put(&mt, k("dead"), 4, v(""), 0);
    memtable_put(&mt, k("zombie"), 6, v("maybe"), 5);

    struct mt_iter iter;
    mt_iter_seek_first(&iter, &mt);

    /* alive */
    EXPECT_COND(mt_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "alive", 5) == 0, "key is alive");
    EXPECT_EQ(mt_iter_value_len(&iter), 3, "has value");
    mt_iter_next(&iter);

    /* dead (tombstone) */
    EXPECT_COND(mt_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "dead", 4) == 0, "key is dead");
    EXPECT_EQ(mt_iter_value_len(&iter), 0, "tombstone");
    mt_iter_next(&iter);

    /* zombie */
    EXPECT_COND(mt_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(mt_iter_key(&iter), "zombie", 6) == 0, "key is zombie");
    EXPECT_EQ(mt_iter_value_len(&iter), 5, "has value");
    mt_iter_next(&iter);

    EXPECT_COND(!mt_iter_is_valid(&iter), "exhausted");

    mt_iter_destroy(&iter);
    memtable_destroy(&mt);
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
    test_seek_first_empty();
    test_seek_first_invalid();

    /* full scan */
    test_full_scan();
    test_full_scan_single();

    /* seek_key */
    test_seek_key_exact();
    test_seek_key_between();
    test_seek_key_before_first();
    test_seek_key_past_end();
    test_seek_key_then_scan();
    test_seek_key_invalid();

    /* tombstone */
    test_tombstone();

    TEST_SUMMARY();
}