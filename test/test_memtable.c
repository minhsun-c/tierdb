#include <stdlib.h>
#include <string.h>
#include "checker.h"
#include "memtable.h"
#include "util.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

#define MAX_LEVEL 8

static const uint8_t *k(const char *s)
{
    return (const uint8_t *) s;
}

/*
 * ========================================
 * tests
 * ========================================
 */
static void test_init(void)
{
    printf(COLOR_BLUE "\n--- Init ---\n" COLOR_RESET);
    struct memtable mt;
    int ret = memtable_init(&mt, MAX_LEVEL, 0);

    EXPECT_EQ(ret, 0, "init returns 0");
    EXPECT_EQ(mt.size, 0, "size is 0");
    EXPECT_EQ(mt.approx_size, 0, "approx_size is 0");
    EXPECT_EQ(mt.id, 0, "id is 0");
    EXPECT_COND(memtable_iter_first(&mt) == NULL, "empty after init");

    memtable_destroy(&mt);
}

static void test_put_and_get(void)
{
    printf(COLOR_BLUE "\n--- Put and Get ---\n" COLOR_RESET);
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("apple"), 5, k("1"), 1);
    memtable_put(&mt, k("banana"), 6, k("2"), 1);
    memtable_put(&mt, k("cherry"), 6, k("3"), 1);

    struct memtable_entry *e;

    e = memtable_get(&mt, k("apple"), 5);
    EXPECT_COND(e != NULL, "apple found");
    EXPECT_STR_EQ((char *) e->value, "1", "apple value is 1");

    e = memtable_get(&mt, k("banana"), 6);
    EXPECT_COND(e != NULL, "banana found");
    EXPECT_STR_EQ((char *) e->value, "2", "banana value is 2");

    e = memtable_get(&mt, k("cherry"), 6);
    EXPECT_COND(e != NULL, "cherry found");
    EXPECT_STR_EQ((char *) e->value, "3", "cherry value is 3");

    e = memtable_get(&mt, k("mango"), 5);
    EXPECT_COND(e == NULL, "mango not found");

    EXPECT_EQ(mt.size, 3, "size is 3");

    memtable_destroy(&mt);
}

static void test_overwrite(void)
{
    printf(COLOR_BLUE "\n--- Overwrite ---\n" COLOR_RESET);
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("key"), 3, k("old"), 3);
    memtable_put(&mt, k("key"), 3, k("new"), 3);

    struct memtable_entry *e = memtable_get(&mt, k("key"), 3);
    EXPECT_COND(e != NULL, "key found after overwrite");
    EXPECT_STR_EQ((char *) e->value, "new", "value is updated to new");
    EXPECT_EQ(mt.size, 1, "size stays 1 after overwrite");

    memtable_destroy(&mt);
}

static void test_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Tombstone ---\n" COLOR_RESET);
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("key"), 3, k("val"), 3);
    memtable_put(&mt, k("key"), 3, NULL, 0); /* delete */

    struct memtable_entry *e = memtable_get(&mt, k("key"), 3);
    EXPECT_COND(e != NULL, "tombstone entry still exists");
    EXPECT_EQ(e->value_len, 0, "value_len is 0 (tombstone)");
    EXPECT_COND(e->value == NULL, "value pointer is NULL");

    memtable_destroy(&mt);
}

static void test_sorted_iteration(void)
{
    printf(COLOR_BLUE "\n--- Sorted Iteration ---\n" COLOR_RESET);
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("dog"), 3, k("4"), 1);
    memtable_put(&mt, k("apple"), 5, k("1"), 1);
    memtable_put(&mt, k("cherry"), 6, k("3"), 1);
    memtable_put(&mt, k("banana"), 6, k("2"), 1);

    const char *expected[] = {"apple", "banana", "cherry", "dog"};
    int i = 0;
    struct memtable_entry *e = memtable_iter_first(&mt);
    while (e) {
        EXPECT_STR_EQ((char *) e->key, expected[i], expected[i]);
        i++;
        e = memtable_iter_next(e);
    }
    EXPECT_EQ(i, 4, "iterated 4 entries");

    memtable_destroy(&mt);
}

static void test_is_full(void)
{
    printf(COLOR_BLUE "\n--- Is Full ---\n" COLOR_RESET);
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    memtable_put(&mt, k("key"), 3, k("val"), 3);

    EXPECT_COND(!memtable_is_full(&mt, 100), "not full below threshold");
    EXPECT_COND(memtable_is_full(&mt, 6), "full at threshold");
    EXPECT_COND(memtable_is_full(&mt, 1), "full above threshold");

    memtable_destroy(&mt);
}

static void test_invalid_key(void)
{
    printf(COLOR_BLUE "\n--- Invalid Key ---\n" COLOR_RESET);
    struct memtable mt;
    memtable_init(&mt, MAX_LEVEL, 1);

    EXPECT_EQ(memtable_put(&mt, NULL, 3, k("val"), 3), -1,
              "NULL key returns -1");
    EXPECT_EQ(memtable_put(&mt, k("key"), 0, k("val"), 3), -1,
              "zero key_len returns -1");

    memtable_destroy(&mt);
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    srand(42);

    test_init();
    test_put_and_get();
    test_overwrite();
    test_tombstone();
    test_sorted_iteration();
    test_is_full();
    test_invalid_key();

    TEST_SUMMARY();
}