#include <stdlib.h>
#include <string.h>
#include "checker.h"
#include "engine.h"
#include "lsm_iter.h"
#include "memtable.h"
#include "util.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static struct engine_options default_opts(void)
{
    struct engine_options opts = {
        .threshold = 4096,
        .imm_cap = 8,
        .max_level = 8,
    };
    return opts;
}

static const uint8_t *k(const char *s)
{
    return (const uint8_t *) s;
}

static const uint8_t *v(const char *s)
{
    return (const uint8_t *) s;
}

/* build sources array: mutable first, then imm newest→oldest */
static void build_sources(struct engine *e,
                          struct memtable **sources,
                          uint32_t *count)
{
    *count = 1 + e->imm_count;
    sources[0] = e->memtable;
    for (uint32_t i = 0; i < e->imm_count; i++)
        sources[1 + i] = e->imm_memtables[e->imm_count - 1 - i];
}

/*
 * ========================================
 * tests
 * ========================================
 */
static void test_single_memtable(void)
{
    printf(COLOR_BLUE "\n--- Single Memtable ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);

    struct memtable *sources[16];
    uint32_t count;
    build_sources(&e, sources, &count);

    struct lsm_iter iter;
    lsm_iter_init(&iter, sources, count, NULL, 0, NULL, 0);

    const char *expected[] = {"apple", "banana", "cherry"};
    const char *values[] = {"1", "2", "3"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        EXPECT_STR_EQ((char *) lsm_iter_value(&iter), values[i], values[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 3, "iterated 3 entries");

    lsm_iter_destroy(&iter);
    engine_close(&e);
}

static void test_lower_upper_bound(void)
{
    printf(COLOR_BLUE "\n--- Lower and Upper Bound ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);
    engine_put(&e, k("mango"), 5, v("4"), 1);

    struct memtable *sources[16];
    uint32_t count;
    build_sources(&e, sources, &count);

    struct lsm_iter iter;
    lsm_iter_init(&iter, sources, count, k("banana"), 6, k("cherry"), 6);

    const char *expected[] = {"banana", "cherry"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 2, "iterated 2 entries within bounds");

    lsm_iter_destroy(&iter);
    engine_close(&e);
}

static void test_tombstone_skipped(void)
{
    printf(COLOR_BLUE "\n--- Tombstone Skipped ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_delete(&e, k("banana"), 6);
    engine_put(&e, k("cherry"), 6, v("3"), 1);

    struct memtable *sources[16];
    uint32_t count;
    build_sources(&e, sources, &count);

    struct lsm_iter iter;
    lsm_iter_init(&iter, sources, count, NULL, 0, NULL, 0);

    const char *expected[] = {"apple", "cherry"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 2, "banana tombstone skipped");

    lsm_iter_destroy(&iter);
    engine_close(&e);
}

static void test_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Across Freeze ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);
    engine_freeze_memtable(&e);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("mango"), 5, v("4"), 1);

    struct memtable *sources[16];
    uint32_t count;
    build_sources(&e, sources, &count);

    struct lsm_iter iter;
    lsm_iter_init(&iter, sources, count, NULL, 0, NULL, 0);

    const char *expected[] = {"apple", "banana", "cherry", "mango"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 4, "iterated 4 entries across freeze");

    lsm_iter_destroy(&iter);
    engine_close(&e);
}

static void test_overwrite_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Overwrite Across Freeze ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("key"), 3, v("old"), 3);
    engine_freeze_memtable(&e);
    engine_put(&e, k("key"), 3, v("new"), 3);

    struct memtable *sources[16];
    uint32_t count;
    build_sources(&e, sources, &count);

    struct lsm_iter iter;
    lsm_iter_init(&iter, sources, count, NULL, 0, NULL, 0);

    EXPECT_COND(lsm_iter_is_valid(&iter), "iter is valid");
    EXPECT_STR_EQ((char *) lsm_iter_key(&iter), "key", "key is correct");
    EXPECT_STR_EQ((char *) lsm_iter_value(&iter), "new", "newest value wins");
    lsm_iter_next(&iter);
    EXPECT_COND(!lsm_iter_is_valid(&iter), "no more entries");

    lsm_iter_destroy(&iter);
    engine_close(&e);
}

static void test_empty(void)
{
    printf(COLOR_BLUE "\n--- Empty ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    struct memtable *sources[16];
    uint32_t count;
    build_sources(&e, sources, &count);

    struct lsm_iter iter;
    lsm_iter_init(&iter, sources, count, NULL, 0, NULL, 0);

    EXPECT_COND(!lsm_iter_is_valid(&iter), "empty iterator is invalid");

    lsm_iter_destroy(&iter);
    engine_close(&e);
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    srand(42);

    test_single_memtable();
    test_lower_upper_bound();
    test_tombstone_skipped();
    test_across_freeze();
    test_overwrite_across_freeze();
    test_empty();

    TEST_SUMMARY();
}