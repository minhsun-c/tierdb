#include <stdlib.h>
#include <string.h>
#include "checker.h"
#include "engine.h"
#include "memtable.h"
#include "util.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static struct engine_options default_opts(void)
{
    struct engine_options opts = {
        .threshold = 1024, /* 1KB */
        .imm_cap = 4,
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

/*
 * ========================================
 * tests
 * ========================================
 */
static void test_open_close(void)
{
    printf(COLOR_BLUE "\n--- Open and Close ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();

    int ret = engine_open(&e, &opts);
    EXPECT_EQ(ret, 0, "engine_open returns 0");
    EXPECT_EQ(e.imm_count, 0, "imm_count is 0");
    EXPECT_EQ(e.next_id, 1, "next_id is 1");
    EXPECT_COND(e.memtable != NULL, "memtable allocated");
    EXPECT_COND(e.imm_memtables != NULL, "imm_memtables allocated");

    engine_close(&e);
}

static void test_put_and_get(void)
{
    printf(COLOR_BLUE "\n--- Put and Get ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);

    struct memtable_entry *entry;

    entry = engine_get(&e, k("apple"), 5);
    EXPECT_COND(entry != NULL, "apple found");
    EXPECT_STR_EQ((char *) entry->value, "1", "apple value is 1");

    entry = engine_get(&e, k("banana"), 6);
    EXPECT_COND(entry != NULL, "banana found");
    EXPECT_STR_EQ((char *) entry->value, "2", "banana value is 2");

    entry = engine_get(&e, k("mango"), 5);
    EXPECT_COND(entry == NULL, "mango not found");

    engine_close(&e);
}

static void test_overwrite(void)
{
    printf(COLOR_BLUE "\n--- Overwrite ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("key"), 3, v("old"), 3);
    engine_put(&e, k("key"), 3, v("new"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "key found");
    EXPECT_STR_EQ((char *) entry->value, "new", "value updated to new");

    engine_close(&e);
}

static void test_delete(void)
{
    printf(COLOR_BLUE "\n--- Delete ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("key"), 3, v("val"), 3);
    engine_delete(&e, k("key"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "tombstone entry exists");
    EXPECT_EQ(entry->value_len, 0, "value_len is 0 (tombstone)");

    engine_close(&e);
}

static void test_freeze(void)
{
    printf(COLOR_BLUE "\n--- Freeze ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("key"), 3, v("val"), 3);
    uint64_t old_id = e.memtable->id;

    int ret = engine_freeze_memtable(&e);
    EXPECT_EQ(ret, 0, "freeze returns 0");
    EXPECT_EQ(e.imm_count, 1, "imm_count is 1");
    EXPECT_COND(e.memtable->id != old_id, "new memtable has new id");
    EXPECT_EQ(e.memtable->size, 0, "new memtable is empty");

    engine_close(&e);
}

static void test_get_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Get Across Freeze ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    /* put before freeze */
    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_freeze_memtable(&e);

    /* put after freeze */
    engine_put(&e, k("banana"), 6, v("2"), 1);

    struct memtable_entry *entry;

    entry = engine_get(&e, k("apple"), 5);
    EXPECT_COND(entry != NULL, "apple found across freeze");
    EXPECT_STR_EQ((char *) entry->value, "1", "apple value correct");

    entry = engine_get(&e, k("banana"), 6);
    EXPECT_COND(entry != NULL, "banana found in mutable");
    EXPECT_STR_EQ((char *) entry->value, "2", "banana value correct");

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

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "key found");
    EXPECT_STR_EQ((char *) entry->value, "new", "new value takes precedence");

    engine_close(&e);
}

static void test_delete_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Delete Across Freeze ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts);

    engine_put(&e, k("key"), 3, v("val"), 3);
    engine_freeze_memtable(&e);
    engine_delete(&e, k("key"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "tombstone found");
    EXPECT_EQ(entry->value_len, 0, "tombstone hides old value");

    engine_close(&e);
}

static void test_auto_freeze(void)
{
    printf(COLOR_BLUE "\n--- Auto Freeze on Threshold ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = {
        .threshold = 10, /* very small threshold */
        .imm_cap = 4,
        .max_level = 8,
    };
    engine_open(&e, &opts);

    /* each put adds ~10 bytes, should trigger freeze */
    engine_put(&e, k("aaaaa"), 5, v("bbbbb"), 5);
    engine_put(&e, k("ccccc"), 5, v("ddddd"), 5);

    EXPECT_COND(e.imm_count > 0, "auto freeze triggered");

    engine_close(&e);
}

static void test_freeze_full(void)
{
    printf(COLOR_BLUE "\n--- Freeze Full ---\n" COLOR_RESET);
    struct engine e;
    struct engine_options opts = {
        .threshold = 1024,
        .imm_cap = 2,
        .max_level = 8,
    };
    engine_open(&e, &opts);

    engine_freeze_memtable(&e);
    engine_freeze_memtable(&e);

    int ret = engine_freeze_memtable(&e);
    EXPECT_EQ(ret, -1, "freeze returns -1 when imm array full");

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

    test_open_close();
    test_put_and_get();
    test_overwrite();
    test_delete();
    test_freeze();
    test_get_across_freeze();
    test_overwrite_across_freeze();
    test_delete_across_freeze();
    test_auto_freeze();
    test_freeze_full();

    TEST_SUMMARY();
}