#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "checker.h"
#include "engine.h"
#include "lsm_iter.h"
#include "memtable.h"
#include "util.h"

#define BLOCK_SIZE 4096

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const char *DB_PATH = "/tmp/test_engine_db";

static struct engine_options default_opts(void)
{
    struct engine_options opts = {
        .threshold = 1024, /* 1KB */
        .imm_cap = 4,
        .max_level = 8,
        .block_size = BLOCK_SIZE,
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

static void cleanup_db(void)
{
    /* remove SST files and directory */
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", DB_PATH);
    system(cmd);
}

/*
 * ========================================
 * tests - basic operations
 * ========================================
 */
static void test_open_close(void)
{
    printf(COLOR_BLUE "\n--- Open and Close ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();

    int ret = engine_open(&e, &opts, DB_PATH);
    EXPECT_EQ(ret, 0, "engine_open returns 0");
    EXPECT_EQ(e.imm_count, 0, "imm_count is 0");
    EXPECT_EQ(e.sst_count, 0, "sst_count is 0");
    EXPECT_EQ(e.next_id, 1, "next_id is 1");
    EXPECT_COND(e.memtable != NULL, "memtable allocated");
    EXPECT_COND(e.imm_memtables != NULL, "imm_memtables allocated");
    EXPECT_COND(e.ssts != NULL, "ssts allocated");
    EXPECT_COND(e.db_path != NULL, "db_path set");

    engine_close(&e);
    cleanup_db();
}

static void test_put_and_get(void)
{
    printf(COLOR_BLUE "\n--- Put and Get ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

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
    cleanup_db();
}

static void test_overwrite(void)
{
    printf(COLOR_BLUE "\n--- Overwrite ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("old"), 3);
    engine_put(&e, k("key"), 3, v("new"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "key found");
    EXPECT_STR_EQ((char *) entry->value, "new", "value updated to new");

    engine_close(&e);
    cleanup_db();
}

static void test_delete(void)
{
    printf(COLOR_BLUE "\n--- Delete ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("val"), 3);
    engine_delete(&e, k("key"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "tombstone entry exists");
    EXPECT_EQ(entry->value_len, 0, "value_len is 0 (tombstone)");

    engine_close(&e);
    cleanup_db();
}

/*
 * ========================================
 * tests - freeze
 * ========================================
 */
static void test_freeze(void)
{
    printf(COLOR_BLUE "\n--- Freeze ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("val"), 3);
    uint64_t old_id = e.memtable->id;

    int ret = engine_freeze_memtable(&e);
    EXPECT_EQ(ret, 0, "freeze returns 0");
    EXPECT_EQ(e.imm_count, 1, "imm_count is 1");
    EXPECT_COND(e.memtable->id != old_id, "new memtable has new id");
    EXPECT_EQ(e.memtable->size, 0, "new memtable is empty");

    engine_close(&e);
    cleanup_db();
}

static void test_get_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Get Across Freeze ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_freeze_memtable(&e);
    engine_put(&e, k("banana"), 6, v("2"), 1);

    struct memtable_entry *entry;

    entry = engine_get(&e, k("apple"), 5);
    EXPECT_COND(entry != NULL, "apple found across freeze");
    EXPECT_STR_EQ((char *) entry->value, "1", "apple value correct");

    entry = engine_get(&e, k("banana"), 6);
    EXPECT_COND(entry != NULL, "banana found in mutable");
    EXPECT_STR_EQ((char *) entry->value, "2", "banana value correct");

    engine_close(&e);
    cleanup_db();
}

static void test_overwrite_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Overwrite Across Freeze ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("old"), 3);
    engine_freeze_memtable(&e);
    engine_put(&e, k("key"), 3, v("new"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "key found");
    EXPECT_STR_EQ((char *) entry->value, "new", "new value takes precedence");

    engine_close(&e);
    cleanup_db();
}

static void test_delete_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Delete Across Freeze ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("val"), 3);
    engine_freeze_memtable(&e);
    engine_delete(&e, k("key"), 3);

    struct memtable_entry *entry = engine_get(&e, k("key"), 3);
    EXPECT_COND(entry != NULL, "tombstone found");
    EXPECT_EQ(entry->value_len, 0, "tombstone hides old value");

    engine_close(&e);
    cleanup_db();
}

static void test_auto_freeze(void)
{
    printf(COLOR_BLUE "\n--- Auto Freeze on Threshold ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = {
        .threshold = 10,
        .imm_cap = 4,
        .max_level = 8,
        .block_size = BLOCK_SIZE,
    };
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("aaaaa"), 5, v("bbbbb"), 5);
    engine_put(&e, k("ccccc"), 5, v("ddddd"), 5);

    EXPECT_COND(e.imm_count > 0, "auto freeze triggered");

    engine_close(&e);
    cleanup_db();
}

static void test_freeze_full(void)
{
    printf(COLOR_BLUE "\n--- Freeze Full ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = {
        .threshold = 1024,
        .imm_cap = 2,
        .max_level = 8,
        .block_size = BLOCK_SIZE,
    };
    engine_open(&e, &opts, DB_PATH);

    engine_freeze_memtable(&e);
    engine_freeze_memtable(&e);

    int ret = engine_freeze_memtable(&e);
    EXPECT_EQ(ret, -1, "freeze returns -1 when imm array full");

    engine_close(&e);
    cleanup_db();
}

/*
 * ========================================
 * tests - flush
 * ========================================
 */
static void test_flush_basic(void)
{
    printf(COLOR_BLUE "\n--- Flush Basic ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("111"), 3);
    engine_put(&e, k("banana"), 6, v("222"), 3);
    engine_put(&e, k("cherry"), 6, v("333"), 3);
    engine_freeze_memtable(&e);

    EXPECT_EQ(e.imm_count, 1, "1 immutable before flush");
    EXPECT_EQ(e.sst_count, 0, "0 SSTs before flush");

    int ret = engine_flush(&e);
    EXPECT_EQ(ret, 0, "flush returns 0");
    EXPECT_EQ(e.imm_count, 0, "0 immutables after flush");
    EXPECT_EQ(e.sst_count, 1, "1 SST after flush");

    engine_close(&e);
    cleanup_db();
}

static void test_flush_multiple(void)
{
    printf(COLOR_BLUE "\n--- Flush Multiple ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    /* freeze twice */
    engine_put(&e, k("aaa"), 3, v("111"), 3);
    engine_freeze_memtable(&e);
    engine_put(&e, k("bbb"), 3, v("222"), 3);
    engine_freeze_memtable(&e);

    EXPECT_EQ(e.imm_count, 2, "2 immutables before flush");

    /* flush oldest */
    engine_flush(&e);
    EXPECT_EQ(e.imm_count, 1, "1 immutable after first flush");
    EXPECT_EQ(e.sst_count, 1, "1 SST after first flush");

    /* flush second */
    engine_flush(&e);
    EXPECT_EQ(e.imm_count, 0, "0 immutables after second flush");
    EXPECT_EQ(e.sst_count, 2, "2 SSTs after second flush");

    engine_close(&e);
    cleanup_db();
}

static void test_flush_no_imm(void)
{
    printf(COLOR_BLUE "\n--- Flush No Immutable ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    int ret = engine_flush(&e);
    EXPECT_EQ(ret, -1, "flush returns -1 with no immutables");

    engine_close(&e);
    cleanup_db();
}

static void test_flush_preserves_mutable(void)
{
    printf(COLOR_BLUE "\n--- Flush Preserves Mutable ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("old"), 3, v("imm"), 3);
    engine_freeze_memtable(&e);
    engine_put(&e, k("new"), 3, v("mut"), 3);

    engine_flush(&e);

    /* mutable memtable should still have "new" */
    struct memtable_entry *entry = engine_get(&e, k("new"), 3);
    EXPECT_COND(entry != NULL, "mutable entry survives flush");
    EXPECT_STR_EQ((char *) entry->value, "mut", "mutable value correct");

    engine_close(&e);
    cleanup_db();
}

static void test_flush_with_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Flush With Tombstone ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("111"), 3);
    engine_delete(&e, k("banana"), 6);
    engine_put(&e, k("cherry"), 6, v("333"), 3);
    engine_freeze_memtable(&e);

    int ret = engine_flush(&e);
    EXPECT_EQ(ret, 0, "flush with tombstone returns 0");
    EXPECT_EQ(e.sst_count, 1, "1 SST created");

    engine_close(&e);
    cleanup_db();
}

/*
 * ========================================
 * tests - iterators
 * ========================================
 */
static void test_scan_basic(void)
{
    printf(COLOR_BLUE "\n--- Scan Basic ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);
    engine_put(&e, k("mango"), 5, v("4"), 1);

    struct lsm_iter iter;
    engine_scan(&e, NULL, 0, NULL, 0, &iter);

    const char *expected[] = {"apple", "banana", "cherry", "mango"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 4, "scanned 4 entries");

    lsm_iter_destroy(&iter);
    engine_close(&e);
    cleanup_db();
}

static void test_scan_with_bounds(void)
{
    printf(COLOR_BLUE "\n--- Scan With Bounds ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);
    engine_put(&e, k("mango"), 5, v("4"), 1);

    struct lsm_iter iter;
    engine_scan(&e, k("banana"), 6, k("cherry"), 6, &iter);

    const char *expected[] = {"banana", "cherry"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 2, "scanned 2 entries within bounds");

    lsm_iter_destroy(&iter);
    engine_close(&e);
    cleanup_db();
}

static void test_scan_across_freeze(void)
{
    printf(COLOR_BLUE "\n--- Scan Across Freeze ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);
    engine_freeze_memtable(&e);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_put(&e, k("mango"), 5, v("4"), 1);

    struct lsm_iter iter;
    engine_scan(&e, NULL, 0, NULL, 0, &iter);

    const char *expected[] = {"apple", "banana", "cherry", "mango"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 4, "scanned 4 entries across freeze");

    lsm_iter_destroy(&iter);
    engine_close(&e);
    cleanup_db();
}

static void test_scan_skip_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Scan Skip Tombstone ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("banana"), 6, v("2"), 1);
    engine_delete(&e, k("banana"), 6);
    engine_put(&e, k("cherry"), 6, v("3"), 1);

    struct lsm_iter iter;
    engine_scan(&e, NULL, 0, NULL, 0, &iter);

    const char *expected[] = {"apple", "cherry"};
    int i = 0;
    while (lsm_iter_is_valid(&iter)) {
        EXPECT_STR_EQ((char *) lsm_iter_key(&iter), expected[i], expected[i]);
        i++;
        lsm_iter_next(&iter);
    }
    EXPECT_EQ(i, 2, "tombstone skipped in scan");

    lsm_iter_destroy(&iter);
    engine_close(&e);
    cleanup_db();
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
    test_flush_basic();
    test_flush_multiple();
    test_flush_no_imm();
    test_flush_preserves_mutable();
    test_flush_with_tombstone();
    test_scan_basic();
    test_scan_with_bounds();
    test_scan_across_freeze();
    test_scan_skip_tombstone();

    TEST_SUMMARY();
}