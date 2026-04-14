#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "block.h"
#include "checker.h"
#include "engine.h"
#include "lsm_iter.h"
#include "memtable.h"
#include "util.h"

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

    char buf[256];
    size_t vlen;

    EXPECT_EQ(engine_get(&e, k("apple"), 5, (uint8_t *) buf, 256, &vlen), 0,
              "apple found");
    EXPECT_EQ(vlen, 1, "apple value_len is 1");
    EXPECT_COND(memcmp(buf, "1", 1) == 0, "apple value is 1");

    EXPECT_EQ(engine_get(&e, k("banana"), 6, (uint8_t *) buf, 256, &vlen), 0,
              "banana found");
    EXPECT_COND(memcmp(buf, "2", 1) == 0, "banana value is 2");

    EXPECT_EQ(engine_get(&e, k("mango"), 5, (uint8_t *) buf, 256, &vlen), -1,
              "mango not found");

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

    char buf[256];
    size_t vlen;
    engine_get(&e, k("key"), 3, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "new", 3) == 0, "value updated to new");

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

    size_t vlen;
    char buf[256];
    int ret = engine_get(&e, k("key"), 3, (uint8_t *) buf, 256, &vlen);
    EXPECT_EQ(ret, 0, "tombstone entry found");
    EXPECT_EQ(vlen, 0, "value_len is 0 (tombstone)");

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

    char buf[256];
    size_t vlen;

    engine_get(&e, k("apple"), 5, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "1", 1) == 0, "apple found across freeze");

    engine_get(&e, k("banana"), 6, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "2", 1) == 0, "banana found in mutable");

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

    char buf[256];
    size_t vlen;
    engine_get(&e, k("key"), 3, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "new", 3) == 0, "new value takes precedence");

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

    size_t vlen;
    char buf[256];
    int ret = engine_get(&e, k("key"), 3, (uint8_t *) buf, 256, &vlen);
    EXPECT_EQ(ret, 0, "tombstone found");
    EXPECT_EQ(vlen, 0, "tombstone hides old value");

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

    engine_put(&e, k("aaa"), 3, v("111"), 3);
    engine_freeze_memtable(&e);
    engine_put(&e, k("bbb"), 3, v("222"), 3);
    engine_freeze_memtable(&e);

    EXPECT_EQ(e.imm_count, 2, "2 immutables before flush");

    engine_flush(&e);
    EXPECT_EQ(e.imm_count, 1, "1 immutable after first flush");
    EXPECT_EQ(e.sst_count, 1, "1 SST after first flush");

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

    char buf[256];
    size_t vlen;
    engine_get(&e, k("new"), 3, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "mut", 3) == 0, "mutable entry survives flush");

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
 * tests - get across flush (SST read path)
 * ========================================
 */
static void test_get_from_sst(void)
{
    printf(COLOR_BLUE "\n--- Get From SST ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("red"), 3);
    engine_put(&e, k("banana"), 6, v("yellow"), 6);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    char buf[256];
    size_t vlen;

    EXPECT_EQ(engine_get(&e, k("apple"), 5, (uint8_t *) buf, 256, &vlen), 0,
              "apple found in SST");
    EXPECT_COND(memcmp(buf, "red", 3) == 0, "apple value is red");

    EXPECT_EQ(engine_get(&e, k("banana"), 6, (uint8_t *) buf, 256, &vlen), 0,
              "banana found in SST");
    EXPECT_COND(memcmp(buf, "yellow", 6) == 0, "banana value is yellow");

    EXPECT_EQ(engine_get(&e, k("mango"), 5, (uint8_t *) buf, 256, &vlen), -1,
              "mango not found");

    engine_close(&e);
    cleanup_db();
}

static void test_get_memtable_overrides_sst(void)
{
    printf(COLOR_BLUE "\n--- Get Memtable Overrides SST ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("old"), 3);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    engine_put(&e, k("key"), 3, v("new"), 3);

    char buf[256];
    size_t vlen;
    engine_get(&e, k("key"), 3, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "new", 3) == 0, "memtable value wins over SST");

    engine_close(&e);
    cleanup_db();
}

static void test_delete_hides_sst(void)
{
    printf(COLOR_BLUE "\n--- Delete Hides SST ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("red"), 3);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    engine_delete(&e, k("apple"), 5);

    size_t vlen;
    char buf[256];
    int ret = engine_get(&e, k("apple"), 5, (uint8_t *) buf, 256, &vlen);
    EXPECT_EQ(ret, 0, "tombstone found");
    EXPECT_EQ(vlen, 0, "delete hides SST value");

    engine_close(&e);
    cleanup_db();
}

static void test_get_across_multiple_ssts(void)
{
    printf(COLOR_BLUE "\n--- Get Across Multiple SSTs ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("sst0"), 4);
    engine_put(&e, k("banana"), 6, v("sst0"), 4);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    engine_put(&e, k("banana"), 6, v("sst1"), 4);
    engine_put(&e, k("cherry"), 6, v("sst1"), 4);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    char buf[256];
    size_t vlen;

    engine_get(&e, k("apple"), 5, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "sst0", 4) == 0, "apple from older SST");

    engine_get(&e, k("banana"), 6, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "sst1", 4) == 0, "banana from newer SST");

    engine_get(&e, k("cherry"), 6, (uint8_t *) buf, 256, &vlen);
    EXPECT_COND(memcmp(buf, "sst1", 4) == 0, "cherry from newer SST");

    engine_close(&e);
    cleanup_db();
}

static void test_get_buffer_too_small(void)
{
    printf(COLOR_BLUE "\n--- Get Buffer Too Small ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("longvalue"), 9);

    char buf[4];
    size_t vlen;
    int ret = engine_get(&e, k("key"), 3, (uint8_t *) buf, 4, &vlen);
    EXPECT_EQ(ret, -1, "buffer too small returns -1");
    EXPECT_EQ(vlen, 9, "value_len still set");

    engine_close(&e);
    cleanup_db();
}

/*
 * ========================================
 * tests - scan
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

static void test_scan_across_flush(void)
{
    printf(COLOR_BLUE "\n--- Scan Across Flush ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("1"), 1);
    engine_put(&e, k("cherry"), 6, v("3"), 1);
    engine_freeze_memtable(&e);
    engine_flush(&e);

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
    EXPECT_EQ(i, 4, "scanned 4 entries across flush");

    lsm_iter_destroy(&iter);
    engine_close(&e);
    cleanup_db();
}

static void test_scan_dedup_across_flush(void)
{
    printf(COLOR_BLUE "\n--- Scan Dedup Across Flush ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("key"), 3, v("old"), 3);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    engine_put(&e, k("key"), 3, v("new"), 3);

    struct lsm_iter iter;
    engine_scan(&e, NULL, 0, NULL, 0, &iter);

    EXPECT_COND(lsm_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(lsm_iter_key(&iter), "key", 3) == 0, "key is key");
    EXPECT_COND(memcmp(lsm_iter_value(&iter), "new", 3) == 0,
                "memtable value wins");

    lsm_iter_next(&iter);
    EXPECT_COND(!lsm_iter_is_valid(&iter), "only one entry (dedup)");

    lsm_iter_destroy(&iter);
    engine_close(&e);
    cleanup_db();
}

static void test_scan_tombstone_hides_sst(void)
{
    printf(COLOR_BLUE "\n--- Scan Tombstone Hides SST ---\n" COLOR_RESET);
    cleanup_db();
    struct engine e;
    struct engine_options opts = default_opts();
    engine_open(&e, &opts, DB_PATH);

    engine_put(&e, k("apple"), 5, v("red"), 3);
    engine_put(&e, k("banana"), 6, v("yellow"), 6);
    engine_freeze_memtable(&e);
    engine_flush(&e);

    engine_delete(&e, k("apple"), 5);

    struct lsm_iter iter;
    engine_scan(&e, NULL, 0, NULL, 0, &iter);

    EXPECT_COND(lsm_iter_is_valid(&iter), "valid");
    EXPECT_COND(memcmp(lsm_iter_key(&iter), "banana", 6) == 0,
                "apple hidden, banana visible");

    lsm_iter_next(&iter);
    EXPECT_COND(!lsm_iter_is_valid(&iter), "only banana");

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

    /* basic */
    test_open_close();
    test_put_and_get();
    test_overwrite();
    test_delete();

    /* freeze */
    test_freeze();
    test_get_across_freeze();
    test_overwrite_across_freeze();
    test_delete_across_freeze();
    test_auto_freeze();
    test_freeze_full();

    /* flush */
    test_flush_basic();
    test_flush_multiple();
    test_flush_no_imm();
    test_flush_preserves_mutable();
    test_flush_with_tombstone();

    /* get across flush */
    test_get_from_sst();
    test_get_memtable_overrides_sst();
    test_delete_hides_sst();
    test_get_across_multiple_ssts();
    test_get_buffer_too_small();

    /* scan */
    test_scan_basic();
    test_scan_with_bounds();
    test_scan_across_freeze();
    test_scan_skip_tombstone();
    test_scan_across_flush();
    test_scan_dedup_across_flush();
    test_scan_tombstone_hides_sst();

    TEST_SUMMARY();
}