#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "block.h"
#include "checker.h"
#include "sst.h"
#include "sst_builder.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const char *SST_PATH = "/tmp/test_sst_builder.sst";

static const uint8_t *k(const char *s)
{
    return (const uint8_t *) s;
}
static const uint8_t *v(const char *s)
{
    return (const uint8_t *) s;
}

static void block_entry_at(const struct block *blk,
                           uint16_t i,
                           const uint8_t **key,
                           uint16_t *key_len,
                           const uint8_t **value,
                           uint16_t *value_len)
{
    uint16_t off = blk->offsets[i];
    const uint8_t *p = blk->data + off;
    memcpy(key_len, p, 2);
    p += 2;
    *key = p;
    p += *key_len;
    memcpy(value_len, p, 2);
    p += 2;
    *value = p;
}

static void cleanup(void)
{
    unlink(SST_PATH);
}

/*
 * ========================================
 * init tests
 * ========================================
 */
static void test_init(void)
{
    printf(COLOR_BLUE "\n--- Builder Init ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    int ret = sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);
    EXPECT_EQ(ret, 0, "init returns 0");
    EXPECT_COND(sb.fd >= 0, "fd is valid");
    EXPECT_EQ(sb.n_blocks, 0, "n_blocks is 0");
    EXPECT_EQ(sb.data_size, 0, "data_size is 0");
    EXPECT_EQ(sb.block_size, BLOCK_SIZE, "block_size matches");
    EXPECT_COND(sb.first_key == NULL, "first_key is NULL");
    EXPECT_COND(sb.last_key == NULL, "last_key is NULL");

    sst_builder_destroy(&sb);
    cleanup();
}

static void test_init_invalid(void)
{
    printf(COLOR_BLUE "\n--- Builder Init Invalid ---\n" COLOR_RESET);

    EXPECT_EQ(sst_builder_init(NULL, BLOCK_SIZE, SST_PATH), -1,
              "NULL builder returns -1");
}

/*
 * ========================================
 * add tests
 * ========================================
 */
static void test_add_single(void)
{
    printf(COLOR_BLUE "\n--- Builder Add Single ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);

    int ret = sst_builder_add(&sb, k("apple"), 5, v("red"), 3);
    EXPECT_EQ(ret, 0, "add returns 0");
    EXPECT_COND(sb.first_key != NULL, "first_key set");
    EXPECT_COND(memcmp(sb.first_key, "apple", 5) == 0, "first_key is apple");
    EXPECT_COND(sb.last_key != NULL, "last_key set");
    EXPECT_COND(memcmp(sb.last_key, "apple", 5) == 0, "last_key is apple");

    sst_builder_destroy(&sb);
    cleanup();
}

static void test_add_multiple(void)
{
    printf(COLOR_BLUE "\n--- Builder Add Multiple ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);

    sst_builder_add(&sb, k("apple"), 5, v("1"), 1);
    sst_builder_add(&sb, k("banana"), 6, v("2"), 1);
    sst_builder_add(&sb, k("cherry"), 6, v("3"), 1);

    EXPECT_COND(memcmp(sb.first_key, "apple", 5) == 0, "first_key is apple");
    EXPECT_COND(memcmp(sb.last_key, "cherry", 6) == 0, "last_key is cherry");

    sst_builder_destroy(&sb);
    cleanup();
}

static void test_add_invalid(void)
{
    printf(COLOR_BLUE "\n--- Builder Add Invalid ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);

    EXPECT_EQ(sst_builder_add(&sb, NULL, 5, v("1"), 1), -1,
              "NULL key returns -1");
    EXPECT_EQ(sst_builder_add(&sb, k("a"), 0, v("1"), 1), -1,
              "zero key_len returns -1");
    EXPECT_EQ(sst_builder_add(NULL, k("a"), 1, v("1"), 1), -1,
              "NULL builder returns -1");

    sst_builder_destroy(&sb);
    cleanup();
}

static void test_estimated_size(void)
{
    printf(COLOR_BLUE "\n--- Builder Estimated Size ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);

    EXPECT_EQ(sst_builder_estimated_size(&sb), 0, "initial size is 0");

    sst_builder_destroy(&sb);
    cleanup();
}

/*
 * ========================================
 * build tests
 * ========================================
 */
static void test_build_single_block(void)
{
    printf(COLOR_BLUE "\n--- Build Single Block ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);

    sst_builder_add(&sb, k("apple"), 5, v("red"), 3);
    sst_builder_add(&sb, k("banana"), 6, v("yellow"), 6);
    sst_builder_add(&sb, k("cherry"), 6, v("dark"), 4);

    struct sst sst;
    int ret = sst_builder_build(&sb, SST_PATH, 1, &sst);
    EXPECT_EQ(ret, 0, "build returns 0");
    EXPECT_EQ(sst.n_blocks, 1, "1 block");
    EXPECT_EQ(sst.id, 1, "id is 1");
    EXPECT_COND(sst.fd >= 0, "fd is valid");
    EXPECT_COND(sst.file_size > 0, "file_size > 0");
    EXPECT_COND(sst.meta_offset > 0, "meta_offset > 0");

    /* verify meta */
    EXPECT_EQ(sst.metas[0].offset, 0, "block 0 offset is 0");
    EXPECT_EQ(sst.metas[0].first_key_len, 5, "first_key_len is 5");
    EXPECT_COND(memcmp(sst.metas[0].first_key, "apple", 5) == 0,
                "first_key is apple");
    EXPECT_EQ(sst.metas[0].last_key_len, 6, "last_key_len is 6");
    EXPECT_COND(memcmp(sst.metas[0].last_key, "cherry", 6) == 0,
                "last_key is cherry");

    /* read block and verify entries */
    struct block *blk = sst_read_block(&sst, 0);
    EXPECT_COND(blk != NULL, "read_block returns non-NULL");
    EXPECT_EQ(blk->n, 3, "block has 3 entries");

    const uint8_t *key, *val;
    uint16_t key_len, val_len;

    block_entry_at(blk, 0, &key, &key_len, &val, &val_len);
    EXPECT_EQ(key_len, 5, "entry[0] key_len");
    EXPECT_COND(memcmp(key, "apple", 5) == 0, "entry[0] key is apple");
    EXPECT_EQ(val_len, 3, "entry[0] val_len");
    EXPECT_COND(memcmp(val, "red", 3) == 0, "entry[0] val is red");

    block_entry_at(blk, 1, &key, &key_len, &val, &val_len);
    EXPECT_COND(memcmp(key, "banana", 6) == 0, "entry[1] key is banana");
    EXPECT_COND(memcmp(val, "yellow", 6) == 0, "entry[1] val is yellow");

    block_entry_at(blk, 2, &key, &key_len, &val, &val_len);
    EXPECT_COND(memcmp(key, "cherry", 6) == 0, "entry[2] key is cherry");
    EXPECT_COND(memcmp(val, "dark", 4) == 0, "entry[2] val is dark");

    block_destroy(blk);
    free(blk);
    sst_close(&sst);
    cleanup();
}

static void test_build_multiple_blocks(void)
{
    printf(COLOR_BLUE "\n--- Build Multiple Blocks ---\n" COLOR_RESET);
    cleanup();

    size_t small_block = 48;
    struct sst_builder sb;
    sst_builder_init(&sb, small_block, SST_PATH);

    sst_builder_add(&sb, k("apple"), 5, v("11111"), 5);
    sst_builder_add(&sb, k("banana"), 6, v("22222"), 5);
    sst_builder_add(&sb, k("cherry"), 6, v("33333"), 5);
    sst_builder_add(&sb, k("date"), 4, v("44444"), 5);
    sst_builder_add(&sb, k("elderberry"), 10, v("55555"), 5);

    struct sst sst;
    int ret = sst_builder_build(&sb, SST_PATH, 2, &sst);
    EXPECT_EQ(ret, 0, "build returns 0");
    EXPECT_COND(sst.n_blocks > 1, "more than 1 block");
    EXPECT_EQ(sst.id, 2, "id is 2");

    /* verify block offsets are monotonically increasing */
    for (uint32_t i = 1; i < sst.n_blocks; i++) {
        EXPECT_COND(sst.metas[i].offset > sst.metas[i - 1].offset,
                    "block offsets increase");
    }

    /* verify all blocks are readable */
    for (uint32_t i = 0; i < sst.n_blocks; i++) {
        struct block *blk = sst_read_block(&sst, i);
        EXPECT_COND(blk != NULL, "block readable");
        EXPECT_COND(blk->n > 0, "block has entries");
        block_destroy(blk);
        free(blk);
    }

    sst_close(&sst);
    cleanup();
}

static void test_build_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Build with Tombstone ---\n" COLOR_RESET);
    cleanup();

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);

    sst_builder_add(&sb, k("alive"), 5, v("yes"), 3);
    sst_builder_add(&sb, k("dead"), 4, NULL, 0);
    sst_builder_add(&sb, k("zombie"), 6, v("maybe"), 5);

    struct sst sst;
    int ret = sst_builder_build(&sb, SST_PATH, 3, &sst);
    EXPECT_EQ(ret, 0, "build returns 0");

    struct block *blk = sst_read_block(&sst, 0);
    EXPECT_COND(blk != NULL, "read_block returns non-NULL");

    const uint8_t *key, *val;
    uint16_t key_len, val_len;

    block_entry_at(blk, 0, &key, &key_len, &val, &val_len);
    EXPECT_COND(memcmp(key, "alive", 5) == 0, "entry[0] key is alive");
    EXPECT_EQ(val_len, 3, "entry[0] has value");

    block_entry_at(blk, 1, &key, &key_len, &val, &val_len);
    EXPECT_COND(memcmp(key, "dead", 4) == 0, "entry[1] key is dead");
    EXPECT_EQ(val_len, 0, "entry[1] is tombstone");

    block_entry_at(blk, 2, &key, &key_len, &val, &val_len);
    EXPECT_COND(memcmp(key, "zombie", 6) == 0, "entry[2] key is zombie");
    EXPECT_EQ(val_len, 5, "entry[2] has value");

    block_destroy(blk);
    free(blk);
    sst_close(&sst);
    cleanup();
}

static void test_build_invalid(void)
{
    printf(COLOR_BLUE "\n--- Build Invalid ---\n" COLOR_RESET);

    EXPECT_EQ(sst_builder_build(NULL, SST_PATH, 1, &(struct sst) {0}), -1,
              "NULL builder returns -1");

    struct sst_builder sb;
    sst_builder_init(&sb, BLOCK_SIZE, SST_PATH);
    EXPECT_EQ(sst_builder_build(&sb, NULL, 1, &(struct sst) {0}), -1,
              "NULL path returns -1");
    sst_builder_destroy(&sb);

    cleanup();
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    test_init();
    test_init_invalid();
    test_add_single();
    test_add_multiple();
    test_add_invalid();
    test_estimated_size();
    test_build_single_block();
    test_build_multiple_blocks();
    test_build_tombstone();
    test_build_invalid();

    TEST_SUMMARY();
}