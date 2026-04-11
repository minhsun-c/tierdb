#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "block.h"
#include "checker.h"
#include "sst.h"
#include "sst_builder.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

static const char *SST_PATH = "/tmp/test_sst.sst";

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

/* helper: build an SST file on disk, then close it */
static void build_sst_file(size_t block_size,
                           const char **keys,
                           const char **vals,
                           uint32_t n)
{
    struct sst_builder sb;
    sst_builder_init(&sb, block_size, SST_PATH);
    for (uint32_t i = 0; i < n; i++) {
        uint16_t kl = (uint16_t) strlen(keys[i]);
        uint16_t vl = vals[i] ? (uint16_t) strlen(vals[i]) : 0;
        sst_builder_add(&sb, k(keys[i]), kl, vals[i] ? v(vals[i]) : NULL, vl);
    }
    struct sst sst;
    sst_builder_build(&sb, SST_PATH, 1, &sst);
    sst_close(&sst);
}

/*
 * ========================================
 * sst_open tests
 * ========================================
 */
static void test_open_single_block(void)
{
    printf(COLOR_BLUE "\n--- SST Open Single Block ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"alpha", "beta", "gamma"};
    const char *vals[] = {"aaa", "bbb", "ggg"};
    build_sst_file(BLOCK_SIZE, keys, vals, 3);

    struct sst sst;
    int ret = sst_open(&sst, SST_PATH, 10);
    EXPECT_EQ(ret, 0, "sst_open returns 0");
    EXPECT_EQ(sst.n_blocks, 1, "1 block");
    EXPECT_EQ(sst.id, 10, "id is 10");
    EXPECT_COND(sst.fd >= 0, "fd is valid");
    EXPECT_COND(sst.file_size > 0, "file_size > 0");
    EXPECT_COND(sst.meta_offset > 0, "meta_offset > 0");

    /* verify meta */
    EXPECT_EQ(sst.metas[0].first_key_len, 5, "first_key_len is 5");
    EXPECT_COND(memcmp(sst.metas[0].first_key, "alpha", 5) == 0,
                "first_key is alpha");
    EXPECT_EQ(sst.metas[0].last_key_len, 5, "last_key_len is 5");
    EXPECT_COND(memcmp(sst.metas[0].last_key, "gamma", 5) == 0,
                "last_key is gamma");

    sst_close(&sst);
    cleanup();
}

static void test_open_multiple_blocks(void)
{
    printf(COLOR_BLUE "\n--- SST Open Multiple Blocks ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc", "ddd", "eee", "fff"};
    const char *vals[] = {"111", "222", "333", "444", "555", "666"};
    build_sst_file(48, keys, vals, 6);

    struct sst sst;
    int ret = sst_open(&sst, SST_PATH, 20);
    EXPECT_EQ(ret, 0, "sst_open returns 0");
    EXPECT_COND(sst.n_blocks > 1, "more than 1 block");
    EXPECT_EQ(sst.id, 20, "id is 20");

    /* verify block offsets increase */
    for (uint32_t i = 1; i < sst.n_blocks; i++) {
        EXPECT_COND(sst.metas[i].offset > sst.metas[i - 1].offset,
                    "block offsets increase");
    }

    sst_close(&sst);
    cleanup();
}

static void test_open_invalid(void)
{
    printf(COLOR_BLUE "\n--- SST Open Invalid ---\n" COLOR_RESET);

    struct sst sst;
    EXPECT_EQ(sst_open(&sst, "/tmp/nonexistent_sst.sst", 1), -1,
              "nonexistent file returns -1");
    EXPECT_EQ(sst_open(NULL, SST_PATH, 1), -1, "NULL sst returns -1");
    EXPECT_EQ(sst_open(&sst, NULL, 1), -1, "NULL path returns -1");
}

/*
 * ========================================
 * sst_read_block tests
 * ========================================
 */
static void test_read_block_single(void)
{
    printf(COLOR_BLUE "\n--- Read Block Single ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"apple", "banana", "cherry"};
    const char *vals[] = {"red", "yellow", "dark"};
    build_sst_file(BLOCK_SIZE, keys, vals, 3);

    struct sst sst;
    sst_open(&sst, SST_PATH, 1);

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

static void test_read_block_all(void)
{
    printf(COLOR_BLUE "\n--- Read All Blocks ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"aaa", "bbb", "ccc", "ddd", "eee", "fff"};
    const char *vals[] = {"111", "222", "333", "444", "555", "666"};
    build_sst_file(48, keys, vals, 6);

    struct sst sst;
    sst_open(&sst, SST_PATH, 1);

    uint32_t total_entries = 0;
    for (uint32_t i = 0; i < sst.n_blocks; i++) {
        struct block *blk = sst_read_block(&sst, i);
        EXPECT_COND(blk != NULL, "block readable");
        EXPECT_COND(blk->n > 0, "block has entries");
        total_entries += blk->n;
        block_destroy(blk);
        free(blk);
    }
    EXPECT_EQ(total_entries, 6, "total 6 entries across all blocks");

    sst_close(&sst);
    cleanup();
}

static void test_read_block_tombstone(void)
{
    printf(COLOR_BLUE "\n--- Read Block Tombstone ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"alive", "dead", "zombie"};
    const char *vals[] = {"yes", NULL, "maybe"};
    build_sst_file(BLOCK_SIZE, keys, vals, 3);

    struct sst sst;
    sst_open(&sst, SST_PATH, 1);

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

static void test_read_block_invalid(void)
{
    printf(COLOR_BLUE "\n--- Read Block Invalid ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"only"};
    const char *vals[] = {"one"};
    build_sst_file(BLOCK_SIZE, keys, vals, 1);

    struct sst sst;
    sst_open(&sst, SST_PATH, 1);

    EXPECT_COND(sst_read_block(&sst, 999) == NULL,
                "out-of-range index returns NULL");
    EXPECT_COND(sst_read_block(NULL, 0) == NULL, "NULL sst returns NULL");

    sst_close(&sst);
    cleanup();
}

/*
 * ========================================
 * sst_close tests
 * ========================================
 */
static void test_close(void)
{
    printf(COLOR_BLUE "\n--- SST Close ---\n" COLOR_RESET);
    cleanup();

    const char *keys[] = {"bar", "foo"};
    const char *vals[] = {"222", "111"};
    build_sst_file(BLOCK_SIZE, keys, vals, 2);

    struct sst sst;
    sst_open(&sst, SST_PATH, 1);
    sst_close(&sst);

    /* after close, everything should be zeroed */
    EXPECT_EQ(sst.n_blocks, 0, "n_blocks is 0 after close");
    EXPECT_COND(sst.metas == NULL, "metas is NULL after close");

    cleanup();
}

/*
 * ========================================
 * main
 * ========================================
 */
int main(void)
{
    /* open tests */
    test_open_single_block();
    test_open_multiple_blocks();
    test_open_invalid();

    /* read block tests */
    test_read_block_single();
    test_read_block_all();
    test_read_block_tombstone();
    test_read_block_invalid();

    /* close tests */
    test_close();

    TEST_SUMMARY();
}