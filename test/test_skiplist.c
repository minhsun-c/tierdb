#include <stdlib.h>
#include <string.h>
#include "checker.h"
#include "skiplist.h"
#include "util.h"

uint32_t total_test = 0;
uint32_t failed_test = 0;

#define MAX_LEVEL 8

/*
 * ========================================
 * test entry — embeds skiplist node
 * ========================================
 */
struct entry {
    int key;
    struct skiplist node;
};

static struct entry *make_entry(int key, uint32_t max_level)
{
    struct entry *e = malloc(sizeof(*e));
    e->key = key;
    skiplist_init(&e->node, max_level);
    return e;
}

static int entry_cmp(struct skiplist *a, struct skiplist *b)
{
    struct entry *ea = container_of(a, struct entry, node);
    struct entry *eb = container_of(b, struct entry, node);
    return ea->key - eb->key;
}

static void free_entry(struct entry *e)
{
    skiplist_free(&e->node);
    free(e);
}

/*
 * ========================================
 * helpers
 * ========================================
 */
static int is_sorted(struct skiplist *sentinel)
{
    struct skiplist *cur = skiplist_next(sentinel);
    while (cur && cur->forward[0]) {
        struct entry *a = container_of(cur, struct entry, node);
        struct entry *b = container_of(cur->forward[0], struct entry, node);
        if (a->key >= b->key)
            return 0;
        cur = cur->forward[0];
    }
    return 1;
}

static int count(struct skiplist *sentinel)
{
    int n = 0;
    struct skiplist *cur = skiplist_next(sentinel);
    while (cur) {
        n++;
        cur = skiplist_next(cur);
    }
    return n;
}

static struct entry *find(struct skiplist *sentinel, int key)
{
    struct skiplist *cur = skiplist_next(sentinel);
    while (cur) {
        struct entry *e = container_of(cur, struct entry, node);
        if (e->key == key)
            return e;
        if (e->key > key)
            return NULL;
        cur = skiplist_next(cur);
    }
    return NULL;
}

/*
 * ========================================
 * tests
 * ========================================
 */
static void test_init(void)
{
    printf(COLOR_BLUE "\n--- Initialization ---\n" COLOR_RESET);
    struct skiplist sentinel;
    skiplist_init(&sentinel, MAX_LEVEL);

    EXPECT_COND(skiplist_next(&sentinel) == NULL,
                "all forward pointers are NULL after init");
}

static void test_insert_single(void)
{
    printf(COLOR_BLUE "\n--- Insert Single ---\n" COLOR_RESET);
    struct skiplist sentinel;
    skiplist_init(&sentinel, MAX_LEVEL);

    struct entry *e = make_entry(42, MAX_LEVEL);
    skiplist_insert(&sentinel, &e->node, entry_cmp);

    EXPECT_EQ(count(&sentinel), 1, "count is 1");
    EXPECT_COND(find(&sentinel, 42), "key 42 is found");
    EXPECT_COND(!find(&sentinel, 99), "key 99 not found");

    free_entry(e);
}

static void test_insert_sorted(void)
{
    printf(COLOR_BLUE "\n--- Insert Sorted Order ---\n" COLOR_RESET);
    struct skiplist sentinel;
    skiplist_init(&sentinel, MAX_LEVEL);

    int keys[] = {30, 10, 50, 20, 40};
    struct entry *entries[5];
    for (int i = 0; i < 5; i++) {
        entries[i] = make_entry(keys[i], MAX_LEVEL);
        skiplist_insert(&sentinel, &entries[i]->node, entry_cmp);
    }

    EXPECT_EQ(count(&sentinel), 5, "count is 5");
    EXPECT_COND(is_sorted(&sentinel), "list is sorted");
    EXPECT_COND(find(&sentinel, 10), "key 10 found");
    EXPECT_COND(find(&sentinel, 30), "key 30 found");
    EXPECT_COND(find(&sentinel, 50), "key 50 found");
    EXPECT_COND(!find(&sentinel, 99), "key 99 not found");

    for (int i = 0; i < 5; i++)
        free_entry(entries[i]);
}

static void test_remove(void)
{
    printf(COLOR_BLUE "\n--- Remove ---\n" COLOR_RESET);
    struct skiplist sentinel;
    skiplist_init(&sentinel, MAX_LEVEL);

    struct entry *a = make_entry(10, MAX_LEVEL);
    struct entry *b = make_entry(20, MAX_LEVEL);
    struct entry *c = make_entry(30, MAX_LEVEL);
    skiplist_insert(&sentinel, &a->node, entry_cmp);
    skiplist_insert(&sentinel, &b->node, entry_cmp);
    skiplist_insert(&sentinel, &c->node, entry_cmp);

    int ret = skiplist_remove(&sentinel, &b->node, entry_cmp);
    EXPECT_EQ(ret, 0, "remove returns 0 on success");
    EXPECT_EQ(count(&sentinel), 2, "count is 2 after remove");
    EXPECT_COND(!find(&sentinel, 20), "key 20 no longer found");
    EXPECT_COND(find(&sentinel, 10), "key 10 still found");
    EXPECT_COND(find(&sentinel, 30), "key 30 still found");
    EXPECT_COND(is_sorted(&sentinel), "still sorted after remove");

    free_entry(a);
    free_entry(b);
    free_entry(c);
}

static void test_remove_not_found(void)
{
    printf(COLOR_BLUE "\n--- Remove Not Found ---\n" COLOR_RESET);
    struct skiplist sentinel;
    skiplist_init(&sentinel, MAX_LEVEL);

    struct entry *a = make_entry(10, MAX_LEVEL);
    struct entry *ghost = make_entry(99, MAX_LEVEL);
    skiplist_insert(&sentinel, &a->node, entry_cmp);

    int ret = skiplist_remove(&sentinel, &ghost->node, entry_cmp);
    EXPECT_EQ(ret, -1, "remove returns -1 when not found");
    EXPECT_EQ(count(&sentinel), 1, "count unchanged");

    free_entry(a);
    free_entry(ghost);
}

static void test_insert_many(void)
{
    printf(COLOR_BLUE "\n--- Insert Many ---\n" COLOR_RESET);
    struct skiplist sentinel;
    skiplist_init(&sentinel, MAX_LEVEL);

    int n = 100;
    struct entry **entries = malloc(n * sizeof(*entries));
    for (int i = 0; i < n; i++) {
        entries[i] = make_entry(i * 3, MAX_LEVEL);
        skiplist_insert(&sentinel, &entries[i]->node, entry_cmp);
    }

    struct entry *first =
        container_of(skiplist_next(&sentinel), struct entry, node);
    EXPECT_EQ(count(&sentinel), 100, "count is 100");
    EXPECT_COND(is_sorted(&sentinel), "list is sorted");
    EXPECT_EQ(first->key, 0, "first key is 0");

    for (int i = 0; i < n; i++)
        free_entry(entries[i]);
    free(entries);
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
    test_insert_single();
    test_insert_sorted();
    test_remove();
    test_remove_not_found();
    test_insert_many();

    TEST_SUMMARY();
}