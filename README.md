# tierdb

A lightweight LSM-tree key-value storage engine written in C.

## Overview

tierdb is a from-scratch implementation of an LSM-tree (Log-Structured Merge-tree) storage engine, inspired by [mini-lsm](https://github.com/skyzh/mini-lsm). It is built as a learning project to understand the internals of modern key-value stores like LevelDB and RocksDB.

## Architecture

```
engine
├── memtable          — mutable in-memory write buffer (skiplist)
├── imm_memtables[]   — frozen immutable memtables, awaiting flush
└── sstables[]        — on-disk sorted string tables
```

## Features

- **Skiplist-based memtable** — intrusive linked node design inspired by Linux `list_head`
- **Memtable iterator** — seek-based iteration over a single memtable via skiplist traversal
- **Write path** — `put`, `delete` (tombstone), automatic freeze on size threshold
- **Read path** — `get` searches memtables newest-first
- **Scan** — sorted range iteration via k-way merge iterator over all memtables
- **Block** — compact binary encoding for on-disk key-value storage, 4KB aligned
- **Block iterator** — seek-based access with `seek_first` and `seek_key` (first key >= target)
- **SSTable** — immutable on-disk sorted table with block-based data and metadata index
- **SST builder** — streaming construction with automatic block splitting and incremental disk writes
- **SST iterator** — seek-based iteration over an entire SST with automatic cross-block loading
- **Unified iterator interface** — generic cursor with function pointers, allowing type-erased iteration over any source
- **Merge iterator** — k-way merge over mixed memtable and SST iterators with dedup by source priority
- **L0 flush** — writes frozen immutable memtables to SST files on disk

## Building

```bash
make        # build all
make test   # run all tests
make skiplist   # run skiplist tests only
make memtable   # run memtable tests only
make mt_iter    # run memtable iterator tests only
make engine     # run engine tests only
make lsm_iter   # run iterator tests only
make block      # run block tests only
make block_iter # run block iterator tests only
make sst_builder    # run SSTable builder tests only
make sst            # run SSTable tests only
make sst_iter       # run SSTable iterator tests only
make merge_iter     # run merge iterator tests only
```

## Usage

```c
struct engine e;
struct engine_options opts = {
    .threshold = 64 * 1024 * 1024, /* 64 MB */
    .imm_cap = 4,
    .max_level = 16,
    .block_size = 4096,
};
engine_open(&e, &opts, "/tmp/mydb");

engine_put(&e, (uint8_t *) "hello", 5, (uint8_t *) "world", 5);

struct memtable_entry *entry = engine_get(&e, (uint8_t *) "hello", 5);
if (entry && entry->value_len > 0)
    printf("%.*s\n", (int) entry->value_len, entry->value);

/* range scan */
struct lsm_iter iter;
engine_scan(&e, NULL, 0, NULL, 0, &iter);
while (lsm_iter_is_valid(&iter)) {
    printf("%.*s\n", (int) lsm_iter_key_len(&iter), lsm_iter_key(&iter));
    lsm_iter_next(&iter);
}
lsm_iter_destroy(&iter);

engine_close(&e);
```

## Roadmap

- [x] Skiplist
- [x] Memtable
- [x] Engine (put / get / delete / scan)
- [x] Freeze memtable
- [x] Block (on-disk storage unit)
- [x] Block iterator
- [x] SSTable (flush to disk)
- [x] SSTable iterator
- [x] Merge iterator (k-way merge over memtables and SSTs)
- [x] L0 flush (memtable → SST)
- [ ] Bloom filter (skip SSTables that cannot contain a key)
- [ ] Compaction
- [ ] WAL (crash recovery)
- [ ] Block cache

## References

- [mini-lsm](https://github.com/skyzh/mini-lsm) by Alex Chi Z
