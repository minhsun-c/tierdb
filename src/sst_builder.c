#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "block.h"
#include "sst.h"
#include "sst_builder.h"
#include "util.h"

int sst_builder_init(struct sst_builder *sb,
                     size_t block_size,
                     const char *path)
{
    if (!sb)
        return -1;

    int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0)
        return -1;

    memset(sb, 0, sizeof(struct sst_builder));
    if (block_builder_init(&sb->bb, block_size) < 0)
        return -1;
    sb->metas = calloc(16, sizeof(struct block_meta));
    if (!sb->metas) {
        block_builder_destroy(&sb->bb);
        return -1;
    }

    sb->fd = fd;
    sb->metas_cap = 16;
    sb->block_size = block_size;
    return 0;
}

void sst_builder_destroy(struct sst_builder *sb)
{
    block_builder_destroy(&sb->bb);

    /* free meta[i] */
    for (uint32_t i = 0; i < sb->n_blocks; i++) {
        free(sb->metas[i].first_key);
        free(sb->metas[i].last_key);
    }
    free(sb->metas);

    free(sb->first_key);
    free(sb->last_key);
    close(sb->fd);
    memset(sb, 0, sizeof(struct sst_builder));
}

static int __sst_builder_flush_block(struct sst_builder *sb)
{
    if (!sb)
        return -1;

    /* struct block_builder -> struct block */
    struct block blk;
    if (block_builder_build(&sb->bb, &blk) < 0)
        return -1;

    /* struct block -> raw bytes */
    size_t raw_byte_len;
    uint8_t *raw_byte = block_encode(&blk, &raw_byte_len);
    if (!raw_byte)
        return -1;
    block_destroy(&blk);

    /* raw bytes -> disk file */
    ssize_t wrret = write(sb->fd, raw_byte, raw_byte_len);
    free(raw_byte);
    if (wrret < 0) {
        return -1;
    }

    /* struct block_meta */
    struct block_meta meta = {.offset = (uint32_t) sb->data_size,
                              .first_key = sb->first_key,
                              .first_key_len = sb->first_key_len,
                              .last_key = sb->last_key,
                              .last_key_len = sb->last_key_len};
    if (sb->n_blocks >= sb->metas_cap) {
        sb->metas_cap *= 2;
        struct block_meta *ext_meta =
            realloc(sb->metas, sb->metas_cap * sizeof(struct block_meta));
        if (!ext_meta)
            return -1;
        sb->metas = ext_meta;
    }
    memcpy(&sb->metas[sb->n_blocks], &meta, sizeof(struct block_meta));

    /* update */
    sb->n_blocks++;
    sb->data_size += raw_byte_len;

    /* reset */
    if (block_builder_init(&sb->bb, sb->block_size) < 0)
        return -1;
    sb->first_key = NULL;
    sb->last_key = NULL;
    sb->first_key_len = 0;
    sb->last_key_len = 0;
    return 0;
}

static int __sst_builder_flush_meta(struct sst_builder *sb)
{
    struct block_meta *m;
    ssize_t wrret;
    for (uint32_t i = 0; i < sb->n_blocks; i++) {
        m = &sb->metas[i];

        /* offset */
        wrret = write(sb->fd, &m->offset, sizeof(m->offset));
        if (wrret < 0 || (size_t) wrret != sizeof(m->offset))
            return -1;

        /* first key */
        wrret = write(sb->fd, &m->first_key_len, sizeof(m->first_key_len));
        if (wrret < 0 || (size_t) wrret != sizeof(m->first_key_len))
            return -1;
        wrret = write(sb->fd, m->first_key, m->first_key_len);
        if (wrret < 0 || (size_t) wrret != m->first_key_len)
            return -1;

        /* last key */
        wrret = write(sb->fd, &m->last_key_len, sizeof(m->last_key_len));
        if (wrret < 0 || (size_t) wrret != sizeof(m->last_key_len))
            return -1;
        wrret = write(sb->fd, m->last_key, m->last_key_len);
        if (wrret < 0 || (size_t) wrret != m->last_key_len)
            return -1;
    }

    uint32_t meta_offset = (uint32_t) sb->data_size;
    wrret = write(sb->fd, &meta_offset, sizeof(meta_offset));
    if (wrret < 0 || (size_t) wrret != sizeof(meta_offset))
        return -1;

    return 0;
}

int sst_builder_add(struct sst_builder *sb,
                    const uint8_t *key,
                    uint16_t key_len,
                    const uint8_t *value,
                    uint16_t value_len)
{
    if (!sb || !key || key_len == 0)
        return -1;

    /* try to add to current block */
    int ret = block_builder_add(&sb->bb, key, key_len, value, value_len);

    if (ret == 1) {
        /* block full — finalize current block */
        if (__sst_builder_flush_block(sb) < 0)
            return -1;

        /* try again on new block */
        ret = block_builder_add(&sb->bb, key, key_len, value, value_len);
    }

    if (ret < 0)
        return -1;

    /* update first_key of current block if this is the first entry */
    if (sb->first_key == NULL) {
        sb->first_key = malloc(key_len);
        if (!sb->first_key)
            return -1;
        memcpy(sb->first_key, key, key_len);
        sb->first_key_len = key_len;
    } else {
        free(sb->last_key);
    }

    /* update last_key */
    sb->last_key = malloc(key_len);
    if (!sb->last_key)
        return -1;
    memcpy(sb->last_key, key, key_len);
    sb->last_key_len = key_len;

    return 0;
}

int sst_builder_build(struct sst_builder *sb,
                      const char *path,
                      uint64_t id,
                      struct sst *sst)
{
    if (!sb || !path || !sst)
        return -1;

    /* flush the final block */
    if (!block_builder_is_empty(&sb->bb)) {
        if (__sst_builder_flush_block(sb) < 0)
            return -1;
    }

    /* write meta & meta_offset */
    if (__sst_builder_flush_meta(sb) < 0)
        return -1;

    /* fd: WRONLY -> RDONLY */
    close(sb->fd);
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;

    sst->fd = fd;

    /* file size */
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }
    sst->file_size = (size_t) st.st_size;

    /* sst_builder -> sst */
    sst->metas = sb->metas;
    sst->n_blocks = sb->n_blocks;
    sst->id = id;
    sst->meta_offset = sb->data_size;

    /* clear states */
    free(sb->first_key);
    free(sb->last_key);
    memset(sb, 0, sizeof(struct sst_builder));
    sb->fd = -1;
    return 0;
}