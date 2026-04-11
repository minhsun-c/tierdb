#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "block.h"
#include "sst.h"
#include "util.h"

static int __sst_read_raw(int fd, uint32_t start, uint32_t size, uint8_t *raw)
{
    if (fd < 0 || raw == NULL)
        return -1;
    if (lseek(fd, start, SEEK_SET) < 0)  // file start + start
        return -1;
    ssize_t rdret = read(fd, raw, (size_t) size);
    if (rdret < 0 || (size_t) rdret != size)
        return -1;
    return 0;
}

static int __sst_open_file(const char *path, size_t *fsize)
{
    int fd = open(path, O_RDONLY);
    if (fd < 0)
        return -1;
    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return -1;
    }
    *fsize = (size_t) st.st_size;
    return fd;
}

static int __sst_read_meta_offset(int fd, uint32_t *meta_offset)
{
    if (fd < 0 || meta_offset == NULL)
        return -1;
    if (lseek(fd, -(off_t) sizeof(*meta_offset), SEEK_END) < 0)
        return -1;
    ssize_t rdret = read(fd, meta_offset, sizeof(*meta_offset));
    if (rdret < 0 || (size_t) rdret != sizeof(*meta_offset))
        return -1;
    return 0;
}

static int __sst_read_meta(int fd,
                           uint32_t meta_offset,
                           uint32_t meta_size,
                           uint8_t *raw_meta)
{
    return __sst_read_raw(fd, meta_offset, meta_size, raw_meta);
}

static uint32_t __sst_count_meta_blk(uint8_t *raw_meta, uint32_t meta_size)
{
    uint8_t *p = raw_meta;
    uint8_t *end = raw_meta + meta_size;
    uint32_t blk_cnt = 0;
    uint16_t fk_len, lk_len;
    while (p < end) {
        // [ offset(4B) | fk_len(2B) | fk | lk_len(2B) | lk ]
        p += sizeof(uint32_t);
        memcpy(&fk_len, p, sizeof(uint16_t));
        p += sizeof(fk_len) + fk_len;
        memcpy(&lk_len, p, sizeof(uint16_t));
        p += sizeof(lk_len) + lk_len;
        blk_cnt++;
    }
    return blk_cnt;
}

static int __sst_decode_meta(uint8_t *raw_meta,
                             uint32_t meta_size,
                             struct block_meta *metas,
                             size_t blk_meta_size)
{
    uint8_t *p = raw_meta;
    uint8_t *end = raw_meta + meta_size;
    uint32_t idx = 0;
    uint16_t fk_len, lk_len;
    uint32_t offset;
    while (p < end) {
        // [ offset(4B) | fk_len(2B) | fk | lk_len(2B) | lk ]

        /* offset */
        memcpy(&offset, p, sizeof(uint32_t));
        metas[idx].offset = offset;
        p += sizeof(uint32_t);

        /* first key */
        memcpy(&fk_len, p, sizeof(uint16_t));
        metas[idx].first_key_len = fk_len;
        p += sizeof(fk_len);
        metas[idx].first_key = malloc(sizeof(uint8_t) * fk_len);
        if (!metas[idx].first_key)
            goto SST_DECODE_FREE_KEY;
        memcpy(metas[idx].first_key, p, sizeof(uint8_t) * fk_len);
        p += fk_len;

        /* last key */
        memcpy(&lk_len, p, sizeof(uint16_t));
        metas[idx].last_key_len = lk_len;
        p += sizeof(lk_len);
        metas[idx].last_key = malloc(sizeof(uint8_t) * lk_len);
        if (!metas[idx].last_key) {
            free(metas[idx].first_key);
            goto SST_DECODE_FREE_KEY;
        }
        memcpy(metas[idx].last_key, p, sizeof(uint8_t) * lk_len);
        p += lk_len;

        idx++;
    }
    return 0;
SST_DECODE_FREE_KEY:
    for (uint32_t i = 0; i < idx; i++) {
        free(metas[i].first_key);
        free(metas[i].last_key);
    }
    memset(metas, 0, blk_meta_size);
    return -1;
}

int sst_open(struct sst *sst, const char *path, uint64_t id)
{
    if (!sst || !path)
        return -1;

    /* shared variables */
    int ret;

    /* open file */
    size_t fsize;
    int fd = __sst_open_file(path, &fsize);
    if (fd < 0)
        return -1;

    /* read meta_offset */
    uint32_t meta_offset;
    ret = __sst_read_meta_offset(fd, &meta_offset);
    if (ret < 0)
        goto SST_OPEN_CLOSE_FD;

    /* read meta section */
    uint32_t meta_size = fsize - meta_offset - sizeof(meta_offset);
    uint8_t *raw_metas = malloc(meta_size);
    if (!raw_metas)
        goto SST_OPEN_CLOSE_FD;
    ret = __sst_read_meta(fd, meta_offset, meta_size, raw_metas);
    if (ret < 0)
        goto SST_OPEN_FREE_RAW_META;

    /* decode meta section */
    uint32_t num_block = __sst_count_meta_blk(raw_metas, meta_size);
    size_t blk_meta_size = num_block * sizeof(struct block_meta);
    struct block_meta *metas = calloc(num_block, sizeof(struct block_meta));
    if (!metas)
        goto SST_OPEN_FREE_RAW_META;
    ret = __sst_decode_meta(raw_metas, meta_size, metas, blk_meta_size);
    if (ret < 0)
        goto SST_OPEN_FREE_META;

    sst->fd = fd;
    sst->file_size = fsize;
    sst->id = id;
    sst->metas = metas;
    sst->n_blocks = num_block;
    sst->meta_offset = meta_offset;

    free(raw_metas);
    return 0;

SST_OPEN_FREE_META:
    free(metas);
SST_OPEN_FREE_RAW_META:
    free(raw_metas);
SST_OPEN_CLOSE_FD:
    close(fd);
    return -1;
}

void sst_close(struct sst *sst)
{
    if (!sst)
        return;
    for (uint32_t i = 0; i < sst->n_blocks; i++) {
        free(sst->metas[i].first_key);
        free(sst->metas[i].last_key);
    }
    memset(sst->metas, 0, sst->n_blocks * sizeof(struct block_meta));
    free(sst->metas);
    close(sst->fd);
    memset(sst, 0, sizeof(struct sst));
}

struct block *sst_read_block(const struct sst *sst, uint32_t block_idx)
{
    if (!sst || block_idx >= sst->n_blocks)
        return NULL;

    /* get data size */
    uint32_t start = sst->metas[block_idx].offset;
    uint32_t end = (block_idx + 1 < sst->n_blocks)
                       ? sst->metas[block_idx + 1].offset
                       : sst->meta_offset;
    uint32_t data_size = end - start;

    /* read data */
    uint8_t *raw_data = malloc(data_size);
    if (!raw_data)
        return NULL;
    int ret = __sst_read_raw(sst->fd, start, data_size, raw_data);
    if (ret < 0)
        goto SST_READ_FREE_DATA;

    /* raw data -> block */
    struct block *blk = malloc(sizeof(struct block));
    if (!blk)
        goto SST_READ_FREE_DATA;

    if (block_decode(blk, raw_data, data_size) < 0)
        goto SST_READ_FREE_BLK;

    return blk;

SST_READ_FREE_BLK:
    free(blk);
SST_READ_FREE_DATA:
    free(raw_data);
    return NULL;
}
