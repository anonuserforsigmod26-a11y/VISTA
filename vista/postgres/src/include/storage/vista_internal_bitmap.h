#ifndef VISTA_INTERNAL_BITMAP_H
#define VISTA_INTERNAL_BITMAP_H

#define MAX_TABLES 64 /* maximum number of tables a cache bitmap can handle */
#define MAX_TABLE_SIZE (1024UL*1024*1024*1024) /* maximum table size a cache bitmap can handle (in bytes) - 1TB */
#define CACHEBLOCK_SIZE (16*1024*1024) /* 16MB cache block size */
#define NUM_CACHEBLOCKS (MAX_TABLE_SIZE / CACHEBLOCK_SIZE) /* number of cache blocks per table == number of bits per bitmap */
#define NUM_BUFFERS_PER_CACHEBLOCK (CACHEBLOCK_SIZE / BLCKSZ) /* number of Postgres buffers per cache block */

#endif /* VISTA_INTERNAL_BITMAP_H */