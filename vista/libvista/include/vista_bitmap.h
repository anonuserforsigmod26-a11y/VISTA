#ifndef __VISTA_BITMAP_H__
#define __VISTA_BITMAP_H__

#include "libvista.h"
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>

#include "vista_seqlock.h"
#include "vista_pg_read_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

#define BITS_PER_WORD (sizeof(unsigned long) * 8)

/*
+--------------------------------------------------+  <- bitmap_base
|               VistaBitmapHeader                  |
+--------------------------------------------------+
|           TableControlBlock (TCB) Pool           |  <- bitmap_base + tcb_pool_offset
|  [TCB_0] [TCB_1] ... [TCB_N]                     |
+--------------------------------------------------+
|                  Bitmap Pool                     |  <- bitmap_base + bitmap_pool_offset
|  [B_0A] [B_0B] [B_1A] [B_1B] ... [B_NA] [B_NB]   |
+--------------------------------------------------+
*/

/* bitmap header */

typedef struct _VistaBitmapHeader {
    uint32_t max_tables;      /* max number of tables */
    _Atomic uint32_t num_current_tables; /* current number of tables */
    uint32_t num_bits_per_bitmap; /* number of bits per bitmap */
    uint32_t bitmap_size_bytes_aligned; /* size of each bitmap in bytes */
    uint64_t tcb_pool_offset; /* offset to TCB pool */
    uint64_t bitmap_pool_offset; /* offset to bitmap pool */
    char Padding[64 - 4 * sizeof(uint32_t) - 2 * sizeof(uint64_t)];
} VistaBitmapHeader;

typedef struct _VistaBitmapTableControlBlock {
    uint32_t table_id;
    bool is_in_use;
    /* padding */
    char _padding[3];
    uint32_t bitmap_idx_A;
    uint32_t bitmap_idx_B;
    _Atomic uint32_t active_idx_flag; /* 0 for A, 1 for B */
    char _padding2[4];
    _Atomic uint64_t version;
    /* padding */
    char Padding[64 - 4 * sizeof(uint32_t) - 2 * sizeof(uint64_t)];
} VistaBitmapTableControlBlock; /* padded to 64 bytes */

/* bitmap struct */
typedef struct {
    unsigned long *words;
    size_t num_bits;
} vista_bitmap_t;

typedef _Atomic(unsigned long) *vista_atomic_bitmap_ptr;

size_t vista_calculate_bitmap_shmem_size(uint32_t max_tables, uint32_t num_bits_per_bitmap);

void vista_bitmap_manager_init(void *bitmap_base, uint32_t max_tables, uint32_t num_bits_per_bitmap);

void* vista_find_and_attach_bitmap_manager(void *shmem_base);

bool vista_add_new_table_to_bitmap(void* bitmap_mgr_base, uint32_t table_id);

bool vista_test_bit_pre_work(void* bitmap_mgr_base, uint32_t table_id, size_t bit_idx, uint64_t* pre_work_version, libvista_BufferPoolControl *ctl);

bool vista_clear_bit_post_work(void* bitmap_mgr_base, uint32_t table_id, size_t bit_idx, uint64_t pre_work_version, libvista_BufferPoolControl *ctl);

void vista_non_atomic_set_bit(vista_bitmap_t *bm, size_t bit_idx);

void vista_non_atomic_bitmap_init(vista_bitmap_t *bm, void *mem, size_t num_bits);

vista_atomic_bitmap_ptr vista_get_bitmap_by_idx(void* bitmap_mgr_base, uint32_t bitmap_idx);

VistaBitmapTableControlBlock* vista_get_tcb_by_table_id(void* bitmap_mgr_base, uint32_t table_id);

VistaBitmapTableControlBlock* vista_init_and_get_tcb_by_table_id(void* bitmap_mgr_base, uint32_t table_id);

#ifdef __cplusplus
}
#endif

#endif // __VISTA_BITMAP_H__
