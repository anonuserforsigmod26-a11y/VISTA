#include "include/vista_bitmap.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

/* Calculate the shared memory size required for the bitmap 
 * Arguments: max_tables - the maximum number of tables
 *            num_bits_per_bitmap - the number of bits per bitmap
 * Returns: the total size in bytes
*/
size_t vista_calculate_bitmap_shmem_size(uint32_t max_tables, uint32_t num_bits_per_bitmap)
{
    size_t total_size = 0;

    /* header */
    total_size += sizeof(VistaBitmapHeader);

    total_size += max_tables * sizeof(VistaBitmapTableControlBlock);

    size_t num_words_per_bitmap = (num_bits_per_bitmap + BITS_PER_WORD - 1) / BITS_PER_WORD;
    size_t single_bitmap_size = num_words_per_bitmap * sizeof(unsigned long);
    size_t aligned_bitmap_size = VISTA_CACHELINE_ALIGN(single_bitmap_size);   
    size_t bitmap_pool_size = max_tables * 2 * aligned_bitmap_size; // 2 bitmaps per table (A and B)

    total_size += bitmap_pool_size;

    return total_size;
}

/* Initialize the bitmap manager 
 * Arguments: bitmap_base - the base address of the allocated shared memory for the bitmap
 *            max_tables - the maximum number of tables
 *            num_bits_per_bitmap - the number of bits per bitmap
 * Returns: void
*/
void vista_bitmap_manager_init(void *bitmap_base, uint32_t max_tables, uint32_t num_bits_per_bitmap)
{
    VistaBitmapHeader *header = (VistaBitmapHeader *)bitmap_base;
    header->max_tables = max_tables;
    header->num_current_tables = 0;
    header->num_bits_per_bitmap = num_bits_per_bitmap;
    header->bitmap_size_bytes_aligned = VISTA_CACHELINE_ALIGN((num_bits_per_bitmap + BITS_PER_WORD - 1) / BITS_PER_WORD * sizeof(unsigned long));
    header->tcb_pool_offset = sizeof(VistaBitmapHeader);
    header->bitmap_pool_offset = header->tcb_pool_offset + max_tables * sizeof(VistaBitmapTableControlBlock);

    // Initialize TCBs
    VistaBitmapTableControlBlock *tcb_pool = (VistaBitmapTableControlBlock *)((char *)bitmap_base + header->tcb_pool_offset);
    for (uint32_t i = 0; i < max_tables; i++) {
        tcb_pool[i].table_id = 0;
        tcb_pool[i].is_in_use = false;
        tcb_pool[i].bitmap_idx_A = i * 2;     // A bitmap index
        tcb_pool[i].bitmap_idx_B = i * 2 + 1; // B bitmap index
        atomic_store(&tcb_pool[i].active_idx_flag, 0); // Start with A as active
        atomic_store(&tcb_pool[i].version, 0);
    }

    // Initialize bitmaps to zero
    unsigned long *bitmap_pool = (unsigned long *)((char *)bitmap_base + header->bitmap_pool_offset);
    size_t total_bitmaps = max_tables * 2;
    memset(bitmap_pool, 0xFF, total_bitmaps * header->bitmap_size_bytes_aligned);
}

/* Find and attach the bitmap manager 
 * Arguments: shmem_base - the base address of the shared memory region
 * Returns: pointer to the bitmap manager, or NULL if not found
 */
void* vista_find_and_attach_bitmap_manager(void *shmem_base)
{
    /* assumes that the offset struct is at the beginning of the shared memory */
    uint32_t aligned_offset = *((int*)shmem_base);
    vista_bufferpool_offsets *offsets = (vista_bufferpool_offsets *)((char*)shmem_base + aligned_offset);
    
    if (offsets == NULL)
    {
        fprintf(stderr, "[libvista] find_and_attach_bitmap_manager: cannot locate buffer pool offsets.\n");
        return NULL;
    }

    void* bitmap_mgr_ptr = (char*) shmem_base + offsets->bitmap_mgr_offset;
    if (bitmap_mgr_ptr == NULL)
    {
        fprintf(stderr, "[libvista] find_and_attach_bitmap_manager: bitmap_mgr_offset is not set.\n");
        return NULL;
    }

    return bitmap_mgr_ptr;
}

/* Get the bitmap by index */
vista_atomic_bitmap_ptr vista_get_bitmap_by_idx(void* bitmap_mgr_base, uint32_t bitmap_idx)
{
    VistaBitmapHeader *header = (VistaBitmapHeader *)bitmap_mgr_base;
    return (vista_atomic_bitmap_ptr) ((char *)bitmap_mgr_base + header->bitmap_pool_offset + bitmap_idx * header->bitmap_size_bytes_aligned);
}

/* Get the TCB by table ID */
VistaBitmapTableControlBlock* vista_get_tcb_by_table_id(void* bitmap_mgr_base, uint32_t table_id)
{
    VistaBitmapHeader *header = (VistaBitmapHeader *)bitmap_mgr_base;
    VistaBitmapTableControlBlock *tcb_pool = (VistaBitmapTableControlBlock *)((char *)bitmap_mgr_base + header->tcb_pool_offset);
    for (uint32_t i = 0; i < header->max_tables; i++)
    {
        if (tcb_pool[i].is_in_use && tcb_pool[i].table_id == table_id)
            return &tcb_pool[i];
    }
    return NULL;
}

/* Initialize and Get TCB by table ID (only for use by writer)*/
VistaBitmapTableControlBlock* vista_init_and_get_tcb_by_table_id(void* bitmap_mgr_base, uint32_t table_id)
{
    VistaBitmapHeader *header = (VistaBitmapHeader *)bitmap_mgr_base;
    VistaBitmapTableControlBlock *tcb_pool = (VistaBitmapTableControlBlock *)((char *)bitmap_mgr_base + header->tcb_pool_offset);
    /* If it already exists, return it */
    for (uint32_t i = 0; i < header->max_tables; i++)
    {
        if (tcb_pool[i].is_in_use && tcb_pool[i].table_id == table_id)
            return &tcb_pool[i];
    }

    // Not found, try to initialize a new TCB
    for (uint32_t i = 0; i < header->max_tables; i++)
    {
        if (!tcb_pool[i].is_in_use)
        {
            tcb_pool[i].table_id = table_id;
            tcb_pool[i].is_in_use = true;

            atomic_fetch_add(&header->num_current_tables, 1);
            /* we re-initialize these two fields, although not necessary in current implementation */
            atomic_store(&tcb_pool[i].active_idx_flag, 0); // Start with A as active
            atomic_store(&tcb_pool[i].version, 0);
            return &tcb_pool[i];
        }
    }

    // No free TCB found
    fprintf(stderr, "[libvista] init_and_get_tcb_by_table_id: no free TCB found for table_id %u.\n", table_id);
    return NULL;
}

/* Add a new table to the bitmap 
 * Arguments: bitmap_mgr_base - the base address of the bitmap manager
 *            table_id - the ID of the new table
 * Returns: true if successful, false if failed (e.g., max tables reached)
*/
bool vista_add_new_table_to_bitmap(void* bitmap_mgr_base, uint32_t table_id)
{
    VistaBitmapHeader *header = (VistaBitmapHeader *)bitmap_mgr_base;

    VistaBitmapTableControlBlock *tcb_pool = (VistaBitmapTableControlBlock *)((char *)bitmap_mgr_base + header->tcb_pool_offset);
    uint32_t current_tables = atomic_load(&header->num_current_tables);
    if (current_tables >= header->max_tables)
    {
        fprintf(stderr, "[libvista] add_new_table_to_bitmap: maximum number of tables reached.\n");
        return false;
    }

    /* If it already exists, do not add it */
    for (uint32_t i = 0; i < header->max_tables; i++)
    {
        if (tcb_pool[i].is_in_use && tcb_pool[i].table_id == table_id)
        {    
            fprintf(stderr, "[libvista] add_new_table_to_bitmap: cannot add a table that is already present!.\n");
            return false;
        }
    }

    for (uint32_t i = 0; i < header->max_tables; i++)
    {
        if (!tcb_pool[i].is_in_use)
        {
            tcb_pool[i].table_id = table_id;
            tcb_pool[i].is_in_use = true;
            atomic_store(&tcb_pool[i].active_idx_flag, 0); // Start with A as active
            atomic_fetch_add(&header->num_current_tables, 1);
            return true;
        }
    }

    // Should not reach here
    fprintf(stderr, "[libvista] add_new_table_to_bitmap: no free TCB found despite space available.\n");
    return false;
}

/* 
 * Test a specific bit in the bitmap 
 * Returns true if the bit is set, false otherwise.
 */
bool vista_test_bit_pre_work(void* bitmap_mgr_base, uint32_t table_id, size_t bit_idx, uint64_t* pre_work_version, libvista_BufferPoolControl *ctl)
{
    VistaBitmapTableControlBlock *tcb = vista_get_tcb_by_table_id(bitmap_mgr_base, table_id);
    if (tcb == NULL)
    {
        //fprintf(stderr, "[libvista] test_bit_pre_work: table_id %u not found.\n", table_id);
        return false;
    }

    uint32_t old_val;
    uint32_t active_idx;
    uint32_t bitmap_idx;
	do {
		old_val = libvista_seqlock_read_begin(&ctl->ctl_seqlock);
        if (pre_work_version != NULL)
            *pre_work_version = atomic_load(&tcb->version);
        else {
            fprintf(stderr, "[libvista] test_bit_pre_work: pre_work_version pointer is NULL.\n");
            return false;
        }
        active_idx = atomic_load(&tcb->active_idx_flag);
	} while (libvista_seqlock_read_retry(&ctl->ctl_seqlock, old_val));

    bitmap_idx = (active_idx == 0) ? tcb->bitmap_idx_A : tcb->bitmap_idx_B;
    vista_atomic_bitmap_ptr bm = vista_get_bitmap_by_idx(bitmap_mgr_base, bitmap_idx);

    if (bm == NULL)
    {
        fprintf(stderr, "[libvista] test_bit_pre_work: bitmap pointer is NULL for table_id %u.\n", table_id);
        return false;
    }

    size_t word_idx = bit_idx / BITS_PER_WORD;
    size_t bit_offset = bit_idx % BITS_PER_WORD;
    unsigned long mask = 1UL << bit_offset;
    unsigned long word_val = atomic_load(&bm[word_idx]);

    return (word_val & mask) != 0;
}

/* 
 * Clear a specific bit in the bitmap 
 * Returns true if successful, false otherwise.
 */
bool vista_clear_bit_post_work(void* bitmap_mgr_base, uint32_t table_id, size_t bit_idx, uint64_t pre_work_version, libvista_BufferPoolControl *ctl)
{
    VistaBitmapTableControlBlock *tcb = vista_get_tcb_by_table_id(bitmap_mgr_base, table_id);
    if (tcb == NULL)
    {
        //fprintf(stderr, "[libvista] clear_bit_post_work: table_id %u not found.\n", table_id);
        return false;
    }

    uint32_t old_val;
    uint32_t active_idx;
    uint32_t bitmap_idx;
	do {
		old_val = libvista_seqlock_read_begin(&ctl->ctl_seqlock);
        /* Get the current version of this table bitmaps from TCB */
        uint64_t current_version = atomic_load(&tcb->version);
        if (current_version != pre_work_version)
        {
            // Version mismatch, another thread has modified the bitmap
            // fprintf(stderr, "[libvista-info] clear_bit_post_work: version mismatch for table_id %u. pre_work_version=%lu, current_version=%lu\n",
            //        table_id, pre_work_version, current_version);
            return false;
        }

        active_idx = atomic_load(&tcb->active_idx_flag);
	} while (libvista_seqlock_read_retry(&ctl->ctl_seqlock, old_val));

    bitmap_idx = (active_idx == 0) ? tcb->bitmap_idx_A : tcb->bitmap_idx_B;
    vista_atomic_bitmap_ptr bm = vista_get_bitmap_by_idx(bitmap_mgr_base, bitmap_idx);

    if (bm == NULL)
    {
        fprintf(stderr, "[libvista] clear_bit_post_work: bitmap pointer is NULL for table_id %u.\n", table_id);
        return false;
    }

    size_t word_idx = bit_idx / BITS_PER_WORD;
    size_t bit_offset = bit_idx % BITS_PER_WORD;
    unsigned long mask = 1UL << bit_offset;

    // Atomically clear the bit
    atomic_fetch_and(&bm[word_idx], ~mask);

    return true;
}

/* 
 * Non-atomic set/clear/test functions
 * These are used by the global coordinator (e.g., Postgres Flush Worker) 
 * to manipulate the bitmap without atomicity guarantees.
 * It is not thread-safe, so should not be used by concurrent OLAP workers.
 */

void vista_non_atomic_set_bit(vista_bitmap_t *bm, size_t bit_idx)
{
    if (bm == NULL || bit_idx >= bm->num_bits) return;
    bm->words[bit_idx / BITS_PER_WORD] |= (1UL << (bit_idx % BITS_PER_WORD));
}

void vista_non_atomic_clear_bit(vista_bitmap_t *bm, size_t bit_idx)
{
    if (bm == NULL || bit_idx >= bm->num_bits) return;
    bm->words[bit_idx / BITS_PER_WORD] &= ~(1UL << (bit_idx % BITS_PER_WORD));
}

bool vista_non_atomic_test_bit(vista_bitmap_t *bm, size_t bit_idx)
{
    if (bm == NULL || bit_idx >= bm->num_bits)
    {
        fprintf(stderr, "[libvista] non_atomic_test_bit: bitmap pointer is NULL or bit index out of range.\n");
        return false;
    }
    return (bm->words[bit_idx / BITS_PER_WORD] & (1UL << (bit_idx % BITS_PER_WORD))) != 0;
}

void vista_non_atomic_bitmap_init(vista_bitmap_t *bm, void *mem, size_t num_bits)
{
    bm->words = (unsigned long *)mem;
    bm->num_bits = num_bits;
}


// #ifdef 0
// /* Get the active bitmap for a given table ID 
//  * Arguments: bitmap_mgr_base - the base address of the bitmap manager
//  *            table_id - the ID of the table
//  * Returns: pointer to the active bitmap, or NULL if not found
// */
// vista_atomic_bitmap_ptr vista_get_active_bitmap(void* bitmap_mgr_base, uint32_t table_id, uint64_t* pre_work_version)
// {
//     VistaBitmapTableControlBlock *tcb = get_tcb_by_table_id(bitmap_mgr_base, table_id);
//     if (tcb == NULL)
//     {
//         fprintf(stderr, "[libvista] get_active_bitmap: table_id %u not found.\n", table_id);
//         return NULL;
//     }

//     uint32_t active_idx = atomic_load(&tcb->active_idx_flag);
//     uint32_t bitmap_idx = (active_idx == 0) ? tcb->bitmap_idx_A : tcb->bitmap_idx_B;

//     if (pre_work_version != NULL)
//         *pre_work_version = atomic_load(&tcb->version);

//     return get_bitmap_by_idx(bitmap_mgr_base, bitmap_idx);
// }

// /* 
//  * Atomically test if a bit is set in the bitmap.
//  * Arguments: bm - pointer to the atomic bitmap
//  *            bit_idx - index of the bit to test
//  * Returns: true if the bit is **set**, false otherwise
// */
// bool vista_atomic_test_bit(vista_atomic_bitmap_ptr bm, size_t bit_idx)
// {
//     if (bm == NULL)
//     {
//         fprintf(stderr, "[libvista] atomic_test_bit: bitmap pointer is NULL.\n");
//         return;
//     }

//     size_t word_idx = bit_idx / BITS_PER_WORD;
//     size_t bit_offset = bit_idx % BITS_PER_WORD;
//     unsigned long mask = 1UL << bit_offset;
//     unsigned long word_val = atomic_load(&bm[word_idx]);

//     return (word_val & mask) != 0;
// }
// #endif
