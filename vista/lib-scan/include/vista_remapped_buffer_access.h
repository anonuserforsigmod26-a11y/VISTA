#ifndef __VISTA_SCAN_H__
#define __VISTA_SCAN_H__

#include <stdbool.h>
#include <stdint.h>
#include "vista_pg_read_buffer.h"

#ifdef __cplusplus
extern "C" {
#endif

extern void* bitmap_mgr_ptr;
/*
 * vista_scan_init: Initialize the VISTA scan client.
 * This must be called once before any other vista_scan functions are used.
 */
void vista_scan_init(void);

/*
 * vista_try_read_remapped_page: Try to read a page from the remapped buffer pool.
 * If successful, it returns a pointer to the page. If not, it returns NULL.
 * If a page is returned, the caller must call vista_release_remapped_page()
 * after finishing the use of the page.
 */
void* vista_try_read_remapped_page(int spcNode, int dbNode, int relNode, vista_pg_BlockNumber blockNum, unsigned int* gen);

/*
 * vista_release_remapped_page: Release a page that was read from the
 * remapped buffer pool.
 *
 * This must be called after processing a page that was successfully returned by
 * vista_try_read_remapped_page(), to ensure the unmap process can proceed.
 */
void vista_release_remapped_page(void);

/*
 * vista_scan_close: Close the remapped buffer pool and unmap the VMSeg if this process
 * is the last one using it.
 */
void vista_scan_close(void);

void vista_external_unmap(void);

unsigned long vista_get_generation_flush_values(void);

/*
 * Bitmap wrapper functions for cache validation
 */
bool LibScan_TestBitPreWork(uint32_t table_id, size_t bit_idx, uint64_t* pre_work_version);
bool LibScan_ClearBitPostWork(uint32_t table_id, size_t bit_idx, uint64_t pre_work_version);

#ifdef __cplusplus
}
#endif

#endif /* __VISTA_SCAN_H__ */
