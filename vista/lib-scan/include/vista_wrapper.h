#ifndef VISTA_WRAPPER_H
#define VISTA_WRAPPER_H

#include <stdint.h>

#ifdef USE_VISTA_IO

#ifdef __cplusplus
extern "C" {
#endif

/* Vista constants - provide access to libvista constants for C++ */
extern const unsigned int VISTA_NO_MORE_PAGES_TO_READ_VALUE;
extern const unsigned int VISTA_INVALID_VD_VALUE;
extern const unsigned int VISTA_NO_PUT_PAGE_CALLED_VALUE;
extern const unsigned int VISTA_RETRY_REMAP_ACCESS_VALUE;
extern const unsigned int VISTA_NO_REMAP_ACCESS_VALUE;

/* Vista wrapper functions for C++ compatibility */
void* vista_wrapper_alloc_addrs(void);
void vista_wrapper_free_addrs(void* addrs);
int vista_wrapper_register_olap(void* addrs);
void vista_wrapper_unregister_olap(void* addrs);
int vista_wrapper_open_range(int fd, int start_blkno, int end_blkno);
void* vista_wrapper_get_page(int vd, unsigned int gen);
int vista_wrapper_open_multi_range(int fd, uint32_t block_group_start, uint32_t block_group_end, uint32_t sub_group_size, uint32_t uncached_mask);
int vista_wrapper_open_custom(void* ranges, int range_count, unsigned long page_size, int sq_polling);
void vista_wrapper_put_page(int vd, void* page);
void vista_wrapper_close(int vd);

/* Vista remapped buffer wrapper functions */
void vista_wrapper_scan_init(void);
void* vista_wrapper_try_read_remapped_page(int spcNode, int dbNode, int relNode, unsigned int blockNum, unsigned int* gen);
void vista_wrapper_release_remapped_page(void);
void vista_wrapper_scan_close(void);
void vista_wrapper_external_unmap(void);

#ifdef __cplusplus
}
#endif

#endif /* USE_VISTA_IO */

#endif /* VISTA_WRAPPER_H */
