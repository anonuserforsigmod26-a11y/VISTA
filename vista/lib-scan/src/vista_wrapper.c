#ifdef USE_VISTA_IO

#include <libvista.h>
#include <stdio.h>
#include "vista_wrapper.h"
#include "vista_remapped_buffer_access.h"

#ifndef PG_BLOCK_SIZE
#define PG_BLOCK_SIZE 8192
#endif

/* Export Vista constants for C++ access */
const unsigned int VISTA_NO_MORE_PAGES_TO_READ_VALUE = VISTA_NO_MORE_PAGES_TO_READ;
const unsigned int VISTA_INVALID_VD_VALUE = VISTA_INVALID_VD;
const unsigned int VISTA_NO_PUT_PAGE_CALLED_VALUE = VISTA_NO_PUT_PAGE_CALLED;
const unsigned int VISTA_RETRY_REMAP_ACCESS_VALUE = VISTA_RETRY_REMAP_ACCESS;
const unsigned int VISTA_NO_REMAP_ACCESS_VALUE = VISTA_NO_REMAP_ACCESS;

void* vista_wrapper_alloc_addrs(void) {
    return malloc(sizeof(struct vista_addrs));
}

void vista_wrapper_free_addrs(void* addrs) {
    if (addrs) {
        free(addrs);
    }
}

int vista_wrapper_register_olap(void* addrs) {
    struct vista_addrs* vista_addrs = (struct vista_addrs*)addrs;
    if (vista_register(1, VISTA_MAP_OLAP, vista_addrs) != 0) {
        return -1;
    }
    vista_init_vmseg(1, VISTA_MAP_OLAP, vista_addrs);
#ifdef USE_VISTA_IO
		// printf("vista version!\n");
#endif
    return 0;
}

void vista_wrapper_unregister_olap(void* addrs) {
    struct vista_addrs* vista_addrs = (struct vista_addrs*)addrs;
    vista_unregister(vista_addrs, 1);
}

/* NOT USED
int vista_wrapper_open_range(int fd, int start_blkno, int end_blkno) {
    struct vista_read_range range = {
        .fd = fd,
        .start = start_blkno % 131072 ,
        .end = end_blkno % 131072 + 1 // Vista uses exclusive end
    };
    
    struct vista_io_range_vec iovec = {
        .io_range_base = &range,
        .io_range_len = 1
    };
    
    struct vista_read_param param = {
        .page_size = PG_BLOCK_SIZE,
        .sq_polling = false
    };
    
    return vista_open(&iovec, &param);
}
*/

int vista_wrapper_open_multi_range(int fd, uint32_t block_group_start, uint32_t block_group_end, uint32_t sub_group_size, uint32_t uncached_mask) {
    uint32_t block_group_size = block_group_end - block_group_start + 1;
    uint32_t subgroups_per_bg = (block_group_size + sub_group_size - 1) / sub_group_size;  // Round up

    struct vista_read_range* ranges = (struct vista_read_range*)malloc(subgroups_per_bg * sizeof(struct vista_read_range));
    if (!ranges) {
        return -1;
    }

    int range_count = 0;

    // Create ranges for uncached SubGroups
    for (uint32_t sg = 0; sg < subgroups_per_bg; sg++) {
        if (uncached_mask & (1U << sg)) {
            uint32_t sg_start = block_group_start + sg * sub_group_size;
            uint32_t sg_end = sg_start + sub_group_size - 1;

            // Don't exceed BlockGroup boundary
            if (sg_end > block_group_end) {
                sg_end = block_group_end;
            }

            ranges[range_count].fd = fd;
            ranges[range_count].start = sg_start % 131072;
            ranges[range_count].end = (sg_end % 131072) + 1;  // Vista uses exclusive end

            range_count++;
        }
    }

    if (range_count == 0) {
        free(ranges);
        return -1;  // No ranges to open
    }

    struct vista_io_range_vec iovec = {
        .io_range_base = ranges,
        .io_range_len = range_count
    };

    struct vista_read_param param = {
        .page_size = PG_BLOCK_SIZE,
        .sq_polling = false
    };

    int result = vista_open(&iovec, &param);
    free(ranges);
    return result;
}

void* vista_wrapper_get_page(int vd, unsigned int gen) {
    return vista_get_page(vd, gen);
}

void vista_wrapper_put_page(int vd, void* page) {
    vista_put_page(vd);
}

void vista_wrapper_close(int vd) {
    vista_close(vd);
}

int vista_wrapper_open_custom(void* ranges, int range_count, unsigned long page_size, int sq_polling) {
    struct vista_io_range_vec iovec = {
        .io_range_base = (struct vista_read_range*)ranges,
        .io_range_len = range_count
    };

    struct vista_read_param param = {
        .page_size = page_size,
        .sq_polling = sq_polling
    };

    return vista_open(&iovec, &param);
}

/* Vista remapped buffer wrapper implementations */
void vista_wrapper_scan_init(void) {
    vista_scan_init();
}

void* vista_wrapper_try_read_remapped_page(int spcNode, int dbNode, int relNode, unsigned int blockNum, unsigned int* gen) {
    return vista_try_read_remapped_page(spcNode, dbNode, relNode, blockNum, gen);
}

void vista_wrapper_release_remapped_page(void) {
    vista_release_remapped_page();
}

void vista_wrapper_scan_close(void) {
    vista_scan_close();
}

void vista_wrapper_external_unmap(void) {
    vista_external_unmap();
}

#endif /* USE_VISTA_IO */
