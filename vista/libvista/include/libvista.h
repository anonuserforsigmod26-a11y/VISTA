#ifndef __LIBVISTA_H__
#define __LIBVISTA_H__

#include <linux/vista.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <sched.h>
#include <numa.h>

#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/file.h>
#include <fcntl.h>
#include <errno.h>

#include <liburing.h>
#include <limits.h>

#define VISTA_API
#define VISTA_OLTP_API
#define VISTA_OLAP_API

#define VISTA_OLAP_FLAG (0x1)
#define VISTA_OLTP_FLAG (0x2)

#define N_POOLS 3 

#define NR_SYSCALL_vista_mmap (560)
#define NR_SYSCALL_vista_munmap (561)
#define NR_SYSCALL_vista_remap (562)
#define NR_SYSCALL_vista_unmap (563)

// We use the cacheline align logic of PostgreSQL.
#define VISTA_CACHELINE_SIZE (128)
#define VISTA_CACHELINE_ALIGN(LEN) (((uintptr_t) (LEN) + ((VISTA_CACHELINE_SIZE) - 1)) & ~((uintptr_t) ((VISTA_CACHELINE_SIZE) - 1)))

/*
 * The OLAP VMSeg is divided into two parts, header and data.
 * The header is used to store metadata, and the data part is used to store the actual data.
 * Each part has a fixed size (32 MiB).
 */
#define VISTA_OLAP_MAGIC_NUMBER (0x5649535441) /* 'VISTA' in ASCII */
#define VISTA_OLAP_VMSEG_HEADER (4UL * 1024 * 1024) /* 4 MiB (why 4MiB? to align the rest region at 4MiB) */
#define VISTA_DATA_BUFFER_SIZE (4UL * 1024 * 1024) /* 4 MiB, For now, we're using double buffering */
#define VISTA_DATA_BUFFER_ENTRY_SIZE (2UL * 1024 * 1024) /* 2 MiB, one chunk */
#define VISTA_NR_DATA_BUFFER_ENTRY (VISTA_DATA_BUFFER_SIZE / VISTA_DATA_BUFFER_ENTRY_SIZE) /* Number of data buffer entries */
#define VISTA_MAX_PARTITIONS (255) /* Except for the header. (max partition count for 1GiB) */

#define VISTA_IO_URING_ENTRIES (512) /* NOTE: This can be smaller than now */
#define VISTA_IO_SIZE (VISTA_DATA_BUFFER_ENTRY_SIZE)

#define VISTA_NUMA_OLAP (0) /* OLAP NUMA node */
#define VISTA_NUMA_OLTP (1) /* OLTP NUMA node */

#define VISTA_MAP_OLAP (1)
#define VISTA_MAP_OLTP (2)

#define VISTA_SHMEM_KEY (0xDEADBEEF)
#define VISTA_SHMEM_SIZE (32UL* 1024 * 1024) /* 32MiB */
#define VISTA_SHMEM_LOCK_FILE "/run/lock/vista/vista_shm.lock"

#define VISTA_SEGSTATE_READY 0
#define VISTA_SEGSTATE_NORMAL 1
#define VISTA_SEGSTATE_QUIESCE 2
#define VISTA_SEGSTATE_WRITEBACK 3
#define VISTA_SEGSTATE_DRAIN 4

/* 
 * Status flags for the VISTA SQE
 * (empty -> sqe submitted -> prefetched) cycle.
 * 
 * VISTA_SAFE_BUFFER indicates that the buffer does not contain any pages that
 * are possibly torn. (i.e., all pages in the buffer were prefetched in the safe period.)
 */
#define VISTA_EMPTY 		(0x0)
#define VISTA_SQE_SUBMITTED (0x1)
#define VISTA_PREFETCHED 	(0x2)
#define VISTA_SAFE_BUFFER	(0x4) 

/*
 * Maximum number of file descriptors for a channel.
 * This may not be enough for all use cases.
 */
#define VISTA_MAX_NR_FD (128)
#define VISTA_MAX_NR_READ_RANGE (512)

#define VISTA_PAGE_SIZE (4096UL)

#ifndef MIN
#define MIN(a, b) ((a) < (b) ? (a) : (b))
#endif

#ifndef likely
#define likely(x)   __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

#ifdef __cplusplus
extern "C" {
#endif

/*
 * This structure represents a read range for a specific file.
 * e.g., { fd=3, start=200, end=1000 }
 * |---------[====================]----------|  <- file(fd=3)
 * 0         ^                    ^         EOF
 *           |                    |
 *      page_idx=200        page_idx=1000
 */
struct vista_read_range {
	/* 
	 * NOTE: If struct is used in io_uring, `fd` is not a real file descriptor.
	 * Because we use io_uring_register_files() to register it,
	 * this fd means the index of the registered file. (i.e., [0, nr_fd))
	 */
	int fd;
	unsigned long start;
	unsigned long end;
};

struct vista_io_range_vec {
	struct vista_read_range *io_range_base;
	unsigned int io_range_len;
};

/*
 * Read-related configurations.
 */
struct vista_read_param {
	/*
	 * Page size in bytes. (e.g., 8KiB)
	 * It must be a power of 2 and it must be smaller than VISTA_IO_SIZE (2MiB).
	 * Default = 4096 (4KiB)
	 */
	unsigned long page_size;
	bool sq_polling; // Use SQ polling mode in io_uring. Default = false. (Not supported yet)
};

struct vista_uring_user_data {
	union {
		unsigned long __raw;
		struct {
			unsigned int generation:32;
			unsigned int buffer_idx:16;
			unsigned int is_flushing:1;
			unsigned int reserved:15;
		};
	};
};
typedef struct vista_uring_user_data vista_gen_flush_info_t;

struct vista_channel {
	struct io_uring ring;
	struct vista_read_param param;

	int channel_idx;
	
	int fds[VISTA_MAX_NR_FD];
	unsigned int nr_fd;

	/*
	 * read_ranges[0] { fd=3, start=200, end=1000 }
	 * |---------[====================]----------|  <- file(fd=3)
	 * 0         ^                    ^         EOF
	 *           |                    |
	 *      page_idx=200        page_idx=1000
	 *
     * read_ranges[1] { fd=4, start=500, end=800 }
	 * |--------------[=+=====]----------|  <- file(fd=4)
	 * 0              ^ ^      ^         EOF
	 *                | |      |
	 *     page_idx=500 |    page_idx=800
	 *                  |
	 *           next to prefetch (e.g., `page_idx`=600, `read_range_idx`=1)
	 *
	 * If the channel reaches end of the current read range, then it moves to
	 * the next read range.
	 */

	unsigned int next_page_idx; // The next page idx to prefetch.
	char *page_base; // The current page's start address.

	struct vista_read_range read_ranges[VISTA_MAX_NR_READ_RANGE];
	int nr_read_ranges; // The total number of read ranges.
	int read_range_idx; // The current read range.

	/*
	 * In default, we use double buffering. Each buffer is 2 MiB in size.
	 * [============][------------]
	 *   buffer 0      buffer 1
	 *
	 * If the read size = 8KiB (like in postgres),
	 * Each buffer can contain 256 pages.
	 *
	 * We set each buffer's status in `prefetched`.
	 * `VISTA_EMPTY`: Nothing here, no SQE submitted currently.
	 * `VISTA_SQE_SUBMITTED`: SQE has been submitted for this buffer.
	 * `VISTA_PREFETCHED`: This buffer has been prefetched. (=ready to use)
	 */
	unsigned int buffer_idx;
	unsigned int curr_buf_page_idx; // The current page idx(in buffer) being consumed by user.

	unsigned int nr_buffer;
	unsigned int *buffer_status;
	unsigned int *buffer_gen; // The generation of each buffer.
	struct vista_read_range *buffer_read_ranges; // The read range of each buffer.
	unsigned int *buffer_nr_page;
	char *buffers;

	unsigned int nr_remaining_page;
	bool put_page_called; // Whether vista_put_page() has been called at least once.
};

#define VISTA_URING(channel) (&(channel)->ring)
#define VISTA_USER_PAGE_SIZE(channel) ((channel)->param.page_size)
#define VISTA_BUFFER(channel, idx) ((channel)->buffers + ((idx) * VISTA_DATA_BUFFER_ENTRY_SIZE))
#define VISTA_CURRENT_BUFFER(channel) (VISTA_BUFFER(channel, channel->buffer_idx))
#define VISTA_BUFFER_STATUS(channel, idx) ((channel)->buffer_status[idx])
#define VISTA_NEXT_READ_BYTE_OFFSET(channel) ((channel)->next_page_idx * VISTA_USER_PAGE_SIZE(channel))
#define VISTA_CURRENT_READ_RANGE_FD(channel) ((channel)->read_ranges[(channel)->read_range_idx].fd)
#define VISTA_CURRENT_END_BYTE_OFFSET(channel) ((channel)->read_ranges[(channel)->read_range_idx].end * VISTA_USER_PAGE_SIZE(channel))
#define VISTA_CURRENT_BUFFER_STATUS(channel) (VISTA_BUFFER_STATUS(channel, channel->buffer_idx))
#define VISTA_MOVE_TO_NEXT_BUFFER(channel) ((channel)->buffer_idx = ((channel)->buffer_idx + 1) % (channel)->nr_buffer)
#define VISTA_NOT_STARTED (UINT_MAX)
#define VISTA_NO_MORE_PAGES_TO_READ (UINT_MAX)
#define VISTA_NO_PUT_PAGE_CALLED (UINT_MAX-1)
#define VISTA_NO_GET_PAGE_CALLED (UINT_MAX-2)
#define VISTA_INVALID_VD (UINT_MAX-3)
/* User should retry remap access to prohibit torn page read */
#define VISTA_RETRY_REMAP_ACCESS (UINT_MAX-4)
/* Use this value as gen when remap access is not needed (i.e., read format cache) */
#define VISTA_NO_REMAP_ACCESS (UINT_MAX)
#define VISTA_CURRENT_FILE_PAGE_IDX(channel) ((channel)->buffer_read_ranges[(channel)->buffer_idx].start + (channel)->curr_buf_page_idx)
/* Warning: This macro returns the real fd, not the fd offset which is used by io_uring */
#define VISTA_CURRENT_FILE_FD(channel) ((channel)->buffer_read_ranges[(channel)->buffer_idx].fd)
#define VISTA_BUFFER_GEN(channel, idx) ((channel)->buffer_gen[(idx)])
#define VISTA_CURRENT_BUFFER_GEN(channel) (VISTA_BUFFER_GEN(channel, channel->buffer_idx))

#define VISTA_SETUP_FIRST_READ(channel) \
	do { \
		(channel)->next_page_idx = (channel)->read_ranges[0].start; \
		(channel)->read_range_idx = 0; \
	} while (0)

struct vista_olap_vmseg_header {
	atomic_int ref_count;
	atomic_flag is_allocated[VISTA_MAX_PARTITIONS];
	atomic_int owner_pid[VISTA_MAX_PARTITIONS];
};

struct vista_olap_vmseg {
	struct vista_olap_vmseg_header header;
	char ____padding[VISTA_OLAP_VMSEG_HEADER - sizeof(struct vista_olap_vmseg_header)]; /* DO NOT USE IT */
	char data[];
};

/*
 * The VMSeg memory allocator structure.
 * It is used to allocate VMSeg memory during buffer pool initialization by PostgreSQL.
 * It has certain assumptions:
 * 1. The VMSeg memory is never freed until the DBMS shutdown (which is the same as in shmem of PostgreSQL).
 * 2. No runtime allocation is allowed; we assume that the VMSeg memory is allocated only during the DBMS startup.
 * Thus, this structure does not reside in shared memory, but in per-process memory space (of Postmaster process).
 * In the same reason, we do not include this structure in the VMSeg header.
 * It also does not need to be thread-safe.
 */
typedef struct vista_vmseg_allocator {
	void* VmsegBase;
	void* VmsegEnd; // End of the VMSeg memory.
	uint32_t freeoffset; // A single VMSeg has 1GB memory, so we can use 32-bit unsigned int for the offset.
} vista_vmseg_allocator;

typedef struct vista_shmem_allocator {
	void* ShmemBase; // The actual start address of the shmem.
	void* AlignedBase; // The aligned start address of the shmem.
	void* ShmemEnd; // End of the shmem.
	uint32_t freeoffset; // A single shmem has 32MB memory, so we can use 32-bit unsigned int for the offset.
} vista_shmem_allocator;


/*
 * This structure is used to store the offsets of the different components of the PostgreSQL buffer pool
 * within the VMSeg. It is used to calculate the addresses of the components by an OLAP process,
 * by adding the offsets to the base address of the OLAP's VMSeg.
*/
typedef struct vista_hash_table_offsets {
	uint32_t hctl_offset;
	uint32_t dir_offset;
	uint32_t keysize;
	uint32_t ssize;
	uint32_t sshift;
} vista_hash_table_offsets;

typedef struct vista_bufferpool_offsets {
	/* These offsets are used to locate the components within the VMSegs */
	uint64_t vmseg_base_addr[N_POOLS];
	uint64_t shmem_base_addr;
    uint32_t seg_hdr_offset[N_POOLS];
	uint32_t buf_desc_offset[N_POOLS];
	uint32_t buf_blocks_offset[N_POOLS];
	// uint32_t buf_iocv_offset[N_POOLS];
	// uint32_t ckpt_buf_ids_offset[N_POOLS];
	vista_hash_table_offsets buf_hash_offsets[N_POOLS];
	/* offsets below exist in the shared metadata VMSeg */
	uint32_t buf_ctl_offset;
	uint32_t snapshot_hash_lock_offset;
	vista_hash_table_offsets snapshot_hash_offset;
	uint32_t bitmap_mgr_offset;
} vista_bufferpool_offsets;

/*
 * libvista APIs
 */
extern VISTA_API int vista_register(int count, int flag, struct vista_addrs *addrs);
extern VISTA_API int vista_unregister(struct vista_addrs *addrs, int count);
extern VISTA_API void vista_init_vmseg(int count, int flag, struct vista_addrs *addrs);

extern VISTA_OLAP_API int vista_open(struct vista_io_range_vec *iov, struct vista_read_param *param);
extern VISTA_OLAP_API void *vista_get_page(int vd, unsigned int gen);
extern VISTA_OLAP_API int vista_put_page(int vd);
extern VISTA_OLAP_API int vista_close(int vd);

extern VISTA_OLAP_API int vista_unmap(void);

extern VISTA_OLTP_API int vista_remap(unsigned int idx);

extern VISTA_API void vista_dummy(); // dummy function to test library linking 
extern VISTA_OLTP_API void vista_vmseg_allocator_init(vista_vmseg_allocator *allocator, void *base);
extern VISTA_OLTP_API void* vista_vmseg_alloc(vista_vmseg_allocator *allocator, size_t size);
extern VISTA_OLTP_API void vista_shmem_allocator_init(vista_shmem_allocator *allocator, void *base);
extern VISTA_OLTP_API void* vista_shmem_alloc(vista_shmem_allocator *allocator, size_t size);

extern VISTA_API void vista_print_stats();

#ifdef __cplusplus
}
#endif
#endif /* __LIBVISTA_H__ */
