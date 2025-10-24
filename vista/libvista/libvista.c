#include "include/libvista.h"
#include "include/vista_chtable.h"
#include "vista_pg_buf_ctl.h"

#include <liburing.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdatomic.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/types.h>

#ifdef VISTA_MEASURE_LATENCY
#include <time.h>

static __thread struct {
	long get_page_time;
	long put_page_time;
	long wait_cqe_time;
	long submit_sqes_time;
	long get_page_count;
	long put_page_count;
	long wait_cqe_count;
	long submit_sqes_count;
	long reread_count;
} latency_stats;
#endif

#ifdef VISTA_MEASURE_LATENCY
#define VISTA_LATENCY_START(name) \
struct timespec __##name##_start, __##name##_end; \
do { \
	clock_gettime(CLOCK_MONOTONIC, &__##name##_start); \
} while (0)
#else
#define VISTA_LATENCY_START(name)
#endif

#ifdef VISTA_MEASURE_LATENCY
#define VISTA_LATENCY_END(name) \
do { \
	clock_gettime(CLOCK_MONOTONIC, &__##name##_end); \
	latency_stats.name##_time +=\
		(__##name##_end.tv_sec - __##name##_start.tv_sec) * 1000000000 + \
		(__##name##_end.tv_nsec - __##name##_start.tv_nsec); \
} while (0)
#else
#define VISTA_LATENCY_END(name)
#endif

#ifdef VISTA_MEASURE_LATENCY
#define VISTA_MEASURE_UPDATE_COUNT(name) \
do { \
	latency_stats.name##_count++; \
} while (0)
#else
#define VISTA_MEASURE_UPDATE_COUNT(name)
#endif

#ifdef VISTA_MEASURE_LATENCY
VISTA_API void vista_print_stats()
{
	printf("=== VISTA Latency Stats ===\n");
	if (latency_stats.get_page_count > 0)
		printf("get_page: total %ld ns, avg %ld ns over %ld calls\n",
			   latency_stats.get_page_time,
			   latency_stats.get_page_time / latency_stats.get_page_count,
			   latency_stats.get_page_count);
	if (latency_stats.put_page_count > 0)
		printf("put_page: total %ld ns, avg %ld ns over %ld calls\n",
			   latency_stats.put_page_time,
			   latency_stats.put_page_time / latency_stats.put_page_count,
			   latency_stats.put_page_count);
	if (latency_stats.wait_cqe_time > 0 && latency_stats.wait_cqe_count > 0)
		printf("wait_cqe: total %ld ns, avg %ld ns over %ld calls\n",
			   latency_stats.wait_cqe_time,
			   latency_stats.wait_cqe_time / latency_stats.wait_cqe_count,
			   latency_stats.wait_cqe_count);
	if (latency_stats.submit_sqes_time > 0 && latency_stats.submit_sqes_count > 0)
		printf("submit_sqes: total %ld ns, avg %ld ns over %ld calls\n",
			   latency_stats.submit_sqes_time,
			   latency_stats.submit_sqes_time / latency_stats.submit_sqes_count,
			   latency_stats.submit_sqes_count);
	if (latency_stats.reread_count > 0)
		printf("reread: total %ld times\n",
			   latency_stats.reread_count);
	printf("==========================\n");
}
#else
VISTA_API void vista_print_stats() {}
#endif

static struct vista_addrs __addrs;

static int g_lock_fd = -1; /* fd variable for shmem lock file */

/*
 * Allocate a data buffer from the VMSeg.
 */
static void vista_alloc_data_buffer(void **data_buffer, void *vmseg)
{
	int iter_count = 0;
	struct vista_olap_vmseg *__vmseg = (struct vista_olap_vmseg *)vmseg;

	while (true) {
		// Iterate over the partitions to find an available one.
		for (int i = 0; i < VISTA_MAX_PARTITIONS; ++i) {
			if (!atomic_flag_test_and_set(&__vmseg->header.is_allocated[i])) {
				// Found an available partition, allocate it.
				atomic_fetch_add(&__vmseg->header.ref_count, 1);
				atomic_store(&__vmseg->header.owner_pid[i], getpid());
				*data_buffer = &__vmseg->data[i * VISTA_DATA_BUFFER_SIZE];
				return;
			}
		}

		iter_count++;
		if (iter_count % 100000 == 0) // Just a random number to avoid too frequent logging. (this will very rarely happen or never)
			fprintf(stderr, "[libvista] vista_alloc_data_buffer() failed, no available partition found after %d iterations.\n", iter_count);
	}
}

static void vista_free_data_buffer(void *data_buffer, void *vmseg)
{
	// Get a offset from the data buffer pointer.
	struct vista_olap_vmseg *__vmseg = (struct vista_olap_vmseg *)vmseg;
	unsigned long offset = (unsigned long)data_buffer - (unsigned long)__vmseg->data;

	// Calculate the partition index.
	int partition_index = offset / VISTA_DATA_BUFFER_SIZE;
	if (partition_index < 0 || partition_index >= VISTA_MAX_PARTITIONS) {
		fprintf(stderr, "[libvista] vista_free_data_buffer() failed, invalid partition index: %d\n", partition_index);
		return;
	}

	// Check if the partition is allocated by the current process.
	if (atomic_load(&__vmseg->header.owner_pid[partition_index]) != getpid()) {
		fprintf(stderr, "[libvista] vista_free_data_buffer() failed, partition %d is not owned by the current process. (%d, %d)\n",
	  		partition_index, atomic_load(&__vmseg->header.owner_pid[partition_index]), getpid());
		return;
	}

	// Free the partition.
	atomic_fetch_sub(&__vmseg->header.ref_count, 1);
	atomic_store(&__vmseg->header.owner_pid[partition_index], 0);
	atomic_flag_clear(&__vmseg->header.is_allocated[partition_index]);
}

/* 
 * file lock (used for safe shm attach / detach)
 * Although it is almost impossible to have concurrency here, we place a file lock.
 * There would be no contention during the actual 'runtime', since the shmem is
 * only attached/detached during initialization and cleanup
 */
static int __vista_filelock()
{
    g_lock_fd = open(VISTA_SHMEM_LOCK_FILE, O_CREAT | O_RDWR, 0666);
    if (g_lock_fd == -1)
    {
        fprintf(stderr, "[libvista] Failed to open lock file %s with errno %d\n", VISTA_SHMEM_LOCK_FILE, errno);
        return -1;
    }

    /* acquire the flock, exclusively */
    if (flock(g_lock_fd, LOCK_EX) == -1)
    {
        fprintf(stderr, "[libvista] Failed to acquire lock on file %s\n", VISTA_SHMEM_LOCK_FILE);
        close(g_lock_fd);
        g_lock_fd = -1;
        return -1;
    }
    return 0;
}

static void __vista_fileunlock()
{
    if (g_lock_fd != -1)
    {
        flock(g_lock_fd, LOCK_UN);
        close(g_lock_fd);
        g_lock_fd = -1;
    }
}

/* 
 * __vista_attach_shm(): Allocate(create or find) shmem and return the start address of the region.
 * key and size are already defined in libvista.h
 */
static void* __vista_attach_shm()
{
    if (__vista_filelock() == -1)
    {
        return NULL;
    }

    int shm_id;
    char* addr;

    shm_id = shmget(VISTA_SHMEM_KEY, VISTA_SHMEM_SIZE, IPC_CREAT | 0666);
    if (shm_id == -1)
    {
        fprintf(stderr, "[libvista] vista_register() failed at shm allocation with error code %d\n", errno);
        __vista_fileunlock();
        return NULL;
    }

    addr = shmat(shm_id, NULL, 0);
    if (addr == (void *) -1)
    {
        fprintf(stderr, "[libvista] vista_register() failed at shm attachment with error code %d\n", errno);
        /* If failed, we remove the shmem anyway */
        shmctl(shm_id, IPC_RMID, NULL);
        __vista_fileunlock();
        return NULL;
    }
    __vista_fileunlock();

    return (void *) addr;
}

static int __vista_detach_shm(void *shm_addr) 
{
    int shm_id;
    struct shmid_ds buf;

    /* we do not need the lock for detaching */
    if (shmdt(shm_addr) == -1)
    {
        fprintf(stderr, "[libvista] cleanup failed at shm detach with error code %d\n", errno);
        return -1;
    }

    if (__vista_filelock() == -1)
    {
        return -1;
    }

    shm_id = shmget(VISTA_SHMEM_KEY, 0, 0);
    if (shm_id == -1)
    {
        /* already removed by another process */
        if (errno == ENOENT)
        {
            __vista_fileunlock();
            return 0;
        }

        fprintf(stderr, "[libvista] vista_unregister() failed at shm id retrieval with error code %d\n", errno);
        __vista_fileunlock();
        return -1;
    }

    if (shmctl(shm_id, IPC_STAT, &buf) == -1)
    {
        fprintf(stderr, "[libvista] vista_unregister() failed at shm status check with error code %d\n", errno);
        __vista_fileunlock();
        return -1;
    }

    if (buf.shm_nattch == 0) {
        /* remove the shmem if the nattach is zero */
        if (shmctl(shm_id, IPC_RMID, NULL) == -1)
        {
            fprintf(stderr, "[libvista] vista_unregister() failed at shm removal with error code %d\n", errno);
            __vista_fileunlock();
            return -1;
         }
    }
    __vista_fileunlock();
    return 0;
}

VISTA_API int vista_register(int count, int flag, struct vista_addrs *addrs)
{
	long ret;
	if ((ret = syscall(NR_SYSCALL_vista_mmap, count, flag, addrs))) {
		fprintf(stderr, "[libvista] vista_register() failed, syscall, %ld\n", ret);
		return ret;
	}

    /* for small shared metadata region (OLAP-OLTP), we use POSIX shm  */
    addrs->shrd_metadata_ptr = (unsigned long) __vista_attach_shm();
    if (addrs->shrd_metadata_ptr == 0)
    {
        return 1;
    }

	memcpy(&__addrs, addrs, sizeof(struct vista_addrs));
    return 0;
}

VISTA_API int vista_unregister(struct vista_addrs *addrs, int count)
{
	long ret;
	if ((ret = syscall(NR_SYSCALL_vista_munmap, addrs, count))) {
		fprintf(stderr, "[libvista] vista_unregister() failed, syscall, %ld\n", ret);
		return ret;
	}
	memset(&__addrs, 0x0, sizeof(struct vista_addrs));
	__vista_detach_shm((void*) addrs->shrd_metadata_ptr);
    return 0;
}

/*
 * Initialize the VMSeg.
 * This function should be called only once at the DBMS startup.
 ! TODO: This function clear the whole VMSeg memory, we may need to change it later.
 */
VISTA_API void vista_init_vmseg(int count, int flag, struct vista_addrs *addrs)
{
	if ((flag & VISTA_OLAP_FLAG) && addrs->olap_ptr) {
		memset((void *)addrs->olap_ptr, 0x0, (long)count * (1UL << 30));
	} else if ((flag & VISTA_OLTP_FLAG) && addrs->oltp_ptr) {
		memset((void *)addrs->oltp_ptr, 0x0, (long)count * (1UL << 30));
	} else {
		fprintf(stderr, "[libvista] vista_init_vmseg() failed, no valid VMSeg address provided.\n");
		return;
	}
}

static void vista_channel_init(struct vista_channel *channel)
{
	channel->nr_buffer = 0;
	channel->buffer_idx = 0;
	channel->buffer_status = NULL;
	channel->page_base = NULL;
	channel->next_page_idx = 0;
	channel->nr_read_ranges = 0;
	channel->curr_buf_page_idx = 0;
	channel->nr_remaining_page = 0;
	memset(channel->fds, 0, sizeof(int) * VISTA_MAX_NR_FD);
	channel->nr_fd = 0;
	channel->put_page_called = true; /* To allow very first get_page */
}

static int vista_channel_uring_init(struct vista_channel *channel)
{
	int ret;
	if ((ret = io_uring_queue_init(VISTA_IO_URING_ENTRIES,
								 VISTA_URING(channel), 0)) < 0) {
		fprintf(stderr, "[libvista] vista_channel_uring_init() failed, io_uring_queue_init() returned %d\n", ret);
		return ret;
	}

	/*
	 * Register the data buffer to the ring.
	 */
	struct iovec iovec = {
		.iov_base = channel->buffers,
		.iov_len = VISTA_DATA_BUFFER_SIZE,
	};
	if ((ret = io_uring_register_buffers(VISTA_URING(channel),
										 &iovec, 1)) < 0) {
		fprintf(stderr, "[libvista] vista_channel_uring_init() failed, io_uring_register_buffers() returned %d\n", ret);
		return ret;
	}

	/*
	 * Register the file descriptors to the ring.
	 */
	if ((ret = io_uring_register_files(VISTA_URING(channel),
										 channel->fds,
										 channel->nr_fd)) < 0) {
		fprintf(stderr, "[libvista] vista_channel_uring_init() failed, io_uring_register_files() returned %d\n", ret);
		return ret;
	}

	return 0;
}

/*
 * Add [start, end) range to the channel.
 * NOTE: We now assume that `start` and `end` are page (4KiB) indices.
 */
static int vista_add_range(int vd, struct vista_read_range *range)
{
	unsigned int i;
	unsigned long eof_pos;
	unsigned long start = range->start;
	unsigned long end = range->end;
	int fd = range->fd;
	struct vista_channel *channel = vista_chtable_get(vd);
	if (!channel) {
		fprintf(stderr, "[libvista] vista_add_range() failed, invalid vd %d\n", vd);
		return -EINVAL;
	}
	/*
	 * Check the fd is valid.
	 */
	if (unlikely(fcntl(fd, F_GETFD) == -1 && errno == EBADF)) {
		fprintf(stderr, "[libvista] vista_add_range() failed, invalid fd %d\n", fd);
		return -EINVAL;
	}

	/*
	 * Register the fd here.
	 */
	for (i = 0; i < channel->nr_fd; i++) {
		if (channel->fds[i] == fd)
			break;
	}
	if (i == channel->nr_fd)
		channel->fds[channel->nr_fd++] = fd;

	/*
	 * Check the range is valid.
	 * [start, end) so, end <= total pages.
	 */
	eof_pos = (unsigned long)lseek(fd, 0, SEEK_END);
	eof_pos = (eof_pos + VISTA_USER_PAGE_SIZE(channel) - 1) 
			  / VISTA_USER_PAGE_SIZE(channel); // Convert to page index.
	if (unlikely(start >= end || end > eof_pos)) {
		fprintf(stderr, "[libvista] vista_add_range() failed, invalid range [%lu, %lu) for fd %d with eof_pos %lu\n",
				start, end, fd, eof_pos);
		return -EINVAL;
	}

	/*
	 * No more than VISTA_MAX_NR_READ_RANGE ranges.
	 */
	if (unlikely(channel->nr_read_ranges >= VISTA_MAX_NR_READ_RANGE)) {
		fprintf(stderr, "[libvista] vista_add_range() failed, too many read ranges\n");
		return -ENOMEM;
	}

	/*
	 * Register the read range.
	 * You can think fd is weird, but, this is the index of the registered file.
	 * (with IOSQE_FIXED_FILE setup, the actual fd is not used)
	 */
	channel->read_ranges[channel->nr_read_ranges].fd = i;
	channel->read_ranges[channel->nr_read_ranges].start = start;
	channel->read_ranges[channel->nr_read_ranges].end = end;
	channel->nr_read_ranges++;

	channel->nr_remaining_page += end - start;

	return 0;
}

static bool vista_reread_data_if_needed(struct vista_channel *channel)
{
	vista_gen_flush_info_t curr_gen_flush, prev_gen_flush;
	struct vista_read_range reread_range = channel->buffer_read_ranges[channel->buffer_idx];
	unsigned int start_page_idx = reread_range.start + (channel->curr_buf_page_idx);
	unsigned int end_page_idx = reread_range.end;
	unsigned long remain_pages = end_page_idx - start_page_idx;
	unsigned int buffer_idx = channel->buffer_idx;
	unsigned int prev_gen, prev_is_flushing;
	unsigned int curr_gen, curr_is_flushing;
	unsigned long n;

	if (VISTA_CURRENT_BUFFER_STATUS(channel) & VISTA_SQE_SUBMITTED)
		return false; // Still I/O is in progress.

	/*
	 * Check if we need to reread the buffer.
	 * If the current buffer is safe to use, we do not need to reread it.
	 */
	if (VISTA_CURRENT_BUFFER_STATUS(channel) & VISTA_SAFE_BUFFER)
		return false;

	/*
	 * If the buffer is dangerous to use, we need to figure out whether we are in safe state or not.
	 * g == buf.g and f = true -> safe (There is a hashtable to determine this page is safe to use)
	 * Otherwise, we need to switch to reread mode.
	 */
	curr_gen_flush = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);
	if (curr_gen_flush.generation == VISTA_CURRENT_BUFFER_GEN(channel) && curr_gen_flush.is_flushing)
		return false;

	VISTA_MEASURE_UPDATE_COUNT(reread);
	/*
	 * We're now re-reading the remaining pages.
	 * Sounds weird, re-re-read can be happened. So, we need to consider it.
	 */
vista_retry_reread:
	bool should_retry = false;
	prev_gen_flush = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);
	n = pread(reread_range.fd,
		VISTA_CURRENT_BUFFER(channel) + (channel->curr_buf_page_idx * VISTA_USER_PAGE_SIZE(channel)),
		remain_pages * VISTA_USER_PAGE_SIZE(channel),
		start_page_idx * VISTA_USER_PAGE_SIZE(channel));
	curr_gen_flush = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);

	if (unlikely(n != remain_pages * VISTA_USER_PAGE_SIZE(channel))) {
		fprintf(stderr, "[libvista] vista_reread_data_if_needed() failed, pread() returned %zd, expected %lu\n",
			n, remain_pages * VISTA_USER_PAGE_SIZE(channel));
		// We should retry it immediately.
		goto vista_retry_reread;
	}

	prev_gen = prev_gen_flush.generation, prev_is_flushing = prev_gen_flush.is_flushing;
	curr_gen = curr_gen_flush.generation, curr_is_flushing = curr_gen_flush.is_flushing;

	// Case 1. State is changed safely (i.e., (g, nf) -> (g, nf))
	// This is the common case.
	if (likely(curr_gen == prev_gen && !prev_is_flushing && !curr_is_flushing)) {
		VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED | VISTA_SAFE_BUFFER;

	// Case 2.1. State is changed dangerously but okay to use when the OLTP hashtable is accessible. (i.e., (g, f) -> (g, f))
	// Alert it to the application.
	} else if (curr_gen == prev_gen && prev_is_flushing && curr_is_flushing) {
		VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED;
		should_retry = true;

	// Case 2.2. State is changed dangerously but okay to use when the OLTP hashtable is accessible. (i.e., (g, nf) -> (g+1, f))
	// Alert it to the application.
	} else if (curr_gen == prev_gen+1 && !prev_is_flushing && curr_is_flushing) {
		VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED;
		should_retry = true;

	// Case 3. State is changed dangerously and NEVER okay to use.
	} else {
		// We need to discard this buffer and reread it again. (very rarely happen)
		fprintf(stderr, "[libvista] WARNING: vista_reread_data_if_needed() retry reread, (prev_gen, prev_f) = (%u, %u), (curr_gen, curr_f) = (%u, %u)\n",
				prev_gen, prev_is_flushing, curr_gen, curr_is_flushing);
		goto vista_retry_reread;
	}

	VISTA_BUFFER_GEN(channel, buffer_idx) = curr_gen;
	return should_retry;
}

static bool vista_resubmit_sqe(struct vista_channel *channel, unsigned int buffer_idx)
{
	bool should_retry;
	struct io_uring_sqe *sqe;
	unsigned int io_size;
	unsigned long offset;
	int fd;
	struct vista_uring_user_data user_data;
	
	if (buffer_idx == channel->buffer_idx) {
		VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED; // Mark it as prefetched first. (reread function works for this status)
		should_retry = vista_reread_data_if_needed(channel); // We're doing pread for the current buffer. (reread)
		return should_retry;
	}

	user_data = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);
	for (int i = 0; i < (int)channel->nr_fd; ++i) {
		if (channel->fds[i] == channel->buffer_read_ranges[buffer_idx].fd) {
			fd = i;
			break;
		}
	}

	VISTA_LATENCY_START(submit_sqes);
	sqe = io_uring_get_sqe(VISTA_URING(channel));
	if (unlikely(!sqe)) {
		fprintf(stderr, "[libvista] vista_resubmit_sqe() failed, io_uring_get_sqe() returned NULL\n");
		return false;
	}

	io_size = channel->buffer_nr_page[buffer_idx] * VISTA_USER_PAGE_SIZE(channel);
	offset = channel->buffer_read_ranges[buffer_idx].start * VISTA_USER_PAGE_SIZE(channel);

	user_data.buffer_idx = buffer_idx;

	sqe->user_data = user_data.__raw;
	sqe->flags |= IOSQE_FIXED_FILE;

	io_uring_prep_read_fixed(sqe, fd, VISTA_BUFFER(channel, buffer_idx),
							 io_size,
							 offset,
							 0);
	VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_SQE_SUBMITTED;

	io_uring_submit(VISTA_URING(channel));
	VISTA_MEASURE_UPDATE_COUNT(submit_sqes);
	VISTA_LATENCY_END(submit_sqes);

	return false;
}

/*
 * Submit SQEs for empty buffers.
 */
static void vista_submit_sqes(struct vista_channel *channel)
{
	struct io_uring_sqe *sqe;
	unsigned int io_size, remain;
	unsigned int i;
	struct vista_uring_user_data user_data = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);

	VISTA_LATENCY_START(submit_sqes);
	/*
	 * Shortcut, the channel consumed all range provided.
	 */
	if (unlikely(channel->next_page_idx == VISTA_NO_MORE_PAGES_TO_READ))
		return;

	for (i = 0; i < channel->nr_buffer; ++i) {
		if (VISTA_BUFFER_STATUS(channel, i) == VISTA_EMPTY) {
			sqe = io_uring_get_sqe(VISTA_URING(channel));
			if (unlikely(!sqe)) {
				fprintf(stderr, "[libvista] vista_submit_sqes() failed, io_uring_get_sqe() returned NULL\n");
				return;
			}
			
			/*
			 * Calculate the remaining I/O size.
			 * Mostly, the I/O size will be 2 MiB, but for the last chunk,
			 * we need to calculate the actual remaining size.
			 */
			remain = VISTA_CURRENT_END_BYTE_OFFSET(channel) - VISTA_NEXT_READ_BYTE_OFFSET(channel);
			io_size = MIN(VISTA_IO_SIZE, remain);

			user_data.buffer_idx = i;

			sqe->user_data = user_data.__raw;
			sqe->flags |= IOSQE_FIXED_FILE;

			io_uring_prep_read_fixed(sqe, VISTA_CURRENT_READ_RANGE_FD(channel),
									 VISTA_BUFFER(channel, i),
									 io_size,
									 VISTA_NEXT_READ_BYTE_OFFSET(channel),
									 0);


			VISTA_BUFFER_STATUS(channel, i) = VISTA_SQE_SUBMITTED;
			channel->buffer_nr_page[i] = io_size / VISTA_USER_PAGE_SIZE(channel);
			channel->buffer_read_ranges[i].start = channel->next_page_idx;
			channel->buffer_read_ranges[i].end = channel->next_page_idx + (io_size / VISTA_USER_PAGE_SIZE(channel));
			if (channel->buffer_read_ranges[i].end > channel->read_ranges[channel->read_range_idx].end)
				channel->buffer_read_ranges[i].end = channel->read_ranges[channel->read_range_idx].end;

			// Looks weird, but, we need to set the actual fd here. (for later reread)
			// The fd in `channel->read_ranges` is just the index of the registered fd.
			// (with IOSQE_FIXED_FILE setup, the actual fd is not used)
			channel->buffer_read_ranges[i].fd = channel->fds[channel->read_ranges[channel->read_range_idx].fd];

			// Update the next page to read.
			channel->next_page_idx += io_size / VISTA_USER_PAGE_SIZE(channel);
			if (unlikely(channel->next_page_idx >= channel->read_ranges[channel->read_range_idx].end)) {
				// We can now move to next read range
				if (++channel->read_range_idx < channel->nr_read_ranges) {
					// Reset the next page index for the new read range
					channel->next_page_idx = channel->read_ranges[channel->read_range_idx].start;
				} else {
					// No more pages to read
					channel->next_page_idx = VISTA_NO_MORE_PAGES_TO_READ;
					channel->read_range_idx = VISTA_NO_MORE_PAGES_TO_READ;
					break;
				}
			}
		}
	}

	io_uring_submit(VISTA_URING(channel));
	VISTA_MEASURE_UPDATE_COUNT(submit_sqes);
	VISTA_LATENCY_END(submit_sqes);
}

static void vista_channel_setup_param(struct vista_channel *channel, struct vista_read_param *param)
{
	if (param) {
		memcpy(&channel->param, param, sizeof(struct vista_read_param));
	} else {
		memset(&channel->param, 0x0, sizeof(struct vista_read_param));
		channel->param.page_size = VISTA_PAGE_SIZE; // Default page size = 4KiB.
	}
}

static bool vista_validate_param(struct vista_read_param *param)
{
	// Check the param is valid.
	if (param) {
		// Set default page size if not provided.
		if (param->page_size == 0)
			param->page_size = VISTA_PAGE_SIZE; // Default page size = 4KiB.
	
		// `page_size` must be a power of 2 and it must be smaller than VISTA_IO_SIZE (2MiB).
		if ((param->page_size & (param->page_size - 1)) != 0 ||
			param->page_size > VISTA_IO_SIZE) {
			fprintf(stderr, "[libvista] vista_open() failed, invalid param->page_size: %lu\n", param->page_size);
			return false;
		}
	}
	return true;
}

static int vista_setup_data_buffer(struct vista_channel *channel)
{
	// Allocate a data buffer from the VMSeg.
	vista_alloc_data_buffer((void **)&channel->buffers, (void *)__addrs.olap_ptr);
	channel->nr_buffer = VISTA_NR_DATA_BUFFER_ENTRY;
	channel->buffer_status = (unsigned int *)calloc(channel->nr_buffer, sizeof(int));
	if (unlikely(!channel->buffer_status)) {
		fprintf(stderr, "[libvista] vista_setup_data_buffer() failed, calloc() returned NULL\n");
		return -ENOMEM;
	}
	channel->buffer_nr_page = (unsigned int *)calloc(channel->nr_buffer, sizeof(int));
	if (unlikely(!channel->buffer_nr_page)) {
		fprintf(stderr, "[libvista] vista_setup_data_buffer() failed, calloc() returned NULL\n");
		free(channel->buffer_status);
		return -ENOMEM;
	}
	channel->buffer_read_ranges = (struct vista_read_range *)calloc(channel->nr_buffer, sizeof(struct vista_read_range));
	if (unlikely(!channel->buffer_read_ranges)) {
		fprintf(stderr, "[libvista] vista_setup_data_buffer() failed, calloc() returned NULL\n");
		free(channel->buffer_status);
		free(channel->buffer_nr_page);
		return -ENOMEM;
	}
	channel->buffer_gen = (unsigned int *)calloc(channel->nr_buffer, sizeof(int));
	if (unlikely(!channel->buffer_gen)) {
		fprintf(stderr, "[libvista] vista_setup_data_buffer() failed, calloc() returned NULL\n");
		free(channel->buffer_status);
		free(channel->buffer_nr_page);
		free(channel->buffer_read_ranges);
		return -ENOMEM;
	}
	return 0;
}

/*
 * Open a new vista channel.
 * Returns a valid vista descriptor (vd) on success, or a negative error code on failure.
 */
VISTA_OLAP_API int vista_open(struct vista_io_range_vec *iov, struct vista_read_param *param)
{
	int ret, vd, i;
	struct vista_channel *channel;

	if (unlikely(!iov || iov->io_range_len == 0)) {
		fprintf(stderr, "[libvista] vista_open() failed, iov is missing\n");
		return -EINVAL;
	}

	if (!vista_validate_param(param)) {
		fprintf(stderr, "[libvista] vista_open() failed, invalid read_param\n");
		return -EINVAL;
	}

	vd = vista_chtable_add();
	if (unlikely(vd < 0)) {
		fprintf(stderr, "[libvista] vista_open() failed, vista_chtable_add() returned %d\n", vd);
		return -ENOMEM;
	}
	
	channel = vista_chtable_get(vd);
	vista_channel_init(channel);
	vista_channel_setup_param(channel, param);

	for (i = 0; i < (int)iov->io_range_len; ++i) {
		if ((ret = vista_add_range(vd, &iov->io_range_base[i])) != 0) {
			fprintf(stderr, "[libvista] vista_open() failed, vista_add_range() returned %d\n", ret);
			vista_chtable_del(vd);
			return ret;
		}
	}

	if (unlikely((ret = vista_setup_data_buffer(channel)) != 0)) {
		fprintf(stderr, "[libvista] vista_open() failed, vista_setup_data_buffer() returned %d\n", ret);
		vista_chtable_del(vd);
		return ret;
	}

	if (unlikely((ret = vista_channel_uring_init(channel)) != 0)) {
		fprintf(stderr, "[libvista] vista_open() failed, vista_channel_uring_init() returned %d\n", ret);
		vista_free_data_buffer(channel->buffers, (void *)__addrs.olap_ptr);
		free(channel->buffer_status);
		free(channel->buffer_nr_page);
		free(channel->buffer_read_ranges);
		free(channel->buffer_gen);
		vista_chtable_del(vd);
		return ret;
	}

	/*
	 * Submit very first I/O requests via io_uring.
	 */
	VISTA_SETUP_FIRST_READ(channel);
	vista_submit_sqes(channel);

	return vd;
}

VISTA_OLAP_API int vista_close(int vd)
{
	struct io_uring_cqe *cqe;
	unsigned int i, n, nr_submitted = 0;
	unsigned head;
	struct vista_channel *channel = vista_chtable_get(vd);
	if (unlikely(!channel)) {
		fprintf(stderr, "[libvista] vista_close() failed, invalid vd %d\n", vd);
		return -EINVAL;
	}

	if (unlikely(channel->nr_remaining_page)) {
		/*
		 * Query can be terminated before reading all pages. (e.g., LIMIT in SQL)
		 * In this case, we just clean up the channel.
		 */
		
		for (i = 0; i < channel->nr_buffer; ++i) {
			if (VISTA_BUFFER_STATUS(channel, i) == VISTA_SQE_SUBMITTED)
				nr_submitted++;
		}

		while (nr_submitted) {
			n = 0;

			io_uring_for_each_cqe(VISTA_URING(channel), head, cqe) {
				nr_submitted--;
				n++;
			}

			if (n)
				io_uring_cq_advance(VISTA_URING(channel), n);
		}
	}

	io_uring_queue_exit(VISTA_URING(channel));
	vista_free_data_buffer(channel->buffers, (void *)__addrs.olap_ptr);
	free(channel->buffer_status);
	free(channel->buffer_nr_page);
	free(channel->buffer_read_ranges);
	free(channel->buffer_gen);

	vista_chtable_del(vd);
	return 0;
}

static bool vista_handle_cqes(struct vista_channel *channel, bool should_drop)
{
	bool should_retry = false;
	struct io_uring_cqe *cqe;
	unsigned head;
	unsigned n = 0;
	vista_gen_flush_info_t gen_flush = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);
	unsigned int curr_gen = gen_flush.generation;
	unsigned int curr_is_flushing = gen_flush.is_flushing;

	io_uring_for_each_cqe(VISTA_URING(channel), head, cqe) {
		struct vista_uring_user_data user_data = *(struct vista_uring_user_data *)(&cqe->user_data);
		unsigned int prev_gen = user_data.generation;
		unsigned int prev_is_flushing = user_data.is_flushing;
		unsigned int buffer_idx = user_data.buffer_idx;

		n++;

		if (unlikely(cqe->res < 0)) { 
			fprintf(stderr, "[libvista] vista_handle_cqes() failed, cqe->res = %d\n", cqe->res);
			VISTA_BUFFER_STATUS(channel, user_data.buffer_idx) = VISTA_EMPTY; // We should retry it later.
			continue;
		} 

		// Happy case (i.e., read cache)
		if (!should_drop) {
			VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED | VISTA_SAFE_BUFFER;
			continue;
		}

		// Case 1. State is changed safely (i.e., (g, nf) -> (g, nf))
		if (curr_gen == prev_gen && !prev_is_flushing && !curr_is_flushing) {
			VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED | VISTA_SAFE_BUFFER; 

		// Case 2.1. State is changed dangerously but okay to use when the OLTP hashtable is accessible. (i.e., (g, f) -> (g, f))
		} else if (curr_gen == prev_gen && prev_is_flushing && curr_is_flushing) {
			VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED;

		// Case 2.2. State is changed dangerously but okay to use when the OLTP hashtable is accessible. (i.e., (g, nf) -> (g+1, f))
		} else if (curr_gen == prev_gen+1 && !prev_is_flushing && curr_is_flushing) {
			VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_PREFETCHED;

		// Case 3. State is changed dangerously and NEVER okay to use.
		} else {
			// We need to discard this buffer and reread it later.
			VISTA_BUFFER_STATUS(channel, buffer_idx) = VISTA_EMPTY;
			should_retry |= vista_resubmit_sqe(channel, buffer_idx);
			continue;
		}
		
		VISTA_BUFFER_GEN(channel, buffer_idx) = curr_gen;
	}

	if (n)
		io_uring_cq_advance(VISTA_URING(channel), n);

	return should_retry;
}

VISTA_OLAP_API void *vista_get_page(int vd, unsigned int gen)
{
	bool should_retry = false;
	struct vista_channel *channel;
	VISTA_LATENCY_START(get_page);
	channel = vista_chtable_get(vd);
	if (unlikely(!channel)) {
		fprintf(stderr, "[libvista] vista_get_page() failed, invalid vd %d\n", vd);
		return (void *)VISTA_INVALID_VD;
	}

	if (unlikely(!channel->put_page_called)) {
		fprintf(stderr, "[libvista] vista_get_page() failed, vista_get_page() cannot be called before vista_put_page() for last page\n");
		return (void *)VISTA_NO_PUT_PAGE_CALLED;
	}

	if (unlikely(!channel->nr_remaining_page))
		return (void *)VISTA_NO_MORE_PAGES_TO_READ;

	if (gen != VISTA_NO_REMAP_ACCESS)
		should_retry = vista_reread_data_if_needed(channel);

	VISTA_LATENCY_START(wait_cqe);
	while (unlikely((VISTA_CURRENT_BUFFER_STATUS(channel) & VISTA_PREFETCHED) == 0))
		should_retry |= vista_handle_cqes(channel, gen != VISTA_NO_REMAP_ACCESS);
	VISTA_MEASURE_UPDATE_COUNT(wait_cqe);
	VISTA_LATENCY_END(wait_cqe);

	if (gen != VISTA_NO_REMAP_ACCESS) {
		vista_gen_flush_info_t curr_gen_flush = vista_get_gen_flush_info((void *)__addrs.shrd_metadata_ptr);
		if (unlikely(gen != curr_gen_flush.generation || should_retry)) {
			channel->put_page_called = true; // To allow next get_page
			return (void *)VISTA_RETRY_REMAP_ACCESS;
		}
	}

	/*
	* We're okay to read the page from the current buffer.
	*/
	channel->page_base = VISTA_CURRENT_BUFFER(channel)
						 + (channel->curr_buf_page_idx * VISTA_USER_PAGE_SIZE(channel));
	channel->put_page_called = false;

	VISTA_MEASURE_UPDATE_COUNT(get_page);
	VISTA_LATENCY_END(get_page);

	return channel->page_base;
}

VISTA_OLAP_API int vista_put_page(int vd)
{
	struct vista_channel *channel;
	VISTA_LATENCY_START(put_page);

	channel = vista_chtable_get(vd);

	if (unlikely(!channel)) {
		fprintf(stderr, "[libvista] vista_put_page() failed, invalid vd %d\n", vd);
		return -EINVAL;
	}

	if (unlikely(channel->put_page_called)) {
		fprintf(stderr, "[libvista] vista_put_page() failed, put_page called more than once without get_page\n");
		return -EINVAL;
	}

	channel->put_page_called = true;
	channel->nr_remaining_page--;
	if (++channel->curr_buf_page_idx == channel->buffer_nr_page[channel->buffer_idx]) {
		// We've consumed all pages in the current buffer, move to next buffer.
		VISTA_BUFFER_STATUS(channel, channel->buffer_idx) = VISTA_EMPTY;
		channel->curr_buf_page_idx = 0;

		// Move to the next buffer.
		VISTA_MOVE_TO_NEXT_BUFFER(channel);

		// Submit SQEs for empty buffers.
		vista_submit_sqes(channel);
	}

	VISTA_MEASURE_UPDATE_COUNT(put_page);
	VISTA_LATENCY_END(put_page);
	return 0;
}

VISTA_OLAP_API int vista_unmap(void)
{
	long ret;
	if ((ret = syscall(NR_SYSCALL_vista_unmap, __addrs.remap_window_ptr))) {
		fprintf(stderr, "[libvista] vista_unmap() failed, syscall, %ld\n", ret);
		return ret;
	}
	return 0;
}

VISTA_OLTP_API int vista_remap(unsigned int idx)
{
	long ret;
	if ((ret = syscall(NR_SYSCALL_vista_remap, idx))) {
		fprintf(stderr, "[libvista] vista_remap() failed, syscall, %ld\n", ret);
		return ret;
	}
	return 0;
}

VISTA_API void vista_dummy()
{
    fprintf(stderr, "[libvista] You called a dummy function. It does nothing.\n");
    return;
}

/*
 * Initialize the VMSeg memory allocator.
 * It should be called only once at the DBMS startup.
 */
VISTA_OLTP_API void vista_vmseg_allocator_init(vista_vmseg_allocator *allocator, void *base)
{
	char* aligned;

	allocator->VmsegBase = base;
	allocator->VmsegEnd = (char*) base + (1UL << 30); // 1GB VMSeg size
	allocator->freeoffset = 0;

	// Although the VMSeg huge pages are expected to be aligned to 1GB,
	// we align the base address and freeoffset to the cacheline size just in case.
	aligned = (char *) VISTA_CACHELINE_ALIGN((char *) base); // since freeoffset is 0, we can align the base address directly.
	allocator->freeoffset = aligned - (char *) base; 
}

/*
 * Allocate a memory from the VMSeg.
 * The allocator should be initialized with vista_vmseg_allocator_init() before calling this function.
 * It returns the pointer to the allocated memory, or NULL if the allocation fails.
 */
VISTA_OLTP_API void* vista_vmseg_alloc(vista_vmseg_allocator *allocator, size_t size)
{
	size_t newStart;
	size_t newFree;
	void* newSpace;
	size = VISTA_CACHELINE_ALIGN(size); // Align the size to cacheline size.

	newStart = allocator->freeoffset;
	newFree = newStart + size;

	if (newFree > (size_t) (1UL << 30)) {
		fprintf(stderr, "[VISTA] vista_vmseg_alloc() failed, not enough memory in VMSeg.\n");
		return NULL;
	} else {
		newSpace = (void*) ((char *) allocator->VmsegBase + newStart);
		allocator->freeoffset = newFree;
	}

	return newSpace;
}

VISTA_OLTP_API void vista_shmem_allocator_init(vista_shmem_allocator *allocator, void *base)
{
	char* aligned;
	uint32_t offset;

	allocator->ShmemBase = base;
	allocator->ShmemEnd = (char*) base + VISTA_SHMEM_SIZE;
	
    aligned = (char *) VISTA_CACHELINE_ALIGN((char *) base);
	offset = aligned - (char *) base;

	if (offset == 0) {
		offset = VISTA_CACHELINE_SIZE;
		aligned += VISTA_CACHELINE_SIZE;
	}

	*((uint32_t *) base) = offset;
	allocator->AlignedBase = aligned;
	allocator->freeoffset = 0;
}

VISTA_OLTP_API void* vista_shmem_alloc(vista_shmem_allocator *allocator, size_t size)
{
	size_t newStart;
	size_t newFree;
	void* newSpace;
	size = VISTA_CACHELINE_ALIGN(size);

	newStart = allocator->freeoffset;
	newFree = newStart + size;

	if ((char*)allocator->AlignedBase + newFree > (char*)allocator->ShmemEnd) {
		fprintf(stderr, "[VISTA] vista_shmem_alloc() failed, not enough memory in shmem.\n");
		return NULL;
	} else {
		newSpace = (void*) ((char *) allocator->AlignedBase + newStart);
		allocator->freeoffset = newFree;
	}

	return newSpace;
}

