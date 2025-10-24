/*-------------------------------------------------------------------------
 *
 * buf_init.c
 *	  buffer manager initialization routines
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_init.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/vista_internal_bitmap.h"

#define N_POOLS 3

vista_BufferPoolControlPadded *vista_BufCtl;
vista_bufferpool_offsets *vista_BufOffsets;
vista_SegmentHeader *vista_SegHdr[N_POOLS];
VistaBitmapHeader *vista_BitmapMgr;

struct vista_addrs vista_shmem_addrs;

BufferDescPadded *vista_BufferDescriptors[N_POOLS];
char	   *vista_BufferBlocks[N_POOLS];
ConditionVariableMinimallyPadded *vista_BufferIOCVArray[N_POOLS];
WritebackContext vista_BackendWritebackContext[N_POOLS];
CkptSortItem *vista_CkptBufferIds[N_POOLS];
// uint32 private_seg_refcount[N_POOLS][2]; // 0 -> NonFlushing, 1 -> Flushing

uint8 *seg_refcount[N_POOLS]; // 0-> unset, 1->pinned during non-flushing, 2->pinned during flushing

// XXXVISTA: we use N_POOLS + 1(shared metadata region) VMSegs. Each VMSeg requires its own allocator.
vista_vmseg_allocator vista_VmSegAllocatorPerPool[N_POOLS];
vista_shmem_allocator vista_ShmemAllocator;
static void vista_numa_init_oltp(void);
/*
 * Data Structures:
 *		buffers live in a freelist and a lookup data structure.
 *
 *
 * Buffer Lookup:
 *		Two important notes.  First, the buffer has to be
 *		available for lookup BEFORE an IO begins.  Otherwise
 *		a second process trying to read the buffer will
 *		allocate its own copy and the buffer pool will
 *		become inconsistent.
 *
 * Buffer Replacement:
 *		see freelist.c.  A buffer cannot be replaced while in
 *		use either by data manager or during IO.
 *
 *
 * Synchronization/Locking:
 *
 * IO_IN_PROGRESS -- this is a flag in the buffer descriptor.
 *		It must be set when an IO is initiated and cleared at
 *		the end of the IO.  It is there to make sure that one
 *		process doesn't start to use a buffer while another is
 *		faulting it in.  see WaitIO and related routines.
 *
 * refcount --	Counts the number of processes holding pins on a buffer.
 *		A buffer is pinned during IO and immediately after a BufferAlloc().
 *		Pins must be released before end of transaction.  For efficiency the
 *		shared refcount isn't increased if an individual backend pins a buffer
 *		multiple times. Check the PrivateRefCount infrastructure in bufmgr.c.
 */


/*
 * Initialize shared buffer pool
 * (VISTA) We enable multiple buffer pools. In the very first prototype, we simply initialize
 * the same buffer pool multiple times. 
 *
 * This is called once during shared-memory initialization (either in the
 * postmaster, or in a standalone backend).
 */
void
InitBufferPool(void)
{
	int			i;
	int 		pool_id;
	// VISTA
	void *vmseg_base;
	vista_SegmentHeader *seg_hdr;			
	vista_BufferPoolControl *ctl;

	elog(LOG, "[VISTA] Initializing buffer pool");

	// VISTA: setup the NUMA
	vista_numa_init_oltp();

	// VISTA: Since VMSegs are only used for buffer pool, we can allocate and initialize it here.
	if (vista_register(N_POOLS, VISTA_MAP_OLTP, &vista_shmem_addrs) != 0) {
		ereport(FATAL,
				(errmsg("vista_register failed for OLTP buffer pool initialization")));
	}
	vista_init_vmseg(N_POOLS, VISTA_MAP_OLTP, &vista_shmem_addrs); // zero out the VMSeg memory

	// We use special VMSeg and allocator for the shared metadata region.
	vista_shmem_allocator_init(&vista_ShmemAllocator, (void*) vista_shmem_addrs.shrd_metadata_ptr);

    vista_BufOffsets = (vista_bufferpool_offsets *) vista_shmem_alloc(&vista_ShmemAllocator,
                        sizeof(vista_bufferpool_offsets));
	if (vista_BufOffsets == NULL) {
		ereport(FATAL, errmsg("vista_shmem_alloc failed for vista_bufferpool_offsets"));
	}

    vista_BufOffsets->shmem_base_addr = (uint64_t) vista_ShmemAllocator.ShmemBase; 

    // VISTA: Buffer Pool Control structure is located in the shared metadata region.
	vista_BufCtl = (vista_BufferPoolControlPadded *) vista_shmem_alloc(&vista_ShmemAllocator,
					sizeof(vista_BufferPoolControlPadded));
	if (vista_BufCtl == NULL) {
		ereport(FATAL, errmsg("vista_shmem_alloc failed for vista_BufferPoolControl"));
	}

	vista_BufOffsets->buf_ctl_offset = (uint32_t) ((char *) vista_BufCtl - (char *) vista_ShmemAllocator.ShmemBase);

	elog(LOG, "[VISTA] VMSeg initialized, now initializing the buffer pool");

	vmseg_base = (void*)vista_shmem_addrs.oltp_ptr;

	for (pool_id = 0; pool_id < N_POOLS; pool_id++) {

		// XXXVISTA: initialize VMSeg for each pool.
		// As of 250815, multiple huge pages are allocated starting from addrs.oltp_ptr.
		vista_vmseg_allocator_init(&vista_VmSegAllocatorPerPool[pool_id], (char*) vmseg_base +  pool_id * (1UL << 30));
		vista_BufOffsets->vmseg_base_addr[pool_id] = (uint64_t) vista_VmSegAllocatorPerPool[pool_id].VmsegBase;

		vista_SegHdr[pool_id] = (vista_SegmentHeader *) vista_vmseg_alloc(&vista_VmSegAllocatorPerPool[pool_id],
								sizeof(vista_SegmentHeader));
		if (vista_SegHdr[pool_id] == NULL) {
			ereport(FATAL, errmsg("vista_vmseg_alloc failed for vista_SegmentHeader"));
		}

		// XXXVISTA: Currently, we set the config parameter for shared buffers to be 998MB.

		/* Align descriptors to a cacheline boundary. */
		vista_BufferDescriptors[pool_id] = (BufferDescPadded *) vista_vmseg_alloc(&vista_VmSegAllocatorPerPool[pool_id],
										NBuffers * sizeof(BufferDescPadded));
		if (vista_BufferDescriptors[pool_id] == NULL) {
			ereport(FATAL, errmsg("vista_vmseg_alloc failed for vista_BufferDescriptors"));
		}

		vista_BufferBlocks[pool_id] = (char *) vista_vmseg_alloc(&vista_VmSegAllocatorPerPool[pool_id],
									NBuffers * (Size) BLCKSZ);
		if (vista_BufferBlocks[pool_id] == NULL) {
			ereport(FATAL, errmsg("vista_vmseg_alloc failed for vista_BufferBlocks"));
		}

		/* Align condition variables to cacheline boundary. */
		vista_BufferIOCVArray[pool_id] = (ConditionVariableMinimallyPadded *)
			vista_vmseg_alloc(&vista_VmSegAllocatorPerPool[pool_id],
							  NBuffers * sizeof(ConditionVariableMinimallyPadded));
		if (vista_BufferIOCVArray[pool_id] == NULL) {
			ereport(FATAL, errmsg("vista_vmseg_alloc failed for vista_BufferIOCVArray"));
		}

		/*
		* The array used to sort to-be-checkpointed buffer ids is located in
		* shared memory, to avoid having to allocate significant amounts of
		* memory at runtime. As that'd be in the middle of a checkpoint, or when
		* the checkpointer is restarted, memory allocation failures would be
		* painful.
		*/

		vista_CkptBufferIds[pool_id] = (CkptSortItem *) vista_vmseg_alloc(&vista_VmSegAllocatorPerPool[pool_id],
										NBuffers * sizeof(CkptSortItem));
		if (vista_CkptBufferIds[pool_id] == NULL) {
			ereport(FATAL, errmsg("vista_vmseg_alloc failed for vista_CkptBufferIds"));
		}

		vista_BufOffsets->seg_hdr_offset[pool_id] = (uint32_t) ((char *) vista_SegHdr[pool_id] - (char *) vista_VmSegAllocatorPerPool[pool_id].VmsegBase);
		vista_BufOffsets->buf_desc_offset[pool_id] = (uint32_t) ((char *) vista_BufferDescriptors[pool_id] - (char *) vista_VmSegAllocatorPerPool[pool_id].VmsegBase);
		vista_BufOffsets->buf_blocks_offset[pool_id] = (uint32_t) ((char *) vista_BufferBlocks[pool_id] - (char *) vista_VmSegAllocatorPerPool[pool_id].VmsegBase);
		// vista_BufOffsets->buf_iocv_offset[pool_id] = (uint32_t) ((char *) vista_BufferIOCVArray[pool_id] - (char *) vista_VmSegAllocatorPerPool[pool_id].VmsegBase);
		// vista_BufOffsets->ckpt_buf_ids_offset[pool_id] = (uint32_t) ((char *) vista_CkptBufferIds[pool_id] - (char *) vista_VmSegAllocatorPerPool[pool_id].VmsegBase);


		// XXXVISTA: currently we do not support EXEC_BACKEND case.
		// To do so, we will need to manage whether the VMSeg memory is already allocated or not
		// for specific requests.
		// This can be easily fixed by having something similar to ShmemIndex.
		// if (foundDescs || foundBufs || foundIOCV || foundBufCkpt || foundSegHdr)
		// {
		// 	/* should find all of these, or none of them */
		// 	Assert(foundDescs && foundBufs && foundIOCV && foundBufCkpt && foundSegHdr);
		// 	/* note: this path is only taken in EXEC_BACKEND case */
		// }
		// else
		// {
		/* Initialize the Segment Header */
		seg_hdr = vista_GetSegmentHeader(pool_id);			
		seg_hdr->seg_generation = 1;
		pg_atomic_init_u32(&seg_hdr->seg_state, VISTA_SEGSTATE_NORMAL);
		pg_atomic_init_u32(&seg_hdr->seg_refcount_active,     0);
		pg_atomic_init_u32(&seg_hdr->seg_refcount_sealed,  0);
		seg_hdr->seg_pool_id = pool_id;
		vista_seqlock_init(&seg_hdr->seg_seqlock);
		// LWLockInitialize(&seg_hdr->entry_lock, LWTRANCHE_BUFFER_SEGMENT_ENTRY);
		/*
		* Initialize all the buffer headers.
		* (VISTA) We initialize the descriptors to have a contiguous ids,
		* regardless of the pool they belong to.
		* Then, the buf_id can be used to identify the pool by dividing it by NBuffers.
		*/
		for (i = pool_id * NBuffers; i < (pool_id + 1) * NBuffers; i++)
		{
			BufferDesc *buf = vista_GetBufferDescriptor(i);

			CLEAR_BUFFERTAG(buf->tag);

			pg_atomic_init_u32(&buf->state, 0);
			buf->wait_backend_pgprocno = INVALID_PGPROCNO;

			buf->buf_id = i;

			/*
			* Initially link all the buffers together as unused. Subsequent
			* management of this list is done by freelist.c.
			*/
			buf->freeNext = i + 1;

			pg_atomic_init_flag(&buf->being_copied);
			pg_atomic_init_flag(&buf->copy_done);

			LWLockInitialize(BufferDescriptorGetContentLock(buf),
							LWTRANCHE_BUFFER_CONTENT);

			ConditionVariableInit(vista_BufferDescriptorGetIOCV(buf));
		}

		/* Correct last entry of linked list */
		vista_GetBufferDescriptor((pool_id + 1) * NBuffers - 1)->freeNext = FREENEXT_END_OF_LIST;
		// }

		/* Init other shared buffer-management stuff */
		vista_StrategyInitialize(pool_id);

		/* Initialize per-backend file flush context */
		WritebackContextInit(&vista_BackendWritebackContext[pool_id],
							&backend_flush_after);

		/* VISTA: logging for vista shmem debugging */
		elog(LOG, "[VISTA] VMSeg Base for Pool %d: %p", pool_id, vista_VmSegAllocatorPerPool[pool_id].VmsegBase);
		elog(LOG, "[VISTA] Initialized vista_SegmentHeader for Pool %d: %p", pool_id, vista_SegHdr[pool_id]);
		elog(LOG, "[VISTA] Initialized vista_BufferDescriptors for Pool %d: %p", pool_id, vista_BufferDescriptors[pool_id]);
		elog(LOG, "[VISTA] Initialized vista_BufferBlocks for Pool %d: %p", pool_id, vista_BufferBlocks[pool_id]);
		elog(LOG, "[VISTA] Initialized vista_BufferIOCVArray for Pool %d: %p", pool_id, vista_BufferIOCVArray[pool_id]);
		elog(LOG, "[VISTA] Initialized vista_CkptBufferIds for Pool %d: %p", pool_id, vista_CkptBufferIds[pool_id]);
		elog(LOG, "[VISTA] VmSeg's current free offset for Pool %d: %u", pool_id, vista_VmSegAllocatorPerPool[pool_id].freeoffset);
	}

	vista_InitSnapshotHash();

	vista_BitmapMgr = (VistaBitmapHeader *) vista_shmem_alloc(&vista_ShmemAllocator, vista_calculate_bitmap_shmem_size(MAX_TABLES, NUM_CACHEBLOCKS));
	if (vista_BitmapMgr == NULL) {
		ereport(FATAL, errmsg("vista_shmem_alloc failed for vista_BitmapMgr"));
	}
	vista_bitmap_manager_init((void*) vista_BitmapMgr, MAX_TABLES, NUM_CACHEBLOCKS);

	vista_BufOffsets->bitmap_mgr_offset = (uint32_t) ((char *) vista_BitmapMgr - (char *) vista_ShmemAllocator.ShmemBase);
	
    elog(LOG, "[VISTA] Initialized cache bitmap: %p", vista_BitmapMgr);
    
	elog(LOG, "[VISTA] Shmem allocator used %u bytes",
		 vista_ShmemAllocator.freeoffset);

	// Now we can initialize the buffer pool control structure.
	ctl = vista_GetBufferCtl(vista_BufCtl);
	vista_seqlock_init(&ctl->ctl_seqlock);
	ctl->global_generation = 1;
	pg_atomic_init_u32(&ctl->snapshot_generation, 0);
	ctl->vista_op_mode = VISTA_MODE_NORMAL;
	ctl->active_segment = vista_GetSegmentHeader(POOL_OLTP1);
	ctl->sealed_segment = vista_GetSegmentHeader(POOL_OLTP2);
	ctl->flushbgwprocno = -1; /* No background worker is flushing yet */
	ctl->initial_snapshot_ready = false;
	ctl->oldest_olap_generation = 0;
	pg_atomic_init_u32(&ctl->remap_count, 0);

    elog(LOG, "[VISTA] Initialized Buffer Pool Control.");
}

/*
 * BufferShmemSize
 *
 * compute the size of shared memory for the buffer pool including
 * data pages, buffer descriptors, hash tables, etc.
 */
Size
BufferShmemSize(void)
{
	Size		size = 0;
	Size special_size = 0;

	special_size = add_size(special_size, sizeof(vista_BufferPoolControlPadded));
	special_size = add_size(special_size, PG_CACHE_LINE_SIZE);

	special_size = add_size(special_size, SnapshotShmemSize());

	size = add_size(size, sizeof(vista_SegmentHeader));
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of buffer descriptors */
	size = add_size(size, mul_size(NBuffers, sizeof(BufferDescPadded)));
	/* to allow aligning buffer descriptors */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of data pages */
	size = add_size(size, mul_size(NBuffers, BLCKSZ));

	/* size of stuff controlled by freelist.c */
	size = add_size(size, StrategyShmemSize());

	/* size of I/O condition variables */
	size = add_size(size, mul_size(NBuffers,
								   sizeof(ConditionVariableMinimallyPadded)));
	/* to allow aligning the above */
	size = add_size(size, PG_CACHE_LINE_SIZE);

	/* size of checkpoint sort array in bufmgr.c */
	size = add_size(size, mul_size(NBuffers, sizeof(CkptSortItem)));

	size = mul_size(size, N_POOLS); // Multiply by the number of pools
	// So we have space for segment headers of all pools, although we use them only for OLTP pools.

	size = add_size(size, special_size);

	return size;
}

// static void
// vista_RefCountCleanUp(int code, Datum arg)
// {
// 	int pool_i;
// 	bool is_flushing = (vista_BufCtl->ctl.flush_state == DURING_FLUSHING);
// 	for (pool_i = POOL_OLTP1; pool_i < POOL_OLTP2 + 1; pool_i++)
// 	{
// 		uint32 leftover = private_seg_refcount[pool_i];
// 		if (leftover > 0)
// 		{
// 			if (is_flushing)
// 			{

// 			}
// 			pg_atomic_sub_fetch_u32()
// 		}
// 	}
// }

/*
 * vista_numa_init
 *		Initialize NUMA settings for VISTA.
 *
 * This is called once during shared-memory initialization, in InitBufferPool().
*/
static void
vista_numa_init_oltp() {
	struct bitmask *mask;

	if (numa_available() < 0) {
		ereport(FATAL,
				(errmsg("NUMA is not available on this system.")));
	}

	if (numa_max_node() < 1) {
		ereport(FATAL,
				(errmsg("libvista requires at least 2 NUMA nodes.")));
	}

	mask = numa_allocate_nodemask();
	if (!mask) {
		ereport(FATAL,
				(errmsg("Failed to allocate NUMA node mask.")));
	}
	numa_bitmask_setbit(mask, VISTA_NUMA_OLTP);
	if (numa_run_on_node_mask(mask) < 0) {
		ereport(FATAL,
				(errmsg("Failed to set NUMA node for OLTP.")));
	}
	
	numa_free_nodemask(mask);

	elog(LOG, "[VISTA] Postmaster initialized on NUMA node %d", VISTA_NUMA_OLTP);
}
