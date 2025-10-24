#include "storage/vista_flushworker.h"
#include "storage/vista_freelist.h"
#include "utils/hsearch.h"

#include <time.h>
#include <stdio.h>

#include <liburing.h>
#include "storage/md.h"
#include "lib/ilist.h"
#include "storage/fd.h"
#include "storage/vista_internal_bitmap.h"
#include <stdatomic.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/multixact.h"
#include "access/subtrans.h"
#include "storage/sync.h"

// VISTA: For io_uring
#define QUEUE_DEPTH 4096

#define SMGR_CACHE_SIZE 64

typedef struct DirtyBufferInfo
{
	BufferDesc *buf;
	File		file;
	uint32	    table_id;
} DirtyBufferInfo;

static void vista_flush_buffers_uring(BufferPoolId pool_id);

int VistaFlusherDelay = 100; // 100 ms delay for the flush worker
int first_noop = 1;

// static void vista_flush_one_segment(void);
// static void vista_flush_buffers(BufferPoolId pool_id);
static bool vista_should_flush_now(int rc);
static void clean_old_segment(vista_SegmentHeader *old_seg);
static void flush_segment_and_update_bitmap(void);
static void vista_flush_buffers_uring_with_info(DirtyBufferInfo *dirty_buffers, int num_dirty);

#define vista_BufHdrGetBlock(bufHdr) \
	((Block) (vista_BufferBlocks[vista_PoolId(bufHdr->buf_id)] + \
			  ((Size) vista_SlotId(bufHdr->buf_id)) * BLCKSZ))
#define vista_BufferGetLSN(bufHdr) \
    (PageGetLSN(vista_BufHdrGetBlock(bufHdr)))

void VistaFlushWorkerMain(void)
{
    sigjmp_buf local_sigjmp_buf;
    MemoryContext flushworker_context;
    vista_BufferPoolControl *ctl;

    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGINT, SIG_IGN);
    pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
    pqsignal(SIGALRM, SIG_IGN);
    pqsignal(SIGPIPE, SIG_IGN);
    pqsignal(SIGUSR1, procsignal_sigusr1_handler);
    pqsignal(SIGUSR2, SIG_IGN);   

    pqsignal(SIGCHLD, SIG_DFL);

    flushworker_context = AllocSetContextCreate(TopMemoryContext,
                                                "Vista Flush Worker",
                                                ALLOCSET_DEFAULT_SIZES);

    MemoryContextSwitchTo(flushworker_context);

	vista_InitSyncForFlushWorker(flushworker_context);

	MemSet(&CheckpointStats, 0, sizeof(CheckpointStats));

   if (sigsetjmp(local_sigjmp_buf, 1) != 0)
    {
        error_context_stack = NULL;
        HOLD_INTERRUPTS();
        EmitErrorReport();

        LWLockReleaseAll();
        ConditionVariableCancelSleep();
        AbortBufferIO();
        UnlockBuffers();
        ReleaseAuxProcessResources(false);
        AtEOXact_Buffers(false);
        AtEOXact_SMgr();
        AtEOXact_Files(false);
        AtEOXact_HashTables(false);

        MemoryContextSwitchTo(flushworker_context);
        FlushErrorState();

        MemoryContextResetAndDeleteChildren(flushworker_context);

        RESUME_INTERRUPTS();

        pg_usleep(1000000L); // Sleep 1s after error

        smgrcloseall();

        pgstat_report_wait_end();
    }

    PG_exception_stack = &local_sigjmp_buf;

    PG_SETMASK(&UnBlockSig);

    elog(LOG, "Vista Flush Worker started - Just before accessing VMSeg");
	ctl = vista_GetBufferCtl(vista_BufCtl);
    // Need to let the buffer manager know the flush worker's procno
    ctl->flushbgwprocno = MyProc->pgprocno;

    for (;;)
    {
        bool need_flush = false;
        int rc;
        struct timespec start, end;
        long elapsed_time;


        rc = WaitLatch(MyLatch,
                       WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                       VistaFlusherDelay /* ms */, WAIT_EVENT_VISTA_FLUSH_WORKER_MAIN); // Wait for 0.1 second
        
        ResetLatch(MyLatch);

        HandleMainLoopInterrupts();
        if (ShutdownRequestPending || ProcDiePending)
        {
             break;
        }


        need_flush = vista_should_flush_now(rc);
        
        if (need_flush)
        {
            // compute the segment flush latency.
            clock_gettime(CLOCK_MONOTONIC, &start);
            flush_segment_and_update_bitmap();
            clock_gettime(CLOCK_MONOTONIC, &end);
            elapsed_time = (end.tv_sec - start.tv_sec) * 1000000L + (end.tv_nsec - start.tv_nsec) / 1000L; // in microseconds
            elog(LOG, "Flushed Segment generation %u in %ld microseconds", ctl->global_generation - 1, elapsed_time);
        }
    }

    proc_exit(0);
}


/*
 * Comparison function for qsort. Sorts buffers based on:
 * 1. table_id (for cache-friendly bitmap updates)
 * 2. file handle (for efficient io_uring submission)
 * 3. block number
 */
static int
compare_dirty_buffer_info_for_bitmap(const void *a, const void *b)
{
	const DirtyBufferInfo *da = (const DirtyBufferInfo *) a;
	const DirtyBufferInfo *db = (const DirtyBufferInfo *) b;

	if (da->table_id < db->table_id)
		return -1;
	if (da->table_id > db->table_id)
		return 1;

	if (da->file < db->file)
		return -1;
	if (da->file > db->file)
		return 1;

	if (da->buf->tag.blockNum < db->buf->tag.blockNum)
		return -1;
	if (da->buf->tag.blockNum > db->buf->tag.blockNum)
		return 1;

	return 0;
}

static void
flush_segment_and_update_bitmap(void)
{
	vista_BufferPoolControl *ctl = vista_GetBufferCtl(vista_BufCtl);
	vista_SegmentHeader *old_seg, *new_seg;
	BufferPoolId pool_id;
	uint32 current_generation;
	int i;
	XLogRecPtr max_lsn_recptr = InvalidXLogRecPtr;

	/* Collect dirty buffer information */
	DirtyBufferInfo *dirty_buffers;
	int num_dirty = 0;

	/* Keep track of modified table IDs during this flush period */
	uint32 modified_table_ids[MAX_TABLES];
	int num_modified_tables = 0;
	
	typedef struct SMgrRelationCacheEntry
	{
		RelFileNode rnode;
		SMgrRelation rel;
	} SMgrRelationCacheEntry;

	/* Used to efficiently cache SMgrRelation lookups */
	SMgrRelationCacheEntry smgr_cache[SMGR_CACHE_SIZE];
	int num_smgr_cache_entries = 0;

	/* Standard segment swap logic */
	old_seg = ctl->active_segment;
	new_seg = ctl->sealed_segment;
	pool_id = old_seg->seg_pool_id;

	/* Start flush period */
	vista_seqlock_write_begin(&ctl->ctl_seqlock);
	Assert(ctl->vista_op_mode == VISTA_MODE_NORMAL);
	ctl->vista_op_mode = VISTA_MODE_FLUSH;
	ctl->active_segment = new_seg;
	ctl->sealed_segment = old_seg;
	ctl->global_generation++;
	current_generation = ctl->global_generation;
	new_seg->seg_generation = current_generation;
	pg_atomic_write_u32(&new_seg->seg_state, VISTA_SEGSTATE_NORMAL);
	pg_atomic_write_u32(&old_seg->seg_state, VISTA_SEGSTATE_QUIESCE);
	vista_seqlock_write_end(&ctl->ctl_seqlock);

	elog(LOG, "Flush Worker: Waiting for active refcnt on gen %u to drop.", old_seg->seg_generation);
	while (pg_atomic_read_u32(&old_seg->seg_refcount_active) > 0)
	{
		/* Spin or sleep briefly */
	}
	elog(LOG, "Flush Worker: Active refcnt is zero. Collecting dirty buffers.");

	/* 1. Collect Dirty Buffer Information */
	dirty_buffers = (DirtyBufferInfo *) palloc(sizeof(DirtyBufferInfo) * NBuffers);
	for (i = pool_id * NBuffers; i < (pool_id + 1) * NBuffers; i++)
	{
		BufferDesc *buf = vista_GetBufferDescriptor(i);
		if ((pg_atomic_read_u32(&buf->state) & (BM_VALID | BM_DIRTY)) == (BM_VALID | BM_DIRTY))
		{
			SMgrRelation reln = NULL;
			MdfdVec *v;
			bool found_in_cache = false;
			int k;
			XLogRecPtr recptr;

			/* This saves smgropen */
			for (k = 0; k < num_smgr_cache_entries; k++)
			{
				if (RelFileNodeEquals(buf->tag.rnode, smgr_cache[k].rnode))
				{
					reln = smgr_cache[k].rel;
					found_in_cache = true;
					break;
				}
			}

			if (!found_in_cache)
			{
				reln = smgropen(buf->tag.rnode, InvalidBackendId);
				if (num_smgr_cache_entries < SMGR_CACHE_SIZE)
				{
					smgr_cache[num_smgr_cache_entries].rnode = buf->tag.rnode;
					smgr_cache[num_smgr_cache_entries].rel = reln;
					num_smgr_cache_entries++;
				}
			}

			/* Get segment information */
			v = mdfd_getseg(reln, buf->tag.forkNum, buf->tag.blockNum, false, EXTENSION_FAIL);
			
			dirty_buffers[num_dirty].buf = buf;
			dirty_buffers[num_dirty].table_id = buf->tag.rnode.relNode;
			dirty_buffers[num_dirty].file = v->mdfd_vfd;
			num_dirty++;

			recptr = vista_BufferGetLSN(buf);
			if (recptr > max_lsn_recptr)
				max_lsn_recptr = recptr;
		}
	}

	elog(LOG, "Flush Worker: Found %d dirty buffers. Sorting and updating bitmaps.", num_dirty);

	/* 2. Sort dirty buffers for cache efficiency */
	if (num_dirty > 0)
		pg_qsort(dirty_buffers, num_dirty, sizeof(DirtyBufferInfo), compare_dirty_buffer_info_for_bitmap);

	/* 3. Update Inactive Bitmaps Incrementally */
	vista_bitmap_t temp_bitmap_wrapper;
    uint32 last_table_id = -1; /* Invalid OID */
    VistaBitmapTableControlBlock *current_tcb = NULL;
	void *active_bitmap_ptr = NULL;
	void *inactive_bitmap_ptr = NULL;
	uint32 active_idx = -1;
	uint32 inactive_idx = -1;
	uint32 active_flag = -1;
	size_t bit_idx = -1;

	for (i = 0; i < num_dirty; i++)
	{
		uint32 table_id = dirty_buffers[i].table_id;
		BlockNumber blockNum = dirty_buffers[i].buf->tag.blockNum;

		/* 
		 * Since we sorted the dirty_buffers by table_id, we expect that
		 * all buffers for the same table_id are contiguous.
		 */
		if (table_id != last_table_id)
		{
			last_table_id = table_id;
			/* this either finds a table or initializes a new one */
			current_tcb = vista_init_and_get_tcb_by_table_id((void*)vista_BitmapMgr, table_id);

			if (current_tcb)
			{
				/* 
				 * modified_table_ids keep track of the tables
				 * that have had their bitmaps modified during this flush period
				 */
				if (num_modified_tables < MAX_TABLES)
				{
					bool already_present = false;
					for (int j = 0; j < num_modified_tables; j++)
					{
						if (modified_table_ids[j] == table_id)
						{
						 	/* 
							 * This should not happen (due to sorted table_ids),
							 * but we check if the table is already in the
							 * modified_table_ids array. 
							 */
							already_present = true;
							break;
						}
					}

					active_flag = atomic_load(&current_tcb->active_idx_flag);
					active_idx = (active_flag == 0) ? current_tcb->bitmap_idx_A : current_tcb->bitmap_idx_B;
					inactive_idx = (active_flag == 0) ? current_tcb->bitmap_idx_B : current_tcb->bitmap_idx_A;
					active_bitmap_ptr = (void*) vista_get_bitmap_by_idx((void*)vista_BitmapMgr, active_idx);
					inactive_bitmap_ptr = (void*) vista_get_bitmap_by_idx((void*)vista_BitmapMgr, inactive_idx);

					/* 
					 * If we encountered a bitmap that had been already updated during this flush period, 
					 * we do not add it again to the modified_table_ids array.
					 * Also, we do not memcpy the active bitmap to inactive bitmap again.
					 */
					if (!already_present)
					{
						modified_table_ids[num_modified_tables++] = table_id;

						memcpy(inactive_bitmap_ptr, active_bitmap_ptr, vista_BitmapMgr->bitmap_size_bytes_aligned);
					}
					/* bitmap wrapper is used to access the bitmap */
					temp_bitmap_wrapper.words = NULL;
					temp_bitmap_wrapper.num_bits = 0;
				}
				else
				{
					elog(ERROR, "Flush Worker: Exceeded MAX_TABLES limit for bitmap updates. Skipping table %u.", table_id);
					current_tcb = NULL; 
				}
			}
			else 
			{
				elog(ERROR, "Flush Worker: Failed to get or initialize TCB for table_id %u.", table_id);
				current_tcb = NULL;
			}
		}

		if (current_tcb)
		{
			if (!temp_bitmap_wrapper.words)
			{
				vista_non_atomic_bitmap_init(&temp_bitmap_wrapper,
											(void*) vista_get_bitmap_by_idx((void*)vista_BitmapMgr, inactive_idx), 
											vista_BitmapMgr->num_bits_per_bitmap);
			}
			bit_idx = blockNum / NUM_BUFFERS_PER_CACHEBLOCK;

			vista_non_atomic_set_bit(&temp_bitmap_wrapper, bit_idx);
		} 
		else
		{
			elog(WARNING, "Flush Worker: Skipping bitmap update for table_id %u due to previous errors.", table_id);
		}
	}

    elog(LOG, "Flush Worker: Flushing SLRUs...");
	CheckPointCLOG();
	CheckPointSUBTRANS();
	CheckPointMultiXact();

	ProcessSyncRequests();

	MemSet(&CheckpointStats, 0, sizeof(CheckpointStats));

	/* 4. Atomically Swap Bitmaps and Remap Memory */
	vista_seqlock_write_begin(&ctl->ctl_seqlock);

    vista_remap(pool_id);

	elog(LOG, "Flush Worker: Remapped pool %d.", pool_id);

	/* 
	 * We must do this inside seqlock to synchronize remapping and bitmap swapping
	 * It means that the reader also needs to use the same seqlock.
	 * Otherwise, the reader might think, for example, that bit is set and 
	 * create cache from the old data (since not remapped yet). 
	 * In another case, the reader might think that bit is not set 
	 * and use the existing cache, dismissing the updated data (though remapped already)
	 */
	uint32 old_flag = -1;
	for (i = 0; i < num_modified_tables; i++)
	{
		VistaBitmapTableControlBlock *tcb = vista_get_tcb_by_table_id((void*)vista_BitmapMgr, modified_table_ids[i]);
		if (tcb)
		{
			atomic_fetch_add(&tcb->version, 1);
			old_flag = atomic_load(&tcb->active_idx_flag);
			atomic_store(&tcb->active_idx_flag, 1 - old_flag);
		}
		else 
		{
			elog(ERROR, "Flush Worker: Unexpectedly failed to get TCB for table_id %u during bitmap flag flip.", modified_table_ids[i]);
		}
	}
	elog(LOG, "Flush Worker: Flipped active bitmap flags for %d tables.", num_modified_tables);

	pg_atomic_write_u32(&old_seg->seg_state, VISTA_SEGSTATE_WRITEBACK);
	vista_seqlock_write_end(&ctl->ctl_seqlock);

    elog(LOG, "Flush Worker: Flushing WALs...");

	if (max_lsn_recptr != InvalidXLogRecPtr)
		XLogFlush(max_lsn_recptr);

    elog(LOG, "Flush Worker: Flushing Data...");

	if (num_dirty > 0)
		vista_flush_buffers_uring_with_info(dirty_buffers, num_dirty);

	pfree(dirty_buffers);

	/* Final cleanup logic */
	vista_seqlock_write_begin(&ctl->ctl_seqlock);
	pg_atomic_write_u32(&old_seg->seg_state, VISTA_SEGSTATE_DRAIN);
	ctl->vista_op_mode = VISTA_MODE_NORMAL;
	vista_seqlock_write_end(&ctl->ctl_seqlock);

	elog(LOG, "Flush Worker: Waiting for sealed refcnt and remap count to be zero.");
	while (pg_atomic_read_u32(&old_seg->seg_refcount_sealed) > 0 || pg_atomic_read_u32(&ctl->remap_count) > 0)
	{
		// spin
	}

	clean_old_segment(old_seg);

	if (current_generation > 0 && current_generation % 512 == 0)
	{
		vista_CleanupSnapshotEntries(ctl->oldest_olap_generation);
	}
}

static bool
vista_should_flush_now(int rc)
{
    bool woke_by_latch = (rc & WL_LATCH_SET) != 0;

    if (woke_by_latch)
    {
        if (unlikely(first_noop))
        {
            first_noop = 0;
        }
        else 
        {
            return true; // If we woke up by latch, it means that the pool is full.
        }
    }
    // we always rely on the latch by backends.
    return false;
}

static void
clean_old_segment(vista_SegmentHeader *old_seg)
{
    int i;
    BufferDesc *buf;
    // This function cleans the old segment.
    // It is called when the old segment is no longer used.
    // We can safely release the resources of the old segment.
    BufferPoolId pool_id = old_seg->seg_pool_id;

    // Reset the bump allocator
    pg_atomic_write_u32(&vista_StrategyControl[pool_id]->nextVictimBuffer, pool_id * NBuffers);

    // We do not need to increase the segment generation, 
    // since it will be set to the next generation when we start flushing again
    // and this 'old_seg' is used as the next generation pool.
    // I intentionally do not increase this, so that OLAP worker might be able to access the hash table.

    // Clear the descriptors - it is done during flush
    for (i = pool_id * NBuffers; i < (pool_id + 1) * NBuffers; i++)
    {
        buf = vista_GetBufferDescriptor(i);
        CLEAR_BUFFERTAG(buf->tag);
        pg_atomic_write_u32(&buf->state, 0);
        buf->wait_backend_pgprocno = INVALID_PGPROCNO;
        pg_atomic_clear_flag(&buf->being_copied);
        pg_atomic_clear_flag(&buf->copy_done);
    }

    reset_buf_table(pool_id);

    // Reset the segment header data.
    Assert(pg_atomic_read_u32(&old_seg->seg_refcount_sealed) == 0);
    Assert(pg_atomic_read_u32(&old_seg->seg_refcount_active) == 0);
    pg_atomic_write_u32(&old_seg->seg_refcount_sealed, 0);
    pg_atomic_write_u32(&old_seg->seg_refcount_active, 0);
    pg_atomic_write_u32(&old_seg->seg_state, VISTA_SEGSTATE_READY);
}


static void
vista_flush_buffers_uring_with_info(DirtyBufferInfo *dirty_buffers, int num_dirty)
{
	struct io_uring ring;
    int i, ret;
	unsigned head;
	int inflight_ops = 0;
	int successful_ops = 0;
	int submitted_ops = 0;

	ret = io_uring_queue_init(QUEUE_DEPTH, &ring, 0);
	if (ret < 0)
	{
		elog(ERROR, "io_uring_queue_init failed: %s", strerror(-ret));
		return;
	}

	elog(LOG, "VISTA I/O Uring: Flushing %d dirty buffers.", num_dirty);

    i = 0;
    while (i < num_dirty)
    {
        File current_file = dirty_buffers[i].file;
        int raw_fd = vista_FileGetRawDesc(current_file);
        int j = i;

        while (j < num_dirty && dirty_buffers[j].file == current_file)
        {
            BufferDesc *buf = dirty_buffers[j].buf;
            struct io_uring_sqe *sqe;
            off_t offset;
            Block bufBlock;

            sqe = io_uring_get_sqe(&ring);
            if (!sqe)
            {
                ret = io_uring_submit(&ring);
                if (ret < 0)
                {
                    elog(ERROR, "io_uring_submit failed: %s", strerror(-ret));
                    goto cleanup;
                }
                submitted_ops += ret;

                while (inflight_ops >= QUEUE_DEPTH / 2)
                {
                    struct io_uring_cqe *cqe;
                    int wait_ret = io_uring_wait_cqe(&ring, &cqe);
                    if (wait_ret < 0)
                    {
                        elog(ERROR, "io_uring_wait_cqe failed: %s", strerror(-wait_ret));
                        goto cleanup;
                    }

                    int completions_found = 0;
                    io_uring_for_each_cqe(&ring, head, cqe)
                    {
                        if (cqe->res >= 0)
                            successful_ops++;
                        else
                            elog(WARNING, "io_uring op failed: %s", strerror(-cqe->res));
                        completions_found++;
                    }

                    if (completions_found > 0)
                    {
                        inflight_ops -= completions_found;
                        io_uring_cq_advance(&ring, completions_found);
                    }
                }

                sqe = io_uring_get_sqe(&ring);
                if (!sqe)
                {
                    elog(ERROR, "Failed to get SQE after submitting and reaping. Ring may be stuck.");
                    goto cleanup;
                }
            }

            offset = (off_t)BLCKSZ * (buf->tag.blockNum % ((BlockNumber)RELSEG_SIZE));
            bufBlock = vista_BufHdrGetBlock(buf);

            io_uring_prep_write(sqe, raw_fd, bufBlock, BLCKSZ, offset);
            io_uring_sqe_set_data(sqe, buf);
            inflight_ops++;
            j++;
        }

        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
        if (!sqe)
        {
            ret = io_uring_submit(&ring);
            if (ret < 0) { elog(ERROR, "io_uring_submit failed: %s", strerror(-ret)); goto cleanup; }
            submitted_ops += ret;

            while (inflight_ops >= QUEUE_DEPTH / 2)
            {
                struct io_uring_cqe *cqe;
                int wait_ret = io_uring_wait_cqe(&ring, &cqe);
                if (wait_ret < 0) { elog(ERROR, "io_uring_wait_cqe failed: %s", strerror(-wait_ret)); goto cleanup; }

                int completions_found = 0;
                io_uring_for_each_cqe(&ring, head, cqe)
                {
                    if (cqe->res >= 0)
                        successful_ops++;
                    else
                        elog(WARNING, "io_uring op failed: %s", strerror(-cqe->res));
                    completions_found++;
                }

                if (completions_found > 0)
                {
                    inflight_ops -= completions_found;
                    io_uring_cq_advance(&ring, completions_found);
                }
            }
            sqe = io_uring_get_sqe(&ring);
            if (!sqe) { elog(ERROR, "Failed to get SQE after submitting and reaping. Ring may be stuck."); goto cleanup; }
        }
        io_uring_prep_fsync(sqe, raw_fd, 0);
        io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN);
        io_uring_sqe_set_data(sqe, NULL);
        inflight_ops++;

        i = j;

		ret = io_uring_submit(&ring);
		if (ret < 0)
		{
			elog(ERROR, "io_uring_submit failed: %s", strerror(-ret));
			goto cleanup;
		}
		submitted_ops += ret;
	}

	while (inflight_ops > 0)
	{
		struct io_uring_cqe *cqe;
		unsigned head;
		int completions_found = 0;

		ret = io_uring_wait_cqe(&ring, &cqe);
		if (ret < 0)
		{
			elog(ERROR, "io_uring_wait_cqe failed: %s", strerror(-ret));
			break;
		}

		io_uring_for_each_cqe(&ring, head, cqe)
		{
			completions_found++;
			if (cqe->res >= 0)
			{
				successful_ops++;
			}
			else
			{
				BufferDesc *buf = (BufferDesc *) (uintptr_t) cqe->user_data;
				if (buf)
					elog(WARNING, "io_uring write failed for buffer with tag %d/%d/%d: %s",
						 buf->tag.rnode.spcNode, buf->tag.rnode.dbNode, buf->tag.rnode.relNode,
						 strerror(-cqe->res));
				else
					elog(WARNING, "io_uring fsync failed: %s", strerror(-cqe->res));
			}
		}

		if (completions_found > 0)
		{
			inflight_ops -= completions_found;
			io_uring_cq_advance(&ring, completions_found);
		}
	}

	elog(LOG, "VISTA I/O Uring: %d ops succeeded, %d failed out of %d submitted.", successful_ops, submitted_ops - successful_ops, submitted_ops);

cleanup:
	io_uring_queue_exit(&ring);
}
