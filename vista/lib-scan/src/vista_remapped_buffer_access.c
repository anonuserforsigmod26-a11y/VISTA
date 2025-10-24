/*-------------------------------------------------------------------------
 *
 * vista_buffer_access.c
 *	  Functions for accessing remapped buffer pool in VISTA.
 *    This is used by the OLAP scan operator to read pages from
 *    the remapped PostgreSQL buffer pool.
 * 	  This does not control what happens inside the buffer pool,
 *    which is handled by libvista's vista_read_buffer() function.
 * 
 *-------------------------------------------------------------------------
 */

#include "libvista.h"
#include "vista_pg_read_buffer.h"
#include "vista_pg_atomics.h"
#include "vista_seqlock.h"
#include "vista_bitmap.h"
#include "vista_remapped_buffer_access.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static struct vista_addrs v_addrs;
static libvista_BufferPoolControl *vista_ctl = NULL; /* control must be read from the shared region */
static vista_bufferpool_offsets *vista_offsets = NULL;

void* bitmap_mgr_ptr = NULL;

/* OLAP engine's process state on the remapped VMSeg */
typedef enum {
    VISTA_STATE_DETACHED = 0,
    VISTA_STATE_ATTACHING = 1,
    VISTA_STATE_ATTACHED = 2,
    VISTA_STATE_UNMAPPING = 3
} ProcessRemapState;

static vista_atomic_uint32 process_remap_state = { .value = VISTA_STATE_DETACHED };

/* 
 * We manage the OLAP refcount separately from `sealed_refcnt` of PostgreSQL.
 * This is because we have to unmap the VMSeg when there are no OLAP readers,
 * and we need to know when.
 * 
 * Note: attached OLAP readers increment the `remap_count` of the buffer pool control struct,
 * to prevent the buffer pool from being destroyed while they are reading.
 */
static vista_atomic_uint32 olap_process_refcount = { .value = 0 };

static vista_atomic_uint32 olap_begin_unmap = { .value = 0 };

/* XXX VISTA initialization state - we would not need it if we find the correct place for vista_scan_init() */
static bool is_vista_initialized = false;

void *VistaGetShmem(void) // simple getter.
{
	return (void *)v_addrs.shrd_metadata_ptr;
}

/*
 * vista_scan_init: Initialize the VISTA scan.
 * XXX This must be called only once at the beginning of the DuckDB initialization.
 * Currently we call it in every call of vista_try_read_remapped_page(),
 * but it is okay because vista_register() is idempotently implemented.
 */
void vista_scan_init(void)
{
    int aligned_offset;

    if (is_vista_initialized)
        return;

    if (vista_register(1, VISTA_MAP_OLAP, &v_addrs) != 0)
    {
        fprintf(stderr, "VISTA: vista_register failed.\n");
        exit(1);
    }
    vista_init_vmseg(1, VISTA_MAP_OLAP, &v_addrs);

    /* The shared metadata pointer gives us access to the control structure */
    
    aligned_offset = *((int*) v_addrs.shrd_metadata_ptr);

    vista_offsets = (vista_bufferpool_offsets *) ((char*) v_addrs.shrd_metadata_ptr + aligned_offset); 
    if (vista_offsets == NULL)
    {
        fprintf(stderr, "VISTA: shared metadata pointer is null.\n");
        exit(1);
    }

	bitmap_mgr_ptr = vista_find_and_attach_bitmap_manager((void*)v_addrs.shrd_metadata_ptr);
	if (bitmap_mgr_ptr == NULL)
	{
		fprintf(stderr, "VISTA-info: (THIS could be displayed until we integrate with Postgres that supports bitmap for caching; cannot find bitmap manager in shared metadata.\n");
	}

	vista_ctl = (libvista_BufferPoolControl *)((char *)v_addrs.shrd_metadata_ptr + vista_offsets->buf_ctl_offset);

    is_vista_initialized = true;

	printf("VISTA: OLAP process initialized. remap_alert_ptr=%p, remap_window_ptr=%p\n",
		   (void*) v_addrs.remap_alert_ptr, (void*) v_addrs.remap_window_ptr);

	printf("VISTA: current remap alert value is %d\n", 
		   (int) vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr));
}

/*
 * vista_remapped_open: This function is responsible for opening a remapped VMSeg.
 * Following the POSIX shm interface, this opens a remapped VMSeg if not already opened,
 * or does nothing if already opened.
 * Return True if the remapped VMSeg is opened or attached to, False if not.
 * The caller must have already incremented the olap_process_refcount.
 * 
 * If the Remap State is VISTA_STATE_DETACHED, a thread will change it to VISTA_STATE_ATTACHED,
 * and increase a remap_count at the buffer pool control struct, attaching to the remapped VMSeg.
 * 
 * If already VISTA_STATE_ATTACHED, return immediately.
 * We accept VISTA_STATE_UNMAPPING as well, since the VMSeg could be in the process of unmapping,
 * but unmapping cannot proceed as this thread already increased the olap_process_refcount.
 */
static bool vista_remapped_open(void)
{
	uint32_t oldVal;
	bool is_flushing;
	ProcessRemapState current_state;

    current_state = vista_atomic_read_u32(&process_remap_state);
    if (current_state == VISTA_STATE_ATTACHED)
    {
		vista_reconstruct_buffer_pool((void*) v_addrs.remap_window_ptr, vista_offsets);
		return true;
	} else if (current_state == VISTA_STATE_UNMAPPING)
	{
		return false;
	}
	vista_pg_compiler_barrier();

	/* If we reached here, the state is either VISTA_STATE_DETACHED or VISTA_STATE_ATTACHING */

	uint32_t expected_state = VISTA_STATE_DETACHED;
	if (vista_atomic_compare_exchange_u32(&process_remap_state, &expected_state, VISTA_STATE_ATTACHING))
	{
		/* This thread is the winner, responsible for setup. */

		vista_atomic_add_fetch_u32(&vista_ctl->remap_count, 1);

		/* 
		 * We're in this function because we saw the op mode VISTA_MODE_FLUSH, 
		 * but it could have changed to VISTA_MODE_NORMAL already.
		 * The flush worker could have already started the cleanup,
		 * believing that no OLAP readers are present(remap_count == 0).
		 * We must ensure that the op mode is still VISTA_MODE_FLUSH. 
		 */

		for (;;)
		{
			oldVal = libvista_seqlock_read_begin(&vista_ctl->ctl_seqlock);
			is_flushing = (vista_ctl->vista_op_mode == LIBVISTA_MODE_FLUSH);
			if (libvista_seqlock_read_retry(&vista_ctl->ctl_seqlock, oldVal))
			{
				/* Contention on the seqlock means the op mode has changed. Retry. */
				continue;
			}

			if (!is_flushing || vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr) == 0)
			{
				/* The op mode changed to NORMAL, which means that the flush worker could have started cleanup. */
				/* Though this might be okay by chance, we cancel the attaching for safety */
				vista_atomic_sub_fetch_u32(&vista_ctl->remap_count, 1);
				/* no barrier here because sub_fetch has full barrier semantics */
				vista_atomic_write_u32(&process_remap_state, VISTA_STATE_DETACHED);
				return false; /* We cannot attach to the remapped segment. Return immediately. */
			}
			break; /* We're good to go. */
		}
		vista_pg_compiler_barrier();
		vista_atomic_write_u32(&process_remap_state, VISTA_STATE_ATTACHED);

		/* Reconstruct the buffer pool using the remapped window. */
		vista_reconstruct_buffer_pool((void*) v_addrs.remap_window_ptr, vista_offsets);
		return true;
	}
	else 
	{
		/* 
		 * Since we already increased the olap refcount, we can be assured that
		 * the VMSeg is not unmapped, i.e., becomes VISTA_STATE_DETACHED.
		 */
		current_state = vista_atomic_read_u32(&process_remap_state);
		vista_pg_compiler_barrier();

		while (current_state != VISTA_STATE_ATTACHED)
		{
			current_state = vista_atomic_read_u32(&process_remap_state);
			vista_pg_compiler_barrier();

			/* 
			 * We're in this function because we saw the op mode VISTA_MODE_FLUSH, 
			 * but it could have changed to VISTA_MODE_NORMAL already.
			 * Thus, we must return if the process state becomes VISTA_STATE_DETACHED.
			 * (See the winner thread's logic above.)
			 */
			if (current_state == VISTA_STATE_DETACHED || current_state == VISTA_STATE_UNMAPPING || vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr) == 0)
			{
				return false;
			}
		}
		vista_reconstruct_buffer_pool((void*) v_addrs.remap_window_ptr, vista_offsets);
		return true;
	}
}

/*
 * vista_unmap_logic: This function is responsible for unmapping the remapped VMSeg
 * if it is currently mapped.
 * 
 * If the process state is VISTA_STATE_ATTACHED, a thread will change it to VISTA_STATE_UNMAPPING,
 * wait for all OLAP readers to finish, and unmap the VMSeg.
 * 
 * If the process state is VISTA_STATE_DETACHED or VISTA_STATE_UNMAPPING, do nothing and return immediately.
 * If the process state is VISTA_STATE_ATTACHING, wait until it becomes either ATTACHED or DETACHED.
 * 
 * We can do not have to wait for the winner thread to finish unmapping,
 * because the other threads can fall back to the normal read path, as if no remapped VMSeg exists.
 */
static void vista_unmap_logic()
{
    ProcessRemapState current_state = vista_atomic_read_u32(&process_remap_state);
    if (current_state == VISTA_STATE_DETACHED || current_state == VISTA_STATE_UNMAPPING)
    {
		return;
	}

	//fprintf(stderr, "VISTA: OLAP process starts unmap logic. current_state=%d\n", (int) current_state);

	/* 
	 * If someone is attaching the VmSeg, we need to wait until it is attached,
	 * so that we can unmap it safely.
	 */
	while (current_state == VISTA_STATE_ATTACHING)
	{
		current_state = vista_atomic_read_u32(&process_remap_state);
		vista_pg_compiler_barrier();
	}

	vista_pg_compiler_barrier();

	uint32_t expected_state = VISTA_STATE_ATTACHED;
    if (vista_atomic_compare_exchange_u32(&process_remap_state, &expected_state, VISTA_STATE_UNMAPPING))
    {
        /* This thread is the designated unmapper. */

		vista_atomic_add_fetch_u32(&olap_begin_unmap, 1);

		/* Turn off the alert to prevent new entrance to the remapped VMSeg */
		vista_atomic_write_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr, 0);

        while (vista_atomic_add_fetch_u32(&olap_process_refcount, 0) > 0)
		
        {
        	vista_pg_compiler_barrier();
            /* Wait for readers to finish */
        }

        vista_atomic_sub_fetch_u32(&vista_ctl->remap_count, 1);
//		fprintf(stderr, "VISTA: OLAP process decreased remap_count to %u and proceeds to unmap\n", 
//				(unsigned int) vista_atomic_read_u32(&vista_ctl->remap_count));
        vista_unmap();
        vista_atomic_write_u32(&process_remap_state, VISTA_STATE_DETACHED);

		vista_atomic_sub_fetch_u32(&olap_begin_unmap, 1);
        return;
    }
    else
    {
        /* Another thread is already handling the unmap. We don't need to do anything. */
        return;
    }
}

/*
 * vista_try_read_remapped_page: Try to read a page from the remapped buffer pool.
 * If successful, it returns a pointer to the page. If not, it returns NULL.
 * If a page is returned, the caller must call vista_release_remapped_page()
 * after finishing the use of the page.
 */
void* vista_try_read_remapped_page(int spcNode, int dbNode, int relNode, vista_pg_BlockNumber blockNum, unsigned int* gen)
{
    uint32_t oldVal;
    bool is_flushing;
    bool remap_alerted;
	bool remap_opened;

    /* 
     * To run this, you must have already run vista_scan_init()!
     */

	while (true)
	{
		remap_alerted = (vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr) == 1);

    	// printf("VISTA: current remap alert value is %d\n", (int) vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr));
        
		vista_pg_compiler_barrier(); // in x86, we only need compiler barrier. 

		if (remap_alerted)
		{
			/* 
			* A VMSeg is remapped by the vista flush background worker, 
			* but we are not sure whether the segment state is FLUSH or DRAIN.
			* We must check the op mode, with a seqlock.
			*/
			for (;;)
			{
				oldVal = libvista_seqlock_read_begin(&vista_ctl->ctl_seqlock);

				is_flushing = (vista_ctl->vista_op_mode == LIBVISTA_MODE_FLUSH);
				*gen = vista_ctl->global_generation;
				vista_atomic_add_fetch_u32(&olap_process_refcount, 1);

				/* This DOES NOT prevent VMSeg cleanup by the vista flush background worker. */

				if (libvista_seqlock_read_retry(&vista_ctl->ctl_seqlock, oldVal))
				{
					/* 
					* Contention on the seqlock means the op mode has changed(FLUSH -> NORMAL).
					* We need to unmap the VMSeg, so just continue.
					*/
					vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
					break;
				}

				if (is_flushing)
				{
					/* It is possible that this is a stale, false remap alert. 
					 * In such a case, we should not try to attach to the remapped segment.
					 * We must check 
					*/

					/* Reconfirm the remap alert */
					/* This is to ensure that we do not conflict with a potential unmap operation. */
					remap_alerted = (vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr) == 1);
					if (!remap_alerted)
					{
						vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
						break; /* Go back to the top-level loop */
					}

					/* 
					 * Now it means that we have increased the refcount before 
					 * a potential unmapper turned off the remap alert.
					 */

					/* 
					 * We need another check to ensure that this is not a stale remap alert.
					 * A stale alert could happen if we did not attach/detach during the previous flush cycle,
					 * leaving the alert on.
					 * To check this, we need to access the currently remapped segment and
					 * see if its segment state is WRITEBACK. (It should not be QUIESCE, or NORMAL)
					 */
					uint32_t oldVal = libvista_seqlock_read_begin(&vista_ctl->ctl_seqlock);
					libvista_SegmentHeader* seghdr = (libvista_SegmentHeader*) v_addrs.remap_window_ptr;
					bool is_segstate_writeback = (vista_atomic_read_u32(&seghdr->seg_state) == VISTA_SEGSTATE_WRITEBACK);
					if (libvista_seqlock_read_retry(&vista_ctl->ctl_seqlock, oldVal))
					{
						/* Contention on the seqlock means the op mode has changed. Retry. */
						vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
						break;
					}

					if (!is_segstate_writeback)
					{
						/* need to retry */
						vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
						break; /* Go back to the top-level loop */
					}

					/*
					* Ensure this process is attached to the remapped segment. This call
				is internally thread-safe and idempotent.
					*/
					remap_opened = vista_remapped_open();

					if (!remap_opened)
					{
						vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
						 /* We cannot attach to the remapped segment. Return immediately. */
						 /* No need to unmap */
						return NULL;
					}
				
					/* We hold a valid reference count. Proceed with the read. */
					void *page = vista_read_buffer(&g_reconstructed_pool, spcNode, dbNode, relNode, blockNum);
					if (page)
					{
						/* 
						* Success. The caller is now responsible for calling
						* vista_release_remapped_page() to decrement the refcount.
						*/
						return page;
					}

					/* 
					* Cache miss. We didn't get a page, so we must release the refcount
					* we acquired.
					*/
					vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
					return NULL;              
				}
				else 
				{
					/* 
					* The op mode is NORMAL, which means that the segment has entered DRAIN state.
					* We need to unmap the VMSeg.
					*/

					vista_atomic_sub_fetch_u32(&olap_process_refcount, 1);
					vista_unmap_logic();
					return NULL;                
				} // is_flushing
			} // for loop
		}
		else 
		{
			// We need to get a generation value even if no remap is alerted.
			do {
				oldVal = libvista_seqlock_read_begin(&vista_ctl->ctl_seqlock);
				*gen = vista_ctl->global_generation;
			} while (libvista_seqlock_read_retry(&vista_ctl->ctl_seqlock, oldVal));

			/*
			* A VMSeg is currently not remapped. If this process was previously
			* attached, it might be in a stale state.
			*/
			return NULL;
		} // remap_alert
	}
} // vista_try_read_remapped_page


/*
 * vista_release_remapped_page: Release a page that was read from the
 * remapped buffer pool.
 *
 * This must be called after processing a page that was successfully returned by
 * vista_try_read_remapped_page(), to ensure the unmap process can proceed.
 * 
 * However, this does NOT unmap the remapped VMSeg itself.
 */
void vista_release_remapped_page(void)
{
	/* decrease the refcount */
	vista_atomic_sub_fetch_u32(&olap_process_refcount, 1); 
}

void vista_external_unmap(void)
{
	if (vista_atomic_read_u32((vista_atomic_uint32 *)v_addrs.remap_alert_ptr) == 1) {
		vista_unmap_logic();
	}
}

/*
 * vista_scan_close: Close the scan and release any resources.
 * This must be called when the OLAP process finishes scanning.
 * It will unmap the remapped VMSeg if it is still mapped.
 */
void vista_scan_close(void)
{
	vista_unmap_logic();
    vista_unregister(&v_addrs, VISTA_MAP_OLAP);
}

/*
 * Getter for the current generation and flush values.
 * This is same as the one in libvista.c, but we duplicate it here
 * to avoid exposing the entire libvista.h.
 */
unsigned long vista_get_generation_flush_values(void)
{
	uint32_t old, gen, flush;
	do {
		old = libvista_seqlock_read_begin(&vista_ctl->ctl_seqlock);
		gen = vista_ctl->global_generation;
		flush = vista_ctl->vista_op_mode;
	} while (libvista_seqlock_read_retry(&vista_ctl->ctl_seqlock, old));
	return ((unsigned long)gen << 32UL) | (unsigned long)flush;
}

/*
 * Bitmap wrapper functions for cache validation
 */
bool LibScan_TestBitPreWork(uint32_t table_id, size_t bit_idx, uint64_t* pre_work_version)
{
	if (bitmap_mgr_ptr == NULL) {
		fprintf(stderr, "[libscan] FATAL: bitmap_mgr_ptr is NULL\n");
		exit(1);
	}
	if (vista_ctl == NULL)
	{
		fprintf(stderr, "[libscan] FATAL: vista_ctl is NULL in LibScan_TestBitPreWork\n");
		exit(1);
	}
	return vista_test_bit_pre_work(bitmap_mgr_ptr, table_id, bit_idx, pre_work_version, vista_ctl);
}

bool LibScan_ClearBitPostWork(uint32_t table_id, size_t bit_idx, uint64_t pre_work_version)
{
	if (bitmap_mgr_ptr == NULL) {
		fprintf(stderr, "[libscan] FATAL: bitmap_mgr_ptr is NULL\n");
		exit(1);
	}
	return vista_clear_bit_post_work(bitmap_mgr_ptr, table_id, bit_idx, pre_work_version, vista_ctl);
}
