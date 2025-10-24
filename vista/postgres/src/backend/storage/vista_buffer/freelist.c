/*-------------------------------------------------------------------------
 *
 * freelist.c
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/freelist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port/atomics.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/proc.h"
#include "storage/vista_freelist.h"


#define INT_ACCESS_ONCE(var)	((int)(*((volatile int *)&(var))))

#define N_POOLS 3


/* Pointers to shared state */
BufferStrategyControl *vista_StrategyControl[N_POOLS] = {0};

/*
 * Private (non-shared) state for managing a ring of shared buffers to re-use.
 * This is currently the only kind of BufferAccessStrategy object, but someday
 * we might have more kinds.
 */
typedef struct BufferAccessStrategyData
{
	/* Overall strategy type */
	BufferAccessStrategyType btype;
	/* Number of elements in buffers[] array */
	int			ring_size;

	/*
	 * Index of the "current" slot in the ring, ie, the one most recently
	 * returned by GetBufferFromRing.
	 */
	int			current;

	/*
	 * True if the buffer just returned by StrategyGetBuffer had been in the
	 * ring already.
	 */
	bool		current_was_in_ring;

	/*
	 * Array of buffer numbers.  InvalidBuffer (that is, zero) indicates we
	 * have not yet selected a buffer for this ring slot.  For allocation
	 * simplicity this is palloc'd together with the fixed fields of the
	 * struct.
	 */
	Buffer		buffers[FLEXIBLE_ARRAY_MEMBER];
}			BufferAccessStrategyData;


/* Prototypes for internal functions */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy,
									 uint32 *buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy,
							BufferDesc *buf);

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32
vista_ClockSweepTick(BufferPoolId poolId)
{
	uint32		victim;

	/*
	 * Atomically move hand ahead one buffer - if there's several processes
	 * doing this, this can lead to buffers being returned slightly out of
	 * apparent order.
	 */
	victim =
		pg_atomic_fetch_add_u32(&vista_StrategyControl[poolId]->nextVictimBuffer, 1);

	if (victim >= (poolId + 1) * NBuffers)
	{
		uint32		originalVictim = victim;

		/* always wrap what we look up in BufferDescriptors */
		victim = victim % NBuffers + poolId * NBuffers;

		/*
		 * If we're the one that just caused a wraparound, force
		 * completePasses to be incremented while holding the spinlock. We
		 * need the spinlock so StrategySyncStart() can return a consistent
		 * value consisting of nextVictimBuffer and completePasses.
		 */
		if (victim == poolId * NBuffers)
		{
			uint32		expected;
			uint32		wrapped;
			bool		success = false;

			expected = originalVictim + 1;

			while (!success)
			{
				/*
				 * Acquire the spinlock while increasing completePasses. That
				 * allows other readers to read nextVictimBuffer and
				 * completePasses in a consistent manner which is required for
				 * StrategySyncStart().  In theory delaying the increment
				 * could lead to an overflow of nextVictimBuffers, but that's
				 * highly unlikely and wouldn't be particularly harmful.
				 */
				SpinLockAcquire(&vista_StrategyControl[poolId]->buffer_strategy_lock);

				wrapped = expected % NBuffers + poolId * NBuffers;

				success = pg_atomic_compare_exchange_u32(&vista_StrategyControl[poolId]->nextVictimBuffer,
														 &expected, wrapped);
				if (success)
					vista_StrategyControl[poolId]->completePasses++;
				SpinLockRelease(&vista_StrategyControl[poolId]->buffer_strategy_lock);
			}
		}
	}
	return victim;
}

/*
 * have_free_buffer -- a lockless check to see if there is a free buffer in
 *					   buffer pool.
 *
 * If the result is true that will become stale once free buffers are moved out
 * by other operations, so the caller who strictly want to use a free buffer
 * should not call this.
 * (VISTAXXX) I see this is called by pg_prewarm, so I set the poolid to 0(metadata pool).
 */
bool
have_free_buffer(void)
{
	int poolId = 0;
	if (vista_StrategyControl[poolId]->firstFreeBuffer >= 0)
		return true;
	else
		return false;
}

/* vista_StrategyGetBufferNoEvict
 * Called by the bufmgr to get the next candidate buffer to use in BufferALloc().
 * In VISTA, we have no eviction and no explicit freelist.
 * This function includes the function of a freelist.
 * This function returns a buffer by using a bump allocator. 
 * Of course the buffer is not pinned by anyone.
 */
BufferDesc *
vista_StrategyGetBufferNoEvict(BufferPoolId poolId, uint32 *buf_state)
{
	BufferDesc *buf;
	uint32  local_buf_state;	/* to avoid repeated (de-)referencing */
	uint32  nextBuffer;
	// We do not need a strategy object here.
	// Also, we do not need to wake up the bgwriter.

	 /* Get the next buffer from the bump allocator */
	nextBuffer = pg_atomic_fetch_add_u32(&vista_StrategyControl[poolId]->nextVictimBuffer, 1);

	if (nextBuffer >= (poolId + 1) * NBuffers)
	{
		*buf_state = 0;
		return NULL; // No more buffers available in this pool. This will trigger flushing of the pool.
	}

	 /* Get the buffer descriptor */
	buf = vista_GetBufferDescriptor(nextBuffer);
	local_buf_state = LockBufHdr(buf);

	/*
	 * This is a sanity check for VISTA. Originally it was used to ensure that
	 * the buffer is not pinned or has a nonzero usage_count,
	 * but in VISTA we are more focusing on the reusability of buffer pools.
	 * At least the buf_state should be reset after the pool is flushed.
	 */
	// We don't check the usage count!.
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
	{
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);
	elog(LOG, "vista_StrategyGetBufferNoEvict: buffer %d is not reusable, refcount: %d, usage_count: %d",
	  nextBuffer, BUF_STATE_GET_REFCOUNT(local_buf_state), BUF_STATE_GET_USAGECOUNT(local_buf_state));
	*buf_state = -1; // Error state, buffer is not reusable.
	return NULL;
}


/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 * 
 */
BufferDesc *
vista_StrategyGetBuffer(BufferPoolId poolId, BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	int			bgwprocno;
	int			trycounter;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */

	/*
	 * If given a strategy object, see whether it can select a buffer. We
	 * assume strategy objects don't need buffer_strategy_lock.
	 */
	if (strategy != NULL)
	{
		buf = GetBufferFromRing(strategy, buf_state);
		if (buf != NULL)
			return buf;
	}

	/*
	 * If asked, we need to waken the bgwriter. Since we don't want to rely on
	 * a spinlock for this we force a read from shared memory once, and then
	 * set the latch based on that value. We need to go through that length
	 * because otherwise bgwprocno might be reset while/after we check because
	 * the compiler might just reread from memory.
	 *
	 * This can possibly set the latch of the wrong process if the bgwriter
	 * dies in the wrong moment. But since PGPROC->procLatch is never
	 * deallocated the worst consequence of that is that we set the latch of
	 * some arbitrary process.
	 */
	bgwprocno = INT_ACCESS_ONCE(vista_StrategyControl[poolId]->bgwprocno);
	if (bgwprocno != -1)
	{
		/* reset bgwprocno first, before setting the latch */
		vista_StrategyControl[poolId]->bgwprocno = -1;

		/*
		 * Not acquiring ProcArrayLock here which is slightly icky. It's
		 * actually fine because procLatch isn't ever freed, so we just can
		 * potentially set the wrong process' (or no process') latch.
		 */
		SetLatch(&ProcGlobal->allProcs[bgwprocno].procLatch);
	}

	/*
	 * We count buffer allocation requests so that the bgwriter can estimate
	 * the rate of buffer consumption.  Note that buffers recycled by a
	 * strategy object are intentionally not counted here.
	 * (VISTA) we would not need this, since we do not use the bgwriter.
	 */
	pg_atomic_fetch_add_u32(&vista_StrategyControl[poolId]->numBufferAllocs, 1);

	/*
	 * First check, without acquiring the lock, whether there's buffers in the
	 * freelist. Since we otherwise don't require the spinlock in every
	 * StrategyGetBuffer() invocation, it'd be sad to acquire it here -
	 * uselessly in most cases. That obviously leaves a race where a buffer is
	 * put on the freelist but we don't see the store yet - but that's pretty
	 * harmless, it'll just get used during the next buffer acquisition.
	 *
	 * If there's buffers on the freelist, acquire the spinlock to pop one
	 * buffer of the freelist. Then check whether that buffer is usable and
	 * repeat if not.
	 *
	 * Note that the freeNext fields are considered to be protected by the
	 * buffer_strategy_lock not the individual buffer spinlocks, so it's OK to
	 * manipulate them without holding the spinlock.
	 * (VISTA) For us, the freelist can be a) default strategy, b) non-eviction strategy
	 */
	if (vista_StrategyControl[poolId]->firstFreeBuffer >= 0)
	{
		while (true)
		{
			/* Acquire the spinlock to remove element from the freelist */
			SpinLockAcquire(&vista_StrategyControl[poolId]->buffer_strategy_lock);

			if (vista_StrategyControl[poolId]->firstFreeBuffer < 0)
			{
				SpinLockRelease(&vista_StrategyControl[poolId]->buffer_strategy_lock);
				break;
			}

			buf = vista_GetBufferDescriptor(vista_StrategyControl[poolId]->firstFreeBuffer);
			Assert(buf->freeNext != FREENEXT_NOT_IN_LIST);

			/* Unconditionally remove buffer from freelist */
			vista_StrategyControl[poolId]->firstFreeBuffer = buf->freeNext;
			buf->freeNext = FREENEXT_NOT_IN_LIST;

			/*
			 * Release the lock so someone else can access the freelist while
			 * we check out this buffer.
			 */
			SpinLockRelease(&vista_StrategyControl[poolId]->buffer_strategy_lock);

			/*
			 * If the buffer is pinned or has a nonzero usage_count, we cannot
			 * use it; discard it and retry.  (This can only happen if VACUUM
			 * put a valid buffer in the freelist and then someone else used
			 * it before we got to it.  It's probably impossible altogether as
			 * of 8.3, but we'd better check anyway.)
			 */
			local_buf_state = LockBufHdr(buf);
			if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
				&& BUF_STATE_GET_USAGECOUNT(local_buf_state) == 0)
			{
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
			UnlockBufHdr(buf, local_buf_state);
		}
	}

	/* Nothing on the freelist, so run the "clock sweep" algorithm */
	trycounter = NBuffers;
	for (;;)
	{
		buf = vista_GetBufferDescriptor(vista_ClockSweepTick(poolId));

		/*
		 * If the buffer is pinned or has a nonzero usage_count, we cannot use
		 * it; decrement the usage_count (unless pinned) and keep scanning.
		 */
		local_buf_state = LockBufHdr(buf);

		if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0)
		{
			if (BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0)
			{
				local_buf_state -= BUF_USAGECOUNT_ONE;

				trycounter = NBuffers;
			}
			else
			{
				/* Found a usable buffer */
				if (strategy != NULL)
					AddBufferToRing(strategy, buf);
				*buf_state = local_buf_state;
				return buf;
			}
		}
		else if (--trycounter == 0)
		{
			/*
			 * We've scanned all the buffers without making any state changes,
			 * so all the buffers are pinned (or were when we looked at them).
			 * We could hope that someone will free one eventually, but it's
			 * probably better to fail than to risk getting stuck in an
			 * infinite loop.
			 */
			UnlockBufHdr(buf, local_buf_state);
			elog(ERROR, "no unpinned buffers available");
		}
		UnlockBufHdr(buf, local_buf_state);
	}
}

/*
 * StrategyFreeBuffer: put a buffer on the freelist
 * (VISTA) support multiple buffer pools. I did not add the pool_id argument,
 * since we can deduce it from the buffer descriptor.(hopefully)
 */
void
vista_StrategyFreeBuffer(BufferDesc *buf)
{
	int poolId = vista_BufferDescriptorGetPoolId(buf);
	SpinLockAcquire(&vista_StrategyControl[poolId]->buffer_strategy_lock);

	/*
	 * It is possible that we are told to put something in the freelist that
	 * is already in it; don't screw up the list if so.
	 */
	if (buf->freeNext == FREENEXT_NOT_IN_LIST)
	{
		buf->freeNext = vista_StrategyControl[poolId]->firstFreeBuffer;
		if (buf->freeNext < 0)
			vista_StrategyControl[poolId]->lastFreeBuffer = buf->buf_id;
		vista_StrategyControl[poolId]->firstFreeBuffer = buf->buf_id;
	}

	SpinLockRelease(&vista_StrategyControl[poolId]->buffer_strategy_lock);
}

/*
 * vista_StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.  The alloc count is reset after
 * being read.
 */
int
vista_StrategySyncStart(BufferPoolId poolId, uint32 *complete_passes, uint32 *num_buf_alloc)
{

	uint32		nextVictimBuffer;
	int			result;

	SpinLockAcquire(&vista_StrategyControl[poolId]->buffer_strategy_lock);
	nextVictimBuffer = pg_atomic_read_u32(&vista_StrategyControl[poolId]->nextVictimBuffer);
	result = nextVictimBuffer % NBuffers + poolId * NBuffers;

	if (complete_passes)
	{
		*complete_passes = vista_StrategyControl[poolId]->completePasses;

		/*
		 * Additionally add the number of wraparounds that happened before
		 * completePasses could be incremented. C.f. ClockSweepTick().
		 */
		*complete_passes += (nextVictimBuffer - (poolId * NBuffers)) / NBuffers;
	}

	if (num_buf_alloc)
	{
		*num_buf_alloc = pg_atomic_exchange_u32(&vista_StrategyControl[poolId]->numBufferAllocs, 0);
	}
	SpinLockRelease(&vista_StrategyControl[poolId]->buffer_strategy_lock);
	return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwprocno isn't -1, the next invocation of StrategyGetBuffer will
 * set that latch.  Pass -1 to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void
vista_StrategyNotifyBgWriter(BufferPoolId poolId, int bgwprocno)
{
	/*
	 * We acquire buffer_strategy_lock just to ensure that the store appears
	 * atomic to StrategyGetBuffer.  The bgwriter should call this rather
	 * infrequently, so there's no performance penalty from being safe.
	 */
	SpinLockAcquire(&vista_StrategyControl[poolId]->buffer_strategy_lock);
	vista_StrategyControl[poolId]->bgwprocno = bgwprocno;
	SpinLockRelease(&vista_StrategyControl[poolId]->buffer_strategy_lock);
}


/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size
StrategyShmemSize(void)
{
	Size		size = 0;

	/* size of lookup hash table ... see comment in StrategyInitialize */
	size = add_size(size, BufTableShmemSize(2 * NBuffers + NUM_BUFFER_PARTITIONS));

	/* size of the shared replacement strategy control block */
	size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

	return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void
vista_StrategyInitialize(BufferPoolId poolId)
{
	/*
	 * Initialize the shared buffer lookup hashtable.
	 *
	 * Since we can't tolerate running out of lookup table entries, we must be
	 * sure to specify an adequate table size here.  The maximum steady-state
	 * usage is of course NBuffers entries, but BufferAlloc() tries to insert
	 * a new entry before deleting the old.  In principle this could be
	 * happening in each partition concurrently, so we could need as many as
	 * NBuffers + NUM_BUFFER_PARTITIONS entries.
	 */
	// Please look at StrategyShmemSize() as well.
    vista_InitBufTable(poolId, 2 * NBuffers + NUM_BUFFER_PARTITIONS);

	/*
	 * Get or create the shared strategy control block
	 */

	vista_StrategyControl[poolId] = (BufferStrategyControl *) vista_vmseg_alloc(&vista_VmSegAllocatorPerPool[poolId],
		sizeof(BufferStrategyControl));
	if (vista_StrategyControl[poolId] == NULL) {
		ereport(FATAL, errmsg("vista_vmseg_alloc failed for BufferStrategyControl"));
	}

	// if (!found)
	// {
	/*
		* Only done once, usually in postmaster
		*/
	SpinLockInit(&vista_StrategyControl[poolId]->buffer_strategy_lock);

	/*
		* Grab the whole linked list of free buffers for our strategy. We
		* assume it was previously set up by InitBufferPool().
		*/
	vista_StrategyControl[poolId]->firstFreeBuffer = 0 + poolId * NBuffers;
	vista_StrategyControl[poolId]->lastFreeBuffer = (poolId + 1) * NBuffers - 1;

	/* Initialize the clock sweep pointer */
	pg_atomic_init_u32(&vista_StrategyControl[poolId]->nextVictimBuffer, poolId * NBuffers);

	/* Clear statistics */
	vista_StrategyControl[poolId]->completePasses = 0;
	pg_atomic_init_u32(&vista_StrategyControl[poolId]->numBufferAllocs, 0);

	/* No pending notification */
	vista_StrategyControl[poolId]->bgwprocno = -1;
	// }
	// else
	// 	Assert(!init);
}


/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */


/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy
GetAccessStrategy(BufferAccessStrategyType btype)
{
	BufferAccessStrategy strategy;
	int			ring_size;

	/*
	 * Select ring size to use.  See buffer/README for rationales.
	 *
	 * Note: if you change the ring size for BAS_BULKREAD, see also
	 * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
	 */
	switch (btype)
	{
		case BAS_NORMAL:
			/* if someone asks for NORMAL, just give 'em a "default" object */
			return NULL;

		case BAS_BULKREAD:
			ring_size = 256 * 1024 / BLCKSZ;
			break;
		case BAS_BULKWRITE:
			ring_size = 16 * 1024 * 1024 / BLCKSZ;
			break;
		case BAS_VACUUM:
			ring_size = 256 * 1024 / BLCKSZ;
			break;

		default:
			elog(ERROR, "unrecognized buffer access strategy: %d",
				 (int) btype);
			return NULL;		/* keep compiler quiet */
	}

	/* Make sure ring isn't an undue fraction of shared buffers */
	ring_size = Min(NBuffers / 8, ring_size);

	/* Allocate the object and initialize all elements to zeroes */
	strategy = (BufferAccessStrategy)
		palloc0(offsetof(BufferAccessStrategyData, buffers) +
				ring_size * sizeof(Buffer));

	/* Set fields that don't start out zero */
	strategy->btype = btype;
	strategy->ring_size = ring_size;

	return strategy;
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void
FreeAccessStrategy(BufferAccessStrategy strategy)
{
	/* don't crash if called on a "default" strategy */
	if (strategy != NULL)
		pfree(strategy);
}

/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *
GetBufferFromRing(BufferAccessStrategy strategy, uint32 *buf_state)
{
	BufferDesc *buf;
	Buffer		bufnum;
	uint32		local_buf_state;	/* to avoid repeated (de-)referencing */


	/* Advance to next ring slot */
	if (++strategy->current >= strategy->ring_size)
		strategy->current = 0;

	/*
	 * If the slot hasn't been filled yet, tell the caller to allocate a new
	 * buffer with the normal allocation strategy.  He will then fill this
	 * slot by calling AddBufferToRing with the new buffer.
	 */
	bufnum = strategy->buffers[strategy->current];
	if (bufnum == InvalidBuffer)
	{
		strategy->current_was_in_ring = false;
		return NULL;
	}

	/*
	 * If the buffer is pinned we cannot use it under any circumstances.
	 *
	 * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
	 * since our own previous usage of the ring element would have left it
	 * there, but it might've been decremented by clock sweep since then). A
	 * higher usage_count indicates someone else has touched the buffer, so we
	 * shouldn't re-use it.
	 */
	buf = vista_GetBufferDescriptor(bufnum - 1);
	local_buf_state = LockBufHdr(buf);
	if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0
		&& BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1)
	{
		strategy->current_was_in_ring = true;
		*buf_state = local_buf_state;
		return buf;
	}
	UnlockBufHdr(buf, local_buf_state);

	/*
	 * Tell caller to allocate a new buffer with the normal allocation
	 * strategy.  He'll then replace this ring element via AddBufferToRing.
	 */
	strategy->current_was_in_ring = false;
	return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void
AddBufferToRing(BufferAccessStrategy strategy, BufferDesc *buf)
{
	strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.  This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool
StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf)
{
	/* We only do this in bulkread mode */
	if (strategy->btype != BAS_BULKREAD)
		return false;

	/* Don't muck with behavior of normal buffer-replacement strategy */
	if (!strategy->current_was_in_ring ||
		strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
		return false;

	/*
	 * Remove the dirty buffer from the ring; necessary to prevent infinite
	 * loop if all ring members are dirty.
	 */
	strategy->buffers[strategy->current] = InvalidBuffer;

	return true;
}
