/*-------------------------------------------------------------------------
 *
 * buf_internals.h
 *	  Internal definitions for buffer manager and the buffer replacement
 *	  strategy.
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/buf_internals.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFMGR_INTERNALS_H
#define BUFMGR_INTERNALS_H

#include "port/atomics.h"
#include "storage/buf.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/smgr.h"
#include "storage/spin.h"
#include "utils/relcache.h"

#ifdef VISTA
#include "storage/vista_seqlock.h"
#include "libvista.h"
#include "vista_bitmap.h"
#endif

/*
 * Buffer state is a single 32-bit variable where following data is combined.
 *
 * - 18 bits refcount
 * - 4 bits usage count
 * - 10 bits of flags
 *
 * Combining these values allows to perform some operations without locking
 * the buffer header, by modifying them together with a CAS loop.
 *
 * The definition of buffer state components is below.
 */
#define BUF_REFCOUNT_ONE 1
#define BUF_REFCOUNT_MASK ((1U << 18) - 1)
#define BUF_USAGECOUNT_MASK 0x003C0000U
#define BUF_USAGECOUNT_ONE (1U << 18)
#define BUF_USAGECOUNT_SHIFT 18
#define BUF_FLAG_MASK 0xFFC00000U

/* Get refcount and usagecount from buffer state */
#define BUF_STATE_GET_REFCOUNT(state) ((state) & BUF_REFCOUNT_MASK)
#define BUF_STATE_GET_USAGECOUNT(state) (((state) & BUF_USAGECOUNT_MASK) >> BUF_USAGECOUNT_SHIFT)

/*
 * Flags for buffer descriptors
 *
 * Note: BM_TAG_VALID essentially means that there is a buffer hashtable
 * entry associated with the buffer's tag.
 */
#define BM_LOCKED				(1U << 22)	/* buffer header is locked */
#define BM_DIRTY				(1U << 23)	/* data needs writing */
#define BM_VALID				(1U << 24)	/* data is valid */
#define BM_TAG_VALID			(1U << 25)	/* tag is assigned */
#define BM_IO_IN_PROGRESS		(1U << 26)	/* read or write in progress */
#define BM_IO_ERROR				(1U << 27)	/* previous I/O failed */
#define BM_JUST_DIRTIED			(1U << 28)	/* dirtied since write started */
#define BM_PIN_COUNT_WAITER		(1U << 29)	/* have waiter for sole pin */
#define BM_CHECKPOINT_NEEDED	(1U << 30)	/* must write for checkpoint */
#define BM_PERMANENT			(1U << 31)	/* permanent buffer (not unlogged,
											 * or init fork) */
/*
 * The maximum allowed value of usage_count represents a tradeoff between
 * accuracy and speed of the clock-sweep buffer management algorithm.  A
 * large value (comparable to NBuffers) would approximate LRU semantics.
 * But it can take as many as BM_MAX_USAGE_COUNT+1 complete cycles of
 * clock sweeps to find a free buffer, so in practice we don't want the
 * value to be very large.
 */
#define BM_MAX_USAGE_COUNT	5

/*
 * Buffer tag identifies which disk block the buffer contains.
 *
 * Note: the BufferTag data must be sufficient to determine where to write the
 * block, without reference to pg_class or pg_tablespace entries.  It's
 * possible that the backend flushing the buffer doesn't even believe the
 * relation is visible yet (its xact may have started before the xact that
 * created the rel).  The storage manager must be able to cope anyway.
 *
 * Note: if there's any pad bytes in the struct, INIT_BUFFERTAG will have
 * to be fixed to zero them, since this struct is used as a hash key.
 */
typedef struct buftag
{
	RelFileNode rnode;			/* physical relation identifier */
	ForkNumber	forkNum;
	BlockNumber blockNum;		/* blknum relative to begin of reln */
} BufferTag;

#define CLEAR_BUFFERTAG(a) \
( \
	(a).rnode.spcNode = InvalidOid, \
	(a).rnode.dbNode = InvalidOid, \
	(a).rnode.relNode = InvalidOid, \
	(a).forkNum = InvalidForkNumber, \
	(a).blockNum = InvalidBlockNumber \
)

#define INIT_BUFFERTAG(a,xx_rnode,xx_forkNum,xx_blockNum) \
( \
	(a).rnode = (xx_rnode), \
	(a).forkNum = (xx_forkNum), \
	(a).blockNum = (xx_blockNum) \
)

#define BUFFERTAGS_EQUAL(a,b) \
( \
	RelFileNodeEquals((a).rnode, (b).rnode) && \
	(a).blockNum == (b).blockNum && \
	(a).forkNum == (b).forkNum \
)

/*
 * The shared buffer mapping table is partitioned to reduce contention.
 * To determine which partition lock a given tag requires, compute the tag's
 * hash code with BufTableHashCode(), then apply BufMappingPartitionLock().
 * NB: NUM_BUFFER_PARTITIONS must be a power of 2!
 */

#define BufTableHashPartition(hashcode) \
	((hashcode) % NUM_BUFFER_PARTITIONS)
#ifdef VISTA
#define vista_BufMappingPartitionLock(pool_id, hashcode) \
	(&MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET_POOL(pool_id) + \
		BufTableHashPartition(hashcode)].lock)
// BufMappingPartitionLockByIndex() is not used anywhere in Postgres...
#else
#define BufMappingPartitionLock(hashcode) \
	(&MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET + \
		BufTableHashPartition(hashcode)].lock)
#endif
#define BufMappingPartitionLockByIndex(i) \
	(&MainLWLockArray[BUFFER_MAPPING_LWLOCK_OFFSET + (i)].lock)

/*
 *	BufferDesc -- shared descriptor/state data for a single shared buffer.
 *
 * Note: Buffer header lock (BM_LOCKED flag) must be held to examine or change
 * tag, state or wait_backend_pgprocno fields.  In general, buffer header lock
 * is a spinlock which is combined with flags, refcount and usagecount into
 * single atomic variable.  This layout allow us to do some operations in a
 * single atomic operation, without actually acquiring and releasing spinlock;
 * for instance, increase or decrease refcount.  buf_id field never changes
 * after initialization, so does not need locking.  freeNext is protected by
 * the buffer_strategy_lock not buffer header lock.  The LWLock can take care
 * of itself.  The buffer header lock is *not* used to control access to the
 * data in the buffer!
 *
 * It's assumed that nobody changes the state field while buffer header lock
 * is held.  Thus buffer header lock holder can do complex updates of the
 * state variable in single write, simultaneously with lock release (cleaning
 * BM_LOCKED flag).  On the other hand, updating of state without holding
 * buffer header lock is restricted to CAS, which insure that BM_LOCKED flag
 * is not set.  Atomic increment/decrement, OR/AND etc. are not allowed.
 *
 * An exception is that if we have the buffer pinned, its tag can't change
 * underneath us, so we can examine the tag without locking the buffer header.
 * Also, in places we do one-time reads of the flags without bothering to
 * lock the buffer header; this is generally for situations where we don't
 * expect the flag bit being tested to be changing.
 *
 * We can't physically remove items from a disk page if another backend has
 * the buffer pinned.  Hence, a backend may need to wait for all other pins
 * to go away.  This is signaled by storing its own pgprocno into
 * wait_backend_pgprocno and setting flag bit BM_PIN_COUNT_WAITER.  At present,
 * there can be only one such waiter per buffer.
 *
 * We use this same struct for local buffer headers, but the locks are not
 * used and not all of the flag bits are useful either. To avoid unnecessary
 * overhead, manipulations of the state field should be done without actual
 * atomic operations (i.e. only pg_atomic_read_u32() and
 * pg_atomic_unlocked_write_u32()).
 *
 * Be careful to avoid increasing the size of the struct when adding or
 * reordering members.  Keeping it below 64 bytes (the most common CPU
 * cache line size) is fairly important for performance.
 *
 * Per-buffer I/O condition variables are currently kept outside this struct in
 * a separate array.  They could be moved in here and still fit within that
 * limit on common systems, but for now that is not done.
 */
typedef struct BufferDesc
{
	BufferTag	tag;			/* ID of page contained in buffer */
	int			buf_id;			/* buffer's index number (from 0) */

	/* state of the tag, containing flags, refcount and usagecount */
	pg_atomic_uint32 state;

	int			wait_backend_pgprocno;	/* backend of pin-count waiter */
	int			freeNext;		/* link in freelist chain */
	LWLock		content_lock;	/* to lock access to buffer contents */
#ifdef VISTA
    pg_atomic_flag        being_copied; /* true if buffer is being copied */
	pg_atomic_flag        copy_done; /* true if copy is done */
#endif
} BufferDesc;

/*
 * Concurrent access to buffer headers has proven to be more efficient if
 * they're cache line aligned. So we force the start of the BufferDescriptors
 * array to be on a cache line boundary and force the elements to be cache
 * line sized.
 *
 * XXX: As this is primarily matters in highly concurrent workloads which
 * probably all are 64bit these days, and the space wastage would be a bit
 * more noticeable on 32bit systems, we don't force the stride to be cache
 * line sized on those. If somebody does actual performance testing, we can
 * reevaluate.
 *
 * Note that local buffer descriptors aren't forced to be aligned - as there's
 * no concurrent access to those it's unlikely to be beneficial.
 *
 * We use a 64-byte cache line size here, because that's the most common
 * size. Making it bigger would be a waste of memory. Even if running on a
 * platform with either 32 or 128 byte line sizes, it's good to align to
 * boundaries and avoid false sharing.
 */
#define BUFFERDESC_PAD_TO_SIZE	(SIZEOF_VOID_P == 8 ? 64 : 1)

typedef union BufferDescPadded
{
	BufferDesc	bufferdesc;
	char		pad[BUFFERDESC_PAD_TO_SIZE];
} BufferDescPadded;

#ifdef VISTA

#define N_POOLS 3
#define vista_PoolId(id) ((id) / NBuffers)
#define vista_SlotId(id) ((id) % NBuffers)

#define vista_GetBufferCtl(bufCtl) (&(bufCtl->ctl))
#define vista_GetSegmentHeader(poolId) (vista_SegHdr[poolId])
#define vista_SegmentHeaderGetPoolId(seg_hdr) (seg_hdr->seg_pool_id)
#define vista_GetBufferDescriptor(id) (&vista_BufferDescriptors[vista_PoolId(id)][vista_SlotId(id)].bufferdesc)
#define vista_BufferDescriptorGetIOCV(bdesc) \
	(&(vista_BufferIOCVArray[vista_PoolId((bdesc)->buf_id)][vista_SlotId((bdesc)->buf_id)]).cv)
#define vista_BufferDescriptorGetPoolId(bdesc) (vista_PoolId((bdesc)->buf_id))
#define vista_BufferGetPoolId(buf) ((buf - 1) / NBuffers) // should be used for valid buffers (buf > 0)

#define GetLocalBufferDescriptor(id) (&LocalBufferDescriptors[(id)])
#define BufferDescriptorGetBuffer(bdesc) ((bdesc)->buf_id + 1)
#define BufferDescriptorGetContentLock(bdesc) \
	((LWLock*) (&(bdesc)->content_lock))

extern PGDLLIMPORT ConditionVariableMinimallyPadded *vista_BufferIOCVArray[N_POOLS];

/* Indicates how the VISTA buffer manager should behave. */
typedef enum VistaOpMode
{
	VISTA_MODE_NORMAL = 0, /* Behave as usual buffer pool. Could be 'NORMAL' or 'DRAIN' phase */
    VISTA_MODE_FLUSH = 1, /* Behave as COW buffer pool. Could be 'QUIESCE' or 'WRITEBACK' phase */
} VistaOpMode;

/* per-VMSeg header */
typedef struct vista_SegmentHeader
{
	/* read-mostly line */
    vista_seqlock_t seg_seqlock;
	uint32 seg_generation; /* generation of the segment */
	BufferPoolId seg_pool_id;
	pg_atomic_uint32 seg_state;
	/* padding for cacheline */
    char _pad1[VISTA_PADDING_64 - sizeof(vista_seqlock_t) - sizeof(uint32) - sizeof(BufferPoolId) - sizeof(pg_atomic_uint32)];
    /* write-heavy line */
    pg_atomic_uint32 seg_refcount_active;
	pg_atomic_uint32 seg_refcount_sealed;
    /* padding for cacheline */
    char _pad2[VISTA_PADDING_64 - sizeof(pg_atomic_uint32) - sizeof(pg_atomic_uint32)];
} vista_SegmentHeader;

typedef struct vista_BufferPoolControl
{
	vista_seqlock_t ctl_seqlock; /* seqlock for ctl */
	uint32 global_generation;
	VistaOpMode vista_op_mode; /* 0 = no flush, 1 = during flushing */
	vista_SegmentHeader *active_segment; /* pointer to the active segment. During flushing, it is new; during non-flushing, only this is used */
	vista_SegmentHeader *sealed_segment; /* pointer to the old segment. During flushing, it is sealed; During non-flushing, it is unused */
	int 	flushbgwprocno;
	bool	initial_snapshot_ready;
	pg_atomic_uint32 snapshot_generation;
	pg_atomic_uint32 remap_count; /* Number of OLAP processes attached to the remapped segment */
	uint32 oldest_olap_generation; /* The oldest OLAP generation among all attached OLAP processes */
} vista_BufferPoolControl;

typedef union vista_BufferPoolControlPadded
{
	vista_BufferPoolControl ctl;
	char pad[PG_CACHE_LINE_SIZE];
} vista_BufferPoolControlPadded;

#else

#define GetBufferDescriptor(id) (&BufferDescriptors[(id)].bufferdesc)
#define GetLocalBufferDescriptor(id) (&LocalBufferDescriptors[(id)])

#define BufferDescriptorGetBuffer(bdesc) ((bdesc)->buf_id + 1)

#define BufferDescriptorGetIOCV(bdesc) \
	(&(BufferIOCVArray[(bdesc)->buf_id]).cv)
#define BufferDescriptorGetContentLock(bdesc) \
	((LWLock*) (&(bdesc)->content_lock))

extern PGDLLIMPORT ConditionVariableMinimallyPadded *BufferIOCVArray;

#endif

/*
 * The freeNext field is either the index of the next freelist entry,
 * or one of these special values:
 */
#define FREENEXT_END_OF_LIST	(-1)
#define FREENEXT_NOT_IN_LIST	(-2)

/*
 * Functions for acquiring/releasing a shared buffer header's spinlock.  Do
 * not apply these to local buffers!
 */
extern uint32 LockBufHdr(BufferDesc *desc);
#define UnlockBufHdr(desc, s)	\
	do {	\
		pg_write_barrier(); \
		pg_atomic_write_u32(&(desc)->state, (s) & (~BM_LOCKED)); \
	} while (0)


/*
 * The PendingWriteback & WritebackContext structure are used to keep
 * information about pending flush requests to be issued to the OS.
 */
typedef struct PendingWriteback
{
	/* could store different types of pending flushes here */
	BufferTag	tag;
} PendingWriteback;

/* struct forward declared in bufmgr.h */
typedef struct WritebackContext
{
	/* pointer to the max number of writeback requests to coalesce */
	int		   *max_pending;

	/* current number of pending writeback requests */
	int			nr_pending;

	/* pending requests */
	PendingWriteback pending_writebacks[WRITEBACK_MAX_PENDING_FLUSHES];
} WritebackContext;

/* in buf_init.c */
#ifdef VISTA
extern PGDLLIMPORT vista_BufferPoolControlPadded *vista_BufCtl;
extern PGDLLIMPORT vista_bufferpool_offsets *vista_BufOffsets;
extern PGDLLIMPORT vista_SegmentHeader *vista_SegHdr[N_POOLS];
extern PGDLLIMPORT VistaBitmapHeader *vista_BitmapMgr;
extern PGDLLIMPORT BufferDescPadded *vista_BufferDescriptors[N_POOLS];
extern PGDLLIMPORT WritebackContext vista_BackendWritebackContext[N_POOLS];
extern PGDLLIMPORT char *vista_BufferBlocks[N_POOLS];
extern PGDLLIMPORT vista_vmseg_allocator vista_VmSegAllocatorPerPool[N_POOLS];
extern PGDLLIMPORT vista_shmem_allocator vista_ShmemAllocator;
extern PGDLLIMPORT struct vista_addrs vista_shmem_addrs;
#else
extern PGDLLIMPORT BufferDescPadded *BufferDescriptors;
extern PGDLLIMPORT WritebackContext BackendWritebackContext;
#endif
/* in localbuf.c */
extern PGDLLIMPORT BufferDesc *LocalBufferDescriptors;

/* in bufmgr.c */

/*
 * Structure to sort buffers per file on checkpoints.
 *
 * This structure is allocated per buffer in shared memory, so it should be
 * kept as small as possible.
 */
typedef struct CkptSortItem
{
	Oid			tsId;
	Oid			relNode;
	ForkNumber	forkNum;
	BlockNumber blockNum;
	int			buf_id;
} CkptSortItem;
#ifdef VISTA
extern PGDLLIMPORT CkptSortItem *vista_CkptBufferIds[N_POOLS];
#else
extern PGDLLIMPORT CkptSortItem *CkptBufferIds;
#endif
/*
 * Internal buffer management routines
 */
/* bufmgr.c */
extern void WritebackContextInit(WritebackContext *context, int *max_pending);
extern void IssuePendingWritebacks(WritebackContext *context);
extern void ScheduleBufferTagForWriteback(WritebackContext *context, BufferTag *tag);

/* freelist.c */
#ifdef VISTA
extern BufferDesc *vista_StrategyGetBufferNoEvict(BufferPoolId poolId, uint32 *buf_state);
extern BufferDesc *vista_StrategyGetBuffer(BufferPoolId poolId, BufferAccessStrategy strategy,
									 uint32 *buf_state);
extern void vista_StrategyFreeBuffer(BufferDesc *buf);
extern void vista_StrategyInitialize(BufferPoolId poolId);
#else
extern BufferDesc * StrategyGetBuffer(BufferAccessStrategy strategy,
									  uint32 *buf_state);
extern void StrategyFreeBuffer(BufferDesc *buf);
extern void StrategyInitialize(bool init);
#endif
extern bool StrategyRejectBuffer(BufferAccessStrategy strategy,
								 BufferDesc *buf);

#ifdef VISTA
extern int vista_StrategySyncStart(BufferPoolId poolId, uint32 *complete_passes, uint32 *num_buf_alloc);
extern void vista_StrategyNotifyBgWriter(BufferPoolId poolId, int bgwprocno);
#else
extern int	StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc);
extern void StrategyNotifyBgWriter(int bgwprocno);
#endif
extern Size StrategyShmemSize(void);
extern bool have_free_buffer(void);

/* buf_table.c */
extern Size BufTableShmemSize(int size);
#ifdef VISTA
extern void vista_InitBufTable(BufferPoolId poolId, int size);

extern uint32 vista_BufTableHashCode(BufferPoolId poolId, BufferTag *tagPtr);
extern int	vista_BufTableLookup(BufferPoolId poolId, BufferTag *tagPtr, uint32 hashcode);
extern int	vista_BufTableInsert(BufferPoolId poolId,BufferTag *tagPtr, uint32 hashcode, int buf_id);
extern void vista_BufTableDelete(BufferPoolId poolId,BufferTag *tagPtr, uint32 hashcode);
#else
extern void InitBufTable(int size);
extern uint32 BufTableHashCode(BufferTag *tagPtr);
extern int	BufTableLookup(BufferTag *tagPtr, uint32 hashcode);
extern int	BufTableInsert(BufferTag *tagPtr, uint32 hashcode, int buf_id);
extern void BufTableDelete(BufferTag *tagPtr, uint32 hashcode);
#endif

/* localbuf.c */
extern PrefetchBufferResult PrefetchLocalBuffer(SMgrRelation smgr,
												ForkNumber forkNum,
												BlockNumber blockNum);
extern BufferDesc *LocalBufferAlloc(SMgrRelation smgr, ForkNumber forkNum,
									BlockNumber blockNum, bool *foundPtr);
extern void MarkLocalBufferDirty(Buffer buffer);
extern void DropRelFileNodeLocalBuffers(RelFileNode rnode, ForkNumber forkNum,
										BlockNumber firstDelBlock);
extern void DropRelFileNodeAllLocalBuffers(RelFileNode rnode);
extern void AtEOXact_LocalBuffers(bool isCommit);


#endif							/* BUFMGR_INTERNALS_H */
