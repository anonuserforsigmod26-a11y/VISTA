#ifndef __VISTA_BUFFER_ACCESS_H__
#define __VISTA_BUFFER_ACCESS_H__

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "vista_pg_atomics.h"
#include "vista_seqlock.h"
#include "libvista.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * These definitions are ported from PostgreSQL source code to allow libvista
 * to construct buffer tags and interpret page data without linking against
 * PostgreSQL itself.
 */

/* Basic Postgres types */
typedef size_t vista_pg_Size;

/* OID type as used in PostgreSQL */
typedef uint32_t vista_pg_Oid;

/* Block number within a relation */
typedef uint32_t vista_pg_BlockNumber;

#define VISTA_PG_BLCKSZ 8192
#define VISTA_NBUFFERS (127744)

/* Fork number within a relation */
typedef enum vista_pg_ForkNumber
{
	LIBVISTA_InvalidForkNumber = -1,
	LIBVISTA_MAIN_FORKNUM = 0,
	LIBVISTA_FSM_FORKNUM,
	LIBVISTA_VISIBILITYMAP_FORKNUM,
	LIBVISTA_INIT_FORKNUM
} vista_pg_ForkNumber;

/*
 * vista_pg_RelFileNode - identifies a physical relation file.
 */
typedef struct vista_pg_RelFileNode
{
	vista_pg_Oid			spcNode;		/* tablespace */
	vista_pg_Oid			dbNode;			/* database */
	vista_pg_Oid			relNode;		/* relation */
} vista_pg_RelFileNode;

/*
 * BufferTag - identifies a block within a relation. This is the key
 * for the buffer pool hash table.
 */
typedef struct vista_pg_BufferTag
{
	vista_pg_RelFileNode rnode;			/* physical relation identifier */
	vista_pg_ForkNumber	forkNum;
	vista_pg_BlockNumber blockNum;		/* blknum relative to begin of reln */
} vista_pg_BufferTag;

#define VISTA_PG_INIT_RELNODE(a,xx_spcNode,xx_dbNode,xx_relNode) \
( \
	(a).spcNode = (xx_spcNode), \
	(a).dbNode = (xx_dbNode), \
	(a).relNode = (xx_relNode) \
)

#define VISTA_PG_INIT_BUFFERTAG(a,xx_rnode,xx_forkNum,xx_blockNum) \
( \
	(a).rnode = (xx_rnode), \
	(a).forkNum = (xx_forkNum), \
	(a).blockNum = (xx_blockNum) \
)

/*
 * This is a placeholder for LWLock. In the context of lib-scan, we only
 * need to know its size to correctly calculate offsets. The lock itself
 * is not used by the read-only OLAP process.
 */
typedef struct vista_pg_LWLock
{
	uint16_t	tranche;
	vista_atomic_uint32 state;
	void	   *waiters; /* Actually a proclist_head */
} vista_pg_LWLock;

/*
 * Copied from buf_internals.h
 */
typedef struct vista_pg_BufferDesc
{
	vista_pg_BufferTag	tag;
	int			buf_id;
	vista_atomic_uint32 state;
	int			wait_backend_pgprocno;
	int			freeNext;
	vista_pg_LWLock	content_lock;
    vista_atomic_flag being_copied;
	vista_atomic_flag copy_done;
} vista_pg_BufferDesc;

#define VISTA_PG_BUFFERDESC_PAD_TO_SIZE	(sizeof(void *) == 8 ? 64 : 1)

typedef union vista_pg_BufferDescPadded
{
	vista_pg_BufferDesc	bufferdesc;
	char		pad[VISTA_PG_BUFFERDESC_PAD_TO_SIZE];
} vista_pg_BufferDescPadded;


typedef enum vista_pg_BufferPoolId
{
	LIBVISTA_ID_SPECIAL = -1,
	LIBVISTA_POOL_META = 0,
	LIBVISTA_POOL_OLTP1 = 1,
	LIBVISTA_POOL_OLTP2 = 2,
//	LIBVISTA_POOL_OLAP = 3,
	LIBVISTA_POOL_CHECK_OLTP = 3
} vista_pg_BufferPoolId;

#define LIBVISTA_PADDING_64 (64)

/* Forward declaration */
struct libvista_SegmentHeader;

/*
 * These definitions are copied from src/include/storage/buf_internals.h
 * to allow lib-scan to interpret the shared control structures without
 * including PostgreSQL internal headers.
 */
typedef enum libvistaOpMode
{
	LIBVISTA_MODE_NORMAL = 0, /* Behave as usual buffer pool. Could be 'NORMAL' or 'DRAIN' phase */
    LIBVISTA_MODE_FLUSH = 1, /* Behave as COW buffer pool. Could be 'QUIESCE' or 'WRITEBACK' phase */
} libvistaOpMode;

typedef struct libvista_BufferPoolControl
{
	libvista_seqlock_t ctl_seqlock; /* seqlock for ctl */
	uint32_t global_generation;
	libvistaOpMode vista_op_mode; /* 0 = no flush, 1 = during flushing */
	struct libvista_SegmentHeader *active_segment; /* pointer to the active segment. During flushing, it is new; during non-flushing, only this is used */
	struct libvista_SegmentHeader *sealed_segment; /* pointer to the old segment. During flushing, it is sealed; During non-flushing, it is unused */
	int 	flushbgwprocno;
	bool	initial_snapshot_ready;
	vista_atomic_uint32 snapshot_generation;
	vista_atomic_uint32 remap_count; /* Number of OLAP processes attached to the remapped segment */
	uint32_t oldest_olap_generation; /* The oldest OLAP generation among all attached OLAP processes */
} libvista_BufferPoolControl;

typedef struct libvista_SegmentHeader
{
	libvista_seqlock_t seg_seqlock;
	uint32_t seg_generation;
	vista_pg_BufferPoolId seg_pool_id;
	vista_atomic_uint32 seg_state;
	char _pad1[LIBVISTA_PADDING_64 - sizeof(libvista_seqlock_t) - sizeof(uint32_t) - sizeof(vista_pg_BufferPoolId) - sizeof(vista_atomic_uint32)];
	vista_atomic_uint32 seg_refcount_active;
	vista_atomic_uint32 seg_refcount_sealed;
	char _pad2[LIBVISTA_PADDING_64 - sizeof(vista_atomic_uint32) - sizeof(vista_atomic_uint32)];
} libvista_SegmentHeader;

/*
 * These definitions are ported from PostgreSQL's hsearch.h and dynahash.c
 * to allow navigation of the hash table structures from libvista.
 */

// Ported from storage/spin.h
typedef volatile unsigned char vista_pg_slock_t;

typedef struct VISTA_PG_HASHELEMENT
{
	struct VISTA_PG_HASHELEMENT *link;	/* link to next entry in same bucket */
	uint32_t		hashvalue;		/* hash function result for this entry */
} VISTA_PG_HASHELEMENT;

typedef VISTA_PG_HASHELEMENT *VISTA_PG_HASHBUCKET;
typedef VISTA_PG_HASHBUCKET *VISTA_PG_HASHSEGMENT;

// Ported from utils/dynahash.c
typedef struct vista_pg_FreeListData
{
	vista_pg_slock_t		mutex;			/* spinlock for this freelist */
	long		nentries;		/* number of entries in associated buckets */
	VISTA_PG_HASHELEMENT *freeList;		/* chain of free elements */
} vista_pg_FreeListData;

// Ported from utils/dynahash.c
#define VISTA_PG_NUM_FREELISTS 32
typedef struct VISTA_PG_HASHHDR
{
	vista_pg_FreeListData freeList[VISTA_PG_NUM_FREELISTS];
	long		dsize;
	long		nsegs;
	uint32_t	max_bucket;
	uint32_t	high_mask;
	uint32_t	low_mask;
	uint32_t	keysize;
	uint32_t	entrysize;
	long		num_partitions;
	long		max_dsize;
	long		ssize;
	int			sshift;
	int			nelem_alloc;
} VISTA_PG_HASHHDR;

// typedef struct
// {
// 	vista_pg_BufferTag key;
// 	int			id;
// 	uint32_t		generation;
// } vista_pg_BufferLookupEnt;

typedef struct
{
	vista_pg_BufferTag key;
	int			id;
	uint32_t		generation;
} vista_pg_BufferLookupEnt;

uint32_t
vista_get_hash_value(const void *keyPtr, vista_pg_Size keysize);

/*
 * Structure to hold the reconstructed pointers to the buffer pool components.
 * These are the virtual addresses from the perspective of the external process.
 */
typedef struct vista_reconstructed_buffer_pool
{
	libvista_SegmentHeader *seg_hdr;
	vista_pg_BufferDescPadded *descriptors;
	char *blocks;
	
	uint64_t remapped_vmseg_base_addr;
	uint64_t original_vmseg_base_addr;

	// Reconstructed hash table info
	VISTA_PG_HASHHDR *hash_header;
	VISTA_PG_HASHSEGMENT *hash_directory;
	vista_pg_Size keysize;
	long ssize;
	int sshift;

} vista_reconstructed_buffer_pool;

extern __thread vista_reconstructed_buffer_pool g_reconstructed_pool;


/*
 * vista_hash_search
 *		Look up a key in a remapped hash table.
 */
void *
vista_hash_search(vista_reconstructed_buffer_pool *pool,
				  const void *keyPtr,
				  uint32_t hashvalue);

/*
 * Function to reconstruct the buffer pool.
 */
void vista_reconstruct_buffer_pool(void *remapped_vmseg_base, vista_bufferpool_offsets *offsets);


/*
 * vista_read_buffer -- perform a read-only lookup for a buffer page.
 *
 * This function attempts to find a specific database block in the remapped
 * buffer pool. It reconstructs pointers and uses a translated version of
 * PostgreSQL's hash search algorithm. It is read-only and uses a sequence
 * lock to ensure safety against concurrent modifications by the main
 * PostgreSQL backend.
 *
 * segment: A pointer to the initialized vista_segment.
 * spcNode: OID of the tablespace.
 * dbNode: OID of the database.
 * relNode: OID of the relation's relfilenode.
 * blockNum: The block number to look up.
 *
 * Returns a pointer to the buffer's content (a page of size VISTA_PG_BLCKSZ) on a
 * cache hit, or NULL on a cache miss. The returned pointer is valid only
 * as long as the remapped segment is attached.
 */
void *vista_read_buffer(vista_reconstructed_buffer_pool *pool,
						vista_pg_Oid spcNode,
						vista_pg_Oid dbNode,
						vista_pg_Oid relNode,
						vista_pg_BlockNumber blockNum);

#ifdef __cplusplus
}
#endif

#endif /* VISTA_BUFFER_ACCESS_H */
