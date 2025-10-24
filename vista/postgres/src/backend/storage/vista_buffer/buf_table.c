/*-------------------------------------------------------------------------
 *
 * buf_table.c
 *	  routines for mapping BufferTags to buffer indexes.
 *
 * Note: the routines in this file do no locking of their own.  The caller
 * must hold a suitable lock on the appropriate BufMappingLock, as specified
 * in the comments.  We can't do the locking inside these functions because
 * in most cases the caller needs to adjust the buffer header contents
 * before the lock is released (see notes in README).
 *
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/buffer/buf_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"

/* entry for buffer lookup hashtable */
typedef struct
{
	BufferTag	key;			/* Tag of a disk page */
	int			id;				/* Associated buffer ID */
	uint32		generation; /* this must match the segment generation to be a valid entry.*/
} BufferLookupEnt;

#define N_POOLS 3
static HTAB *vista_SharedBufHash[N_POOLS];


/*
 * Estimate space needed for mapping hashtable
 *		size is the desired hash table size (possibly more than NBuffers)
 */
Size
BufTableShmemSize(int size)
{
	return hash_estimate_size(size, sizeof(BufferLookupEnt));
}

/*
 * Initialize shmem hash table for mapping buffers
 *		size is the desired hash table size (possibly more than NBuffers)
 * (VISTA) Added pool_id argument to support multiple buffer pools.
 */
void
vista_InitBufTable(BufferPoolId poolId, int size)
{
	HASHCTL		info;
	char name_temp[64]; // VISTA

	/* assume no locking is needed yet */

	/* BufferTag maps to Buffer */
	info.keysize = sizeof(BufferTag);
	info.entrysize = sizeof(BufferLookupEnt);
	info.num_partitions = NUM_BUFFER_PARTITIONS;
	info.is_buffer_table = true;
	info.pool_id = poolId;

	snprintf(name_temp, sizeof(name_temp), "Shared Buffer Lookup Table%d", poolId);

	vista_SharedBufHash[poolId] = ShmemInitHash(name_temp,
								  size, size,
								  &info,
								  HASH_ELEM | HASH_BLOBS | HASH_PARTITION);
}

/*
 * BufTableHashCode
 *		Compute the hash code associated with a BufferTag
 *
 * This must be passed to the lookup/insert/delete routines along with the
 * tag.  We do it like this because the callers need to know the hash code
 * in order to determine which buffer partition to lock, and we don't want
 * to do the hash computation twice (hash_any is a bit slow).
 */
uint32
vista_BufTableHashCode(BufferPoolId poolId, BufferTag *tagPtr)
{
	return get_hash_value(vista_SharedBufHash[poolId], (void *) tagPtr);
}

/*
 * BufTableLookup
 *		Lookup the given BufferTag; return buffer ID, or -1 if not found
 *
 * Caller must hold at least share lock on BufMappingLock for tag's partition
 * (VISTA) Since we use the contiguous range of buffer IDs, regardless of the pool,
 * the returned buffer ID should be correctly interpreted using something like vista_GetBufferDescriptor().
 */
int
vista_BufTableLookup(BufferPoolId poolId, BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result, *reclaim_result;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(vista_SharedBufHash[poolId],
									(void *) tagPtr,
									hashcode,
									HASH_FIND,
									NULL);

	if (!result)
		return -1;

	return result->id;
}

/*
 * BufTableInsert
 *		Insert a hashtable entry for given tag and buffer ID,
 *		unless an entry already exists for that tag
 *
 * Returns -1 on successful insertion.  If a conflicting entry exists
 * already, returns the buffer ID in that entry.
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 */
int
vista_BufTableInsert(BufferPoolId poolId, BufferTag *tagPtr, uint32 hashcode, int buf_id)
{
	BufferLookupEnt *result;
	bool		found;
	uint32		curr_generation;

	Assert(buf_id >= 0);		/* -1 is reserved for not-in-table */
	Assert(tagPtr->blockNum != P_NEW);	/* invalid tag */

	curr_generation = vista_GetSegmentHeader(poolId)->seg_generation;

	result = (BufferLookupEnt *)
		hash_search_with_hash_value(vista_SharedBufHash[poolId],
									(void *) tagPtr,
									hashcode,
									HASH_ENTER,
									&found);

	if (found)					/* found something already in the table */
	{
		if (result->generation == curr_generation)
		{
			// found a valid entry (collision)
			return result->id;
		}
		else
		{
			// stale entry from older generation, we can update it.
			result->id = buf_id;
			result->generation = curr_generation;
			return -1; // successfully updated the entry and returned -1.
		}
	}

	// successfully inserted a new entry
	result->id = buf_id;
	result->generation = curr_generation;

	return -1;
}

/*
 * BufTableDelete
 *		Delete the hashtable entry for given tag (which must exist)
 *
 * Caller must hold exclusive lock on BufMappingLock for tag's partition
 * (VISTA) actually we don't care about the pool_id here.
 */
void
vista_BufTableDelete(BufferPoolId poolId, BufferTag *tagPtr, uint32 hashcode)
{
	BufferLookupEnt *result;
	bool found;
	int pool_id;
	
	if (poolId >= 0)
	{
		// in this case, we use the specified pool_id
		pool_id = poolId;
		result = (BufferLookupEnt *)
			hash_search_with_hash_value(vista_SharedBufHash[pool_id],
										(void *) tagPtr,
										hashcode,
										HASH_REMOVE,
										NULL);
		if (!result)				/* shouldn't happen */
			elog(ERROR, "shared buffer hash table corrupted %d", pool_id);
		if (result->generation != vista_GetSegmentHeader(pool_id)->seg_generation)
		{
			elog(ERROR, "shared buffer hash table corrupted - generation mismatch at pool %d", pool_id);
		}

		return;
	}
	// otherwise, we search all pools (this is strictly for vista_InvalidateBuffer() use)

	found = false;
	for (pool_id = 0; pool_id < N_POOLS; pool_id++)
	{
		result = (BufferLookupEnt *)
			hash_search_with_hash_value(vista_SharedBufHash[pool_id],
										(void *) tagPtr,
										hashcode,
									HASH_FIND,
									NULL);
		if (result)
		{
			found = true;
			break;
		}
	}
	if (!found)
	{
		elog(ERROR, "shared buffer hash table corrupted");
	}
	result = (BufferLookupEnt *)
		hash_search_with_hash_value(vista_SharedBufHash[pool_id],
									(void *) tagPtr,
									hashcode,
								HASH_REMOVE,
								NULL);
	if (!result)				/* shouldn't happen */
		elog(ERROR, "shared buffer hash table corrupted at pool %d - this is impossible.", pool_id);



        // if (!result)				/* shouldn't happen */
	// 	elog(ERROR, "shared buffer hash table corrupted");
}

void
reset_buf_table(BufferPoolId poolId)
{
    HASH_SEQ_STATUS status;
    BufferLookupEnt *ent;

    hash_seq_init(&status, vista_SharedBufHash[poolId]);
    while ((ent = (BufferLookupEnt *) hash_seq_search(&status)) != NULL)
    {
        hash_search(vista_SharedBufHash[poolId],
                    (void*) &ent->key,
                    HASH_REMOVE, NULL);
    }
}
