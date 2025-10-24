#include <stdio.h>
#include "include/vista_pg_read_buffer.h"
#include "include/vista_seqlock.h"

// Global pointers for the reconstructed buffer pool
__thread vista_reconstructed_buffer_pool g_reconstructed_pool;

/*
 * vista_reconstruct_buffer_pool
 *		Reconstructs the buffer pool data structures from the remapped VMSeg.
 *
 * This function should be called by the external process after it has remapped
 * the VMSeg. It uses the base address of the remapped window and the offsets
 * stored in the shared metadata region to calculate the correct virtual
 * addresses for all the buffer pool components.
 */
void
vista_reconstruct_buffer_pool(void *remapped_vmseg_base, vista_bufferpool_offsets *offsets)
{
    libvista_SegmentHeader* seghdr = (libvista_SegmentHeader*) remapped_vmseg_base;
    int pool_id = seghdr->seg_pool_id;

	// Reconstruct the main buffer pool components
	g_reconstructed_pool.seg_hdr = (libvista_SegmentHeader *) ((char *) remapped_vmseg_base + offsets->seg_hdr_offset[pool_id]);
	g_reconstructed_pool.descriptors = (vista_pg_BufferDescPadded *) ((char *) remapped_vmseg_base + offsets->buf_desc_offset[pool_id]);
	g_reconstructed_pool.blocks = (char *) remapped_vmseg_base + offsets->buf_blocks_offset[pool_id];

	g_reconstructed_pool.remapped_vmseg_base_addr = (uint64_t) remapped_vmseg_base;
	g_reconstructed_pool.original_vmseg_base_addr = (uint64_t) offsets->vmseg_base_addr[pool_id];

	// Reconstruct the hash table components
	vista_hash_table_offsets *hash_offsets = &offsets->buf_hash_offsets[pool_id];
	g_reconstructed_pool.hash_header = (VISTA_PG_HASHHDR *) ((char *) remapped_vmseg_base + hash_offsets->hctl_offset);
	g_reconstructed_pool.hash_directory = (VISTA_PG_HASHSEGMENT *) ((char *) remapped_vmseg_base + hash_offsets->dir_offset);
	g_reconstructed_pool.keysize = hash_offsets->keysize;
	g_reconstructed_pool.ssize = hash_offsets->ssize;
	g_reconstructed_pool.sshift = hash_offsets->sshift;
}

/*
 * This is a replacement for vista_ReadBuffer. 
 */
void *vista_read_buffer(struct vista_reconstructed_buffer_pool *pool,
						vista_pg_Oid spcNode,
						vista_pg_Oid dbNode,
						vista_pg_Oid relNode,
						vista_pg_BlockNumber blockNum)
{
	vista_pg_BufferTag tag;
	uint32_t hash;
	int buf_id;
	void *buf_block;
	vista_pg_BufferLookupEnt *result;
    int pool_id;

    pool_id = pool->seg_hdr->seg_pool_id;

	/* 1. Initialize the buffer tag to search for. */
	VISTA_PG_INIT_RELNODE(tag.rnode, spcNode, dbNode, relNode);
	VISTA_PG_INIT_BUFFERTAG(tag, tag.rnode, LIBVISTA_MAIN_FORKNUM, blockNum);

	/*
	 * 2. Begin read. We must have already increased the sealed_refcnt to safely read the buffer.
	 * We first need to get the hash code.
	 */
	hash = vista_get_hash_value(&tag, pool->keysize);

	/*
	* 4. Perform the hash search. This function is designed to work with
	* the remapped address space.
	*/
	result = (vista_pg_BufferLookupEnt*) vista_hash_search(pool, &tag, hash);

	buf_id = (result != NULL) ? result->id : -1;

	/*
	* 5. If the buffer is not found, it's a cache miss. Exit the loop
	* and return NULL.
	*/
	if (buf_id < 0)
	{
		return NULL;
	}

	/*
	 * 6. On a cache hit, reconstruct the pointers to the buffer descriptor
	 * and the actual buffer page (block).
	 */
	buf_block = (void *) ((char*) pool->blocks + (vista_pg_Size) (buf_id - (VISTA_NBUFFERS * pool_id)) * VISTA_PG_BLCKSZ);
	return buf_block;

	// /*
	//  * (Optional)
	//  * Final check: The tag in the buffer descriptor should match our
	//  * search tag. This is a sanity check; the hash search should have
	//  * already guaranteed this.
	//  */
	// if (buf_desc->tag.rnode.relNode == tag.rnode.relNode &&
	// 	buf_desc->tag.blockNum == tag.blockNum)
	// {
	// 	return buf_block;
	// }
}
