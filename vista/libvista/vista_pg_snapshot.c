#include "vista_pg_snapshot.h"
#include "vista_pg_read_buffer.h"
#include "vista_seqlock.h"

#include <stdint.h>
#include <string.h>
#include <pthread.h> // For pthread_rwlock_t
#include <stdio.h>

typedef struct VistaPGSnapshotEntry
{
    uint32_t generation;
    char     name[VISTA_PG_SNAPSHOT_NAME_MAX]; // look at ExportSnapshot() to understand this length choice.
} VistaPGSnapshotEntry;

static int vista_load_generation_snapshot_name(void *shrd_metadata_ptr, uint32_t gen, char *name)
{
    uint32_t hash;
    char *base = (char *)shrd_metadata_ptr;
    vista_bufferpool_offsets *offsets = (vista_bufferpool_offsets *)(base + *((int *)base));
    vista_hash_table_offsets *snapshot_hash_offsets = &offsets->snapshot_hash_offset;
    VistaPGSnapshotEntry *result;
	pthread_rwlock_t *snapshot_lock = (pthread_rwlock_t *)(base + offsets->snapshot_hash_lock_offset);
	int ret = 0;

    /*
     * (Abuse the reconstructed buffer pool structure for convenience..)
     * We only need the hash table related fields.
     */
    vista_reconstructed_buffer_pool pool;
    pool.remapped_vmseg_base_addr = (uint64_t) base;
    pool.original_vmseg_base_addr = (uint64_t) offsets->shmem_base_addr;
    pool.hash_header = (VISTA_PG_HASHHDR *) (base + snapshot_hash_offsets->hctl_offset);
	pool.hash_directory = (VISTA_PG_HASHSEGMENT *) (base + snapshot_hash_offsets->dir_offset);
	pool.keysize = snapshot_hash_offsets->keysize;
	pool.ssize = snapshot_hash_offsets->ssize;
	pool.sshift = snapshot_hash_offsets->sshift;

    hash = vista_get_hash_value((const void *)&gen, pool.keysize);

	pthread_rwlock_rdlock(snapshot_lock);

    result = (VistaPGSnapshotEntry *)vista_hash_search(&pool,
                                                       (const void *)&gen,
                                                       hash);
    
    if (unlikely(!result))
    {
		ret = -1;
        goto exit;
    }

    if (unlikely(gen != result->generation))
    {
		ret = -1;
        goto exit;
    }

    memcpy(name, result->name, VISTA_PG_SNAPSHOT_NAME_MAX);

exit:
	pthread_rwlock_unlock(snapshot_lock);
    return ret;
}

int vista_get_snapshot_name(void *shrd_metadata_ptr, char *name, uint32_t* generation)
{
    uint32_t old;
    uint32_t gen = 0;
    char *base = (char *)shrd_metadata_ptr;
    vista_bufferpool_offsets *offsets = (vista_bufferpool_offsets *)(base + *((int *)base));
    libvista_BufferPoolControl *buf_ctl = (libvista_BufferPoolControl *)(base + offsets->buf_ctl_offset);

    /*
     * Get a generation number from the shmem.
     */
    do {
        old = libvista_seqlock_read_begin(&buf_ctl->ctl_seqlock);
        gen = buf_ctl->global_generation - 1;
    } while (unlikely(libvista_seqlock_read_retry(&buf_ctl->ctl_seqlock, old)));

    if (generation)
    {
        *generation = gen;
    }
    else
    {
        fprintf(stderr, "[libscan] failed to get generation\n");
    }

    return vista_load_generation_snapshot_name(shrd_metadata_ptr, gen, name);
}

/* 
 * This has a strong assumption that the OLAP generation monotonically increases.
 * Thus, we do not need to consider a running OLAP transaction that has an older generation than the current one.
 */
void vista_set_oldest_olap_generation(void *shrd_metadata_ptr, uint32_t olap_current_generation)
{
    char *base = (char *)shrd_metadata_ptr;
    vista_bufferpool_offsets *offsets = (vista_bufferpool_offsets *)(base + *((int *)base));
    libvista_BufferPoolControl *buf_ctl = (libvista_BufferPoolControl *)(base + offsets->buf_ctl_offset);

    if (buf_ctl->oldest_olap_generation < olap_current_generation)
    {
        buf_ctl->oldest_olap_generation = olap_current_generation;
    }
    return;
}
