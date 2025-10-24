
#include "postgres.h"
#include <pthread.h> // For pthread_rwlock_t

#include "storage/buf_internals.h"
#include "storage/bufmgr.h"

#define MAX_SNAPSHOT 12288

uint32 vista_olap_generation = 0; // 0 is an invalid generation no.

static HTAB *vista_SnapshotHash;
static pthread_rwlock_t *vista_SnapshotHashLock;

typedef struct SnapshotEntry
{
    uint32 generation;
    char   name[MAXPGPATH]; // look at ExportSnapshot() to understand this length choice.
} SnapshotEntry;

static uint32 vista_SnapshotHashCode(uint32 *generationPtr);
static SnapshotEntry *vista_SnapshotLookup(uint32 *generationPtr, uint32 hashcode);
static int vista_SnapshotInsert(uint32 *generationPtr, uint32 hashcode, const char *name);

Size
SnapshotShmemSize(void)
{
    Size size = 0;

    size = add_size(size, hash_estimate_size(MAX_SNAPSHOT, sizeof(SnapshotEntry)));

    size = add_size(size, MAXALIGN(sizeof(pthread_rwlock_t)));

    return size;
}

void
vista_InitSnapshotHash(void)
{
    HASHCTL info;
	pthread_rwlockattr_t attr;

    // first we need a shmem for the vista_SnapshotHashLock
    vista_SnapshotHashLock = (pthread_rwlock_t *) vista_shmem_alloc(&vista_ShmemAllocator, sizeof(pthread_rwlock_t));
    if (vista_SnapshotHashLock == NULL)
    {
        ereport(FATAL, errmsg("vista_shmem_alloc failed for vista_SnapshotHashLock"));
    }

	// Initialize the lock for sharing between processes
	pthread_rwlockattr_init(&attr);
	pthread_rwlockattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
	pthread_rwlock_init(vista_SnapshotHashLock, &attr);
	pthread_rwlockattr_destroy(&attr);


    vista_BufOffsets->snapshot_hash_lock_offset = (uint32_t) ((char *) vista_SnapshotHashLock
                                                    - (char *) vista_ShmemAllocator.ShmemBase);
    info.keysize = sizeof(uint32);
    info.entrysize = sizeof(SnapshotEntry);
    // we do not use partitions for snapshots
    info.is_buffer_table = true;
    info.pool_id = N_POOLS;

    vista_SnapshotHash = ShmemInitHash("Vista Snapshot Hash",
                                        MAX_SNAPSHOT, MAX_SNAPSHOT,
                                        &info,
                                        HASH_ELEM | HASH_BLOBS);
}

static uint32
vista_SnapshotHashCode(uint32 *generationPtr)
{
    return get_hash_value(vista_SnapshotHash, (void*) generationPtr);
}

static SnapshotEntry *
vista_SnapshotLookup(uint32 *generationPtr, uint32 hashcode)
{
    SnapshotEntry *result;
    result = (SnapshotEntry *)
        hash_search_with_hash_value(vista_SnapshotHash,
                                    (void *) generationPtr,
                                    hashcode,
                                    HASH_FIND,
                                    NULL);
    if (!result)
    {
        elog(ERROR, "Snapshot for the given generation not found");
        return NULL;
    }
    return result;
}

static int
vista_SnapshotInsert(uint32 *generationPtr, uint32 hashcode, const char *name)
{
    SnapshotEntry *result;
    bool found;

    Assert(strlen(name) < MAXPGPATH); // ensure name fits

    result = (SnapshotEntry *)
        hash_search_with_hash_value(vista_SnapshotHash,
                                    (void *) generationPtr,
                                    hashcode,
                                    HASH_ENTER,
                                    &found);
    
    if (found)
    {
        elog(ERROR, "Snapshot for the given generation already exists");
        return result->generation; // or some error code
    }

    result->generation = *generationPtr;
    // copy the name into the entry
    strlcpy(result->name, name, MAXPGPATH);
    return -1; // success, no error
}

// We would not need to remove snapshots from the hash table

int
vista_StoreGenerationSnapshot(uint32 generation)
{
    Snapshot snap = GetTransactionSnapshot();
    char* name = ExportSnapshot(snap);
    int result;

    pthread_rwlock_wrlock(vista_SnapshotHashLock);
    result = vista_SnapshotInsert(&generation, vista_SnapshotHashCode(&generation), name);
    if (result >= 0)
    {
        pthread_rwlock_unlock(vista_SnapshotHashLock);
        elog(ERROR, "Failed to store snapshot for generation %u: %d", generation, result);
        return -1; // error
    }
    elog(LOG, "Stored a snapshot %s in gen %u", name, generation);
    pthread_rwlock_unlock(vista_SnapshotHashLock);
    return 0; // success    
}

int
vista_LoadGenerationSnapshot(uint32 generation)
{
    SnapshotEntry *result;
    pthread_rwlock_rdlock(vista_SnapshotHashLock);
    result = vista_SnapshotLookup(&generation, vista_SnapshotHashCode(&generation));
    if (!result)
    {
        pthread_rwlock_unlock(vista_SnapshotHashLock);
        elog(ERROR, "Failed to load snapshot for generation %u", generation);
        return -1; // error
    }
    pthread_rwlock_unlock(vista_SnapshotHashLock);
    ImportSnapshot(result->name);
    elog(LOG, "Loaded a snapshot %s in gen %u", result->name, generation);
    return 0; // success
}

/* This is used to clean up the snapshot hash table's old entries.
 * OLAP workers update the buffer control's oldest_olap_generation
 * when they finished query processing.
 * We can remove all snapshot entries older than that generation.
 * However, this does NOT remove the snapshot files from disk.
 * Snapshot files are very small, so we do not worry about that for now. 
*/
void
vista_CleanupSnapshotEntries(uint32 oldest_completed_generation)
{
	HASH_SEQ_STATUS status;
	SnapshotEntry *entry;

	pthread_rwlock_wrlock(vista_SnapshotHashLock);

	hash_seq_init(&status, vista_SnapshotHash);

	while ((entry = (SnapshotEntry *) hash_seq_search(&status)) != NULL)
	{
		if (entry->generation < oldest_completed_generation)
		{
			if (hash_search(vista_SnapshotHash,
							(void *) &(entry->generation),
							HASH_REMOVE,
							NULL) == NULL)
			{
				elog(WARNING, "Tried to remove snapshot for generation %u but it was not found", entry->generation);
			}
		}
	}

	pthread_rwlock_unlock(vista_SnapshotHashLock);

    elog(LOG, "Cleaned up snapshot entries older than generation %u", oldest_completed_generation);
}

pthread_rwlock_t *
GetVistaSnapshotLock(void)
{
	return vista_SnapshotHashLock;
}


// Need to allocate shared memory for the array of vista_Snapshot
// we will need to copy the data to the shared memory
// but, is it the best? we could perhaps use a linked list, or hash table

// (possible implementation)
// We need to know the curretn size of the array(current_generation)
// Then, in procarray.c, we modify ComputeXidHorizons()
// to check the array to find the oldest non-removable transaction ID.
// (example)
// for (int i = 0; i < MAX_GENERATIONS; i++)
// {
//     TransactionId sxmin = genSnaps[i].xmin;
//     if (TransactionIdIsValid(sxmin) &&
//         TransactionIdPrecedes(sxmin, h->data_oldest_nonremovable))
//         h->data_oldest_nonremovable = sxmin;
// }


// OLAP worker can RestoreSnapshot() to get the snapshot