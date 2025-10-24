#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <assert.h>
#include <stdatomic.h>
#include <pthread.h>

#include "pg_types.h"
#include "pg_constants.h"
#include "pg_structs.h"
#include "file_access.h"
#include "memory_utils.h"

/* Table-specific file storage */
typedef struct {
    int db_node;
    int rel_node;
    File files[MAX_SEGMENT_COUNT];
    pthread_mutex_t lock;  // Per-table lock for OpenOrGetSegment
    int in_use;
} TableFiles;

static TableFiles table_files[MAX_TABLES] = {0};
static pthread_rwlock_t table_allocation_rwlock = PTHREAD_RWLOCK_INITIALIZER;  // Global rwlock for GetTableFiles

/* Get table-specific files array */
static TableFiles* GetTableFiles(int db_node, int rel_node) {
    // Try read lock first (fast path - table already exists)
    pthread_rwlock_rdlock(&table_allocation_rwlock);
    for (int i = 0; i < MAX_TABLES; i++) {
        if (table_files[i].in_use &&
            table_files[i].db_node == db_node &&
            table_files[i].rel_node == rel_node) {
            pthread_rwlock_unlock(&table_allocation_rwlock);
            return &table_files[i];
        }
    }
    pthread_rwlock_unlock(&table_allocation_rwlock);

    // Not found - acquire write lock to allocate
    pthread_rwlock_wrlock(&table_allocation_rwlock);

    // Double-check: another thread may have allocated it
    for (int i = 0; i < MAX_TABLES; i++) {
        if (table_files[i].in_use &&
            table_files[i].db_node == db_node &&
            table_files[i].rel_node == rel_node) {
            pthread_rwlock_unlock(&table_allocation_rwlock);
            return &table_files[i];
        }
    }

    // Allocate new table entry
    for (int i = 0; i < MAX_TABLES; i++) {
        if (!table_files[i].in_use) {
            table_files[i].db_node = db_node;
            table_files[i].rel_node = rel_node;
            table_files[i].in_use = 1;
            memset(table_files[i].files, 0, sizeof(table_files[i].files));
            pthread_mutex_init(&table_files[i].lock, NULL);
            pthread_rwlock_unlock(&table_allocation_rwlock);
            return &table_files[i];
        }
    }

    pthread_rwlock_unlock(&table_allocation_rwlock);
    return NULL;
}

/*
 * FileSize: Get the size of a file.
 *
 * file: The file descriptor to get the size of.
 *
 * Returns the size of the file in bytes, or -1 on error.
 */
off_t FileSize(File file) {
  struct stat st;
  if (fstat(file, &st) < 0) {
    return -1;
  }
  return st.st_size;
}

/*
 * FileRead: Read data from a file.
 * 
 * file: The file descriptor to read from.
 * buffer: The buffer to store the read data.
 * amount: The number of bytes to read.
 * offset: The offset in the file to start reading from.
 * 
 * Returns the number of bytes read.
 */
int FileRead(File file, char *buffer, int amount, off_t offset) {
  int ret;

  ret = pread(file, buffer, amount, offset);

  return ret;
}

/*
 * OpenOrGetSegment: Open a segment file or return the already opened file.
 * 
 * dbNode: The database node number.
 * relNode: The relation node number.
 * segment: The segment number.
 * 
 * Returns the file descriptor for the segment, or -1 if file doesn't exist.
 */
File OpenOrGetSegment(int dbNode, int relNode, int segment) {
  TableFiles* table_files = GetTableFiles(dbNode, relNode);
  if (!table_files) {
    return -1;
  }

  pthread_mutex_lock(&table_files->lock);

  // Check if already opened
  if (table_files->files[segment] != 0) {
    File file = table_files->files[segment];
    pthread_mutex_unlock(&table_files->lock);
    return file;
  }

  // Open new segment
  char filename[512];
  if (segment) {
    snprintf(filename, sizeof(filename), "%s/base/%u/%u.%u", POSTGRES_DATA_DIR, dbNode, relNode,
             segment);
  } else {
    snprintf(filename, sizeof(filename), "%s/base/%u/%u", POSTGRES_DATA_DIR, dbNode, relNode);
  }

  File file = open(filename, O_RDONLY | O_DIRECT);
  if (file < 0) {
    pthread_mutex_unlock(&table_files->lock);
    return -1;  /* File doesn't exist - this is normal for segments */
  }

  table_files->files[segment] = file;
  pthread_mutex_unlock(&table_files->lock);
  return file;
}


/*
 * FileBufferInit: Initialize file descriptors for a range of segments.
 * 
 * args: The arguments structure containing the database node, relation node,
 *       start segment, and end segment.
 */
void FileBufferInit(void *args) {
  struct args_struct *args_ptr = (struct args_struct *)args;

  int dbNode = args_ptr->arg1;
  int relNode = args_ptr->arg2;
  int start = args_ptr->arg3;
  int end = args_ptr->arg4;

  for (int segment = start; segment < end; segment++) {
    File file = OpenOrGetSegment(dbNode, relNode, segment);
    if (file < 0) {
      /* Segment doesn't exist, skip it */
      continue;
    }
  }
}

/*
 * mdnblocks: Get the number of blocks in a relation.
 *
 * dbNode: The database node number.
 * relNode: The relation node number.
 *
 * Returns the number of blocks in the relation.
 */
BlockNumber mdnblocks(int dbNode, int relNode) {
  BlockNumber count = 0;
  for (int segment = 0; segment < MAX_SEGMENT_COUNT; segment++) {
    File file;
    Size size;

    file = OpenOrGetSegment(dbNode, relNode, segment);
    if (file < 0) {
      /* No more segments exist - PostgreSQL segments are sequential */
      break;
    }

    size = FileSize(file);
    count += (BlockNumber)(size / BLOCK_SIZE);
  }
  return count;
}

/*
 * md_getseg: Get the segment number for a given block number.
 *
 * blkno: The block number to get the segment for.
 *
 * Returns the segment number.
 */
int md_getseg(BlockNumber blkno) { 
	return (int)(blkno / RELSEG_SIZE); 
}

/*
 * mdread: Read a block from a relation using direct file I/O.
 *
 * dbNode: The database node number.
 * relNode: The relation node number.
 * blkno: The block number to read.
 *
 * Returns a pointer to the page header of the block, or NULL on failure.
 */
PageHeader mdread(int dbNode, int relNode, BlockNumber blkno) {
  int segment = md_getseg(blkno);
  
  /* Thread-local storage for block buffer */
  static __thread char block_buffer[BLOCK_SIZE];
  
  TableFiles* table_files = GetTableFiles(dbNode, relNode);
  if (!table_files) {
    return NULL;
  }

  /* Ensure the segment file is open */
  if (table_files->files[segment] <= 0) {
    File file = OpenOrGetSegment(dbNode, relNode, segment);
    if (file < 0) {
      return NULL;  /* Segment doesn't exist */
    }
  }

  /* Calculate offset within the segment */
  off_t offset = (off_t)(blkno % RELSEG_SIZE) * BLOCK_SIZE;
  
  /* Read block directly from file */
  if (FileRead(table_files->files[segment], block_buffer, BLOCK_SIZE, offset) != BLOCK_SIZE) {
    return NULL;  /* Read failed */
  }

  return (PageHeader)block_buffer;
}
