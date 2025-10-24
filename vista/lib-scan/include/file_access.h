#ifndef FILE_ACCESS_H
#define FILE_ACCESS_H

#include "pg_types.h"
#include "pg_structs.h"

/* PostgreSQL data directory base path - modify this for your environment */
#ifndef POSTGRES_DATA_DIR
#define POSTGRES_DATA_DIR "/path/to/your/data"
#endif

#ifdef __cplusplus
extern "C" {
#endif


/* File operations */
off_t FileSize(File file);
int FileRead(File file, char *buffer, int amount, off_t offset);

/* Buffer management */
void FileBufferInit(void *args);

/* Block access */
BlockNumber mdnblocks(int dbNode, int relNode);
int md_getseg(BlockNumber blkno);
PageHeader mdread(int dbNode, int relNode, BlockNumber blkno);

/* Segment access */
File OpenOrGetSegment(int dbNode, int relNode, int segment);

#ifdef __cplusplus
}
#endif

#endif /* FILE_ACCESS_H */
