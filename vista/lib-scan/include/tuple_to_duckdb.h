#ifndef TUPLE_TO_DUCKDB_H
#define TUPLE_TO_DUCKDB_H

#include "pg_types.h"
#include "pg_structs.h"
#include "tuple_access.h"
#include "vista_heapam_visibility.h"

// Forward declarations for DuckDB types (avoid namespace conflicts)
namespace duckdb {
    class Vector;
    class DataChunk;
    class LogicalType;
}

/* Insert a single PostgreSQL tuple into DuckDB DataChunk */
void InsertTupleIntoChunk(duckdb::DataChunk &output, TupleTableSlot *slot, uint64_t row_idx);

/* Process one block and return number of tuples processed
 * start_tuple_offset: which tuple in the block to start from
 * next_tuple_offset: where to continue next time (output parameter)
 * all_tuples_valid: output parameter - 1 if all tuples in this block are in stable state, 0 otherwise
 * Returns: number of tuples processed
 */
uint64_t ProcessBlock(duckdb::DataChunk &chunk, TupleDesc tupdesc,
                    PageHeader page, uint64_t start_offset,
                    OffsetNumber start_tuple_offset, OffsetNumber *next_tuple_offset,
                    VistaSnapshot snapshot=NULL, int *all_tuples_valid=NULL);

/* High-level API for DuckDB integration */

// PostgreSQL table scan state
typedef struct {
    int db_node;
    int rel_node;
    TupleDesc tupdesc;
    BlockNumber total_blocks;
    BlockNumber current_blkno;
    OffsetNumber current_tuple_offset;
    bool finished;
} PostgresScanState;

// BlockGroupInfo is defined in libscan_c_api.h
#include "libscan_c_api.h"

// BlockGroup scan state (for parallel processing)
typedef struct {
    uint32_t spc_node;
    uint32_t db_node;
    uint32_t rel_node;
    TupleDesc tupdesc;
    
    // BlockGroup range
    uint32_t block_group_id;
    uint32_t block_group_size;
    uint32_t sub_group_size;      // SubGroup size for cache invalidation
    uint32_t uncached_subgroup_mask;  // Bitmask of SubGroups that need vista_open
    BlockNumber start_blkno;
    BlockNumber end_blkno;
    
    // Current position
    BlockNumber current_blkno;
    OffsetNumber current_tuple_offset;
    bool finished;

    // SubGroup validity tracking
    int current_subgroup_all_valid;  // 1 if all tuples in current SubGroup are stable, 0 otherwise

#ifdef USE_VISTA_IO
    // Vista-specific fields
    int vd;                    // Vista descriptor
    bool vista_initialized;    // Vista channel initialization status
    void* current_page;        // Current active page pointer
    bool has_active_page;      // Whether we have an active page
#ifdef USE_VISTA_REMAP
    bool using_remapped_page;  // Whether current_page is from remapped buffer
#endif
#endif
    VistaSnapshot snapshot; // Snapshot for visibility checks (can be NULL)
} PostgresScanBlockGroupState;

/* Cleanup global file buffers (for GlobalState destructor) */
void CleanupFileBuffers();

/* Initialize global file buffers (for GlobalState constructor) */
void InitFileBuffers(uint32_t db_node, uint32_t rel_node);


/* 
 * Create TupleDesc from column information (for DuckDB integration)
 * Returns: void* pointer to TupleDesc (to avoid exposing PostgreSQL types to DuckDB)
 */
void* CreateTupleDescFromColumns(int num_columns,
                                int* type_oids,
                                int16_t* lengths,
                                bool* nullables,
                                char* alignments,
                                bool* by_values);

/* Free TupleDesc created by CreateTupleDescFromColumns */
void FreeTupleDesc(void* tupdesc);

/* BlockGroup API for parallel scanning */

/* Get BlockGroup information for a table */
BlockGroupInfo GetBlockGroupInfo(uint32_t db_node, uint32_t rel_node, uint32_t block_group_size);

/* 
 * Initialize PostgreSQL scanner for specific BlockGroup 
 * Returns: void* pointer to PostgresScanBlockGroupState (to match DuckDB extern "C" signature)
 */
void* InitPostgresScanForBlockGroup(uint32_t spc_node, uint32_t db_node, uint32_t rel_node, void* tupdesc,
                                   uint32_t block_group_id, uint32_t block_group_size, uint32_t sub_group_size,
                                   uint32_t uncached_subgroup_mask, VistaSnapshot snapshot=NULL);

/*
 * Fill DuckDB chunk from specific BlockGroup
 * Returns: true if chunk was filled, false if BlockGroup is finished
 */
bool FillChunkFromBlockGroup(void* chunk, void* scan_state);

#ifdef USE_VISTA_IO
/* Vista-based BlockGroup implementation */
bool FillChunkFromBlockGroup_Vista(void* chunk, void* scan_state);
#endif

/* Cleanup PostgreSQL BlockGroup scanner state */
void CleanupPostgresScanBlockGroup(void* scan_state);

/* Get current SubGroup ID within BlockGroup (0-based) */
uint32_t GetCurrentSubGroupId(void* scan_state);

/* Get current SubGroup validity status (1 if all tuples stable, 0 otherwise) */
int GetCurrentSubGroupAllValid(void* scan_state);

/* Skip SubGroup pages in Vista (for cache hit) */
bool SkipSubGroupInVista(void* scan_state, uint32_t subgroup_id);

#endif /* TUPLE_TO_DUCKDB_H */
