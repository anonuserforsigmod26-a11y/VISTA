#ifndef LIBSCAN_C_API_H
#define LIBSCAN_C_API_H

#include <cstddef>
#include <stdint.h>
#include <stdbool.h>

// BlockGroup information structure
typedef struct {
    uint32_t total_blocks;
    uint32_t total_groups;
    uint32_t blocks_per_group;
} BlockGroupInfo;

#ifdef __cplusplus
extern "C" {
#endif

/* Create TupleDesc from column information */
void* LibScan_CreateTupleDescFromColumns(int num_columns,
                                        int* type_oids,
                                        int16_t* lengths,
                                        bool* nullables,
                                        char* alignments,
                                        bool* by_values);

/* Free TupleDesc created by CreateTupleDescFromColumns */
void LibScan_FreeTupleDesc(void* tupdesc);

/* Get BlockGroup information for a table */
BlockGroupInfo LibScan_GetBlockGroupInfo(uint32_t db_node, uint32_t rel_node, uint32_t block_group_size);

/* Initialize PostgreSQL scanner for specific BlockGroup */
void* LibScan_InitPostgresScanForBlockGroup(uint32_t spc_node, uint32_t db_node, uint32_t rel_node, void* tupdesc,
                                           uint32_t block_group_id, uint32_t block_group_size, uint32_t sub_group_size,
                                           uint32_t uncached_subgroup_mask, void* snapshot=NULL);

/* Fill DuckDB chunk from specific BlockGroup */
bool LibScan_FillChunkFromBlockGroup(void* chunk, void* scan_state);

/* Cleanup PostgreSQL BlockGroup scanner state */
void LibScan_CleanupPostgresScanBlockGroup(void* scan_state);

/* Initialize global file buffers (call once per table) */
void LibScan_InitFileBuffers(uint32_t db_node, uint32_t rel_node);

/* Cleanup global file buffers (call at end) */
void LibScan_CleanupFileBuffers(void);

/* Get current SubGroup ID within BlockGroup (0-based) */
uint32_t LibScan_GetCurrentSubGroupId(void* scan_state);

/* Skip SubGroup pages in Vista (for cache hit) */
bool LibScan_SkipSubGroupInVista(void* scan_state, uint32_t subgroup_id);

#ifdef __cplusplus
}
#endif

#endif /* LIBSCAN_C_API_H */
