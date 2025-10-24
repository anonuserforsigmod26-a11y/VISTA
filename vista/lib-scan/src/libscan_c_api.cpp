#include "libscan_c_api.h"
#include "tuple_to_duckdb.h"
#include "vista_heapam_visibility.h"

// ===== C API Wrappers for DuckDB integration =====

extern "C" {

void* LibScan_CreateTupleDescFromColumns(int num_columns,
                                        int* type_oids,
                                        int16_t* lengths,
                                        bool* nullables,
                                        char* alignments,
                                        bool* by_values) {
    return CreateTupleDescFromColumns(num_columns, type_oids, lengths, nullables, alignments, by_values);
}

void LibScan_FreeTupleDesc(void* tupdesc) {
    FreeTupleDesc(tupdesc);
}

BlockGroupInfo LibScan_GetBlockGroupInfo(uint32_t db_node, uint32_t rel_node, uint32_t block_group_size) {
    return GetBlockGroupInfo(db_node, rel_node, block_group_size);
}

void* LibScan_InitPostgresScanForBlockGroup(uint32_t spc_node, uint32_t db_node, uint32_t rel_node, void* tupdesc,
                                           uint32_t block_group_id, uint32_t block_group_size, uint32_t sub_group_size,
                                           uint32_t uncached_subgroup_mask, void* snapshot) {
    return InitPostgresScanForBlockGroup(spc_node, db_node, rel_node, tupdesc, block_group_id, block_group_size, sub_group_size, uncached_subgroup_mask, (VistaSnapshot)snapshot);
}

bool LibScan_FillChunkFromBlockGroup(void* chunk, void* scan_state) {
    return FillChunkFromBlockGroup(chunk, scan_state);
}

void LibScan_CleanupPostgresScanBlockGroup(void* scan_state) {
    CleanupPostgresScanBlockGroup(scan_state);
}

void LibScan_InitFileBuffers(uint32_t db_node, uint32_t rel_node) {
    InitFileBuffers(db_node, rel_node);
}

void LibScan_CleanupFileBuffers(void) {
    CleanupFileBuffers();
}

uint32_t LibScan_GetCurrentSubGroupId(void* scan_state) {
    return GetCurrentSubGroupId(scan_state);
}

bool LibScan_SkipSubGroupInVista(void* scan_state, uint32_t subgroup_id) {
    return SkipSubGroupInVista(scan_state, subgroup_id);
}

int LibScan_GetCurrentSubGroupAllValid(void* scan_state) {
		return GetCurrentSubGroupAllValid(scan_state);
}

} // extern "C"
