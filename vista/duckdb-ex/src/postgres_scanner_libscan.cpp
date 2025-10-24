// postgres_scanner_libscan.cpp - PostgresScanner using lib-scan for local file access

#include "duckdb.hpp"
#include <cstdio>
#include <iostream>
#include <libpq-fe.h>
#include <chrono>
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "postgres_filter_pushdown.hpp"
#include "postgres_scanner.hpp"
#include "postgres_result.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "storage/postgres_table_set.hpp"
#include "global_cache_manager.hpp"
#include "vista_cache_loader.hpp"
#include "cache_constants.hpp"

// original code: postgres_scanner.cpp

// BlockGroup information structure
struct BlockGroupInfo {
    uint32_t total_blocks;
    uint32_t total_groups;
    uint32_t blocks_per_group;
};

// Include lib-scan C API
extern "C" {
    void* LibScan_CreateTupleDescFromColumns(int num_columns, int* type_oids, int16_t* lengths, 
                                             bool* nullables, char* alignments, bool* by_values);
    void LibScan_FreeTupleDesc(void* tupdesc);
    void LibScan_InitFileBuffers(uint32_t db_node, uint32_t rel_node);
    void LibScan_CleanupFileBuffers(void);
    uint32_t LibScan_GetCurrentSubGroupId(void* scan_state);
    int LibScan_GetCurrentSubGroupAllValid(void* scan_state);
    bool LibScan_SkipSubGroupInVista(void* scan_state, uint32_t subgroup_id);
    
    BlockGroupInfo LibScan_GetBlockGroupInfo(uint32_t db_node, uint32_t rel_node, uint32_t block_group_size);
    void* LibScan_InitPostgresScanForBlockGroup(uint32_t spc_node, uint32_t db_node, uint32_t rel_node, void* tupdesc,
                                               uint32_t block_group_id, uint32_t block_group_size, uint32_t sub_group_size, uint32_t uncached_subgroup_mask,
                                               void *snapshot=NULL);
    bool LibScan_FillChunkFromBlockGroup(void* chunk, void* scan_state);
    void LibScan_CleanupPostgresScanBlockGroup(void* scan_state);

    // Vista generation and flush tracking
    // Returns: (generation << 32) | flush
    unsigned long vista_get_generation_flush_values(void);
}

namespace duckdb {

struct PostgresGlobalState;

// Local state for lib-scan
struct PostgresLocalState : public LocalTableFunctionState {
    bool done = false;
    vector<column_t> column_ids;
    TableFilterSet *filters;
    void* libscan_state = nullptr;
    void* tupdesc = nullptr;
    uint32_t current_block_group = UINT32_MAX;

    // SubGroup processing state
    uint32_t current_subgroup_id = UINT32_MAX;
    std::vector<DataChunk*> subgroup_chunks;
    uint32_t current_chunk_idx = 0;
    bool subgroup_finished = false;  // Flag to track if current SubGroup is completely processed
    bool blockgroup_done = false;     // Flag to track if current BlockGroup ended

    bool subgroup_from_cache = false; // Flag to track if current SubGroup was loaded from cache
    // New fields for tracking SubGroup processing
    uint32_t processed_subgroup_index = 0;  // Which SubGroup (0-based) we're currently processing
    uint32_t total_subgroups_in_bg = 0;     // Total SubGroups in current BlockGroup
    uint32_t uncached_subgroup_mask = 0;    // Bitmask of uncached SubGroups
    uint64_t subgroup_pre_work_versions[MAX_SUBGROUPS_PER_BG];  // Bitmap versions for each SubGroup
    // Cache system
    BlockGroupCache* cache = nullptr;
    // Vista-based cache loader for optimized I/O
    VistaCacheLoader vista_loader;

    PostgresLocalState() {
        // Initialize version array with MAX value
        for (uint32_t i = 0; i < MAX_SUBGROUPS_PER_BG; i++) {
            subgroup_pre_work_versions[i] = UINT64_MAX;
        }
    }

    ~PostgresLocalState() {
        if (libscan_state) {
            LibScan_CleanupPostgresScanBlockGroup(libscan_state);
        }
        if (tupdesc) {
            LibScan_FreeTupleDesc(tupdesc);
        }
        // Clean up SubGroup chunks
        for (auto chunk : subgroup_chunks) {
            delete chunk;
        }
        subgroup_chunks.clear();
    }

    void ScanChunk(ClientContext &context, const PostgresBindData &bind_data, PostgresGlobalState &gstate,
                   DataChunk &output);
};

// Global state for lib-scan
struct PostgresGlobalState : public GlobalTableFunctionState {
    explicit PostgresGlobalState(idx_t max_threads, uint32_t bg_size = DEFAULT_BLOCK_GROUP_SIZE)
        : max_threads(max_threads), block_group_size(bg_size) {
        next_block_group.store(0);
        current_total_blocks.store(0);
        current_total_block_groups.store(0);
        file_size_updated.store(false);
        spc_oid = 0;
        db_oid = 0;
        rel_oid = 0;
        initial_generation = 0;
        initial_was_flushing = false;
    }

    ~PostgresGlobalState() {
        LibScan_CleanupFileBuffers();
    }

    // Thread management
    idx_t max_threads;

    // BlockGroup distribution (atomic for thread safety)
    std::atomic<uint32_t> next_block_group;
    std::atomic<uint32_t> current_total_blocks;
    std::atomic<uint32_t> current_total_block_groups;
    std::atomic<bool> file_size_updated;

    uint32_t block_group_size;
    BlockGroupInfo initial_bg_info;

    // Generation tracking for file size updates
    uint32_t initial_generation;
    bool initial_was_flushing;
    
    uint32_t spc_oid;
    uint32_t db_oid;
    uint32_t rel_oid;

    void *snapshot;
    
    // Connection for catalog queries
    PostgresConnection connection;
    
    // Filter information (stored like Parquet extension)  
    vector<column_t> column_ids;
    optional_ptr<TableFilterSet> filters;
    vector<string> column_names;  // For debugging
    
    // Get next BlockGroup atomically
    uint32_t GetNextBlockGroup() {
        uint32_t bg = next_block_group.fetch_add(1);
        uint32_t total = current_total_block_groups.load();
        // printf("GetNextBlockGroup: bg=%u, total=%u\n", bg, total);
        if (bg >= total) {
            return UINT32_MAX;  // No more BlockGroups available
        }
        return bg;
    }
    
    idx_t MaxThreads() const override {
        //THREAD_LOG("MaxThreads() called, returning: %zu\n", max_threads);
        return max_threads;
    }
    
    // Connection management
    PostgresConnection &GetConnection() {
        return connection;
    }
    
    void SetConnection(PostgresConnection conn) {
        this->connection = std::move(conn);
    }
    
    void SetConnection(shared_ptr<OwnedPostgresConnection> conn) {
        this->connection = PostgresConnection(std::move(conn));
    }
};

// Check if we can use lib-scan
static void ValidateLibScanRequirements(const PostgresBindData &bind_data, PostgresConnection &connection) {
    auto table = bind_data.GetTable();
    
    // Check basic requirements
    if (!table) {
        throw IOException("lib-scan requires a valid table reference");
    }
    
    if (table->relfilenode == 0) {
        throw IOException("lib-scan requires a valid relfilenode for table %s.%s", 
                         bind_data.schema_name, bind_data.table_name);
    }
    
    if (bind_data.table_name.empty()) {
        throw IOException("lib-scan requires a table name");
    }
    
    if (!bind_data.sql.empty()) {
        throw IOException("lib-scan does not support complex SQL queries, only simple table scans");
    }
    
    if (!bind_data.read_only) {
        throw IOException("lib-scan only supports read-only access");
    }
    
    // Check if connection is local
    auto result = connection.Query("SELECT inet_server_addr() IS NULL AS is_local");
    if (!result || result->Count() == 0) {
        throw IOException("Failed to check if PostgreSQL connection is local");
    }
    
    string is_local = result->GetString(0, 0);
    if (is_local != "t") {
        throw IOException("lib-scan requires a local PostgreSQL connection");
    }
}

// Get database OID
static int GetDatabaseOid(PostgresConnection &connection) {
    auto result = connection.Query("SELECT oid FROM pg_database WHERE datname = current_database()");
    if (!result || result->Count() == 0) {
        throw IOException("Failed to get database OID - check PostgreSQL connection and permissions");
    }
    return result->GetInt32(0, 0);
}

// Get tablespace OID for table
static uint32_t GetTablespaceOid(PostgresConnection &connection, uint32_t rel_oid) {
    auto result = connection.Query(
        "SELECT CASE WHEN c.reltablespace = 0 THEN d.dattablespace ELSE c.reltablespace END "
        "FROM pg_class c JOIN pg_database d ON d.datname = current_database() "
        "WHERE c.relfilenode = " + std::to_string(rel_oid)
    );
    if (!result || result->Count() == 0) {
        throw IOException("Failed to get tablespace OID for relfilenode %u", rel_oid);
    }
    return result->GetInt32(0, 0);
}

// Get BlockGroup information for table
static BlockGroupInfo GetBlockGroupInfoForTable(PostgresConnection &connection, const string &schema, const string &table, 
                                               uint32_t db_oid, uint32_t rel_oid, uint32_t block_group_size) {
    // First check if table exists
    auto check_result = connection.Query(StringUtil::Format(
        "SELECT 1 FROM information_schema.tables WHERE table_schema='%s' AND table_name='%s'",
        schema, table
    ));
    
    if (!check_result || check_result->Count() == 0) {
        throw IOException("Table %s.%s does not exist", schema, table);
    }
    
    // Use lib-scan to get BlockGroup information
    return LibScan_GetBlockGroupInfo(db_oid, rel_oid, block_group_size);
}

// Create TupleDesc from catalog info
static void* CreateTupleDescFromBindData(const PostgresBindData &bind_data) {
    int num_columns = bind_data.postgres_types.size();
    if (num_columns == 0) {
        throw IOException("Cannot create TupleDesc: table has no columns");
    }
    
    // All arrays for lib-scan API (all parameters are pointers)
    int* type_oids = new int[num_columns];
    int16_t* lengths = new int16_t[num_columns];
    char* alignments = new char[num_columns];
    bool* nullables = new bool[num_columns];
    bool* by_values = new bool[num_columns];
    
    for (size_t i = 0; i < bind_data.postgres_types.size(); i++) {
        const auto &pg_type = bind_data.postgres_types[i];
        
        // Type OID from PostgresType
        type_oids[i] = pg_type.oid;
        
        // Get column info from PostgresColumnInfo array (must exist)
        if (i >= bind_data.postgres_column_infos.size()) {
            // Clean up allocated memory before throwing
            delete[] type_oids;
            delete[] lengths;
            delete[] alignments;
            delete[] nullables;
            delete[] by_values;
            throw IOException("PostgresColumnInfo missing for column %zu - expected %zu columns", 
                             i, bind_data.postgres_column_infos.size());
        }
        
        const auto &col_info = bind_data.postgres_column_infos[i];
        lengths[i] = col_info.length;
        alignments[i] = col_info.alignment;
        by_values[i] = col_info.by_value;
        
        // All columns are nullable by default (PostgresColumnInfo doesn't have not_null)
        nullables[i] = true;
    }
    
    void* tupdesc = LibScan_CreateTupleDescFromColumns(
        num_columns,
        type_oids,
        lengths,
        nullables,
        alignments,
        by_values
    );
    
    // Clean up all allocated arrays
    delete[] type_oids;
    delete[] lengths;
    delete[] alignments;
    delete[] nullables;
    delete[] by_values;
    
    if (!tupdesc) {
        throw IOException("Failed to create TupleDesc for lib-scan");
    }
    
    return tupdesc;
}

// PrepareBind function (from original postgres_scanner.cpp)  
void PostgresScanFunction::PrepareBind(PostgresVersion version, ClientContext &context, PostgresBindData &bind_data,
                                       idx_t approx_num_pages) {
    Value pages_per_task;
    if (context.TryGetCurrentSetting("pg_pages_per_task", pages_per_task)) {
        bind_data.pages_per_task = UBigIntValue::Get(pages_per_task);
        if (bind_data.pages_per_task == 0) {
            bind_data.pages_per_task = PostgresBindData::DEFAULT_PAGES_PER_TASK;
        }
    }
    bool use_ctid_scan = true;
    Value pg_use_ctid_scan;
    if (context.TryGetCurrentSetting("pg_use_ctid_scan", pg_use_ctid_scan)) {
        use_ctid_scan = BooleanValue::Get(pg_use_ctid_scan);
    }
    if (version.major_v < 14) {
        // Disable parallel CTID scan on older Postgres versions since it is not efficient
        // see https://github.com/duckdb/postgres_scanner/issues/186
        use_ctid_scan = false;
    }
    if (!use_ctid_scan) {
        approx_num_pages = 0;
    }
    bind_data.SetTablePages(approx_num_pages);
    bind_data.version = version;
}

void PostgresBindData::SetTablePages(idx_t approx_num_pages) {
    this->pages_approx = approx_num_pages;
    if (!read_only) {
        max_threads = 1;
    } else {
        max_threads = MaxValue<idx_t>(pages_approx / pages_per_task, 1);
    }
}


void PostgresBindData::SetCatalog(PostgresCatalog &catalog) {
    this->pg_catalog = &catalog;
}

void PostgresBindData::SetTable(PostgresTableEntry &table) {
    this->pg_table = &table;
}

// Apply filters to output chunk
void ApplyFiltersToChunk(DataChunk &output, const vector<column_t> &column_ids, 
                         TableFilterSet *filters, const vector<string> &column_names) {
    if (!filters || filters->filters.empty() || output.size() == 0) {
        return;  // No filters to apply or no data
    }
    // Create a selection vector for filtered rows
    SelectionVector sel(output.size());
    idx_t result_count = 0;
    
    // Check each row against filters
    for (idx_t row_idx = 0; row_idx < output.size(); row_idx++) {
        bool passes_filter = true;
        
        // Check all filters for this row
        for (auto &filter_entry : filters->filters) {
            auto col_idx = filter_entry.first;
            auto &filter = *filter_entry.second;

            // col_idx is an index into the column_ids array
            // column_ids[col_idx] is the actual column index in the table
            if (col_idx >= column_ids.size()) {
                continue;
            }

            // Since ScanChunk projects columns in column_ids order,
            // output.data[col_idx] corresponds to the data for column_ids[col_idx]
            idx_t actual_table_col_idx = column_ids[col_idx];  // Actual table column index
            idx_t output_col_idx = col_idx;                    // Index in output.data[] after projection

            // Range check
            if (output_col_idx >= output.ColumnCount()) {
                continue;
            }
            
            // Use DuckDB's built-in filter evaluation using ColumnSegment::FilterSelection pattern
            auto &vector = output.data[output_col_idx];
            UnifiedVectorFormat vdata;
            vector.ToUnifiedFormat(output.size(), vdata);
            
            SelectionVector temp_sel(output.size());
            idx_t approved_count = output.size();
            
            // Apply the filter using DuckDB's internal logic pattern
            switch (filter.filter_type) {
                case TableFilterType::CONSTANT_COMPARISON: {
                    auto &constant_filter = filter.Cast<ConstantFilter>();
                    Value row_value = vector.GetValue(row_idx);
                    // NULL values don't pass comparison filters (SQL standard)
                    if (row_value.IsNull()) {
                        passes_filter = false;
                    } else {
                        passes_filter = constant_filter.Compare(row_value);
                    }
                    break;
                }
                case TableFilterType::IS_NULL: {
                    Value row_value = vector.GetValue(row_idx);
                    passes_filter = row_value.IsNull();
                    break;
                }
                case TableFilterType::IS_NOT_NULL: {
                    Value row_value = vector.GetValue(row_idx);
                    passes_filter = !row_value.IsNull();
                    break;
                }
                default: {
                    passes_filter = true;  // Pass through unsupported filters
                    break;
                }
            }
            
            if (!passes_filter) {
                break;  // Skip remaining filters for this row
            }
        }
        
        if (passes_filter) {
            sel.set_index(result_count++, row_idx);
        }
    }
    
    // Apply selection vector to output if some rows were filtered
    
    if (result_count < output.size()) {
        output.Slice(sel, result_count);
    }
}

// Scan implementation
void PostgresLocalState::ScanChunk(ClientContext &context, const PostgresBindData &bind_data,
                                  PostgresGlobalState &gstate, DataChunk &output) {
    while (!done) {
        if (!libscan_state) {
            // Check if file size needs to be updated (only if initial state was flushing)
            if (gstate.initial_was_flushing && !gstate.file_size_updated.load()) {
                unsigned long gen_flush = vista_get_generation_flush_values();
                uint32_t current_gen = (uint32_t)(gen_flush >> 32);
                bool current_flushing = (uint32_t)(gen_flush & 0xFFFFFFFF);

                // Check if generation changed or flush completed
                if (current_gen != gstate.initial_generation || !current_flushing) {
                    // Try to claim the update with CAS - only one thread succeeds
                    bool expected = false;
                    if (gstate.file_size_updated.compare_exchange_strong(expected, true)) {
                        // This thread won - update file size
                        BlockGroupInfo new_info = LibScan_GetBlockGroupInfo(gstate.db_oid, gstate.rel_oid, gstate.block_group_size);
                        gstate.current_total_blocks.store(new_info.total_blocks);
                        gstate.current_total_block_groups.store(new_info.total_groups);
                    }
                }
            }

            // Get next BlockGroup
            current_block_group = gstate.GetNextBlockGroup();
            if (current_block_group == UINT32_MAX) {
                done = true;
                output.SetCardinality(0);
                return;
            }

            // If this is the last BlockGroup, ensure file size is up-to-date
            uint32_t total_groups = gstate.current_total_block_groups.load();
            if (current_block_group == total_groups - 1) {
                if (gstate.initial_was_flushing) {
                    // Wait until file size is updated
                    while (!gstate.file_size_updated.load()) {
                        unsigned long gen_flush = vista_get_generation_flush_values();
                        uint32_t current_gen = (uint32_t)(gen_flush >> 32);
                        bool current_flushing = (uint32_t)(gen_flush & 0xFFFFFFFF);

                        if (current_gen != gstate.initial_generation || !current_flushing) {
                            // Generation changed or flush completed - try to update
                            bool expected = false;
                            if (gstate.file_size_updated.compare_exchange_strong(expected, true)) {
                                BlockGroupInfo new_info = LibScan_GetBlockGroupInfo(gstate.db_oid, gstate.rel_oid, gstate.block_group_size);
                                gstate.current_total_blocks.store(new_info.total_blocks);
                                gstate.current_total_block_groups.store(new_info.total_groups);
                                break;
                            } else {
																break;
														}
                        } else {
                            // Continue waiting
                        }
                    }
                }
            }

            // Reset SubGroup state for new BlockGroup
            current_subgroup_id = UINT32_MAX;
            current_chunk_idx = 0;
            subgroup_finished = false;
            blockgroup_done = false;
            for (auto chunk : subgroup_chunks) {
                delete chunk;
            }
            subgroup_chunks.clear();

            // Reset cache for new BlockGroup
            cache = nullptr;

            // Initialize cache for this BlockGroup
            cache = GlobalCacheManager::GetOrCreateBlockGroupCache(gstate.db_oid, gstate.rel_oid, current_block_group);

            // Check all SubGroup cache status for this BlockGroup
            // Calculate actual SubGroups in this specific BlockGroup
            uint32_t bg_start = current_block_group * gstate.block_group_size;
            uint32_t bg_end = bg_start + gstate.block_group_size - 1;
            uint32_t total_blocks = gstate.current_total_blocks.load();
            if (bg_end >= total_blocks) {
                bg_end = total_blocks - 1;
            }
            uint32_t actual_bg_size = bg_end - bg_start + 1;
            uint32_t subgroups_per_bg = (actual_bg_size + DEFAULT_SUB_GROUP_SIZE - 1) / DEFAULT_SUB_GROUP_SIZE;

            // Initialize SubGroup tracking for new BlockGroup
            total_subgroups_in_bg = subgroups_per_bg;
            processed_subgroup_index = 0;

            // Reset version array for new BlockGroup
            for (uint32_t i = 0; i < MAX_SUBGROUPS_PER_BG; i++) {
                subgroup_pre_work_versions[i] = UINT64_MAX;
            }

            // Get invalid SubGroup mask (1 = invalid, needs reload) with bitmap versions
            uncached_subgroup_mask = cache->getInvalidSubGroupMask(subgroups_per_bg, subgroup_pre_work_versions);

            // Initialize for BlockGroup with invalid SubGroup mask
            libscan_state = LibScan_InitPostgresScanForBlockGroup(gstate.spc_oid, gstate.db_oid, gstate.rel_oid,
                                                                 tupdesc,
                                                                 current_block_group,
                                                                 gstate.block_group_size,
                                                                 DEFAULT_SUB_GROUP_SIZE,
                                                                 uncached_subgroup_mask,
                                                                 gstate.snapshot);

            if (!libscan_state) {
                throw IOException("Failed to initialize lib-scan for BlockGroup %u of table %s.%s",
                                current_block_group, bind_data.schema_name, bind_data.table_name);
            }
        }

        // Step 1: Load SubGroup if we don't have chunks loaded
        if (subgroup_chunks.empty()) {
            // Check if all SubGroups processed
            while (processed_subgroup_index < total_subgroups_in_bg) {
                uint32_t sg_id = processed_subgroup_index;
                // Check if cached using uncached_mask
                if (!(uncached_subgroup_mask & (1U << sg_id))) {
                    // Cached - load from cache using Vista
                    subgroup_chunks = cache->loadSubGroup(sg_id, &vista_loader, column_ids, bind_data.physical_types);
                    subgroup_from_cache = true;

                    // Update lib-scan position to skip this cached SubGroup (only modify state->current_blkno)
                    LibScan_SkipSubGroupInVista(libscan_state, sg_id);

                    processed_subgroup_index++;
                    break;  // Got data, exit loop
                } else {
                    // Uncached - this SubGroup is in Vista/lib-scan
                    uint32_t libscan_sg_id = LibScan_GetCurrentSubGroupId(libscan_state);

                    // Verify lib-scan is at the right SubGroup
                    if (libscan_sg_id != sg_id) {
                        // Mismatch - this shouldn't happen if our logic is correct
                        // Skip this SubGroup
                        processed_subgroup_index++;
                        continue;
                    }

                    // Load entire SubGroup from lib-scan
                    while (true) {
                        DataChunk* chunk = new DataChunk();
                        chunk->Initialize(Allocator::Get(context), bind_data.physical_types);

                        bool has_more = LibScan_FillChunkFromBlockGroup(chunk, libscan_state);

                        if (chunk->size() > 0) {
                            subgroup_chunks.push_back(chunk);
                        } else {
                            delete chunk;
                            // Empty chunk - add nullptr marker
                            subgroup_chunks.push_back(nullptr);
                        }

                        if (!has_more) {
                            // No more data from lib-scan
                            break;
                        }

                        // Check if we've moved to a different SubGroup
                        uint32_t current_sg = LibScan_GetCurrentSubGroupId(libscan_state);
                        if (current_sg != libscan_sg_id) {
                            // Moved to next SubGroup in lib-scan
                            break;
                        }
                    }

                    // Check if all tuples in SubGroup are stable
                    int subgroup_all_valid = LibScan_GetCurrentSubGroupAllValid(libscan_state);

                    // Store loaded SubGroup in cache only if all tuples are stable and bitmap is valid
                    if (subgroup_all_valid) {
                        if (cache->ValidateBitmapForCache(sg_id, subgroup_pre_work_versions[sg_id])) {
                            cache->storeSubGroup(sg_id, subgroup_chunks);
                        }
                    } 

                    subgroup_from_cache = false;
                    processed_subgroup_index++;
                    break;  // Got data, exit loop
                }
            }

            // If we've processed all SubGroups and no data, mark BlockGroup as done
            if (processed_subgroup_index >= total_subgroups_in_bg && subgroup_chunks.empty()) {
                blockgroup_done = true;
                // libscan_state cleanup happens in Step 3
            } else if (!subgroup_chunks.empty()) {
                // We have data to process
                current_subgroup_id = processed_subgroup_index - 1;  // We already incremented
                current_chunk_idx = 0;
                subgroup_finished = false;
            }
        }

        // Step 2: Return one chunk from loaded SubGroup
        if (!subgroup_chunks.empty() && current_chunk_idx < subgroup_chunks.size()) {
            DataChunk* chunk = subgroup_chunks[current_chunk_idx];
            current_chunk_idx++;

            // Check for nullptr marker (empty chunk)
            if (chunk == nullptr) {
                // Skip this empty chunk marker
                // Check if SubGroup finished
                if (current_chunk_idx >= subgroup_chunks.size()) {
                    // SubGroup is now finished
                    subgroup_finished = true;

                    // Clean up chunks (skip nullptr)
                    for (auto c : subgroup_chunks) {
                        if (c != nullptr) {
                            delete c;
                        }
                    }
                    subgroup_chunks.clear();
                    current_chunk_idx = 0;

                    // If BlockGroup finished, cleanup libscan
                    if (blockgroup_done) {
                        LibScan_CleanupPostgresScanBlockGroup(libscan_state);
                        libscan_state = nullptr;
                    } else {
                        // Reset for next SubGroup
                        current_subgroup_id = UINT32_MAX;
                        subgroup_finished = false;
                    }
                }
                // Continue outer while to get next chunk or SubGroup
                continue;
            }

            // Project columns (skip if loaded from cache as it's already projected)
            output.Reset();
            if (subgroup_from_cache) {
                // Cache chunks are already projected - copy directly
                for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
                    output.data[i].Reference(chunk->data[i]);
                }
            } else {
                // Non-cache chunks need projection
                for (idx_t i = 0; i < column_ids.size(); i++) {
                    idx_t source_col = column_ids[i];
                    if (source_col >= chunk->ColumnCount()) {
                        throw IOException("Column index %zu out of range", source_col);
                    }
                    output.data[i].Reference(chunk->data[source_col]);
                }
            }
            output.SetCardinality(chunk->size());

            // Step 3: Clean up if SubGroup finished
            if (current_chunk_idx >= subgroup_chunks.size()) {
                // SubGroup is now finished
                subgroup_finished = true;

                // Clean up chunks (skip nullptr)
                for (auto c : subgroup_chunks) {
                    if (c != nullptr) {
                        delete c;
                    }
                }
                subgroup_chunks.clear();
                current_chunk_idx = 0;

                // If BlockGroup finished, cleanup libscan
                if (blockgroup_done) {
                    LibScan_CleanupPostgresScanBlockGroup(libscan_state);
                    libscan_state = nullptr;
                } else {
                    // Reset for next SubGroup
                    current_subgroup_id = UINT32_MAX;
                    subgroup_finished = false;  // Reset for next SubGroup
                }
            }
            return;
        }

        // Step 3: Clean up if SubGroup finished
        if (libscan_state) {
            LibScan_CleanupPostgresScanBlockGroup(libscan_state);
            libscan_state = nullptr;
        }
    }
    
    output.SetCardinality(0);
}

// Bind function (from original postgres_scanner.cpp)
// Most fields are unused. 
static unique_ptr<FunctionData> PostgresBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto bind_data = make_uniq<PostgresBindData>();

    bind_data->dsn = input.inputs[0].GetValue<string>();
    bind_data->schema_name = input.inputs[1].GetValue<string>();
    bind_data->table_name = input.inputs[2].GetValue<string>();

    auto con = PostgresConnection::Open(bind_data->dsn);
    auto version = con.GetPostgresVersion();
		
    auto info = PostgresTableSet::GetTableInfo(con, bind_data->schema_name, bind_data->table_name);

    bind_data->postgres_types = info->postgres_types;
    for (auto &col : info->create_info->columns.Logical()) {
        names.push_back(col.GetName());
        return_types.push_back(col.GetType());
    }
    bind_data->names = info->postgres_names;
    
    bind_data->types = return_types;
    bind_data->can_use_main_thread = true;
    bind_data->requires_materialization = false;

    PostgresScanFunction::PrepareBind(version, context, *bind_data, info->approx_num_pages);
    return std::move(bind_data);
}

// Init global state
static unique_ptr<GlobalTableFunctionState> PostgresInitGlobalState(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<PostgresBindData>();
    
    auto gstate = make_uniq<PostgresGlobalState>(bind_data.max_threads);
    
    // Store filter information
    gstate->column_ids = input.column_ids;
    gstate->filters = input.filters;
    gstate->column_names = bind_data.names;  // For debugging
    gstate->snapshot = NULL; 
    
    // Get connection from transaction or open new connection (same as original)
    auto pg_catalog = bind_data.GetCatalog();
    if (pg_catalog) {
        auto &transaction = Transaction::Get(context, *pg_catalog).Cast<PostgresTransaction>();
        auto &con = transaction.GetConnection();
        gstate->SetConnection(con.GetConnection());
        gstate->snapshot = transaction.GetSnapshot();
    } else {
        auto con = PostgresConnection::Open(bind_data.dsn);
        gstate->SetConnection(std::move(con));
    }
    
    // Validate lib-scan requirements
    ValidateLibScanRequirements(bind_data, gstate->GetConnection());
    
    // Get database and relation OIDs
    auto table = bind_data.GetTable();
    gstate->db_oid = GetDatabaseOid(gstate->GetConnection());
    gstate->rel_oid = table->relfilenode;
    gstate->spc_oid = GetTablespaceOid(gstate->GetConnection(), gstate->rel_oid);
    
    // Get BlockGroup information
    gstate->initial_bg_info = GetBlockGroupInfoForTable(gstate->GetConnection(),
                                                         bind_data.schema_name,
                                                         bind_data.table_name,
                                                         gstate->db_oid,
                                                         gstate->rel_oid,
                                                         gstate->block_group_size);

    // Initialize atomic counters
    gstate->current_total_blocks.store(gstate->initial_bg_info.total_blocks);
    gstate->current_total_block_groups.store(gstate->initial_bg_info.total_groups);

    // Get initial generation and flush state from Vista
    unsigned long gen_flush = vista_get_generation_flush_values();
    gstate->initial_generation = (uint32_t)(gen_flush >> 32);
    gstate->initial_was_flushing = (uint32_t)(gen_flush & 0xFFFFFFFF) != 0;

    // Initialize file buffers globally
    LibScan_InitFileBuffers(gstate->db_oid, gstate->rel_oid);
    
    return std::move(gstate);
}

// Init local state
static unique_ptr<LocalTableFunctionState> PostgresInitLocalState(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
    auto result = make_uniq<PostgresLocalState>();
    auto &bind_data = input.bind_data->Cast<PostgresBindData>();

    // Set basic fields
    result->column_ids = input.column_ids;
    result->filters = input.filters.get();

    // Create TupleDesc from catalog info
    result->tupdesc = CreateTupleDescFromBindData(bind_data);

    // BlockGroup allocation and initialization will be done in ScanChunk
    result->libscan_state = nullptr;
    result->cache = nullptr;
    result->done = false;

    return std::move(result);
}

// Main scan function
static void PostgresScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &lstate = data.local_state->Cast<PostgresLocalState>();
    auto &gstate = data.global_state->Cast<PostgresGlobalState>();
    auto &bind_data = data.bind_data->Cast<PostgresBindData>();
    
    // If we have filters, keep scanning until we get filtered results or no more data
    if (gstate.filters) {
        while (true) {
            lstate.ScanChunk(context, bind_data, gstate, output);
            
            if (output.size() == 0) {
                // No more data from scan
                break;
            }
            
            ApplyFiltersToChunk(output, gstate.column_ids, gstate.filters.get(), gstate.column_names);
            
            if (output.size() > 0) {
                // We have filtered results
                break;
            }
            
            // No results after filtering, continue with next chunk
            if (lstate.done) {
                // No more data available
                output.SetCardinality(0);
                break;
            }
        }
    } else {
        // No filters, just scan normally
        lstate.ScanChunk(context, bind_data, gstate, output);
    }
}

static OperatorPartitionData PostgresGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
    if (input.partition_info.RequiresPartitionColumns()) {
        throw InternalException("PostgresScan::GetPartitionData: partition columns not supported");
    }
    auto &local_state = input.local_state->Cast<PostgresLocalState>();
    return OperatorPartitionData(local_state.current_block_group);
}

// Table function definitions for lib-scan version
PostgresScanFunction::PostgresScanFunction()
    : TableFunction("postgres_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                    PostgresScan, PostgresBind, PostgresInitGlobalState, PostgresInitLocalState) {
    get_partition_data = PostgresGetPartitionData;  
    projection_pushdown = true;
    filter_pushdown = true;  
    global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

PostgresScanFunctionFilterPushdown::PostgresScanFunctionFilterPushdown()
    : TableFunction("postgres_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    PostgresScan, PostgresBind, PostgresInitGlobalState, PostgresInitLocalState) {
    get_partition_data = PostgresGetPartitionData;  
    projection_pushdown = true;
    filter_pushdown = true;  
    global_initialization = TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

} // namespace duckdb
