#pragma once

#include "duckdb.hpp"
#include <vector>
#include <string>
#include <unordered_map>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>

class VistaCacheLoader;  // Forward declaration

namespace duckdb {

class BlockGroupCache {
private:
    std::string cache_file_path;
    uint64_t file_size = 0;
    mutable std::mutex cache_mutex;

    // BlockGroup identification for bitmap
    uint32_t db_oid;
    uint32_t rel_oid;
    uint32_t block_group_id;

    // Chunk metadata stored in memory
    struct ChunkMetadata {
        uint64_t file_offset;           // Start offset in file
        uint32_t col_count;             // Number of columns
        uint64_t cardinality;           // Number of rows
        std::vector<uint32_t> vector_sizes;  // Size of each vector data
        // No type_sizes needed - types come from gstate
    };

    // Key: (sg_id, chunk_idx) → Value: ChunkMetadata
    std::unordered_map<uint32_t, ChunkMetadata> chunk_metadata;

    // Key: sg_id → Value: chunk_count (within this BlockGroup)
    std::unordered_map<uint32_t, uint32_t> subgroup_chunk_counts;

    // Helper function to create key with bit packing
    uint32_t makeKey(uint32_t sg_id, uint32_t chunk_idx);
    std::vector<uint8_t> serializeChunkVectors(const DataChunk& chunk);
    void deserializeChunkVectors(VistaCacheLoader* vista_loader, DataChunk& result, const ChunkMetadata& metadata, const std::vector<column_t>& column_ids, const std::vector<LogicalType>& table_types);

    // Check if specific chunk is valid (cached + not invalidated)
    bool isChunkValid(uint32_t sg_id, uint32_t chunk_idx);

    // Check if SubGroup is valid (based on first chunk)
    bool isSubGroupValid(uint32_t sg_id);

    // Append single chunk to buffer and return metadata (internal function)
    std::pair<uint64_t, std::vector<uint32_t>> appendChunk(uint8_t* dest_buffer, size_t current_offset, const DataChunk& chunk);

public:
    BlockGroupCache(const std::string& file_path, uint32_t db_oid, uint32_t rel_oid, uint32_t bg_id);
    ~BlockGroupCache();

    // Get bitmask of invalid SubGroups (1 = invalid, needs reload)
    // pre_work_versions: array to store version for each SubGroup (for later ClearBitPostWork)
    uint32_t getInvalidSubGroupMask(uint32_t total_subgroups, uint64_t* pre_work_versions = nullptr);

    // Load entire SubGroup from cache using Vista-based I/O
    std::vector<DataChunk*> loadSubGroup(uint32_t sg_id, VistaCacheLoader* vista_loader, const std::vector<column_t>& column_ids, const std::vector<LogicalType>& table_types);

    // Validate bitmap and clear bit if needed - returns true if safe to cache
    // pre_work_version: version from TestBitPreWork (UINT64_MAX if bitmap was clean)
    bool ValidateBitmapForCache(uint32_t sg_id, uint64_t pre_work_version);

    // Store entire SubGroup to cache file
    void storeSubGroup(uint32_t sg_id, const std::vector<DataChunk*>& chunks);
};

} // namespace duckdb