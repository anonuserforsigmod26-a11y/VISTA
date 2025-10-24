#include "blockgroup_cache.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "vista_cache_loader.hpp"
#include "cache_constants.hpp"
#include <cassert>
#include <numeric>

extern "C" {
    bool LibScan_TestBitPreWork(uint32_t table_id, size_t bit_idx, uint64_t* pre_work_version);
    bool LibScan_ClearBitPostWork(uint32_t table_id, size_t bit_idx, uint64_t pre_work_version);
}

namespace duckdb {

BlockGroupCache::BlockGroupCache(const std::string& file_path, uint32_t db_oid, uint32_t rel_oid, uint32_t bg_id)
    : cache_file_path(file_path), db_oid(db_oid), rel_oid(rel_oid), block_group_id(bg_id) {

    // Always create fresh cache file (PostgreSQL state may have changed)
    int fd = open(file_path.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
    if (fd < 0) {
        throw IOException("Failed to create cache file: %s", file_path.c_str());
    }

    // Write 512-byte dummy block for O_DIRECT alignment
    void* dummy_buffer = nullptr;
    if (posix_memalign(&dummy_buffer, 512, 512) != 0) {
        close(fd);
        throw IOException("Failed to allocate aligned buffer");
    }
    memset(dummy_buffer, 0, 512);

    ssize_t written = pwrite(fd, dummy_buffer, 512, 0);
    free(dummy_buffer);

    if (written != 512) {
        close(fd);
        throw IOException("Failed to write to cache file: %s", file_path.c_str());
    }

    file_size = 512;
    close(fd);

    // Clear any existing metadata
    chunk_metadata.clear();
    subgroup_chunk_counts.clear();
}

BlockGroupCache::~BlockGroupCache() {
    // No need to close file - using local file descriptors now
}

uint32_t BlockGroupCache::makeKey(uint32_t sg_id, uint32_t chunk_idx) {
    // Assert bit limits: sg_id < 2^16, chunk_idx < 2^16
    assert(sg_id < 65536);
    assert(chunk_idx < 65536);

    // Pack: sg_id(16bit) | chunk_idx(16bit)
    return ((uint32_t)sg_id << 16) | chunk_idx;
}

bool BlockGroupCache::isChunkValid(uint32_t sg_id, uint32_t chunk_idx) {
    uint32_t key = makeKey(sg_id, chunk_idx);
    auto it = chunk_metadata.find(key);

    // Check if metadata exists and has valid offset (>0)
    return it != chunk_metadata.end() && it->second.file_offset > 0;
}

bool BlockGroupCache::isSubGroupValid(uint32_t sg_id) {
    // SubGroup is valid if first chunk is valid
    return isChunkValid(sg_id, 0);
}

uint32_t BlockGroupCache::getInvalidSubGroupMask(uint32_t total_subgroups, uint64_t* pre_work_versions) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    uint32_t mask = 0;

    for (uint32_t sg = 0; sg < total_subgroups; sg++) {
        // 1. Check cache validity
        bool cache_invalid = !isSubGroupValid(sg);

        // 2. Check bitmap dirty
        uint32_t global_sg_idx = block_group_id * MAX_SUBGROUPS_PER_BG + sg;
        uint64_t version;
        bool bitmap_dirty = LibScan_TestBitPreWork(rel_oid, global_sg_idx, &version);

        if (cache_invalid || bitmap_dirty) {
            mask |= (1U << sg);

            // Store version for this SubGroup
            if (pre_work_versions) {
                pre_work_versions[sg] = version;
            }
        }
    }
    return mask;
}

std::vector<uint8_t> BlockGroupCache::serializeChunkVectors(const DataChunk& chunk) {
    std::vector<uint8_t> result;

    // Only serialize pure vector data (no types, no metadata)
    for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
        // Reference the vector to avoid potentially mutating it during serialization
        Vector serialized_vector(chunk.data[col].GetType());
        serialized_vector.Reference(chunk.data[col]);

        // Serialize Vector data using DuckDB's BinarySerializer
        MemoryStream stream;
        BinarySerializer serializer(stream);

        // Create object context for Vector::Serialize
        serializer.Begin();
        serializer.WriteObject(100, "v", [&](Serializer &object) {
            serialized_vector.Serialize(object, chunk.size());
        });
        serializer.End();

        // Get serialized data
        auto data_ptr = stream.GetData();
        auto data_size = stream.GetPosition();

        // Append vector data directly
        result.insert(result.end(), data_ptr, data_ptr + data_size);
    }

    return result;
}

void BlockGroupCache::deserializeChunkVectors(VistaCacheLoader* vista_loader, DataChunk& result, const ChunkMetadata& metadata, const std::vector<column_t>& column_ids, const std::vector<LogicalType>& table_types) {
    // Initialize result chunk with requested column types
    vector<LogicalType> requested_types;
    for (auto col_id : column_ids) {
        requested_types.push_back(table_types[col_id]);
    }
    result.Initialize(Allocator::DefaultAllocator(), requested_types);

    uint64_t current_offset = metadata.file_offset;

    // Process each column in the file, but only deserialize requested ones
    for (idx_t file_col = 0; file_col < metadata.col_count; file_col++) {
        assert(file_col < metadata.vector_sizes.size());
        uint32_t vector_size = metadata.vector_sizes[file_col];

        // Check if this column is requested
        auto it = std::find(column_ids.begin(), column_ids.end(), file_col);
        if (it != column_ids.end()) {
            // This column is requested - read and deserialize
            size_t output_col = std::distance(column_ids.begin(), it);

            // Read vector data using Vista
            std::vector<uint8_t> vector_data;
            vista_loader->readVectorData(vector_data, current_offset, vector_size);

            // Deserialize directly into the result vector
            MemoryStream stream(vector_data.data(), vector_data.size());
            BinaryDeserializer deserializer(stream);

            // Read object context for Vector::Deserialize
            deserializer.Begin();
            deserializer.ReadObject(100, "v", [&](Deserializer &object) {
                result.data[output_col].Deserialize(object, metadata.cardinality);
            });
            deserializer.End();
        }
        // Skip to next column (whether we read it or not)
        current_offset += vector_size;
    }

    result.SetCardinality(metadata.cardinality);
}

std::pair<uint64_t, std::vector<uint32_t>> BlockGroupCache::appendChunk(uint8_t* dest_buffer, size_t current_offset, const DataChunk& chunk) {
    std::vector<uint32_t> vector_sizes;
    size_t write_offset = current_offset;

    // Calculate vector sizes while serializing
    for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
        // Reference the vector to avoid potentially mutating it during serialization
        Vector serialized_vector(chunk.data[col].GetType());
        serialized_vector.Reference(chunk.data[col]);

        // Serialize Vector data using DuckDB's BinarySerializer
        MemoryStream stream;
        BinarySerializer serializer(stream);

        // Create object context for Vector::Serialize
        serializer.Begin();
        serializer.WriteObject(100, "v", [&](Serializer &object) {
            serialized_vector.Serialize(object, chunk.size());
        });
        serializer.End();

        // Get serialized data
        auto data_ptr = stream.GetData();
        auto data_size = stream.GetPosition();

        // Store size for metadata
        vector_sizes.push_back(data_size);

        // Copy directly to destination buffer
        memcpy(dest_buffer + write_offset, data_ptr, data_size);
        write_offset += data_size;
    }

    return {current_offset, vector_sizes};
}

std::vector<DataChunk*> BlockGroupCache::loadSubGroup(uint32_t sg_id, VistaCacheLoader* vista_loader, const std::vector<column_t>& column_ids, const std::vector<LogicalType>& table_types) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    std::vector<DataChunk*> result;

    // Get chunk count for this SubGroup
    auto it = subgroup_chunk_counts.find(sg_id);
    if (it == subgroup_chunk_counts.end()) {
        return result; // SubGroup not found
    }

    uint32_t chunk_count = it->second;

    // Open file for reading with O_DIRECT
    int fd = open(cache_file_path.c_str(), O_RDONLY | O_DIRECT);
    if (fd < 0) {
        throw IOException("Failed to open cache file for reading: %s", cache_file_path.c_str());
    }

    // Step 1: Collect metadata and VectorInfo for all chunks in this SubGroup
    std::vector<const ChunkMetadata*> chunk_metas;
    std::vector<VistaCacheLoader::VectorInfo> all_vectors;
    for (uint32_t chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
        uint32_t key = makeKey(sg_id, chunk_idx);
        auto meta_it = chunk_metadata.find(key);
        assert(meta_it != chunk_metadata.end());
        const ChunkMetadata& meta = meta_it->second;
        chunk_metas.push_back(&meta);

        // Skip nullptr markers (cardinality = 0)
        if (meta.cardinality == 0) {
            continue;
        }

        uint64_t current_offset = meta.file_offset;
        for (idx_t file_col = 0; file_col < meta.col_count; file_col++) {
            uint32_t vector_size = meta.vector_sizes[file_col];

            // Check if this column is requested
            auto col_it = std::find(column_ids.begin(), column_ids.end(), file_col);
            if (col_it != column_ids.end()) {
                all_vectors.push_back({current_offset, vector_size});
            }
            current_offset += vector_size;
        }
    }

    // Step 2: Initialize Vista with all vectors (only if there are real chunks to load)
    if (!all_vectors.empty()) {
        if (!vista_loader->initializeVista(fd, all_vectors)) {
            close(fd);
            throw IOException("Failed to initialize Vista for cache loading");
        }
    }

    // Step 3: Load all chunks using Vista
    for (uint32_t chunk_idx = 0; chunk_idx < chunk_count; chunk_idx++) {
        const ChunkMetadata* meta = chunk_metas[chunk_idx];

        // Check for nullptr marker (cardinality = 0)
        if (meta->cardinality == 0) {
            result.push_back(nullptr);
        } else {
            DataChunk* chunk = new DataChunk();
            deserializeChunkVectors(vista_loader, *chunk, *meta, column_ids, table_types);
            result.push_back(chunk);
        }
    }

    // Step 4: Cleanup Vista and close file (only if Vista was initialized)
    if (!all_vectors.empty()) {
        vista_loader->cleanup();
    }
    close(fd);

    return result;
}


bool BlockGroupCache::ValidateBitmapForCache(uint32_t sg_id, uint64_t pre_work_version) {
    // This function is only called for directly converted SubGroups
    // pre_work_version must be a valid version from TestBitPreWork
    assert(pre_work_version != UINT64_MAX);

    uint32_t global_sg_idx = block_group_id * MAX_SUBGROUPS_PER_BG + sg_id;

    // Try to clear bit with version check
    return LibScan_ClearBitPostWork(rel_oid, global_sg_idx, pre_work_version);
}

void BlockGroupCache::storeSubGroup(uint32_t sg_id, const std::vector<DataChunk*>& chunks) {
    std::lock_guard<std::mutex> lock(cache_mutex);

    // Step 1: Allocate buffer based on SubGroup size (e.g., 256 blocks * 8KB = 2MB)
    size_t buffer_size = (SUBGROUP_DATA_SIZE + 511) & ~511UL;  // Align to 512

    // Step 2: Allocate aligned buffer
    uint8_t* aligned_buffer = nullptr;
    if (posix_memalign((void**)&aligned_buffer, 512, buffer_size) != 0) {
        throw IOException("Failed to allocate aligned buffer for O_DIRECT");
    }

    // Step 3: Serialize all chunks directly into aligned buffer
    uint64_t base_offset = file_size;
    size_t current_offset = 0;

    for (size_t i = 0; i < chunks.size(); i++) {
        if (chunks[i] == nullptr) {
            // Store metadata for nullptr marker (empty chunk)
            ChunkMetadata metadata;
            metadata.file_offset = base_offset + current_offset;
            metadata.col_count = 0;
            metadata.cardinality = 0;
            metadata.vector_sizes.clear();

            uint32_t key = makeKey(sg_id, i);
            chunk_metadata[key] = metadata;
            // current_offset stays the same (no data written)
        } else {
            // Serialize actual chunk
            auto append_result = appendChunk(aligned_buffer, current_offset, *chunks[i]);
            auto relative_offset = append_result.first;
            auto vector_sizes = append_result.second;

            ChunkMetadata metadata;
            metadata.file_offset = base_offset + relative_offset;
            metadata.col_count = chunks[i]->ColumnCount();
            metadata.cardinality = chunks[i]->size();
            metadata.vector_sizes = vector_sizes;

            uint32_t key = makeKey(sg_id, i);
            chunk_metadata[key] = metadata;

            // Update current_offset for next chunk
            current_offset += vector_sizes.size() > 0 ?
                std::accumulate(vector_sizes.begin(), vector_sizes.end(), 0UL) : 0;
        }
    }

    // Step 4: Align final size and pad with zeros if needed
    size_t raw_size = current_offset;
    size_t aligned_size = (raw_size + 511) & ~511UL;
    if (aligned_size > raw_size) {
        memset(aligned_buffer + raw_size, 0, aligned_size - raw_size);
    }

    // Step 5: Write to file with O_DIRECT
    int fd = open(cache_file_path.c_str(), O_RDWR | O_DIRECT);
    if (fd < 0) {
        free(aligned_buffer);
        throw IOException("Failed to open cache file for writing: %s", cache_file_path.c_str());
    }

    ssize_t bytes_written = pwrite(fd, aligned_buffer, aligned_size, file_size);
    free(aligned_buffer);
    close(fd);

    if (bytes_written != static_cast<ssize_t>(aligned_size)) {
        throw IOException("Failed to write SubGroup to cache file");
    }

    // Step 6: Update file_size with aligned size
    file_size += aligned_size;

    // Store chunk count for this SubGroup
    subgroup_chunk_counts[sg_id] = chunks.size();
}


} // namespace duckdb
