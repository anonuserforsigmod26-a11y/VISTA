#pragma once

#include "blockgroup_cache.hpp"
#include <unordered_map>
#include <string>
#include <mutex>

namespace duckdb {

class GlobalCacheManager {
private:
    static std::unordered_map<std::string, BlockGroupCache*> cache_map;
    static std::mutex cache_mutex;

public:
    static void Initialize();
    static void Cleanup();

    static BlockGroupCache* GetOrCreateBlockGroupCache(uint32_t db_oid, uint32_t rel_oid, uint32_t bg_id);

private:
    static std::string MakeKey(uint32_t db_oid, uint32_t rel_oid, uint32_t bg_id);
};

} // namespace duckdb