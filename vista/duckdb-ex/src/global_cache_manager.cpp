#include "global_cache_manager.hpp"
#include <string>

namespace duckdb {

std::unordered_map<std::string, BlockGroupCache*> GlobalCacheManager::cache_map;
std::mutex GlobalCacheManager::cache_mutex;

void GlobalCacheManager::Initialize() {
    // Empty initialization - caches created on demand
}

void GlobalCacheManager::Cleanup() {
    std::lock_guard<std::mutex> lock(cache_mutex);

    for (auto& pair : cache_map) {
        delete pair.second;
    }
    cache_map.clear();
}

BlockGroupCache* GlobalCacheManager::GetOrCreateBlockGroupCache(uint32_t db_oid, uint32_t rel_oid, uint32_t bg_id) {
    std::lock_guard<std::mutex> lock(cache_mutex);

    std::string key = MakeKey(db_oid, rel_oid, bg_id);

    auto it = cache_map.find(key);
    if (it == cache_map.end()) {
        std::string cache_path = "/path/to/your/vista-caches/duckdb_cache_" + key + ".bin";
        cache_map[key] = new BlockGroupCache(cache_path, db_oid, rel_oid, bg_id);
    }
    return cache_map[key];
}

std::string GlobalCacheManager::MakeKey(uint32_t db_oid, uint32_t rel_oid, uint32_t bg_id) {
    return std::to_string(db_oid) + "_" + std::to_string(rel_oid) + "_" + std::to_string(bg_id);
}

} // namespace duckdb
