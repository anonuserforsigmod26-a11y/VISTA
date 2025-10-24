include_directories(include)

add_subdirectory(storage)

add_library(
  postgres_ext_library OBJECT
  blockgroup_cache.cpp
  global_cache_manager.cpp
  postgres_binary_copy.cpp
  postgres_connection.cpp
  postgres_copy_from.cpp
  postgres_copy_to.cpp
  postgres_extension.cpp
  postgres_filter_pushdown.cpp
  postgres_scanner_libscan.cpp
  postgres_storage.cpp
  postgres_utils.cpp
  vista_cache_loader.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:postgres_ext_library>
    PARENT_SCOPE)
