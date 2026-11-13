add_library(raftkv_shard STATIC
  shard_map.cc)

target_include_directories(raftkv_shard PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_shard
  PUBLIC raftkv_lsm
  PRIVATE raftkv_warnings)
