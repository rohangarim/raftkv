add_library(raftkv_storage STATIC
  raft_storage.cc)

target_include_directories(raftkv_storage PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_storage
  PUBLIC raftkv_proto raftkv_lsm
  PRIVATE raftkv_warnings)
