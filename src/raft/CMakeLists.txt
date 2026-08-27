add_library(raftkv_raft STATIC
  raft_log.cc
  raft_node.cc)

target_include_directories(raftkv_raft PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_raft
  PUBLIC raftkv_proto raftkv_lsm
  PRIVATE raftkv_warnings)
