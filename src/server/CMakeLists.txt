add_library(raftkv_server STATIC
  raft_driver.cc)

target_include_directories(raftkv_server PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_server
  PUBLIC raftkv_raft raftkv_storage raftkv_statemachine raftkv_proto
  PRIVATE raftkv_common raftkv_warnings)

add_executable(raftkv_node node_main.cc)
target_link_libraries(raftkv_node
  PRIVATE raftkv_server raftkv_transport raftkv_statemachine raftkv_common raftkv_warnings)
