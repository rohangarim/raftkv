add_library(raftkv_client STATIC
  kv_client.cc)

target_include_directories(raftkv_client PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_client
  PUBLIC raftkv_proto raftkv_common raftkv_lsm
  PRIVATE raftkv_warnings)

add_executable(raftkv_cli cli_main.cc)
target_link_libraries(raftkv_cli PRIVATE raftkv_client raftkv_warnings)
