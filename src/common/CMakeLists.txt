add_library(raftkv_common STATIC
  log.cc)

target_include_directories(raftkv_common PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_common PRIVATE raftkv_warnings)
