add_library(raftkv_statemachine STATIC
  kv_state_machine.cc)

target_include_directories(raftkv_statemachine PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_statemachine
  PUBLIC raftkv_lsm raftkv_proto
  PRIVATE raftkv_warnings)
