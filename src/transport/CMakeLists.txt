add_library(raftkv_transport STATIC
  admin_service.cc)

target_include_directories(raftkv_transport PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_transport
  PUBLIC raftkv_proto raftkv_common
  PRIVATE raftkv_warnings)
