add_library(raftkv_engine STATIC
  builtin_engine.cc
  engine.cc)

target_include_directories(raftkv_engine PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_engine
  PUBLIC raftkv_lsm raftkv_common
  PRIVATE raftkv_warnings)

if(RAFTKV_WITH_LSMTREE)
  target_sources(raftkv_engine PRIVATE lsmtree_engine.cc)
  # PRIVATE, so lsmtree's include directory -- whose `lsm/` prefix is the same
  # one other projects use -- reaches only this library's own sources and never
  # leaks to anything that links raftkv_engine.
  target_link_libraries(raftkv_engine PRIVATE lsm)
  target_compile_definitions(raftkv_engine PRIVATE RAFTKV_HAVE_LSMTREE=1)
endif()
