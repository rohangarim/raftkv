add_library(raftkv_lsm STATIC
  crc32c.cc
  file.cc
  memtable.cc
  sstable.cc
  status.cc
  wal.cc
  write_batch.cc)

target_include_directories(raftkv_lsm PUBLIC ${CMAKE_SOURCE_DIR}/src)
target_link_libraries(raftkv_lsm
  PUBLIC raftkv_common
  PRIVATE raftkv_warnings)
