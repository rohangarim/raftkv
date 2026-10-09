# raftkv_test(<name> SOURCES ...)
function(raftkv_test name)
  cmake_parse_arguments(ARG "" "TIMEOUT" "SOURCES;LIBS" ${ARGN})
  if(NOT ARG_TIMEOUT)
    set(ARG_TIMEOUT 120)
  endif()
  add_executable(${name} ${ARG_SOURCES})
  target_link_libraries(${name} PRIVATE ${ARG_LIBS} GTest::gtest_main raftkv_warnings)
  gtest_discover_tests(${name}
    PROPERTIES TIMEOUT ${ARG_TIMEOUT}
    DISCOVERY_MODE PRE_TEST
    # Default discovery timeout is 5s. Sanitized binaries start slower and a
    # loaded machine makes that worse; a discovery timeout aborts the whole
    # ctest run and looks nothing like the flake it is.
    DISCOVERY_TIMEOUT 120)
endfunction()

raftkv_test(log_test
  SOURCES log_test.cc
  LIBS raftkv_common)

raftkv_test(admin_grpc_test
  SOURCES admin_grpc_test.cc
  LIBS raftkv_transport)

raftkv_test(lsm_coding_test
  SOURCES lsm_coding_test.cc
  LIBS raftkv_lsm)

raftkv_test(lsm_wal_test
  SOURCES lsm_wal_test.cc
  LIBS raftkv_lsm)

raftkv_test(lsm_memtable_test
  SOURCES lsm_memtable_test.cc
  LIBS raftkv_lsm)

raftkv_test(lsm_sstable_test
  SOURCES lsm_sstable_test.cc
  LIBS raftkv_lsm)

raftkv_test(lsm_db_test
  SOURCES lsm_db_test.cc
  LIBS raftkv_lsm)

raftkv_test(state_machine_test
  SOURCES state_machine_test.cc
  LIBS raftkv_statemachine)

raftkv_test(raft_log_test
  SOURCES raft_log_test.cc
  LIBS raftkv_raft)

# The 2000-seed soak takes ~13 s uninstrumented but ~132 s under TSan, which
# overran ctest's 120 s default and reported as a failure rather than as the
# slow-but-passing run it is. Sized for the sanitizer, not the fast path.
raftkv_test(raft_cluster_test
  SOURCES raft_cluster_test.cc raft_harness.cc
  LIBS raftkv_raft
  TIMEOUT 600)
target_include_directories(raft_cluster_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

raftkv_test(raft_storage_test
  SOURCES raft_storage_test.cc
  LIBS raftkv_storage)

raftkv_test(raft_grpc_cluster_test
  SOURCES raft_grpc_cluster_test.cc
  LIBS raftkv_server raftkv_transport raftkv_statemachine
  TIMEOUT 120)

raftkv_test(raft_read_index_test
  SOURCES raft_read_index_test.cc raft_harness.cc
  LIBS raftkv_raft
  TIMEOUT 600)
target_include_directories(raft_read_index_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
