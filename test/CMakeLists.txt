# raftkv_test(<name> SOURCES ...)
function(raftkv_test name)
  cmake_parse_arguments(ARG "" "" "SOURCES;LIBS" ${ARGN})
  add_executable(${name} ${ARG_SOURCES})
  target_link_libraries(${name} PRIVATE ${ARG_LIBS} GTest::gtest_main raftkv_warnings)
  gtest_discover_tests(${name}
    PROPERTIES TIMEOUT 120
    DISCOVERY_MODE PRE_TEST)
endfunction()

raftkv_test(log_test
  SOURCES log_test.cc
  LIBS raftkv_common)

raftkv_test(admin_grpc_test
  SOURCES admin_grpc_test.cc
  LIBS raftkv_transport)
