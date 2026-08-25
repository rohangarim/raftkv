include(FetchContent)

set(FETCHCONTENT_QUIET OFF)

# ---------------------------------------------------------------------------
# gRPC + Protobuf + Abseil
#
# Pinned by tag, not by branch, so a clean clone two years from now builds the
# same bytes. gRPC vendors its own protobuf and abseil; we deliberately do NOT
# mix a system protobuf with a fetched gRPC because the generated-code ABI is
# not stable across protobuf majors.
# ---------------------------------------------------------------------------
if(RAFTKV_USE_SYSTEM_GRPC)
  find_package(Protobuf CONFIG REQUIRED)
  find_package(gRPC CONFIG REQUIRED)
  set(RAFTKV_PROTOC_BIN $<TARGET_FILE:protobuf::protoc>)
  set(RAFTKV_GRPC_PLUGIN_BIN $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
else()
  set(gRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_CSHARP_EXT OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF CACHE BOOL "" FORCE)
  set(gRPC_INSTALL OFF CACHE BOOL "" FORCE)
  set(protobuf_BUILD_TESTS OFF CACHE BOOL "" FORCE)
  set(protobuf_INSTALL OFF CACHE BOOL "" FORCE)
  set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "" FORCE)
  set(ABSL_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
  set(RE2_BUILD_TESTING OFF CACHE BOOL "" FORCE)

  FetchContent_Declare(grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG        v1.75.1
    GIT_SHALLOW    TRUE
    GIT_PROGRESS   TRUE
    SYSTEM)
  FetchContent_MakeAvailable(grpc)

  set(RAFTKV_PROTOC_BIN $<TARGET_FILE:protoc>)
  set(RAFTKV_GRPC_PLUGIN_BIN $<TARGET_FILE:grpc_cpp_plugin>)
endif()

# ---------------------------------------------------------------------------
# GoogleTest
# ---------------------------------------------------------------------------
if(RAFTKV_BUILD_TESTS)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.17.0
    GIT_SHALLOW    TRUE
    GIT_PROGRESS   TRUE
    SYSTEM)
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
  include(GoogleTest)
endif()

# ---------------------------------------------------------------------------
# Normalize target names across the two acquisition paths.
# ---------------------------------------------------------------------------
if(TARGET gRPC::grpc++)
  set(RAFTKV_GRPCPP gRPC::grpc++)
elseif(TARGET grpc++)
  set(RAFTKV_GRPCPP grpc++)
else()
  message(FATAL_ERROR "raftkv: no grpc++ target found")
endif()

if(TARGET protobuf::libprotobuf)
  set(RAFTKV_PROTOBUF protobuf::libprotobuf)
elseif(TARGET libprotobuf)
  set(RAFTKV_PROTOBUF libprotobuf)
else()
  message(FATAL_ERROR "raftkv: no protobuf target found")
endif()
