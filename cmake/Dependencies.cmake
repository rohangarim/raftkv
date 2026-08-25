include(FetchContent)

# Shared source checkout, per-preset object files.
#
# Every preset has its own binaryDir, so the default FetchContent layout would
# re-clone gRPC and all of its submodules (>1 GB) once per sanitizer preset.
# We pin SOURCE_DIR and SUBBUILD_DIR outside the build tree so the download
# happens exactly once, while BINARY_DIR stays inside the preset's build tree
# because the sanitizer presets compile the dependencies with different flags
# and must not share object files.
set(RAFTKV_DEPS_SRC "${CMAKE_SOURCE_DIR}/.deps" CACHE PATH
    "Shared source checkout for fetched dependencies")

# Acquire the sources with our own script rather than FetchContent's git step.
#
# FetchContent's download is all-or-nothing: one dropped connection among
# gRPC's submodules and it deletes the whole tree and restarts a multi-gigabyte
# clone. scripts/fetch_deps.sh is shallow, per-submodule, retrying, and
# resumable. FetchContent still owns the interesting part -- adding the sources
# to our build and wiring up targets -- via FETCHCONTENT_SOURCE_DIR_*, which is
# the documented way to point it at a tree you already have.
option(RAFTKV_FETCH_DEPS "Run scripts/fetch_deps.sh at configure time" ON)
if(RAFTKV_FETCH_DEPS AND NOT RAFTKV_USE_SYSTEM_GRPC)
  message(STATUS "raftkv: fetching dependency sources into ${RAFTKV_DEPS_SRC}")
  execute_process(
    COMMAND "${CMAKE_SOURCE_DIR}/scripts/fetch_deps.sh"
    WORKING_DIRECTORY "${CMAKE_SOURCE_DIR}"
    RESULT_VARIABLE fetch_result)
  if(NOT fetch_result EQUAL 0)
    message(FATAL_ERROR
      "raftkv: dependency fetch failed. Re-run `scripts/fetch_deps.sh` "
      "directly; it resumes from what it already downloaded.")
  endif()
endif()

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

  set(FETCHCONTENT_SOURCE_DIR_GRPC "${RAFTKV_DEPS_SRC}/grpc-src" CACHE PATH "" FORCE)
  FetchContent_Declare(grpc
    GIT_REPOSITORY https://github.com/grpc/grpc.git
    GIT_TAG        v1.75.1
    GIT_SHALLOW    TRUE
    # Only the submodules a C++ build actually consumes. gRPC's default
    # `--recursive` pulls ~1.3 GB including bloaty, benchmark, googletest and
    # opentelemetry-cpp, none of which we compile -- and every extra clone is
    # another chance for the network to drop the build on the floor.
    GIT_SUBMODULES "third_party/abseil-cpp;third_party/boringssl-with-bazel;third_party/cares/cares;third_party/envoy-api;third_party/googleapis;third_party/opencensus-proto;third_party/protobuf;third_party/protoc-gen-validate;third_party/re2;third_party/xds;third_party/zlib"
    SOURCE_DIR     "${RAFTKV_DEPS_SRC}/grpc-src"
    SUBBUILD_DIR   "${RAFTKV_DEPS_SRC}/grpc-subbuild"
    BINARY_DIR     "${CMAKE_BINARY_DIR}/_deps/grpc-build"
    SYSTEM)
  FetchContent_MakeAvailable(grpc)

  set(RAFTKV_PROTOC_BIN $<TARGET_FILE:protoc>)
  set(RAFTKV_GRPC_PLUGIN_BIN $<TARGET_FILE:grpc_cpp_plugin>)
endif()

# ---------------------------------------------------------------------------
# GoogleTest
# ---------------------------------------------------------------------------
if(RAFTKV_BUILD_TESTS)
  set(FETCHCONTENT_SOURCE_DIR_GOOGLETEST "${RAFTKV_DEPS_SRC}/googletest-src" CACHE PATH "" FORCE)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.17.0
    GIT_SHALLOW    TRUE
    SOURCE_DIR     "${RAFTKV_DEPS_SRC}/googletest-src"
    SUBBUILD_DIR   "${RAFTKV_DEPS_SRC}/googletest-subbuild"
    BINARY_DIR     "${CMAKE_BINARY_DIR}/_deps/googletest-build"
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
