cmake_minimum_required(VERSION 3.24)

project(raftkv
  VERSION 0.1.0
  DESCRIPTION "Sharded, fault-tolerant, linearizable key-value store"
  LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

option(RAFTKV_BUILD_TESTS "Build tests" ON)
option(RAFTKV_USE_SYSTEM_GRPC "Use find_package(gRPC) instead of FetchContent" OFF)
option(RAFTKV_WERROR "Treat warnings as errors in raftkv targets" ON)
set(RAFTKV_SANITIZER_SUMMARY "none" CACHE STRING "Human-readable sanitizer config, set by presets")

include(Dependencies)
include(ProtoLibrary)

# Warning flags are attached to our targets only, never to fetched dependencies.
add_library(raftkv_warnings INTERFACE)
target_compile_options(raftkv_warnings INTERFACE
  -Wall -Wextra -Wpedantic
  -Wshadow -Wnon-virtual-dtor -Wold-style-cast -Wcast-align
  -Woverloaded-virtual -Wnull-dereference -Wdouble-promotion
  -Wimplicit-fallthrough)
if(RAFTKV_WERROR)
  target_compile_options(raftkv_warnings INTERFACE -Werror)
endif()

add_subdirectory(proto)
add_subdirectory(src)

if(RAFTKV_BUILD_TESTS)
  enable_testing()
  add_subdirectory(test)
endif()

message(STATUS "raftkv: build type      = ${CMAKE_BUILD_TYPE}")
message(STATUS "raftkv: sanitizers      = ${RAFTKV_SANITIZER_SUMMARY}")
message(STATUS "raftkv: system gRPC     = ${RAFTKV_USE_SYSTEM_GRPC}")
