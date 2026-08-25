# raftkv_proto_library(<name> PROTOS a.proto b.proto)
#
# Generates C++ message code and gRPC service stubs for the given .proto files
# and wraps them in a single static library target. The .proto files are the
# single source of truth for the wire format; nothing hand-writes serialization.
function(raftkv_proto_library name)
  cmake_parse_arguments(ARG "" "" "PROTOS" ${ARGN})
  if(NOT ARG_PROTOS)
    message(FATAL_ERROR "raftkv_proto_library(${name}): no PROTOS given")
  endif()

  set(gen_dir "${CMAKE_CURRENT_BINARY_DIR}/gen")
  file(MAKE_DIRECTORY "${gen_dir}")

  set(generated "")
  foreach(proto IN LISTS ARG_PROTOS)
    get_filename_component(abs "${proto}" ABSOLUTE)
    get_filename_component(stem "${proto}" NAME_WE)
    get_filename_component(proto_dir "${abs}" DIRECTORY)

    set(pb_cc "${gen_dir}/${stem}.pb.cc")
    set(pb_h  "${gen_dir}/${stem}.pb.h")
    set(gr_cc "${gen_dir}/${stem}.grpc.pb.cc")
    set(gr_h  "${gen_dir}/${stem}.grpc.pb.h")

    add_custom_command(
      OUTPUT "${pb_cc}" "${pb_h}" "${gr_cc}" "${gr_h}"
      COMMAND ${RAFTKV_PROTOC_BIN}
        --proto_path "${proto_dir}"
        --cpp_out "${gen_dir}"
        --grpc_out "${gen_dir}"
        --plugin=protoc-gen-grpc=${RAFTKV_GRPC_PLUGIN_BIN}
        "${abs}"
      DEPENDS "${abs}" ${RAFTKV_PROTOC_BIN} ${RAFTKV_GRPC_PLUGIN_BIN}
      COMMENT "protoc ${stem}.proto"
      VERBATIM)

    list(APPEND generated "${pb_cc}" "${gr_cc}")
  endforeach()

  add_library(${name} STATIC ${generated})
  target_include_directories(${name} SYSTEM PUBLIC "${gen_dir}")
  target_link_libraries(${name} PUBLIC ${RAFTKV_GRPCPP} ${RAFTKV_PROTOBUF})
  # Generated code is not ours; do not hold it to our warning bar.
  target_compile_options(${name} PRIVATE -w)
  set_target_properties(${name} PROPERTIES CXX_CLANG_TIDY "")
endfunction()
