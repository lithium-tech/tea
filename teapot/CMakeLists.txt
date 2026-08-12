get_filename_component(teapot_proto teapot.proto ABSOLUTE)
get_filename_component(root_path "${CMAKE_SOURCE_DIR}" ABSOLUTE)

# Generated sources
set(teapot_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/teapot.pb.cc")
set(teapot_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/teapot.pb.h")
set(teapot_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/teapot.grpc.pb.cc")
set(teapot_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/teapot.grpc.pb.h")
if(NOT Protobuf_INCLUDE_DIRS)
  set(Protobuf_INCLUDE_DIRS ${root_path})
endif()
add_custom_command(
  OUTPUT "${teapot_proto_srcs}" "${teapot_proto_hdrs}" "${teapot_grpc_srcs}"
         "${teapot_grpc_hdrs}"
  COMMAND
    ${_PROTOBUF_PROTOC} ARGS --grpc_out "${CMAKE_BINARY_DIR}" --cpp_out
    "${CMAKE_BINARY_DIR}" -I "${Protobuf_INCLUDE_DIRS}" -I "${root_path}"
    --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}" "${teapot_proto}"
  DEPENDS "${teapot_proto}")

# teapot_grpc_proto
add_library(teapot_grpc_proto ${teapot_grpc_srcs} ${teapot_grpc_hdrs}
                              ${teapot_proto_srcs} ${teapot_proto_hdrs})
target_link_libraries(teapot_grpc_proto ${_REFLECTION} ${_GRPC_GRPCPP}
                      ${_PROTOBUF_LIBPROTOBUF})
target_include_directories(teapot_grpc_proto PUBLIC ${CMAKE_BINARY_DIR})

set_target_properties(teapot_grpc_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)

get_target_property(ABSL_LOG_FLAGS_TYPE absl::log_flags TYPE)
if(ABSL_LOG_FLAGS_TYPE STREQUAL "STATIC_LIBRARY")
  set(ABSL_WHOLE_ARCHIVE "$<LINK_LIBRARY:WHOLE_ARCHIVE,absl::log_flags>")
endif()
