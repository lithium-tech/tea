get_filename_component(stats_state_proto stats_state.proto ABSOLUTE)
get_filename_component(root_path "${CMAKE_SOURCE_DIR}" ABSOLUTE)

# Generated sources
set(stats_state_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/stats_state.pb.cc")
set(stats_state_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/stats_state.pb.h")
set(stats_state_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/stats_state.grpc.pb.cc")
set(stats_state_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/stats_state.grpc.pb.h")
if(NOT Protobuf_INCLUDE_DIRS)
  set(Protobuf_INCLUDE_DIRS ${root_path})
endif()
add_custom_command(
  OUTPUT "${stats_state_proto_srcs}" "${stats_state_proto_hdrs}" "${stats_state_grpc_srcs}"
         "${stats_state_grpc_hdrs}"
  COMMAND
    ${_PROTOBUF_PROTOC} ARGS --grpc_out "${CMAKE_BINARY_DIR}" --cpp_out
    "${CMAKE_BINARY_DIR}" -I "${Protobuf_INCLUDE_DIRS}" -I "${root_path}"
    --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}" "${stats_state_proto}"
  DEPENDS "${stats_state_proto}")

# stats_state_grpc_proto
add_library(stats_state_grpc_proto ${stats_state_grpc_srcs} ${stats_state_grpc_hdrs}
                              ${stats_state_proto_srcs} ${stats_state_proto_hdrs})
target_link_libraries(stats_state_grpc_proto ${_REFLECTION} ${_GRPC_GRPCPP}
                      ${_PROTOBUF_LIBPROTOBUF})
target_include_directories(stats_state_grpc_proto PUBLIC ${CMAKE_BINARY_DIR})

set_target_properties(stats_state_grpc_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)


add_library(stats_state STATIC
  stats_to_proto.cpp)

target_link_libraries(stats_state iceberg_positional_delete iceberg_equality_delete stats_state_grpc_proto iceberg)
target_include_directories(stats_state PUBLIC ${CMAKE_SOURCE_DIR})
set_target_properties(stats_state PROPERTIES POSITION_INDEPENDENT_CODE ON)
