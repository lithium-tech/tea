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

# teapot_mock_server
add_executable(teapot_mock_server mock_server.cpp)
target_include_directories(teapot_mock_server PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(teapot_mock_server teapot_grpc_proto ${_GRPC_GRPCPP}
                      Arrow::arrow_static Parquet::parquet_static
                      ${_PROTOBUF_LIBPROTOBUF} absl::flags absl::flags_parse absl::log
                      ${ABSL_WHOLE_ARCHIVE})

add_executable(teapot_cli cli.cpp)
target_include_directories(teapot_cli PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(teapot_cli teapot_grpc_proto ${_GRPC_GRPCPP}
                      ${_PROTOBUF_LIBPROTOBUF} absl::flags absl::flags_parse absl::log
                      ${ABSL_WHOLE_ARCHIVE})

add_executable(compare_iceberg compare_iceberg.cpp)
target_include_directories(compare_iceberg PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(compare_iceberg teapot_grpc_proto ${_GRPC_GRPCPP}
                      ${_PROTOBUF_LIBPROTOBUF} absl::flags absl::flags_parse absl::log samovar_network_layer samovar_client tea_metadata
                      ${ABSL_WHOLE_ARCHIVE})
