get_filename_component(samovar_proto proto/samovar.proto ABSOLUTE)
get_filename_component(root_path "${CMAKE_SOURCE_DIR}" ABSOLUTE)

set(samovar_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/proto/samovar.pb.cc")
set(samovar_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/proto/samovar.pb.h")

add_custom_command(
  OUTPUT "${samovar_proto_srcs}" "${samovar_proto_hdrs}"
  COMMAND
    ${_PROTOBUF_PROTOC} ARGS --proto_path "${CMAKE_SOURCE_DIR}" --grpc_out "${CMAKE_BINARY_DIR}" --cpp_out
    "${CMAKE_BINARY_DIR}" -I "${root_path}"
    --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}" "${samovar_proto}"
  DEPENDS "${samovar_proto}")

add_library(samovar_proto ${samovar_proto_srcs} ${samovar_proto_hdrs})
target_link_libraries(samovar_proto ${_REFLECTION} ${_GRPC_GRPCPP}
                      ${_PROTOBUF_LIBPROTOBUF} teapot_grpc_proto tea_log)
target_include_directories(samovar_proto PUBLIC "${CMAKE_BINARY_DIR} ${CMAKE_SOURCE_DIR}/teapot/")

set_target_properties(samovar_proto PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_subdirectory(network_layer)

add_library(
  samovar_client STATIC
  planner.cpp
  planner.h
  samovar_data_client.cpp
  samovar_data_client.h
  batcher.cpp
  batcher.h
  single_queue_client.cpp
  single_queue_client.h
  utils.cpp
  utils.h
)

target_compile_definitions(samovar_client PUBLIC TEA_VERSION="${TEA_VERSION}")
target_include_directories(samovar_client PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(samovar_client PUBLIC
  samovar_network_layer
  teapot_grpc_proto
  iceberg
  table_public
  samovar_proto
  hiredis
  compression_lib
)

add_subdirectory(cli)

add_executable(
  samovar_test
  ut/redis_test.cpp
  ut/utils_test.cpp
)

target_include_directories(samovar_test PUBLIC ${CMAKE_SOURCE_DIR} ${Greenplum_INCLUDE_SERVER})
target_link_libraries(samovar_test PUBLIC pq gtest_main gmock 
  samovar_client teapot_grpc_proto tea_log samovar_network_layer compression_lib)
if (NOT APPLE)
  target_link_options(samovar_test PRIVATE "-Wl,--allow-multiple-definition")
endif()
add_test(NAME samovar_test COMMAND samovar_test)

include(GoogleTest)
gtest_discover_tests(samovar_test)
