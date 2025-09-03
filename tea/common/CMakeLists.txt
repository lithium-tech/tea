add_library(table_public STATIC
  config.cpp
  config.h
  file_reader_provider.cpp
  file_reader_provider.h
  utils.cpp
  utils.h
  iceberg_json.cpp
  iceberg_json.h
  reader_properties.cpp
  reader_properties.h
  row_groups_utils.cpp
  row_groups_utils.h
)

target_include_directories(table_public PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(table_public PUBLIC iceberg_common_fs iceberg _rapidjson teapot_grpc_proto)
