add_library(table_public STATIC
  batch_size.cpp
  batch_size.h
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

add_executable(
  table_public_test
  ut/batch_size_test.cpp
)

target_link_libraries(table_public_test PRIVATE gtest_main gmock table table_public tea_test_utils_static)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_link_options(table_public_test PRIVATE "-Wl,--allow-multiple-definition")
endif()

add_test(NAME table_public_test COMMAND table_public_test)
add_dependencies(check table_public_test)

include(GoogleTest)
gtest_discover_tests(table_public_test)
