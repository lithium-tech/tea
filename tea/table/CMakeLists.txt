add_library(
  table STATIC
  bridge.cpp
  bridge.h
  filter_convert.cpp
  filter_convert.h
  gp_funcs.h
  gp_funcs.cpp
  numeric_var.h
  numeric_var.cpp
  numeric.h
  numeric.cpp
  shared_state.cpp
  shared_state.h
  json_substitutor.cpp
  json_substitutor.h
)
set_target_properties(table PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_compile_definitions(table PRIVATE TEA_VERSION="${TEA_VERSION}")
target_include_directories(table PUBLIC ${CMAKE_SOURCE_DIR}
  ${Greenplum_INCLUDE_SERVER})
target_link_libraries(table PUBLIC Arrow::arrow_static Parquet::parquet_static
  teapot_grpc_proto _rapidjson Iconv::Iconv tea_log filter_representation row_filter stats_filter teapot_file_filter iceberg_streams iceberg_positional_delete iceberg_equality_delete iceberg_common_fs)

target_link_libraries(table PUBLIC tea_metadata)
target_link_libraries(table PUBLIC iceberg)
target_link_libraries(table PUBLIC stats_state_grpc_proto stats_state)

add_executable(
  table_test
  ut/bridge_test.cpp
  ut/config_test.cpp
  ut/json_substitutor_test.cpp
  ut/mock_gp.c
  ut/numeric_var_test.cpp
  ut/profile_to_tables_test.cpp
)

target_link_libraries(table_test PRIVATE gtest_main gmock table tea_test_utils_static
  absl::cleanup Arrow::arrow_static)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_link_options(table_test PRIVATE "-Wl,--allow-multiple-definition")
endif()

add_test(NAME table_test COMMAND table_test)
add_dependencies(check table_test)

include(GoogleTest)
gtest_discover_tests(table_test)
