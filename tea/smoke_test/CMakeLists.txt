add_library(
  tea_test
  pq.cpp
  fragment_info.cpp
  mock_teapot.h
  mock_teapot.cpp
  environment.cpp
  stats_state.cpp
)

target_link_libraries(tea_test stats_state_grpc_proto)
target_include_directories(tea_test PUBLIC ${CMAKE_SOURCE_DIR}
  ${Greenplum_INCLUDE_SERVER})

target_link_libraries(tea_test teapot_grpc_proto gtest
  tea_test_utils_static Arrow::arrow_static Parquet::parquet_static pq absl::flags absl::flags_parse iceberg iceberg-tools)

if (NOT APPLE)
  target_link_options(tea_test PRIVATE "-Wl,--allow-multiple-definition")
endif()

set(smoke_test_sources
  run_tests.cpp
  analyze_test.cpp
  complex_queries_test.cpp
  types_test.cpp
  cancel_test.cpp
  count_test.cpp
  equality_delete_test.cpp
  positional_delete_test.cpp
  potential_rg_filter_test.cpp
  delete_test.cpp
  encoding_test.cpp
  filter_tests/all_any_test.cpp
  filter_tests/arithmetic_test.cpp
  filter_tests/bitwise_test.cpp
  filter_tests/case_test.cpp
  filter_tests/coalesce_test.cpp
  filter_tests/comparison_test.cpp
  filter_tests/filter_test_base.cpp
  filter_tests/function_test.cpp
  filter_tests/in_test.cpp
  filter_tests/logical_test.cpp
  filter_tests/many_files_filter_test.cpp
  filter_tests/miscellaneous_test.cpp
  filter_tests/name_mapping_test.cpp
  filter_tests/null_test.cpp
  filter_tests/returned_error_test.cpp
  filter_tests/string_test.cpp
  filter_tests/temporal_test.cpp
  filter_tests/temporal_convert_test.cpp
  filter_tests/temporal_arithmetic_test.cpp
  filter_tests/unary_test.cpp
  session_test.cpp
  system_column_test.cpp
  stats_test.cpp
  iceberg_test.cpp
  iceberg_stats_test.cpp
  no_column_test.cpp
  projection_test.cpp
  real_parquet_test.cpp
  row_group_filter_test.cpp
  mapping_test.cpp
  self_join_test.cpp
  large_metadata_test.cpp
  mismatching_types_test.cpp
  part_of_file_test.cpp
  negative_server_test.cpp
)

add_executable(smoke_test ${smoke_test_sources})

target_link_libraries(smoke_test tea_test samovar_client)
target_compile_definitions(smoke_test PUBLIC PG_VERSION_MAJOR=${pg_version_major})

target_include_directories(smoke_test PUBLIC ${CMAKE_SOURCE_DIR}
  ${Greenplum_INCLUDE_SERVER})

if (NOT APPLE)
  target_link_options(smoke_test PRIVATE "-Wl,--allow-multiple-definition")
endif()

if (ICECXX_GENERATOR)
  add_executable(perf_test
    perf_test/main.cpp
    perf_test/table.cpp
    perf_test/json_table.cpp
    perf_test/json_test.cpp
    perf_test/test.cpp
    perf_test/positional_delete_table.cpp
    perf_test/positional_delete_test.cpp
  )

  target_include_directories(perf_test PUBLIC ${CMAKE_SOURCE_DIR})
  target_link_libraries(perf_test tea_test data-gen)

  add_executable(tpch_test
    tpch/main.cpp
  )

  target_include_directories(tpch_test PUBLIC ${CMAKE_SOURCE_DIR})
  target_link_libraries(tpch_test tea_test tpch-gen-lib)
  target_compile_definitions(tpch_test PUBLIC PG_VERSION_MAJOR=${pg_version_major})
endif()
