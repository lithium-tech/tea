add_library(
  tea_metadata STATIC
  access_empty.cpp
  access_empty.h
  access_file.cpp
  access_file.h
  access_iceberg.cpp
  access_iceberg.h
  metadata.h
  entries_stream_config.cpp
  entries_stream_config.h
  planner.cpp
  planner.h
  estimator.h
  estimator.cpp
)

target_include_directories(tea_metadata PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tea_metadata PUBLIC table_public iceberg tea_log filter_representation stats_filter teapot_file_filter compression_lib)
