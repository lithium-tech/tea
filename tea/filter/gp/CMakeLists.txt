add_library(gp_filter_convert STATIC
  array_converter.cpp
  array_converter.h
  convert.cpp
  convert.h
  datetime.cpp
  datetime.h
  pg_array.cpp
  pg_array.h
  types_mapping.cpp
  types_mapping.h
  validate.cpp
  validate.h
)

target_link_libraries(gp_filter_convert PRIVATE iceberg filter_representation tea_util Arrow::arrow_static)
target_include_directories(gp_filter_convert PUBLIC ${CMAKE_SOURCE_DIR} ${Greenplum_INCLUDE_SERVER})

set_target_properties(gp_filter_convert PROPERTIES POSITION_INDEPENDENT_CODE ON)
