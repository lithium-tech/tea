add_library(tea_log STATIC
  ext_stats.h
  reader_stats.cpp
  reader_stats.h
  return_fl.cpp
  return_fl.h
  tea_log.cpp
  tea_log.h
)

target_include_directories(tea_log PRIVATE
  ${CMAKE_SOURCE_DIR}
  ${Greenplum_INCLUDE_SERVER}
)

set_target_properties(tea_log PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_link_libraries(tea_log PUBLIC tea_util iceberg _rapidjson Arrow::arrow_static)

