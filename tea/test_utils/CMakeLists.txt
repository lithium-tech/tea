set(test_utils_sources
    column.h
    location.h
    metadata.cpp
    metadata.h
    nessie_utils.cpp
    nessie_utils.h
    oid.h)

add_library(tea_test_utils_static STATIC ${test_utils_sources})

target_include_directories(
  tea_test_utils_static PUBLIC ${CMAKE_SOURCE_DIR} ${Greenplum_INCLUDE_SERVER})
target_link_libraries(tea_test_utils_static PUBLIC Arrow::arrow_static
                                                   Parquet::parquet_static iceberg iceberg-tools iceberg_test_utils)

if (NOT APPLE)
  target_link_options(tea_test_utils_static PRIVATE "-Wl,--allow-multiple-definition")
endif()
