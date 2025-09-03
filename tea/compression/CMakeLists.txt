add_library(compression_lib STATIC
  compression_registry.cpp
  compression_registry.h
  arrow_codec.cpp
  arrow_codec.h
  lz4.cpp
  lz4.h
  identity.cpp
  identity.h
  snappy.cpp
  snappy.h
)

target_include_directories(compression_lib PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(compression_lib PUBLIC Arrow::arrow_static)

add_executable(
  compression_test
  ut/test_compressors.cpp
)

target_include_directories(compression_test PUBLIC ${CMAKE_SOURCE_DIR} ${Greenplum_INCLUDE_SERVER})
target_link_libraries(compression_test PUBLIC compression_lib gtest_main gmock)
if (NOT APPLE)
  target_link_options(compression_test PRIVATE "-Wl,--allow-multiple-definition")
endif()
add_test(NAME compression_test COMMAND compression_test)
