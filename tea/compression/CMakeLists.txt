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
