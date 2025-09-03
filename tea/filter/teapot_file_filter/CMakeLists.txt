add_library(teapot_file_filter STATIC
  filter.cpp
  filter.h
  node.cpp
  node.h
  serializer.cpp
  serializer.h
)

target_link_libraries(teapot_file_filter PRIVATE iceberg filter_representation _rapidjson Arrow::arrow_static)
target_include_directories(teapot_file_filter PUBLIC ${CMAKE_SOURCE_DIR})

set_target_properties(teapot_file_filter PROPERTIES POSITION_INDEPENDENT_CODE ON)
