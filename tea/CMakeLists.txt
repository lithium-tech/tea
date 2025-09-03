add_subdirectory(util)
add_subdirectory(common)
add_subdirectory(debug)
add_subdirectory(observability)
add_subdirectory(gpext)
add_subdirectory(filter)
add_subdirectory(table)
add_subdirectory(smoke_test)
add_subdirectory(test_utils)
add_subdirectory(samovar)
add_subdirectory(metadata)
add_subdirectory(compression)
add_subdirectory(cli)

add_library(
  reader STATIC
  reader.h
  reader.cpp
)
set_target_properties(reader PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_link_libraries(reader PUBLIC table)
