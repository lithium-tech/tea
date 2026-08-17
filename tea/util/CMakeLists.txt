add_library(tea_util STATIC
  measure.h
  measure_c.h
  multishot_timer.cpp
  multishot_timer.h
)

target_include_directories(tea_util PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tea_util PUBLIC Arrow::arrow_static)
