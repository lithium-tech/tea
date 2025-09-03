add_library(tea_util STATIC
  measure.h
  measure_c.h
  multishot_timer.cpp
  multishot_timer.h
)

target_include_directories(tea_util PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(tea_util PUBLIC Arrow::arrow_static)

add_executable(
  util_test
  ut/thread_pool_test.cpp
  ut/lru_cache_test.cpp)

target_link_libraries(util_test PRIVATE gtest_main tea_util)
target_include_directories(util_test PUBLIC ${CMAKE_SOURCE_DIR})

if(TEA_USE_THREAD_SANITIZER)
  set_target_properties(util_test PROPERTIES
    LINK_FLAGS -fsanitize=thread
    COMPILE_FLAGS -fsanitize=thread)
endif()

add_test(NAME util_test COMMAND util_test)
add_dependencies(check util_test)

include(GoogleTest)
gtest_discover_tests(util_test)
