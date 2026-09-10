add_library(cli_validators STATIC
  validate.cpp
  validate.h
)
target_include_directories(cli_validators PUBLIC ${CMAKE_SOURCE_DIR})
target_link_libraries(cli_validators PUBLIC table_public)

add_executable(tea_cli cli.cpp)
target_link_libraries(tea_cli cli_validators iceberg_common_fs table iceberg-cpp absl::flags absl::flags_parse)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(tea_cli PRIVATE "-Wl,--allow-multiple-definition")
endif()

set_target_properties(
  tea_cli
  PROPERTIES PREFIX ""
             OUTPUT_NAME tea_cli-${TEA_VERSION})

install(TARGETS tea_cli RUNTIME
  DESTINATION "bin"
  COMPONENT tea)

add_executable(
  cli_validators_test
  ut/validate_test.cpp
)
target_link_libraries(cli_validators_test PRIVATE gtest_main gmock cli_validators tea_test_utils_static tea_log)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(cli_validators_test PRIVATE "-Wl,--allow-multiple-definition")
endif()

add_test(NAME cli_validators_test COMMAND cli_validators_test)
add_dependencies(check cli_validators_test)

include(GoogleTest)
gtest_discover_tests(cli_validators_test)
