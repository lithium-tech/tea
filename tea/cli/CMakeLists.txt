add_executable(tea_cli cli.cpp)
target_link_libraries(tea_cli iceberg_common_fs table iceberg-cpp absl::flags absl::flags_parse)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(tea_cli PRIVATE "-Wl,--allow-multiple-definition")
endif()

target_compile_definitions(tea_cli PUBLIC TEA_VERSION="${TEA_VERSION}")
set_target_properties(
  tea_cli
  PROPERTIES PREFIX ""
             OUTPUT_NAME tea_cli-${TEA_VERSION})

install(TARGETS tea_cli RUNTIME
  DESTINATION "bin"
  COMPONENT tea)
