add_executable(samovar_cli main.cpp)
target_include_directories(samovar_cli PUBLIC ${CMAKE_SOURCE_DIR} ${Greenplum_INCLUDE_SERVER})
target_link_libraries(samovar_cli iceberg table_public iceberg-cpp absl::flags absl::flags_parse samovar_network_layer samovar_client tea_metadata)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_link_options(samovar_cli PRIVATE "-Wl,--allow-multiple-definition")
endif()
