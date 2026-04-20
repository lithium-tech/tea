add_library(tea MODULE)

target_sources(tea PRIVATE tea_exttable.c tea_exttable_proj.c tea_import.c
                           tea.c tea_reader.cpp tea_tuple.c tea_common.c)
if(TEA_BUILD_FDW)
  target_sources(tea PRIVATE tea_fdw.c tea_fdw_proj.c tea_fdw_options.c)
endif()

# TODO(gmusya): enable or remove
# if(TEA_BUILD_STATS)
  # add_compile_definitions(-DTEA_BUILD_STATS)
  # target_sources(tea PRIVATE tea_stats.c)
# endif()

target_include_directories(tea PRIVATE ${CMAKE_SOURCE_DIR}
                                       ${Greenplum_INCLUDE_SERVER})

target_link_libraries(
  tea PRIVATE reader tea_log Arrow::arrow_static
              Parquet::parquet_static gp_filter_convert)

if (USE_REST)
  target_link_libraries(tea PRIVATE cpr)
endif()

target_compile_definitions(tea PRIVATE TEA_VERSION="${TEA_VERSION}")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  target_link_options(tea PRIVATE -static-libstdc++
                      -Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/tea.map)
  if(GRPC_USE_VENDOR)
    target_link_options(tea PRIVATE "-Wl,--allow-multiple-definition")
  endif()
endif()
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_link_options(tea PRIVATE -static-libgcc)
endif()
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
  target_link_options(tea PRIVATE "-bundle_loader"
                      "${Greenplum_BINDIR}/postgres")
endif()

set_target_properties(
  tea
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             LINK_FLAGS ${Greenplum_LDFLAGS}
             PREFIX ""
             OUTPUT_NAME tea-${TEA_VERSION})

install(TARGETS tea LIBRARY
  DESTINATION "lib/postgresql"
  COMPONENT tea)

