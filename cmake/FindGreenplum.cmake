find_program(
        PG_CONFIG pg_config
        PATHS ${Greenplum_ROOT}
        PATH_SUFFIXES bin
        NO_DEFAULT_PATH
)

if(NOT PG_CONFIG)
    message(FATAL_ERROR "Could not find pg_config in greenplum path")
else()
    set(Greenplum_FOUND TRUE)
    message(STATUS "Greenplum found; pg_config is: ${PG_CONFIG}")
endif()

execute_process(
        COMMAND ${PG_CONFIG} "--ldflags"
        OUTPUT_VARIABLE gp_ldflags
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
string(REPLACE " -Wl,-rpath,\$ORIGIN/../lib " " " gp_ldflags_x ${gp_ldflags})
message(STATUS "Additional (Greenplum) LDFLAGS: " ${gp_ldflags_x})
set(Greenplum_LDFLAGS "${gp_ldflags_x}" CACHE PATH "Greenplum ldflags.")

execute_process(
        COMMAND ${PG_CONFIG} "--pkglibdir"
        OUTPUT_VARIABLE gp_pkglibdir
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Greenplum_PKGLIBDIR "${gp_pkglibdir}" CACHE PATH "Greenplum pkglibdir.")

execute_process(
        COMMAND ${PG_CONFIG} "--sharedir"
        OUTPUT_VARIABLE gp_sharedir
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Greenplum_SHAREDIR "${gp_sharedir}" CACHE PATH "Greenplum sharedir.")

execute_process(
        COMMAND ${PG_CONFIG} "--bindir"
        OUTPUT_VARIABLE gp_bindir
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Greenplum_BINDIR "${gp_bindir}" CACHE PATH "Greenplum bindir.")

execute_process(
        COMMAND ${PG_CONFIG} "--libdir"
        OUTPUT_VARIABLE gp_libdir
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Greenplum_LIBDIR "${gp_libdir}" CACHE PATH "Greenplum libdir.")


execute_process(
        COMMAND ${PG_CONFIG} "--version"
        OUTPUT_VARIABLE pg_version
        OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS pg_version " is ${pg_version}")

string(SUBSTRING ${pg_version} 11 1 pg_version_major) # skip "PostgreSQL "
