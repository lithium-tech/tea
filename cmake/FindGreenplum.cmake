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
        COMMAND ${PG_CONFIG} "--includedir-server"
        OUTPUT_VARIABLE gp_include_server
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Greenplum_INCLUDE_SERVER "${gp_include_server}" CACHE PATH "Greenplum include-server.")

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
find_library(LIBPQ pq PATHS "${Greenplum_LIBDIR}" REQUIRED NO_DEFAULT_PATH)
add_library(pq SHARED IMPORTED)
set_property(TARGET pq PROPERTY IMPORTED_LOCATION "${LIBPQ}")

find_library(LIBGPLDAP ldap PATHS "${Greenplum_LIBDIR}" NO_DEFAULT_PATH)
if(LIBGPLDAP)
        target_link_libraries(pq INTERFACE ${LIBGPLDAP})
endif()
find_library(LIBGPLDAP_R ldap_r PATHS "${Greenplum_LIBDIR}" NO_DEFAULT_PATH)
if(LIBGPLDAP_R)
        target_link_libraries(pq INTERFACE ${LIBGPLDAP_R})
endif()
find_library(LIBGPLBER lber PATHS "${Greenplum_LIBDIR}" NO_DEFAULT_PATH)
if(LIBGPLBER)
        target_link_libraries(pq INTERFACE ${LIBGPLBER})
endif()

execute_process(
        COMMAND ${PG_CONFIG} "--includedir"
        OUTPUT_VARIABLE gp_includedir
        OUTPUT_STRIP_TRAILING_WHITESPACE
)
set(Greenplum_INCLUDE "${gp_includedir}" CACHE PATH "Greenplum includedir.")
target_include_directories(pq INTERFACE "${Greenplum_INCLUDE}")

execute_process(
        COMMAND ${PG_CONFIG} "--version"
        OUTPUT_VARIABLE pg_version
        OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS pg_version " is ${pg_version}")

string(SUBSTRING ${pg_version} 11 1 pg_version_major) # skip "PostgreSQL "
