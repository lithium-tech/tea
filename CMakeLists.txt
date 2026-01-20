option(TEA_BUILD_FDW "Build Foreign Data Wrapper" ON)
option(HAS_ARROW_CSV "Arrow built with csv" ON)
option(ICECXX_GENERATOR "" ON)
option(USE_REST "Enable Rest catalog" ON)
option(TEA_USE_THREAD_SANITIZER "Enable running tests with ThreadSanitizer" ON)

cmake_minimum_required(VERSION 3.25)
project(tea)

# Base versions of Tea to generate upgrade and downgrade scripts
set(TEA_BASE_VERSIONS 1.71.2 1.72.2 1.72.3 1.73.0 1.74.0 1.74.1 1.75.0 1.76.0 1.76.1 1.76.2 1.76.3 1.77.0 1.77.1 1.77.2)
set(TEA_BASE_VERSIONS_WO_ICE_UDF 1.63.0 1.63.1 1.63.2 1.63.3 1.63.4 1.63.5 1.63.6 1.64.0 1.64.1 1.65.0 1.65.1 1.66.0 1.67.0 1.68.0 1.69.0 1.70.0)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

if("${CMAKE_BUILD_TYPE}" STREQUAL "")
  set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

set(BUILD_SHARED_LIBS OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(OPT_FLAGS "-fno-omit-frame-pointer")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OPT_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OPT_FLAGS}")

# deps

include(FindIconv)
include(cmake/grpc.cmake)
find_package(Arrow REQUIRED PATHS ${TEA_DEPS_PATH})
find_package(Parquet REQUIRED PATHS ${TEA_DEPS_PATH})
find_package(Greenplum REQUIRED)

find_package(Gandiva REQUIRED PATHS ${TEA_DEPS_PATH})

if (NOT DEFINED GITLAB_URL)
  if (GIT_HTTPS)
    set(GITLAB_URL https://${GITLAB})
  else()
    set(GITLAB_URL ssh://git@${GITLAB}:7999)
  endif()
endif()
set(GITHUB_MIRROR ${GITLAB_URL}/github-mirror)

add_subdirectory(vendor)

#

if(TEA_BUILD_STATS)
  add_compile_definitions(TEA_BUILD_STATS)
endif()

if(TEA_BUILD_FDW)
  add_compile_definitions(TEA_BUILD_FDW)
endif()

if(USE_REST)
  add_compile_definitions(USE_REST)
endif()

enable_testing()
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND})

set(PEDANTIC "-Werror -Wall -Wextra -Wshadow -Wunreachable-code -Wpointer-arith")
#set(PEDANTIC "${PEDANTIC} -Wfatal-errors")
set(IGNORES "-Wno-ignored-attributes -Wno-unused-parameter -Wno-missing-field-initializers")
set(CXX_IGNORES "-Wno-register")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
  set(IGNORES "${IGNORES} \
    -Wno-c2x-extensions \
    -Wno-deprecated-non-prototype")
  set(MUTED_ERRORS "\
    -Wno-error=mismatched-tags \
    -Wno-error=braced-scalar-init \
    -Wno-error=shadow \
    -Wno-error=sign-compare \
    -Wno-error=unused-function \
    -Wno-error=unused-const-variable \
    -Wno-error=unused-command-line-argument")
else()
  set(IGNORES "${IGNORES} \
    -Wno-shadow")
  set(MUTED_ERRORS "\
    -Wno-error=mismatched-tags \
    -Wno-error=sign-compare \
    -Wno-error=unused-function \
    -Wno-error=unknown-pragmas \
    -Wno-error=maybe-uninitialized \
    -Wno-error=shift-negative-value \
    -Wno-error=clobbered")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pipe ${PEDANTIC} ${MUTED_ERRORS} ${IGNORES}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pipe ${PEDANTIC} ${MUTED_ERRORS} ${IGNORES} ${CXX_IGNORES}")

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX
    ${Greenplum_ROOT}
    CACHE PATH "..." FORCE)
endif()
set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY true)

add_subdirectory(teapot)
add_subdirectory(tea)
add_subdirectory(extension)

if(PACK_ICETOOLS)
  # CMake ignores CPACK_ARCHIVE_tea_FILE_NAME for unknown reason. PACK_ICETOOLS affects tea's package name now.
  set(CPACK_COMPONENTS_ALL "tea;icetools")
  set(CPACK_COMPONENTS_GROUPING IGNORE)
  set(CPACK_ARCHIVE_COMPONENT_INSTALL ON)
  set(CPACK_ARCHIVE_tea_FILE_NAME $CPACK_PACKAGE_FILE_NAME)
endif()

set(CPACK_GENERATOR TGZ)
include(CPack)
