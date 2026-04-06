set(USE_ARROW ON)

include(FetchContent)

set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  hiredis
  EXCLUDE_FROM_ALL
  URL_HASH SHA256=9a2058e2ae1ec3b547a2e6b6bbc32e1bb464034909c9b2152f1be1c4365c841b
  URL https://github.com/redis/hiredis/archive/e0f48202aaf556a8ca17e02017c1c2559a20d433.tar.gz
)

set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  googletest
  EXCLUDE_FROM_ALL
  GIT_REPOSITORY ${GITHUB}/google/googletest.git
  GIT_TAG release-1.11.0
)

FetchContent_Declare(
  rapidjson
  URL https://github.com/miloyip/rapidjson/archive/232389d4f1012dddec4ef84861face2d2ba85709.tar.gz
  URL_HASH MD5=577d3495a07b66fcd4a2866c93831bc4
  PATCH_COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/rapidjson/CMakeLists.txt ./
)

FetchContent_Declare(
  iceberg-cxx
  EXCLUDE_FROM_ALL
  GIT_REPOSITORY ${GITHUB}/lithium-tech/iceberg-cxx.git
<<<<<<< HEAD
  GIT_TAG c6bb332f2a0a1ae8d580828e732c64e059a3c031
=======
  GIT_TAG 088cfcff6fcf9a539a280a2f41012ac61b6594d4
  PATCH_COMMAND git reset --hard HEAD && git clean -df
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/iceberg-empty.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/iceberg-ignore-partition-validation.patch
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/iceberg-expose-manifests-filtering.patch
<<<<<<< HEAD
>>>>>>> 1078081 (Change patch order)
=======
    && git apply ${CMAKE_CURRENT_SOURCE_DIR}/iceberg-thrift-tuuid.patch
>>>>>>> 12bcd1c (Check if iceberg-cxx thirft sources were broken)
)

FetchContent_MakeAvailable(googletest hiredis)

set(ICECXX_BUILD_ARROW OFF CACHE BOOL "")
set(ICECXX_BUILD_ABSEIL OFF CACHE BOOL "")
set(ICECXX_BUILD_TOOLS ON CACHE BOOL "")
set(ICECXX_GENERATOR ${ICECXX_GENERATOR} CACHE BOOL "")
set(ICECXX_USE_REST ${USE_REST} CACHE BOOL "")
FetchContent_MakeAvailable(iceberg-cxx)
