# Copyright 2018 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# cmake build file for C++ route_guide example. Assumes protobuf and gRPC have
# been installed using cmake. See cmake_externalproject/CMakeLists.txt for
# all-in-one cmake build that automatically builds all the dependencies before
# building route_guide.

cmake_minimum_required(VERSION 3.10)

if(MSVC)
  add_definitions(-D_WIN32_WINNT=0x600)
endif()

find_package(Threads REQUIRED)

if(GRPC_USE_VENDOR)
  # Grpc flags.
  set(protobuf_INSTALL OFF)
  set(protobuf_BUILD_TESTS OFF)
  set(gRPC_BUILD_CSHARP_EXT OFF)
  set(gRPC_BUILD_GRPC_CSHARP_PLUGIN OFF)
  set(gRPC_BUILD_GRPC_NODE_PLUGIN OFF)
  set(gRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN OFF)
  set(gRPC_BUILD_GRPC_PHP_PLUGIN OFF)
  set(gRPC_BUILD_GRPC_PYTHON_PLUGIN OFF)
  set(gRPC_BUILD_GRPC_RUBY_PLUGIN OFF)
  set(gRPC_BUILD_TESTS OFF)
  set(gRPC_INSTALL OFF)
  set(gRPC_SSL_PROVIDER package)

  add_subdirectory("${CMAKE_SOURCE_DIR}/vendor/grpc" EXCLUDE_FROM_ALL)

  set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
  set(_GRPC_GRPCPP grpc++)
  set(_REFLECTION grpc++_reflection)
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
  set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
  get_target_property(Protobuf_INCLUDE_DIRS protobuf::libprotobuf INTERFACE_INCLUDE_DIRECTORIES)

elseif(GRPC_FETCHCONTENT)
  # Another way is to use CMake's FetchContent module to clone gRPC at configure
  # time. This makes gRPC's source code available to your project, similar to a
  # git submodule.
  message(STATUS "Using gRPC via add_subdirectory (FetchContent).")
  include(FetchContent)
  FetchContent_Declare(
    grpc
    GIT_REPOSITORY ${GITHUB_MIRROR}/grpc/grpc.git
    GIT_TAG v1.62.3)
  FetchContent_MakeAvailable(grpc)

  # Since FetchContent uses add_subdirectory under the hood, we can use the grpc
  # targets directly from this build.
  set(_PROTOBUF_LIBPROTOBUF libprotobuf)
  set(_REFLECTION grpc++_reflection)
  set(_PROTOBUF_PROTOC $<TARGET_FILE:protoc>)
  set(_GRPC_GRPCPP grpc++)
  if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
  else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:grpc_cpp_plugin>)
  endif()
else()
  # This branch assumes that gRPC and all its dependencies are already installed
  # on this system, so they can be located by find_package().
  find_package(absl CONFIG REQUIRED)
  message(STATUS "Using absl ${absl_VERSION}")

  # Find Protobuf installation Looks for protobuf-config.cmake file installed by
  # Protobuf's cmake installation.

  set(protobuf_MODULE_COMPATIBLE TRUE)
  find_package(Protobuf CONFIG REQUIRED)
  message(STATUS "Using protobuf ${Protobuf_VERSION}")

  set(_PROTOBUF_LIBPROTOBUF protobuf::libprotobuf)
  set(_REFLECTION gRPC::grpc++_reflection)
  if(CMAKE_CROSSCOMPILING)
    find_program(_PROTOBUF_PROTOC protoc)
  else()
    set(_PROTOBUF_PROTOC $<TARGET_FILE:protobuf::protoc>)
  endif()

  # Find gRPC installation Looks for gRPCConfig.cmake file installed by gRPC's
  # cmake installation.
  find_package(gRPC CONFIG REQUIRED)
  message(STATUS "Using gRPC ${gRPC_VERSION}")

  set(_GRPC_GRPCPP gRPC::grpc++)
  if(CMAKE_CROSSCOMPILING)
    find_program(_GRPC_CPP_PLUGIN_EXECUTABLE grpc_cpp_plugin)
  else()
    set(_GRPC_CPP_PLUGIN_EXECUTABLE $<TARGET_FILE:gRPC::grpc_cpp_plugin>)
  endif()
endif()
