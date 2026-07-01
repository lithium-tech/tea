#!/usr/bin/env bash
# Build and install a static gRPC 1.62.3 into $HOME/local.
set -eo pipefail

git clone https://github.com/grpc/grpc.git -b v1.62.3 --depth 1
cd grpc
git submodule update --init --single-branch --depth 1
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX="$HOME/local" \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DgRPC_BUILD_SHARED_LIBS=OFF -DgRPC_BUILD_STATIC_LIBS=ON \
  -DgRPC_BUILD_TESTS=OFF -DgRPC_BUILD_EXAMPLES=OFF \
  -DgRPC_BUILD_CSHARP_EXT=OFF -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
  -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF  -DgRPC_SSL_PROVIDER:STRING=package ..
make -j8
make -j8 install

cd ../..
rm -rf grpc
