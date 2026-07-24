#!/usr/bin/env bash
# Configure and build tea against the gpdb installed in $HOME/local/gpdb.
set -eo pipefail

mkdir -p build/arrow-thirdparty
if [ -d "$HOME/build/arrow-thirdparty" ]; then
  cp -an "$HOME"/build/arrow-thirdparty/. build/arrow-thirdparty/
fi

cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DGreenplum_ROOT="$HOME/local/gpdb" -DCMAKE_PREFIX_PATH="$HOME/local" \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DTEA_USE_THREAD_SANITIZER=OFF -DTEA_BUILD_EXT=OFF ..
ninja
ninja hive_metastore_server hive_metastore_client
ninja install
