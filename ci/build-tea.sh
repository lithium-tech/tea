#!/usr/bin/env bash
# Configure and build tea against the gpdb installed in $HOME/local/gpdb.
#
# Usage: build-tea.sh [tea_build_ext]
#
# tea_build_ext controls -DTEA_BUILD_EXT (the tea external-table protocol),
# ON by default. Pass OFF for forks that don't support it (e.g. open-gpdb).
set -eo pipefail

tea_build_ext="${1:-ON}"

mkdir -p build/arrow-thirdparty
if [ -d "$HOME/build/arrow-thirdparty" ]; then
  cp -an "$HOME"/build/arrow-thirdparty/. build/arrow-thirdparty/
fi

cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DGreenplum_ROOT="$HOME/local/gpdb" -DCMAKE_PREFIX_PATH="$HOME/local" \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DTEA_USE_THREAD_SANITIZER=OFF -DTEA_BUILD_EXT="${tea_build_ext}" ..
ninja
ninja hive_metastore_server hive_metastore_client
ninja install
