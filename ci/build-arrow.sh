#!/usr/bin/env bash
# Build and install a static Apache Arrow 15 into $HOME/local. Third-party
# downloads land in build/arrow-thirdparty (cached by the workflow).
set -eo pipefail

git clone https://github.com/apache/arrow.git -b maint-15.0.2 --depth 1
cd arrow
git apply ../vendor/arrow/fix_c-ares_url.patch
git apply ../vendor/arrow/like-dot-nl.patch
git apply ../vendor/arrow/ilike.patch
git apply ../vendor/arrow/snappy.patch
./cpp/thirdparty/download_dependencies.sh ../build/arrow-thirdparty
mkdir cpp/build
cd cpp/build
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX="$HOME/local" \
  -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
  -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
  -DARROW_DEPENDENCY_SOURCE=BUNDLED -DARROW_NO_DEPRECTATED_API=ON \
  -DARROW_LLVM_USE_SHARED=OFF -DARROW_FILESYSTEM=ON -DARROW_PARQUET=ON \
  -DARROW_S3=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON \
  -DARROW_IPC=ON -DARROW_CSV=ON -DARROW_WITH_RAPIDJSON=ON \
  -DARROW_GANDIVA=ON -DARROW_COMPUTE=ON ..
make -j8
make -j8 install

cd ../../..
rm -rf arrow
