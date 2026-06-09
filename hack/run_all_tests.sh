#!/bin/bash
set -e

# --- Configuration ---
COMPILE_DIR="/root/compile"
INSTALL_PREFIX="/root/compile/bin"
GPDB_PREFIX="/root/compile/gpdb_bin"
TEA_DIR="/workspaces/tea"
NPROC=$(nproc)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() { echo -e "${BLUE}[INFO]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

mkdir -p $COMPILE_DIR
mkdir -p $INSTALL_PREFIX

log "Starting reproduction and testing process with $NPROC cores..."

# ==============================================================================
# PHASE 1: BUILD & UNIT TESTS (from reproduce.sh)
# ==============================================================================
log "=== PHASE 1: BUILD & UNIT TESTS ==="

# --- 1. Greenplum 6 ---
log "Step 1.1: Building Greenplum 6..."
cd $COMPILE_DIR
if [ ! -d "gpdb" ]; then
    git clone https://github.com/arenadata/gpdb.git -b 6.29.1_arenadata68 --depth 1
fi
cd gpdb
git submodule update --init

# IMPORTANT: Use GCC 11 for Greenplum compatibility
export CC=gcc-11
export CXX=g++-11

log "Configuring GPDB..."
./configure --with-perl --with-python --with-libxml --with-gssapi --with-pythonsrc-ext --prefix=$GPDB_PREFIX

log "Compiling and installing GPDB (this may take a while)..."
make -j$NPROC
make install
success "Greenplum 6 installed to $GPDB_PREFIX"

# --- 2. Apache Arrow ---
log "Step 1.2: Building Apache Arrow 15.0.2..."
cd $COMPILE_DIR
if [ ! -d "arrow" ]; then
    git clone https://github.com/apache/arrow.git -b maint-15.0.2 --depth 1
fi
cd arrow

log "Applying Tea patches to Arrow..."
git apply $TEA_DIR/vendor/arrow/*.patch || log "Patches already applied or skipped."

log "Downloading Arrow third-party dependencies..."
./cpp/thirdparty/download_dependencies.sh $COMPILE_DIR/arrow-thirdparty

mkdir -p cpp/build && cd cpp/build

# Use GCC 13 for modern components
export CC=gcc-13
export CXX=g++-13

log "Configuring Arrow..."
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX \
  -DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX \
  -DARROW_BUILD_STATIC=ON -DARROW_BUILD_SHARED=OFF \
  -DARROW_DEPENDENCY_SOURCE=BUNDLED -DOpenSSL_SOURCE=SYSTEM -DARROW_NO_DEPRECTATED_API=ON \
  -DARROW_LLVM_USE_SHARED=OFF -DARROW_FILESYSTEM=ON -DARROW_PARQUET=ON \
  -DARROW_S3=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_LZ4=ON \
  -DARROW_WITH_ZLIB=ON -DARROW_WITH_ZSTD=ON -DARROW_IPC=ON -DARROW_CSV=ON \
  -DARROW_WITH_RAPIDJSON=ON -DARROW_GANDIVA=ON -DARROW_COMPUTE=ON ..

log "Compiling Arrow..."
make -j$NPROC
make install
success "Apache Arrow installed to $INSTALL_PREFIX"

# --- 3. gRPC ---
log "Step 1.3: Building gRPC 1.62.3..."
cd $COMPILE_DIR
if [ ! -d "grpc" ]; then
    git clone https://github.com/grpc/grpc.git -b v1.62.3 --depth 1
fi
cd grpc
git submodule update --init --single-branch --depth 1

mkdir -p build && cd build
log "Configuring gRPC..."
cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=$INSTALL_PREFIX \
  -DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX \
  -DgRPC_BUILD_SHARED_LIBS=OFF -DgRPC_BUILD_STATIC_LIBS=ON \
  -DgRPC_BUILD_TESTS=OFF -DgRPC_BUILD_EXAMPLES=OFF \
  -DgRPC_SSL_PROVIDER:STRING=package ..

log "Compiling gRPC..."
make -j$NPROC
make install
success "gRPC installed to $INSTALL_PREFIX"

# --- 4. Tea ---
log "Step 1.4: Building Tea..."
cd $TEA_DIR
rm -rf build && mkdir build
mkdir -p build/arrow-thirdparty
cp $COMPILE_DIR/arrow-thirdparty/* build/arrow-thirdparty/ 2>/dev/null || true

cd build
log "Configuring Tea with Ninja..."
cmake .. -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DGreenplum_ROOT=$GPDB_PREFIX \
  -DCMAKE_PREFIX_PATH=$INSTALL_PREFIX \
  -DCMAKE_C_COMPILER=$CC -DCMAKE_CXX_COMPILER=$CXX \
  -DTEA_USE_THREAD_SANITIZER=OFF \
  -DUSE_REST=OFF

log "Compiling Tea..."
ninja
ninja hive_metastore_server hive_metastore_client
ninja install
success "Tea built and installed successfully!"

# --- 5. Unit Testing ---
log "Step 1.5: Running unit tests..."
ctest --output-on-failure
success "Unit testing complete!"
