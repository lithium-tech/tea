# --------------------------------------------------------------------
# Dockerfile for Tea Build Environment (Ubuntu 22.04)
# --------------------------------------------------------------------
# Based on the open-gpdb jammy/22.04 build image. Pre-installs Tea
# compile dependencies (Arrow, gRPC, gcc-13, CMake) but does NOT build
# Greenplum or Tea itself — those stay in CI workflows so Tea sources
# can change without rebuilding this image.
#
# Key Features:
# - open-gpdb base (gpadmin, SSH, GPDB build tooling)
# - gcc-13 / g++-13 and CMake >= 3.25 for Tea
# - LLVM 15 + clang for Arrow Gandiva
# - Static Arrow (lithium-tech/arrow with Tea patches) and gRPC under
#   /home/gpadmin/local
# - TestInfra tests under /tests
#
# Usage:
#   docker build -t tea-build:ubuntu22.04 .
#   docker run -h cdw -it tea-build:ubuntu22.04
#
# Build Tea inside the image (after installing Greenplum elsewhere):
#   mkdir -p build && cd build
#   cmake -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo \
#     -DGreenplum_ROOT=$Greenplum_ROOT -DCMAKE_PREFIX_PATH=$CMAKE_PREFIX_PATH \
#     -DCMAKE_C_COMPILER=gcc-13 -DCMAKE_CXX_COMPILER=g++-13 \
#     -DTEA_USE_THREAD_SANITIZER=OFF ..
#   ninja && ninja hive_metastore_server hive_metastore_client && ninja install
# --------------------------------------------------------------------

ARG BASE_IMAGE=ghcr.io/open-gpdb/gpdb-env:jammy-latest
FROM ${BASE_IMAGE}

ARG DEBIAN_FRONTEND=noninteractive
ARG CMAKE_VERSION=3.31.11
ARG ARROW_REPO=https://github.com/lithium-tech/arrow.git
ARG ARROW_REF=maint-15.0.2-tea
ARG GRPC_REF=v1.62.3
ARG PARALLEL=8

USER root

ENV USER=gpadmin \
    HOME=/home/gpadmin \
    DEPS_PREFIX=/home/gpadmin/local \
    Greenplum_ROOT=/opt/greenplum-db-6 \
    CMAKE_PREFIX_PATH=/home/gpadmin/local \
    PATH="/opt/greenplum-db-6/bin:/usr/local/bin:${PATH}"

# Packages, CMake, TestInfra — single layer.
# Arrow Gandiva needs LLVM >= 15 and clang; the base image only ships llvm-14.
RUN apt-get update -o Acquire::AllowInsecureRepositories=true \
  && apt-get install -y --no-install-recommends --allow-unauthenticated \
    clang-15 \
    libclang-15-dev \
    libcurl4-openssl-dev \
    libipc-run-perl \
    libprotobuf-dev \
    libtool \
    libxslt1-dev \
    llvm-15 \
    llvm-15-dev \
    pkg-config \
    protobuf-compiler \
    python3-pip \
    redis-server \
    software-properties-common \
  && add-apt-repository -y ppa:ubuntu-toolchain-r/test \
  && apt-get install -y --no-install-recommends --allow-unauthenticated \
    gcc-13 \
    g++-13 \
  && (systemctl disable --now redis-server || service redis-server stop || true) \
  && ln -s -f python2.7 /usr/bin/python \
  && locale-gen "en_US.UTF-8" \
  && update-locale LC_ALL="en_US.UTF-8" \
  && echo '/usr/local/lib' > /etc/ld.so.conf.d/local.conf \
  && ldconfig \
  && ARCH="$(uname -m)" \
  && case "${ARCH}" in \
       x86_64) CMAKE_ARCH=x86_64 ;; \
       aarch64) CMAKE_ARCH=aarch64 ;; \
       *) echo "Unsupported arch: ${ARCH}" >&2; exit 1 ;; \
     esac \
  && curl -fsSL \
       "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-${CMAKE_ARCH}.tar.gz" \
     | tar -xz -C /usr/local --strip-components=1 \
  && pip3 install --no-cache-dir pytest-testinfra paramiko \
  && rm -rf /var/lib/apt/lists/*

COPY tests /tests

USER gpadmin
WORKDIR /home/gpadmin

# Arrow (lithium-tech fork with Tea patches) + gRPC — single layer.
RUN mkdir -p "${DEPS_PREFIX}" /tmp/src /home/gpadmin/build/arrow-thirdparty \
  && git clone --depth 1 -b "${ARROW_REF}" "${ARROW_REPO}" /tmp/src/arrow \
  && cd /tmp/src/arrow \
  && ./cpp/thirdparty/download_dependencies.sh /tmp/src/arrow-thirdparty \
  && cp /tmp/src/arrow-thirdparty/* /home/gpadmin/build/arrow-thirdparty \
  && mkdir cpp/build && cd cpp/build \
  && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DCMAKE_INSTALL_PREFIX="${DEPS_PREFIX}" \
       -DCMAKE_C_COMPILER=gcc-13 \
       -DCMAKE_CXX_COMPILER=g++-13 \
       -DLLVM_ROOT=/usr/lib/llvm-15 \
       -DLLVM_DIR=/usr/lib/llvm-15/lib/cmake/llvm \
       -DClang_DIR=/usr/lib/llvm-15/lib/cmake/clang \
       -DCLANG_EXECUTABLE=/usr/bin/clang-15 \
       -DARROW_BUILD_STATIC=ON \
       -DARROW_BUILD_SHARED=OFF \
       -DARROW_DEPENDENCY_SOURCE=BUNDLED \
       -DARROW_NO_DEPRECATED_API=ON \
       -DARROW_LLVM_USE_SHARED=OFF \
       -DARROW_FILESYSTEM=ON \
       -DARROW_PARQUET=ON \
       -DARROW_S3=ON \
       -DARROW_WITH_SNAPPY=ON \
       -DARROW_WITH_LZ4=ON \
       -DARROW_WITH_ZLIB=ON \
       -DARROW_WITH_ZSTD=ON \
       -DARROW_IPC=ON \
       -DARROW_CSV=ON \
       -DARROW_WITH_RAPIDJSON=ON \
       -DARROW_GANDIVA=ON \
       -DARROW_COMPUTE=ON \
       .. \
  && make -j"${PARALLEL}" && make -j"${PARALLEL}" install \
  && git clone --depth 1 -b "${GRPC_REF}" https://github.com/grpc/grpc.git /tmp/src/grpc \
  && cd /tmp/src/grpc \
  && git submodule update --init --single-branch --depth 1 \
  && mkdir build && cd build \
  && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo \
       -DCMAKE_INSTALL_PREFIX="${DEPS_PREFIX}" \
       -DCMAKE_C_COMPILER=gcc-13 \
       -DCMAKE_CXX_COMPILER=g++-13 \
       -DgRPC_BUILD_SHARED_LIBS=OFF \
       -DgRPC_BUILD_STATIC_LIBS=ON \
       -DgRPC_BUILD_TESTS=OFF \
       -DgRPC_BUILD_EXAMPLES=OFF \
       -DgRPC_BUILD_CSHARP_EXT=OFF \
       -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF \
       -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
       -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF \
       -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
       -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF \
       -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
       -DgRPC_SSL_PROVIDER:STRING=package \
       .. \
  && make -j"${PARALLEL}" && make -j"${PARALLEL}" install \
  && rm -rf /tmp/src

CMD ["bash", "-c", "/tmp/init_system.sh"]
