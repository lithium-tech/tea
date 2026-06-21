FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y \
    autoconf \
    bison \
    build-essential \
    cmake \
    curl \
    flex \
    git \
    gnupg \
    iproute2 \
    iputils-ping \
    libapr1-dev \
    libaprutil1-dev \
    libbz2-dev \
    libcurl4-openssl-dev \
    libevent-dev \
    libipc-run-perl \
    libkrb5-dev \
    libpam-dev \
    libperl-dev \
    libreadline-dev \
    libssl-dev \
    libtool \
    libuv1-dev \
    libxerces-c-dev \
    libxml2-dev \
    libxslt-dev \
    libyaml-dev \
    libzstd-dev \
    locales \
    net-tools \
    ninja-build \
    openssh-client \
    openssh-server \
    pkg-config \
    python2 \
    python2-dev \
    python3 \
    python3-dev \
    redis-server \
    rsync \
    software-properties-common \
    sudo \
    tar \
    unzip \
    wget \
    zlib1g-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    ./llvm.sh 17 && \
    apt-get install -y clangd-17 && \
    update-alternatives --install /usr/bin/clangd clangd /usr/bin/clangd-17 100 && \
    rm llvm.sh

RUN add-apt-repository ppa:ubuntu-toolchain-r/test -y && \
    apt-get update && \
    apt-get install -y gcc-11 g++-11 gcc-13 g++-13 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN ARCH=$(uname -m) && \
    if [ "$ARCH" = "x86_64" ]; then CMAKE_ARCH="x86_64"; else CMAKE_ARCH="aarch64"; fi && \
    wget https://github.com/Kitware/CMake/releases/download/v3.31.11/cmake-3.31.11-linux-${CMAKE_ARCH}.tar.gz && \
    tar xf cmake-3.31.11-linux-${CMAKE_ARCH}.tar.gz && \
    rm cmake-3.31.11-linux-${CMAKE_ARCH}.tar.gz && \
    mv cmake-3.31.11-linux-${CMAKE_ARCH} /opt/cmake
ENV PATH=/opt/cmake/bin:$PATH

RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

RUN ln -s -f python2 /usr/bin/python

RUN mkdir -p /root/compile
ENV COMPILE_DIR=/root/compile

ENV CC=gcc-13
ENV CXX=g++-13

WORKDIR /workspaces/tea
