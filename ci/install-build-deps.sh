#!/usr/bin/env bash
# Install the build-time dependencies needed to compile gpdb, Arrow, gRPC and
# tea from source (shared by every build job).
set -eo pipefail

sudo apt-get update
sudo apt-get install -y \
  libapr1-dev \
  libcurl4-openssl-dev \
  libevent-dev \
  libkrb5-dev \
  libperl-dev \
  libxerces-c-dev \
  python2 \
  python2-dev \
  redis-server \
  software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get install -y gcc-13 g++-13
sudo systemctl disable --now redis-server
