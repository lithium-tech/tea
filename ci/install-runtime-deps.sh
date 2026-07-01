#!/usr/bin/env bash
# Install the runtime dependencies needed to start a gpdb cluster and run the
# smoke tests (shared by the smoke-test jobs).
set -eo pipefail

sudo apt-get update
sudo apt-get install -y \
  libxerces-c3.2 \
  python2 \
  redis-server \
  software-properties-common
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test
sudo apt-get install -y libstdc++6
sudo systemctl disable --now redis-server
sudo ln -s -f python2 /usr/bin/python
sudo locale-gen "en_US.UTF-8"
