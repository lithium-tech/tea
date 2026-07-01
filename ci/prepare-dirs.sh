#!/usr/bin/env bash
# Create the build/install directories and set up python2 + locale (shared by
# every build job).
set -eo pipefail

mkdir -p "$HOME/local" build
sudo ln -s -f python2 /usr/bin/python
sudo locale-gen "en_US.UTF-8"
