#!/usr/bin/env bash
# GitHub-hosted runners ship ~25 GB of preinstalled toolchains we do not use;
# the from-source gpdb + Arrow + gRPC build plus tea otherwise fills the disk
# and the final link fails with ENOSPC. Reclaim the space before building.
set -eo pipefail

sudo rm -rf /usr/local/lib/android /usr/share/dotnet /opt/ghc \
  /usr/local/.ghcup /usr/share/swift /opt/hostedtoolcache/CodeQL || true
sudo docker image prune --all --force > /dev/null 2>&1 || true
