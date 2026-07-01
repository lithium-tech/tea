#!/usr/bin/env bash
# Self-hosted runners persist state between jobs; wipe /opt (keeping the
# actions-runner's own directories) so each build starts from a clean slate.
set -eo pipefail

cd /opt
find . -maxdepth 1 -mindepth 1 \
  '!' -path ./containerd \
  '!' -path ./actionarchivecache \
  '!' -path ./runner \
  '!' -path ./runner-cache \
  -exec rm -rf '{}' ';'
