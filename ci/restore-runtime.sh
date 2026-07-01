#!/usr/bin/env bash
# Unpack the runtime artifacts produced by the build job: the installed
# gpdb+tea, the smoke_test binary and the Hive Metastore tools.
set -eo pipefail

mkdir -p "$HOME/local" build/tea/smoke_test build/hms
tar -xzf ci-artifacts/gpdb-with-tea.tar.gz -C "$HOME/local"
install -m 0755 ci-artifacts/smoke_test build/tea/smoke_test/smoke_test
install -m 0755 ci-artifacts/hms/hive_metastore_server build/hms/hive_metastore_server
install -m 0755 ci-artifacts/hms/hive_metastore_client build/hms/hive_metastore_client
