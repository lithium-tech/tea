#!/usr/bin/env bash
# Pack the runtime artifacts (installed gpdb+tea, the smoke_test binary and the
# Hive Metastore tools) into ci-artifacts/ for the downstream test jobs.
set -eo pipefail

mkdir -p ci-artifacts
tar -C "$HOME/local" -czf ci-artifacts/gpdb-with-tea.tar.gz gpdb
cp build/tea/smoke_test/smoke_test ci-artifacts/smoke_test
mkdir -p ci-artifacts/hms
install -m 0755 build/_deps/iceberg-cxx-build/tools/hive_metastore_server ci-artifacts/hms/hive_metastore_server
install -m 0755 build/_deps/iceberg-cxx-build/tools/hive_metastore_client ci-artifacts/hms/hive_metastore_client
