#!/usr/bin/env bash
# Run the tea smoke_test binary for one matrix combination.
#
# Usage: run-smoke.sh <metadata_type> <table_type> <profile>
set -eo pipefail

source "$HOME/local/gpdb/greenplum_path.sh"
export MASTER_DATA_DIRECTORY=/gpdata/master/gpsne-1
build/tea/smoke_test/smoke_test \
  --table_type="$1" \
  --profile="$2"
