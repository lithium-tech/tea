#!/usr/bin/env bash
# Run the tea smoke_test binary for one matrix combination.
#
# Usage: run-smoke.sh <metadata_type> <table_type> <profile>
set -eo pipefail

source "$HOME/local/gpdb/greenplum_path.sh"
export MASTER_DATA_DIRECTORY=/gpdata/master/gpsne-1
build/tea/smoke_test/smoke_test \
  --metadata_type="$1" \
  --table_type="$2" \
  --profile="$3"
