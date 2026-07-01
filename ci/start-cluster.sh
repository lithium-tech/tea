#!/usr/bin/env bash
# Initialize and start a 2-segment demo gpdb cluster in /gpdata.
#
# Usage: start-cluster.sh [--create-extension]
#
# Pass --create-extension to also run "CREATE EXTENSION tea" in tea_ci (the
# smoke tests need it; the REST test creates the extension itself later).
set -eo pipefail

# TODO(gmusya): consider using make create-demo-cluster
sudo locale-gen "ru_RU.CP1251"
sudo mkdir -p /gpdata
sudo chown "$USER" /gpdata

source "$HOME/local/gpdb/greenplum_path.sh"
export MASTER_DATA_DIRECTORY=/gpdata/master/gpsne-1
NUM_SEGS=2 bash test/start-gp.sh "$HOME/local/gpdb" /gpdata

if [ "${1:-}" = "--create-extension" ]; then
  psql -d tea_ci -c 'CREATE EXTENSION tea;'
fi
