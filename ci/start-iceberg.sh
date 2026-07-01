#!/usr/bin/env bash
# Download Minio + mc, then bring up the Iceberg services (Minio + Hive
# Metastore) and upload the test data.
set -eo pipefail

wget -q https://dl.min.io/server/minio/release/linux-amd64/minio -O /tmp/minio
wget -q https://dl.min.io/client/mc/release/linux-amd64/mc -O /tmp/mc
chmod +x /tmp/minio /tmp/mc

export CI_PROJECT_DIR="$PWD"
HMS_DIR="$CI_PROJECT_DIR/build/hms"

MINIO_EXECUTABLE=/tmp/minio MC_EXECUTABLE=/tmp/mc \
MINIO_DATA_DIR=/tmp/minio-data HMS_DIR="$HMS_DIR" \
  bash test/iceberg/gen/init.sh
