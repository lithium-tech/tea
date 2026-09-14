#!/usr/bin/env bash
# Download SILO (MinIO-compatible server, https://github.com/pgsty/silo) + its
# mcli client, then bring up the Iceberg services (SILO + Hive Metastore) and
# upload the test data.
set -eo pipefail

wget -q https://github.com/pgsty/silo/releases/download/RELEASE.2026-09-03T13-18-01Z/silo_20260903131801.0.0_linux_amd64.tar.gz -O /tmp/silo.tar.gz
wget -q https://github.com/pgsty/mc/releases/download/RELEASE.2026-09-13T00-00-00Z/mcli_20260913000000.0.0_linux_amd64.tar.gz -O /tmp/mcli.tar.gz
tar -xzf /tmp/silo.tar.gz -C /tmp silo
tar -xzf /tmp/mcli.tar.gz -C /tmp mcli
mv /tmp/silo /tmp/minio
mv /tmp/mcli /tmp/mc
chmod +x /tmp/minio /tmp/mc

export CI_PROJECT_DIR="$PWD"
HMS_DIR="$CI_PROJECT_DIR/build/hms"

MINIO_EXECUTABLE=/tmp/minio MC_EXECUTABLE=/tmp/mc \
MINIO_DATA_DIR=/tmp/minio-data HMS_DIR="$HMS_DIR" \
  bash test/iceberg/gen/init.sh
