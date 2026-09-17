#!/bin/bash
set -e

# --- Configuration ---
COMPILE_DIR="/root/compile"
INSTALL_PREFIX="/root/compile/bin"
GPDB_PREFIX="/root/compile/gpdb_bin"
TEA_DIR="/workspaces/tea"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() { echo -e "${BLUE}[INFO]${NC} $1"; }
error() { echo -e "${RED}[ERROR]${NC} $1"; exit 1; }
success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }

# ==============================================================================
# PHASE 2: FUNCTIONAL TESTS (SMOKE TESTS) - ENVIRONMENT PREPARATION
# ==============================================================================
log "=== PHASE 2: FUNCTIONAL (SMOKE) TESTS - PREPARING ENVIRONMENT ==="

cd $TEA_DIR

log "Step 2.1: Preparing base environment..."
sudo apt-get update && sudo apt-get install -y redis-server wget
sudo locale-gen "ru_RU.CP1251" || true
sudo mkdir -p /gpdata
sudo chown -R $(whoami) /gpdata

log "Step 2.2: Starting Redis..."
sudo /etc/init.d/redis-server start || true
redis-cli ping

log "Step 2.3: Initializing Greenplum..."
if ! id "gpadmin" &>/dev/null; then
    useradd -m -s /bin/bash gpadmin
fi
chmod 755 /root

# Forcefully stop any running Greenplum processes and clean data dir to allow re-runs
pkill -u gpadmin postgres || true
rm -rf /gpdata/*
rm -f /tmp/.s.PGSQL.*

chown -R gpadmin:gpadmin /gpdata
mkdir -p "$TEA_DIR/gpdata"
chown -R gpadmin:gpadmin "$TEA_DIR/gpdata"

su gpadmin -c "source $GPDB_PREFIX/greenplum_path.sh && cd $TEA_DIR && NUM_SEGS=2 bash test/start-gp.sh $GPDB_PREFIX /gpdata"
su gpadmin -c "source $GPDB_PREFIX/greenplum_path.sh && export MASTER_DATA_DIRECTORY=/gpdata/master/gpsne-1 && psql -d postgres -c 'CREATE EXTENSION IF NOT EXISTS tea;'"

log "Step 2.4: Deploying Iceberg (Minio + HMS)..."
# Kill any existing Minio/HMS processes and free ports.
pkill -9 -f minio || true
pkill -9 -f hive_metastore || true
# Kill anything still holding ports 9090 (HMS) and 9000 (Minio)
for port in 9090 9000; do
  pid=$(ss -tlnp | grep ":${port} " | grep -oP 'pid=\K[0-9]+' || true)
  if [ -n "$pid" ]; then
    log "Killing PID $pid on port $port"
    kill -9 "$pid" 2>/dev/null || true
  fi
done
sleep 2

rm -f /tmp/minio /tmp/mc /tmp/silo.tar.gz /tmp/mcli.tar.gz
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
  SILO_ARCH="linux_amd64"
  MCLI_ARCH="linux_amd64"
elif [ "$ARCH" = "aarch64" ] || [ "$ARCH" = "arm64" ]; then
  SILO_ARCH="linux_arm64"
  MCLI_ARCH="linux_arm64"
else
  SILO_ARCH="linux_amd64"
  MCLI_ARCH="linux_amd64"
fi
wget -q https://github.com/pgsty/silo/releases/download/RELEASE.2026-09-03T13-18-01Z/silo_20260903131801.0.0_${SILO_ARCH}.tar.gz -O /tmp/silo.tar.gz
wget -q https://github.com/pgsty/mc/releases/download/RELEASE.2026-09-13T00-00-00Z/mcli_20260913000000.0.0_${MCLI_ARCH}.tar.gz -O /tmp/mcli.tar.gz
tar -xzf /tmp/silo.tar.gz -C /tmp silo
tar -xzf /tmp/mcli.tar.gz -C /tmp mcli
mv -f /tmp/silo /tmp/minio
mv -f /tmp/mcli /tmp/mc
chmod +x /tmp/minio /tmp/mc

# Copy HMS tools AFTER killing old processes
mkdir -p $TEA_DIR/build/hms
cp $TEA_DIR/build/_deps/iceberg-cxx-build/tools/hive_metastore_server $TEA_DIR/build/hms/ || error "hive_metastore_server not found — run 'ninja hive_metastore_server' in build/"
cp $TEA_DIR/build/_deps/iceberg-cxx-build/tools/hive_metastore_client $TEA_DIR/build/hms/ || error "hive_metastore_client not found — run 'ninja hive_metastore_client' in build/"

cd $TEA_DIR
CI_PROJECT_DIR=$(pwd) MINIO_EXECUTABLE=/tmp/minio MC_EXECUTABLE=/tmp/mc MINIO_DATA_DIR=/tmp/minio-data HMS_DIR=$(pwd)/build/hms bash test/iceberg/gen/init.sh

log "Step 2.5: Configuring Tea..."
mkdir -p $GPDB_PREFIX/tea
cp test/config/tea-config.json test/config/tea-config-schema.json $GPDB_PREFIX/tea/

success "Smoke test environment prepared successfully! Minio, HMS, Greenplum, and Redis are running in the background."
