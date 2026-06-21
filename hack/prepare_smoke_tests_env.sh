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

rm -f /tmp/minio /tmp/mc
ARCH=$(uname -m)
if [ "$ARCH" = "x86_64" ]; then
  MINIO_ARCH="linux-amd64"
elif [ "$ARCH" = "aarch64" ]; then
  MINIO_ARCH="linux-arm64"
else
  MINIO_ARCH="linux-amd64"
fi
wget -q -c "https://dl.min.io/server/minio/release/${MINIO_ARCH}/minio" -O /tmp/minio
wget -q -c "https://dl.min.io/client/mc/release/${MINIO_ARCH}/mc" -O /tmp/mc
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
