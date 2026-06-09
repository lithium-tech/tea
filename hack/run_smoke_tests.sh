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
# PHASE 2: FUNCTIONAL TESTS (SMOKE TESTS)
# ==============================================================================
log "=== PHASE 2: FUNCTIONAL (SMOKE) TESTS ==="

# 1. Prepare environment using the standalone script
bash hack/prepare_smoke_tests_env.sh

log "Step 2.6: Running smoke tests..."

# Tests MUST run as gpadmin — Greenplum uses peer auth, so the OS user must match the DB role.
# Running as root causes silent connection failure and all tests get SKIPPED.
SMOKE_TEST="$TEA_DIR/build/tea/smoke_test/smoke_test"
GP_ENV="source $GPDB_PREFIX/greenplum_path.sh && export MASTER_DATA_DIRECTORY=/gpdata/master/gpsne-1 && export PGDATABASE=postgres"

su gpadmin -c "$GP_ENV && $SMOKE_TEST --metadata_type=teapot --table_type=external --profile=''"
su gpadmin -c "$GP_ENV && $SMOKE_TEST --metadata_type=teapot --table_type=foreign --profile=''"
su gpadmin -c "$GP_ENV && $SMOKE_TEST --metadata_type=iceberg --table_type=external --profile='samovar'"
su gpadmin -c "$GP_ENV && $SMOKE_TEST --metadata_type=iceberg --table_type=foreign --profile='samovar'"
su gpadmin -c "$GP_ENV && $SMOKE_TEST --metadata_type=iceberg --table_type=foreign --profile=''"

success "All functional and smoke tests completed successfully!"
