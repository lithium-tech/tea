#!/bin/bash

# expected variables:
# CI_PROJECT_DIR
export BUILD_DIR=${BUILD_DIR:-build}
export MINIO_EXECUTABLE=${MINIO_EXECUTABLE:-minio}
export MINIO_DATA_DIR=${MINIO_DATA_DIR:-/iceberg-data}
export MC_EXECUTABLE=${MC_EXECUTABLE:-mc}
export HMS_DIR=${HMS_DIR:-$CI_PROJECT_DIR/hms}

$CI_PROJECT_DIR/test/iceberg/gen/init_hms.sh
$CI_PROJECT_DIR/test/iceberg/gen/init_minio.sh
