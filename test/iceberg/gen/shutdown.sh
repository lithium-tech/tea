#!/bin/bash

# expected variables:
# CI_PROJECT_DIR
# MINIO_DATA_DIR

$CI_PROJECT_DIR/test/iceberg/gen/shutdown_minio.sh
$CI_PROJECT_DIR/test/iceberg/gen/shutdown_hms.sh
