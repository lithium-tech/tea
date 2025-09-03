#!/bin/bash

# expected variables:
# CI_PROJECT_DIR

$MINIO_EXECUTABLE server $MINIO_DATA_DIR &
sleep 5
$MC_EXECUTABLE alias set 'myminio' 'http://127.0.0.1:9000' 'minioadmin' 'minioadmin'
$MC_EXECUTABLE mb myminio/warehouse
$MC_EXECUTABLE cp --recursive $CI_PROJECT_DIR/test/iceberg/warehouse/  myminio/warehouse/
$MC_EXECUTABLE cp $CI_PROJECT_DIR/test/config/profile-to-tables.json myminio/warehouse/profile-to-tables.json
