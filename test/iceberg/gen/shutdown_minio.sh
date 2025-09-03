#!/bin/bash

killall minio
rm -r $MINIO_DATA_DIR/warehouse
