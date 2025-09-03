#!/bin/bash

# expected variables:
# HMS_DIR

$HMS_DIR/hive_metastore_server &
sleep 5
$HMS_DIR/hive_metastore_client create-table 127.0.0.1 9090 gperov test s3://warehouse/gperov/test/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json
$HMS_DIR/hive_metastore_client create-table 127.0.0.1 9090 empty empty s3://warehouse/empty/empty/metadata/00000-80089c7c-cfe3-4279-a864-ef65495ba43b.metadata.json
$HMS_DIR/hive_metastore_client create-table 127.0.0.1 9090 qa update_null s3://warehouse/qa_etl_table_update_null/metadata/00003-e4c28ff8-09d6-475a-b62c-7b9e5c4e694a.metadata.json
