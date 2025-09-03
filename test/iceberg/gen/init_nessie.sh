#!/bin/bash

# expected variables:
# CI_PROJECT_DIR

nohup java -jar -Dquarkus.log.level=DEBUG -Dquarkus.management.port=9100 /nessie/nessie-quarkus-0.103.0-runner.jar > nessie.log 2>&1 &
sleep 20
cat nessie.log
python3 $CI_PROJECT_DIR/test/iceberg/gen/upload_table.py gperov test s3://warehouse/gperov/test/metadata/00003-ca406d8e-6c7b-4672-87ff-bfd76f84f949.metadata.json
python3 $CI_PROJECT_DIR/test/iceberg/gen/upload_table.py empty empty s3://warehouse/empty/empty/metadata/00000-80089c7c-cfe3-4279-a864-ef65495ba43b.metadata.json
python3 $CI_PROJECT_DIR/test/iceberg/gen/upload_table.py qa update_null s3://warehouse/qa_etl_table_update_null/metadata/00003-e4c28ff8-09d6-475a-b62c-7b9e5c4e694a.metadata.json
