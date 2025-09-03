#!/bin/bash

for table_name in part supplier partsupp customer orders lineitem
do
    mkdir -p ${TPCH_STRUCTURED_DATA}/tpch.${table_name}
    for file_num in $(seq 0 $(($GEN_FILES_PER_TABLE - 1)))
    do
        from_file_path=${TPCH_DATA}/${table_name}${file_num}.parquet
        to_file_path=${TPCH_STRUCTURED_DATA}/tpch.${table_name}/${file_num}.parquet
        ln -s $from_file_path $to_file_path
    done
done
