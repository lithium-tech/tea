DROP FUNCTION IF EXISTS tea_external_table_location(schemaname text, tablename text);

DROP FUNCTION IF EXISTS tea_iceberg_get_metrics(
  IN location text,
  OUT total_records int8,
  OUT total_data_files int8,
  OUT total_files_size int8,
  OUT total_equality_deletes int8,
  OUT total_position_deletes int8,
  OUT total_delete_files int8
);

DROP EXTERNAL TABLE IF EXISTS iceberg_tables_metrics;
