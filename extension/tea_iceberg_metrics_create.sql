-- GP does not support `IF NOT EXISTS` for external tables, so we must ensure that we drop table on downgrade`
CREATE EXTERNAL TABLE iceberg_tables_metrics(
  location text,
  total_records int8,
  total_data_files int8,
  total_files_size int8,
  total_equality_deletes int8,
  total_position_deletes int8,
  total_delete_files int8
) LOCATION ('tea://special://iceberg_tables_metrics?profile=tea_iceberg_get_metrics')
FORMAT 'custom' (formatter = tea_import)
ENCODING 'UTF8';

CREATE OR REPLACE FUNCTION tea_iceberg_get_metrics(
  IN location text,
  OUT total_records int8,
  OUT total_data_files int8,
  OUT total_files_size int8,
  OUT total_equality_deletes int8,
  OUT total_position_deletes int8,
  OUT total_delete_files int8
) RETURNS record
    LANGUAGE 'plpgsql'
    VOLATILE
  AS
$BODY$
BEGIN
  IF location IS NULL THEN
    RAISE EXCEPTION 'tea_iceberg_get_metrics: location must be not null';
  END IF;
  EXECUTE ('SELECT iceberg_tables_metrics.total_records, iceberg_tables_metrics.total_data_files, iceberg_tables_metrics.total_files_size, iceberg_tables_metrics.total_equality_deletes, iceberg_tables_metrics.total_position_deletes, iceberg_tables_metrics.total_delete_files FROM iceberg_tables_metrics WHERE location = ''' || location || '''') INTO
  total_records, total_data_files, total_files_size, total_equality_deletes, total_position_deletes, total_delete_files;
END;
$BODY$;

CREATE OR REPLACE FUNCTION tea_external_table_location(schemaname text, tablename text)
  RETURNS text
AS
$$
  SELECT
    trim(BOTH '{}' FROM urilocation::varchar)
  FROM
      pg_exttable a,
      pg_class b,
      pg_namespace c
  WHERE
      c.nspname = $1
      and b.relname = $2
      and b.relnamespace = c.oid
      and a.reloid = b.oid
      and urilocation::varchar like '%tea://%'
$$ LANGUAGE SQL;
