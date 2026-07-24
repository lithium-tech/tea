SELECT COUNT(*) = 1 AS tea_extension_exists
FROM pg_extension
WHERE extname = 'tea';

SELECT extname, extversion IS NOT NULL AS has_version
FROM pg_extension
WHERE extname = 'tea';

CREATE FOREIGN TABLE tea_fdw_empty_probe (a bigint)
SERVER tea_server
OPTIONS (location 'tea://special://empty');

SELECT count(*)::int AS rows_read
FROM tea_fdw_empty_probe;

DO $$
DECLARE
  external_table_failed boolean := false;
BEGIN
  BEGIN
    CREATE READABLE EXTERNAL TABLE tea_ext_disabled_probe (a bigint)
    LOCATION ('tea://special://empty')
    FORMAT 'custom' (formatter = tea_import);
  EXCEPTION WHEN others THEN
    external_table_failed := true;
    RAISE NOTICE 'external table creation failed as expected';
  END;

  IF NOT external_table_failed THEN
    RAISE EXCEPTION 'external table creation unexpectedly succeeded';
  END IF;
END
$$;

DROP FOREIGN TABLE IF EXISTS tea_fdw_empty_probe;
