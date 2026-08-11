-- start_ignore
CREATE EXTENSION IF NOT EXISTS tea;
-- end_ignore

CREATE READABLE EXTERNAL TABLE tea_ext_empty_probe (a bigint)
LOCATION ('tea://special://empty')
FORMAT 'custom' (formatter = tea_import) ENCODING 'UTF8';

SELECT count(*)::int AS rows_read
FROM tea_ext_empty_probe;

DROP EXTERNAL TABLE tea_ext_empty_probe;

DROP EXTENSION tea;
