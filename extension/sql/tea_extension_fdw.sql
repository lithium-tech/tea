-- start_ignore
CREATE EXTENSION IF NOT EXISTS tea;
-- end_ignore

CREATE FOREIGN TABLE tea_fdw_empty_probe (a bigint)
SERVER tea_server
OPTIONS (location 'tea://special://empty');

SELECT count(*)::int AS rows_read
FROM tea_fdw_empty_probe;

DROP FOREIGN TABLE tea_fdw_empty_probe;

DROP EXTENSION tea;
