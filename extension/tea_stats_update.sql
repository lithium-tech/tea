CREATE OR REPLACE FUNCTION tea_get_stats_from_iceberg(oid, text)
  RETURNS SETOF iceberg_stats
  AS 'MODULE_PATHNAME', 'tea_get_stats_from_iceberg'
LANGUAGE C STRICT;
