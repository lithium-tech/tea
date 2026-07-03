CREATE OR REPLACE FUNCTION tea_fdw_get_create_query(text, text)
  RETURNS cstring
  AS 'MODULE_PATHNAME'
LANGUAGE C;
