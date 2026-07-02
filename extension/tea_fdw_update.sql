CREATE OR REPLACE FUNCTION pg_catalog.tea_fdw_handler()
  RETURNS fdw_handler
  AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_catalog.tea_fdw_validator(text[], oid)
  RETURNS void
  AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION tea_fdw_get_create_query(text, text)
  RETURNS cstring
  AS 'MODULE_PATHNAME'
LANGUAGE C;
