CREATE OR REPLACE FUNCTION pg_catalog.tea_read() RETURNS integer
AS 'MODULE_PATHNAME', 'teaprotocol_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.tea_write() RETURNS integer
AS 'MODULE_PATHNAME', 'teaprotocol_export'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.tea_validate() RETURNS void
AS 'MODULE_PATHNAME', 'teaprotocol_validate_urls'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.tea_import() RETURNS record
AS 'MODULE_PATHNAME', 'teaformat_import'
LANGUAGE C STABLE;

CREATE OR REPLACE FUNCTION pg_catalog.tea_export(record) RETURNS bytea
AS 'MODULE_PATHNAME', 'teaformat_export'
LANGUAGE C STABLE;
