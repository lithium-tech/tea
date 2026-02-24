<div align=center>

[![Apache 2.0 License](https://img.shields.io/badge/license-Apache%202.0-blueviolet?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)

</div>

Tea is an open-source extension for Greenplum Database that allows it to access Apache Iceberg™ data from S3-compatible storage written in Apache Parquet format.
Tea adopts [PXF](https://github.com/greenplum-db/pxf-archive) logic to standard PostgreSQL/Greenplum interfaces such as
[Foreign data](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_FOREIGN_TABLE.html) and
[External tables](https://techdocs.broadcom.com/us/en/vmware-tanzu/data-solutions/tanzu-greenplum/6/greenplum-database/ref_guide-sql_commands-CREATE_EXTERNAL_TABLE.html)

## Restrictions

Greenplum 5 is based on PostgreSQL that has no `FOREIGN TABLE` support so you can use Tea with `EXTERNAL TABLE` only for them.
Greenplum 6 has restricted support of `FOREIGN TABLE`. Especially you cannot use [GP Orca](https://www.vmware.com/docs/white-paper-orca-a-modular-query-optimizer-architecture-for-big-data) with them.
`EXTERNAL TABLE` has no separate qusery coordinator. Such tables require special tasks-coordination logic enabled in config (we call it `samovar`) and separate Redis or Valkey installation its work.
`Samovar` is also used for work-stealing between Greenplum segments.

## Install

Extract Tea from tea-{platform}-{version}.tar.gz into Greenplum home directory.
```
export GPHOME=/path/to/greenplum
tar xzf tea-linux-1.70.0.tar.gz --strip-components=1 -C $GPHOME
```

Edit $GPHOME/tea/tea-config.json. At least you have to set access and secret keys to object storage and address for Iceberg catalog.
```
{
    "common": {
        "s3": {
            "access_key": "minioadmin",
            "secret_key": "minioadmin",
            "endpoint_override": "127.0.0.1:9000",
            "scheme": "http"
        },
        "catalog": {
            "type" : "hms",
            "hms": "127.0.0.1:9083",
            "rest": "127.0.0.1:19120"
        }
    }
}
```
You are able to set additional profiles sections that override `common` settings.
More configuration fields you can find in [tea-config.json](test/config/tea-config.json) and [tea-config-schema.json](test/config/tea-config-schema.json).

## How to use

To access data from Apache Iceberg you should register `tea` extention and create `EXTERNAL TABLE` or `FOREIGN TABLE` to every Iceberg table you want to read.

#### External tables

```
CREATE EXTENSION tea;

CREATE READABLE EXTERNAL TABLE table_name (...)
LOCATION ('tea://iceberg_namespace.iceberg_table')
FORMAT 'custom' (formatter = tea_import);
```
It creates an `EXTERNAL TABLE` linked to an Iceberg table `iceberg_namespace.iceberg_table` declared in Iceberg catalog.
Created table is accessible for reading.

#### FDW
```
CREATE EXTENSION tea;
CREATE SERVER tea_server FOREIGN DATA WRAPPER tea_fdw;

CREATE FOREIGN TABLE table_name (...)
SERVER tea_server
OPTIONS(location 'tea://iceberg_namespace.iceberg_table');
```
It creates a `FOREIGN TABLE` linked to an Iceberg table `iceberg_namespace.iceberg_table` declared in Iceberg catalog.
Created table is accessible for reading.

## Update Tea

Extract a new version
```
export GPHOME=/path/to/greenplum
tar xzf tea-linux-1.71.0.tar.gz --strip-components=1 -C $GPHOME
```
Update extension in PostgreSQL's way
```
ALTER EXTENSION tea UPDATE TO 1.71.0;
SELECT installed_version FROM pg_available_extensions WHERE name='tea';
```
Downgrade extension if needed
```
ALTER EXTENSION tea UPDATE TO 1.70.0;
```

## How To buld (Linux, macOS)

TODO
