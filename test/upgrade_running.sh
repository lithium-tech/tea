#!/bin/bash

# Verifies that Tea upgrade while running a query doesn't produce a crash.
#
# Prerequisites:
# - A running local Greenplum cluster
# - Tea is not installed.

set -o errexit
set -o nounset
set -o pipefail

if [[ $# -ne 7 ]]; then
  echo "Usage: $0 gp_home base_package_path base_package_version upgrade_package_path upgrade_package_version gp_version" >&2
  exit 1
fi
GPHOME="$1"
base_package_path="$2"
base_package_version="$3"
upgrade_package_path="$4"
upgrade_package_version="$5"
tpch_dir="$6"
gp_version="$7"

src_root="$(cd "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"
export PGDATABASE=tea_ci
# shellcheck disable=SC1091
source "${GPHOME}/greenplum_path.sh"

cp "${src_root}/test/config/tea-config.json" "${GPHOME}/tea"

tar -C "$GPHOME" --strip-components=1 -xzf "${base_package_path}"

psql <<EOM
CREATE EXTENSION tea;

CREATE EXTERNAL TABLE lineitem (
    L_ORDERKEY      BIGINT,
    L_PARTKEY       INTEGER,
    L_SUPPKEY       INTEGER,
    L_LINENUMBER    INTEGER,
    L_QUANTITY      DECIMAL(12,2),
    L_EXTENDEDPRICE DECIMAL(12,2),
    L_DISCOUNT      DECIMAL(12,2),
    L_TAX           DECIMAL(12,2),
    L_RETURNFLAG    TEXT,
    L_LINESTATUS    TEXT,
    L_SHIPDATE      DATE,
    L_COMMITDATE    DATE,
    L_RECEIPTDATE   DATE,
    L_SHIPINSTRUCT  TEXT,
    L_SHIPMODE      TEXT,
    L_COMMENT       TEXT
  )
  LOCATION ('tea://file://${tpch_dir}/lineitem0.parquet')
  FORMAT 'custom' (formatter = tea_import)
  ENCODING 'UTF8';
EOM

# Run long-running query in background
psql <<EOM &
-- run long query during upgrade
SELECT count(*), pg_sleep(10) FROM lineitem
UNION ALL
SELECT NULL, pg_sleep(10)
UNION ALL
SELECT count(*), NULL FROM lineitem;

-- run another query to be executed after the upgrade
SELECT count(*), pg_sleep(10) FROM lineitem
UNION ALL
SELECT NULL, pg_sleep(10)
UNION ALL
SELECT count(*), NULL FROM lineitem;
EOM
bg_id=$!

tar -C "$GPHOME" --strip-components=1 -xzf "${upgrade_package_path}"

psql -c "ALTER EXTENSION tea UPDATE TO '${upgrade_package_version}';" >&2
psql -c "SELECT * FROM lineitem LIMIT 1;" >&2

if [[ $gp_version -eq 6 ]]; then
  psql -c "SELECT tea.external_table_location('public', 'lineitem');" >&2
  psql -c "SELECT * FROM tea.iceberg_get_metrics('tea://gperov.test');" >&2
  psql -c "SELECT * FROM tea.iceberg_tables_metrics WHERE location = 'tea://gperov.test';" >&2
fi

# Wait for the background query
wait $bg_id

# Downgrade to base version
psql -c "ALTER EXTENSION tea UPDATE TO '${base_package_version}';" >&2
psql -c "SELECT * FROM lineitem LIMIT 1;" >&2

# Upgrade back
psql -c "ALTER EXTENSION tea UPDATE TO '${upgrade_package_version}';" >&2
psql -c "SELECT * FROM lineitem LIMIT 1;" >&2

# Downgrade to base version (2)
psql -c "ALTER EXTENSION tea UPDATE TO '${base_package_version}';" >&2
psql -c "SELECT * FROM lineitem LIMIT 1;" >&2

# Upgrade back (2)
psql -c "ALTER EXTENSION tea UPDATE TO '${upgrade_package_version}';" >&2
psql -c "SELECT * FROM lineitem LIMIT 1;" >&2
