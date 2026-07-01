#!/usr/bin/env bash
# tea relies on the OPENGPDB macro (defined in pg_config_manual.h) to select
# the open-gpdb sampling API; make sure the freshly built gpdb defines it.
set -ex

grep -q '#define OPENGPDB' \
  "$HOME/local/gpdb/include/postgresql/server/pg_config_manual.h"
