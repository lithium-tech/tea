#!/usr/bin/env bash
# Deploy the tea config into $GPHOME/tea.
#
# Usage: deploy-tea-config.sh [config-file]
#
# Defaults to test/config/tea-config.json (smoke tests); the REST test passes
# test/config/tea-config-rest.json.
set -eo pipefail

config="${1:-test/config/tea-config.json}"

source "$HOME/local/gpdb/greenplum_path.sh"
mkdir -p "$GPHOME/tea"
cp "$config" "$GPHOME/tea/tea-config.json"
cp test/config/tea-config-schema.json "$GPHOME/tea/tea-config-schema.json"
