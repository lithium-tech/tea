#!/usr/bin/env bash
# Run Tea pg_regress tests from a Tea source checkout staged under gpcontrib.
#
# Usage: run-pg-regress.sh <gpdb-source-dir> [database] [tea_build_ext]
#
# tea_build_ext must match the TEA_BUILD_EXT value the runtime was built
# with (ON by default). When ON, tea_extension.out is swapped for the
# ext-enabled variant before running pg_regress, since the external-table
# probe in tea_extension.sql behaves differently once the tea external-table
# protocol is actually installed.
set -eo pipefail

gpdb_src="${1:?gpdb source directory is required}"
database="${2:-tea_ci}"
tea_build_ext="${3:-ON}"
repo_root="$(pwd)"
stage_dir="${gpdb_src}/gpcontrib/tea"
extension_dir="${stage_dir}/extension"
log_dir="${repo_root}/build-logs/details"
results_dir="${repo_root}/build-logs/pg_regress"
pg_config="${HOME}/local/gpdb/bin/pg_config"

mkdir -p "${log_dir}" "${results_dir}"

if [ ! -x "${pg_config}" ]; then
  echo "pg_config not found at ${pg_config}" >&2
  exit 1
fi

source "${HOME}/local/gpdb/greenplum_path.sh"

rm -rf "${stage_dir}"
mkdir -p "${stage_dir}"

# Place the Tea checkout under gpdb/gpcontrib/tea, excluding generated/heavy CI
# directories and the target gpdb checkout itself to avoid recursive copies.
tar \
  --exclude='./.git' \
  --exclude='./build' \
  --exclude='./build-logs' \
  --exclude='./ci-artifacts' \
  --exclude='./gpdb' \
  --exclude='./cmake-build-*' \
  --exclude='./out' \
  -cf - . | tar -xf - -C "${stage_dir}"

if [ "${tea_build_ext}" = "OFF" ]; then
  cp "${extension_dir}/expected/tea_extension_ext_disabled.out" \
     "${extension_dir}/expected/tea_extension_ext.out"
fi

set +e
make -C "${extension_dir}" installcheck \
  PG_CONFIG="${pg_config}" \
  PG_REGRESS_DB="${database}" \
  2>&1 | tee "${log_dir}/pg-regress-installcheck.log"
installcheck_status=${PIPESTATUS[0]}
set -e

{
  echo "=== pg_regress staged extension directory ==="
  echo "${extension_dir}"
  echo
  echo "=== pg_regress result files ==="
  find "${extension_dir}" -type f \
    \( -path '*/results/*' -o -name 'regression.out' -o -name 'regression.diffs' \) \
    -print -exec ls -l '{}' \;
} | tee "${log_dir}/pg-regress-results.log"

find "${extension_dir}" -type f \
  \( -path '*/results/*' -o -name 'regression.out' -o -name 'regression.diffs' \) \
  -exec cp --parents '{}' "${results_dir}/" \; || true

exit "${installcheck_status}"
