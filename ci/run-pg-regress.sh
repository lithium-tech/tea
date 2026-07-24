#!/usr/bin/env bash
# Run Tea pg_regress tests from a Tea source checkout staged under gpcontrib.
#
# Usage: run-pg-regress.sh <gpdb-source-dir> [database]
set -eo pipefail

gpdb_src="${1:?gpdb source directory is required}"
database="${2:-tea_ci}"
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

( cd "${extension_dir}" && \
  find . -type f \
    \( -path '*/results/*' -o -name 'regression.out' -o -name 'regression.diffs' \) \
    -exec cp --parents '{}' "${results_dir}/" \; ) || true

exit "${installcheck_status}"
