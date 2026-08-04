#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

gp_root="${1:-/usr/local/greenplum-db}"
data_root="${2:-/gpdata}"
# locale and encoding should be compatible
locale="${3:-ru_RU.cp1251}"
encoding="${4:-WIN1251}"
num_segs="${NUM_SEGS:-1}"
src_root="$(cd "$(dirname -- "${BASH_SOURCE[0]}")"/.. && pwd)"

readonly seg_prefix=gpsne
readonly config_dir="$data_root/config"
readonly master_dir="$data_root/master"
declare -a seg_dirs
for i in $(seq 1 "$num_segs"); do
  seg_dirs+=("$data_root/s$i")
done

# Not following ...: does not exist
# shellcheck disable=SC1091
source "$gp_root"/greenplum_path.sh
export PATH="$GPHOME/bin:$src_root/test/bin:$PATH"
export LD_LIBRARY_PATH="$GPHOME/lib:$LD_LIBRARY_PATH"
export MASTER_DATA_DIRECTORY="" # Clear any stale setting
export GPSSH_ENABLE_EXPERIMENTAL="YES" 

mkdir -p "$config_dir" "$master_dir" "${seg_dirs[@]}"
readonly hostlist="$config_dir/hostlist"
echo "$HOSTNAME" >"$hostlist"
readonly init_config="$config_dir/gpinitsystem_config"
cat <<EOM >"$init_config"
ARRAY_NAME="Tea CI"
MACHINE_LIST_FILE=$hostlist
SEG_PREFIX=$seg_prefix
PORT_BASE=6000
declare -a DATA_DIRECTORY=(${seg_dirs[@]})
MASTER_HOSTNAME=$HOSTNAME
MASTER_DIRECTORY=$master_dir
MASTER_PORT=5432
TRUSTED_SHELL="$src_root/test/bin/ssh"
CHECK_POINT_SEGMENTS=8
ENCODING=$encoding
DATABASE_NAME=tea_ci
IP_ALLOW=0.0.0.0/0
EOM
export MASTER_DATA_DIRECTORY="$master_dir/$seg_prefix-1"
echo "Locale: $locale"
echo "Config: $init_config"
echo "GPHOME: $GPHOME"
echo "PATH: $PATH"
gpinitsystem -a -D -v \
  -c "$init_config" \
  --lc-ctype="$locale" \
  --lc-collate="$locale" \
  --locale="$locale"
rc=$?
if [[ $rc -ne 0 && $rc -ne 1 ]]; then
  echo "=== gpinitsystem FAILED ==="
  # Try to print the specific segment log if it exists
  if [[ -f "$HOME/gpAdminLogs/gpinitsystem_$(date +%Y%m%d).log" ]]; then
     echo "--- Last 50 lines of gpinitsystem log ---"
     tail -n 50 "$HOME/gpAdminLogs/gpinitsystem_$(date +%Y%m%d).log"
  fi
  exit 1
fi
gpconfig -c log_min_messages -v notice
gpstop -u
