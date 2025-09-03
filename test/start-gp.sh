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

mkdir "$config_dir" "$master_dir" "${seg_dirs[@]}"
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

# Path for Greenplum mgmt utils and Greenplum binaries
PATH=$src_root/test/bin:$GPHOME/bin:$PATH
LD_LIBRARY_PATH=$GPHOME/lib:$LD_LIBRARY_PATH
export PATH
export LD_LIBRARY_PATH
export MASTER_DATA_DIRECTORY
export TRUSTED_SHELL
EOM
export MASTER_DATA_DIRECTORY="$master_dir/$seg_prefix-1"
gpinitsystem -a -D \
  -c "$init_config" \
  --lc-ctype="$locale" \
  --lc-collate="$locale" \
  --locale="$locale" && rc=$? || rc=$?
if [[ rc -ne 0 && rc -ne 1 ]]; then
  # checking for 1 ignores warnings like open file limit and localhost in /etc/hosts
  exit 1
fi
export PATH="$src_root/test/bin:$PATH"
gpconfig -c log_min_messages -v notice
gpstop -u
