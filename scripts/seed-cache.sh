#!/bin/sh
set -eu

: "${NEBULA_TOKEN:?NEBULA_TOKEN must be set}"

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
project_root=$(CDPATH= cd "$script_dir/.." && pwd)
address=${NEBULA_ADDRESS:-127.0.0.1:17171}
target_dir=${CARGO_TARGET_DIR:-target}
total_records=10000
batch_limit=100

random_hex() {
  od -An -N16 -tx1 /dev/urandom | tr -d ' \n'
}

cd "$project_root"
cargo build --quiet -p nebula-client
client=$target_dir/debug/nebula

wait_batch() {
  batch_failed=0
  for pid in $batch_pids; do
    if ! wait "$pid"; then
      batch_failed=1
    fi
  done
  batch_pids=
  if [ "$batch_failed" -ne 0 ]; then
    printf 'One or more cache records failed in this batch\n' >&2
    return 1
  fi
}

index=0
batch_size=0
batch_pids=
while [ "$index" -lt "$total_records" ]; do
  key="test:seed:$(random_hex)"
  value="cache-test-value-$(random_hex)"
  if [ -n "${NEBULA_TLS_CA:-}" ]; then
    "$client" --address "$address" --tls-ca "$NEBULA_TLS_CA" set "$key" "$value" &
  else
    "$client" --address "$address" set "$key" "$value" &
  fi
  batch_pids="$batch_pids $!"
  index=$((index + 1))
  batch_size=$((batch_size + 1))
  if [ "$batch_size" -eq "$batch_limit" ]; then
    wait_batch
    batch_size=0
  fi
done

if [ -n "$batch_pids" ]; then
  wait_batch
fi

printf 'Inserted %s cache records into %s\n' "$total_records" "$address"
