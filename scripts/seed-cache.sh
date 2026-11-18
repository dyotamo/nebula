#!/bin/sh
set -eu

# The CLI inherits the token from the environment; keep it out of process arguments.
: "${NEBULA_TOKEN:?NEBULA_TOKEN must be set}"

# Resolve paths from this script rather than the caller's directory; ignore shell CDPATH output.
script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
project_root=$(CDPATH= cd "$script_dir/.." && pwd)
address=${NEBULA_ADDRESS:-127.0.0.1:17171}
target_dir=${CARGO_TARGET_DIR:-target}
total_records=10000
# Each CLI invocation opens its own authenticated connection; bound concurrent processes/sockets.
batch_limit=100

# Generate independent 128-bit suffixes to avoid collisions with earlier seeding runs.
random_hex() {
  od -An -N16 -tx1 /dev/urandom | tr -d ' \n'
}

# Build once before launching batches so child processes only perform cache operations.
cd "$project_root"
cargo build --quiet -p nebula-client
client=$target_dir/debug/nebula

# A failed batch stops further insertion under set -e after every tracked child has been reaped.
wait_batch() {
  # Reap every child even after a failure; bare wait would lose individual exit statuses.
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
  # Passing a CA opts each connection into TLS verification; otherwise use the normal CLI transport.
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

# Drain any partial final batch before reporting the requested total as successfully inserted.
if [ -n "$batch_pids" ]; then
  wait_batch
fi

printf 'Inserted %s cache records into %s\n' "$total_records" "$address"
