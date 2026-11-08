#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
project_root=$(CDPATH= cd "$script_dir/.." && pwd)
address=${1:-${NEBULA_ADDR:-127.0.0.1:7001}}
tls_ca=${2:-${NEBULA_TLS_CA:-}}
target_dir=${CARGO_TARGET_DIR:-target}

cd "$project_root"
cargo build --quiet -p nebula-client
client=$target_dir/debug/nebula

index=1
while [ "$index" -le 1000 ]; do
  key="test:seed:$index"
  value="cache-test-value-$index"
  if [ -n "$tls_ca" ]; then
    "$client" --address "$address" --tls-ca "$tls_ca" set "$key" "$value"
  else
    "$client" --address "$address" set "$key" "$value"
  fi
  index=$((index + 1))
done

printf 'Inserted 1000 cache records into %s\n' "$address"
