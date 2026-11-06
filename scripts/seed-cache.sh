#!/bin/sh
set -eu

script_dir=$(CDPATH= cd "$(dirname "$0")" && pwd)
project_root=$(CDPATH= cd "$script_dir/.." && pwd)
address=${1:-${NEBULA_ADDR:-127.0.0.1:7001}}
target_dir=${CARGO_TARGET_DIR:-target}

cd "$project_root"
cargo build --quiet -p nebula-client
client=$target_dir/debug/nebula-client

index=1
while [ "$index" -le 1000 ]; do
  key="test:seed:$index"
  value="cache-test-value-$index"
  "$client" --address "$address" set "$key" "$value"
  index=$((index + 1))
done

printf 'Inserted 1000 cache records into %s\n' "$address"
