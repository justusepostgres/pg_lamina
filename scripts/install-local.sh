#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG_CONFIG=${PGRX_PG_CONFIG_PATH:-/opt/homebrew/opt/postgresql@17/bin/pg_config}
BUILD_TARGET=${CARGO_TARGET_DIR:-$ROOT/target}
LIB="$BUILD_TARGET/release/libpg_lamina.so"
SUFFIX=so
if [[ "$(uname -s)" == Darwin ]]; then LIB="$BUILD_TARGET/release/libpg_lamina.dylib"; SUFFIX=dylib; fi
install -m 755 "$LIB" "$("$PG_CONFIG" --pkglibdir)/pg_lamina.$SUFFIX"
install -m 644 "$ROOT/pg_lamina.control" "$("$PG_CONFIG" --sharedir)/extension/pg_lamina.control"
for sql in "$ROOT"/sql/pg_lamina--*.sql; do
  install -m 644 "$sql" "$("$PG_CONFIG" --sharedir)/extension/$(basename "$sql")"
done
