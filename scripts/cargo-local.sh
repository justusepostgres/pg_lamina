#!/usr/bin/env bash
# Keep task-launched local builds at normal scheduling priority on macOS.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
if [[ "$(uname -s)" == Darwin ]]; then
  taskpolicy -B -p "$$"
  export PGRX_PG_CONFIG_PATH=${PGRX_PG_CONFIG_PATH:-/opt/homebrew/opt/postgresql@17/bin/pg_config}
  export LIBCLANG_PATH=${LIBCLANG_PATH:-/Library/Developer/CommandLineTools/usr/lib}
fi
cd "$ROOT"
exec cargo "$@"
