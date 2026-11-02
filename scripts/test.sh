#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG_BIN=${PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}
export PGHOST="$ROOT/.local/socket" PGPORT=55437 PGUSER=postgres
# Only this dedicated test database is recreated.
"$PG_BIN/dropdb" --if-exists pg_lamina_test
"$PG_BIN/createdb" pg_lamina_test
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/integration.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/sorted-conversion.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/column-compression.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/conversion-memory.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/prepared-reads.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/schema-cache.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/plan-cache.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/late-materialization.sql"
"$PG_BIN/psql" -X -d pg_lamina_test -f "$ROOT/tests/memory-budget.sql"
python3 "$ROOT/scripts/cache-snapshot-test.py"
python3 "$ROOT/scripts/test-late-snapshot.py"
