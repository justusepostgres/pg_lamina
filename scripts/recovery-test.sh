#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG_BIN=${PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}
export PGHOST="$ROOT/.local/socket" PGPORT=55437 PGUSER=postgres
PSQL=("$PG_BIN/psql" -XAtq -v ON_ERROR_STOP=1 -d pg_lamina_test)

expect_four() {
  local result
  result=$("${PSQL[@]}" -c "SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM events') q")
  [[ "$result" == 4 ]] || { echo "Unexpected recovered count: $result" >&2; exit 1; }
}

expect_sorted() {
  local result
  result=$("${PSQL[@]}" -c "SELECT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM sorted_events') q)='600000' AND (SELECT string_agg(q->>'plan',' ') LIKE '%SortPreservingMergeExec:%' AND string_agg(q->>'plan',' ') NOT LIKE '%SortExec:%' FROM analytics.query('EXPLAIN SELECT k,label FROM sorted_events ORDER BY k ASC NULLS LAST,label ASC NULLS LAST LIMIT 10') q)")
  [[ "$result" == t ]] || { echo "Sorted metadata did not survive recovery" >&2; exit 1; }
}

expect_four
expect_sorted
# Hold a completed conversion uncommitted, then crash. Recovery must retain the
# original heap and discard its uncommitted analytical representation.
"${PSQL[@]}" -c "CREATE TABLE crash_source(id bigint); INSERT INTO crash_source VALUES(42);"
PGAPPNAME=pg_lamina_recovery_writer "${PSQL[@]}" -c "BEGIN; SELECT analytics.convert_table('crash_source','crash_uncommitted'); SELECT pg_sleep(60); COMMIT;" > "$ROOT/.local/crash-writer.log" 2>&1 &
WRITER_PID=$!
for attempt in $(seq 1 100); do
  ready=$("${PSQL[@]}" -c "SELECT EXISTS (SELECT FROM pg_stat_activity WHERE application_name='pg_lamina_recovery_writer' AND wait_event='PgSleep')")
  [[ "$ready" == t ]] && break
  sleep 0.1
done
if [[ "$ready" != t ]]; then
  kill "$WRITER_PID" 2>/dev/null || true
  echo 'Uncommitted conversion did not reach crash checkpoint' >&2
  exit 1
fi
# Force WAL crash recovery, not a clean shutdown/checkpoint.
"$PG_BIN/pg_ctl" -D "$ROOT/.local/pgdata" -m immediate -w stop
wait "$WRITER_PID" || true
bash "$ROOT/scripts/dev-cluster.sh" start
expect_four
expect_sorted
result=$("${PSQL[@]}" -c "SELECT (SELECT count(*) FROM crash_source)=1 AND NOT EXISTS(SELECT FROM analytics.tables WHERE name='crash_uncommitted')")
[[ "$result" == t ]]
echo 'Crash recovery passed'

# Logical dump must include extension-owned storage/catalog data.
"$PG_BIN/pg_dump" -Fc pg_lamina_test > "$ROOT/.local/analytics.dump"
"$PG_BIN/dropdb" --if-exists pg_lamina_restore
"$PG_BIN/createdb" pg_lamina_restore
"$PG_BIN/pg_restore" --exit-on-error -d pg_lamina_restore "$ROOT/.local/analytics.dump"
PSQL=("$PG_BIN/psql" -XAtq -v ON_ERROR_STOP=1 -d pg_lamina_restore)
expect_four
expect_sorted
# Verify the identity sequence also survived the logical restore.
"${PSQL[@]}" -c "CREATE TABLE after_restore(id bigint); INSERT INTO after_restore VALUES(99); SELECT analytics.convert_table('after_restore','after_restore');"
echo 'Logical backup/restore passed'

# An isolated physical standby tests WAL transport and promotion without Kubernetes.
REPLICA=$(mktemp -d "$ROOT/.local/replica.XXXXXX")
REPLICA_SOCKET=$(mktemp -d "$ROOT/.local/replica-socket.XXXXXX")
cleanup() { "$PG_BIN/pg_ctl" -D "$REPLICA" -m fast -w stop >/dev/null 2>&1 || true; }
trap cleanup EXIT
"$PG_BIN/pg_basebackup" -D "$REPLICA" -X stream -R --checkpoint=fast
"$PG_BIN/pg_ctl" -D "$REPLICA" -l "$REPLICA/server.log" -o "-p 55438 -k $REPLICA_SOCKET" -w start
PSQL=("$PG_BIN/psql" -h "$REPLICA_SOCKET" -p 55438 -XAtq -v ON_ERROR_STOP=1 -d pg_lamina_test)
expect_four
expect_sorted
"$PG_BIN/psql" -XAtq -v ON_ERROR_STOP=1 -d pg_lamina_test -c "CREATE TABLE after_backup(id bigint); INSERT INTO after_backup VALUES(73); SELECT analytics.convert_table('after_backup','after_backup');"
LSN=$("$PG_BIN/psql" -XAtq -d pg_lamina_test -c 'SELECT pg_current_wal_flush_lsn()')
for attempt in $(seq 1 100); do
  caught_up=$("${PSQL[@]}" -c "SELECT coalesce(pg_last_wal_replay_lsn() >= '$LSN'::pg_lsn, false)")
  [[ "$caught_up" == t ]] && break
  sleep 0.1
done
[[ "$caught_up" == t ]] || { echo 'Standby did not catch up' >&2; exit 1; }
result=$("${PSQL[@]}" -c "SELECT q->>'id' FROM analytics.query('SELECT id FROM after_backup') q")
[[ "$result" == 73 ]]
"$PG_BIN/pg_ctl" -D "$REPLICA" -w promote
expect_four
expect_sorted
"${PSQL[@]}" -c "CREATE TABLE after_promotion(id bigint); INSERT INTO after_promotion VALUES(74); SELECT analytics.convert_table('after_promotion','after_promotion');"
echo "Physical replication and promotion passed; stopped replica retained at $REPLICA"
