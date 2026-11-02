#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PG_BIN=${PG_BIN:-/opt/homebrew/opt/postgresql@17/bin}
DATA="$ROOT/.local/pgdata"
SOCKET="$ROOT/.local/socket"
mkdir -p "$SOCKET"
chmod 700 "$SOCKET"
case "${1:-start}" in
  start)
    # Tool-launched macOS processes can inherit background scheduling.
    if [[ "$(uname -s)" == Darwin ]]; then taskpolicy -B -p "$$"; fi
    if [[ ! -f "$DATA/PG_VERSION" ]]; then
      "$PG_BIN/initdb" -D "$DATA" -U postgres --auth-local=trust --auth-host=reject --encoding=UTF8 --locale=C --data-checksums
      cat >> "$DATA/postgresql.conf" <<EOF
listen_addresses = ''
unix_socket_directories = '$SOCKET'
port = 55437
shared_buffers = '128MB'
max_parallel_workers_per_gather = 4
wal_level = replica
EOF
    fi
    if ! "$PG_BIN/pg_ctl" -D "$DATA" status >/dev/null 2>&1; then
      "$PG_BIN/pg_ctl" -D "$DATA" -l "$ROOT/.local/postgres.log" -w start
    fi
    if [[ "$(uname -s)" == Darwin ]]; then
      read -r local_postmaster_pid < "$DATA/postmaster.pid"
      taskpolicy -B -p "$local_postmaster_pid"
    fi
    ;;
  stop) "$PG_BIN/pg_ctl" -D "$DATA" -m fast -w stop ;;
  *) echo 'usage: dev-cluster.sh start|stop' >&2; exit 2 ;;
esac
