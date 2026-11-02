"""Shared helpers for the dedicated local PostgreSQL regression tests."""
import os
from pathlib import Path
import platform
import subprocess

ROOT = Path(__file__).resolve().parents[1]
PG_BIN = Path(os.environ.get("PG_BIN", "/opt/homebrew/opt/postgresql@17/bin"))
ENV = dict(os.environ, PGHOST=str(ROOT / ".local/socket"), PGPORT="55437", PGUSER="postgres")

def literal(value):
    return "'" + value.replace("'", "''") + "'"

def normal_driver_scheduling():
    if platform.system() == 'Darwin':
        subprocess.run(['taskpolicy', '-B', '-p', str(os.getpid())], check=True)

class Postgres:
    def __init__(self, database='pg_lamina_test'):
        self.process = subprocess.Popen(
            [str(PG_BIN/'psql'), '-XAtq', '-v', 'ON_ERROR_STOP=1', '-d', database],
            env=ENV, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True, bufsize=1)

    def query(self, statement):
        self.process.stdin.write(statement.rstrip(';') + ';\n\\echo __PG_ANALYTICS_DONE__\n')
        self.process.stdin.flush()
        lines = []
        while True:
            line = self.process.stdout.readline()
            if not line:
                raise RuntimeError('psql exited during query')
            if line.strip() == '__PG_ANALYTICS_DONE__':
                return ''.join(lines).strip()
            lines.append(line)

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=10)
