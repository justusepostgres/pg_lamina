#!/usr/bin/env python3
"""Test the batch-local regex reuse with production Cargo artifacts, without linking PG."""
import json
import os
from pathlib import Path
import subprocess
from test_support import normal_driver_scheduling

normal_driver_scheduling()

ROOT = Path(__file__).resolve().parents[1]
env = os.environ.copy()
env.setdefault('PGRX_PG_CONFIG_PATH', '/opt/homebrew/opt/postgresql@17/bin/pg_config')
env.setdefault('LIBCLANG_PATH', '/Library/Developer/CommandLineTools/usr/lib')
result = subprocess.run(['cargo', 'build', '--release', '--locked', '-j', '4',
                         '--message-format=json'], cwd=ROOT, env=env,
                        stdout=subprocess.PIPE, text=True, check=True)
artifacts = {}
for line in result.stdout.splitlines():
    message = json.loads(line)
    if message['reason'] == 'compiler-artifact' and message['target']['name'] in ('datafusion',):
        artifacts[message['target']['name']] = next(p for p in message['filenames'] if p.endswith('.rlib'))
assert len(artifacts) == 1, artifacts
binary = ROOT/'.local/regex-reuse-tests'
binary.parent.mkdir(exist_ok=True)
command = ['rustc', '--edition=2021', '--test', 'src/regex_reuse.rs',
           '-L', 'dependency='+str(Path(artifacts['datafusion']).parent), '-o', str(binary)]
for name, path in artifacts.items():
    command.extend(['--extern', name+'='+path])
subprocess.run(command, cwd=ROOT, check=True)
subprocess.run([str(binary)], cwd=ROOT, check=True)
