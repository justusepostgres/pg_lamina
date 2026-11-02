#!/usr/bin/env python3
"""Concurrent segment replacement while same-statement two-pass reads execute."""
import argparse,hashlib,json,subprocess,threading,time,platform,statistics
from pathlib import Path
from test_support import ROOT,PG_BIN,literal
import test_support as persistent
parser=argparse.ArgumentParser();parser.add_argument('--label');args=parser.parse_args()
assert args.label is None or (args.label and all(c.isalnum() or c in '-_' for c in args.label))
output=ROOT/'.local/test-results'/f'late-snapshot-{args.label}.json' if args.label else None
assert output is None or not output.exists()
if output is not None:output.parent.mkdir(parents=True,exist_ok=True)
reader=persistent.Postgres('pg_lamina_test');setup=persistent.Postgres('pg_lamina_test')
stop=threading.Event();ready=threading.Event();commits=[];errors=[];thread=None
names=['late_race_a','late_race_b','late_race_target']
query="SELECT * FROM late_race_target WHERE url LIKE '%google%' ORDER BY eventtime,watchid LIMIT 10"
def replace(client,source):
    return client.query("UPDATE analytics.segments dst SET data=src.data FROM analytics.segments src WHERE dst.table_id=(SELECT id FROM analytics.tables WHERE name='late_race_target') AND src.table_id=(SELECT id FROM analytics.tables WHERE name="+literal(source)+") AND src.ordinal=dst.ordinal")
def profile():
    return json.loads(reader.query("WITH started AS MATERIALIZED (SELECT extract(epoch FROM clock_timestamp()) AS t), run AS MATERIALIZED (SELECT t,analytics.profile_query("+literal(query)+") AS p FROM started) SELECT json_build_object('started',t,'finished',extract(epoch FROM clock_timestamp()),'profile',p) FROM run"))
def writer():
    client=persistent.Postgres('pg_lamina_test')
    try:
        # Test visibility, not crash durability: slow WAL flushes otherwise make
        # the commit timestamp bracket longer than a complete analytical read.
        client.query("SET statement_timeout='30s'; SET synchronous_commit=off")
        ready.set()
        deadline=time.monotonic()+300
        i=0
        while not stop.is_set():
            if time.monotonic()>=deadline:raise TimeoutError("writer exceeded 300-second stress-test deadline")
            source='late_race_b' if i%2==0 else 'late_race_a'
            client.query('BEGIN');replace(client,source)
            before=float(client.query('SELECT extract(epoch FROM clock_timestamp())'))
            client.query('COMMIT')
            after=float(client.query('SELECT extract(epoch FROM clock_timestamp())'))
            commits.append({'before':before,'after':after,'source':source})
            i+=1
            stop.wait(0.005)
    except Exception as error:errors.append(str(error));ready.set()
    finally:client.close()
report={'writer_synchronous_commit':'off (visibility stress only; not a durability test)','scope':'Concurrent commit stress through installed PostgreSQL two-pass path; server timestamps bracket commits and complete read statements','reads':[]}
try:
    setup.query("SET statement_timeout='120s'; CREATE TABLE late_race_source_a AS SELECT timestamp '2020-01-01'+i*interval '1 second' AS eventtime,i::bigint AS watchid,'google/'||i::text AS url,repeat('old-',50)||i::text AS payload FROM generate_series(1,1000000) i ORDER BY i")
    setup.query("CREATE TABLE late_race_source_b AS SELECT timestamp '2020-01-01'+(1000000-i)*interval '1 second' AS eventtime,i::bigint AS watchid,'google/'||i::text AS url,repeat('new-',70)||i::text AS payload FROM generate_series(1,1000000) i ORDER BY i DESC")
    setup.query('CREATE TABLE late_race_source_target AS TABLE late_race_source_a')
    for suffix in ['a','b','target']:setup.query(f"SELECT analytics.convert_table('late_race_source_{suffix}','late_race_{suffix}')")
    reader.query("LOAD 'pg_lamina'; SET statement_timeout='30s'; SET analytics.late_materialization=on; SET analytics.statistics_cache_mb=64")
    expected={}
    for source in ['late_race_a','late_race_b']:
        sql=query.replace('late_race_target',source)
        expected[source]=json.loads(reader.query('SELECT analytics.profile_query('+literal(sql)+')'))['rows']
    assert expected['late_race_a']!=expected['late_race_b']
    reader.query('BEGIN ISOLATION LEVEL REPEATABLE READ')
    frozen=profile();assert frozen['profile']['rows']==expected['late_race_a']
    thread=threading.Thread(target=writer);thread.start();assert ready.wait(10)
    for isolation,count in [('repeatable_read',20),('read_committed',40)]:
        if isolation=='read_committed':reader.query('COMMIT')
        for attempt in range(400):
            result=profile();p=result.pop('profile');assert p['late_materialization']
            if isolation=='repeatable_read':assert p['rows']==expected['late_race_a'],'repeatable-read snapshot changed'
            else:assert p['rows'] in expected.values(),'mixed-version two-pass result'
            result['version']=next(name for name,rows in expected.items() if p['rows']==rows)
            result['isolation']=isolation;report['reads'].append(result)
            assert not errors,errors
            overlaps=sum(any(r['started']<c['before']<=c['after']<r['finished'] for c in list(commits)) for r in report['reads'] if r['isolation']==isolation)
            if attempt+1>=count and overlaps>=3:break
        else:raise AssertionError(f'{isolation}: fewer than three overlapping statements after 400 reads')
    stop.set();thread.join(timeout=40);assert not thread.is_alive();assert not errors,errors
    for read in report['reads']:
        read['commits_fully_inside_statement']=sum(read['started']<commit['before']<=commit['after']<read['finished'] for commit in commits)
    report['overlapping_statements']={level:sum(r['commits_fully_inside_statement']>0 for r in report['reads'] if r['isolation']==level) for level in ['repeatable_read','read_committed']}
    assert all(n>=3 for n in report['overlapping_statements'].values()),report['overlapping_statements']
    report['commits']=commits;report['status']='passed'
    library=Path(subprocess.check_output([str(PG_BIN/'pg_config'),'--pkglibdir'],text=True).strip())/('pg_lamina.dylib' if platform.system()=='Darwin' else 'pg_lamina.so')
    report['library_sha256']=hashlib.sha256(library.read_bytes()).hexdigest()
    if output is not None:output.write_text(json.dumps(report,indent=2)+'\n')
    print(report['overlapping_statements'],len(commits),'commits')
finally:
    stop.set()
    if thread is not None:thread.join(timeout=40)
    durations=[r['finished']-r['started'] for r in report['reads']]
    brackets=[c['after']-c['before'] for c in commits]
    print({'reads':len(durations),'commits':len(commits),'median_read_seconds':statistics.median(durations) if durations else None,'median_commit_bracket_seconds':statistics.median(brackets) if brackets else None})
    reader.close()
    try:setup.query("DELETE FROM analytics.tables WHERE name IN ('late_race_a','late_race_b','late_race_target')")
    finally:setup.close()
