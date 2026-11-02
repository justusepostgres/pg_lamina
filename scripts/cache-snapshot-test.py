#!/usr/bin/env python3
"""Exercise warmed metadata across a concurrent commit and snapshot boundaries."""
import json
import test_support as persistent


def count(client, expected_hit):
    profile = json.loads(client.query("SELECT analytics.profile_query('SELECT count(*) AS n FROM snapshot_a')"))
    assert profile['plan_cache_hit'] == expected_hit, 'unexpected plan-cache state across snapshot boundary'
    return profile['rows'][0]['n']


def main():
    reader=persistent.Postgres('pg_lamina_test')
    writer=persistent.Postgres('pg_lamina_test')
    try:
        writer.query("CREATE TABLE snapshot_source_a AS SELECT i::bigint AS id FROM generate_series(1,2) i")
        writer.query("CREATE TABLE snapshot_source_b AS SELECT i::bigint AS id FROM generate_series(1,5) i")
        writer.query("SELECT analytics.convert_table('snapshot_source_a','snapshot_a')")
        writer.query("SELECT analytics.convert_table('snapshot_source_b','snapshot_b')")
        reader.query('BEGIN ISOLATION LEVEL REPEATABLE READ')
        assert count(reader,False)==2
        assert count(reader,True)==2
        writer.query("UPDATE analytics.segments SET data=(SELECT data FROM analytics.segments WHERE table_id=(SELECT id FROM analytics.tables WHERE name='snapshot_b') AND ordinal=0) WHERE table_id=(SELECT id FROM analytics.tables WHERE name='snapshot_a') AND ordinal=0")
        assert count(reader,True)==2, 'repeatable-read snapshot lost'
        reader.query('COMMIT')
        assert count(reader,False)==5, 'metadata stale after concurrent commit'
        assert count(reader,True)==5
        writer.query("DELETE FROM analytics.tables WHERE name IN ('snapshot_a','snapshot_b')")
        print('Concurrent commit and repeatable-read metadata/plan-cache tests passed.')
    finally:
        reader.close()
        writer.close()


if __name__=='__main__':
    main()
