\set ON_ERROR_STOP on
BEGIN;
CREATE TABLE schema_cache_source AS SELECT 7::bigint AS id;
CREATE TABLE schema_cache_changed AS SELECT 'changed'::text AS label;
SELECT analytics.convert_table('schema_cache_source', 'schema_cache_a');
SELECT analytics.convert_table('schema_cache_changed', 'schema_cache_b');
DO $$ BEGIN
  ASSERT (SELECT q->>'id' FROM analytics.query('SELECT * FROM schema_cache_a') q) = '7';
  ASSERT (SELECT q->>'id' FROM analytics.query('SELECT * FROM schema_cache_a') q) = '7';
END $$;
SAVEPOINT original_schema;
UPDATE analytics.segments SET data=(
  SELECT data FROM analytics.segments
  WHERE table_id=(SELECT id FROM analytics.tables WHERE name='schema_cache_b') AND ordinal=0)
WHERE table_id=(SELECT id FROM analytics.tables WHERE name='schema_cache_a') AND ordinal=0;
DO $$ BEGIN
  ASSERT (SELECT q->>'label' FROM analytics.query('SELECT label FROM schema_cache_a') q) = 'changed';
  ASSERT (SELECT q->>'label' FROM analytics.query('SELECT * FROM schema_cache_a') q) = 'changed';
END $$;
ROLLBACK TO original_schema;
SET LOCAL analytics.statistics_cache_mb=64;
DO $$ BEGIN
  ASSERT (SELECT q->>'id' FROM analytics.query('SELECT id FROM schema_cache_a') q) = '7';
END $$;
SET LOCAL analytics.statistics_cache_mb=1;
DO $$ BEGIN
  ASSERT (SELECT q->>'id' FROM analytics.query('SELECT id FROM schema_cache_a') q) = '7';
END $$;
DO $$ DECLARE profile jsonb; BEGIN
  PERFORM analytics.profile_query('SELECT count(*) AS n FROM schema_cache_a');
  profile := analytics.profile_query('SELECT count(*) AS n FROM schema_cache_a');
  ASSERT (profile->'rows'->0->>'n')::bigint = 1;
  ASSERT (profile->>'read_bytes')::bigint = 0, 'Warm count reread segment bytes';
  ASSERT (profile->>'read_requests')::bigint = 0, 'Warm count reread segment ranges';
END $$;
SET LOCAL analytics.metadata_cache=off;
DO $$ BEGIN
  ASSERT (SELECT q->>'id' FROM analytics.query('SELECT * FROM schema_cache_a') q) = '7';
  ASSERT (analytics.profile_query('SELECT count(*) AS n FROM schema_cache_a')->>'read_bytes')::bigint > 0;
END $$;
ROLLBACK;
