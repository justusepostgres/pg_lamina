\set ON_ERROR_STOP on
BEGIN;
SET LOCAL analytics.plan_cache=on;
CREATE TABLE plan_cache_original AS SELECT 7::bigint AS id;
CREATE TABLE plan_cache_replacement AS SELECT 8::bigint AS id;
CREATE TABLE plan_cache_new_schema AS SELECT 'changed'::text AS label;
SELECT analytics.convert_table('plan_cache_original', 'plan_cache_a');
SELECT analytics.convert_table('plan_cache_replacement', 'plan_cache_b');
SELECT analytics.convert_table('plan_cache_new_schema', 'plan_cache_c');

CREATE FUNCTION pg_temp.check_cached_plan(sql text, expected jsonb, hit bool)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE p jsonb;
BEGIN
  p := analytics.profile_query(sql);
  ASSERT p->'rows' = expected, 'unexpected analytical result';
  ASSERT (p->>'plan_cache_hit')::bool = hit, 'unexpected plan-cache hit state';
  ASSERT (p->>'plan_cache_bytes')::bigint BETWEEN 0 AND 1048576;
  IF hit THEN ASSERT (p->>'plan_cache_bytes')::bigint > 0; END IF;
END $$;

SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);
SAVEPOINT original_data;
UPDATE analytics.segments SET data=(SELECT data FROM analytics.segments
 WHERE table_id=(SELECT id FROM analytics.tables WHERE name='plan_cache_b') AND ordinal=0)
 WHERE table_id=(SELECT id FROM analytics.tables WHERE name='plan_cache_a') AND ordinal=0;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":8}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":8}]', true);
ROLLBACK TO original_data;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);
UPDATE analytics.segments SET data=(SELECT data FROM analytics.segments
 WHERE table_id=(SELECT id FROM analytics.tables WHERE name='plan_cache_c') AND ordinal=0)
 WHERE table_id=(SELECT id FROM analytics.tables WHERE name='plan_cache_a') AND ordinal=0;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"label":"changed"}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"label":"changed"}]', true);
DO $$ BEGIN
  BEGIN
    PERFORM analytics.profile_query('SELECT id FROM plan_cache_a');
    RAISE EXCEPTION 'removed column unexpectedly resolved';
  EXCEPTION WHEN OTHERS THEN
    ASSERT SQLERRM LIKE '%No field named id%', SQLERRM;
  END;
END $$;
ROLLBACK TO original_data;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);

SET LOCAL analytics.memory_limit_mb=128;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);
SET LOCAL analytics.memory_limit_mb=256;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SET LOCAL analytics.parquet_filter_pushdown=off;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);
SET LOCAL analytics.parquet_filter_pushdown=on;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SET LOCAL analytics.regex_reuse=off;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SET LOCAL analytics.regex_reuse=on;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);

SET LOCAL analytics.plan_cache=off;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
DO $$ BEGIN
 ASSERT (analytics.profile_query('SELECT * FROM plan_cache_a')->>'plan_cache_bytes')::bigint=0;
END $$;
SET LOCAL analytics.plan_cache=on;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);
SET LOCAL analytics.metadata_cache=off;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SET LOCAL analytics.metadata_cache=on;
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', false);
SELECT pg_temp.check_cached_plan('SELECT * FROM plan_cache_a', '[{"id":7}]', true);

DO $$ DECLARE sql text; first jsonb; second jsonb; BEGIN
 FOREACH sql IN ARRAY ARRAY[
   'SELECT now() AS value', 'SELECT random() AS value',
   'WITH t AS (SELECT now() AS value) SELECT * FROM t',
   'SELECT sum(random()) AS value FROM plan_cache_a'
 ] LOOP
   first := analytics.profile_query(sql);
   PERFORM pg_sleep(0.002);
   second := analytics.profile_query(sql);
   ASSERT NOT (first->>'plan_cache_hit')::bool;
   ASSERT NOT (second->>'plan_cache_hit')::bool;
   ASSERT first->'rows' <> second->'rows';
   ASSERT (second->>'plan_cache_bytes')::bigint=0;
 END LOOP;
 sql := 'SELECT length('||quote_literal(repeat('x',1048576))||') AS n';
 FOR i IN 1..2 LOOP
   second := analytics.profile_query(sql);
   ASSERT (second->'rows'->0->>'n')::int=1048576;
   ASSERT NOT (second->>'plan_cache_hit')::bool;
   ASSERT (second->>'plan_cache_bytes')::bigint=0;
 END LOOP;
END $$;
ROLLBACK;
