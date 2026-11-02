\set ON_ERROR_STOP on
LOAD 'pg_lamina';
CREATE TABLE late_source AS
SELECT (timestamp '2020-01-01' + ((i*73)%5000)*interval '1 second') AS eventtime,
       i::bigint AS watchid,
       CASE WHEN i%7=0 THEN NULL WHEN i%3=0 THEN 'google/東京' ELSE 'other' END AS url,
       CASE WHEN i%11=0 THEN NULL ELSE repeat('payload-',30)||i END AS payload
FROM generate_series(1,5000) i;
CREATE TABLE late_source_b AS SELECT eventtime,watchid,url,'changed'::text AS payload FROM late_source;
SELECT analytics.convert_table('late_source','late_test');
SELECT analytics.convert_table('late_source_b','late_test_b');
DO $$
DECLARE q text; expected jsonb; actual jsonb; warm jsonb;
BEGIN
    IF current_setting('analytics.late_materialization')<>'off' THEN RAISE EXCEPTION 'late fetch must default off'; END IF;
    FOREACH q IN ARRAY ARRAY[
        'SELECT * FROM late_test WHERE url LIKE ''%google%'' ORDER BY eventtime,watchid LIMIT 10',
        'SELECT * FROM late_test WHERE watchid>7 ORDER BY eventtime DESC,watchid DESC LIMIT 3',
        'SELECT * FROM late_test WHERE watchid<0 ORDER BY eventtime,watchid LIMIT 1'
    ] LOOP
        PERFORM set_config('analytics.late_materialization','off',true);
        expected:=analytics.profile_query(q);
        PERFORM set_config('analytics.late_materialization','on',true);
        actual:=analytics.profile_query(q);
        warm:=analytics.profile_query(q);
        IF NOT (actual->>'late_materialization')::boolean OR actual->'rows'<>expected->'rows' OR warm->'rows'<>expected->'rows' THEN
            RAISE EXCEPTION 'late result mismatch: %',q;
        END IF;
        IF (actual->>'plan_cache_hit')::boolean OR NOT (warm->>'plan_cache_hit')::boolean THEN
            RAISE EXCEPTION 'late setting missing from plan cache key';
        END IF;
    END LOOP;
    FOREACH q IN ARRAY ARRAY[
        'SELECT watchid FROM late_test ORDER BY watchid LIMIT 3',
        'SELECT * FROM late_test ORDER BY eventtime,watchid LIMIT 3 OFFSET 1',
        'SELECT count(*) FROM late_test'
    ] LOOP
        actual:=analytics.profile_query(q);
        IF (actual->>'late_materialization')::boolean THEN RAISE EXCEPTION 'unsupported plan used late fetch'; END IF;
    END LOOP;
END $$;
SET analytics.late_materialization=on;
CREATE TEMP TABLE late_before AS SELECT analytics.profile_query('SELECT * FROM late_test ORDER BY eventtime,watchid LIMIT 10')->'rows' AS rows;
BEGIN;
UPDATE analytics.segments SET data=(SELECT data FROM analytics.segments WHERE table_id=(SELECT id FROM analytics.tables WHERE name='late_test_b') AND ordinal=0)
WHERE table_id=(SELECT id FROM analytics.tables WHERE name='late_test') AND ordinal=0;
DO $$ BEGIN
    IF analytics.profile_query('SELECT * FROM late_test ORDER BY eventtime,watchid LIMIT 10')->'rows'=(SELECT rows FROM late_before) THEN
        RAISE EXCEPTION 'late fetch reused replaced data';
    END IF;
END $$;
ROLLBACK;
DO $$ BEGIN
    IF analytics.profile_query('SELECT * FROM late_test ORDER BY eventtime,watchid LIMIT 10')->'rows'<>(SELECT rows FROM late_before) THEN
        RAISE EXCEPTION 'late fetch did not observe rollback';
    END IF;
END $$;
DELETE FROM analytics.tables WHERE name IN ('late_test','late_test_b');
RESET analytics.late_materialization;
