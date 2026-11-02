\set ON_ERROR_STOP on
CREATE EXTENSION pg_lamina VERSION '0.1.0';
CREATE TABLE original (id bigint, n integer, label text, value double precision, active boolean);
INSERT INTO original VALUES (1, 10, 'hello', 1.25, true), (2, NULL, 'quoted '' text', NULL, false), (3, -7, NULL, -2.5, NULL), (9223372036854775807, 3, '世界', 0, true);
SELECT analytics.convert_table('original', 'events');
ALTER EXTENSION pg_lamina UPDATE TO '0.2.0';
DO $$ BEGIN
  ASSERT to_regclass('original') IS NULL;
  ASSERT (SELECT row_count FROM analytics.tables WHERE name='events') = 4;
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM events') q) = '4';
  ASSERT (SELECT q->>'total' FROM analytics.query('SELECT sum(n) AS total FROM events') q) = '6';
  ASSERT (SELECT count(*) FROM analytics.query('EXPLAIN ANALYZE SELECT sum(n) FROM events')) > 0;
  ASSERT (SELECT q->>'id' FROM analytics.query('SELECT max(id) AS id FROM events') q) = '9223372036854775807';
  ASSERT (SELECT q->>'label' FROM analytics.query('SELECT label FROM events WHERE id=9223372036854775807') q) = '世界';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM events WHERE label IS NULL') q) = '1';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM events a JOIN events b ON a.id=b.id') q) = '4';
END $$;

-- Codec choice applies at conversion; existing Zstd and new LZ4 data coexist.
SET analytics.storage_compression='lz4';
CREATE TABLE codec_test_source(id bigint, label text);
INSERT INTO codec_test_source VALUES (1, '世界'), (2, NULL), (3, '');
SELECT analytics.convert_table('codec_test_source','codec_test');
RESET analytics.storage_compression;
DO $$ BEGIN
  ASSERT lower(current_setting('analytics.storage_compression')) = 'zstd';
  ASSERT (SELECT q->>'label' FROM analytics.query('SELECT label FROM codec_test WHERE id=1') q) = '世界';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM codec_test WHERE label IS NULL') q) = '1';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM codec_test WHERE label = ''''') q) = '1';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM events a JOIN codec_test b ON a.id=b.id') q) = '3';
END $$;

CREATE TABLE rollback_source (id bigint);
INSERT INTO rollback_source VALUES (42);
BEGIN;
SELECT analytics.convert_table('rollback_source', 'rolled_back');
ROLLBACK;
DO $$ BEGIN
  ASSERT (SELECT count(*) FROM rollback_source) = 1;
  ASSERT NOT EXISTS (SELECT FROM analytics.tables WHERE name='rolled_back');
END $$;

CREATE TABLE unsupported (exact numeric);
INSERT INTO unsupported VALUES (123456789.123456789);
DO $$ BEGIN
  BEGIN
    PERFORM analytics.convert_table('unsupported', 'bad_type');
    RAISE EXCEPTION 'expected unsupported type failure';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%unsupported PostgreSQL type%' THEN RAISE; END IF;
  END;
  ASSERT to_regclass('unsupported') IS NOT NULL;
  ASSERT NOT EXISTS (SELECT FROM analytics.tables WHERE name='bad_type');
END $$;

CREATE TABLE depended_on (id integer);
INSERT INTO depended_on VALUES (1);
CREATE VIEW dependent_view AS SELECT * FROM depended_on;
DO $$ BEGIN
  BEGIN
    PERFORM analytics.convert_table('depended_on', 'dependent');
    RAISE EXCEPTION 'expected dependency failure';
  EXCEPTION WHEN dependent_objects_still_exist THEN NULL;
  END;
  ASSERT (SELECT count(*) FROM dependent_view) = 1;
  ASSERT NOT EXISTS (SELECT FROM analytics.tables WHERE name='dependent');
END $$;

CREATE TABLE empty_source (id integer, label text);
SELECT analytics.convert_table('empty_source', 'empty_events');
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM empty_events') q) = '0';
END $$;

CREATE TABLE "odd table" ("a'b" text, "x""y" bigint);
INSERT INTO "odd table" VALUES ('safe', 9);
SELECT analytics.convert_table('"odd table"', 'quoted');
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM quoted') q) = '1';
  BEGIN
    PERFORM analytics.query('CREATE TABLE forbidden (x INT)');
    RAISE EXCEPTION 'expected DDL refusal';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%DDL not supported%' THEN RAISE; END IF;
  END;
END $$;

CREATE ROLE analytics_unprivileged;
SET ROLE analytics_unprivileged;
DO $$ BEGIN
  BEGIN
    PERFORM analytics.query('SELECT 1');
    RAISE EXCEPTION 'expected permissions failure';
  EXCEPTION WHEN insufficient_privilege THEN NULL;
  END;
END $$;
RESET ROLE;
DROP ROLE analytics_unprivileged;

-- Cross both the 8192-row cursor batch and 131072-row segment boundary.
CREATE TABLE boundary_source AS SELECT i::bigint AS id FROM generate_series(1, 131073) i;
SELECT analytics.convert_table('boundary_source', 'boundary');
DO $$ BEGIN
  ASSERT (SELECT count(*) FROM analytics.segments s JOIN analytics.tables t ON t.id=s.table_id WHERE t.name='boundary') = 2;
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM boundary') q) = '131073';
  ASSERT (SELECT q->>'total' FROM analytics.query('SELECT sum(id) AS total FROM boundary') q) = '8590131201';
  BEGIN
    PERFORM analytics.query('SELECT id FROM boundary');
    RAISE EXCEPTION 'expected output limit failure';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%output limit%' THEN RAISE; END IF;
  END;
END $$;

SET statement_timeout = '200ms';
DO $$ BEGIN
  BEGIN
    PERFORM analytics.query('SELECT sum(a.id+b.id) FROM boundary a CROSS JOIN boundary b');
    RAISE EXCEPTION 'expected query cancellation';
  EXCEPTION WHEN query_canceled THEN NULL;
  END;
END $$;
RESET statement_timeout;
SELECT * FROM analytics.query('SELECT count(*) AS n FROM events');

-- Projection must save actual backend reads, not only Parquet decoding.
CREATE TABLE io_source AS SELECT i::bigint AS id, md5(i::text) AS payload
  FROM generate_series(1,262144) i;
SELECT analytics.convert_table('io_source', 'io_events');
DO $$ DECLARE profile jsonb; payload_bytes bigint; BEGIN
  SELECT sum(octet_length(s.data)) INTO payload_bytes FROM analytics.segments s
    JOIN analytics.tables t ON t.id=s.table_id WHERE t.name='io_events';
  profile := analytics.profile_query('WITH x AS (SELECT id FROM io_events) SELECT sum(id) AS n FROM x');
  ASSERT profile->'rows'->0->>'n' = '34359869440';
  ASSERT (profile->>'read_bytes')::bigint < payload_bytes / 3;
  ASSERT (analytics.profile_query('SELECT 1')->>'read_bytes')::bigint = 0;
  profile := analytics.profile_query('SELECT sum(id) AS n FROM IO_EVENTS WHERE id=262144');
  ASSERT profile->'rows'->0->>'n' = '262144';
END $$;
BEGIN;
UPDATE analytics.segments SET data='\x00'::bytea
 WHERE table_id=(SELECT id FROM analytics.tables WHERE name='quoted');
-- An unreadable unrelated table must not be opened for schema inference.
SELECT * FROM analytics.query('SELECT count(*) AS n FROM io_events');
ROLLBACK;
CREATE TABLE temporal_source(s smallint, d date, t timestamp, c char(1));
INSERT INTO temporal_source VALUES
  (-32768, '1969-12-31', '1969-12-31 23:59:59.123456', 'x'),
  (32767, '2000-02-29', '2000-02-29 12:34:56.654321', 'y'),
  (NULL, NULL, NULL, NULL);
SELECT analytics.convert_table('temporal_source', 'temporal');
DO $$ DECLARE r jsonb; BEGIN
  SELECT q INTO r FROM analytics.query('SELECT min(s) AS lo, max(s) AS hi, count(t) AS n, min(d) AS d, min(t) AS t FROM temporal') q;
  ASSERT r->>'lo' = '-32768' AND r->>'hi' = '32767' AND r->>'n' = '2';
  ASSERT r->>'d' = '1969-12-31';
  ASSERT r->>'t' = '1969-12-31T23:59:59.123456';
  SELECT q INTO r FROM analytics.query('SELECT max(d) AS d, max(t) AS t, max(c) AS c FROM temporal') q;
  ASSERT r->>'d' = '2000-02-29' AND r->>'t' = '2000-02-29T12:34:56.654321';
  ASSERT r->>'c' = 'y';
END $$;
CREATE TABLE infinite_source(d date, t timestamp);
INSERT INTO infinite_source VALUES ('infinity', '2000-01-01');
DO $$ BEGIN
  BEGIN
    PERFORM analytics.convert_table('infinite_source','infinite');
    RAISE EXCEPTION 'expected infinity rejection';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%infinite dates%' THEN RAISE; END IF;
  END;
  ASSERT (SELECT count(*) FROM infinite_source) = 1;
  ASSERT NOT EXISTS (SELECT FROM analytics.tables WHERE name='infinite');
END $$;
UPDATE infinite_source SET d='2000-01-01', t='-infinity';
DO $$ BEGIN
  BEGIN
    PERFORM analytics.convert_table('infinite_source','infinite');
    RAISE EXCEPTION 'expected infinity rejection';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%infinite timestamps%' THEN RAISE; END IF;
  END;
  ASSERT (SELECT count(*) FROM infinite_source) = 1;
END $$;
-- Warm metadata must not survive a visible segment mutation or rollback.
CREATE TABLE cache_a_source AS SELECT i::bigint AS id FROM generate_series(1,2) i;
CREATE TABLE cache_b_source AS SELECT i::bigint AS id FROM generate_series(11,15) i;
SELECT analytics.convert_table('cache_a_source','cache_a');
SELECT analytics.convert_table('cache_b_source','cache_b');
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM cache_a') q) = '2';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM cache_a') q) = '2';
END $$;
BEGIN;
UPDATE analytics.segments SET data=(SELECT data FROM analytics.segments
  WHERE table_id=(SELECT id FROM analytics.tables WHERE name='cache_b') AND ordinal=0)
  WHERE table_id=(SELECT id FROM analytics.tables WHERE name='cache_a') AND ordinal=0;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM cache_a') q) = '5';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT min(id) AS n FROM cache_a') q) = '11';
END $$;
ROLLBACK;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM cache_a') q) = '2';
END $$;
-- Drop and recreate storage with reused logical name and sequence IDs, in the
-- same backend that holds the cached metadata. Physical identity must invalidate.
DROP EXTENSION pg_lamina CASCADE;
CREATE EXTENSION pg_lamina;
CREATE TABLE cache_recreated AS SELECT i::bigint AS id FROM generate_series(1,7) i;
SELECT analytics.convert_table('cache_recreated','cache_a');
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM cache_a') q) = '7';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT sum(id) AS n FROM cache_a') q) = '28';
END $$;
-- Restore the small events fixture used by recovery-test.sh.
CREATE TABLE restored_events AS SELECT i::bigint AS id FROM generate_series(1,4) i;
SELECT analytics.convert_table('restored_events','events');
SELECT 'integration tests passed' AS result;
