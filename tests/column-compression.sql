\set ON_ERROR_STOP on
LOAD 'pg_lamina';
CREATE TABLE column_codec_source(id bigint, "odd.name" text, plain text);
INSERT INTO column_codec_source SELECT i,CASE WHEN i%3=0 THEN NULL ELSE '世界'||i END,'payload'||i FROM generate_series(1,131073) i;
SET analytics.column_compression='{"odd.name":"lz4","id":"zstd"}';
SELECT analytics.convert_table_sorted('column_codec_source','column_codec',ARRAY['id']);
RESET analytics.column_compression;
DO $$ BEGIN
 ASSERT current_setting('analytics.column_compression')='{}';
 ASSERT (SELECT count(*) FROM analytics.segments WHERE table_id=(SELECT id FROM analytics.tables WHERE name='column_codec'))=2;
 ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM column_codec') q)='131073';
 ASSERT (SELECT q->>'v' FROM analytics.query('SELECT "odd.name" AS v FROM column_codec WHERE id=131072') q)='世界131072';
 ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM column_codec WHERE "odd.name" IS NULL') q)='43691';
END $$;
CREATE TABLE column_codec_empty(id bigint);
DO $$ DECLARE setting text; rejected boolean; BEGIN
 FOREACH setting IN ARRAY ARRAY['[]','null','{"missing":"lz4"}','{"id":"snappy"}','{"id":4}','{broken'] LOOP
  PERFORM set_config('analytics.column_compression',setting,true);
  rejected:=false;
  BEGIN
   PERFORM analytics.convert_table('column_codec_empty','must_not_exist_codec');
  EXCEPTION WHEN OTHERS THEN rejected:=true;
  END;
  ASSERT rejected,setting;
  ASSERT to_regclass('column_codec_empty') IS NOT NULL;
  ASSERT NOT EXISTS(SELECT FROM analytics.tables WHERE name='must_not_exist_codec');
 END LOOP;
END $$;
RESET analytics.column_compression;
BEGIN;
SET LOCAL analytics.storage_compression='lz4';
SET LOCAL analytics.column_compression='{"id":"zstd"}';
SELECT analytics.convert_table('column_codec_empty','codec_rollback');
ROLLBACK;
DO $$ BEGIN
 ASSERT to_regclass('column_codec_empty') IS NOT NULL;
 ASSERT NOT EXISTS(SELECT FROM analytics.tables WHERE name='codec_rollback');
END $$;
