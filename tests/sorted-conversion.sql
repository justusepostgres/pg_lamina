\set ON_ERROR_STOP on
-- Deliberately scrambled input spanning several segments; conversion establishes order.
CREATE TABLE sorted_source AS
SELECT i::bigint AS id, CASE WHEN i%1000=0 THEN NULL ELSE i%97 END AS k,
       CASE WHEN i%101=0 THEN NULL ELSE md5(i::text) END AS label
FROM generate_series(1,600000) i ORDER BY (i::bigint*7919)%600001;
CREATE TABLE sorted_expected AS TABLE sorted_source;
SELECT analytics.convert_table_sorted('sorted_source','sorted_events',ARRAY['k','label']);
DO $$ DECLARE actual jsonb; expected jsonb; plan text; BEGIN
  ASSERT to_regclass('sorted_source') IS NULL;
  ASSERT (SELECT count(*) FROM analytics.segments s JOIN analytics.tables t ON t.id=s.table_id WHERE t.name='sorted_events')=5;
  SELECT jsonb_agg(q) INTO actual FROM analytics.query('SELECT k,label FROM sorted_events ORDER BY k ASC NULLS LAST,label ASC NULLS LAST LIMIT 100') q;
  SELECT jsonb_agg(jsonb_strip_nulls(to_jsonb(t))) INTO expected FROM (SELECT k,label FROM sorted_expected ORDER BY k ASC NULLS LAST,label COLLATE "C" ASC NULLS LAST LIMIT 100) t;
  ASSERT actual=expected, 'ascending sorted scan differs';
  SELECT jsonb_agg(q) INTO actual FROM analytics.query('SELECT k,label FROM sorted_events WHERE k>50 ORDER BY k DESC NULLS LAST,label DESC NULLS LAST LIMIT 100') q;
  SELECT jsonb_agg(jsonb_strip_nulls(to_jsonb(t))) INTO expected FROM (SELECT k,label FROM sorted_expected WHERE k>50 ORDER BY k DESC NULLS LAST,label COLLATE "C" DESC NULLS LAST LIMIT 100) t;
  ASSERT actual=expected, 'descending filtered scan differs';
  SELECT string_agg(q->>'plan',E'\n') INTO plan FROM analytics.query('EXPLAIN SELECT k,label FROM sorted_events ORDER BY k ASC NULLS LAST,label ASC NULLS LAST LIMIT 10') q;
  ASSERT plan NOT LIKE '%SortExec:%', 'expected stored ordering to eliminate sorting';
  ASSERT plan LIKE '%SortPreservingMergeExec:%', 'expected sorted multi-file merge';
END $$;

-- Exact identifiers, binary text collation and null ordering.
CREATE TABLE odd_sort_source("odd.name" text, "quote""key" bigint);
INSERT INTO odd_sort_source VALUES ('世界',2),('A',3),('a',1),('',4),(NULL,5),('é',6),('z',7);
SELECT analytics.convert_table_sorted('odd_sort_source','odd_sorted',ARRAY['odd.name','quote"key']);
DO $$ DECLARE actual jsonb; BEGIN
 SELECT jsonb_agg(q->'odd.name') INTO actual FROM analytics.query('SELECT "odd.name" FROM odd_sorted ORDER BY "odd.name" ASC NULLS LAST LIMIT 7') q;
 ASSERT actual='["","A","a","z","é","世界",null]'::jsonb;
END $$;

CREATE TABLE invalid_sort_source(id bigint, f double precision);
INSERT INTO invalid_sort_source VALUES (1, 1.5);
DO $$ DECLARE keys text[]; failed boolean; BEGIN
 FOREACH keys SLICE 1 IN ARRAY ARRAY[ARRAY['missing','missing'],ARRAY['id','id'],ARRAY['id',NULL],ARRAY['f','id']] LOOP
   failed := false;
   BEGIN PERFORM analytics.convert_table_sorted('invalid_sort_source','invalid_sorted',keys);
   EXCEPTION WHEN OTHERS THEN failed := true; END;
   ASSERT failed, 'invalid sort key accepted';
   ASSERT to_regclass('invalid_sort_source') IS NOT NULL;
   ASSERT NOT EXISTS(SELECT FROM analytics.tables WHERE name='invalid_sorted');
 END LOOP;
 failed := false;
 BEGIN PERFORM analytics.convert_table_sorted('invalid_sort_source','invalid_sorted',ARRAY[]::text[]);
 EXCEPTION WHEN OTHERS THEN failed := true; END;
 ASSERT failed, 'empty sort key list accepted';
END $$;
BEGIN;
SELECT analytics.convert_table_sorted('invalid_sort_source','rollback_sorted',ARRAY['id']);
ROLLBACK;
DO $$ BEGIN
 ASSERT to_regclass('invalid_sort_source') IS NOT NULL;
 ASSERT NOT EXISTS(SELECT FROM analytics.tables WHERE name='rollback_sorted');
END $$;
SELECT 'sorted conversion tests passed';
CREATE TABLE sorted_empty_source(id bigint);
SELECT analytics.convert_table_sorted('sorted_empty_source','sorted_empty',ARRAY['id']);
DO $$ BEGIN
 ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM sorted_empty') q)='0';
 ASSERT NOT EXISTS(SELECT FROM analytics.query('SELECT id FROM sorted_empty ORDER BY id LIMIT 1'));
END $$;
