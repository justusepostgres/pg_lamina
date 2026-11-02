\set ON_ERROR_STOP on
-- Warm both SPI plans, including actual segment range reads.
SET analytics.prepared_reads=on;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT sum(id) AS n FROM events') q)='10';
END $$;
-- Relation rewrite and harmless DDL invalidate dependencies without changing rows.
VACUUM FULL analytics.segments;
ALTER TABLE analytics.segments ADD COLUMN prepared_read_test integer;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT sum(id) AS n FROM events') q)='10';
END $$;
ALTER TABLE analytics.segments DROP COLUMN prepared_read_test;
-- A planning error inside a subtransaction must not poison the retained plan.
BEGIN;
ALTER TABLE analytics.segments RENAME COLUMN data TO unavailable_data;
DO $$ BEGIN
  BEGIN
    PERFORM analytics.query('SELECT sum(id) AS n FROM events');
    RAISE EXCEPTION 'expected missing-column error';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT LIKE '%s.data%' THEN RAISE; END IF;
  END;
END $$;
ROLLBACK;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT sum(id) AS n FROM events') q)='10';
END $$;
SET analytics.prepared_reads=off;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT sum(id) AS n FROM events') q)='10';
END $$;
RESET analytics.prepared_reads;
SELECT 'Prepared SPI rewrite and error recovery checks passed' AS result;
