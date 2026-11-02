\set ON_ERROR_STOP on
-- Check actual allocation enforcement and recovery in the same backend.
CREATE TABLE memory_source AS
SELECT i::bigint AS id, repeat(md5(i::text), 4) AS value
FROM generate_series(1,100000) AS i;
SELECT analytics.convert_table('memory_source', 'memory_fixture');
SET analytics.memory_limit_mb = 1;
DO $$ BEGIN
  BEGIN
    PERFORM * FROM analytics.query('SELECT count(DISTINCT value) AS n FROM memory_fixture');
    RAISE EXCEPTION 'low memory budget unexpectedly succeeded';
  EXCEPTION WHEN OTHERS THEN
    IF SQLERRM NOT ILIKE '%resources exhausted%' AND SQLERRM NOT ILIKE '%memory reservation%' THEN
      RAISE;
    END IF;
  END;
END $$;
SET analytics.memory_limit_mb = 64;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(DISTINCT value) AS n FROM memory_fixture') q) = '100000';
END $$;
SET analytics.fair_spill_pool = on;
DO $$ BEGIN
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(DISTINCT value) AS n FROM memory_fixture') q) = '100000';
END $$;
RESET analytics.fair_spill_pool;
RESET analytics.memory_limit_mb;
DO $$ BEGIN
  ASSERT current_setting('analytics.memory_limit_mb') = '256';
END $$;
SELECT 'memory budget tests passed' AS result;
