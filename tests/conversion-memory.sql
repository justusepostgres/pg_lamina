\set ON_ERROR_STOP on
-- Observe the caller's expression contexts while segment INSERTs are running.
-- Checking after convert_table returns would miss retained argument allocations.
BEGIN;
CREATE TEMP TABLE conversion_memory_samples(ordinal integer, segment_bytes bigint, expression_bytes bigint);
CREATE FUNCTION pg_temp.observe_conversion_memory() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  INSERT INTO conversion_memory_samples
  SELECT NEW.ordinal, octet_length(NEW.data), coalesce(sum(total_bytes),0)
  FROM pg_backend_memory_contexts WHERE name='ExprContext';
  RETURN NEW;
END $$;
CREATE TRIGGER observe_conversion_memory BEFORE INSERT ON analytics.segments
FOR EACH ROW EXECUTE FUNCTION pg_temp.observe_conversion_memory();
CREATE TABLE conversion_memory_source AS
SELECT i::bigint AS id,
       md5(i::text) || md5((i+1)::text) || md5((i+2)::text) || md5((i+3)::text) AS payload
FROM generate_series(1,524288) i;
SELECT analytics.convert_table('conversion_memory_source','conversion_memory_events');
TABLE conversion_memory_samples;
DO $$ BEGIN
  ASSERT (SELECT count(*) FROM conversion_memory_samples)=4, 'test must span four segments';
  ASSERT (SELECT sum(segment_bytes) FROM conversion_memory_samples)>8*1024*1024,
    'test requires enough encoded data to expose retained arguments';
  ASSERT (SELECT max(expression_bytes) FROM conversion_memory_samples)<4*1024*1024,
    'segment bytea arguments retained in the caller expression context';
  ASSERT (SELECT q->>'n' FROM analytics.query('SELECT count(*) AS n FROM conversion_memory_events') q)='524288';
END $$;
ROLLBACK;
SELECT 'conversion memory tests passed';
