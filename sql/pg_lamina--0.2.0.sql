CREATE SCHEMA analytics;
REVOKE ALL ON SCHEMA analytics FROM PUBLIC;

CREATE TABLE analytics.tables (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL UNIQUE,
    source_name text NOT NULL,
    row_count bigint NOT NULL DEFAULT 0,
    format_version integer NOT NULL DEFAULT 1 CHECK (format_version = 1),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE analytics.segments (
    table_id bigint NOT NULL REFERENCES analytics.tables(id) ON DELETE CASCADE,
    ordinal integer NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY (table_id, ordinal)
);
-- Parquet already compresses its payload. Avoid a second TOAST compression pass.
ALTER TABLE analytics.segments ALTER COLUMN data SET STORAGE EXTERNAL;
SELECT pg_catalog.pg_extension_config_dump('analytics.tables', '');
SELECT pg_catalog.pg_extension_config_dump('analytics.tables_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('analytics.segments', '');

CREATE FUNCTION analytics.convert_table(source regclass, destination text)
RETURNS bigint AS 'MODULE_PATHNAME', 'convert_table_wrapper'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION analytics.query(sql text)
RETURNS SETOF jsonb AS 'MODULE_PATHNAME', 'query_wrapper'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;
CREATE FUNCTION analytics.profile_query(sql text)
RETURNS jsonb AS 'MODULE_PATHNAME', 'profile_query_wrapper'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA analytics FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA analytics FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA analytics FROM PUBLIC;


CREATE FUNCTION analytics.convert_table_sorted(source regclass, destination text, sort_keys text[])
RETURNS bigint AS 'MODULE_PATHNAME', 'convert_table_sorted_wrapper'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION analytics.convert_table_sorted(regclass, text, text[]) FROM PUBLIC;
