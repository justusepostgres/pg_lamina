CREATE FUNCTION analytics.convert_table_sorted(source regclass, destination text, sort_keys text[])
RETURNS bigint AS 'MODULE_PATHNAME', 'convert_table_sorted_wrapper'
LANGUAGE c STRICT VOLATILE PARALLEL UNSAFE;
REVOKE ALL ON FUNCTION analytics.convert_table_sorted(regclass, text, text[]) FROM PUBLIC;
