<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="assets/pg-lamina-logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="assets/pg-lamina-logo.svg">
    <img alt="pg_lamina" src="assets/pg-lamina-logo.svg" width="520">
  </picture>
</p>

# pg_lamina — experimental PostgreSQL-hosted columnar engine

A Rust/pgrx extension that **converts an existing PostgreSQL heap table** into
Parquet segments and queries them with DataFusion. The source heap is dropped at
successful cutover. This is an offline conversion, not a continuously replicated
mirror. Applications switch to the analytical API after migration.

**Research prototype, not production-ready or ClickHouse-performance-equivalent.**
The initial experiment tests conversion, columnar execution, and PostgreSQL-managed
durability. It deliberately does not implement a table access method.

## Example

```sql
CREATE EXTENSION pg_lamina;
CREATE TABLE events_source (id bigint, tenant integer, amount bigint, region text);
INSERT INTO events_source VALUES (1, 10, 125, 'west'), (2, 10, 200, 'west');

BEGIN;
SELECT analytics.convert_table('events_source', 'events'); -- returns 2
SELECT * FROM analytics.query(
  'SELECT region, sum(amount) AS total FROM events GROUP BY region'
);
COMMIT;
```

`events_source` no longer exists after conversion; `events` is a DataFusion catalog
name, not a PostgreSQL relation. Roll back the transaction to retain the original
table and discard the conversion. `analytics.query` returns one JSONB value per
result row. Its SQL is interpreted by DataFusion and can join analytical tables.

## Conversion contract

- Superuser only for this prototype. No application-facing privilege model yet.
- Destination names: lowercase ASCII identifiers, maximum 63 bytes.
- Supports ordinary logged heap tables, including empty tables. Rejects partitions,
  inheritance, temporary/unlogged tables, and non-heap access methods.
- Supported types: `boolean`, `smallint`, `integer`, `bigint`, `double precision`,
  `text`, `varchar`, `char`, `date`, and `timestamp without time zone`.
  Timestamps retain microsecond precision. Infinite dates/timestamps and timestamp
  values outside Arrow's microsecond range are rejected transactionally.
  `char` values use PostgreSQL's text cast, removing fixed-width trailing padding.
  Other types, including `numeric`, `timestamptz`, arrays and domains, are rejected
  before cutting over. No implicit lossy numeric conversion.
- Takes an `ACCESS EXCLUSIVE` lock for the entire operation, blocking reads/writes
  until transaction end. Use a maintenance window. NOT an online migration.
- Fetches 8,192 rows per SPI session into owned Arrow arrays. Produces independent
  Parquet segments of up to 131,072 rows, using Zstd. Preserves input scan order;
  no automatic sort/cluster selection yet.
- Stores segments in `analytics.segments.data` (`bytea`, TOAST compression disabled)
  and metadata in `analytics.tables`. Both are WAL-logged PostgreSQL relations.
- Creates metadata and segments and drops the source in the same transaction.
  Failure rolls everything back. No `CASCADE`: dependent views/FKs cause failure.
- Source indexes, constraints, triggers, permissions, defaults, and relation OID
  are not transferred. Ordinary source-owned objects disappear with the source.
  Consumers must explicitly migrate their application contract.
- Requires temporary space for BOTH source and columnar data plus WAL until commit.
- Converted data is currently immutable through the public API. Direct ingest,
  updates/deletes, compaction, and analytical table lifecycle APIs are future work.

## Execution and known bottlenecks

The SQL AST identifies referenced tables, including joins and CTE bodies. The
backend loads their segment descriptors and up to 8 KiB of each file's tail in
one catalog query. A custom object store serves metadata from those owned buffers
and requests remaining Parquet byte ranges through a channel to the backend.
PostgreSQL reads only those bytea slices, using uncompressed external TOAST storage.
Projection and row-group pruning now reduce backend payload reads as well as decode
work. No full-catalog copy or 512 MiB catalog-size ceiling remains.

No PostgreSQL pointers, SPI calls, or memory contexts enter DataFusion threads.
Four executor workers process the query. The backend polls PostgreSQL interrupts
while execution runs. DDL, DML, COPY, and session mutation statements are rejected.
`analytics.batch_size` defaults to 32768 (range 1024–131072). `analytics.parquet_filter_pushdown` defaults to `on`; switch it off
per session to compare decoder-level filtering. Both are read on the backend before
launching workers.

Version 0.2.0 adds optional ordered conversion:

```sql
ALTER EXTENSION pg_lamina UPDATE TO '0.2.0'; -- existing installations
SELECT analytics.convert_table_sorted('raw_events', 'events', ARRAY['eventtime', 'userid']);
```

The key list contains exact column names, not SQL expressions. Keys use ascending
order with nulls last; text uses PostgreSQL's bytewise `C` collation, matching the
analytical engine. Boolean, integer, date, timestamp and text keys are supported;
floating-point keys are rejected. Conversion sorts under the source table's
exclusive lock, can spill through PostgreSQL's sort machinery, then writes
immutable Parquet segments and atomically drops the source. It can take more time
and temporary disk space than unordered conversion. `work_mem` controls the
PostgreSQL sort, separately from the analytical query's configurable tracked pool (256 MiB by default).

The sort declaration travels inside each segment's Arrow schema metadata. Readers
use it to advertise per-file ordering; unsorted legacy segments remain supported.
This is an extension-maintained invariant: direct mutation of segment bytes is
not a supported way to append or change a sorted table. The 0.1.0→0.2.0 SQL
upgrade only adds the new function and does not rewrite existing data.
Use `SELECT * FROM analytics.query('EXPLAIN SELECT ...')` to inspect plans.

`analytics.storage_compression` selects the codec for new conversions: `zstd`
(default) or `lz4` (Parquet LZ4_RAW). For example, run
`SET analytics.storage_compression='lz4'` before `analytics.convert_table`.
The codec is stored in each Parquet segment; changing the setting does not
rewrite existing tables, and both codecs can be read in the same query.
For per-column overrides, set a JSON object before conversion:
`SET analytics.column_compression='{"url":"lz4"}'`. Unlisted columns use
`analytics.storage_compression`. Names are exact source column names (including
case and literal dots); values must be `zstd` or `lz4`. Invalid JSON, unknown
columns, and unsupported codecs fail conversion without dropping the source.
The default is `{}`; `RESET analytics.column_compression` clears overrides.
Both sorted and unsorted conversion support this setting, including empty tables.
`analytics.dictionary_limit_kb` sets the Parquet dictionary cap per column for
new conversions (default 1024, range 64–16384). Set it to 8192 before conversion
to try larger dictionaries. Existing segments retain their encoding. On one
million ClickBench rows, a paired 1 MiB/8 MiB test reduced storage by 9.8% and
improved Q21–Q23 by 31–53%, with only 1% median improvement across all queries.
The setting remains optional: conversion memory can grow across many columns,
and this test does not establish a safe default for arbitrary schemas.

`analytics.regex_reuse` defaults to `on`. When `regexp_replace` has scalar
pattern/replacement/flags and a repeated StringView input, it evaluates unique
views within the batch and restores the original row order and nulls. Reuse is
limited to 16,384 unique views and 131,072 rows; a cheap initial check and a
cardinality limit send unsuitable batches through DataFusion's original path.
Nothing is retained across batches or queries. Set the option to `off` for
paired comparisons. This is execution reuse, not a query-result cache.
Run `python3 scripts/test-regex-reuse.py` for differential kernel tests against
the production DataFusion artifacts.

`analytics.prepared_reads` defaults to `on`. The backend retains just two
PostgreSQL SPI plans: catalog lookup and segment range read. PostgreSQL handles
plan dependency invalidation; each execution reads its current snapshot. No
arguments, result tuples or worker references are retained. An interleaved
43-query test measured only about 1% median improvement, so this is a small
read-path optimization rather than evidence of parity. Set it to `off` for A/B
measurements. Integration tests cover extension recreation, relation rewrite,
DDL rollback, planning-error recovery and concurrent snapshots.

`analytics.metadata_cache` defaults to `on`. Each backend can retain up to 32 MiB
of decoded Parquet metadata for its last referenced catalog, plus an 8 MiB
inferred-schema cache, an accounted file-statistics cache (8 MiB by default),
and up to 8 MiB of identity/footer signatures. Set
`analytics.statistics_cache_mb` (1–1024) to tune statistics retention per backend;
this is separate from the query execution memory pool. Changing this budget
invalidates the retained caches, including when reducing it.
Statistics include scalar heap sizes; exceeding the budget clears old entries,
and oversized entries are not retained. All caches share catalog invalidation. Every query rereads the visible catalog and compares
segment tuple identities, relation file identities, logical identities and lengths.
Unchanged identities reuse retained footer tails; otherwise the current snapshot's
footers are reloaded. Any signature difference replaces the cache; larger signatures use a
query-local cache. Table providers, query results, and runtime threads are not
retained. Disable the setting for paired measurements.

`analytics.plan_cache` defaults to `on`. Each backend retains at most one
serialized optimized logical plan, with SQL/configuration key and plan payload
limited to 1 MiB combined, plus fixed entry overhead. Catalog generation changes
invalidate it alongside metadata; execution settings are part of its key.
Decoded table references are replaced with current providers, and physical
planning, execution memory pools, and query results remain query-local.
Stable/volatile functions (including nested expressions), unsupported plan
shapes, serialization failures, and oversized entries use normal planning.
Disabling `analytics.metadata_cache` also prevents cross-query plan retention.
Set `analytics.plan_cache=off` for comparisons. `analytics.profile_query` reports
`plan_cache_hit` and `plan_cache_bytes`. Integration tests cover data/schema
replacement, rollback, settings, oversized plans, fresh clock/random values,
and cache hits across concurrent commits and repeatable-read snapshots.
`analytics.late_materialization` defaults to `off`. This experimental path
finds physical row references before fetching complete rows for eligible
single-table ordered queries with LIMIT 1–10. Both passes share the statement's
catalog and object store. Unsupported shapes use normal execution. Profiles
report `late_materialization` and `late_retained_bytes`; retained fetch buffers
use the query memory pool, which is not a total RSS bound. This path is experimental and should remain disabled unless validated for the
intended workload.
Concurrent replacement stress also checks that each two-pass statement returns
one complete version and that repeatable-read results remain fixed, with
server timestamps confirming commits overlap the read statements.

The optional experimental `analytics.fair_spill_pool` setting changes memory
scheduling and defaults to `off`. Batch size remains 32768.

`SET analytics.memory_limit_mb = 2048` gives each subsequent analytical query
in that session a 2 GiB tracked execution budget. The default is 256 MiB; the
accepted range is 1–65536 MiB. Both memory-pool implementations use this budget.
The setting is captured on the PostgreSQL backend before workers start and does
not control conversion memory. Benchmarks must record the chosen budget and
use explicit comparison-engine budgets; increasing it is not itself an
algorithmic optimization.

Bounds are experimental safeguards, **not a strict process RSS limit**:

- Segment descriptors and footer buffers scale with referenced segment count.
- Configurable tracked DataFusion execution memory pool (256 MiB default); input buffers, some operators,
  PostgreSQL allocations and result serialization are outside that pool.
- Maximum 10,000 output rows. Use aggregations or explicit LIMIT. Results are
  collected before JSONB delivery; not a streaming Arrow API.
- Runtime/catalog creation and footer loading occur on every call. Footer buffers
  are shared across readers. Bounded backend-local metadata/statistics caches
  survive queries and are invalidated when the catalog snapshot changes.
  JSON output adds overhead and does not preserve every SQL type distinction.
- Analytical SQL semantics, collation and NULL behavior follow DataFusion, not a
  promise of full PostgreSQL compatibility.

## Build and run locally

Pinned engine versions: pgrx 0.16.1, DataFusion core 55.0.0 (55.2.0 component
crates), Arrow/Parquet 59.3.2. `Cargo.lock` pins the full dependency graph.
`rust-toolchain.toml` selects Rust 1.99.1. Target: PostgreSQL 17, ARM64. The pgrx CLI is not needed: SQL wrappers are registered by the
checked-in extension installation script.

```sh
export PGRX_PG_CONFIG_PATH=/opt/homebrew/opt/postgresql@17/bin/pg_config
export LIBCLANG_PATH=/Library/Developer/CommandLineTools/usr/lib
cargo build --release --locked -j 4
bash scripts/install-local.sh
bash scripts/dev-cluster.sh start
bash scripts/test.sh
bash scripts/recovery-test.sh
bash scripts/dev-cluster.sh stop
```

Installation writes three extension files to the chosen PostgreSQL installation.
Set `PG_BIN` and `PGRX_PG_CONFIG_PATH` for other installations. The scripts use a
workspace-owned cluster in `.local/pgdata`, a private Unix socket, port 55437, data
checksums, and no TCP listener. They do not start a system service. Tests recreate
only their dedicated `pg_lamina_test`, `pg_lamina_restore`, and
`pg_lamina_bench` databases. Recovery tests stop/restart this cluster and create
a temporary physical replica; do not point these scripts at a production cluster.

`analytics.profile_query(sql)` returns rows plus `read_bytes`, `read_requests`, and
`elapsed_ms`. Bytes include the eagerly fetched footer tails and requested payload
ranges, not PostgreSQL page-level physical I/O. Requests count only individual
payload reads after the batched catalog query. Profiling includes execution but
does not include final JSONB conversion in its elapsed time.
`phases_ms` separates catalog loading, executor setup, logical planning, physical
planning, execution and serialization. Runtime teardown is outside these timers.

## Validation

`tests/integration.sql` exercises values, NULLs, Unicode, bigint precision, joins,
batch/segment boundaries, rollback, dependency failures, unsupported types,
permissions, query restrictions, output limits, and cancellation.

`scripts/recovery-test.sh` checks immediate-stop crash recovery, logical
dump/restore, physical streaming replication, and replica promotion. Storage
tables are registered with `pg_extension_config_dump` so logical dumps include
their contents. Extension binaries must be installed on restored/replica servers.

## CNPG direction

WAL-logged PostgreSQL storage is intended to fit physical replication and CNPG's
backup/failover model without an external segment publication protocol. Native
PostgreSQL recovery tests are a prerequisite, not CNPG certification. Still needed:

1. Reproducible Linux/ARM64 and AMD64 images with the extension installed.
2. Actual CNPG deployment, switchover/failover, backup/PITR and restore tests.
3. Compatible extension upgrades and on-disk format migration policy.
4. Admission/resource controls for analytical CPU and memory use.

## Next architectural experiments

1. Reduce measured remaining execution/planning overhead against ClickHouse and
   TimescaleDB, including stable connection workloads and larger-than-RAM datasets.
2. Evaluate persistent metadata/segment caching versus direct PG-buffer-backed storage;
   quantify WAL/TOAST overhead before committing to the format.
3. Add a direct ingestion API, sort keys, statistics and compaction.
4. Expand type coverage with explicit mapping contracts.
5. Benchmark ClickBench and joins against ClickHouse on identical resources,
   then add concurrent ingestion and data larger than RAM.

The pure Rust statistics-cache identity, eviction, replacement, and oversized-entry
tests use production Cargo artifacts: `python3 scripts/test-statistics-cache.py`.

Use `bash scripts/cargo-local.sh build --release --locked -j 4` for a local
build with normal process scheduling on macOS.
