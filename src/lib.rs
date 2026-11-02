mod convert;
mod engine;
mod late_fetch;
mod late_plan;
mod plan_cache;
mod prepared_reads;
mod regex_reuse;
mod schema_cache;
mod statistics_cache;
mod store;

use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::JsonB;
pgrx::pg_module_magic!();

#[derive(pgrx::PostgresGucEnum, Copy, Clone)]
enum StorageCompression {
    Zstd,
    Lz4,
}
static STORAGE_COMPRESSION: GucSetting<StorageCompression> =
    GucSetting::<StorageCompression>::new(StorageCompression::Zstd);

static DICTIONARY_LIMIT_KB: GucSetting<i32> = GucSetting::<i32>::new(1024);
static COLUMN_COMPRESSION: GucSetting<Option<std::ffi::CString>> =
    GucSetting::<Option<std::ffi::CString>>::new(Some(c"{}"));

static MEMORY_LIMIT_MB: GucSetting<i32> = GucSetting::<i32>::new(256);
static STATISTICS_CACHE_MB: GucSetting<i32> = GucSetting::<i32>::new(8);

static BATCH_SIZE: GucSetting<i32> = GucSetting::<i32>::new(32768);
static FILTER_PUSHDOWN: GucSetting<bool> = GucSetting::<bool>::new(true);
static METADATA_CACHE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static LATE_MATERIALIZATION: GucSetting<bool> = GucSetting::<bool>::new(false);
static PLAN_CACHE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static PREPARED_READS: GucSetting<bool> = GucSetting::<bool>::new(true);
static REGEX_REUSE: GucSetting<bool> = GucSetting::<bool>::new(true);
static FAIR_SPILL_POOL: GucSetting<bool> = GucSetting::<bool>::new(false);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"analytics.column_compression",
        c"JSON object mapping source column names to zstd or lz4 for new conversions",
        c"Overrides storage_compression for named columns; validated at conversion. Existing segments are unchanged.",
        &COLUMN_COMPRESSION,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.late_materialization",
        c"Fetch full rows after a selective ordered limit",
        c"Experimental two-pass execution for eligible single-table full-row queries with LIMIT 1 through 10.",
        &LATE_MATERIALIZATION,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.plan_cache",
        c"Reuse a bounded optimized logical plan for an unchanged visible catalog",
        c"One entry, at most 1 MiB retained payload per backend; volatile and stable functions bypass caching.",
        &PLAN_CACHE_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"analytics.statistics_cache_mb",
        c"Retained file statistics per backend in MiB",
        c"Separate from query execution memory; changing the budget invalidates cached metadata.",
        &STATISTICS_CACHE_MB,
        1,
        1024,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"analytics.memory_limit_mb",
        c"Tracked execution memory per analytical query in MiB",
        c"Bounds DataFusion memory-pool reservations, not total backend RSS or conversion memory.",
        &MEMORY_LIMIT_MB,
        1,
        65536,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"analytics.dictionary_limit_kb",
        c"Parquet dictionary size limit per column for new conversions",
        c"Larger dictionaries may improve scans but increase conversion memory; existing segments are unchanged.",
        &DICTIONARY_LIMIT_KB,
        64,
        16384,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_enum_guc(
        c"analytics.storage_compression",
        c"Compression codec for newly converted columnar segments",
        c"zstd favors space; lz4 favors decode speed. Existing segments retain their codec.",
        &STORAGE_COMPRESSION,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.prepared_reads",
        c"Reuse PostgreSQL SPI plans for catalog and segment reads",
        c"PostgreSQL invalidates plans on dependency changes; snapshots and data remain query-local.",
        &PREPARED_READS,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.regex_reuse",
        c"Reuse regex replacements for repeated string views within a batch",
        c"Bounded batch-local reuse with fallback for high-cardinality inputs.",
        &REGEX_REUSE,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.fair_spill_pool",
        c"Share execution memory fairly between spillable operators",
        c"Experimental memory scheduling within analytics.memory_limit_mb.",
        &FAIR_SPILL_POOL,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.metadata_cache",
        c"Reuse decoded Parquet metadata for an unchanged visible catalog",
        c"Bounded per-backend cache; catalog identity is checked on every query.",
        &METADATA_CACHE_ENABLED,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        c"analytics.parquet_filter_pushdown",
        c"Parquet decoder filter pushdown",
        c"Experimental late materialization for selective filters.",
        &FILTER_PUSHDOWN,
        GucContext::Userset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        c"analytics.batch_size",
        c"Arrow execution batch rows",
        c"Controls vector batch size for analytical execution.",
        &BATCH_SIZE,
        1024,
        131072,
        GucContext::Userset,
        GucFlags::default(),
    );
}

fn require_superuser() {
    if !unsafe { pg_sys::superuser() } {
        error!("pg_lamina prototype requires superuser");
    }
}

#[pg_extern]
fn convert_table(source: pg_sys::Oid, destination: &str) -> i64 {
    require_superuser();
    convert::convert(source, destination, &[]).unwrap_or_else(|e| error!("conversion failed: {e}"))
}

#[pg_extern]
fn convert_table_sorted(
    source: pg_sys::Oid,
    destination: &str,
    sort_keys: Vec<Option<String>>,
) -> i64 {
    require_superuser();
    let sort_keys: Vec<String> = sort_keys
        .into_iter()
        .map(|key| key.unwrap_or_else(|| error!("sort keys cannot contain NULL")))
        .collect();
    if sort_keys.is_empty() {
        error!("at least one sort key is required");
    }
    convert::convert(source, destination, &sort_keys)
        .unwrap_or_else(|e| error!("conversion failed: {e}"))
}

#[pg_extern]
fn query(sql: &str) -> SetOfIterator<'static, JsonB> {
    require_superuser();
    let output = engine::query(sql).unwrap_or_else(|e| error!("analytical query failed: {e}"));
    SetOfIterator::new(output.rows.into_iter().map(JsonB))
}

#[pg_extern]
fn profile_query(sql: &str) -> JsonB {
    require_superuser();
    JsonB(
        engine::query(sql)
            .unwrap_or_else(|e| error!("analytical query failed: {e}"))
            .profile(),
    )
}
