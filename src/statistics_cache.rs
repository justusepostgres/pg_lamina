//! Bounded upstream statistics cache. The engine invalidates it on catalog changes.
use datafusion::execution::cache::{
    cache_manager::FileStatisticsCache, default_cache::DefaultCache,
};
use std::sync::Arc;

pub fn new_cache(limit: usize) -> Arc<FileStatisticsCache> {
    Arc::new(DefaultCache::new(limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        common::{stats::Precision, ScalarValue, Statistics},
        execution::cache::{
            cache_manager::CachedFileMetadata, Cache, CacheValue, SchemaFingerprint,
            TableScopedPath,
        },
    };
    use object_store::{path::Path, ObjectMeta};
    const LIMIT: usize = 8 * 1024 * 1024;

    fn value(bytes: usize) -> CachedFileMetadata {
        let schema = Schema::new(vec![Field::new("text", DataType::Utf8, true)]);
        let mut stats = Statistics::new_unknown(&schema);
        stats.column_statistics[0].min_value =
            Precision::Exact(ScalarValue::Utf8(Some("x".repeat(bytes))));
        CachedFileMetadata::new(
            ObjectMeta {
                location: Path::from("table/0.parquet"),
                last_modified: std::time::UNIX_EPOCH.into(),
                size: 100,
                e_tag: None,
                version: None,
            },
            Arc::new(SchemaFingerprint::from_schema(&schema)),
            Arc::new(stats),
            None,
        )
    }

    #[test]
    fn retained_string_statistics_respect_budget_and_replacement() {
        let cache = DefaultCache::new(LIMIT);
        let key = TableScopedPath {
            table: None,
            path: Path::from("table/0.parquet"),
        };
        let entry = value(3 * 1024 * 1024);
        assert!(entry.size() >= 3 * 1024 * 1024);
        cache.put(&key, entry);
        assert!(cache.get(&key).is_some());
        for i in 1..5 {
            let mut next = key.clone();
            next.path = Path::from(format!("table/{i}.parquet"));
            cache.put(&next, value(3 * 1024 * 1024));
            assert!(cache.memory_used() <= LIMIT);
        }
        cache.put(&key, value(32));
        cache.put(&key, value(LIMIT));
        assert!(
            cache.get(&key).is_none(),
            "oversized replacement must remove stale statistics"
        );
        cache.clear();
        assert_eq!(cache.memory_used(), 0);
        assert!(new_cache(LIMIT).is_empty());
    }
}
