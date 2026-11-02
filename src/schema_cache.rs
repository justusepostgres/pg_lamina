//! Inferred schemas share the engine's exact visible-catalog invalidation.
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::heap_size::{DFHeapSize, DFHeapSizeCtx},
    execution::cache::{default_cache::DefaultCache, CacheValue},
};
use object_store::path::Path;

pub const LIMIT: usize = 8 * 1024 * 1024;
pub type SchemaCache = DefaultCache<Path, CachedSchema>;

#[derive(Clone)]
pub struct CachedSchema(pub SchemaRef);

impl CacheValue for CachedSchema {
    fn size(&self) -> usize {
        let mut ctx = DFHeapSizeCtx::default();
        std::mem::size_of_val(self.0.as_ref())
            + self.0.fields().heap_size(&mut ctx)
            + self.0.metadata().heap_size(&mut ctx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{arrow::datatypes::Schema, execution::cache::Cache};
    use std::{collections::HashMap, sync::Arc};

    #[test]
    fn schema_metadata_counts_toward_retention_budget() {
        let cache = SchemaCache::new(4096);
        let key = Path::from("wide");
        let value = |bytes| {
            CachedSchema(Arc::new(Schema::empty().with_metadata(HashMap::from([(
                "description".into(),
                "x".repeat(bytes),
            )]))))
        };
        cache.put(&key, value(1024));
        assert!(cache.get(&key).is_some());
        assert!(cache.memory_used() >= 1024);
        cache.put(&key, value(8192));
        assert!(cache.get(&key).is_none());
        assert!(cache.memory_used() <= 4096);
    }
}
