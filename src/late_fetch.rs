//! Statement-scoped selective fetch. No filesystem or PostgreSQL calls here.
#![allow(deprecated)] // Pinned Parquet 59 adapter; replace before upstream migration.
use datafusion::{
    arrow::{
        array::{Array, Int64Array, StringArray, StringViewArray},
        compute::cast,
        datatypes::{DataType, SchemaRef},
        record_batch::RecordBatch,
    },
    execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
    parquet::arrow::{
        arrow_reader::{ArrowReaderOptions, RowSelection, RowSelector},
        async_reader::ParquetObjectReader,
        ParquetRecordBatchStreamBuilder,
    },
};
use futures::TryStreamExt;
use object_store::{path::Path, ObjectMeta, ObjectStore};
use std::{collections::BTreeMap, sync::Arc};
type Error = Box<dyn std::error::Error + Send + Sync>;
pub struct Fetched {
    pub batches: Vec<RecordBatch>,
    reservations: Vec<MemoryReservation>,
}
impl Fetched {
    pub fn retained_bytes(&self) -> usize {
        self.reservations.iter().map(|r| r.size()).sum()
    }
}

pub async fn fetch(
    references: &[RecordBatch],
    allowed: &BTreeMap<Path, ObjectMeta>,
    store: Arc<dyn ObjectStore>,
    schema: SchemaRef,
    pool: Arc<dyn MemoryPool>,
) -> Result<Fetched, Error> {
    let mut groups: BTreeMap<Path, Vec<(usize, usize)>> = BTreeMap::new();
    let mut rank = 0;
    for batch in references {
        let files = cast(
            batch
                .column_by_name("__late_file")
                .ok_or("missing file reference")?,
            &DataType::Utf8,
        )?;
        let files = files
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("invalid file reference type")?;
        let rows = batch
            .column_by_name("__late_row")
            .ok_or("missing row reference")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or("invalid row reference type")?;
        for i in 0..batch.num_rows() {
            if rank >= 10 || files.is_null(i) || rows.is_null(i) {
                return Err("invalid or excessive late references".into());
            }
            let path = Path::parse(files.value(i))?;
            if !allowed.contains_key(&path) {
                return Err("late reference outside statement catalog".into());
            }
            let row = usize::try_from(rows.value(i))?;
            groups.entry(path).or_default().push((row, rank));
            rank += 1;
        }
    }
    let mut pending = groups.into_iter();
    let mut jobs = tokio::task::JoinSet::new();
    let mut ranked = Vec::new();
    let mut reservations = Vec::new();
    loop {
        while jobs.len() < 4 {
            let Some((path, positions)) = pending.next() else {
                break;
            };
            let meta = allowed[&path].clone();
            let store = store.clone();
            let schema = schema.clone();
            let pool = pool.clone();
            jobs.spawn(async move { fetch_file(meta, positions, store, schema, pool).await });
        }
        let Some(result) = jobs.join_next().await else {
            break;
        };
        let (rows, reservation) = result??;
        ranked.extend(rows);
        reservations.push(reservation);
    }
    ranked.sort_by_key(|(rank, _)| *rank);
    if ranked.len() != rank {
        return Err("late fetch row count mismatch".into());
    }
    Ok(Fetched {
        batches: ranked.into_iter().map(|(_, batch)| batch).collect(),
        reservations,
    })
}

async fn fetch_file(
    meta: ObjectMeta,
    mut positions: Vec<(usize, usize)>,
    store: Arc<dyn ObjectStore>,
    schema: SchemaRef,
    pool: Arc<dyn MemoryPool>,
) -> Result<(Vec<(usize, RecordBatch)>, MemoryReservation), Error> {
    positions.sort_unstable();
    if positions.windows(2).any(|p| p[0].0 == p[1].0) {
        return Err("duplicate late row reference".into());
    }
    let reader = ParquetObjectReader::new(store, meta.location)
        .with_file_size(meta.size)
        .with_footer_size_hint(8192);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_schema(schema).with_offset_index_policy(datafusion::parquet::file::metadata::PageIndexPolicy::Optional),
    )
    .await?;
    let total = usize::try_from(builder.metadata().file_metadata().num_rows())?;
    let mut selection = Vec::new();
    let mut next = 0;
    for (row, _) in &positions {
        if *row >= total {
            return Err("late row outside file".into());
        }
        if *row > next {
            selection.push(RowSelector::skip(*row - next));
        }
        selection.push(RowSelector::select(1));
        next = row + 1;
    }
    if next < total {
        selection.push(RowSelector::skip(total - next));
    }
    let mut stream = builder
        .with_row_selection(RowSelection::from(selection))
        .with_batch_size(1024)
        .build()?;
    let reservation = MemoryConsumer::new("late fetch retained batches").register(&pool);
    let mut output = Vec::new();
    let mut index = 0;
    while let Some(batch) = stream.try_next().await? {
        // Reserve retained input buffers before copying a tiny selected result.
        // Decoder working buffers are not included, as with the normal reader.
        let original_bytes = batch.get_array_memory_size();
        reservation.try_grow(original_bytes)?;
        let columns = batch
            .columns()
            .iter()
            .map(|a| {
                a.as_any()
                    .downcast_ref::<StringViewArray>()
                    .map(|a| Arc::new(a.gc()) as _)
                    .unwrap_or_else(|| a.clone())
            })
            .collect();
        let compact = RecordBatch::try_new(batch.schema(), columns)?;
        reservation.try_grow(compact.get_array_memory_size())?;
        drop(batch);
        reservation.shrink(original_bytes);
        for row in 0..compact.num_rows() {
            let (_, rank) = positions.get(index).ok_or("excess late fetch rows")?;
            output.push((*rank, compact.slice(row, 1)));
            index += 1;
        }
    }
    if index != positions.len() {
        return Err("short late fetch".into());
    }
    Ok((output, reservation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{execution::memory_pool::GreedyMemoryPool, parquet::arrow::ArrowWriter};
    #[test]
    fn late_fetch_validates_references_and_releases_reservations() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            use object_store::ObjectStoreExt;
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            let source = RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(StringArray::from(vec![
                    "large-value".repeat(100),
                    "second".into(),
                ])) as _,
            )])
            .unwrap();
            let mut bytes = Vec::new();
            let mut writer = ArrowWriter::try_new(&mut bytes, source.schema(), None).unwrap();
            writer.write(&source).unwrap();
            writer.close().unwrap();
            let path = Path::from("table/0.parquet");
            store.put(&path, bytes.into()).await.unwrap();
            let allowed = BTreeMap::from([(path.clone(), store.head(&path).await.unwrap())]);
            let refs = |file: &str, rows: Vec<i64>| {
                RecordBatch::try_from_iter(vec![
                    (
                        "__late_file",
                        Arc::new(StringArray::from(vec![file; rows.len()])) as _,
                    ),
                    ("__late_row", Arc::new(Int64Array::from(rows)) as _),
                ])
                .unwrap()
            };
            let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024 * 1024));
            for (file, rows) in [
                ("outside.parquet", vec![0]),
                ("table/0.parquet", vec![-1]),
                ("table/0.parquet", vec![2]),
                ("table/0.parquet", vec![0, 0]),
                ("table/0.parquet", vec![0; 11]),
            ] {
                assert!(fetch(
                    &[refs(file, rows)],
                    &allowed,
                    store.clone(),
                    source.schema(),
                    pool.clone()
                )
                .await
                .is_err());
                assert_eq!(pool.reserved(), 0);
            }
            let tiny: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1));
            let failed = fetch(
                &[refs("table/0.parquet", vec![0])],
                &allowed,
                store.clone(),
                source.schema(),
                tiny.clone(),
            )
            .await;
            assert!(failed
                .err()
                .unwrap()
                .to_string()
                .contains("late fetch retained batches"));
            assert_eq!(tiny.reserved(), 0);
            let result = fetch(
                &[refs("table/0.parquet", vec![1, 0])],
                &allowed,
                store,
                source.schema(),
                pool.clone(),
            )
            .await
            .unwrap();
            assert_eq!(result.batches.len(), 2);
            assert!(result.retained_bytes() > 0);
            assert_eq!(result.retained_bytes(), pool.reserved());
            assert_eq!(
                result.batches[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "second"
            );
            drop(result);
            assert_eq!(pool.reserved(), 0);
        });
    }
}
