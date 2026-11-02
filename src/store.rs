//! DataFusion requests owned byte ranges; only the backend services PostgreSQL I/O.
use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, *};
use std::{collections::BTreeMap, fmt, ops::Range, sync::mpsc::Sender};

#[derive(Debug, Clone)]
pub struct Segment {
    pub table_id: i64,
    pub ordinal: i32,
    pub meta: ObjectMeta,
    pub tail: Bytes,
}

pub enum Event {
    Read {
        table_id: i64,
        ordinal: i32,
        range: Range<u64>,
        reply: tokio::sync::oneshot::Sender<std::result::Result<Bytes, String>>,
    },
    Complete(std::result::Result<Execution, String>),
}

pub struct Execution {
    pub rows: Vec<serde_json::Value>,
    pub phases_ms: serde_json::Map<String, serde_json::Value>,
    pub plan_cache_hit: bool,
    pub plan_cache_bytes: usize,
    pub late_materialization: bool,
    pub late_retained_bytes: usize,
}

#[derive(Debug)]
pub struct PgStore {
    pub segments: BTreeMap<Path, Segment>,
    pub sender: Sender<Event>,
}

impl fmt::Display for PgStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PostgreSQL segment range reader")
    }
}

fn failure(message: impl ToString) -> Error {
    Error::Generic {
        store: "pg_lamina",
        source: message.to_string().into(),
    }
}

fn read_only() -> Error {
    Error::NotSupported {
        source: "analytical storage is read-only".into(),
    }
}

#[async_trait]
impl ObjectStore for PgStore {
    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let segment = self.segments.get(location).ok_or_else(|| Error::NotFound {
            path: location.to_string(),
            source: "segment not in statement catalog".into(),
        })?;
        options.check_preconditions(&segment.meta)?;
        if options.version.is_some() {
            return Err(read_only());
        }
        let range = match &options.range {
            Some(range) => range.as_range(segment.meta.size).map_err(failure)?,
            None => 0..segment.meta.size,
        };
        let tail_start = segment.meta.size - segment.tail.len() as u64;
        let bytes = if options.head || range.is_empty() {
            Bytes::new()
        } else if range.start >= tail_start {
            segment
                .tail
                .slice((range.start - tail_start) as usize..(range.end - tail_start) as usize)
        } else {
            let (reply, response) = tokio::sync::oneshot::channel();
            self.sender
                .send(Event::Read {
                    table_id: segment.table_id,
                    ordinal: segment.ordinal,
                    range: range.clone(),
                    reply,
                })
                .map_err(failure)?;
            response.await.map_err(failure)?.map_err(failure)?
        };
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(bytes) }).boxed()),
            meta: segment.meta.clone(),
            range,
            attributes: Attributes::default(),
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let objects = self
            .segments
            .values()
            .filter(|s| prefix.is_none_or(|p| s.meta.location.prefix_matches(p)))
            .map(|s| Ok(s.meta.clone()))
            .collect::<Vec<_>>();
        stream::iter(objects).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let objects = self.list(prefix).try_collect().await?;
        // The engine lists flat per-table directories; nested directory discovery is unsupported.
        Ok(ListResult {
            common_prefixes: Vec::new(),
            objects,
        })
    }
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        Err(read_only())
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(read_only())
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        locations.map(|_| Err(read_only())).boxed()
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(read_only())
    }
}
