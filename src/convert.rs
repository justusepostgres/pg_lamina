use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use datafusion::arrow::{
    array::{
        ArrayBuilder, ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int16Builder,
        Int32Builder, Int64Builder, StringBuilder, TimestampMicrosecondBuilder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use datafusion::parquet::{
    arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties,
};
use pgrx::prelude::*;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
pub const SORT_COLUMNS_METADATA: &str = "pg_lamina.sort_columns.v1";
const BATCH_ROWS: i64 = 8192;
const SEGMENT_ROWS: usize = 131_072;

pub fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .enumerate()
            .all(|(i, c)| c == b'_' || c.is_ascii_lowercase() || (i > 0 && c.is_ascii_digit()))
}

fn field_type(oid: u32) -> Result<DataType> {
    Ok(match oid {
        16 => DataType::Boolean,
        20 => DataType::Int64,
        21 => DataType::Int16,
        23 => DataType::Int32,
        1082 => DataType::Date32,
        1114 => DataType::Timestamp(TimeUnit::Microsecond, None),
        701 => DataType::Float64,
        25 | 1042 | 1043 => DataType::Utf8,
        _ => return Err(format!("unsupported PostgreSQL type OID {oid}; supported: boolean, smallint, integer, bigint, date, timestamp, double precision, text, varchar, char").into()),
    })
}

fn builder(ty: &DataType) -> Box<dyn ArrayBuilder> {
    match ty {
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Int16 => Box::new(Int16Builder::new()),
        DataType::Date32 => Box::new(Date32Builder::new()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            Box::new(TimestampMicrosecondBuilder::new())
        }
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        _ => unreachable!("types validated before reading"),
    }
}

fn store_segment(id: i64, ordinal: i32, data: Vec<u8>) -> Result<()> {
    // Build the bytea inside SPI's memory context so each segment's argument
    // is freed on disconnect, rather than retained for the whole conversion.
    Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO analytics.segments(table_id, ordinal, data) VALUES ($1, $2, $3)",
                None,
                &[id.into(), ordinal.into(), data.into()],
            )
            .map(|_| ())
    })?;
    Ok(())
}

fn writer_properties(schema: &Schema) -> Result<WriterProperties> {
    let mut properties = WriterProperties::builder()
        .set_compression(match crate::STORAGE_COMPRESSION.get() {
            crate::StorageCompression::Zstd => Compression::ZSTD(Default::default()),
            crate::StorageCompression::Lz4 => Compression::LZ4_RAW,
        })
        .set_dictionary_page_size_limit(crate::DICTIONARY_LIMIT_KB.get() as usize * 1024)
        .set_max_row_group_row_count(Some(SEGMENT_ROWS));
    let setting = crate::COLUMN_COMPRESSION.get();
    let overrides: serde_json::Value = serde_json::from_str(
        setting
            .as_ref()
            .map(|s| s.to_str())
            .transpose()?
            .unwrap_or("{}"),
    )?;
    let overrides = overrides
        .as_object()
        .ok_or("analytics.column_compression must be a JSON object")?;
    for (name, codec) in overrides {
        schema
            .field_with_name(name)
            .map_err(|_| format!("unknown compression column: {name}"))?;
        let compression = match codec.as_str() {
            Some("zstd") => Compression::ZSTD(Default::default()),
            Some("lz4") => Compression::LZ4_RAW,
            _ => return Err(format!("compression for column {name} must be zstd or lz4").into()),
        };
        // PostgreSQL identifiers containing dots are one field, not nested paths.
        properties = properties.set_column_compression(
            datafusion::parquet::schema::types::ColumnPath::new(vec![name.clone()]),
            compression,
        );
    }
    Ok(properties.build())
}

fn writer(schema: Arc<Schema>, properties: &WriterProperties) -> Result<ArrowWriter<Vec<u8>>> {
    Ok(ArrowWriter::try_new(Vec::new(), schema, Some(properties.clone()))?)
}

/// An offline, transactional cutover. Never CASCADE: dependencies must be migrated explicitly.
pub fn convert(source: pg_sys::Oid, destination: &str, sort_keys: &[String]) -> Result<i64> {
    if !valid_name(destination) {
        return Err("destination must be a lowercase identifier of at most 63 bytes".into());
    }
    let source_oid = source.to_u32();
    let source_name = Spi::get_one::<String>(&format!(
        "SELECT format('%I.%I', n.nspname, c.relname) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE c.oid={source_oid} AND n.nspname NOT IN ('analytics','pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%'"
    ))?.ok_or("source table does not exist or is in a reserved schema")?;

    Spi::run(&format!(
        "LOCK TABLE {source_name} IN ACCESS EXCLUSIVE MODE"
    ))?;
    if Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT pg_catalog.to_regclass($1)::oid",
        &[source_name.as_str().into()],
    )? != Some(source)
    {
        return Err("source identity changed while acquiring conversion lock; retry".into());
    }
    let supported = Spi::get_one::<bool>(&format!(
        "SELECT c.relkind='r' AND c.relpersistence='p' AND a.amname='heap' AND NOT c.relispartition AND NOT EXISTS (SELECT FROM pg_catalog.pg_inherits i WHERE i.inhparent=c.oid OR i.inhrelid=c.oid) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_am a ON a.oid=c.relam WHERE c.oid={source_oid}"
    ))?.unwrap_or(false);
    if !supported {
        return Err("prototype conversion requires an ordinary logged heap table without inheritance or partitions".into());
    }

    let fields: Vec<Field> = Spi::connect(|client| -> Result<_> {
        let rows = client.select(&format!(
            "SELECT attname::text, atttypid::bigint FROM pg_catalog.pg_attribute WHERE attrelid={source_oid} AND attnum>0 AND NOT attisdropped ORDER BY attnum"
        ), None, &[])?;
        rows.map(|row| {
            let name = row.get::<String>(1)?.ok_or("missing column name")?;
            let oid = row.get::<i64>(2)?.ok_or("missing type OID")?;
            Ok(Field::new(name, field_type(oid as u32)?, true))
        })
        .collect()
    })?;
    if fields.is_empty() {
        return Err("zero-column tables are unsupported".into());
    }
    let mut schema = Schema::new(fields);
    let mut seen = BTreeSet::new();
    let mut sort_expressions = Vec::new();
    for key in sort_keys {
        if !seen.insert(key) {
            return Err(format!("duplicate sort key: {key}").into());
        }
        let field = schema.field_with_name(key)?;
        let expression = match field.data_type() {
            DataType::Utf8 => format!("{}::text COLLATE \"C\"", identifier(key)),
            DataType::Boolean
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, None) => identifier(key),
            _ => {
                return Err(format!(
                "unsupported sort key type for {key}; floating point sort keys are not supported"
            )
                .into())
            }
        };
        sort_expressions.push(format!("{expression} ASC NULLS LAST"));
    }
    if !sort_keys.is_empty() {
        schema = schema.with_metadata(HashMap::from([(
            SORT_COLUMNS_METADATA.to_owned(),
            serde_json::to_string(sort_keys)?,
        )]));
    }
    let schema = Arc::new(schema);
    // Validate even empty tables, before registering or reading source rows.
    let properties = writer_properties(&schema)?;
    let order_by = if sort_expressions.is_empty() {
        String::new()
    } else {
        format!(" ORDER BY {}", sort_expressions.join(", "))
    };
    let id = Spi::connect_mut(|client| {
        client
            .update(
                "INSERT INTO analytics.tables(name, source_name) VALUES ($1, $2) RETURNING id",
                None,
                &[destination.into(), source_name.as_str().into()],
            )?
            .first()
            .get_one::<i64>()
    })?
    .ok_or("table registration failed")?;

    let columns = schema
        .fields()
        .iter()
        .map(|f| {
            let name = identifier(f.name());
            if f.data_type() == &DataType::Utf8 {
                // bpchar is not directly compatible with pgrx String. Use PG's
                // text cast (including its removal of fixed-width padding).
                format!("{name}::text")
            } else {
                name
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let cursor_name = Spi::connect(|client| -> Result<String> {
        Ok(client
            .try_open_cursor(
                &format!("SELECT {columns} FROM {source_name}{order_by}"),
                &[],
            )?
            .detach_into_name())
    })?;
    let total = {
        let mut output = writer(schema.clone(), &properties)?;
        let mut segment_rows = 0;
        let mut ordinal = 0;
        let mut total = 0;
        loop {
            pgrx::check_for_interrupts!();
            // A fresh SPI session per fetch frees PostgreSQL's batch allocations.
            let batch = Spi::connect(|client| -> Result<Option<RecordBatch>> {
                let mut cursor = client.find_cursor(&cursor_name)?;
                let rows = cursor.fetch(BATCH_ROWS)?;
                let count = rows.len();
                if count == 0 {
                    return Ok(None);
                } // cursor closes here
                let mut builders = schema
                    .fields()
                    .iter()
                    .map(|f| builder(f.data_type()))
                    .collect::<Vec<_>>();
                for row in rows {
                    for (i, field) in schema.fields().iter().enumerate() {
                        let b = builders[i].as_any_mut();
                        match field.data_type() {
                            DataType::Int16 => b
                                .downcast_mut::<Int16Builder>()
                                .unwrap()
                                .append_option(row.get::<i16>(i + 1)?),
                            DataType::Date32 => {
                                let value = row
                                    .get::<pgrx::datum::Date>(i + 1)?
                                    .map(|date| -> Result<i32> {
                                        if !date.is_finite() {
                                            return Err("infinite dates are unsupported".into());
                                        }
                                        Ok(date
                                            .to_pg_epoch_days()
                                            .checked_add(10_957)
                                            .ok_or("date exceeds Arrow range")?)
                                    })
                                    .transpose()?;
                                b.downcast_mut::<Date32Builder>()
                                    .unwrap()
                                    .append_option(value);
                            }
                            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                                let value = row
                                    .get::<pgrx::datum::Timestamp>(i + 1)?
                                    .map(|timestamp| -> Result<i64> {
                                        let raw = timestamp.into_inner();
                                        if raw == i64::MIN || raw == i64::MAX {
                                            return Err(
                                                "infinite timestamps are unsupported".into()
                                            );
                                        }
                                        Ok(raw
                                            .checked_add(946_684_800_000_000)
                                            .ok_or("timestamp exceeds Arrow range")?)
                                    })
                                    .transpose()?;
                                b.downcast_mut::<TimestampMicrosecondBuilder>()
                                    .unwrap()
                                    .append_option(value);
                            }
                            DataType::Boolean => b
                                .downcast_mut::<BooleanBuilder>()
                                .unwrap()
                                .append_option(row.get::<bool>(i + 1)?),
                            DataType::Int32 => b
                                .downcast_mut::<Int32Builder>()
                                .unwrap()
                                .append_option(row.get::<i32>(i + 1)?),
                            DataType::Int64 => b
                                .downcast_mut::<Int64Builder>()
                                .unwrap()
                                .append_option(row.get::<i64>(i + 1)?),
                            DataType::Float64 => b
                                .downcast_mut::<Float64Builder>()
                                .unwrap()
                                .append_option(row.get::<f64>(i + 1)?),
                            DataType::Utf8 => b
                                .downcast_mut::<StringBuilder>()
                                .unwrap()
                                .append_option(row.get::<String>(i + 1)?),
                            _ => unreachable!(),
                        }
                    }
                }
                let arrays: Vec<ArrayRef> = builders.iter_mut().map(|b| b.finish()).collect();
                let batch = RecordBatch::try_new(schema.clone(), arrays)?;
                cursor.detach_into_name();
                Ok(Some(batch))
            })?;
            let Some(batch) = batch else {
                break;
            };
            let count = batch.num_rows();
            output.write(&batch)?;
            segment_rows += count;
            total += count as i64;
            if segment_rows >= SEGMENT_ROWS {
                store_segment(id, ordinal, output.into_inner()?)?;
                ordinal += 1;
                segment_rows = 0;
                output = writer(schema.clone(), &properties)?;
            }
        }
        // An empty Parquet file preserves the schema of an empty source table.
        if segment_rows > 0 || total == 0 {
            store_segment(id, ordinal, output.into_inner()?)?;
        }
        total
    };
    Spi::run_with_args(
        "UPDATE analytics.tables SET row_count=$1 WHERE id=$2",
        &[total.into(), id.into()],
    )?;
    // No external files are published; both catalog writes and DROP roll back together.
    Spi::run(&format!("DROP TABLE {source_name}"))?;
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn names_and_type_rejection() {
        assert!(valid_name("events_2026"));
        assert!(!valid_name("events; drop table x"));
        assert!(!valid_name("1events"));
        assert_eq!(identifier("a\"b"), "\"a\"\"b\"");
        assert!(field_type(1700).is_err()); // Never silently coerce NUMERIC to float.
    }
}
