//! Two fixed SPI plans, owned and used only by the PostgreSQL backend thread.
//! PostgreSQL retains responsibility for dependency invalidation and snapshots;
//! no tuple, argument datum, result or Rust worker reference is retained.
use pgrx::{
    datum::DatumWithOid,
    pg_sys,
    spi::{OwnedPreparedStatement, SpiClient, SpiResult, SpiTupleTable},
};
use std::cell::RefCell;

pub enum Read {
    Catalog,
    Range,
}

const CATALOG_SQL: &str = "SELECT t.name, t.id, s.ordinal, octet_length(s.data)::bigint, CASE WHEN $2 THEN substring(s.data FROM greatest(octet_length(s.data)-8192,0)+1) ELSE NULL::bytea END, concat(s.tableoid, '/', pg_relation_filenode(s.tableoid), '/', s.xmin, '/', s.ctid) FROM analytics.tables t JOIN analytics.segments s ON s.table_id=t.id WHERE t.name=ANY($1) ORDER BY t.name,s.ordinal";
const RANGE_SQL: &str = "SELECT substring(data FROM $3 FOR $4) FROM analytics.segments WHERE table_id=$1 AND ordinal=$2";

thread_local! {
    static PLANS: RefCell<[Option<OwnedPreparedStatement>; 2]> = const { RefCell::new([None, None]) };
}

pub fn select<'conn>(
    client: &SpiClient<'conn>,
    read: Read,
    args: &[DatumWithOid<'_>],
) -> SpiResult<SpiTupleTable<'conn>> {
    let (index, sql, types) = match read {
        Read::Catalog => (
            0,
            CATALOG_SQL,
            vec![pg_sys::TEXTARRAYOID.into(), pg_sys::BOOLOID.into()],
        ),
        Read::Range => (
            1,
            RANGE_SQL,
            vec![
                pg_sys::INT8OID.into(),
                pg_sys::INT4OID.into(),
                pg_sys::INT4OID.into(),
                pg_sys::INT4OID.into(),
            ],
        ),
    };
    if !crate::PREPARED_READS.get() {
        return client.select(sql, None, args);
    }
    PLANS.with(|plans| {
        let mut plans = plans.borrow_mut();
        if plans[index].is_none() {
            plans[index] = Some(client.prepare(sql, &types)?.keep());
        }
        client.select(plans[index].as_ref().unwrap(), None, args)
    })
}
