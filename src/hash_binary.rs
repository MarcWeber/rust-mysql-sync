//! Raw MySQL binary-protocol row hashing.
//!
//! Rows are read through prepared statements (`exec_iter`) and hashed directly
//! from the binary row payload. No `Vec<Value>` is built for the common path.
//! PK fields are decoded only when a bucket or row diff needs them.
//!
//! The MySQL binary row payload is deterministic for a fixed server/session/
//! schema profile: integers are little-endian fixed width, strings are length-
//! encoded bytes, and the NULL bitmap has a stable layout. We mask unused high
//! bits in the NULL bitmap before hashing so padding differences cannot leak
//! into the digest.

use crate::hash::{self, ChunkMode};
use crate::mysqlutil::{self, PkCol};
use crate::proto::RowHash;
use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Column, Conn, Value};
use mysql_common::io::ParseBuf;
use mysql_common::value::{BinValue, ValueDeserializer};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use tokio::sync::mpsc;

fn row_sql(db: &str, table: &str, where_clause: Option<&str>, order_by: Option<&str>) -> String {
    let mut sql = format!("SELECT * FROM {}", mysqlutil::qualified(db, table));
    if let Some(w) = where_clause {
        if !w.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        }
    }
    if let Some(o) = order_by {
        sql.push_str(o);
    }
    sql
}

async fn for_each_raw_sync<F>(conn: &mut Conn, sql: String, mut f: F) -> Result<()>
where
    F: FnMut(&[Column], &[u8]) -> Result<()>,
{
    let mut result = conn
        .exec_iter(sql, ())
        .await
        .context("binäres SELECT fehlgeschlagen")?;
    while let Some((columns, packet)) = result
        .next_raw()
        .await
        .context("binäre Zeile lesen fehlgeschlagen")?
    {
        f(&columns, &packet)?;
    }
    Ok(())
}

fn null_bitmap_len(columns: usize) -> usize {
    (columns + 7 + 2) / 8
}

/// Hash a binary row payload, normalizing unused NULL-bitmap bits.
pub fn hash_packet(packet: &[u8], columns: usize, algo: hash::HashAlgo) -> Result<String> {
    if packet.is_empty() {
        bail!("leeres binäres Row-Paket");
    }
    let bitmap_len = null_bitmap_len(columns);
    if packet.len() < 1 + bitmap_len {
        bail!(
            "binäres Row-Paket zu kurz: {} < {}",
            packet.len(),
            1 + bitmap_len
        );
    }
    let mut h = hash::RowHasher::new(algo);
    h.update(&packet[..1]);
    let bitmap = &packet[1..1 + bitmap_len];
    if bitmap_len > 0 {
        let (last, rest) = bitmap.split_last().unwrap();
        h.update(rest);
        let used_bits = columns + 2 - 8 * (bitmap_len - 1);
        let mask = if used_bits >= 8 {
            0xFF
        } else {
            (1u8 << used_bits) - 1
        };
        h.update(&[last & mask]);
    }
    h.update(&packet[1 + bitmap_len..]);
    Ok(h.finalize_hex())
}

/// Decode only the PK fields from a binary row payload.
pub fn pk_values_from_packet(
    packet: &[u8],
    columns: &[Column],
    pk_indexes: &[usize],
) -> Result<Vec<Value>> {
    if pk_indexes.is_empty() {
        bail!("keine PK-Indizes für binäre Zeile");
    }
    let max_index = *pk_indexes.iter().max().unwrap();
    if max_index >= columns.len() {
        bail!(
            "PK-Index {} außerhalb der Spaltenanzahl {}",
            max_index,
            columns.len()
        );
    }
    let mut slots: Vec<Option<Value>> = vec![None; pk_indexes.len()];
    let mut buf = ParseBuf(packet);
    buf.checked_eat_u8()
        .context("binäres Row-Paket ohne Header")?;
    let bitmap_len = null_bitmap_len(columns.len());
    let bitmap = buf
        .checked_eat(bitmap_len)
        .context("binäres Row-Paket ohne NULL-Bitmap")?;

    for i in 0..=max_index {
        let is_null = bitmap[i / 8] & (1 << ((i + 2) % 8)) != 0;
        let pos = pk_indexes.iter().position(|&x| x == i);
        if is_null {
            if let Some(pos) = pos {
                slots[pos] = Some(Value::NULL);
            }
            continue;
        }
        let col = &columns[i];
        let value: ValueDeserializer<BinValue> = buf
            .parse_unchecked((col.column_type(), col.flags()))
            .with_context(|| format!("binären Wert für Spalte {i} nicht lesbar"))?;
        if let Some(pos) = pos {
            slots[pos] = Some(value.0);
        }
    }

    slots
        .into_iter()
        .enumerate()
        .map(|(i, v)| v.with_context(|| format!("PK-Spalte {} fehlt im Row-Paket", pk_indexes[i])))
        .collect()
}

/// Streamt abgeschlossene Bucket-Hashes in Sortierreihenfolge.
#[allow(clippy::too_many_arguments)]
pub async fn stream_chunk_hashes(
    tx: mpsc::Sender<hash::ChunkHashEvent>,
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    algo: hash::HashAlgo,
) -> Result<u64> {
    let order = crate::sqlhash::order_by_bucket(pks, mode, chunked)?;
    let sql = row_sql(db, table, None, Some(&order));
    let mut result = conn
        .exec_iter(sql, ())
        .await
        .context("binäres SELECT fehlgeschlagen")?;
    let mut acc = hash::ChunkAccumulator::new(algo);
    let mut total = 0u64;
    while let Some((columns, packet)) = result
        .next_raw()
        .await
        .context("binäre Zeile lesen fehlgeschlagen")?
    {
        let pk_values = pk_values_from_packet(&packet, &columns, pk_indexes)?;
        let bucket = hash::chunk_key_for_pk_values(&pk_values, pks, mode, chunked)?;
        let rh = hash_packet(&packet, columns.len(), algo)?;
        if let Some(item) = acc.push(bucket, &rh) {
            send_chunk(&tx, item).await?;
            total += 1;
        }
    }
    if let Some(item) = acc.finish() {
        send_chunk(&tx, item).await?;
        total += 1;
    }
    Ok(total)
}

async fn send_chunk(
    tx: &mpsc::Sender<hash::ChunkHashEvent>,
    item: hash::ChunkHashValue,
) -> Result<()> {
    tx.send(hash::ChunkHashEvent::Item(item))
        .await
        .map_err(|_| anyhow::anyhow!("Chunk-Hash-Empfänger geschlossen"))
}

pub async fn chunk_hashes<F>(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    algo: hash::HashAlgo,
    mut on_row: F,
) -> Result<HashMap<String, (u64, String)>>
where
    F: FnMut(),
{
    let sql = row_sql(db, table, None, Some(&mysqlutil::order_by(pks)));
    let mut acc: HashMap<String, (hash::RowHasher, u64)> = HashMap::new();
    for_each_raw_sync(conn, sql, |columns, packet| {
        let pk_values = pk_values_from_packet(packet, columns, pk_indexes)?;
        let bucket = hash::chunk_key_for_pk_values(&pk_values, pks, mode, chunked)?;
        let rh = hash_packet(packet, columns.len(), algo)?;
        hash::add_row_hash(&mut acc, algo, bucket, &rh);
        on_row();
        Ok(())
    })
    .await?;
    Ok(hash::finish_chunks(acc))
}

#[allow(clippy::too_many_arguments)]
pub async fn row_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    algo: hash::HashAlgo,
) -> Result<Vec<RowHash>> {
    let mut out = Vec::new();
    for_each_row_hash(
        conn,
        db,
        table,
        pks,
        pk_indexes,
        mode,
        chunked,
        buckets,
        algo,
        |item| {
            out.push(item);
            std::future::ready(Ok::<(), anyhow::Error>(()))
        },
    )
    .await?;
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
pub async fn for_each_row_hash<F, Fut>(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    algo: hash::HashAlgo,
    mut f: F,
) -> Result<()>
where
    F: FnMut(RowHash) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let where_clause = mysqlutil::chunk_where(pks, mode, chunked, buckets)?;
    let wanted: HashSet<String> = buckets.iter().cloned().collect();
    let sql = row_sql(
        db,
        table,
        Some(&where_clause),
        Some(&mysqlutil::order_by(pks)),
    );
    let mut result = conn
        .exec_iter(sql, ())
        .await
        .context("binäres SELECT fehlgeschlagen")?;
    while let Some((columns, packet)) = result
        .next_raw()
        .await
        .context("binäre Zeile lesen fehlgeschlagen")?
    {
        let pk_values = pk_values_from_packet(&packet, &columns, pk_indexes)?;
        let bucket = hash::chunk_key_for_pk_values(&pk_values, pks, mode, chunked)?;
        if !wanted.contains(&bucket) {
            continue;
        }
        f(RowHash {
            bucket,
            pk: hash::pk_components_from_values(&pk_values, pks),
            hash: hash_packet(&packet, columns.len(), algo)?,
        })
        .await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Raw binary protocol -> decoded `Value` rows.
//
// These paths still use prepared statements / the binary protocol, but decode
// the complete row before hashing with the canonical `hash::row_hash` encoding.
// Unlike packet hashing they are safe across MySQL versions, because version-
// specific wire-format details are normalized by `mysql_async`.
// ---------------------------------------------------------------------------

async fn for_each_value_sync<F>(conn: &mut Conn, sql: String, mut f: F) -> Result<()>
where
    F: FnMut(Vec<Value>) -> Result<()>,
{
    let mut result = conn
        .exec_iter(sql, ())
        .await
        .context("binäres SELECT fehlgeschlagen")?;
    while let Some(row) = result
        .next()
        .await
        .context("binäre Zeile dekodieren fehlgeschlagen")?
    {
        f(row.unwrap())?;
    }
    Ok(())
}

/// Stream bucket hashes from decoded binary-protocol rows.
#[allow(clippy::too_many_arguments)]
pub async fn stream_chunk_hashes_vec(
    tx: mpsc::Sender<hash::ChunkHashEvent>,
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    algo: hash::HashAlgo,
    opts: &[crate::proto::FieldOpt],
) -> Result<u64> {
    let order = crate::sqlhash::order_by_bucket(pks, mode, chunked)?;
    let sql = row_sql(db, table, None, Some(&order));
    let mut result = conn
        .exec_iter(sql, ())
        .await
        .context("binäres SELECT fehlgeschlagen")?;
    let mut acc = hash::ChunkAccumulator::new(algo);
    let mut total = 0u64;
    while let Some(row) = result
        .next()
        .await
        .context("binäre Zeile dekodieren fehlgeschlagen")?
    {
        let values = row.unwrap();
        let bucket = hash::chunk_key_for_row(&values, pk_indexes, pks, mode, chunked)?;
        let rh = hash::row_hash_opts(&values, algo, opts);
        if let Some(item) = acc.push(bucket, &rh) {
            send_chunk(&tx, item).await?;
            total += 1;
        }
    }
    if let Some(item) = acc.finish() {
        tx.send(hash::ChunkHashEvent::Item(item))
            .await
            .map_err(|_| anyhow::anyhow!("Chunk-Hash-Empfänger geschlossen"))?;
        total += 1;
    }
    Ok(total)
}

/// Collect bucket hashes from decoded binary-protocol rows.
pub async fn chunk_hashes_vec<F>(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    algo: hash::HashAlgo,
    opts: &[crate::proto::FieldOpt],
    mut on_row: F,
) -> Result<HashMap<String, (u64, String)>>
where
    F: FnMut(),
{
    let sql = row_sql(db, table, None, Some(&mysqlutil::order_by(pks)));
    let mut acc: HashMap<String, (hash::RowHasher, u64)> = HashMap::new();
    for_each_value_sync(conn, sql, |values| {
        let bucket = hash::chunk_key_for_row(&values, pk_indexes, pks, mode, chunked)?;
        let rh = hash::row_hash_opts(&values, algo, opts);
        hash::add_row_hash(&mut acc, algo, bucket, &rh);
        on_row();
        Ok(())
    })
    .await?;
    Ok(hash::finish_chunks(acc))
}

/// Row hashes for selected buckets from decoded binary-protocol rows.
#[allow(clippy::too_many_arguments)]
pub async fn row_hashes_vec(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    algo: hash::HashAlgo,
    opts: &[crate::proto::FieldOpt],
) -> Result<Vec<RowHash>> {
    let where_clause = mysqlutil::chunk_where(pks, mode, chunked, buckets)?;
    let wanted: HashSet<String> = buckets.iter().cloned().collect();
    let sql = row_sql(
        db,
        table,
        Some(&where_clause),
        Some(&mysqlutil::order_by(pks)),
    );
    let mut out = Vec::new();
    for_each_value_sync(conn, sql, |values| {
        let bucket = hash::chunk_key_for_row(&values, pk_indexes, pks, mode, chunked)?;
        if !wanted.contains(&bucket) {
            return Ok(());
        }
        out.push(RowHash {
            bucket,
            pk: hash::pk_components(&values, pk_indexes, pks),
            hash: hash::row_hash_opts(&values, algo, opts),
        });
        Ok(())
    })
    .await?;
    Ok(out)
}
