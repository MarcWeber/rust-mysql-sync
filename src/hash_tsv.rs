//! Raw TSV line hashing.
//!
//! `mysql --batch --skip-column-names` emits exactly one escaped line per row:
//! tab is `\t`, newline is `\n`, backslash is `\\`, NULL is `\N`. Therefore the
//! line bytes can be hashed without parsing all columns or building `Value`s.
//! Only the PK fields are unescaped, and only when a bucket/row diff actually
//! needs them.

use crate::config::Config;
use crate::export::{self, RawMsg};
use crate::hash::{self, ChunkMode};
use crate::mysqlutil::{self, PkCol};
use crate::proto::RowHash;
use anyhow::{bail, Context, Result};
use mysql_async::Value;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use tokio::sync::mpsc;

/// Extract only the PK fields from a raw TSV line, preserving PK order.
pub fn pk_values_from_line(line: &[u8], pk_indexes: &[usize]) -> Result<Vec<Value>> {
    if pk_indexes.is_empty() {
        bail!("keine PK-Indizes für TSV-Zeile");
    }
    let max_index = *pk_indexes.iter().max().unwrap();
    let mut slots: Vec<Option<Value>> = vec![None; pk_indexes.len()];
    let mut field = 0usize;
    let mut start = 0usize;
    let mut i = 0usize;

    let take_field = |idx: usize, raw: &[u8], slots: &mut Vec<Option<Value>>| -> Result<()> {
        if let Some(pos) = pk_indexes.iter().position(|&x| x == idx) {
            let value = match hash::tsv_unescape_field(raw)? {
                None => Value::NULL,
                Some(bytes) => Value::Bytes(bytes),
            };
            slots[pos] = Some(value);
        }
        Ok(())
    };

    while i < line.len() {
        match line[i] {
            b'\\' => {
                i += 2;
            }
            b'\t' => {
                if field > max_index {
                    break;
                }
                take_field(field, &line[start..i], &mut slots)?;
                field += 1;
                start = i + 1;
                if field > max_index {
                    break;
                }
                i += 1;
            }
            _ => i += 1,
        }
    }
    if field <= max_index {
        take_field(field, &line[start..line.len()], &mut slots)?;
    }

    slots
        .into_iter()
        .enumerate()
        .map(|(i, v)| v.with_context(|| format!("PK-Feld {} fehlt in TSV-Zeile", pk_indexes[i])))
        .collect()
}

/// Streamt abgeschlossene Bucket-Hashes in Sortierreihenfolge.
#[allow(clippy::too_many_arguments)]
pub async fn stream_chunk_hashes(
    tx: mpsc::Sender<hash::ChunkHashEvent>,
    cfg: &Config,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
) -> Result<u64> {
    let algo = cfg.hash_algo();
    let order = crate::sqlhash::order_by_bucket(pks, mode, chunked)?;
    let mut rx = export::spawn_raw_tsv(cfg, db.to_string(), table.to_string(), None, Some(order));
    let mut acc = hash::ChunkAccumulator::new(algo);
    let mut total = 0u64;
    while let Some(msg) = rx.recv().await {
        match msg {
            RawMsg::Line(line) => {
                let pk_values = pk_values_from_line(&line, pk_indexes)?;
                let bucket = hash::chunk_key_for_pk_values(&pk_values, pks, mode, chunked)?;
                let rh = hash::hash_bytes(&line, algo);
                if let Some(item) = acc.push(bucket, &rh) {
                    tx.send(hash::ChunkHashEvent::Item(item))
                        .await
                        .map_err(|_| anyhow::anyhow!("Chunk-Hash-Empfänger geschlossen"))?;
                    total += 1;
                }
            }
            RawMsg::Err(e) => bail!("raw-tsv Export-Fehler: {e}"),
            RawMsg::Done => break,
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

pub async fn chunk_hashes<F>(
    cfg: &Config,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    mut on_row: F,
) -> Result<HashMap<String, (u64, String)>>
where
    F: FnMut(),
{
    let algo = cfg.hash_algo();
    let mut rx = export::spawn_raw_tsv(
        cfg,
        db.to_string(),
        table.to_string(),
        None,
        Some(mysqlutil::order_by(pks)),
    );
    let mut acc: HashMap<String, (hash::RowHasher, u64)> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            RawMsg::Line(line) => {
                let pk_values = pk_values_from_line(&line, pk_indexes)?;
                let bucket = hash::chunk_key_for_pk_values(&pk_values, pks, mode, chunked)?;
                let rh = hash::hash_bytes(&line, algo);
                hash::add_row_hash(&mut acc, algo, bucket, &rh);
                on_row();
            }
            RawMsg::Err(e) => bail!("raw-tsv Export-Fehler: {e}"),
            RawMsg::Done => break,
        }
    }
    Ok(hash::finish_chunks(acc))
}

#[allow(clippy::too_many_arguments)]
pub async fn row_hashes(
    cfg: &Config,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
) -> Result<Vec<RowHash>> {
    let mut out = Vec::new();
    for_each_row_hash(
        cfg,
        db,
        table,
        pks,
        pk_indexes,
        mode,
        chunked,
        buckets,
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
    cfg: &Config,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_indexes: &[usize],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    mut f: F,
) -> Result<()>
where
    F: FnMut(RowHash) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let where_clause = mysqlutil::chunk_where(pks, mode, chunked, buckets)?;
    let wanted: HashSet<String> = buckets.iter().cloned().collect();
    let algo = cfg.hash_algo();
    let mut rx = export::spawn_raw_tsv(
        cfg,
        db.to_string(),
        table.to_string(),
        Some(where_clause),
        Some(mysqlutil::order_by(pks)),
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            RawMsg::Line(line) => {
                let pk_values = pk_values_from_line(&line, pk_indexes)?;
                let bucket = hash::chunk_key_for_pk_values(&pk_values, pks, mode, chunked)?;
                if !wanted.contains(&bucket) {
                    continue;
                }
                let item = RowHash {
                    bucket,
                    pk: hash::pk_components_from_values(&pk_values, pks),
                    hash: hash::hash_bytes(&line, algo),
                };
                f(item).await?;
            }
            RawMsg::Err(e) => bail!("raw-tsv Export-Fehler: {e}"),
            RawMsg::Done => break,
        }
    }
    Ok(())
}
