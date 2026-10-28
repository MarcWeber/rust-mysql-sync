//! Sync-Engine (Client-Seite): Schema-Abgleich (--force / --force-schema),
//! differentieller Datenabgleich (--sync) und Vergleichsmodus (--check).
//!
//! Algorithmus pro Tabelle (ein QUIC-Stream, parallel über concurrent_processes):
//!  1. describe/count lokal + remote
//!  2. force: Schema erzwingen (create / drop+create)
//!  3. sync:  Chunk-Hashes beider Seiten → differierende Buckets
//!            → diff% > diff_threshold_percent ? Vollkopie : Drill-Down
//!            → Zeilen-Hashes → sende_csv / sende_dump + DeletePks
//!  4. check: wie 3, aber ohne Schreibzugriffe → Report + Beispiel-IDs

use crate::config::Config;
use crate::export::{self, RowMsg};
use crate::hash;
use crate::mysqlutil::{self, PkCol};
use crate::progress::Progress;
use crate::proto::{self, ChunkHash, Request, Response};
use anyhow::{bail, Context, Result};
use mysql_async::{Conn, Value};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

pub struct TableTask {
    pub local_db: String,
    pub local_table: String,
    pub remote_db: String,
    pub remote_table: String,
}

pub struct TableResult {
    pub label: String,
    pub ok: bool,
    pub mode: Mode,
    pub message: String,
    pub rows_sent: u64,
    pub rows_deleted: u64,
    pub differ_count: Option<u64>,
    pub examples: Vec<String>,
}

#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    /// Daten differentiell abgleichen
    Sync,
    /// Nur Schema erzwingen (kein Datentransfer)
    Force,
    /// Nur vergleichen, kein Schreibzugriff
    Check,
}

pub async fn run_table_task(
    cfg: Config,
    conn: quinn::Connection,
    pool: mysql_async::Pool,
    task: TableTask,
    do_force: bool,
    mode: Mode,
    check_examples: u32,
    progress: Progress,
) -> TableResult {
    let label = format!("{}.{}", task.local_db, task.local_table);
    match run_inner(&cfg, &conn, &pool, &task, do_force, mode, check_examples, &progress).await {
        Ok((sent, deleted, differ, examples)) => TableResult {
            label,
            ok: true,
            mode,
            message: if mode == Mode::Check {
                let d = differ.unwrap_or(0);
                format!("{d} Zeilen differieren")
            } else if mode == Mode::Force {
                "Schema erzwungen".to_string()
            } else {
                format!("{sent} gesendet, {deleted} gelöscht")
            },
            rows_sent: sent,
            rows_deleted: deleted,
            differ_count: differ,
            examples,
        },
        Err(e) => TableResult {
            label,
            ok: false,
            mode,
            message: format!("{e:#}"),
            rows_sent: 0,
            rows_deleted: 0,
            differ_count: None,
            examples: vec![],
        },
    }
}

type SyncStats = (u64, u64, Option<u64>, Vec<String>);

#[allow(clippy::too_many_arguments)]
async fn run_inner(
    cfg: &Config,
    conn: &quinn::Connection,
    pool: &mysql_async::Pool,
    task: &TableTask,
    do_force: bool,
    mode: Mode,
    check_examples: u32,
    progress: &Progress,
) -> Result<SyncStats> {
    let label = format!("{}.{}", task.local_db, task.local_table);
    let (mut send, mut recv) = conn.open_bi().await.context("QUIC-Stream öffnen")?;
    let max_frame = cfg.max_frame;
    let bar = progress.bar(&label, 0);

    // ---- 1. Schema & Counts ----
    bar.phase("Schema/Counts");
    let mut local_conn = mysqlutil::session_conn(pool).await?;
    let local_pks: Vec<PkCol> = mysqlutil::pk_columns(&mut local_conn, &task.local_db, &task.local_table).await?;
    if local_pks.is_empty() {
        bail!("{label} hat keinen Primary Key — Sync nicht möglich");
    }
    let pk_names: Vec<String> = local_pks.iter().map(|p| p.name.clone()).collect();
    let local_info = mysqlutil::table_info(&mut local_conn, &task.local_db, &task.local_table).await?;

    proto::write_json(&mut send, &Request::DescribeCount { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
    let remote_info = match read_response(&mut recv, max_frame).await? {
        Response::SchemaInfo { create_sql, columns, count } => Some((create_sql, columns, count)),
        Response::Err { message } if message.contains("nicht gefunden") || message.contains("doesn't exist") => None,
        Response::Err { message } => bail!("{message}"),
        _ => bail!("Unerwartete Antwort auf DescribeCount"),
    };

    // ---- 2. Force / Schema-Abgleich ----
    if do_force {
        bar.phase("Force-Schema");
        match &remote_info {
            None => {
                proto::write_json(&mut send, &Request::Recreate { db: task.remote_db.clone(), sql: local_info.create_sql.clone() }, max_frame).await?;
                expect_ok(&mut recv, max_frame, "Recreate").await?;
                log::info!("[{label}] Tabelle auf Ziel erstellt");
            }
            Some((remote_create, _, _)) => {
                if mysqlutil::normalize_schema(&local_info.create_sql) != mysqlutil::normalize_schema(remote_create) {
                    log::warn!("[{label}] Schema weicht ab — Tabelle wird neu erstellt (Daten gehen verloren!)");
                    proto::write_json(&mut send, &Request::Recreate { db: task.remote_db.clone(), sql: local_info.create_sql.clone() }, max_frame).await?;
                    expect_ok(&mut recv, max_frame, "Recreate").await?;
                } else {
                    log::debug!("[{label}] Schema identisch — kein Recreate");
                }
            }
        }
    }

    if mode == Mode::Check {
        // Nur vergleichen: Chunk- und Zeilen-Diffs ohne Schreibzugriffe
        let stats = diff_and_report(
            cfg, &mut send, &mut recv, &mut local_conn, &local_pks, &local_info, task, check_examples, &bar,
        )
        .await?;
        bar.done(&format!("{} Zeilen differieren", stats.0));
        return Ok((0, 0, Some(stats.0), stats.1));
    }

    if mode == Mode::Force {
        bar.done("Schema erzwungen (kein Datentransfer)");
        return Ok((0, 0, None, vec![]));
    }

    // ---- 3. Sync ----
    // Remote-Schema nochmal holen (nach Recreate evtl. neu)
    let remote_info2 = match &remote_info {
        Some(_) if !do_force => remote_info.clone(),
        _ => {
            proto::write_json(&mut send, &Request::DescribeCount { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
            match read_response(&mut recv, max_frame).await? {
                Response::SchemaInfo { create_sql, columns, count } => Some((create_sql, columns, count)),
                Response::Err { message } => bail!("{message}"),
                _ => bail!("Unerwartete Antwort"),
            }
        }
    };
    let (remote_create, _, remote_count) = match remote_info2 {
        Some(x) => x,
        None => bail!("{label}: Zieltabelle fehlt und kein --force — Abbruch"),
    };
    if mysqlutil::normalize_schema(&local_info.create_sql) != mysqlutil::normalize_schema(&remote_create) {
        bail!("{label}: Schemas unterscheiden sich — mit --force/--force-schema Schema abgleichen");
    }

    let hex_indexes = mysqlutil::binary_column_indexes(&mut local_conn, &task.local_db, &task.local_table).await?;

    // Vollkopie-Kurzschluss: Ziel leer → alles senden
    if remote_count == 0 && local_info.count > 0 {
        bar.set_total(local_info.count, "Vollkopie (Ziel leer)");
        let sent = transfer_all_rows(cfg, &mut send, &mut recv, &mut local_conn, task, &hex_indexes, &local_info.columns, None, &bar).await?;
        bar.done(&format!("{sent} Zeilen gesendet"));
        return Ok((sent, 0, Some(local_info.count), vec![]));
    }

    // Chunk-Hashes lokal
    bar.set_total(local_info.count, "Lokale Chunk-Hashes");
    let local_chunks = compute_chunk_hashes(&mut local_conn, &task.local_db, &task.local_table, &local_pks, &pk_names, cfg.chunked, Some(&bar)).await?;

    // Chunk-Hashes remote (Stream)
    bar.phase("Remote Chunk-Hashes");
    proto::write_json(
        &mut send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked: cfg.chunked,
            hash_of_chunk: true,
            buckets: None,
        },
        max_frame,
    )
    .await?;
    let mut remote_chunks: HashMap<String, ChunkHash> = HashMap::new();
    loop {
        let resp = read_response(&mut recv, max_frame).await?;
        match resp {
            Response::ChunkHashes { items } => {
                bar.inc(items.len() as u64);
                for it in items {
                    remote_chunks.insert(it.bucket.clone(), it);
                }
            }
            Response::Ok { .. } => break,
            Response::Err { message } => bail!("{message}"),
            _ => bail!("Unerwartete Antwort auf Hashes"),
        }
    }
    bar.done(&format!("{} Buckets verglichen", local_chunks.len().max(remote_chunks.len())));

    // Differierende Buckets
    let mut diff_buckets: Vec<String> = Vec::new();
    for (b, lh) in &local_chunks {
        match remote_chunks.get(b) {
            None => diff_buckets.push(b.clone()),
            Some(rh) => {
                if rh.hash != lh.1 || rh.count != lh.0 {
                    diff_buckets.push(b.clone());
                }
            }
        }
    }
    for b in remote_chunks.keys() {
        if !local_chunks.contains_key(b) {
            diff_buckets.push(b.clone());
        }
    }
    diff_buckets.sort();

    let total_buckets = local_chunks.len().max(remote_chunks.len()).max(1) as f64;
    let diff_pct = diff_buckets.len() as f64 * 100.0 / total_buckets;
    log::info!(
        "[{label}] {} differierende Buckets von {} ({:.1}%, Threshold {:.1}%)",
        diff_buckets.len(),
        total_buckets as u64,
        diff_pct,
        cfg.diff_threshold_percent
    );

    if diff_buckets.is_empty() {
        bar.done("identisch");
        return Ok((0, 0, Some(0), vec![]));
    }

    // Vollkopie bei zu hohem Diff-Anteil?
    if diff_pct > cfg.diff_threshold_percent {
        log::info!("[{label}] Über Threshold — Vollkopie (Truncate + alle Zeilen)");
        bar.phase("Vollkopie");
        if cfg.disable_triggers {
            proto::write_json(&mut send, &Request::DisableTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
            expect_ok(&mut recv, max_frame, "DisableTriggers").await?;
        }
        proto::write_json(&mut send, &Request::Truncate { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
        expect_ok(&mut recv, max_frame, "Truncate").await?;
        let sent = transfer_all_rows(cfg, &mut send, &mut recv, &mut local_conn, task, &hex_indexes, &local_info.columns, None, &bar).await?;
        if cfg.disable_triggers {
            proto::write_json(&mut send, &Request::RestoreTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
            expect_ok(&mut recv, max_frame, "RestoreTriggers").await?;
        }
        bar.done(&format!("Vollkopie: {sent} Zeilen"));
        return Ok((sent, 0, Some(local_info.count), vec![]));
    }

    // ---- Drill-Down: Zeilen-Hashes beider Seiten ----
    bar.phase("Drill-Down (Zeilen-Hashes)");
    let local_rows = compute_row_hashes(
        &mut local_conn, &task.local_db, &task.local_table, &local_pks, &pk_names, cfg.chunked, &diff_buckets, Some(&bar),
    )
    .await?;

    proto::write_json(
        &mut send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked: cfg.chunked,
            hash_of_chunk: false,
            buckets: Some(diff_buckets.clone()),
        },
        max_frame,
    )
    .await?;
    let mut remote_rows: HashMap<String, String> = HashMap::new();
    loop {
        let resp = read_response(&mut recv, max_frame).await?;
        match resp {
            Response::RowHashes { items } => {
                bar.inc(items.len() as u64);
                for it in items {
                    remote_rows.insert(it.pk, it.hash);
                }
            }
            Response::Ok { .. } => break,
            Response::Err { message } => bail!("{message}"),
            _ => bail!("Unerwartete Antwort auf RowHashes"),
        }
    }

    let mut to_send: Vec<String> = Vec::new(); // PKs lokal vorhanden, remote fehlen/abweichen
    let mut to_delete: Vec<String> = Vec::new(); // PKs remote vorhanden, lokal fehlen
    for (pk, lh) in &local_rows {
        match remote_rows.get(pk) {
            None => to_send.push(pk.clone()),
            Some(rh) => {
                if rh != lh {
                    to_send.push(pk.clone());
                }
            }
        }
    }
    for pk in remote_rows.keys() {
        if !local_rows.contains_key(pk) {
            to_delete.push(pk.clone());
        }
    }

    let examples: Vec<String> = {
        let mut ex: Vec<String> = to_send.iter().take(check_examples as usize).cloned().collect();
        for pk in to_delete.iter() {
            if ex.len() >= check_examples as usize {
                break;
            }
            ex.push(pk.clone());
        }
        ex
    };

    if mode == Mode::Check {
        let differ = to_send.len() as u64 + to_delete.len() as u64;
        bar.done(&format!("{differ} Zeilen differieren"));
        return Ok((0, 0, Some(differ), examples));
    }

    // ---- Änderungen senden ----
    bar.set_total((to_send.len() + to_delete.len()) as u64, "Sende Änderungen");
    let mut sent_total = 0u64;
    let mut deleted_total = 0u64;

    if !to_send.is_empty() {
        if cfg.disable_triggers {
            proto::write_json(&mut send, &Request::DisableTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
            expect_ok(&mut recv, max_frame, "DisableTriggers").await?;
        }
        sent_total = send_changed_rows(cfg, &mut send, &mut recv, &mut local_conn, task, &local_pks, &pk_names, &hex_indexes, &local_info.columns, &local_chunks, &diff_buckets, &to_send, &bar).await?;
        if !to_delete.is_empty() {
            for chunk in to_delete.chunks(1000) {
                proto::write_json(
                    &mut send,
                    &Request::DeletePks { db: task.remote_db.clone(), table: task.remote_table.clone(), pks: chunk.to_vec() },
                    max_frame,
                )
                .await?;
                expect_ok(&mut recv, max_frame, "DeletePks").await?;
                deleted_total += chunk.len() as u64;
                bar.inc(chunk.len() as u64);
            }
        }
        if cfg.disable_triggers {
            proto::write_json(&mut send, &Request::RestoreTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, max_frame).await?;
            expect_ok(&mut recv, max_frame, "RestoreTriggers").await?;
        }
    } else if !to_delete.is_empty() {
        for chunk in to_delete.chunks(1000) {
            proto::write_json(
                &mut send,
                &Request::DeletePks { db: task.remote_db.clone(), table: task.remote_table.clone(), pks: chunk.to_vec() },
                max_frame,
            )
            .await?;
            expect_ok(&mut recv, max_frame, "DeletePks").await?;
            deleted_total += chunk.len() as u64;
            bar.inc(chunk.len() as u64);
        }
    }

    bar.done(&format!("{sent_total} gesendet, {deleted_total} gelöscht"));
    Ok((sent_total, deleted_total, Some(to_send.len() as u64 + to_delete.len() as u64), examples))
}

async fn read_response(recv: &mut quinn::RecvStream, max_frame: usize) -> Result<Response> {
    match proto::read_frame(recv, max_frame).await? {
        Some((proto::TAG_JSON, data)) => Ok(serde_json::from_slice(&data).context("Response-JSON ungültig")?),
        Some(_) => bail!("Unerwarteter Binär-Frame vom Server"),
        None => bail!("Server hat den Stream geschlossen"),
    }
}

async fn expect_ok(recv: &mut quinn::RecvStream, max_frame: usize, what: &str) -> Result<()> {
    match read_response(recv, max_frame).await? {
        Response::Ok { .. } => Ok(()),
        Response::Err { message } => bail!("{what} fehlgeschlagen: {message}"),
        _ => bail!("{what}: unerwartete Antwort"),
    }
}

/// Lokale Chunk-Hashes (ein Scan, ORDER BY pk).
async fn compute_chunk_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_names: &[String],
    chunked: u32,
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, (u64, String)>> {
    let names = mysqlutil::column_names(conn, db, table).await?;
    let mut pk_indexes = Vec::new();
    for pk in pk_names {
        let i = names.iter().position(|n| n == pk).with_context(|| format!("PK-Spalte {pk} fehlt lokal"))?;
        pk_indexes.push(i);
    }
    let numeric_last = pks.last().map(|p| p.numeric).unwrap_or(false);
    let mut acc: HashMap<String, (blake3::Hasher, u64)> = HashMap::new();
    mysqlutil::scan_table(conn, db, table, None, true, pks, |values| {
        let comps: Vec<String> = pk_indexes.iter().map(|&i| hash::value_to_pk_component(&values[i])).collect();
        let bucket = hash::bucket_of_pk(&comps, numeric_last, chunked);
        let rh = hash::row_hash(&values);
        let e = acc.entry(bucket).or_insert_with(|| (blake3::Hasher::new(), 0));
        e.0.update(&hex::decode(&rh).unwrap());
        e.1 += 1;
        if let Some(b) = bar {
            b.inc(1);
        }
        futures::future::ready(true)
    })
    .await?;
    Ok(acc
        .into_iter()
        .map(|(b, (h, c))| {
            let final_hash = hash::finalize_chunk(&b, c, &h);
            (b, (c, final_hash))
        })
        .collect())
}

/// Lokale Zeilen-Hashes für bestimmte Buckets (ein Scan mit WHERE).
async fn compute_row_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    pk_names: &[String],
    chunked: u32,
    buckets: &[String],
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, String>> {
    let names = mysqlutil::column_names(conn, db, table).await?;
    let mut pk_indexes = Vec::new();
    for pk in pk_names {
        let i = names.iter().position(|n| n == pk).with_context(|| format!("PK-Spalte {pk} fehlt lokal"))?;
        pk_indexes.push(i);
    }
    let where_clause = mysqlutil::bucket_where(pks, chunked, buckets)?;
    let numeric_last = pks.last().map(|p| p.numeric).unwrap_or(false);
    let mut out: HashMap<String, String> = HashMap::new();
    mysqlutil::scan_table(conn, db, table, Some(&where_clause), true, pks, |values| {
        let pk = hash::pk_canonical(&values, &pk_indexes);
        let rh = hash::row_hash(&values);
        if log::log_enabled!(log::Level::Trace) {
            log::trace!("LOCAL row pk={pk} hash={rh} values={values:?}");
        }
        out.insert(pk, rh);
        if let Some(b) = bar {
            b.inc(1);
        }
        futures::future::ready(true)
    })
    .await?;
    Ok(out)
}

/// Alle Zeilen senden (optional mit WHERE) — CSV- oder Dump-Modus.
#[allow(clippy::too_many_arguments)]
async fn transfer_all_rows(
    cfg: &Config,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    local_conn: &mut Conn,
    task: &TableTask,
    hex_indexes: &[usize],
    columns: &[String],
    where_clause: Option<&str>,
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    if cfg.allow_csv {
        send_rows_csv(cfg, send, recv, local_conn, task, hex_indexes, columns, where_clause, bar).await
    } else {
        send_rows_dump(cfg, send, recv, local_conn, task, hex_indexes, columns, where_clause, bar).await
    }
}

/// Zeilen per CSV streamen (CsvBegin → BIN → CsvEnd).
#[allow(clippy::too_many_arguments)]
async fn send_rows_csv(
    cfg: &Config,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    _local_conn: &mut Conn,
    task: &TableTask,
    hex_indexes: &[usize],
    columns: &[String],
    where_clause: Option<&str>,
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    let hex_names: Vec<String> = hex_indexes.iter().map(|&i| columns[i].clone()).collect();
    proto::write_json(
        send,
        &Request::CsvBegin {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            columns: columns.to_vec(),
            hex_columns: hex_names,
        },
        cfg.max_frame,
    )
    .await?;

    let mut buf: Vec<u8> = Vec::with_capacity(256 * 1024);
    let mut count = 0u64;
    let mut fetch_err: Option<String> = None;

    let mut rx: mpsc::Receiver<RowMsg> = export::spawn_fetch_with_fallback(
        cfg,
        task.local_db.clone(),
        task.local_table.clone(),
        where_clause.map(|s| s.to_string()),
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            RowMsg::Row(values) => {
                hash::row_to_tsv(&values, hex_indexes, &mut buf);
                count += 1;
                bar.inc(1);
                if buf.len() >= 256 * 1024 {
                    proto::write_bulk(send, &buf, cfg.max_frame).await?;
                    buf.clear();
                }
            }
            RowMsg::Err(e) => {
                fetch_err = Some(e);
                break;
            }
            RowMsg::Done => break,
        }
    }
    drop(rx);
    if let Some(e) = fetch_err {
        // Stream verwerfen: CsvEnd nicht senden, Server antwortet nicht → Stream abbauen
        bail!("Export fehlgeschlagen: {e}");
    }
    // RESTLICHEN Puffer spülen (wichtig: letzte Teilzeilen gehen sonst verloren!)
    if !buf.is_empty() {
        proto::write_bulk(send, &buf, cfg.max_frame).await?;
    }
    proto::write_json(send, &Request::CsvEnd {}, cfg.max_frame).await?;
    match read_response(recv, cfg.max_frame).await? {
        Response::Ok { count: Some(n), .. } => {
            if n != count {
                log::warn!("[{}.{}] Server importierte {n} Zeilen, Client erzeugte {count}", task.local_db, task.local_table);
            }
        }
        Response::Ok { .. } => {}
        Response::Err { message } => bail!("CSV-Import fehlgeschlagen: {message}"),
        _ => bail!("Unerwartete Antwort nach CsvEnd"),
    }
    Ok(count)
}

/// Zeilen als SQL-REPLACE-Dump senden (DumpBegin → BIN → DumpEnd).
#[allow(clippy::too_many_arguments)]
async fn send_rows_dump(
    cfg: &Config,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    _local_conn: &mut Conn,
    task: &TableTask,
    hex_indexes: &[usize],
    columns: &[String],
    where_clause: Option<&str>,
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    proto::write_json(
        send,
        &Request::DumpBegin { db: task.remote_db.clone(), table: task.remote_table.clone() },
        cfg.max_frame,
    )
    .await?;

    let mut count = 0u64;
    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(cfg.dump_threshold as usize);
    let mut fetch_err: Option<String> = None;

    let mut rx: mpsc::Receiver<RowMsg> = export::spawn_fetch_with_fallback(
        cfg,
        task.local_db.clone(),
        task.local_table.clone(),
        where_clause.map(|s| s.to_string()),
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            RowMsg::Row(values) => {
                batch.push(values);
                if batch.len() >= cfg.dump_threshold.max(1) as usize {
                    let sql = export::rows_to_replace_sql(&task.remote_db, &task.remote_table, columns, &batch, hex_indexes);
                    proto::write_bulk(send, &sql, cfg.max_frame).await?;
                    count += batch.len() as u64;
                    bar.inc(batch.len() as u64);
                    batch.clear();
                }
            }
            RowMsg::Err(e) => {
                fetch_err = Some(e);
                break;
            }
            RowMsg::Done => break,
        }
    }
    drop(rx);
    if let Some(e) = fetch_err {
        bail!("Export fehlgeschlagen: {e}");
    }
    if !batch.is_empty() {
        let sql = export::rows_to_replace_sql(&task.remote_db, &task.remote_table, columns, &batch, hex_indexes);
        proto::write_bulk(send, &sql, cfg.max_frame).await?;
        count += batch.len() as u64;
        bar.inc(batch.len() as u64);
    }
    proto::write_json(send, &Request::DumpEnd {}, cfg.max_frame).await?;
    match read_response(recv, cfg.max_frame).await? {
        Response::Ok { .. } => {}
        Response::Err { message } => bail!("Dump-Import fehlgeschlagen: {message}"),
        _ => bail!("Unerwartete Antwort nach DumpEnd"),
    }
    Ok(count)
}

/// Geänderte Zeilen senden: kleine Buckets per PK-IN-Liste,
/// große Buckets (> dump_threshold) komplett per Bucket-Filter.
#[allow(clippy::too_many_arguments)]
async fn send_changed_rows(
    cfg: &Config,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    local_conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    pk_names: &[String],
    hex_indexes: &[usize],
    columns: &[String],
    local_chunks: &HashMap<String, (u64, String)>,
    diff_buckets: &[String],
    to_send: &[String],
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    let mut sent = 0u64;

    // PK → Bucket zuordnen: Bucket-Größe bestimmen
    let mut by_bucket: HashMap<String, Vec<String>> = HashMap::new();
    // zu_sendende PKs nach Bucket einordnen: wir nutzen die Bucket-Zugehörigkeit
    // aus den lokalen Chunk-Counts? Nicht möglich (nur Counts) → nachträglich
    // über last-component-Suffix.
    let mut pk_by_bucket: HashMap<String, Vec<String>> = HashMap::new();
    let set: HashSet<&String> = to_send.iter().collect();
    let numeric_last = pks.last().map(|p| p.numeric).unwrap_or(false);
    for pk in &set {
        let comps: Vec<&str> = pk.split(hash::PK_SEP).collect();
        let comps_owned: Vec<String> = comps.iter().map(|s| s.to_string()).collect();
        let bucket = hash::bucket_of_pk(&comps_owned, numeric_last, cfg.chunked);
        pk_by_bucket.entry(bucket).or_default().push((*pk).clone());
    }

    for bucket in diff_buckets {
        let bucket_pks = match pk_by_bucket.get(bucket) {
            Some(v) => v.clone(),
            None => continue,
        };
        let bucket_size = local_chunks.get(bucket).map(|(c, _)| *c).unwrap_or(bucket_pks.len() as u64);
        if bucket_size > cfg.dump_threshold {
            // ganzer Bucket kopieren (sofort-Kurzschluss)
            let where_clause = mysqlutil::bucket_where(pks, cfg.chunked, std::slice::from_ref(bucket))?;
            log::debug!("[{}] Bucket '{bucket}' ({bucket_size} Zeilen) wird komplett kopiert", task.local_table);
            let n = transfer_all_rows(cfg, send, recv, local_conn, task, hex_indexes, columns, Some(&where_clause), bar).await?;
            sent += n;
        } else {
            // nur die konkreten PKs nachladen
            for pk_chunk in bucket_pks.chunks(500) {
                let where_clause = mysqlutil::pk_in_where(pks, pk_chunk)?;
                let n = transfer_all_rows(cfg, send, recv, local_conn, task, hex_indexes, columns, Some(&where_clause), bar).await?;
                sent += n;
            }
        }
    }
    let _ = (pk_names, &mut by_bucket);
    Ok(sent)
}

pub struct DiffReport {
    pub differ: u64,
    pub to_send: u64,
    pub to_delete: u64,
    pub examples: Vec<String>,
}

/// check-Modus: Diffs ermitteln ohne Schreibzugriffe. Gibt (differ, examples) zurück.
#[allow(clippy::too_many_arguments)]
async fn diff_and_report(
    cfg: &Config,
    send: &mut quinn::SendStream,
    recv: &mut quinn::RecvStream,
    local_conn: &mut Conn,
    pks: &[PkCol],
    local_info: &mysqlutil::TableInfo,
    task: &TableTask,
    check_examples: u32,
    bar: &crate::progress::TableBar,
) -> Result<(u64, Vec<String>)> {
    let pk_names: Vec<String> = pks.iter().map(|p| p.name.clone()).collect();
    let chunked = cfg.chunked;

    let local_chunks = compute_chunk_hashes(local_conn, &task.local_db, &task.local_table, pks, &pk_names, chunked, Some(bar)).await?;

    proto::write_json(
        send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked,
            hash_of_chunk: true,
            buckets: None,
        },
        cfg.max_frame,
    )
    .await?;
    let mut remote_chunks: HashMap<String, ChunkHash> = HashMap::new();
    loop {
        match read_response(recv, cfg.max_frame).await? {
            Response::ChunkHashes { items } => {
                bar.inc(items.len() as u64);
                for it in items {
                    remote_chunks.insert(it.bucket.clone(), it);
                }
            }
            Response::Ok { .. } => break,
            Response::Err { message } => bail!("{message}"),
            _ => bail!("Unerwartete Antwort"),
        }
    }

    let mut diff_buckets: Vec<String> = Vec::new();
    for (b, lh) in &local_chunks {
        match remote_chunks.get(b) {
            None => diff_buckets.push(b.clone()),
            Some(rh) => {
                if rh.hash != lh.1 || rh.count != lh.0 {
                    diff_buckets.push(b.clone());
                }
            }
        }
    }
    for b in remote_chunks.keys() {
        if !local_chunks.contains_key(b) {
            diff_buckets.push(b.clone());
        }
    }
    diff_buckets.sort();

    if diff_buckets.is_empty() {
        return Ok((0, vec![]));
    }

    let total_buckets = local_chunks.len().max(remote_chunks.len()).max(1) as f64;
    let diff_pct = diff_buckets.len() as f64 * 100.0 / total_buckets;
    if diff_pct > cfg.diff_threshold_percent {
        // Zu viele Diffs für Drill-Down — grobe Schätzung:
        let est: u64 = diff_buckets
            .iter()
            .map(|b| local_chunks.get(b).map(|(c, _)| *c).unwrap_or(remote_chunks.get(b).map(|r| r.count).unwrap_or(0)))
            .sum();
        log::warn!(
            "[{}.{tbl}] {diff_pct:.1}% Buckets differieren — Zeilen-Anzahl ist eine Schätzung (Mindestwert)",
            task.local_db,
            tbl = task.local_table
        );
        let examples = (0..check_examples.min(est as u32)).map(|i| format!("?#{i}")).collect();
        return Ok((est, examples));
    }

    let local_rows = compute_row_hashes(local_conn, &task.local_db, &task.local_table, pks, &pk_names, chunked, &diff_buckets, Some(bar)).await?;
    proto::write_json(
        send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked,
            hash_of_chunk: false,
            buckets: Some(diff_buckets.clone()),
        },
        cfg.max_frame,
    )
    .await?;
    let mut remote_rows: HashMap<String, String> = HashMap::new();
    loop {
        match read_response(recv, cfg.max_frame).await? {
            Response::RowHashes { items } => {
                bar.inc(items.len() as u64);
                for it in items {
                    remote_rows.insert(it.pk, it.hash);
                }
            }
            Response::Ok { .. } => break,
            Response::Err { message } => bail!("{message}"),
            _ => bail!("Unerwartete Antwort"),
        }
    }

    let mut differ = 0u64;
    let mut examples: Vec<String> = Vec::new();
    for (pk, lh) in &local_rows {
        match remote_rows.get(pk) {
            None => {
                differ += 1;
                if (examples.len() as u32) < check_examples {
                    examples.push(pk.clone());
                }
            }
            Some(rh) => {
                if rh != lh {
                    differ += 1;
                    if (examples.len() as u32) < check_examples {
                        examples.push(pk.clone());
                    }
                }
            }
        }
    }
    for pk in remote_rows.keys() {
        if !local_rows.contains_key(pk) {
            differ += 1;
            if (examples.len() as u32) < check_examples {
                examples.push(pk.clone());
            }
        }
    }
    let _ = local_info;
    Ok((differ, examples))
}

/// Value-Helfer für Tests.
#[allow(dead_code)]
pub fn v_int(i: i64) -> Value {
    Value::Int(i)
}
