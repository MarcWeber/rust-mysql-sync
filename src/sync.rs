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
use crate::hash::{self, ChunkMode};
use crate::mysqlutil::{self, PkCol};
use crate::progress::Progress;
use crate::proto::{self, ChunkHash, Framing, Request, Response};
use crate::sqlhash;
use crate::transport::{RecvHalf, SendHalf};
use anyhow::{bail, Context, Result};
use mysql_async::{Conn, Value};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum HashVia {
    Select,
    Dump,
}

fn hash_via_source(via: HashVia) -> Option<String> {
    match via {
        HashVia::Select => None,
        HashVia::Dump => Some("dump".to_string()),
    }
}

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
    session: std::sync::Arc<crate::transport::Session>,
    pool: mysql_async::Pool,
    task: TableTask,
    do_force: bool,
    mode: Mode,
    check_examples: u32,
    progress: Progress,
    hash_via: HashVia,
) -> TableResult {
    let label = format!("{}.{}", task.local_db, task.local_table);
    match run_inner(&cfg, &session, &pool, &task, do_force, mode, check_examples, &progress, hash_via).await {
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
    session: &crate::transport::Session,
    pool: &mysql_async::Pool,
    task: &TableTask,
    do_force: bool,
    mode: Mode,
    check_examples: u32,
    progress: &Progress,
    hash_via: HashVia,
) -> Result<SyncStats> {
    let label = format!("{}.{}", task.local_db, task.local_table);
    let (mut send, mut recv) = session.open(cfg).await?;
    let f = Framing::from_cfg(cfg);
    let bar = progress.bar(&label, 0);

    // ---- 1. Schema & Counts ----
    bar.phase("Schema/Counts");
    let mut local_conn = mysqlutil::session_conn(pool).await?;
    let local_pks: Vec<PkCol> = mysqlutil::pk_columns(&mut local_conn, &task.local_db, &task.local_table).await?;
    if local_pks.is_empty() {
        bail!("{label} hat keinen Primary Key — Sync nicht möglich");
    }
    let chunk_mode = hash::chunk_mode_for_pk(&local_pks);
    let pk_names: Vec<String> = local_pks.iter().map(|p| p.name.clone()).collect();
    let local_info = mysqlutil::table_info(&mut local_conn, &task.local_db, &task.local_table).await?;

    proto::write_request(&mut send, &Request::DescribeCount { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
    let remote_info = match read_response(&mut recv, &f).await? {
        Response::SchemaInfo { create_sql, columns, count } => Some((create_sql, columns, count)),
        Response::Err { message }
            if message.contains("nicht gefunden")
                || message.contains("doesn't exist")
                || message.to_lowercase().contains("unknown database") =>
        {
            None
        }
        Response::Err { message } => bail!("{message}"),
        _ => bail!("Unerwartete Antwort auf DescribeCount"),
    };

    // ---- 2. Force / Schema-Abgleich ----
    if do_force {
        bar.phase("Force-Schema");
        match &remote_info {
            None => {
                proto::write_request(&mut send, &Request::Recreate { db: task.remote_db.clone(), sql: local_info.create_sql.clone() }, &f).await?;
                expect_ok(&mut recv, &f, "Recreate").await?;
                log::info!("[{label}] Tabelle auf Ziel erstellt");
            }
            Some((remote_create, _, _)) => {
                if mysqlutil::normalize_schema(&local_info.create_sql) != mysqlutil::normalize_schema(remote_create) {
                    log::warn!("[{label}] Schema weicht ab — Tabelle wird neu erstellt (Daten gehen verloren!)");
                    proto::write_request(&mut send, &Request::Recreate { db: task.remote_db.clone(), sql: local_info.create_sql.clone() }, &f).await?;
                    expect_ok(&mut recv, &f, "Recreate").await?;
                } else {
                    log::debug!("[{label}] Schema identisch — kein Recreate");
                }
            }
        }
    }

    if mode == Mode::Check {
        // Nur vergleichen: Chunk- und Zeilen-Diffs ohne Schreibzugriffe
        let stats = diff_and_report(
            cfg, &mut send, &mut recv, &mut local_conn, &local_pks, &local_info, task, check_examples, &bar, hash_via,
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
            proto::write_request(&mut send, &Request::DescribeCount { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
            match read_response(&mut recv, &f).await? {
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
    let local_chunks = match hash_via {
        HashVia::Select => {
            compute_chunk_hashes(&mut local_conn, &task.local_db, &task.local_table, &local_pks, chunk_mode, cfg.chunked, Some(&bar)).await?
        }
        HashVia::Dump => {
            compute_chunk_hashes_via_export(cfg, &mut local_conn, task, &local_pks, &pk_names, chunk_mode, cfg.chunked, Some(&bar)).await?
        }
    };

    // Chunk-Hashes remote (Stream)
    bar.phase("Remote Chunk-Hashes");
    proto::write_request(
        &mut send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked: cfg.chunked,
            hash_of_chunk: true,
            buckets: None,
            hash_source: hash_via_source(hash_via),
        },
        &f,
    )
    .await?;
    let mut remote_chunks: HashMap<String, ChunkHash> = HashMap::new();
    loop {
        let resp = read_response(&mut recv, &f).await?;
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

    let total_buckets = local_chunks.len().max(remote_chunks.len()).max(1) as u64;
    let diff_rows_est: u64 = diff_buckets
        .iter()
        .map(|b| {
            local_chunks
                .get(b)
                .map(|(c, _)| *c)
                .or_else(|| remote_chunks.get(b).map(|r| r.count))
                .unwrap_or(0)
        })
        .sum();
    let total_rows = local_info.count.max(1) as f64;
    let diff_pct = diff_rows_est as f64 * 100.0 / total_rows;
    log::info!(
        "[{label}] {} von {} Buckets differieren (~{} von {} Zeilen, {:.1}%, Threshold {:.1}%)",
        diff_buckets.len(),
        total_buckets,
        diff_rows_est,
        local_info.count,
        diff_pct,
        cfg.diff_threshold_percent
    );

    if diff_buckets.is_empty() {
        bar.done("identisch");
        return Ok((0, 0, Some(0), vec![]));
    }

    // Vollkopie bei zu hohem Diff-Anteil?
    // Bei Range-Chunks ist die Bucket-Quote kein guter Zeilen-Schätzer: ein
    // einzelner Treffer ändert den Hash des ganzen (grossen) Bereichs.
    if chunk_mode == ChunkMode::Mod && diff_pct > cfg.diff_threshold_percent {
        log::info!("[{label}] Über Threshold — Vollkopie (Truncate + alle Zeilen)");
        bar.phase("Vollkopie");
        if cfg.disable_triggers {
            proto::write_request(&mut send, &Request::DisableTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
            expect_ok(&mut recv, &f, "DisableTriggers").await?;
        }
        proto::write_request(&mut send, &Request::Truncate { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
        expect_ok(&mut recv, &f, "Truncate").await?;
        let sent = transfer_all_rows(cfg, &mut send, &mut recv, &mut local_conn, task, &hex_indexes, &local_info.columns, None, &bar).await?;
        if cfg.disable_triggers {
            proto::write_request(&mut send, &Request::RestoreTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
            expect_ok(&mut recv, &f, "RestoreTriggers").await?;
        }
        bar.done(&format!("Vollkopie: {sent} Zeilen"));
        return Ok((sent, 0, Some(local_info.count), vec![]));
    }

    // ---- Drill-Down: Zeilen-Hashes beider Seiten ----
    bar.phase("Drill-Down (Zeilen-Hashes)");
    let local_rows = match hash_via {
        HashVia::Select => {
            compute_row_hashes(&mut local_conn, &task.local_db, &task.local_table, &local_pks, chunk_mode, cfg.chunked, &diff_buckets, Some(&bar)).await?
        }
        HashVia::Dump => {
            compute_row_hashes_via_export(cfg, &mut local_conn, task, &local_pks, &pk_names, chunk_mode, cfg.chunked, &diff_buckets, Some(&bar)).await?
        }
    };

    proto::write_request(
        &mut send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked: cfg.chunked,
            hash_of_chunk: false,
            buckets: Some(diff_buckets.clone()),
            hash_source: hash_via_source(hash_via),
        },
        &f,
    )
    .await?;
    let mut remote_rows: HashMap<String, String> = HashMap::new();
    loop {
        let resp = read_response(&mut recv, &f).await?;
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
            proto::write_request(&mut send, &Request::DisableTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
            expect_ok(&mut recv, &f, "DisableTriggers").await?;
        }
        sent_total = send_changed_rows(cfg, &mut send, &mut recv, &mut local_conn, task, &local_pks, &hex_indexes, &local_info.columns, &to_send, &bar).await?;
        if !to_delete.is_empty() {
            for chunk in to_delete.chunks(1000) {
                proto::write_request(
                    &mut send,
                    &Request::DeletePks { db: task.remote_db.clone(), table: task.remote_table.clone(), pks: chunk.to_vec() },
                    &f,
                )
                .await?;
                expect_ok(&mut recv, &f, "DeletePks").await?;
                deleted_total += chunk.len() as u64;
                bar.inc(chunk.len() as u64);
            }
        }
        if cfg.disable_triggers {
            proto::write_request(&mut send, &Request::RestoreTriggers { db: task.remote_db.clone(), table: task.remote_table.clone() }, &f).await?;
            expect_ok(&mut recv, &f, "RestoreTriggers").await?;
        }
    } else if !to_delete.is_empty() {
        for chunk in to_delete.chunks(1000) {
            proto::write_request(
                &mut send,
                &Request::DeletePks { db: task.remote_db.clone(), table: task.remote_table.clone(), pks: chunk.to_vec() },
                &f,
            )
            .await?;
            expect_ok(&mut recv, &f, "DeletePks").await?;
            deleted_total += chunk.len() as u64;
            bar.inc(chunk.len() as u64);
        }
    }

    bar.done(&format!("{sent_total} gesendet, {deleted_total} gelöscht"));
    Ok((sent_total, deleted_total, Some(to_send.len() as u64 + to_delete.len() as u64), examples))
}

async fn read_response(recv: &mut RecvHalf, f: &Framing) -> Result<Response> {
    match proto::read_response(recv, f).await? {
        Some(resp) => Ok(resp),
        None => bail!("Server hat den Stream geschlossen"),
    }
}

async fn expect_ok(recv: &mut RecvHalf, f: &Framing, what: &str) -> Result<()> {
    match read_response(recv, f).await? {
        Response::Ok { .. } => Ok(()),
        Response::Err { message } => bail!("{what} fehlgeschlagen: {message}"),
        _ => bail!("{what}: unerwartete Antwort"),
    }
}

fn pk_indexes_for(conn_names: &[String], pk_names: &[String]) -> Result<Vec<usize>> {
    let mut idx = Vec::new();
    for pk in pk_names {
        let i = conn_names
            .iter()
            .position(|n| n == pk)
            .with_context(|| format!("PK-Spalte {pk} fehlt lokal"))?;
        idx.push(i);
    }
    Ok(idx)
}

fn push_row_hash_to_bucket(
    acc: &mut HashMap<String, (hash::RowHasher, u64)>,
    algo: crate::hash::HashAlgo,
    bucket: String,
    rh: &str,
) {
    let e = acc.entry(bucket).or_insert_with(|| (hash::RowHasher::new(algo), 0));
    match hex::decode(rh) {
        Ok(bytes) => e.0.update(&bytes),
        Err(_) => e.0.update(rh.as_bytes()),
    }
    e.1 += 1;
}

fn finish_chunks(
    acc: HashMap<String, (hash::RowHasher, u64)>,
) -> HashMap<String, (u64, String)> {
    acc.into_iter()
        .map(|(b, (h, c))| {
            let final_hash = hash::finalize_chunk(&b, c, h);
            (b, (c, final_hash))
        })
        .collect()
}

async fn compute_chunk_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    mode: ChunkMode,
    chunked: u32,
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, (u64, String)>> {
    let out = sqlhash::chunk_hashes(conn, db, table, pks, mode, chunked).await?;
    if let Some(b) = bar {
        b.set_total(out.len() as u64, "Lokale SQL-Gruppen-Hashes");
        b.inc(out.len() as u64);
    }
    Ok(out)
}

async fn compute_row_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, String>> {
    let rows = sqlhash::row_hashes(conn, db, table, pks, mode, chunked, Some(buckets)).await?;
    let out: HashMap<String, String> = rows.into_iter().map(|r| (r.pk, r.hash)).collect();
    if let Some(b) = bar {
        b.set_total(out.len() as u64, "SQL-Zeilen-Hashes");
        b.inc(out.len() as u64);
    }
    Ok(out)
}

/// Wie compute_chunk_hashes, aber die Zeilen kommen aus dem Export-/Parser-Pfad
/// (select-stream, to_file, mysql-e, mysqldump) statt direkt aus SELECT *.
async fn compute_chunk_hashes_via_export(
    cfg: &Config,
    conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    pk_names: &[String],
    mode: ChunkMode,
    chunked: u32,
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, (u64, String)>> {
    let names = mysqlutil::column_names(conn, &task.local_db, &task.local_table).await?;
    let pk_indexes = pk_indexes_for(&names, pk_names)?;
    let algo = cfg.hash_algo();
    let strategies = check_export_strategies(cfg, conn, &task.local_db, &task.local_table).await?;
    let mut rx = export::spawn_fetch_with_strategies(
        cfg,
        task.local_db.clone(),
        task.local_table.clone(),
        None,
        Some(mysqlutil::order_by(pks)),
        strategies,
    );
    let mut acc: HashMap<String, (hash::RowHasher, u64)> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            RowMsg::Row(values) => {
                let bucket = hash::chunk_key_for_row(&values, &pk_indexes, pks, mode, chunked)?;
                let rh = hash::row_hash(&values, algo);
                push_row_hash_to_bucket(&mut acc, algo, bucket, &rh);
                if let Some(b) = bar {
                    b.inc(1);
                }
            }
            RowMsg::Err(e) => bail!("Export/Parser-Fehler: {e}"),
            RowMsg::Done => break,
        }
    }
    Ok(finish_chunks(acc))
}

/// Wie compute_row_hashes, aber über den Export-/Parser-Pfad.
async fn compute_row_hashes_via_export(
    cfg: &Config,
    conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    pk_names: &[String],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, String>> {
    let names = mysqlutil::column_names(conn, &task.local_db, &task.local_table).await?;
    let pk_indexes = pk_indexes_for(&names, pk_names)?;
    let where_clause = mysqlutil::chunk_where(pks, mode, chunked, buckets)?;
    let wanted: HashSet<String> = buckets.iter().cloned().collect();
    let algo = cfg.hash_algo();
    let strategies = check_export_strategies(cfg, conn, &task.local_db, &task.local_table).await?;
    let mut rx = export::spawn_fetch_with_strategies(
        cfg,
        task.local_db.clone(),
        task.local_table.clone(),
        Some(where_clause),
        Some(mysqlutil::order_by(pks)),
        strategies,
    );
    let mut out: HashMap<String, String> = HashMap::new();
    while let Some(msg) = rx.recv().await {
        match msg {
            RowMsg::Row(values) => {
                let bucket = hash::chunk_key_for_row(&values, &pk_indexes, pks, mode, chunked)?;
                if !wanted.contains(&bucket) {
                    continue;
                }
                let pk = hash::pk_canonical(&values, &pk_indexes, pks);
                let rh = hash::row_hash(&values, algo);
                out.insert(pk, rh);
                if let Some(b) = bar {
                    b.inc(1);
                }
            }
            RowMsg::Err(e) => bail!("Export/Parser-Fehler: {e}"),
            RowMsg::Done => break,
        }
    }
    Ok(out)
}

/// Wenn die Tabelle binäre Spalten hat, wird CSV/TSV vermieden und mysqldump
/// bevorzugt (hex/bit-sicher). Sonst möglichst Parser-Strategien (kein
/// select-stream), damit der Transfer-Pfad wirklich getestet wird.
pub async fn check_export_strategies(
    cfg: &Config,
    conn: &mut Conn,
    db: &str,
    table: &str,
) -> Result<Vec<String>> {
    let bin = mysqlutil::binary_column_indexes(conn, db, table).await?;
    let configured = cfg.strategies();
    if !bin.is_empty() {
        let mut v = vec!["mysqldump".to_string()];
        for s in configured {
            if !v.iter().any(|x| x == &s) {
                v.push(s);
            }
        }
        return Ok(v);
    }
    let parsing: Vec<String> = configured
        .iter()
        .filter(|s| {
            let t = s.to_lowercase();
            t != "select-stream" && t != "select_stream" && t != "stream"
        })
        .cloned()
        .collect();
    if parsing.is_empty() {
        Ok(configured)
    } else {
        Ok(parsing)
    }
}

/// Alle Zeilen senden (optional mit WHERE) — CSV- oder Dump-Modus.
#[allow(clippy::too_many_arguments)]
async fn transfer_all_rows(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    task: &TableTask,
    hex_indexes: &[usize],
    columns: &[String],
    where_clause: Option<&str>,
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    // Binäre Spalten niemals über CSV/TSV übertragen — immer SQL-Dump-Pfad.
    let use_csv = cfg.allow_csv && hex_indexes.is_empty();
    if use_csv {
        send_rows_csv(cfg, send, recv, local_conn, task, hex_indexes, columns, where_clause, bar).await
    } else {
        send_rows_dump(cfg, send, recv, local_conn, task, hex_indexes, columns, where_clause, bar).await
    }
}

/// Zeilen per CSV streamen (CsvBegin → BIN → CsvEnd).
#[allow(clippy::too_many_arguments)]
async fn send_rows_csv(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    _local_conn: &mut Conn,
    task: &TableTask,
    hex_indexes: &[usize],
    columns: &[String],
    where_clause: Option<&str>,
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    let f = Framing::from_cfg(cfg);
    let hex_names: Vec<String> = hex_indexes.iter().map(|&i| columns[i].clone()).collect();
    proto::write_request(
        send,
        &Request::CsvBegin {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            columns: columns.to_vec(),
            hex_columns: hex_names,
        },
        &f,
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
        None,
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            RowMsg::Row(values) => {
                hash::row_to_tsv(&values, hex_indexes, &mut buf);
                count += 1;
                bar.inc(1);
                if buf.len() >= 256 * 1024 {
                    proto::write_bulk(send, &buf, &f).await?;
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
        proto::write_bulk(send, &buf, &f).await?;
    }
    proto::write_request(send, &Request::CsvEnd {}, &f).await?;
    match read_response(recv, &f).await? {
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
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    _local_conn: &mut Conn,
    task: &TableTask,
    hex_indexes: &[usize],
    columns: &[String],
    where_clause: Option<&str>,
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    let f = Framing::from_cfg(cfg);
    proto::write_request(
        send,
        &Request::DumpBegin { db: task.remote_db.clone(), table: task.remote_table.clone() },
        &f,
    )
    .await?;

    let mut count = 0u64;
    let mut batch: Vec<Vec<Value>> = Vec::with_capacity(cfg.transfer_batch_rows.max(1));
    let mut fetch_err: Option<String> = None;

    let mut rx: mpsc::Receiver<RowMsg> = export::spawn_fetch_with_fallback(
        cfg,
        task.local_db.clone(),
        task.local_table.clone(),
        where_clause.map(|s| s.to_string()),
        None,
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            RowMsg::Row(values) => {
                batch.push(values);
                if batch.len() >= cfg.transfer_batch_rows.max(1) {
                    let sql = export::rows_to_replace_sql(&task.remote_db, &task.remote_table, columns, &batch, hex_indexes);
                    proto::write_bulk(send, &sql, &f).await?;
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
        proto::write_bulk(send, &sql, &f).await?;
        count += batch.len() as u64;
        bar.inc(batch.len() as u64);
    }
    proto::write_request(send, &Request::DumpEnd {}, &f).await?;
    match read_response(recv, &f).await? {
        Response::Ok { .. } => {}
        Response::Err { message } => bail!("Dump-Import fehlgeschlagen: {message}"),
        _ => bail!("Unerwartete Antwort nach DumpEnd"),
    }
    Ok(count)
}

/// Geänderte Zeilen exakt per PK-IN-Liste senden. Kein Vollkopie-Kurzschluss:
/// unveränderte Zeilen sollen keine Trigger/REPLACE-Last erzeugen.
#[allow(clippy::too_many_arguments)]
async fn send_changed_rows(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    hex_indexes: &[usize],
    columns: &[String],
    to_send: &[String],
    bar: &crate::progress::TableBar,
) -> Result<u64> {
    let mut sent = 0u64;
    for pk_chunk in to_send.chunks(cfg.transfer_batch_rows.max(1)) {
        let where_clause = mysqlutil::pk_in_where(pks, pk_chunk)?;
        sent += transfer_all_rows(cfg, send, recv, local_conn, task, hex_indexes, columns, Some(&where_clause), bar).await?;
    }
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
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    pks: &[PkCol],
    local_info: &mysqlutil::TableInfo,
    task: &TableTask,
    check_examples: u32,
    bar: &crate::progress::TableBar,
    hash_via: HashVia,
) -> Result<(u64, Vec<String>)> {
    let f = Framing::from_cfg(cfg);
    let pk_names: Vec<String> = pks.iter().map(|p| p.name.clone()).collect();
    let chunked = cfg.chunked;
    let mode = hash::chunk_mode_for_pk(pks);

    let local_chunks = match hash_via {
        HashVia::Select => compute_chunk_hashes(local_conn, &task.local_db, &task.local_table, pks, mode, chunked, Some(bar)).await?,
        HashVia::Dump => compute_chunk_hashes_via_export(cfg, local_conn, task, pks, &pk_names, mode, chunked, Some(bar)).await?,
    };

    proto::write_request(
        send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked,
            hash_of_chunk: true,
            buckets: None,
            hash_source: hash_via_source(hash_via),
        },
        &f,
    )
    .await?;
    let mut remote_chunks: HashMap<String, ChunkHash> = HashMap::new();
    loop {
        match read_response(recv, &f).await? {
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

    let total_buckets = local_chunks.len().max(remote_chunks.len()).max(1) as u64;
    let est: u64 = diff_buckets
        .iter()
        .map(|b| local_chunks.get(b).map(|(c, _)| *c).unwrap_or(remote_chunks.get(b).map(|r| r.count).unwrap_or(0)))
        .sum();
    let total_rows = local_info.count.max(1) as f64;
    let diff_pct = est as f64 * 100.0 / total_rows;
    if mode == ChunkMode::Mod && diff_pct > cfg.diff_threshold_percent {
        // Zu viele Diffs für Drill-Down — grobe Schätzung:
        log::warn!(
            "[{}.{tbl}] {} von {total_buckets} Buckets differieren (~{est} Zeilen, {diff_pct:.1}%) — Zeilen-Anzahl ist eine Schätzung (Mindestwert)",
            task.local_db,
            diff_buckets.len(),
            tbl = task.local_table
        );
        let examples = (0..check_examples.min(est as u32)).map(|i| format!("?#{i}")).collect();
        return Ok((est, examples));
    }

    let local_rows = match hash_via {
        HashVia::Select => compute_row_hashes(local_conn, &task.local_db, &task.local_table, pks, mode, chunked, &diff_buckets, Some(bar)).await?,
        HashVia::Dump => compute_row_hashes_via_export(cfg, local_conn, task, pks, &pk_names, mode, chunked, &diff_buckets, Some(bar)).await?,
    };
    proto::write_request(
        send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.clone(),
            chunked,
            hash_of_chunk: false,
            buckets: Some(diff_buckets.clone()),
            hash_source: hash_via_source(hash_via),
        },
        &f,
    )
    .await?;
    let mut remote_rows: HashMap<String, String> = HashMap::new();
    loop {
        match read_response(recv, &f).await? {
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
    Ok((differ, examples))
}

/// Value-Helfer für Tests.
#[allow(dead_code)]
pub fn v_int(i: i64) -> Value {
    Value::Int(i)
}
