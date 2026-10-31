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
use crate::{hash_binary, hash_tsv};
use anyhow::{bail, Context, Result};
use mysql_async::{Conn, Value};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;

const DIFF_BUCKET_BATCH: usize = 200;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum HashVia {
    Select,
    Dump,
    RawBinary,
    RawTsv,
}

impl HashVia {
    pub const ALL: [HashVia; 4] = [
        HashVia::Select,
        HashVia::Dump,
        HashVia::RawBinary,
        HashVia::RawTsv,
    ];

    pub fn name(self) -> &'static str {
        match self {
            HashVia::Select => "select",
            HashVia::Dump => "dump",
            HashVia::RawBinary => "raw-binary",
            HashVia::RawTsv => "raw-tsv",
        }
    }

    fn is_raw(self) -> bool {
        matches!(self, HashVia::RawBinary | HashVia::RawTsv)
    }
}

fn hash_via_source(via: HashVia) -> Option<String> {
    match via {
        HashVia::Select => None,
        HashVia::Dump => Some("dump".to_string()),
        HashVia::RawBinary => Some("raw_binary".to_string()),
        HashVia::RawTsv => Some("raw_tsv".to_string()),
    }
}

async fn fetch_server_profile(
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    f: &Framing,
) -> Result<proto::MysqlProfile> {
    proto::write_request(send, &Request::MysqlVersion {}, f).await?;
    match read_response(recv, f).await? {
        Response::MysqlVersion { profile, .. } => Ok(profile),
        Response::Err { message } => {
            bail!("MySQL-Version/Profil abrufen fehlgeschlagen: {message}")
        }
        _ => bail!("Unerwartete Antwort auf MysqlVersion"),
    }
}

struct ChunkStream {
    local_rx: mpsc::Receiver<hash::ChunkHashEvent>,
    remote_rx: mpsc::Receiver<hash::ChunkHashEvent>,
    local_task: tokio::task::JoinHandle<()>,
    remote_task: tokio::task::JoinHandle<()>,
    chunk_send: SendHalf,
    send: SendHalf,
    recv: RecvHalf,
}

#[allow(clippy::too_many_arguments)]
async fn run_local_chunk_stream(
    tx: mpsc::Sender<hash::ChunkHashEvent>,
    cfg: &Config,
    pool: &mysql_async::Pool,
    task: &TableTask,
    pks: &[PkCol],
    pk_names: &[String],
    hash_via: HashVia,
    mode: ChunkMode,
) -> Result<()> {
    let mut conn = mysqlutil::session_conn(pool).await?;
    let names = mysqlutil::column_names(&mut conn, &task.local_db, &task.local_table).await?;
    let pk_indexes = pk_indexes_for(&names, pk_names)?;
    match hash_via {
        HashVia::Select => {
            sqlhash::stream_chunk_hashes(
                tx,
                &mut conn,
                &task.local_db,
                &task.local_table,
                pks,
                mode,
                cfg.chunked,
            )
            .await?;
        }
        HashVia::Dump => {
            let strategies =
                check_export_strategies(cfg, &mut conn, &task.local_db, &task.local_table).await?;
            export::stream_chunk_hashes(
                tx,
                cfg,
                &task.local_db,
                &task.local_table,
                pks,
                &pk_indexes,
                mode,
                cfg.chunked,
                strategies,
            )
            .await?;
        }
        HashVia::RawBinary => {
            hash_binary::stream_chunk_hashes(
                tx,
                &mut conn,
                &task.local_db,
                &task.local_table,
                pks,
                &pk_indexes,
                mode,
                cfg.chunked,
                cfg.hash_algo(),
            )
            .await?;
        }
        HashVia::RawTsv => {
            hash_tsv::stream_chunk_hashes(
                tx,
                cfg,
                &task.local_db,
                &task.local_table,
                pks,
                &pk_indexes,
                mode,
                cfg.chunked,
            )
            .await?;
        }
    }
    Ok(())
}

async fn read_remote_chunk_stream(
    mut recv: RecvHalf,
    f: Framing,
    tx: mpsc::Sender<hash::ChunkHashEvent>,
) -> Result<()> {
    loop {
        let resp = read_response(&mut recv, &f).await?;
        match resp {
            Response::ChunkHashes { items } => {
                for it in items {
                    if tx
                        .send(hash::ChunkHashEvent::Item((it.bucket, it.count, it.hash)))
                        .await
                        .is_err()
                    {
                        return Ok(());
                    }
                }
            }
            Response::Ok { .. } => return Ok(()),
            Response::Err { message } => bail!("{message}"),
            _ => bail!("Unerwartete Antwort auf Hashes"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn start_chunk_stream(
    cfg: &Config,
    session: &crate::transport::Session,
    pool: &mysql_async::Pool,
    task: &TableTask,
    local_pks: &[PkCol],
    pk_names: &[String],
    hash_via: HashVia,
    mode: ChunkMode,
    raw_profile: Option<proto::MysqlProfile>,
    schema_hash: Option<String>,
    mut chunk_send: SendHalf,
    chunk_recv: RecvHalf,
    f: Framing,
) -> Result<ChunkStream> {
    proto::write_request(
        &mut chunk_send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names.to_vec(),
            chunked: cfg.chunked,
            hash_of_chunk: true,
            buckets: None,
            hash_source: hash_via_source(hash_via),
            profile: raw_profile,
            schema_hash,
        },
        &f,
    )
    .await?;

    let (local_tx, local_rx) = mpsc::channel::<hash::ChunkHashEvent>(cfg.chunk_stream_buffer);
    let (remote_tx, remote_rx) = mpsc::channel::<hash::ChunkHashEvent>(cfg.chunk_stream_buffer);

    let job_cfg = cfg.clone();
    let job_pool = pool.clone();
    let job_task = (*task).clone();
    let job_pks = local_pks.to_vec();
    let job_pk_names = pk_names.to_vec();
    let local_task = tokio::spawn(async move {
        let err_tx = local_tx.clone();
        let res = run_local_chunk_stream(
            local_tx,
            &job_cfg,
            &job_pool,
            &job_task,
            &job_pks,
            &job_pk_names,
            hash_via,
            mode,
        )
        .await;
        if let Err(e) = res {
            let _ = err_tx.send(hash::ChunkHashEvent::Error(e)).await;
        }
    });

    let remote_task = tokio::spawn(async move {
        let err_tx = remote_tx.clone();
        let res = read_remote_chunk_stream(chunk_recv, f, remote_tx).await;
        if let Err(e) = res {
            let _ = err_tx.send(hash::ChunkHashEvent::Error(e)).await;
        }
    });

    let (send, recv) = session.open(cfg).await?;
    Ok(ChunkStream {
        local_rx,
        remote_rx,
        local_task,
        remote_task,
        chunk_send,
        send,
        recv,
    })
}

async fn next_chunk(
    rx: &mut mpsc::Receiver<hash::ChunkHashEvent>,
) -> Result<Option<ChunkHash>> {
    match rx.recv().await {
        None => Ok(None),
        Some(hash::ChunkHashEvent::Item((bucket, count, hash))) => Ok(Some(ChunkHash {
            bucket,
            hash,
            count,
        })),
        Some(hash::ChunkHashEvent::Error(e)) => Err(e),
    }
}

async fn join_stream_tasks(
    local_task: tokio::task::JoinHandle<()>,
    remote_task: tokio::task::JoinHandle<()>,
) -> Result<()> {
    local_task.await.context("lokaler Chunk-Hash-Task")?;
    remote_task.await.context("Remote-Chunk-Hash-Task")?;
    Ok(())
}

#[derive(Clone)]
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

pub struct BenchStats {
    pub rows: u64,
    pub buckets: u64,
    pub ms: f64,
}

impl BenchStats {
    pub fn rows_per_s(&self) -> f64 {
        if self.ms <= 0.0 {
            0.0
        } else {
            self.rows as f64 * 1000.0 / self.ms
        }
    }
}

/// Erzeugt die Chunk-Hashes lokal und verwirft sie. Kein Server, kein Schreiben.
/// Die Zeit enthält nur die Hash-Erzeugung (Datenbank lesen + hashen).
pub async fn benchmark_table(
    cfg: &Config,
    pool: &mysql_async::Pool,
    task: &TableTask,
    hash_via: HashVia,
) -> Result<BenchStats> {
    let mut conn = mysqlutil::session_conn(pool).await?;
    let pks = mysqlutil::pk_columns(&mut conn, &task.local_db, &task.local_table).await?;
    if pks.is_empty() {
        bail!("{}.{} hat keinen Primary Key", task.local_db, task.local_table);
    }
    let mode = hash::chunk_mode_for_pk(&pks);
    let pk_names: Vec<String> = pks.iter().map(|p| p.name.clone()).collect();

    if hash_via == HashVia::RawTsv {
        let binary =
            mysqlutil::binary_column_indexes(&mut conn, &task.local_db, &task.local_table).await?;
        if !binary.is_empty() {
            bail!(
                "{}.{}: raw-tsv unterstützt keine binären Spalten (Indizes {binary:?})",
                task.local_db,
                task.local_table
            );
        }
    }

    let (tx, mut rx) = mpsc::channel::<hash::ChunkHashEvent>(cfg.chunk_stream_buffer.max(1));
    let job_cfg = cfg.clone();
    let job_pool = pool.clone();
    let job_task = task.clone();
    let job_pks = pks.clone();
    let job_pk_names = pk_names.clone();
    let started = std::time::Instant::now();
    let scan = tokio::spawn(async move {
        run_local_chunk_stream(
            tx,
            &job_cfg,
            &job_pool,
            &job_task,
            &job_pks,
            &job_pk_names,
            hash_via,
            mode,
        )
        .await
    });

    let mut rows = 0u64;
    let mut buckets = 0u64;
    while let Some(event) = rx.recv().await {
        match event {
            hash::ChunkHashEvent::Item((_bucket, count, _hash)) => {
                rows += count;
                buckets += 1;
            }
            hash::ChunkHashEvent::Error(e) => return Err(e),
        }
    }
    scan.await.context("Benchmark-Task abgebrochen")??;
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    Ok(BenchStats { rows, buckets, ms })
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
    match run_inner(
        &cfg,
        &session,
        &pool,
        &task,
        do_force,
        mode,
        check_examples,
        &progress,
        hash_via,
    )
    .await
    {
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
    let local_pks: Vec<PkCol> =
        mysqlutil::pk_columns(&mut local_conn, &task.local_db, &task.local_table).await?;
    if local_pks.is_empty() {
        bail!("{label} hat keinen Primary Key — Sync nicht möglich");
    }
    let chunk_mode = hash::chunk_mode_for_pk(&local_pks);
    let pk_names: Vec<String> = local_pks.iter().map(|p| p.name.clone()).collect();
    let local_info =
        mysqlutil::table_info(&mut local_conn, &task.local_db, &task.local_table).await?;

    proto::write_request(
        &mut send,
        &Request::DescribeCount {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
        },
        &f,
    )
    .await?;
    let remote_info = match read_response(&mut recv, &f).await? {
        Response::SchemaInfo {
            create_sql,
            columns,
            count,
        } => Some((create_sql, columns, count)),
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

    // ---- Raw-Hash-Gate: exact MySQL representation profile must match ----
    let raw_profile = if hash_via.is_raw() {
        let local_profile = mysqlutil::mysql_profile(&mut local_conn).await?;
        let remote_profile = fetch_server_profile(&mut send, &mut recv, &f).await?;
        if let Some(diff) = local_profile.difference(&remote_profile) {
            bail!(
                "{label}: Raw-Hashing abgelehnt — MySQL-Profil weicht ab: {diff}. \
                 Nutze --hash-select oder --hash-dump."
            );
        }
        if hash_via == HashVia::RawTsv {
            let binary = mysqlutil::binary_column_indexes(
                &mut local_conn,
                &task.local_db,
                &task.local_table,
            )
            .await?;
            if !binary.is_empty() {
                bail!(
                    "{label}: raw-tsv unterstützt keine binären Spalten (Indizes {binary:?}); \
                     nutze raw-binary, select oder dump."
                );
            }
        }
        Some(local_profile)
    } else {
        None
    };
    let schema_hash = if hash_via.is_raw() {
        Some(hash::hash_bytes(
            mysqlutil::normalize_schema(&local_info.create_sql).as_bytes(),
            cfg.hash_algo(),
        ))
    } else {
        None
    };

    // ---- 2. Force / Schema-Abgleich ----
    if do_force {
        bar.phase("Force-Schema");
        match &remote_info {
            None => {
                proto::write_request(
                    &mut send,
                    &Request::Recreate {
                        db: task.remote_db.clone(),
                        sql: local_info.create_sql.clone(),
                    },
                    &f,
                )
                .await?;
                expect_ok(&mut recv, &f, "Recreate").await?;
                log::info!("[{label}] Tabelle auf Ziel erstellt");
            }
            Some((remote_create, _, _)) => {
                if mysqlutil::normalize_schema(&local_info.create_sql)
                    != mysqlutil::normalize_schema(remote_create)
                {
                    log::warn!("[{label}] Schema weicht ab — Tabelle wird neu erstellt (Daten gehen verloren!)");
                    proto::write_request(
                        &mut send,
                        &Request::Recreate {
                            db: task.remote_db.clone(),
                            sql: local_info.create_sql.clone(),
                        },
                        &f,
                    )
                    .await?;
                    expect_ok(&mut recv, &f, "Recreate").await?;
                } else {
                    log::debug!("[{label}] Schema identisch — kein Recreate");
                }
            }
        }
    }

    if mode == Mode::Check {
        if hash_via.is_raw() {
            match &remote_info {
                Some((remote_create, _, _)) => {
                    if mysqlutil::normalize_schema(&local_info.create_sql)
                        != mysqlutil::normalize_schema(remote_create)
                    {
                        bail!("{label}: Raw-Hashing abgelehnt — Schemas unterscheiden sich");
                    }
                }
                None => bail!("{label}: Zieltabelle fehlt — Raw-Hashing nicht möglich"),
            }
        }
        // Nur vergleichen: Chunk- und Zeilen-Diffs ohne Schreibzugriffe
        let stats = diff_and_report(
            cfg,
            session,
            pool,
            send,
            recv,
            &mut local_conn,
            &local_pks,
            &local_info,
            task,
            check_examples,
            &bar,
            hash_via,
            raw_profile,
            schema_hash,
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
            proto::write_request(
                &mut send,
                &Request::DescribeCount {
                    db: task.remote_db.clone(),
                    table: task.remote_table.clone(),
                },
                &f,
            )
            .await?;
            match read_response(&mut recv, &f).await? {
                Response::SchemaInfo {
                    create_sql,
                    columns,
                    count,
                } => Some((create_sql, columns, count)),
                Response::Err { message } => bail!("{message}"),
                _ => bail!("Unerwartete Antwort"),
            }
        }
    };
    let (remote_create, _, remote_count) = match remote_info2 {
        Some(x) => x,
        None => bail!("{label}: Zieltabelle fehlt und kein --force — Abbruch"),
    };
    if mysqlutil::normalize_schema(&local_info.create_sql)
        != mysqlutil::normalize_schema(&remote_create)
    {
        bail!("{label}: Schemas unterscheiden sich — mit --force/--force-schema Schema abgleichen");
    }

    let hex_indexes =
        mysqlutil::binary_column_indexes(&mut local_conn, &task.local_db, &task.local_table)
            .await?;

    // Vollkopie-Kurzschluss: Ziel leer → alles senden
    if remote_count == 0 && local_info.count > 0 {
        bar.set_total(local_info.count, "Vollkopie (Ziel leer)");
        let sent = transfer_all_rows(
            cfg,
            &mut send,
            &mut recv,
            &mut local_conn,
            task,
            &hex_indexes,
            &local_info.columns,
            None,
            &bar,
        )
        .await?;
        bar.done(&format!("{sent} Zeilen gesendet"));
        return Ok((sent, 0, Some(local_info.count), vec![]));
    }

    // ---- 3. Chunk-Hashes: lokaler und remote Scan laufen parallel ----
    bar.phase("Chunk-Hashes (parallel)");
    let chunk_stream = start_chunk_stream(
        cfg,
        session,
        pool,
        task,
        &local_pks,
        &pk_names,
        hash_via,
        chunk_mode,
        raw_profile.clone(),
        schema_hash.clone(),
        send,
        recv,
        f,
    )
    .await?;
    let ChunkStream {
        mut local_rx,
        mut remote_rx,
        local_task,
        remote_task,
        chunk_send,
        mut send,
        mut recv,
    } = chunk_stream;

    let total_rows = local_info.count.max(1) as f64;
    let mut local_cur: Option<ChunkHash> = None;
    let mut remote_cur: Option<ChunkHash> = None;
    let mut local_done = false;
    let mut remote_done = false;
    let mut batch: Vec<String> = Vec::with_capacity(DIFF_BUCKET_BATCH);
    let mut compared = 0u64;
    let mut diff_bucket_total = 0u64;
    let mut diff_rows_est = 0u64;
    let mut differ_total = 0u64;
    let mut sent_total = 0u64;
    let mut deleted_total = 0u64;
    let mut examples: Vec<String> = Vec::new();

    loop {
        if local_cur.is_none() && !local_done {
            local_cur = next_chunk(&mut local_rx).await?;
            local_done = local_cur.is_none();
        }
        if remote_cur.is_none() && !remote_done {
            remote_cur = next_chunk(&mut remote_rx).await?;
            remote_done = remote_cur.is_none();
        }
        if local_cur.is_none() && remote_cur.is_none() {
            break;
        }

        let take_local = match (&local_cur, &remote_cur) {
            (Some(l), Some(r)) => l.bucket <= r.bucket,
            (Some(_), None) => true,
            _ => false,
        };
        let diff_bucket = if take_local {
            let l = local_cur.take().unwrap();
            compared += 1;
            match remote_cur.as_ref() {
                Some(r) if r.bucket == l.bucket => {
                    let r = remote_cur.take().unwrap();
                    if l.hash != r.hash || l.count != r.count {
                        diff_rows_est += l.count;
                        Some(l.bucket)
                    } else {
                        None
                    }
                }
                _ => {
                    diff_rows_est += l.count;
                    Some(l.bucket)
                }
            }
        } else {
            let r = remote_cur.take().unwrap();
            compared += 1;
            diff_rows_est += r.count;
            Some(r.bucket)
        };
        if let Some(bucket) = diff_bucket {
            batch.push(bucket);
            diff_bucket_total += 1;
        }

        if batch.len() >= DIFF_BUCKET_BATCH {
            let (sent, deleted, differ, ex) = sync_diff_batch(
                cfg,
                &mut send,
                &mut recv,
                &mut local_conn,
                task,
                &local_pks,
                &hex_indexes,
                &local_info.columns,
                &batch,
                hash_via,
                raw_profile.clone(),
                schema_hash.clone(),
                &bar,
                check_examples,
            )
            .await?;
            sent_total += sent;
            deleted_total += deleted;
            differ_total += differ;
            for pk in ex {
                if examples.len() < check_examples as usize {
                    examples.push(pk);
                }
            }
            batch.clear();
        }
    }

    if !batch.is_empty() {
        let (sent, deleted, differ, ex) = sync_diff_batch(
            cfg,
            &mut send,
            &mut recv,
            &mut local_conn,
            task,
            &local_pks,
            &hex_indexes,
            &local_info.columns,
            &batch,
            hash_via,
            raw_profile,
            schema_hash,
            &bar,
            check_examples,
        )
        .await?;
        sent_total += sent;
        deleted_total += deleted;
        differ_total += differ;
        for pk in ex {
            if examples.len() < check_examples as usize {
                examples.push(pk);
            }
        }
    }

    join_stream_tasks(local_task, remote_task).await?;
    drop(chunk_send);

    if diff_bucket_total == 0 {
        bar.done("identisch");
        return Ok((0, 0, Some(0), vec![]));
    }
    let diff_pct = diff_rows_est as f64 * 100.0 / total_rows;
    log::info!(
        "[{label}] {} von {} Buckets differieren (~{} von {} Zeilen, {:.1}%)",
        diff_bucket_total,
        compared.max(1),
        diff_rows_est,
        local_info.count,
        diff_pct
    );
    bar.done(&format!("{sent_total} gesendet, {deleted_total} gelöscht"));
    Ok((sent_total, deleted_total, Some(differ_total), examples))
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
    let e = acc
        .entry(bucket)
        .or_insert_with(|| (hash::RowHasher::new(algo), 0));
    match hex::decode(rh) {
        Ok(bytes) => e.0.update(&bytes),
        Err(_) => e.0.update(rh.as_bytes()),
    }
    e.1 += 1;
}

fn finish_chunks(acc: HashMap<String, (hash::RowHasher, u64)>) -> HashMap<String, (u64, String)> {
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

async fn compute_chunk_hashes_raw_binary(
    conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    pk_names: &[String],
    mode: ChunkMode,
    chunked: u32,
    algo: hash::HashAlgo,
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, (u64, String)>> {
    let names = mysqlutil::column_names(conn, &task.local_db, &task.local_table).await?;
    let pk_indexes = pk_indexes_for(&names, pk_names)?;
    hash_binary::chunk_hashes(
        conn,
        &task.local_db,
        &task.local_table,
        pks,
        &pk_indexes,
        mode,
        chunked,
        algo,
        || {
            if let Some(b) = bar {
                b.inc(1);
            }
        },
    )
    .await
}

async fn compute_row_hashes_raw_binary(
    conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    pk_names: &[String],
    mode: ChunkMode,
    chunked: u32,
    buckets: &[String],
    algo: hash::HashAlgo,
    bar: Option<&crate::progress::TableBar>,
) -> Result<HashMap<String, String>> {
    let names = mysqlutil::column_names(conn, &task.local_db, &task.local_table).await?;
    let pk_indexes = pk_indexes_for(&names, pk_names)?;
    let items = hash_binary::row_hashes(
        conn,
        &task.local_db,
        &task.local_table,
        pks,
        &pk_indexes,
        mode,
        chunked,
        buckets,
        algo,
    )
    .await?;
    if let Some(b) = bar {
        b.inc(items.len() as u64);
    }
    Ok(items.into_iter().map(|r| (r.pk, r.hash)).collect())
}

async fn compute_chunk_hashes_raw_tsv(
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
    hash_tsv::chunk_hashes(
        cfg,
        &task.local_db,
        &task.local_table,
        pks,
        &pk_indexes,
        mode,
        chunked,
        || {
            if let Some(b) = bar {
                b.inc(1);
            }
        },
    )
    .await
}

async fn compute_row_hashes_raw_tsv(
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
    let items = hash_tsv::row_hashes(
        cfg,
        &task.local_db,
        &task.local_table,
        pks,
        &pk_indexes,
        mode,
        chunked,
        buckets,
    )
    .await?;
    if let Some(b) = bar {
        b.inc(items.len() as u64);
    }
    Ok(items.into_iter().map(|r| (r.pk, r.hash)).collect())
}

struct RowDiff {
    to_send: Vec<String>,
    to_delete: Vec<String>,
}

#[allow(clippy::too_many_arguments)]
async fn row_diff_for_buckets(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    pks: &[PkCol],
    task: &TableTask,
    buckets: &[String],
    hash_via: HashVia,
    raw_profile: Option<proto::MysqlProfile>,
    schema_hash: Option<String>,
    bar: &crate::progress::TableBar,
) -> Result<RowDiff> {
    let f = Framing::from_cfg(cfg);
    let pk_names: Vec<String> = pks.iter().map(|p| p.name.clone()).collect();
    let mode = hash::chunk_mode_for_pk(pks);
    let chunked = cfg.chunked;

    let local_rows = match hash_via {
        HashVia::Select => {
            compute_row_hashes(
                local_conn,
                &task.local_db,
                &task.local_table,
                pks,
                mode,
                chunked,
                buckets,
                Some(bar),
            )
            .await?
        }
        HashVia::Dump => {
            compute_row_hashes_via_export(
                cfg,
                local_conn,
                task,
                pks,
                &pk_names,
                mode,
                chunked,
                buckets,
                Some(bar),
            )
            .await?
        }
        HashVia::RawBinary => {
            compute_row_hashes_raw_binary(
                local_conn,
                task,
                pks,
                &pk_names,
                mode,
                chunked,
                buckets,
                cfg.hash_algo(),
                Some(bar),
            )
            .await?
        }
        HashVia::RawTsv => {
            compute_row_hashes_raw_tsv(
                cfg,
                local_conn,
                task,
                pks,
                &pk_names,
                mode,
                chunked,
                buckets,
                Some(bar),
            )
            .await?
        }
    };

    proto::write_request(
        send,
        &Request::Hashes {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
            primary_keys: pk_names,
            chunked,
            hash_of_chunk: false,
            buckets: Some(buckets.to_vec()),
            hash_source: hash_via_source(hash_via),
            profile: raw_profile,
            schema_hash,
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
            _ => bail!("Unerwartete Antwort auf RowHashes"),
        }
    }

    let mut to_send: Vec<String> = Vec::new();
    let mut to_delete: Vec<String> = Vec::new();
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
    Ok(RowDiff { to_send, to_delete })
}

#[allow(clippy::too_many_arguments)]
async fn apply_row_diff(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    hex_indexes: &[usize],
    columns: &[String],
    diff: &RowDiff,
    bar: &crate::progress::TableBar,
) -> Result<(u64, u64)> {
    let f = Framing::from_cfg(cfg);
    let mut sent = 0u64;
    let mut deleted = 0u64;

    if !diff.to_send.is_empty() {
        if cfg.disable_triggers {
            proto::write_request(
                send,
                &Request::DisableTriggers {
                    db: task.remote_db.clone(),
                    table: task.remote_table.clone(),
                },
                &f,
            )
            .await?;
            expect_ok(recv, &f, "DisableTriggers").await?;
        }
        sent = send_changed_rows(
            cfg,
            send,
            recv,
            local_conn,
            task,
            pks,
            hex_indexes,
            columns,
            &diff.to_send,
            bar,
        )
        .await?;
        if !diff.to_delete.is_empty() {
            for chunk in diff.to_delete.chunks(1000) {
                proto::write_request(
                    send,
                    &Request::DeletePks {
                        db: task.remote_db.clone(),
                        table: task.remote_table.clone(),
                        pks: chunk.to_vec(),
                    },
                    &f,
                )
                .await?;
                expect_ok(recv, &f, "DeletePks").await?;
                deleted += chunk.len() as u64;
                bar.inc(chunk.len() as u64);
            }
        }
        if cfg.disable_triggers {
            proto::write_request(
                send,
                &Request::RestoreTriggers {
                    db: task.remote_db.clone(),
                    table: task.remote_table.clone(),
                },
                &f,
            )
            .await?;
            expect_ok(recv, &f, "RestoreTriggers").await?;
        }
    } else if !diff.to_delete.is_empty() {
        for chunk in diff.to_delete.chunks(1000) {
            proto::write_request(
                send,
                &Request::DeletePks {
                    db: task.remote_db.clone(),
                    table: task.remote_table.clone(),
                    pks: chunk.to_vec(),
                },
                &f,
            )
            .await?;
            expect_ok(recv, &f, "DeletePks").await?;
            deleted += chunk.len() as u64;
            bar.inc(chunk.len() as u64);
        }
    }
    Ok((sent, deleted))
}

#[allow(clippy::too_many_arguments)]
async fn sync_diff_batch(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    task: &TableTask,
    pks: &[PkCol],
    hex_indexes: &[usize],
    columns: &[String],
    buckets: &[String],
    hash_via: HashVia,
    raw_profile: Option<proto::MysqlProfile>,
    schema_hash: Option<String>,
    bar: &crate::progress::TableBar,
    check_examples: u32,
) -> Result<(u64, u64, u64, Vec<String>)> {
    let diff = row_diff_for_buckets(
        cfg,
        send,
        recv,
        local_conn,
        pks,
        task,
        buckets,
        hash_via,
        raw_profile,
        schema_hash,
        bar,
    )
    .await?;
    let differ = (diff.to_send.len() + diff.to_delete.len()) as u64;
    let mut examples = Vec::new();
    for pk in diff.to_send.iter().chain(diff.to_delete.iter()) {
        if examples.len() >= check_examples as usize {
            break;
        }
        examples.push(pk.clone());
    }
    let (sent, deleted) = apply_row_diff(
        cfg,
        send,
        recv,
        local_conn,
        task,
        pks,
        hex_indexes,
        columns,
        &diff,
        bar,
    )
    .await?;
    Ok((sent, deleted, differ, examples))
}

#[allow(clippy::too_many_arguments)]
async fn check_diff_batch(
    cfg: &Config,
    send: &mut SendHalf,
    recv: &mut RecvHalf,
    local_conn: &mut Conn,
    pks: &[PkCol],
    task: &TableTask,
    buckets: &[String],
    hash_via: HashVia,
    raw_profile: Option<proto::MysqlProfile>,
    schema_hash: Option<String>,
    bar: &crate::progress::TableBar,
    check_examples: u32,
) -> Result<(u64, Vec<String>)> {
    let diff = row_diff_for_buckets(
        cfg,
        send,
        recv,
        local_conn,
        pks,
        task,
        buckets,
        hash_via,
        raw_profile,
        schema_hash,
        bar,
    )
    .await?;
    let differ = (diff.to_send.len() + diff.to_delete.len()) as u64;
    let mut examples = Vec::new();
    for pk in diff.to_send.iter().chain(diff.to_delete.iter()) {
        if examples.len() >= check_examples as usize {
            break;
        }
        examples.push(pk.clone());
    }
    Ok((differ, examples))
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
        send_rows_csv(
            cfg,
            send,
            recv,
            local_conn,
            task,
            hex_indexes,
            columns,
            where_clause,
            bar,
        )
        .await
    } else {
        send_rows_dump(
            cfg,
            send,
            recv,
            local_conn,
            task,
            hex_indexes,
            columns,
            where_clause,
            bar,
        )
        .await
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
                log::warn!(
                    "[{}.{}] Server importierte {n} Zeilen, Client erzeugte {count}",
                    task.local_db,
                    task.local_table
                );
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
        &Request::DumpBegin {
            db: task.remote_db.clone(),
            table: task.remote_table.clone(),
        },
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
                    let sql = export::rows_to_replace_sql(
                        &task.remote_db,
                        &task.remote_table,
                        columns,
                        &batch,
                        hex_indexes,
                    );
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
        let sql = export::rows_to_replace_sql(
            &task.remote_db,
            &task.remote_table,
            columns,
            &batch,
            hex_indexes,
        );
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
        sent += transfer_all_rows(
            cfg,
            send,
            recv,
            local_conn,
            task,
            hex_indexes,
            columns,
            Some(&where_clause),
            bar,
        )
        .await?;
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
    session: &crate::transport::Session,
    pool: &mysql_async::Pool,
    send: SendHalf,
    recv: RecvHalf,
    local_conn: &mut Conn,
    pks: &[PkCol],
    local_info: &mysqlutil::TableInfo,
    task: &TableTask,
    check_examples: u32,
    bar: &crate::progress::TableBar,
    hash_via: HashVia,
    raw_profile: Option<proto::MysqlProfile>,
    schema_hash: Option<String>,
) -> Result<(u64, Vec<String>)> {
    let f = Framing::from_cfg(cfg);
    let pk_names: Vec<String> = pks.iter().map(|p| p.name.clone()).collect();
    let mode = hash::chunk_mode_for_pk(pks);

    bar.phase("Chunk-Hashes (parallel)");
    let chunk_stream = start_chunk_stream(
        cfg,
        session,
        pool,
        task,
        pks,
        &pk_names,
        hash_via,
        mode,
        raw_profile.clone(),
        schema_hash.clone(),
        send,
        recv,
        f,
    )
    .await?;
    let ChunkStream {
        mut local_rx,
        mut remote_rx,
        local_task,
        remote_task,
        chunk_send,
        mut send,
        mut recv,
    } = chunk_stream;

    let total_rows = local_info.count.max(1) as f64;
    let mut local_cur: Option<ChunkHash> = None;
    let mut remote_cur: Option<ChunkHash> = None;
    let mut local_done = false;
    let mut remote_done = false;
    let mut batch: Vec<String> = Vec::with_capacity(DIFF_BUCKET_BATCH);
    let mut batch_est = 0u64;
    let mut compared = 0u64;
    let mut diff_bucket_total = 0u64;
    let mut diff_rows_est = 0u64;
    let mut differ = 0u64;
    let mut examples: Vec<String> = Vec::new();
    let mut skipped = false;

    loop {
        if local_cur.is_none() && !local_done {
            local_cur = next_chunk(&mut local_rx).await?;
            local_done = local_cur.is_none();
        }
        if remote_cur.is_none() && !remote_done {
            remote_cur = next_chunk(&mut remote_rx).await?;
            remote_done = remote_cur.is_none();
        }
        if local_cur.is_none() && remote_cur.is_none() {
            break;
        }

        let take_local = match (&local_cur, &remote_cur) {
            (Some(l), Some(r)) => l.bucket <= r.bucket,
            (Some(_), None) => true,
            _ => false,
        };
        let diff_bucket = if take_local {
            let l = local_cur.take().unwrap();
            compared += 1;
            match remote_cur.as_ref() {
                Some(r) if r.bucket == l.bucket => {
                    let r = remote_cur.take().unwrap();
                    if l.hash != r.hash || l.count != r.count {
                        diff_rows_est += l.count;
                        batch_est += l.count;
                        Some(l.bucket)
                    } else {
                        None
                    }
                }
                _ => {
                    diff_rows_est += l.count;
                    batch_est += l.count;
                    Some(l.bucket)
                }
            }
        } else {
            let r = remote_cur.take().unwrap();
            compared += 1;
            diff_rows_est += r.count;
            batch_est += r.count;
            Some(r.bucket)
        };
        if let Some(bucket) = diff_bucket {
            batch.push(bucket);
            diff_bucket_total += 1;
        }

        if batch.len() >= DIFF_BUCKET_BATCH {
            let diff_pct = diff_rows_est as f64 * 100.0 / total_rows;
            if mode == ChunkMode::Mod && diff_pct > cfg.diff_threshold_percent {
                skipped = true;
                differ += batch_est;
            } else {
                let (d, ex) = check_diff_batch(
                    cfg,
                    &mut send,
                    &mut recv,
                    local_conn,
                    pks,
                    task,
                    &batch,
                    hash_via,
                    raw_profile.clone(),
                    schema_hash.clone(),
                    bar,
                    check_examples,
                )
                .await?;
                differ += d;
                for pk in ex {
                    if examples.len() < check_examples as usize {
                        examples.push(pk);
                    }
                }
            }
            batch.clear();
            batch_est = 0;
        }
    }

    if !batch.is_empty() {
        let diff_pct = diff_rows_est as f64 * 100.0 / total_rows;
        if mode == ChunkMode::Mod && diff_pct > cfg.diff_threshold_percent {
            skipped = true;
            differ += batch_est;
        } else {
            let (d, ex) = check_diff_batch(
                cfg,
                &mut send,
                &mut recv,
                local_conn,
                pks,
                task,
                &batch,
                hash_via,
                raw_profile,
                schema_hash,
                bar,
                check_examples,
            )
            .await?;
            differ += d;
            for pk in ex {
                if examples.len() < check_examples as usize {
                    examples.push(pk);
                }
            }
        }
    }

    join_stream_tasks(local_task, remote_task).await?;
    drop(chunk_send);

    if diff_bucket_total == 0 {
        return Ok((0, vec![]));
    }

    let diff_pct = diff_rows_est as f64 * 100.0 / total_rows;
    if skipped {
        log::warn!(
            "[{}.{tbl}] {} von {} Buckets differieren (~{} Zeilen, {:.1}%) — Zeilen-Anzahl ist eine Schätzung (Mindestwert)",
            task.local_db,
            diff_bucket_total,
            compared.max(1),
            diff_rows_est,
            diff_pct,
            tbl = task.local_table
        );
        if examples.is_empty() {
            examples = (0..check_examples.min(differ as u32))
                .map(|i| format!("?#{i}"))
                .collect();
        }
    }
    Ok((differ, examples))
}

/// Value-Helfer für Tests.
#[allow(dead_code)]
pub fn v_int(i: i64) -> Value {
    Value::Int(i)
}
