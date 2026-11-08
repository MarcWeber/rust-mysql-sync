//! Client: QUIC-Verbindung, Task-Enumeration aus Selector, parallele Ausführung.
//!
//! Reihenfolge der Plan-Schritte: force-schema → force → sync → check.
//! Alle Tabellen-Tasks laufen parallel (Semaphore = concurrent_processes),
//! jeder Task nutzt einen eigenen QUIC-Stream (Multiplexing).

use crate::config::Config;
use crate::mysqlutil;
use crate::proto::{self, Framing, Request};
use crate::selector::{map_remote, Selector};
use crate::hash::HashVia;
use crate::sync::{self, Mode, TableResult, TableTask};
use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ClientArgs {
    pub cfg: Config,
    pub force: bool,
    pub sync: bool,
    pub force_schema: bool,
    pub check: Option<u32>,
    pub selector: Option<String>,
    pub no_progress: bool,
    pub hash_via_select_sha2: bool,
    pub hash_via_select_crc32: bool,
    pub hash_via_dump: bool,
    pub hash_via_raw_binary: bool,
    pub hash_via_raw_binary_vec: bool,
    pub hash_via_raw_tsv: bool,
}

struct PlannedTask {
    task: TableTask,
    force: bool,
    mode: Mode,
    check_examples: u32,
    hash_via: Option<HashVia>,
}

async fn remote_min_max(
    session: &crate::transport::Session,
    cfg: &Config,
    db: &str,
    table: &str,
    column: &str,
) -> Result<(Option<String>, Option<String>)> {
    let (mut send, mut recv) = session.open(cfg).await?;
    let f = Framing::from_cfg(cfg);
    proto::write_request(
        &mut send,
        &Request::MinMax {
            db: db.to_string(),
            table: table.to_string(),
            column: column.to_string(),
        },
        &f,
    )
    .await?;
    let buf = match proto::read_response_bytes(&mut recv, &f).await? {
        Some(buf) => buf,
        None => bail!("Keine Antwort auf MinMax"),
    };
    match proto::access_response(&buf)? {
        proto::ArchivedResponse::MinMax { min, max } => Ok((
            min.as_ref().map(|s| s.as_str().to_string()),
            max.as_ref().map(|s| s.as_str().to_string()),
        )),
        proto::ArchivedResponse::Err { message } => bail!("{}", message.as_str()),
        _ => bail!("Unerwartete Antwort auf MinMax"),
    }
}

async fn local_min_max(
    conn: &mut mysql_async::Conn,
    db: &str,
    table: &str,
    column: &str,
) -> Result<(Option<i128>, Option<i128>)> {
    let col = mysqlutil::quote_ident(column);
    let q = format!(
        "SELECT CAST(MIN({col}) AS CHAR), CAST(MAX({col}) AS CHAR) FROM {}",
        mysqlutil::qualified(db, table)
    );
    let row: Option<(Option<String>, Option<String>)> = conn.query_first(q).await?;
    let parse = |v: Option<String>| -> Result<Option<i128>> {
        match v {
            Some(s) => Ok(Some(s.parse().with_context(|| {
                format!("MIN/MAX-Wert {s:?} ist keine Ganzzahl")
            })?)),
            None => Ok(None),
        }
    };
    match row {
        Some((min, max)) => Ok((parse(min)?, parse(max)?)),
        None => Ok((None, None)),
    }
}

async fn shard_clauses(
    cfg: &Config,
    session: &crate::transport::Session,
    conn: &mut mysql_async::Conn,
    db: &str,
    table: &str,
    remote_db: &str,
    remote_table: &str,
) -> Result<Vec<Option<String>>> {
    if cfg.shards <= 1 {
        return Ok(vec![None]);
    }
    let pks = mysqlutil::pk_columns(conn, db, table).await?;
    if pks.len() != 1 || !matches!(pks[0].kind, mysqlutil::PkKind::Int | mysqlutil::PkKind::UInt)
    {
        return Ok(vec![None]);
    }
    let column = &pks[0].name;
    let (local_min, local_max) = local_min_max(conn, db, table, column).await?;
    let (remote_min, remote_max) =
        match remote_min_max(session, cfg, remote_db, remote_table, column).await {
            Ok(v) => v,
            Err(e) => {
                log::warn!("[{db}.{table}] Shard-Planung ohne Remote-MIN/MAX: {e:#}");
                (None, None)
            }
        };
    let parse = |v: Option<String>| v.and_then(|s| s.parse::<i128>().ok());
    let remote_min = parse(remote_min);
    let remote_max = parse(remote_max);
    let min = [local_min, remote_min].into_iter().flatten().min();
    let max = [local_max, remote_max].into_iter().flatten().max();
    let (Some(min), Some(max)) = (min, max) else {
        return Ok(vec![None]);
    };
    let span = max.saturating_sub(min);
    if span <= 0 {
        return Ok(vec![None]);
    }
    let total = usize::try_from(span.saturating_add(1))
        .unwrap_or(usize::MAX)
        .min(cfg.shards);
    if total <= 1 {
        return Ok(vec![None]);
    }
    let width = span / total as i128 + 1;
    let col = mysqlutil::quote_ident(column);
    let mut out = Vec::with_capacity(total);
    let mut lo = min;
    for i in 0..total {
        let hi = if i + 1 == total {
            max
        } else {
            lo.saturating_add(width - 1).min(max)
        };
        out.push(Some(format!("{col} >= {lo} AND {col} <= {hi}")));
        if hi >= max {
            break;
        }
        lo = hi + 1;
    }
    if out.len() <= 1 {
        Ok(vec![None])
    } else {
        Ok(out)
    }
}

pub async fn run(args: ClientArgs) -> Result<std::process::ExitCode> {
    let cfg = args.cfg;

    // ---- Transport (QUIC/TLS oder Klartext-TCP) ----
    let session = Arc::new(crate::transport::Session::connect(&cfg).await?);

    // ---- Lokales MySQL ----
    let pool = mysqlutil::new_pool(&cfg.mysql, None)?;

    // ---- Task-Enumeration ----
    let mut local_conn = mysqlutil::session_conn(&pool).await?;
    let mut table_cache: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();

    let mut planned: Vec<PlannedTask> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String, u8)> =
        std::collections::HashSet::new();

    let check_examples = args.check.unwrap_or(10);

    async fn add_plan(
        force: bool,
        selector_str: Option<&str>,
        mode: Mode,
        cfg: &Config,
        session: &crate::transport::Session,
        planned: &mut Vec<PlannedTask>,
        seen: &mut std::collections::HashSet<(String, String, u8)>,
        local_conn: &mut mysql_async::Conn,
        table_cache: &mut std::collections::HashMap<String, Vec<String>>,
        check_examples: u32,
        hash_via: Option<HashVia>,
    ) -> Result<()> {
        let sel = Selector::parse(selector_str)?;
        let pairs = sel.expand(local_conn, table_cache).await?;
        for (db, table) in pairs {
            let tag: u8 = if force { 1 } else { 0 } | if mode == Mode::Check { 2 } else { 0 };
            if !seen.insert((db.clone(), table.clone(), tag)) {
                continue;
            }
            let (rdb, rtable) = map_remote(
                &db,
                &table,
                &cfg.database_mappings,
                &cfg.database_table_mappings,
            );
            let where_clauses = if !force && mode != Mode::Force {
                shard_clauses(cfg, session, local_conn, &db, &table, &rdb, &rtable).await?
            } else {
                vec![None]
            };
            let total = where_clauses.len();
            for (idx, extra_where) in where_clauses.into_iter().enumerate() {
                planned.push(PlannedTask {
                    task: TableTask {
                        local_db: db.clone(),
                        local_table: table.clone(),
                        remote_db: rdb.clone(),
                        remote_table: rtable.clone(),
                        extra_where,
                        shard: (total > 1).then_some((idx + 1, total)),
                    },
                    force,
                    mode,
                    check_examples,
                    hash_via,
                });
            }
        }
        Ok(())
    }

    let hash_via = if args.hash_via_raw_tsv {
        Some(HashVia::RawTsv)
    } else if args.hash_via_raw_binary_vec {
        Some(HashVia::RawBinaryVec)
    } else if args.hash_via_raw_binary {
        Some(HashVia::RawBinary)
    } else if args.hash_via_dump {
        Some(HashVia::Dump)
    } else if args.hash_via_select_crc32 {
        Some(HashVia::SelectCrc32)
    } else if args.hash_via_select_sha2 {
        Some(HashVia::SelectSha2)
    } else {
        None
    };

    // Reihenfolge: force-schema → (force+sync gemerged) → sync → check
    if args.force_schema {
        add_plan(
            true,
            args.selector.as_deref(),
            Mode::Force,
            &cfg,
            session.as_ref(),
            &mut planned,
            &mut seen,
            &mut local_conn,
            &mut table_cache,
            check_examples,
            hash_via,
        )
        .await?;
    }
    if args.force && args.sync {
        // --force --sync: EIN Task (Recreate + differentieller Abgleich in Sequence)
        add_plan(
            true,
            args.selector.as_deref(),
            Mode::Sync,
            &cfg,
            session.as_ref(),
            &mut planned,
            &mut seen,
            &mut local_conn,
            &mut table_cache,
            check_examples,
            hash_via,
        )
        .await?;
    } else {
        if args.force {
            add_plan(
                true,
                args.selector.as_deref(),
                Mode::Force,
                &cfg,
                session.as_ref(),
                &mut planned,
                &mut seen,
                &mut local_conn,
                &mut table_cache,
                check_examples,
                hash_via,
            )
            .await?;
        }
        if args.sync {
            add_plan(
                false,
                args.selector.as_deref(),
                Mode::Sync,
                &cfg,
                session.as_ref(),
                &mut planned,
                &mut seen,
                &mut local_conn,
                &mut table_cache,
                check_examples,
                hash_via,
            )
            .await?;
        }
    }
    if args.check.is_some() {
        add_plan(
            false,
            args.selector.as_deref(),
            Mode::Check,
            &cfg,
            session.as_ref(),
            &mut planned,
            &mut seen,
            &mut local_conn,
            &mut table_cache,
            check_examples,
            hash_via,
        )
        .await?;
    }

    if planned.is_empty() {
        eprintln!("Keine passenden Tabellen für den Selektor gefunden.");
        return Ok(std::process::ExitCode::from(1));
    }
    log::info!(
        "{} Tabellen-Tasks ({} parallel, je 1 QUIC-Stream)",
        planned.len(),
        cfg.concurrent_processes
    );

    // ---- Ausführung mit Semaphore ----
    let semaphore = Arc::new(Semaphore::new(cfg.concurrent_processes.max(1)));
    let cfg_arc = Arc::new(cfg);
    let no_progress = args.no_progress;
    let mut handles = Vec::new();
    for p in planned {
        let permit = semaphore.clone().acquire_owned().await?;
        let cfg = cfg_arc.clone();
        let session = session.clone();
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let progress = crate::progress::Progress::new(!no_progress);
            let res = sync::run_table_task(
                (*cfg).clone(),
                session,
                pool,
                p.task,
                p.force,
                p.mode,
                p.check_examples,
                progress,
                p.hash_via,
            )
            .await;
            drop(permit);
            res
        }));
    }

    let mut results: Vec<TableResult> = Vec::new();
    for h in handles {
        match h.await {
            Ok(r) => results.push(r),
            Err(e) => results.push(TableResult {
                label: "<task>".into(),
                ok: false,
                mode: Mode::Sync,
                message: format!("Task-Panik: {e}"),
                differ_count: None,
                examples: vec![],
            }),
        }
    }
    session.close();

    // ---- Summary ----
    let ok_count = results.iter().filter(|r| r.ok).count();
    let err_count = results.iter().filter(|r| !r.ok).count();
    eprintln!();
    eprintln!("──────────────── Zusammenfassung ────────────────");
    for r in &results {
        if r.ok {
            eprintln!("  ✓ {:<44} {}", r.label, r.message);
            if !r.examples.is_empty() {
                eprintln!("    Beispiele: {}", r.examples.join(", "));
            }
        } else {
            eprintln!("  ✗ {:<44} {}", r.label, r.message);
        }
    }
    eprintln!("─────────────────────────────────────────────────");
    eprintln!("  {ok_count} ok, {err_count} Fehler");

    if err_count > 0 {
        return Ok(std::process::ExitCode::from(1));
    }
    // check: 0 = identisch, 1 = Unterschiede gefunden
    if results
        .iter()
        .any(|r| r.ok && r.mode == Mode::Check && r.differ_count.unwrap_or(0) > 0)
    {
        return Ok(std::process::ExitCode::from(1));
    }
    Ok(std::process::ExitCode::SUCCESS)
}
