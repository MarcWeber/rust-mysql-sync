//! Client: QUIC-Verbindung, Task-Enumeration aus Selector, parallele Ausführung.
//!
//! Reihenfolge der Plan-Schritte: force-schema → force → sync → check.
//! Alle Tabellen-Tasks laufen parallel (Semaphore = concurrent_processes),
//! jeder Task nutzt einen eigenen QUIC-Stream (Multiplexing).

use crate::config::Config;
use crate::mysqlutil;
use crate::selector::{map_remote, Selector};
use crate::sync::{self, HashVia, Mode, TableResult, TableTask};
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub struct ClientArgs {
    pub cfg: Config,
    pub force: Option<String>,
    pub sync: Option<String>,
    pub force_schema: Option<String>,
    pub check: Option<u32>,
    pub selector: Option<String>,
    pub no_progress: bool,
    pub hash_via_select: bool,
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
    hash_via: HashVia,
}

pub async fn run(args: ClientArgs) -> Result<std::process::ExitCode> {
    let cfg = args.cfg;

    // ---- Transport (QUIC/TLS oder Klartext-TCP) ----
    let session = Arc::new(crate::transport::Session::connect(&cfg).await?);

    // ---- Lokales MySQL ----
    let pool = mysqlutil::new_pool(&cfg.mysql, None)?;

    // ---- Task-Enumeration ----
    let mut local_conn = mysqlutil::session_conn(&pool).await?;
    let dbs = mysqlutil::list_databases(&mut local_conn).await?;

    let mut planned: Vec<PlannedTask> = Vec::new();
    let mut seen: std::collections::HashSet<(String, String, u8)> =
        std::collections::HashSet::new();

    let check_examples = args.check.unwrap_or(10);

    async fn add_plan(
        force: bool,
        selector_str: Option<&str>,
        mode: Mode,
        dbs: &[String],
        cfg: &Config,
        planned: &mut Vec<PlannedTask>,
        seen: &mut std::collections::HashSet<(String, String, u8)>,
        local_conn: &mut mysql_async::Conn,
        check_examples: u32,
        hash_via: HashVia,
    ) -> Result<()> {
        let sel = Selector::parse(selector_str)?;
        for db in dbs {
            if !sel.matches_db(db) {
                continue;
            }
            let tables = mysqlutil::list_tables(local_conn, db).await?;
            for table in tables {
                if !sel.matches_table(&table) {
                    continue;
                }
                let tag: u8 = if force { 1 } else { 0 } | if mode == Mode::Check { 2 } else { 0 };
                if !seen.insert((db.clone(), table.clone(), tag)) {
                    continue;
                }
                let (rdb, rtable) = map_remote(
                    db,
                    &table,
                    &cfg.database_mappings,
                    &cfg.database_table_mappings,
                );
                planned.push(PlannedTask {
                    task: TableTask {
                        local_db: db.clone(),
                        local_table: table.clone(),
                        remote_db: rdb,
                        remote_table: rtable,
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
        HashVia::RawTsv
    } else if args.hash_via_raw_binary_vec {
        HashVia::RawBinaryVec
    } else if args.hash_via_raw_binary {
        HashVia::RawBinary
    } else if args.hash_via_dump {
        HashVia::Dump
    } else {
        HashVia::Select
    };

    // Reihenfolge: force-schema → (force+sync gemerged) → sync → check
    if args.force_schema.is_some() {
        add_plan(
            true,
            args.force_schema.as_deref(),
            Mode::Force,
            &dbs,
            &cfg,
            &mut planned,
            &mut seen,
            &mut local_conn,
            check_examples,
            hash_via,
        )
        .await?;
    }
    if args.force.is_some() && args.sync.is_some() {
        // --force --sync: EIN Task (Recreate + differentieller Abgleich in Sequence)
        let sel = args.sync.as_deref().or(args.force.as_deref());
        add_plan(
            true,
            sel,
            Mode::Sync,
            &dbs,
            &cfg,
            &mut planned,
            &mut seen,
            &mut local_conn,
            check_examples,
            hash_via,
        )
        .await?;
    } else {
        if args.force.is_some() {
            add_plan(
                true,
                args.force.as_deref(),
                Mode::Force,
                &dbs,
                &cfg,
                &mut planned,
                &mut seen,
                &mut local_conn,
                check_examples,
                hash_via,
            )
            .await?;
        }
        if args.sync.is_some() {
            add_plan(
                false,
                args.sync.as_deref(),
                Mode::Sync,
                &dbs,
                &cfg,
                &mut planned,
                &mut seen,
                &mut local_conn,
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
            &dbs,
            &cfg,
            &mut planned,
            &mut seen,
            &mut local_conn,
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
                rows_sent: 0,
                rows_deleted: 0,
                differ_count: None,
                examples: vec![],
            }),
        }
    }
    session.close();

    // ---- Summary ----
    let ok_count = results.iter().filter(|r| r.ok).count();
    let err_count = results.len() - ok_count;
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
