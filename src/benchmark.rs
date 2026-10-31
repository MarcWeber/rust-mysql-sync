//! `--benchmark [SEL]`: Chunk-Hashes lokal erzeugen und verwerfen.
//!
//! Kein Server, kein Schreibzugriff. Gemessen wird nur die lokale
//! Hash-Erzeugung (Datenbank lesen + hashen). Ausgabe ist tab-separiert,
//! damit sie sortiert werden kann.

use crate::config::Config;
use crate::mysqlutil;
use crate::selector::Selector;
use crate::sync::{self, HashVia, TableTask};
use anyhow::Result;
use std::process::ExitCode;

pub async fn run(cfg: &Config, selector: Option<&str>, modes: &[HashVia]) -> Result<ExitCode> {
    let pool = mysqlutil::new_pool(&cfg.mysql, None)?;
    let mut conn = mysqlutil::session_conn(&pool).await?;
    let sel = Selector::parse(selector)?;

    let mut tasks = Vec::new();
    for db in mysqlutil::list_databases(&mut conn).await? {
        if !sel.matches_db(&db) {
            continue;
        }
        for table in mysqlutil::list_tables(&mut conn, &db).await? {
            if sel.matches_table(&table) {
                tasks.push((db.clone(), table));
            }
        }
    }

    if tasks.is_empty() {
        eprintln!("Keine passenden Tabellen für den Selektor gefunden.");
        return Ok(ExitCode::from(1));
    }

    println!("table\tmode\tsync_ms\trows\tbuckets\trows_per_s");
    let mut total_ms = 0.0;
    let mut succeeded = 0usize;
    for (db, table) in tasks {
        let task = TableTask {
            local_db: db.clone(),
            local_table: table.clone(),
            remote_db: db.clone(),
            remote_table: table.clone(),
        };
        for &via in modes {
            match sync::benchmark_table(cfg, &pool, &task, via).await {
                Ok(stats) => {
                    succeeded += 1;
                    total_ms += stats.ms;
                    println!(
                        "{db}.{table}\t{}\t{:.1}\t{}\t{}\t{:.0}",
                        via.name(),
                        stats.ms,
                        stats.rows,
                        stats.buckets,
                        stats.rows_per_s()
                    );
                }
                Err(e) => {
                    eprintln!("{db}.{table}\t{}\t-\t-\t-\tERROR: {e:#}", via.name());
                }
            }
        }
    }
    eprintln!("TOTAL\t-\t{total_ms:.1}\t-\t-\t-");
    Ok(if succeeded == 0 {
        ExitCode::from(1)
    } else {
        ExitCode::SUCCESS
    })
}
