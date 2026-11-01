//! Testhilfen: absichtlich ganze Chunks in der eigenen (lokalen) Datenbank
//! loeschen, damit fehlende Chunks auf Quelle bzw. Ziel getestet werden koennen.
//!
//! Ab `S` werden die Chunks `S`, `S+1`, `S+2`, `S+3` bearbeitet:
//! alle Zeilen, erste Zeile, letzte Zeile, zufaellige Zeile.

use crate::config::Config;
use crate::hash;
use crate::mysqlutil::{self, PkCol};
use crate::selector::Selector;
use crate::sqlhash;
use anyhow::{bail, Result};
use mysql_async::prelude::*;

#[derive(Debug, Clone, Copy)]
enum DropPattern {
    All,
    First,
    Last,
    Random,
}

impl DropPattern {
    const ORDER: [DropPattern; 4] = [
        DropPattern::All,
        DropPattern::First,
        DropPattern::Last,
        DropPattern::Random,
    ];

    fn name(self) -> &'static str {
        match self {
            DropPattern::All => "all",
            DropPattern::First => "first",
            DropPattern::Last => "last",
            DropPattern::Random => "random",
        }
    }
}

pub async fn run(
    cfg: &Config,
    selector: Option<&str>,
    start: u32,
) -> Result<std::process::ExitCode> {
    let pool = mysqlutil::new_pool(&cfg.mysql, None)?;
    let mut conn = mysqlutil::session_conn(&pool).await?;
    let dbs = mysqlutil::list_databases(&mut conn).await?;
    let sel = Selector::parse(selector)?;

    let mut tables = Vec::new();
    for db in dbs {
        if !sel.matches_db(&db) {
            continue;
        }
        for table in mysqlutil::list_tables(&mut conn, &db).await? {
            if sel.matches_table(&table) {
                tables.push((db.clone(), table));
            }
        }
    }
    if tables.is_empty() {
        bail!("--testing-drop-chunks: keine Tabelle passt zum Selector");
    }

    for (db, table) in tables {
        drop_chunks(&mut conn, cfg, &db, &table, start).await?;
    }
    Ok(std::process::ExitCode::SUCCESS)
}

fn order_by_desc(pks: &[PkCol]) -> String {
    if pks.is_empty() {
        return String::new();
    }
    let cols: Vec<String> = pks
        .iter()
        .map(|p| format!("{} DESC", mysqlutil::quote_ident(&p.name)))
        .collect();
    format!(" ORDER BY {}", cols.join(", "))
}

async fn drop_chunks(
    conn: &mut mysql_async::Conn,
    cfg: &Config,
    db: &str,
    table: &str,
    start: u32,
) -> Result<()> {
    let pks = mysqlutil::pk_columns(conn, db, table).await?;
    if pks.is_empty() {
        bail!("{db}.{table} hat keinen Primary Key — Chunks koennen nicht geloescht werden");
    }
    let mode = hash::chunk_mode_for_pk(&pks);
    if mode == hash::ChunkMode::Range {
        mysqlutil::validate_range_pk(&pks, db, table)?;
    }
    let bucket = sqlhash::bucket_expr(&pks, mode, cfg.chunked)?;
    let q = format!(
        "SELECT DISTINCT {bucket} AS b FROM {} ORDER BY BINARY b",
        mysqlutil::qualified(db, table)
    );
    let buckets: Vec<String> = conn.exec(q, ()).await?;
    if start as usize >= buckets.len() {
        bail!(
            "{db}.{table}: Chunk {start} existiert nicht (nur {} Chunks)",
            buckets.len()
        );
    }

    let qualified = mysqlutil::qualified(db, table);
    for (offset, pattern) in DropPattern::ORDER.iter().enumerate() {
        let index = start as usize + offset;
        let Some(key) = buckets.get(index) else {
            break;
        };
        let cond = mysqlutil::chunk_where(&pks, mode, cfg.chunked, &[key.clone()])?;
        let sql = match pattern {
            DropPattern::All => format!("DELETE FROM {qualified} WHERE {cond}"),
            DropPattern::First => format!(
                "DELETE FROM {qualified} WHERE {cond}{} LIMIT 1",
                mysqlutil::order_by(&pks)
            ),
            DropPattern::Last => format!(
                "DELETE FROM {qualified} WHERE {cond}{} LIMIT 1",
                order_by_desc(&pks)
            ),
            DropPattern::Random => {
                format!("DELETE FROM {qualified} WHERE {cond} ORDER BY RAND() LIMIT 1")
            }
        };
        conn.exec_drop(sql, ()).await?;
        let deleted = conn.affected_rows();
        log::info!(
            "[{db}.{table}] Chunk {index} ({key:?}) {} geloescht: {deleted} Zeile(n)",
            pattern.name()
        );
    }
    Ok(())
}
