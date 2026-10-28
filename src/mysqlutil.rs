//! MySQL-Hilfsfunktionen: Connections, Schema-Infos, PKs, Streaming-Scans.

use crate::config::MysqlCfg;
use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, OptsBuilder, Pool, Value};

pub const SYSTEM_DBS: &[&str] = &["information_schema", "mysql", "performance_schema", "sys"];

const NUMERIC_TYPES: &[&str] = &[
    "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year",
];

pub fn build_opts(cfg: &MysqlCfg, db: Option<&str>) -> Result<Opts> {
    let mut b = OptsBuilder::default();
    if let Some(sock) = &cfg.socket {
        b = b.socket(Some(sock.clone()));
    } else {
        b = b.ip_or_hostname(cfg.host.clone()).tcp_port(cfg.port);
    }
    b = b
        .user(Some(cfg.user.clone()))
        .pass(Some(cfg.password.clone()))
        .db_name(db.map(|s| s.to_string()))
        .prefer_socket(cfg.socket.is_some());
    Ok(Opts::from(b))
}

pub fn new_pool(cfg: &MysqlCfg, db: Option<&str>) -> Result<Pool> {
    Ok(Pool::new(build_opts(cfg, db)?))
}

/// Neue Verbindung mit normalisierter Session (Zeichensatz + Zeitzone),
/// damit Hashes und Textdarstellungen auf beiden Seiten identisch sind.
pub async fn session_conn(pool: &Pool) -> Result<Conn> {
    let mut conn = pool.get_conn().await.context("MySQL-Verbindung fehlgeschlagen")?;
    conn.query_drop("SET NAMES utf8mb4").await.ok();
    conn.query_drop("SET SESSION time_zone = '+00:00'").await.ok();
    Ok(conn)
}

/// Identifier mit Backticks quoten (und Injection verhindern).
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

pub fn qualified(db: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(db), quote_ident(table))
}

pub async fn list_databases(conn: &mut Conn) -> Result<Vec<String>> {
    let dbs: Vec<String> = conn.query("SHOW DATABASES").await?;
    Ok(dbs.into_iter().filter(|d| !SYSTEM_DBS.contains(&d.as_str())).collect())
}

pub async fn list_tables(conn: &mut Conn, db: &str) -> Result<Vec<String>> {
    let q = format!("SHOW FULL TABLES IN {} WHERE Table_type = 'BASE TABLE'", quote_ident(db));
    let rows: Vec<(String, String)> = conn.query(q).await?;
    Ok(rows.into_iter().map(|(t, _)| t).collect())
}

#[derive(Debug, Clone)]
pub struct TableInfo {
    pub create_sql: String,
    pub columns: Vec<String>,
    pub count: u64,
}

pub async fn table_info(conn: &mut Conn, db: &str, table: &str) -> Result<TableInfo> {
    let q = format!("SHOW CREATE TABLE {}", qualified(db, table));
    let row: Option<(String, String)> = conn.query_first(q).await?;
    let (_, create_sql) = row.with_context(|| format!("Tabelle {db}.{table} nicht gefunden"))?;
    let columns = column_names(conn, db, table).await?;
    let count = table_count(conn, db, table).await?;
    Ok(TableInfo { create_sql, columns, count })
}

pub async fn column_names(conn: &mut Conn, db: &str, table: &str) -> Result<Vec<String>> {
    let q = format!(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    );
    let rows: Vec<String> = conn.exec(q, (db, table)).await?;
    Ok(rows)
}

pub async fn table_count(conn: &mut Conn, db: &str, table: &str) -> Result<u64> {
    let q = format!("SELECT COUNT(*) FROM {}", qualified(db, table));
    let (c,): (u64,) = conn.query_first(q).await?.unwrap_or((0,));
    Ok(c)
}

#[derive(Debug, Clone)]
pub struct PkCol {
    pub name: String,
    pub numeric: bool,
}

/// Primary-Key-Spalten (in Ordinal-Reihenfolge) + Typ-Info.
pub async fn pk_columns(conn: &mut Conn, db: &str, table: &str) -> Result<Vec<PkCol>> {
    let q = format!(
        "SELECT k.COLUMN_NAME, c.DATA_TYPE \
         FROM information_schema.KEY_COLUMN_USAGE k \
         JOIN information_schema.COLUMNS c \
           ON c.TABLE_SCHEMA = k.TABLE_SCHEMA AND c.TABLE_NAME = k.TABLE_NAME AND c.COLUMN_NAME = k.COLUMN_NAME \
         WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ? AND k.CONSTRAINT_NAME = 'PRIMARY' \
         ORDER BY k.ORDINAL_POSITION"
    );
    let rows: Vec<(String, String)> = conn.exec(q, (db, table)).await?;
    Ok(rows
        .into_iter()
        .map(|(name, dt)| PkCol {
            numeric: NUMERIC_TYPES.contains(&dt.to_lowercase().as_str()),
            name,
        })
        .collect())
}

/// Spalten, die binär sind und hex-kodiert übertragen werden.
pub async fn binary_column_indexes(conn: &mut Conn, db: &str, table: &str) -> Result<Vec<usize>> {
    let q = format!(
        "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION"
    );
    let rows: Vec<(String, String)> = conn.exec(q, (db, table)).await?;
    const BIN_TYPES: &[&str] = &[
        "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "geometry",
        "bit",
    ];
    let names = column_names(conn, db, table).await?;
    let mut idx = Vec::new();
    for (i, name) in names.iter().enumerate() {
        if let Some((_, dt)) = rows.iter().find(|(n, _)| n == name) {
            if BIN_TYPES.contains(&dt.to_lowercase().as_str()) {
                idx.push(i);
            }
        }
    }
    Ok(idx)
}

/// Sortier-Klausel nach PK.
pub fn order_by(pks: &[PkCol]) -> String {
    if pks.is_empty() {
        String::new()
    } else {
        let cols: Vec<String> = pks.iter().map(|p| quote_ident(&p.name)).collect();
        format!(" ORDER BY {}", cols.join(", "))
    }
}

/// Streaming-Pass über eine Tabelle (Text-Protokoll) mit async-Callback pro Zeile.
/// `f` liefert false, um den Scan abzubrechen.
pub async fn scan_table<F, Fut>(conn: &mut Conn, db: &str, table: &str, where_clause: Option<&str>, order: bool, pks: &[PkCol], mut f: F) -> Result<()>
where
    F: FnMut(Vec<Value>) -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let mut sql = format!("SELECT * FROM {}", qualified(db, table));
    if let Some(w) = where_clause {
        if !w.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        }
    }
    if order {
        sql.push_str(&order_by(pks));
    }
    let result = conn.query_iter(sql).await.context("SELECT fehlgeschlagen")?;
    let mut result = result;
    while let Some(row) = result.next().await.context("Fehler beim Zeilen-Lesen")? {
        let values: Vec<Value> = row.unwrap();
        if !f(values).await {
            break;
        }
    }
    Ok(())
}

/// WHERE-Klausel für eine Menge von Buckets (Drill-Down, ein Scan für alle).
pub fn bucket_where(pks: &[PkCol], chunked: u32, buckets: &[String]) -> Result<String> {
    if pks.is_empty() {
        bail!("Tabelle hat keinen Primary Key — Bucket-Abfrage unmöglich");
    }
    let last = pks.last().unwrap();
    let col = quote_ident(&last.name);
    let m: u64 = 10u64.pow(chunked.min(9));
    let mut vals: Vec<String> = Vec::with_capacity(buckets.len());
    if last.numeric {
        for b in buckets {
            if !crate::hash::is_valid_int_literal(b) {
                bail!("Ungültiger numerischer Bucket: {b}");
            }
            vals.push(format!("ABS({col}) % {m} = {b}"));
        }
        Ok(format!("({})", vals.join(" OR ")))
    } else {
        let mut quoted = Vec::with_capacity(buckets.len());
        let mut buf = Vec::new();
        for b in buckets {
            buf.clear();
            crate::hash::sql_quote(b.as_bytes(), &mut buf);
            quoted.push(format!("RIGHT({col}, {chunked}) = {}", String::from_utf8_lossy(&buf)));
        }
        Ok(format!("({})", quoted.join(" OR ")))
    }
}

/// WHERE-Klausel für konkrete PK-Werte (kanonische Strings) — zum Zeilen-Nachladen.
pub fn pk_in_where(pks: &[PkCol], canonical_pks: &[String]) -> Result<String> {
    if pks.is_empty() {
        bail!("Tabelle hat keinen Primary Key");
    }
    if canonical_pks.is_empty() {
        return Ok("FALSE".to_string());
    }
    let mut buf = Vec::new();
    if pks.len() == 1 {
        let col = quote_ident(&pks[0].name);
        let mut items = Vec::with_capacity(canonical_pks.len());
        for cp in canonical_pks {
            buf.clear();
            if pks[0].numeric {
                if !crate::hash::is_valid_int_literal(cp) {
                    bail!("PK ist numerisch, aber Wert nicht: {cp}");
                }
                buf.extend_from_slice(cp.as_bytes());
            } else {
                crate::hash::sql_quote(cp.as_bytes(), &mut buf);
            }
            items.push(String::from_utf8_lossy(&buf).to_string());
        }
        Ok(format!("{col} IN ({})", items.join(", ")))
    } else {
        // Composite: (k1,k2) IN ((v1,v2), ...)
        let mut tuples = Vec::with_capacity(canonical_pks.len());
        for cp in canonical_pks {
            let parts: Vec<&str> = cp.split(crate::hash::PK_SEP).collect();
            if parts.len() != pks.len() {
                bail!("PK-Komponentenanzahl passt nicht: {cp}");
            }
            let mut vals = Vec::with_capacity(parts.len());
            for (i, part) in parts.iter().enumerate() {
                buf.clear();
                if pks[i].numeric {
                    if !crate::hash::is_valid_int_literal(part) {
                        bail!("PK ist numerisch, aber Wert nicht: {part}");
                    }
                    buf.extend_from_slice(part.as_bytes());
                } else {
                    crate::hash::sql_quote(part.as_bytes(), &mut buf);
                }
                vals.push(String::from_utf8_lossy(&buf).to_string());
            }
            tuples.push(format!("({})", vals.join(",")));
        }
        let cols: Vec<String> = pks.iter().map(|p| quote_ident(&p.name)).collect();
        Ok(format!("({}) IN ({})", cols.join(","), tuples.join(", ")))
    }
}

/// Keyset-Paginierung: WHERE (k1,k2,…) > (letzte PK-Werte) für die nächste Page.
pub fn pk_gt_where(pks: &[PkCol], last_canonical: &str) -> Result<String> {
    if pks.is_empty() {
        bail!("Tabelle hat keinen Primary Key");
    }
    let parts: Vec<&str> = last_canonical.split(crate::hash::PK_SEP).collect();
    if parts.len() != pks.len() {
        bail!("PK-Komponentenanzahl passt nicht für Keyset: {last_canonical}");
    }
    let mut buf = Vec::new();
    let mut vals = Vec::with_capacity(parts.len());
    for (i, part) in parts.iter().enumerate() {
        buf.clear();
        if pks[i].numeric {
            if !crate::hash::is_valid_int_literal(part) {
                bail!("PK ist numerisch, aber Wert nicht: {part}");
            }
            buf.extend_from_slice(part.as_bytes());
        } else {
            crate::hash::sql_quote(part.as_bytes(), &mut buf);
        }
        vals.push(String::from_utf8_lossy(&buf).to_string());
    }
    let cols: Vec<String> = pks.iter().map(|p| quote_ident(&p.name)).collect();
    Ok(format!("({}) > ({})", cols.join(","), vals.join(",")))
}

/// Kanonischer PK-String eines Zeilenwerts via PK-Metadaten.
pub fn pk_of_row(values: &[Value], pk_indexes: &[usize]) -> String {
    crate::hash::pk_canonical(values, pk_indexes)
}

/// SHOW CREATE TABLE normalisieren (für Schema-Vergleich).
pub fn normalize_schema(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    for line in sql.lines() {
        let l = line.trim();
        if l.starts_with("/*") || l.is_empty() {
            continue;
        }
        // AUTO_INCREMENT=12345 ignorieren (Änderungsalarm sonst bei jedem Insert)
        let mut parts: Vec<&str> = Vec::new();
        for piece in l.split_whitespace() {
            let p = piece.trim_end_matches(',');
            if p.starts_with("AUTO_INCREMENT=") {
                continue;
            }
            parts.push(piece);
        }
        out.push_str(&parts.join(" "));
        out.push('\n');
    }
    out
}
