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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PkKind {
    Int,
    UInt,
    Char,
    Binary,
    /// Not supported by range chunking (DATE, DECIMAL, UUID-as-char is still Char, ...).
    Unsupported,
}

impl PkKind {
    pub fn is_range_supported(self) -> bool {
        matches!(self, PkKind::Int | PkKind::UInt | PkKind::Char | PkKind::Binary)
    }
}

#[derive(Debug, Clone)]
pub struct PkCol {
    pub name: String,
    pub numeric: bool,
    pub kind: PkKind,
    pub column_type: String,
    /// Prefix length for indexed TEXT/BLOB PKs (0 = full column).
    pub sub_part: u64,
}

impl PkCol {
    pub fn is_range_supported(&self) -> bool {
        self.kind.is_range_supported() && self.sub_part == 0
    }
}

/// Primary-Key-Spalten (in Ordinal-Reihenfolge) + Typ-Info.
pub async fn pk_columns(conn: &mut Conn, db: &str, table: &str) -> Result<Vec<PkCol>> {
    let q = "SELECT s.COLUMN_NAME, c.DATA_TYPE, c.COLUMN_TYPE, COALESCE(s.SUB_PART, 0) \
         FROM information_schema.STATISTICS s \
         JOIN information_schema.COLUMNS c \
           ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME \
         WHERE s.TABLE_SCHEMA = ? AND s.TABLE_NAME = ? AND s.INDEX_NAME = 'PRIMARY' \
         ORDER BY s.SEQ_IN_INDEX";
    let rows: Vec<(String, String, String, u64)> = conn.exec(q, (db, table)).await?;
    Ok(rows
        .into_iter()
        .map(|(name, dt, column_type, sub_part)| {
            let dt = dt.to_lowercase();
            let kind = match dt.as_str() {
                "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" | "year" => {
                    if column_type.to_lowercase().contains("unsigned") {
                        PkKind::UInt
                    } else {
                        PkKind::Int
                    }
                }
                "char" | "varchar" => PkKind::Char,
                "binary" | "varbinary" => PkKind::Binary,
                _ => PkKind::Unsupported,
            };
            PkCol {
                numeric: NUMERIC_TYPES.contains(&dt.as_str()),
                name,
                kind,
                column_type,
                sub_part,
            }
        })
        .collect())
}

/// Range-Chunking braucht genau eine PK-Spalte mit einem unterstuetzten Typ.
pub fn validate_range_pk(pks: &[PkCol], db: &str, table: &str) -> Result<()> {
    if pks.is_empty() {
        bail!("{db}.{table} hat keinen Primary Key — Range-Chunking nicht moeglich");
    }
    if pks.len() != 1 {
        let cols: Vec<String> = pks.iter().map(|p| p.name.clone()).collect();
        bail!(
            "{db}.{table}: Range-Chunking unterstuetzt nur einen einspaltigen Primary Key, \
             gefunden: {cols:?}. Setze \"chunk_mode\": \"mod\" fuer den alten Modus."
        );
    }
    let p = &pks[0];
    if !p.is_range_supported() {
        bail!(
            "{db}.{table}.{}: Range-Chunking unterstuetzt nur INTEGER, CHAR/VARCHAR und \
             BINARY/VARBINARY Primary Keys (Typ: {}, SUB_PART: {}). \
             Setze \"chunk_mode\": \"mod\" fuer den alten Modus.",
            p.name,
            p.column_type,
            p.sub_part
        );
    }
    Ok(())
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

/// WHERE-Klausel für eine Menge von Buckets (alter Modus, Drill-Down).
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
        let ct = last.column_type.to_ascii_lowercase();
        let expr = if ct == "date" {
            format!("DATE_FORMAT({col}, '%Y-%m-%d')")
        } else if ct.starts_with("datetime") || ct.starts_with("timestamp") {
            format!("DATE_FORMAT({col}, '%Y-%m-%d %H:%i:%s.%f')")
        } else {
            col.clone()
        };
        let mut quoted = Vec::with_capacity(buckets.len());
        let mut buf = Vec::new();
        for b in buckets {
            buf.clear();
            crate::hash::sql_quote(b.as_bytes(), &mut buf);
            quoted.push(format!("RIGHT({expr}, {chunked}) = {}", String::from_utf8_lossy(&buf)));
        }
        Ok(format!("({})", quoted.join(" OR ")))
    }
}

/// WHERE-Klausel für Range-Chunks. Numerische Keys sind der untere Rand,
/// Char-Keys ein Prefix, Binary-Keys Hex-Bytes.
pub fn chunk_where(
    pks: &[PkCol],
    mode: crate::hash::ChunkMode,
    chunked: u32,
    keys: &[String],
) -> Result<String> {
    if mode == crate::hash::ChunkMode::Mod {
        return bucket_where(pks, chunked, keys);
    }
    validate_range_pk(pks, "", "")?;
    let pk = &pks[0];
    let col = quote_ident(&pk.name);
    if keys.is_empty() {
        return Ok("FALSE".to_string());
    }
    let mut vals: Vec<String> = Vec::with_capacity(keys.len());
    match pk.kind {
        PkKind::Int | PkKind::UInt => {
            let width = 10i128.pow(chunked.min(6) as u32);
            for k in keys {
                if !crate::hash::is_valid_int_literal(k) {
                    bail!("Ungültiger numerischer Chunk-Key: {k}");
                }
                let low = k.parse::<i128>().map_err(|_| anyhow::anyhow!("Chunk-Key zu groß: {k}"))?;
                let high = low + width;
                vals.push(format!("({col} >= {low} AND {col} < {high})"));
            }
        }
        PkKind::Char => {
            let trim = chunked as usize;
            for k in keys {
                if k.is_empty() {
                    vals.push(format!("(CHAR_LENGTH({col}) <= {trim})"));
                    continue;
                }
                let mut escaped = String::with_capacity(k.len() + 8);
                for c in k.chars() {
                    match c {
                        '\\' | '%' | '_' => {
                            escaped.push('\\');
                            escaped.push(c);
                        }
                        _ => escaped.push(c),
                    }
                }
                escaped.push('%');
                let mut buf = Vec::new();
                crate::hash::sql_quote(escaped.as_bytes(), &mut buf);
                let lit = String::from_utf8_lossy(&buf);
                let group_len = k.chars().count() + trim;
                vals.push(format!(
                    "({col} LIKE {lit} ESCAPE '\\\\' AND CHAR_LENGTH({col}) = {group_len})"
                ));
            }
        }
        PkKind::Binary => {
            let trim = chunked as usize;
            for k in keys {
                if k.is_empty() {
                    vals.push(format!("(OCTET_LENGTH({col}) <= {trim})"));
                    continue;
                }
                let prefix = hex::decode(k)
                    .map_err(|_| anyhow::anyhow!("Ungültiger Binary-Chunk-Key: {k}"))?;
                let group_len = prefix.len() + trim;
                let mut cond = format!("{col} >= 0x{}", hex::encode_upper(&prefix));
                if let Some(next) = next_binary_prefix(&prefix) {
                    cond.push_str(&format!(" AND {col} < 0x{}", hex::encode_upper(&next)));
                }
                cond.push_str(&format!(" AND OCTET_LENGTH({col}) = {group_len}"));
                vals.push(format!("({cond})"));
            }
        }
        PkKind::Unsupported => bail!("PK-Typ wird von Range-Chunking nicht unterstuetzt"),
    }
    Ok(format!("({})", vals.join(" OR ")))
}

/// Naechster lexikographischer Byte-Prefix (exklusive Obergrenze).
fn next_binary_prefix(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.last_mut() {
        if *last != 0xFF {
            *last += 1;
            return Some(out);
        }
        out.pop();
    }
    None
}

/// SQL-Literal einer kanonischen PK-Komponente.
fn pk_component_literal(pk: &PkCol, part: &str, out: &mut Vec<u8>) -> Result<()> {
    match pk.kind {
        PkKind::Binary => {
            if !part.chars().all(|c| c.is_ascii_hexdigit()) || part.len() % 2 != 0 {
                bail!("BINARY-PK ist nicht hex-kodiert: {part:?}");
            }
            out.extend_from_slice(b"0x");
            out.extend_from_slice(part.as_bytes());
        }
        _ if pk.numeric => {
            if !crate::hash::is_valid_int_literal(part) {
                bail!("PK ist numerisch, aber Wert nicht: {part}");
            }
            out.extend_from_slice(part.as_bytes());
        }
        _ => crate::hash::sql_quote(part.as_bytes(), out),
    }
    Ok(())
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
            pk_component_literal(&pks[0], cp, &mut buf)?;
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
                pk_component_literal(&pks[i], part, &mut buf)?;
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
        pk_component_literal(&pks[i], part, &mut buf)?;
        vals.push(String::from_utf8_lossy(&buf).to_string());
    }
    let cols: Vec<String> = pks.iter().map(|p| quote_ident(&p.name)).collect();
    Ok(format!("({}) > ({})", cols.join(","), vals.join(",")))
}

/// Kanonischer PK-String eines Zeilenwerts via PK-Metadaten.
pub fn pk_of_row(values: &[Value], pk_indexes: &[usize], pks: &[PkCol]) -> String {
    crate::hash::pk_canonical(values, pk_indexes, pks)
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
