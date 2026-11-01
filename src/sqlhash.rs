use crate::hash::{self, ChunkMode};
use crate::mysqlutil::{self, PkCol, PkKind};
use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_async::Conn;
use std::collections::HashMap;
use tokio::sync::mpsc;

pub struct SqlRowHash {
    pub bucket: String,
    pub pk: Vec<String>,
    pub hash: String,
}

fn width(chunked: u32) -> i128 {
    10i128.pow(chunked.clamp(1, 6))
}

fn text_expr(expr: &str) -> String {
    format!("CONVERT({expr} USING utf8mb4) COLLATE utf8mb4_bin")
}

fn numeric_mod_expr(col: &str, chunked: u32) -> String {
    format!(
        "LPAD(CAST(CAST(ABS(CAST({col} AS DECIMAL(65,0))) % {} AS UNSIGNED) AS CHAR), {}, '0')",
        width(chunked),
        chunked
    )
}

fn time_component_expr(col: &str) -> String {
    let secs = format!("TIME_TO_SEC({col})");
    let abs_secs = format!("ABS({secs})");
    format!(
        "CONCAT(IF({col} < 0, '-', ''),          LPAD(FLOOR({abs_secs} / 86400), 2, '0'), ' ',          LPAD(FLOOR(MOD({abs_secs}, 86400) / 3600), 2, '0'), ':',          LPAD(FLOOR(MOD({abs_secs}, 3600) / 60), 2, '0'), ':',          LPAD(MOD(FLOOR({abs_secs}), 60), 2, '0'), '.',          LPAD(MICROSECOND(ABS({col})), 6, '0'))"
    )
}

fn date_expr(col: &str, column_type: &str) -> Option<String> {
    let ct = column_type.to_ascii_lowercase();
    if ct == "date" {
        Some(format!("RIGHT(DATE_FORMAT({col}, '%Y-%m-%d'), {{N}})"))
    } else if ct.starts_with("datetime") || ct.starts_with("timestamp") {
        Some(format!(
            "RIGHT(DATE_FORMAT({col}, '%Y-%m-%d %H:%i:%s.%f'), {{N}})"
        ))
    } else {
        None
    }
}

fn pk_component_expr(pk: &PkCol) -> String {
    let col = mysqlutil::quote_ident(&pk.name);
    let ct = pk.column_type.to_ascii_lowercase();
    if ct == "date" {
        format!("DATE_FORMAT({col}, '%Y-%m-%d')")
    } else if ct == "time" {
        time_component_expr(&col)
    } else if ct.starts_with("datetime") || ct.starts_with("timestamp") {
        format!("DATE_FORMAT({col}, '%Y-%m-%d %H:%i:%s.%f')")
    } else {
        match pk.kind {
            PkKind::Binary => format!("LOWER(HEX({col}))"),
            _ => format!("CAST({col} AS CHAR)"),
        }
    }
}

pub fn bucket_expr(pks: &[PkCol], mode: ChunkMode, chunked: u32) -> Result<String> {
    if pks.is_empty() {
        anyhow::bail!("Tabelle hat keinen Primary Key");
    }
    let n = chunked.clamp(1, 6);
    match mode {
        ChunkMode::Range => {
            let pk = &pks[0];
            let col = mysqlutil::quote_ident(&pk.name);
            match pk.kind {
                PkKind::Int | PkKind::UInt => {
                    let ty = if pk.kind == PkKind::UInt {
                        "UNSIGNED"
                    } else {
                        "SIGNED"
                    };
                    Ok(format!(
                        "CAST(CAST(FLOOR({col} / {}) * {} AS {ty}) AS CHAR)",
                        width(n),
                        width(n)
                    ))
                }
                PkKind::Char => Ok(text_expr(&format!(
                    "LEFT({col}, GREATEST(CHAR_LENGTH({col}) - {n}, 0))"
                ))),
                PkKind::Binary => Ok(format!(
                    "LOWER(HEX(LEFT({col}, GREATEST(OCTET_LENGTH({col}) - {n}, 0))))"
                )),
                PkKind::Unsupported => {
                    anyhow::bail!("PK-Typ wird von Range-Chunking nicht unterstuetzt")
                }
            }
        }
        ChunkMode::Mod => {
            let pk = pks.last().unwrap();
            let col = mysqlutil::quote_ident(&pk.name);
            if pk.numeric {
                Ok(numeric_mod_expr(&col, n))
            } else if pk.column_type.eq_ignore_ascii_case("time") {
                Ok(text_expr(&format!(
                    "RIGHT({}, {n})",
                    time_component_expr(&col)
                )))
            } else if let Some(expr) = date_expr(&col, &pk.column_type) {
                Ok(text_expr(&expr.replace("{N}", &n.to_string())))
            } else {
                match pk.kind {
                    PkKind::Binary => Ok(format!("RIGHT(LOWER(HEX({col})), {n})")),
                    _ => Ok(text_expr(&format!("RIGHT(CAST({col} AS CHAR), {n})"))),
                }
            }
        }
    }
}

/// Wertausdruck inkl. optionalem Transform/Runden (None = Spalte ignorieren).
fn field_value_expr(name: &str, opt: Option<&crate::proto::FieldOpt>) -> Option<String> {
    use crate::proto::FieldTransform;
    let mut expr = mysqlutil::quote_ident(name);
    if let Some(opt) = opt {
        if opt.transform == Some(FieldTransform::Ignore) {
            return None;
        }
        if opt.transform == Some(FieldTransform::Lower) {
            expr = format!("LOWER({expr})");
        }
        if opt.transform == Some(FieldTransform::Trim) {
            expr = format!("TRIM({expr})");
        }
        if let Some(scale) = opt.float_precision {
            expr = format!("ROUND({expr}, {scale})");
        }
    }
    Some(expr)
}

/// MySQL 9.x loest Built-ins wie MD5/SHA1 nur mit Default-Datenbank auf.
/// Die Queries sind zwar voll qualifiziert, aber `USE` macht den Select-Pfad
/// unabhaengig davon (und ist pro Tabellen-Task-Connection ohnehin eindeutig).
async fn use_database(conn: &mut Conn, db: &str) -> Result<()> {
    conn.query_drop(format!("USE {}", mysqlutil::quote_ident(db)))
        .await
        .context("USE Datenbank fehlgeschlagen")
}

/// SQL-Hash-Algorithmus fuer den Select-Pfad.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SqlHashAlgo {
    #[default]
    Sha2,
    Crc32,
}

impl SqlHashAlgo {
    fn row_hash(self, parts: &str) -> String {
        match self {
            SqlHashAlgo::Sha2 => format!("LEFT(SHA2(CONCAT({}), 256), 32)", parts),
            SqlHashAlgo::Crc32 => format!("LPAD(CAST(CRC32(CONCAT({})) AS CHAR), 10, '0')", parts),
        }
    }

    fn group_hash(self, inner: &str) -> String {
        match self {
            SqlHashAlgo::Sha2 => {
                format!("LOWER(LEFT(SHA2(CONCAT_WS('|', LEFT(SHA2(bucket, 256), 32), {inner}), 256), 32))")
            }
            SqlHashAlgo::Crc32 => {
                format!("LPAD(CAST(CRC32(CONCAT_WS('|', LPAD(CAST(CRC32(bucket) AS CHAR), 10, '0'), {inner})) AS CHAR), 10, '0')")
            }
        }
    }
}

pub fn row_hash_expr(columns: &[String], opts: &[crate::proto::FieldOpt], algo: SqlHashAlgo) -> String {
    let parts: Vec<String> = columns
        .iter()
        .enumerate()
        .filter_map(|(i, name)| {
            let col = mysqlutil::quote_ident(name);
            let expr = field_value_expr(name, crate::proto::field_opt(opts, i))?;
            Some(format!(
                "IF({col} IS NULL, 0x00, CONCAT(0x01, \
                 UNHEX(LPAD(HEX(OCTET_LENGTH(CAST({expr} AS BINARY))), 8, '0')), \
                 CAST({expr} AS BINARY)))"
            ))
        })
        .collect();
    algo.row_hash(&parts.join(", "))
}

fn group_query(
    db: &str,
    table: &str,
    pks: &[PkCol],
    columns: &[String],
    opts: &[crate::proto::FieldOpt],
    mode: ChunkMode,
    chunked: u32,
    algo: SqlHashAlgo,
) -> Result<String> {
    let bucket = bucket_expr(pks, mode, chunked)?;
    let row_hash = row_hash_expr(columns, opts, algo);
    let inner = match algo {
        SqlHashAlgo::Sha2 => {
            "COUNT(*), \
         IFNULL(SUM(CAST(CONV(SUBSTR(row_hash, 1, 16), 16, 10) AS UNSIGNED)), 0), \
         IFNULL(SUM(CAST(CONV(SUBSTR(row_hash, 17, 16), 16, 10) AS UNSIGNED)), 0)"
        }
        SqlHashAlgo::Crc32 => {
            "COUNT(*), \
         IFNULL(SUM(CAST(row_hash AS UNSIGNED)), 0)"
        }
    };
    let hash_expr = algo.group_hash(inner);
    Ok(format!(
        "SELECT bucket, COUNT(*) AS cnt, {hash_expr} AS hash \
         FROM (SELECT {bucket} AS bucket, {row_hash} AS row_hash FROM {}) AS x \
         GROUP BY bucket ORDER BY BINARY bucket",
        mysqlutil::qualified(db, table)
    ))
}

fn row_query(
    db: &str,
    table: &str,
    pks: &[PkCol],
    columns: &[String],
    opts: &[crate::proto::FieldOpt],
    mode: ChunkMode,
    chunked: u32,
    buckets: Option<&[String]>,
    algo: SqlHashAlgo,
) -> Result<String> {
    let bucket = bucket_expr(pks, mode, chunked)?;
    let pk_cols: Vec<String> = pks
        .iter()
        .enumerate()
        .map(|(i, p)| format!("{} AS pk_{i}", pk_component_expr(p)))
        .collect();
    let row_hash = row_hash_expr(columns, opts, algo);
    let mut sql = format!(
        "SELECT {}, {bucket} AS bucket, {row_hash} AS hash \
         FROM {}",
        pk_cols.join(", "),
        mysqlutil::qualified(db, table)
    );
    if let Some(buckets) = buckets {
        if buckets.is_empty() {
            sql.push_str(" WHERE FALSE");
        } else {
            let mut buf = Vec::new();
            let mut vals = Vec::with_capacity(buckets.len());
            for b in buckets {
                buf.clear();
                hash::sql_quote(b.as_bytes(), &mut buf);
                vals.push(String::from_utf8_lossy(&buf).to_string());
            }
            sql.push_str(&format!(" WHERE ({bucket}) IN ({})", vals.join(", ")));
        }
    }
    sql.push_str(&mysqlutil::order_by(pks));
    Ok(sql)
}

/// Sortierklausel fuer einen Stream, dessen Buckets beim Lesen abgeschlossen
/// werden. Range ist bereits durch die PK-Reihenfolge gruppiert; Mod braucht
/// Bucket + PK als Sortierschluessel.
pub fn order_by_bucket(pks: &[PkCol], mode: ChunkMode, chunked: u32) -> Result<String> {
    match mode {
        ChunkMode::Range => Ok(mysqlutil::order_by(pks)),
        ChunkMode::Mod => {
            let bucket = bucket_expr(pks, mode, chunked)?;
            let cols: Vec<String> = pks
                .iter()
                .map(|p| mysqlutil::quote_ident(&p.name))
                .collect();
            Ok(format!(" ORDER BY BINARY ({bucket}), {}", cols.join(", ")))
        }
    }
}

/// Streamt die GROUP-BY-Bucket-Hashes in Bucket-Reihenfolge.
pub async fn stream_chunk_hashes(
    tx: mpsc::Sender<hash::ChunkHashEvent>,
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    opts: &[crate::proto::FieldOpt],
    mode: ChunkMode,
    chunked: u32,
    algo: SqlHashAlgo,
) -> Result<u64> {
    let columns = mysqlutil::column_names(conn, db, table).await?;
    use_database(conn, db).await?;
    let sql = group_query(db, table, pks, &columns, opts, mode, chunked, algo)?;
    log::warn!("SQL group hashes (stream): {sql}");
    let mut result = conn
        .query_iter(&sql)
        .await
        .context("SQL-Gruppen-Hashes fehlgeschlagen")?;
    let mut count = 0u64;
    while let Some(row) = result
        .next()
        .await
        .context("Fehler beim Lesen der Gruppen-Hashes")?
    {
        let (bucket, cnt, hash): (String, u64, String) = mysql_async::from_row(row);
        tx.send(hash::ChunkHashEvent::Item((bucket, cnt, hash)))
            .await
            .map_err(|_| anyhow::anyhow!("Chunk-Hash-Empfänger geschlossen"))?;
        count += 1;
    }
    Ok(count)
}

pub async fn chunk_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    opts: &[crate::proto::FieldOpt],
    mode: ChunkMode,
    chunked: u32,
    algo: SqlHashAlgo,
) -> Result<HashMap<String, (u64, String)>> {
    let columns = mysqlutil::column_names(conn, db, table).await?;
    use_database(conn, db).await?;
    let sql = group_query(db, table, pks, &columns, opts, mode, chunked, algo)?;
    log::warn!("SQL group hashes: {sql}");
    let mut result = conn
        .query_iter(&sql)
        .await
        .context("SQL-Gruppen-Hashes fehlgeschlagen")?;
    let mut out = HashMap::new();
    while let Some(row) = result
        .next()
        .await
        .context("Fehler beim Lesen der Gruppen-Hashes")?
    {
        let (bucket, count, hash): (String, u64, String) = mysql_async::from_row(row);
        out.insert(bucket, (count, hash));
    }
    Ok(out)
}

pub async fn row_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    opts: &[crate::proto::FieldOpt],
    mode: ChunkMode,
    chunked: u32,
    buckets: Option<&[String]>,
    algo: SqlHashAlgo,
) -> Result<Vec<SqlRowHash>> {
    let columns = mysqlutil::column_names(conn, db, table).await?;
    use_database(conn, db).await?;
    let sql = row_query(db, table, pks, &columns, opts, mode, chunked, buckets, algo)?;
    log::warn!("SQL row hashes: {sql}");
    let mut result = conn
        .query_iter(&sql)
        .await
        .context("SQL-Zeilen-Hashes fehlgeschlagen")?;
    let mut out = Vec::new();
    while let Some(row) = result
        .next()
        .await
        .context("Fehler beim Lesen der Zeilen-Hashes")?
    {
        let n = pks.len();
        let mut pk = Vec::with_capacity(n);
        for i in 0..n {
            pk.push(
                row.get_opt::<String, _>(i)
                    .with_context(|| format!("PK-Spalte {i} fehlt"))?
                    .map_err(|e| anyhow::anyhow!("PK-Spalte {i}: {e:?}"))?,
            );
        }
        let bucket = row
            .get_opt::<String, _>(n)
            .context("Bucket-Spalte fehlt")?
            .map_err(|e| anyhow::anyhow!("Bucket-Spalte: {e:?}"))?;
        let hash = row
            .get_opt::<String, _>(n + 1)
            .context("Hash-Spalte fehlt")?
            .map_err(|e| anyhow::anyhow!("Hash-Spalte: {e:?}"))?;
        out.push(SqlRowHash { bucket, pk, hash });
    }
    Ok(out)
}
