use crate::hash::{self, ChunkMode};
use crate::mysqlutil::{self, PkCol, PkKind};
use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_async::Conn;
use std::collections::HashMap;

pub struct SqlRowHash {
    pub bucket: String,
    pub pk: String,
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

pub fn pk_expr(pks: &[PkCol]) -> Result<String> {
    if pks.is_empty() {
        anyhow::bail!("Tabelle hat keinen Primary Key");
    }
    let parts: Vec<String> = pks.iter().map(pk_component_expr).collect();
    if parts.len() == 1 {
        Ok(parts[0].clone())
    } else {
        Ok(format!("CONCAT_WS(CHAR(31), {})", parts.join(", ")))
    }
}

pub fn row_md5_expr(columns: &[String]) -> String {
    let parts: Vec<String> = columns
        .iter()
        .map(|name| {
            let col = mysqlutil::quote_ident(name);
            format!(
                "IF({col} IS NULL, 0x00, CONCAT(0x01, \
                 UNHEX(LPAD(HEX(OCTET_LENGTH(CAST({col} AS BINARY))), 8, '0')), \
                 CAST({col} AS BINARY)))"
            )
        })
        .collect();
    format!("MD5(CONCAT({}))", parts.join(", "))
}

fn group_query(
    db: &str,
    table: &str,
    pks: &[PkCol],
    columns: &[String],
    mode: ChunkMode,
    chunked: u32,
) -> Result<String> {
    let bucket = bucket_expr(pks, mode, chunked)?;
    let row_md5 = row_md5_expr(columns);
    Ok(format!(
        "SELECT bucket, COUNT(*) AS cnt, \
         LOWER(MD5(CONCAT_WS('|', MD5(bucket), COUNT(*), \
         IFNULL(SUM(CAST(CONV(SUBSTR(row_md5, 1, 16), 16, 10) AS UNSIGNED)), 0), \
         IFNULL(SUM(CAST(CONV(SUBSTR(row_md5, 17, 16), 16, 10) AS UNSIGNED)), 0)))) AS hash \
         FROM (SELECT {bucket} AS bucket, {row_md5} AS row_md5 FROM {}) AS x \
         GROUP BY bucket ORDER BY bucket",
        mysqlutil::qualified(db, table)
    ))
}

fn row_query(
    db: &str,
    table: &str,
    pks: &[PkCol],
    columns: &[String],
    mode: ChunkMode,
    chunked: u32,
    buckets: Option<&[String]>,
) -> Result<String> {
    let bucket = bucket_expr(pks, mode, chunked)?;
    let pk = pk_expr(pks)?;
    let row_md5 = row_md5_expr(columns);
    let mut sql = format!(
        "SELECT {pk} AS pk, {bucket} AS bucket, {row_md5} AS hash \
         FROM {}",
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

pub async fn chunk_hashes(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[PkCol],
    mode: ChunkMode,
    chunked: u32,
) -> Result<HashMap<String, (u64, String)>> {
    let columns = mysqlutil::column_names(conn, db, table).await?;
    let sql = group_query(db, table, pks, &columns, mode, chunked)?;
    log::debug!("SQL group hashes: {sql}");
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
    mode: ChunkMode,
    chunked: u32,
    buckets: Option<&[String]>,
) -> Result<Vec<SqlRowHash>> {
    let columns = mysqlutil::column_names(conn, db, table).await?;
    let sql = row_query(db, table, pks, &columns, mode, chunked, buckets)?;
    log::debug!("SQL row hashes: {sql}");
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
        let (pk, bucket, hash): (String, String, String) = mysql_async::from_row(row);
        out.push(SqlRowHash { bucket, pk, hash });
    }
    Ok(out)
}
