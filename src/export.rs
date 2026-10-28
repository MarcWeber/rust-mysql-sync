//! Export-Strategien auf der Client-Seite (Quelle):
//!   - select-stream : in-process Streaming-SELECT (mysql_async) — Default, binärsicher
//!   - to_file       : SELECT ... INTO OUTFILE auf dem MySQL-Host, Datei wird gelesen
//!   - mysql-e       : shellt `mysql --batch -e "SELECT ..."` und parst das TSV
//!   - mysqldump     : mysqldump in Temp-Pipe, INSERT-Statements werden geparst
//!
//! Alle Strategien liefern Value-Zeilen über einen mpsc-Channel (Backpressure).
//! Fallback: Nur wenn eine Strategie NULL Zeilen geliefert hat, wird die nächste
//! probiert (mid-stream-Fehler können nicht sicher wiederholt werden).

use crate::config::{Config, MysqlCfg};
use crate::hash::{tsv_unescape_field, value_to_sql};
use crate::mysqlutil::{self, qualified};
use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::Value;
use std::process::Stdio;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq)]
pub enum Strategy {
    SelectStream,
    ToFile(String),
    MysqlE,
    Mysqldump,
}

#[derive(Debug)]
pub enum RowMsg {
    Row(Vec<Value>),
    Err(String),
    Done,
}

pub fn parse_strategy(s: &str) -> Result<Strategy> {
    let t = s.trim().to_lowercase();
    if let Some(dir) = t.strip_prefix("to_file:").or_else(|| t.strip_prefix("to-file:")) {
        return Ok(Strategy::ToFile(dir.to_string()));
    }
    match t.as_str() {
        "select-stream" | "select_stream" | "stream" => Ok(Strategy::SelectStream),
        "to_file" | "to-file" | "outfile" => Ok(Strategy::ToFile("/tmp".into())),
        "mysql-e" | "mysql_e" | "mysqle" => Ok(Strategy::MysqlE),
        "mysqldump" => Ok(Strategy::Mysqldump),
        other => bail!("Unbekannte Dump-Strategie: '{other}' (erlaubt: select-stream, to_file[:DIR], mysql-e, mysqldump)"),
    }
}

/// Zeilen-Fetch als eigener Task; sendet RowMsg bis Done/Err.
async fn run_strategy(
    strategy: Strategy,
    mysql: MysqlCfg,
    db: String,
    table: String,
    where_clause: Option<String>,
    tx: mpsc::Sender<RowMsg>,
) {
    let res = match strategy {
        Strategy::SelectStream => {
            let r = mysqlutil::new_pool(&mysql, None).and_then(|p| Ok(p));
            match r {
                Ok(pool) => fetch_stream(&pool, &db, &table, where_clause.as_deref(), &tx).await,
                Err(e) => Err(e),
            }
        }
        Strategy::ToFile(dir) => match mysqlutil::new_pool(&mysql, None) {
            Ok(pool) => fetch_to_file(&pool, &db, &table, where_clause.as_deref(), &dir, &tx).await,
            Err(e) => Err(e),
        },
        Strategy::MysqlE => fetch_mysql_e(&mysql, &db, &table, where_clause.as_deref(), &tx).await,
        Strategy::Mysqldump => fetch_mysqldump(&mysql, &db, &table, where_clause.as_deref(), &tx).await,
    };
    if let Err(e) = res {
        let _ = tx.send(RowMsg::Err(format!("{e:#}"))).await;
    }
    let _ = tx.send(RowMsg::Done).await;
}

/// Startet den Row-Fetch mit Strategie-Fallback. Liefert den Empfangskanal.
pub fn spawn_fetch_with_fallback(
    cfg: &Config,
    db: String,
    table: String,
    where_clause: Option<String>,
) -> mpsc::Receiver<RowMsg> {
    let (tx, rx) = mpsc::channel::<RowMsg>(8);
    let strategies = cfg.strategies();
    let mysql = cfg.mysql.clone();
    tokio::spawn(async move {
        let mut last_err: Option<String> = None;
        for s in &strategies {
            let strat = match parse_strategy(s) {
                Ok(x) => x,
                Err(e) => {
                    log::warn!("[{db}.{table}] {e:#}");
                    last_err = Some(e.to_string());
                    continue;
                }
            };
            log::debug!("[{db}.{table}] Export-Strategie: {strat:?}");
            let (itx, mut irx) = mpsc::channel::<RowMsg>(8);
            let task = tokio::spawn(run_strategy(
                strat.clone(),
                mysql.clone(),
                db.clone(),
                table.clone(),
                where_clause.clone(),
                itx,
            ));
            let mut got_any = false;
            let mut task_err: Option<String> = None;
            let mut aborted = false;
            while let Some(msg) = irx.recv().await {
                match msg {
                    RowMsg::Row(v) => {
                        got_any = true;
                        if tx.send(RowMsg::Row(v)).await.is_err() {
                            aborted = true;
                            break;
                        }
                    }
                    RowMsg::Err(e) => task_err = Some(e),
                    RowMsg::Done => break,
                }
            }
            let _ = task.await;
            if aborted {
                return;
            }
            if got_any {
                // Teilweise übertragen — Fehler weiterreichen (kein sicheres Retry)
                if let Some(e) = task_err {
                    let _ = tx.send(RowMsg::Err(e)).await;
                }
                let _ = tx.send(RowMsg::Done).await;
                return;
            }
            // Keine Zeile geliefert → nächste Strategie probieren
            if let Some(e) = task_err {
                last_err = Some(e);
            }
            log::warn!("[{db}.{table}] Strategie {strat:?} lieferte nichts — Fallback …");
        }
        let _ = tx
            .send(RowMsg::Err(last_err.unwrap_or_else(|| "Keine Export-Strategie verfügbar".into())))
            .await;
    });
    rx
}

async fn fetch_stream(pool: &mysql_async::Pool, db: &str, table: &str, where_clause: Option<&str>, tx: &mpsc::Sender<RowMsg>) -> Result<()> {
    let mut conn = mysqlutil::session_conn(pool).await?;
    let mut sql = format!("SELECT * FROM {}", qualified(db, table));
    if let Some(w) = where_clause {
        if !w.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        }
    }
    let result = conn.query_iter(sql).await.context("SELECT fehlgeschlagen")?;
    let mut result = result;
    while let Some(row) = result.next().await.context("Zeilen-Lesen fehlgeschlagen")? {
        let values: Vec<Value> = row.unwrap();
        if tx.send(RowMsg::Row(values)).await.is_err() {
            break;
        }
    }
    Ok(())
}

async fn fetch_to_file(pool: &mysql_async::Pool, db: &str, table: &str, where_clause: Option<&str>, dir: &str, tx: &mpsc::Sender<RowMsg>) -> Result<()> {
    let mut conn = mysqlutil::session_conn(pool).await?;
    let fname = format!(
        "mysqlsync-{}.tsv",
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos()
    );
    let path = format!("{}/{fname}", dir.trim_end_matches('/'));

    let mut sql = format!(
        "SELECT * FROM {} INTO OUTFILE '{}' FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'",
        qualified(db, table),
        path
    );
    if let Some(w) = where_clause {
        if !w.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        }
    }
    conn.query_drop(sql)
        .await
        .context("SELECT INTO OUTFILE fehlgeschlagen (FILE-Privileg / secure_file_priv?)")?;

    let res = read_tsv_file(&path, tx).await;
    let _ = std::fs::remove_file(&path);
    res
}

async fn read_tsv_file(path: &str, tx: &mpsc::Sender<RowMsg>) -> Result<()> {
    let mut file = tokio::fs::File::open(path)
        .await
        .context("OUTFILE nicht lesbar (läuft MySQL auf einem anderen Host?")?;
    let mut buf = Vec::with_capacity(1024 * 1024);
    file.read_to_end(&mut buf).await?;
    parse_tsv_lines(&buf, tx).await
}

async fn fetch_mysql_e(mysql: &MysqlCfg, db: &str, table: &str, where_clause: Option<&str>, tx: &mpsc::Sender<RowMsg>) -> Result<()> {
    let mut sql = format!("SELECT * FROM {}", qualified(db, table));
    if let Some(w) = where_clause {
        if !w.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(w);
        }
    }
    let mut cmd = tokio::process::Command::new("mysql");
    cmd.arg("--batch")
        .arg("--skip-column-names")
        .arg("-h")
        .arg(&mysql.host)
        .arg("-P")
        .arg(mysql.port.to_string())
        .arg("-u")
        .arg(&mysql.user);
    if !mysql.password.is_empty() {
        cmd.arg(format!("-p{}", mysql.password));
    }
    cmd.arg(db).arg("-e").arg(&sql);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn().context("mysql-Client nicht gefunden (mysql-e Strategie)")?;
    let mut stdout = child.stdout.take().context("kein stdout")?;
    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).await?;
    let status = child.wait().await?;
    if !status.success() {
        bail!("mysql -e fehlgeschlagen (Exit {status})");
    }
    parse_tsv_lines(&buf, tx).await
}

async fn fetch_mysqldump(mysql: &MysqlCfg, db: &str, table: &str, where_clause: Option<&str>, tx: &mpsc::Sender<RowMsg>) -> Result<()> {
    if where_clause.is_some() {
        log::debug!(
            "[{db}.{table}] mysqldump: WHERE wird clientseitig gefiltert (Dump der ganzen Tabelle)"
        );
    }
    let mut cmd = tokio::process::Command::new("mysqldump");
    cmd.arg("--no-create-info")
        .arg("--skip-triggers")
        .arg("--skip-lock-tables")
        .arg("--single-transaction")
        .arg("--skip-extended-insert") // ein INSERT pro Zeile = einfaches, stromfähiges Parsing
        .arg("--compact")
        .arg("-h")
        .arg(&mysql.host)
        .arg("-P")
        .arg(mysql.port.to_string())
        .arg("-u")
        .arg(&mysql.user);
    if !mysql.password.is_empty() {
        cmd.arg(format!("-p{}", mysql.password));
    }
    cmd.arg(db).arg(table);
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());

    let mut child = cmd.spawn().context("mysqldump nicht gefunden")?;
    let mut stdout = child.stdout.take().context("kein stdout")?;
    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).await?;
    let status = child.wait().await?;
    if !status.success() {
        bail!("mysqldump fehlgeschlagen (Exit {status})");
    }
    let sql = String::from_utf8_lossy(&buf).to_string();
    let stmts = crate::import::split_sql_statements(&sql);
    for st in stmts {
        if let Some(values) = parse_insert_values(&st)? {
            if tx.send(RowMsg::Row(values)).await.is_err() {
                return Ok(());
            }
        }
    }
    Ok(())
}

async fn parse_tsv_lines(buf: &[u8], tx: &mpsc::Sender<RowMsg>) -> Result<()> {
    for line in buf.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let mut fields: Vec<Vec<u8>> = Vec::new();
        let mut cur: Vec<u8> = Vec::new();
        let mut i = 0;
        while i < line.len() {
            if line[i] == b'\\' && i + 1 < line.len() {
                cur.push(line[i]);
                cur.push(line[i + 1]);
                i += 2;
                continue;
            }
            if line[i] == b'\t' {
                fields.push(std::mem::take(&mut cur));
            } else {
                cur.push(line[i]);
            }
            i += 1;
        }
        fields.push(cur);
        let values: Vec<Value> = fields
            .iter()
            .map(|f| match tsv_unescape_field(f) {
                Ok(None) => Value::NULL,
                Ok(Some(b)) => Value::Bytes(b),
                Err(_) => Value::NULL,
            })
            .collect();
        if tx.send(RowMsg::Row(values)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Parst ein einzelnes INSERT INTO ... VALUES (...)-Statement in eine Zeile.
/// Liefert None für Nicht-INSERT-Statements.
fn parse_insert_values(stmt: &str) -> Result<Option<Vec<Value>>> {
    let trimmed = stmt.trim();
    let lower = trimmed.to_lowercase();
    if !lower.starts_with("insert") {
        return Ok(None);
    }
    let pos = lower.find("values").context("INSERT ohne VALUES")?;
    let rest = trimmed[pos + 6..].trim();
    let rest = rest.strip_prefix('(').context("VALUES ohne (")?;
    let rest = rest.strip_suffix(");").or_else(|| rest.strip_suffix(')')).unwrap_or(rest);

    let mut values = Vec::new();
    let bytes = rest.as_bytes();
    let mut i = 0;
    loop {
        while i < bytes.len() && (bytes[i] as char).is_whitespace() {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }
        let (value, next) = parse_sql_value(rest, i)?;
        values.push(value);
        i = next;
        while i < bytes.len() && (bytes[i] as char).is_whitespace() {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b',' {
            i += 1;
        } else {
            break;
        }
    }
    Ok(Some(values))
}

/// Liest ein SQL-Literal ab Position i; liefert (Value, nächste Position).
fn parse_sql_value(s: &str, i: usize) -> Result<(Value, usize)> {
    let bytes = s.as_bytes();
    if i >= bytes.len() {
        return Ok((Value::NULL, i));
    }
    match bytes[i] {
        b'\'' => {
            let mut out: Vec<u8> = Vec::new();
            let mut j = i + 1;
            while j < bytes.len() {
                match bytes[j] {
                    b'\\' if j + 1 < bytes.len() => {
                        let c = bytes[j + 1];
                        out.push(match c {
                            b'0' => 0x00,
                            b'n' => b'\n',
                            b'r' => b'\r',
                            b't' => b'\t',
                            b'b' => 0x08,
                            b'Z' => 0x1A,
                            other => other,
                        });
                        j += 2;
                    }
                    b'\'' => {
                        if j + 1 < bytes.len() && bytes[j + 1] == b'\'' {
                            out.push(b'\'');
                            j += 2;
                        } else {
                            return Ok((Value::Bytes(out), j + 1));
                        }
                    }
                    _ => {
                        out.push(bytes[j]);
                        j += 1;
                    }
                }
            }
            bail!("Nicht terminierter String in INSERT")
        }
        b'0' if i + 1 < bytes.len() && bytes[i + 1] == b'x' => {
            let start = i + 2;
            let mut j = start;
            while j < bytes.len() && bytes[j].is_ascii_hexdigit() {
                j += 1;
            }
            let decoded = hex::decode(&s[start..j]).unwrap_or_default();
            Ok((Value::Bytes(decoded), j))
        }
        b'b' if i + 1 < bytes.len() && bytes[i + 1] == b'\'' => {
            let mut j = i + 2;
            let mut bits = String::new();
            while j < bytes.len() && (bytes[j] == b'0' || bytes[j] == b'1') {
                bits.push(bytes[j] as char);
                j += 1;
            }
            j += 1; // schließendes '
            let byte = u8::from_str_radix(&bits, 2).unwrap_or(0);
            Ok((Value::Bytes(vec![byte]), j))
        }
        b'N' if s[i..].starts_with("NULL") => Ok((Value::NULL, i + 4)),
        _ => {
            let mut j = i;
            while j < bytes.len() && bytes[j] != b',' {
                j += 1;
            }
            let text = s[i..j].trim();
            Ok((Value::Bytes(text.as_bytes().to_vec()), j))
        }
    }
}

/// Zeilen als SQL-Dump (REPLACE INTO) serialisieren — für allow_csv=false.
pub fn rows_to_replace_sql(db: &str, table: &str, columns: &[String], rows: &[Vec<Value>], hex_indexes: &[usize]) -> Vec<u8> {
    let mut out = Vec::with_capacity(rows.len() * 64);
    for row in rows {
        out.extend_from_slice(b"REPLACE INTO ");
        out.extend_from_slice(qualified(db, table).as_bytes());
        out.extend_from_slice(b" (");
        let col_sql: Vec<String> = columns.iter().map(|c| mysqlutil::quote_ident(c)).collect();
        out.extend_from_slice(col_sql.join(", ").as_bytes());
        out.extend_from_slice(b") VALUES (");
        for (i, v) in row.iter().enumerate() {
            if i > 0 {
                out.push(b',');
            }
            value_to_sql(v, hex_indexes.contains(&i), &mut out);
        }
        out.extend_from_slice(b");\n");
    }
    out
}
