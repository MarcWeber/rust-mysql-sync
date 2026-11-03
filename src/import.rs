//! Server-seitiger Import: TSV-Stream -> batched REPLACE INTO, PK-Löschungen,
//! SQL-Dump-Ausführung (gefiltert), Trigger-Handling.

use crate::hash::tsv_unescape_field;
use crate::mysqlutil::{self, qualified};
use anyhow::{bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Value};
use std::collections::HashSet;

const IMPORT_BATCH: usize = 1000;

/// Streaming-TSV-Parser: sammelt Zeilen, gibt Batches zurück.
pub struct TsvBatcher {
    pub columns: Vec<String>,
    pub hex_indexes: HashSet<usize>,
    buf: Vec<u8>,        // unvollständige Zeile
    batch: Vec<Vec<u8>>, // fertige Zeilen (roh)
}

impl TsvBatcher {
    pub fn new(columns: Vec<String>, hex_columns: &[String]) -> Self {
        let hex_indexes: HashSet<usize> = columns
            .iter()
            .enumerate()
            .filter(|(_, c)| hex_columns.contains(c))
            .map(|(i, _)| i)
            .collect();
        TsvBatcher {
            columns,
            hex_indexes,
            buf: Vec::new(),
            batch: Vec::new(),
        }
    }

    /// BIN-Frame in den Parser schieben; liefert fertige Zeilen zurück.
    pub fn push(&mut self, data: &[u8]) -> Result<Vec<Vec<Value>>> {
        let mut start = 0;
        for (i, &b) in data.iter().enumerate() {
            if b == b'\n' {
                self.buf.extend_from_slice(&data[start..i]);
                start = i + 1;
                self.batch.push(std::mem::take(&mut self.buf));
            }
        }
        self.buf.extend_from_slice(&data[start..]);
        self.flush(false)
    }

    /// Abschluss (letzte Zeile ohne \n) — flusht unabhängig von der Batch-Größe.
    pub fn finish(&mut self) -> Result<Vec<Vec<Value>>> {
        if !self.buf.is_empty() {
            let last = std::mem::take(&mut self.buf);
            self.batch.push(last);
        }
        self.flush(true)
    }

    fn flush(&mut self, force: bool) -> Result<Vec<Vec<Value>>> {
        if !force && self.batch.len() < IMPORT_BATCH {
            return Ok(Vec::new());
        }
        let rows = std::mem::take(&mut self.batch);
        let parsed = rows
            .iter()
            .map(|l| self.parse_line(l))
            .collect::<Result<Vec<_>>>()?;
        Ok(parsed)
    }

    fn parse_line(&self, line: &[u8]) -> Result<Vec<Value>> {
        if line.is_empty() {
            bail!("Leere Zeile im CSV-Stream");
        }
        let mut fields: Vec<&[u8]> = Vec::with_capacity(self.columns.len());
        let mut cur_start = 0usize;
        for (i, &b) in line.iter().enumerate() {
            if b == b'\t' {
                fields.push(&line[cur_start..i]);
                cur_start = i + 1;
            }
        }
        fields.push(&line[cur_start..]);
        if fields.len() != self.columns.len() {
            bail!(
                "Spaltenanzahl passt nicht: {} geliefert, {} erwartet",
                fields.len(),
                self.columns.len()
            );
        }
        let mut out = Vec::with_capacity(fields.len());
        for (i, f) in fields.iter().enumerate() {
            match tsv_unescape_field(f)? {
                None => out.push(Value::NULL),
                Some(bytes) => {
                    if self.hex_indexes.contains(&i) {
                        let decoded =
                            hex::decode(&bytes).context("Hex-Spalte enthält ungültiges Hex")?;
                        out.push(Value::Bytes(decoded));
                    } else {
                        out.push(Value::Bytes(bytes));
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Batches als REPLACE INTO importieren (idempotent — Sync darf erneut laufen).
///
/// Mehrere Zeilen pro Statement: ein Round-Trip und eine Transaktion pro Batch
/// statt einem Round-Trip/Commit pro Zeile (vorher ~10 ms/Zeile).
pub async fn import_rows(
    conn: &mut Conn,
    db: &str,
    table: &str,
    columns: &[String],
    rows: Vec<Vec<Value>>,
) -> Result<u64> {
    if rows.is_empty() {
        return Ok(0);
    }
    const ROWS_PER_STMT: usize = 500;
    let cols_sql: Vec<String> = columns.iter().map(|c| mysqlutil::quote_ident(c)).collect();
    let tuple = format!("({})", vec!["?"; columns.len()].join(","));
    let mut total = 0u64;
    for chunk in rows.chunks(ROWS_PER_STMT) {
        let values = vec![tuple.as_str(); chunk.len()].join(",");
        let sql = format!(
            "REPLACE INTO {} ({}) VALUES {}",
            qualified(db, table),
            cols_sql.join(", "),
            values
        );
        let params: Vec<Value> = chunk.iter().flatten().cloned().collect();
        conn.exec_drop(&sql, params)
            .await
            .with_context(|| format!("REPLACE fehlgeschlagen ({} Zeilen)", chunk.len()))?;
        total += chunk.len() as u64;
    }
    Ok(total)
}

/// PKs löschen (Komponenten je PK, numerische Spalten als Literale).
pub async fn delete_pks<S: AsRef<str> + std::fmt::Debug>(
    conn: &mut Conn,
    db: &str,
    table: &str,
    pks: &[Vec<S>],
) -> Result<u64> {
    use crate::mysqlutil::PkCol;
    if pks.is_empty() {
        return Ok(0);
    }
    // PK-Typen aus information_schema holen
    let meta: Vec<PkCol> = mysqlutil::pk_columns(conn, db, table).await?;

    let mut deleted = 0u64;
    for chunk in pks.chunks(500) {
        let where_clause = crate::mysqlutil::pk_in_where(&meta, chunk)?;
        let sql = format!(
            "DELETE FROM {} WHERE {}",
            qualified(db, table),
            where_clause
        );
        conn.query_drop(sql)
            .await
            .context("DELETE fehlgeschlagen")?;
        deleted += chunk.len() as u64;
    }
    Ok(deleted)
}

/// SQL-Dump ausführen: Statement-Splitting + Allowlist-Filter.
/// Erlaubt: INSERT/REPLACE/DELETE/UPDATE/SET/TRUNCATE (keine DDL aus Dump).
pub async fn execute_dump(conn: &mut Conn, sql: &str) -> Result<u64> {
    let statements = split_sql_statements(sql);
    // Eine Transaktion für den ganzen Dump: sonst committet jedes Statement
    // einzeln (~10 ms/Zeile bei fsync=1).
    conn.query_drop("START TRANSACTION")
        .await
        .context("START TRANSACTION fehlgeschlagen")?;
    let mut executed = 0u64;
    for st in statements {
        let trimmed = st.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !is_allowed_statement(trimmed) {
            log::debug!(
                "Dump-Statement gefiltert: {}",
                &trimmed[..trimmed.len().min(60)]
            );
            continue;
        }
        if let Err(e) = conn.query_drop(trimmed).await {
            let _ = conn.query_drop("ROLLBACK").await;
            return Err(e).with_context(|| {
                format!(
                    "Dump-Statement fehlgeschlagen: {}",
                    &trimmed[..trimmed.len().min(120)]
                )
            });
        }
        executed += 1;
    }
    conn.query_drop("COMMIT")
        .await
        .context("COMMIT fehlgeschlagen")?;
    Ok(executed)
}

fn is_allowed_statement(s: &str) -> bool {
    let lower = s.trim_start().to_lowercase();
    const ALLOW: &[&str] = &["insert", "replace", "delete", "update", "set", "truncate"];
    ALLOW.iter().any(|a| lower.starts_with(a))
}

/// SQL in Statements splitten (Quotes, Backtick-Identifiers, Kommentare respektieren).
pub fn split_sql_statements(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let bytes = sql.as_bytes();
    let mut cur: Vec<u8> = Vec::with_capacity(sql.len());
    let mut i = 0;
    let mut in_squote = false;
    let mut in_dquote = false;
    let mut in_backtick = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        // Kommentare am Zeilenanfang überspringen
        if !in_squote && !in_dquote && !in_backtick {
            if c == '-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
                // Zeilenkommentar inkl. Newline überspringen
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                if i < bytes.len() {
                    i += 1; // '\n' mitkonsumieren
                }
                continue;
            }
            if c == '/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
                // mysqldump-Conditional-Comments /*!40... */ → Inhalt ausführen,
                // normale Kommentare überspringen
                let is_exec = i + 2 < bytes.len() && bytes[i + 2] == b'!';
                let mut j = i + 2;
                while j + 1 < bytes.len() && !(bytes[j] == b'*' && bytes[j + 1] == b'/') {
                    j += 1;
                }
                if is_exec && j + 1 < bytes.len() {
                    let inner = &sql[i + 3..j]; // nach "/*!"
                    cur.extend_from_slice(inner.as_bytes());
                    cur.push(b' ');
                }
                i = (j + 2).min(bytes.len());
                continue;
            }
            match c {
                '\'' if !in_dquote => in_squote = !in_squote,
                '"' if !in_squote => in_dquote = !in_dquote,
                '`' if !in_squote && !in_dquote => in_backtick = !in_backtick,
                ';' if !in_squote && !in_dquote && !in_backtick => {
                    out.push(String::from_utf8_lossy(&cur).trim().to_string());
                    cur.clear();
                    i += 1;
                    continue;
                }
                _ => {}
            }
        } else if c == '\\' && i + 1 < bytes.len() && (in_squote || in_dquote) {
            cur.push(bytes[i]);
            i += 1;
            if i < bytes.len() {
                cur.push(bytes[i]);
                i += 1;
            }
            continue;
        } else if c == '\'' && in_squote {
            in_squote = false;
        } else if c == '"' && in_dquote {
            in_dquote = false;
        } else if c == '`' && in_backtick {
            in_backtick = false;
        }
        cur.push(bytes[i]);
        i += 1;
    }
    let text = String::from_utf8_lossy(&cur);
    if !text.trim().is_empty() {
        out.push(text.trim().to_string());
    }
    out
}

// ---------------------------------------------------------------------------
// Trigger-Handling: Drop + Recreate (Option "disable_triggers")
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TriggerDef {
    pub name: String,
    pub create_sql: String,
}

pub async fn snapshot_and_drop_triggers(
    conn: &mut Conn,
    db: &str,
    table: &str,
) -> Result<Vec<TriggerDef>> {
    let q = format!(
        "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS \
         WHERE TRIGGER_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?"
    );
    let names: Vec<String> = conn.exec(q, (db, table)).await?;
    let mut defs = Vec::new();
    for name in names {
        let q2 = format!(
            "SHOW CREATE TRIGGER {}.{}",
            mysqlutil::quote_ident(db),
            mysqlutil::quote_ident(&name)
        );
        let row: Option<(
            String,
            mysql_async::Value,
            mysql_async::Value,
            String,
            String,
            mysql_async::Value,
            mysql_async::Value,
        )> = conn.query_first(q2).await?;
        if let Some(r) = row {
            let sql = r.3;
            conn.query_drop(format!(
                "DROP TRIGGER IF EXISTS {}.{}",
                mysqlutil::quote_ident(db),
                mysqlutil::quote_ident(&name)
            ))
            .await?;
            defs.push(TriggerDef {
                name,
                create_sql: sql,
            });
        }
    }
    if !defs.is_empty() {
        log::info!("[{db}.{table}] {} Trigger deaktiviert", defs.len());
    }
    Ok(defs)
}

pub async fn restore_triggers(conn: &mut Conn, db: &str, defs: &[TriggerDef]) -> Result<()> {
    for t in defs {
        if let Err(e) = conn.query_drop(&t.create_sql).await {
            log::warn!(
                "[{db}] Trigger {} konnte nicht wiederhergestellt werden: {e}",
                t.name
            );
        } else {
            log::info!("[{db}] Trigger {} wiederhergestellt", t.name);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tsv_roundtrip() {
        let cols = vec!["a".to_string(), "b".to_string()];
        let mut b = TsvBatcher::new(cols, &[]);
        let line = b"hello\tworld\n";
        let _ = b.push(line).unwrap();
        let rows = b.finish().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            vec![
                Value::Bytes(b"hello".to_vec()),
                Value::Bytes(b"world".to_vec())
            ]
        );
    }

    #[test]
    fn tsv_null_and_escapes() {
        let mut b = TsvBatcher::new(vec!["a".into(), "b".into(), "c".into()], &[]);
        let rows = b.push(b"\\N\t1\\t2\ta\\\\b\n").unwrap();
        // \t im Feld bleibt escaped bis unescape — Feld "1\t2" muss 2 chars escaped haben
        assert_eq!(rows.len(), 0); // unter Batch-Größe
        let rows = b.finish().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::NULL);
        assert_eq!(rows[0][1], Value::Bytes(b"1\t2".to_vec()));
        assert_eq!(rows[0][2], Value::Bytes(b"a\\b".to_vec()));
    }

    #[test]
    fn hex_column() {
        let mut b = TsvBatcher::new(vec!["bin".into()], &["bin".to_string()]);
        let _ = b.push(b"deadbeef\n").unwrap();
        let rows = b.finish().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Bytes(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn split_statements() {
        let sql =
            "INSERT INTO `t` VALUES (1,'a;b'); -- comment\nREPLACE INTO `t` VALUES (2,'x\\'y');";
        let st = split_sql_statements(sql);
        assert_eq!(st.len(), 2);
        assert!(st[0].starts_with("INSERT"));
        assert!(st[1].starts_with("REPLACE"));
        assert!(st[1].contains("x\\'y")); // Escape bleibt erhalten (korrekt für SQL-Ausführung)
    }

    #[test]
    fn split_statements_preserves_utf8() {
        let st = split_sql_statements("REPLACE INTO `t` VALUES ('ä');");
        assert_eq!(st.len(), 1);
        assert!(st[0].contains("'ä'"), "UTF-8 wurde verfälscht: {:?}", st[0]);
    }

    #[test]
    fn conditional_comment() {
        let sql = "/*!40000 ALTER TABLE `t` DISABLE KEYS */;\nINSERT INTO t VALUES (1);";
        let st = split_sql_statements(sql);
        let filtered: Vec<_> = st.iter().filter(|s| is_allowed_statement(s)).collect();
        assert_eq!(filtered.len(), 1);
        assert!(filtered[0].starts_with("INSERT"));
    }
}
