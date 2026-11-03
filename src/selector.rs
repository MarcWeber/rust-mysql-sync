//! Selector A.B: A = DB-Liste (oder *), B = Tabellen-Liste (oder *).
//! Beispiel: "db1,db2.*" oder "*" oder "db1.t1,db1.t2"
//! Glob-Patterns: "db*.t*" passt db1.table1, db2.test_foo etc.

use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Selector {
    pub dbs: Vec<String>,
    pub tables: Vec<String>,
}

/// Simple glob match: * matches any sequence of chars, ? matches one char.
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') && !pattern.contains('?') {
        return pattern == text;
    }
    // Convert pattern to a simple regex-like match using recursion
    let mut pi = 0; // pattern index
    let mut ti = 0; // text index
    let pat_bytes: &[u8] = pattern.as_bytes();
    let txt_bytes: &[u8] = text.as_bytes();
    glob_match_rec(pat_bytes, pi, txt_bytes, ti)
}

fn glob_match_rec(pat: &[u8], pi: usize, txt: &[u8], ti: usize) -> bool {
    if pi == pat.len() && ti == txt.len() {
        return true;
    }
    if pi == pat.len() {
        return false;
    }
    if pat[pi] == b'*' {
        // * matches zero or more chars
        // skip consecutive *
        let mut next_pi = pi + 1;
        while next_pi < pat.len() && pat[next_pi] == b'*' {
            next_pi += 1;
        }
        if next_pi == pat.len() {
            return true; // trailing * matches anything
        }
        // try matching rest of pattern against every position in remaining text
        for i in ti..=txt.len() {
            if glob_match_rec(pat, next_pi, txt, i) {
                return true;
            }
        }
        false
    } else if pat[pi] == b'?' {
        // ? matches exactly one char
        if ti < txt.len() {
            glob_match_rec(pat, pi + 1, txt, ti + 1)
        } else {
            false
        }
    } else {
        // literal char
        if ti < txt.len() && pat[pi] == txt[ti] {
            glob_match_rec(pat, pi + 1, txt, ti + 1)
        } else {
            false
        }
    }
}

impl Selector {
    /// None -> "*.*"
    pub fn parse(sel: Option<&str>) -> Result<Selector> {
        let sel = match sel {
            None => {
                return Ok(Selector {
                    dbs: vec!["*".into()],
                    tables: vec!["*".into()],
                })
            }
            Some(s) => s.trim(),
        };
        if sel.is_empty() {
            return Ok(Selector {
                dbs: vec!["*".into()],
                tables: vec!["*".into()],
            });
        }
        // Letzten Punkt als Trenner nehmen: "db1,db2.t1,t2" -> A="db1,db2" B="t1,t2"
        let (a, b) = match sel.rsplit_once('.') {
            Some((a, b)) => (a, b),
            None => (sel, "*"),
        };
        let dbs = split_list(a).context("DB-Liste im Selector ungültig")?;
        let tables = split_list(b).context("Tabellen-Liste im Selector ungültig")?;
        Ok(Selector { dbs, tables })
    }

    /// Returns true if any DB pattern contains a glob wildcard (* or ?).
    pub fn has_db_glob(&self) -> bool {
        self.dbs.iter().any(|d| d.contains('*') || d.contains('?'))
    }

    /// Returns true if any table pattern contains a glob wildcard (* or ?).
    pub fn has_table_glob(&self) -> bool {
        self.tables.iter().any(|t| t.contains('*') || t.contains('?'))
    }

    pub fn matches_db(&self, db: &str) -> bool {
        self.dbs.iter().any(|d| glob_match(d, db))
    }

    pub fn matches_table(&self, table: &str) -> bool {
        self.tables.iter().any(|t| glob_match(t, table))
    }

    /// Expand glob patterns into a list of (db, table) pairs.
    /// Uses `cache` to avoid re-querying the same database multiple times.
    /// Short-circuits for exact (non-glob) names.
    pub async fn expand(
        &self,
        conn: &mut Conn,
        cache: &mut HashMap<String, Vec<String>>,
    ) -> Result<Vec<(String, String)>> {
        // Short-circuit: no globs at all, just return exact pairs
        if !self.has_db_glob() && !self.has_table_glob() {
            let mut pairs = Vec::new();
            for db_pat in &self.dbs {
                for tbl_pat in &self.tables {
                    pairs.push((db_pat.clone(), tbl_pat.clone()));
                }
            }
            return Ok(pairs);
        }

        // Get all databases, filtering by glob patterns
        let all_dbs: Vec<String> = conn.query("SHOW DATABASES").await?;
        let matched_dbs: Vec<&String> = all_dbs
            .iter()
            .filter(|db| self.matches_db(db))
            .collect();

        // For each matched DB, get tables from cache or query
        let mut pairs = Vec::new();
        for db in matched_dbs {
            let tables = cache
                .entry(db.clone())
                .or_insert_with(|| {
                    // Lazy: will be filled on first access
                    Vec::new()
                });
            if tables.is_empty() {
                let q = format!(
                    "SHOW FULL TABLES IN `{}` WHERE Table_type = 'BASE TABLE'",
                    db.replace('`', "``")
                );
                let tbls: Vec<String> = conn.query(&q).await?;
                *tables = tbls;
            }
            for table in tables.iter() {
                if self.matches_table(table) {
                    pairs.push((db.clone(), table.clone()));
                }
            }
        }
        Ok(pairs)
    }
}

fn split_list(s: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let mut p = part.trim().to_string();
        if p.is_empty() {
            continue; // allow trailing/leading commas
        }
        // "db.*" / "db." → "db" (Wildcard-Tabelle impliziert)
        if p.ends_with(".*") {
            p.truncate(p.len() - 2);
        }
        while p.ends_with('.') {
            p.pop();
        }
        if p.is_empty() {
            anyhow::bail!("Leerer DB-Name in Selektor: '{s}'");
        }
        out.push(p);
    }
    if out.is_empty() {
        out.push("*".into()); // all commas or whitespace → match everything
    }
    Ok(out)
}

/// DB-/Tabellen-Mapping auf die Zielnamen anwenden.
pub fn map_remote(
    db: &str,
    table: &str,
    db_map: &std::collections::HashMap<String, String>,
    table_map: &std::collections::HashMap<String, String>,
) -> (String, String) {
    let key = format!("{db}.{table}");
    if let Some(mapped) = table_map.get(&key) {
        if let Some((d, t)) = mapped.split_once('.') {
            return (d.to_string(), t.to_string());
        }
    }
    let rd = db_map.get(db).cloned().unwrap_or_else(|| db.to_string());
    (rd, table.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_full() {
        let s = Selector::parse(Some("db1,db2.*")).unwrap();
        assert_eq!(s.dbs, vec!["db1", "db2"]);
        assert_eq!(s.tables, vec!["*"]);
    }

    #[test]
    fn parse_db_only() {
        let s = Selector::parse(Some("db1")).unwrap();
        assert_eq!(s.dbs, vec!["db1"]);
        assert_eq!(s.tables, vec!["*"]);
    }

    #[test]
    fn parse_tables() {
        let s = Selector::parse(Some("mydb.users,orders")).unwrap();
        assert_eq!(s.dbs, vec!["mydb"]);
        assert_eq!(s.tables, vec!["users", "orders"]);
    }

    #[test]
    fn parse_wildcard() {
        let s = Selector::parse(None).unwrap();
        assert!(s.matches_db("irgendwas"));
        assert!(s.matches_table("egal"));
    }

    #[test]
    fn parse_qualified_wildcards() {
        // "db1.*,db2.*" muss beide DBs liefern (Tabellen-Wildcard)
        let s = Selector::parse(Some("db1.*,db2.*")).unwrap();
        assert_eq!(s.dbs, vec!["db1", "db2"]);
        assert_eq!(s.tables, vec!["*"]);
        assert!(s.matches_db("db1"));
        assert!(s.matches_db("db2"));
    }

    #[test]
    fn mapping() {
        let mut db_map = std::collections::HashMap::new();
        db_map.insert("db".to_string(), "target_db".to_string());
        let mut t_map = std::collections::HashMap::new();
        t_map.insert("a.b".to_string(), "x.y".to_string());
        assert_eq!(
            map_remote("a", "b", &db_map, &t_map),
            ("x".into(), "y".into())
        );
        assert_eq!(
            map_remote("db", "t", &db_map, &t_map),
            ("target_db".into(), "t".into())
        );
    }

    #[test]
    fn glob_match_basic() {
        assert!(glob_match("db*", "db1"));
        assert!(glob_match("db*", "db123"));
        assert!(glob_match("db*", "db"));
        assert!(!glob_match("db*", "abc"));
        assert!(glob_match("t*_foo", "test_foo"));
        assert!(glob_match("t*_foo", "t_foo"));
        assert!(!glob_match("t*_foo", "bar_foo"));
    }

    #[test]
    fn glob_match_suffix() {
        assert!(glob_match("*log", "error_log"));
        assert!(glob_match("*log", "log"));
        assert!(!glob_match("*log", "logs"));
    }

    #[test]
    fn glob_match_middle() {
        assert!(glob_match("db*_t*", "db1_test"));
        assert!(glob_match("db*_t*", "db_test"));
        assert!(glob_match("db*_t*", "dbx_t")); // * matches zero chars
        assert!(!glob_match("db*_t*", "dbx_x"));
    }

    #[test]
    fn glob_match_question() {
        assert!(glob_match("db?", "db1"));
        assert!(glob_match("db?", "dbX"));
        assert!(!glob_match("db?", "db12"));
        assert!(!glob_match("db?", "db"));
    }

    #[test]
    fn glob_selector() {
        let s = Selector::parse(Some("db*.t*")).unwrap();
        assert!(s.matches_db("db1"));
        assert!(s.matches_db("db_production"));
        assert!(!s.matches_db("abc"));
        assert!(s.matches_table("test"));
        assert!(s.matches_table("t"));
        assert!(!s.matches_table("abc"));
    }

    #[test]
    fn glob_has_glob() {
        let s = Selector::parse(Some("db*.t*")).unwrap();
        assert!(s.has_db_glob());
        assert!(s.has_table_glob());
        let s2 = Selector::parse(Some("mydb.users")).unwrap();
        assert!(!s2.has_db_glob());
        assert!(!s2.has_table_glob());
    }

    #[test]
    fn trailing_commas() {
        let s = Selector::parse(Some("db1*,db2*.foo*,bar*,")).unwrap();
        assert_eq!(s.dbs, vec!["db1*", "db2*"]);
        assert_eq!(s.tables, vec!["foo*", "bar*"]);
        assert!(s.matches_db("db1"));
        assert!(s.matches_db("db123"));
        assert!(s.matches_db("db2x"));
        assert!(!s.matches_db("other"));
        assert!(s.matches_table("foo"));
        assert!(s.matches_table("foobar"));
        assert!(s.matches_table("bar_x"));
        assert!(!s.matches_table("baz"));
    }

    #[test]
    fn leading_commas() {
        let s = Selector::parse(Some(",db1*.t1*,")).unwrap();
        assert_eq!(s.dbs, vec!["db1*"]);
        assert_eq!(s.tables, vec!["t1*"]);
    }
}
