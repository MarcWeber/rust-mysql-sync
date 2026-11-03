//! Selector A:B: A = DB-Liste (oder *), B = Tabellen-Liste (oder *).
//! Beispiel: "db1,db2:*" oder "*" oder "db1:t1,t2"
//! Glob-Patterns: "db*:t*" passt db1.table1, db2.test_foo etc.
//! Patterns starting with / are treated as regex: "/^db1.*/,/.*db2$:/.*foo$/,/^bar.*/"

use anyhow::{Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use regex::Regex;
use std::collections::HashMap;
use std::str::FromStr;

/// A compiled pattern: exact name or regex (globs are converted to regex at parse time).
#[derive(Debug, Clone)]
pub enum Pattern {
    Name(String),
    Regex(Regex),
}

impl FromStr for Pattern {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        if let Some(inner) = s.strip_prefix('/').and_then(|s| s.strip_suffix('/')) {
            let re = Regex::new(&format!("^{}$", inner)).context("Invalid regex pattern")?;
            Ok(Pattern::Regex(re))
        } else if s.contains('*') || s.contains('?') {
            let re_str = format!("^{}$", s.replace('*', ".*").replace('?', "."));
            let re = Regex::new(&re_str).context("Invalid glob pattern")?;
            Ok(Pattern::Regex(re))
        } else {
            Ok(Pattern::Name(s.to_string()))
        }
    }
}

impl Pattern {
    fn matches(&self, text: &str) -> bool {
        match self {
            Pattern::Name(name) => name == text,
            Pattern::Regex(re) => re.is_match(text),
        }
    }

    fn is_wildcard(&self) -> bool {
        !matches!(self, Pattern::Name(_))
    }
}

#[derive(Debug, Clone)]
pub struct Selector {
    pub dbs: Vec<Pattern>,
    pub tables: Vec<Pattern>,
}

impl Selector {
    /// None -> "*:*"
    pub fn parse(sel: Option<&str>) -> Result<Selector> {
        let sel = match sel {
            None => {
                return Ok(Selector {
                    dbs: vec!["*".parse().unwrap()],
                    tables: vec!["*".parse().unwrap()],
                })
            }
            Some(s) => s.trim().to_string(),
        };
        if sel.is_empty() {
            return Ok(Selector {
                dbs: vec!["*".parse().unwrap()],
                tables: vec!["*".parse().unwrap()],
            });
        }
        let (a, b) = split_db_table(&sel);
        let dbs = split_list(a).context("DB-Liste im Selector ungültig")?;
        let tables = split_list(b).context("Tabellen-Liste im Selector ungültig")?;
        Ok(Selector { dbs, tables })
    }

    pub fn has_db_glob(&self) -> bool {
        self.dbs.iter().any(Pattern::is_wildcard)
    }

    pub fn has_table_glob(&self) -> bool {
        self.tables.iter().any(Pattern::is_wildcard)
    }

    pub fn matches_db(&self, db: &str) -> bool {
        self.dbs.iter().any(|p| p.matches(db))
    }

    pub fn matches_table(&self, table: &str) -> bool {
        self.tables.iter().any(|p| p.matches(table))
    }

    /// Expand glob patterns into a list of (db, table) pairs.
    /// Uses `cache` to avoid re-querying the same database multiple times.
    /// Short-circuits for exact (non-glob) names.
    pub async fn expand(
        &self,
        conn: &mut Conn,
        cache: &mut HashMap<String, Vec<String>>,
    ) -> Result<Vec<(String, String)>> {
        // Short-circuit: no globs at all, just return exact name pairs
        if !self.has_db_glob() && !self.has_table_glob() {
            let mut pairs = Vec::new();
            for db_pat in &self.dbs {
                if let Pattern::Name(db) = db_pat {
                    for tbl_pat in &self.tables {
                        if let Pattern::Name(table) = tbl_pat {
                            pairs.push((db.clone(), table.clone()));
                        }
                    }
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

fn split_list(s: &str) -> Result<Vec<Pattern>> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_regex = false;
    for ch in s.chars() {
        match ch {
            '/' if !in_regex => { in_regex = true; current.push(ch); }
            '/' if in_regex => { in_regex = false; current.push(ch); }
            ',' if !in_regex => {
                let p = current.trim().to_string();
                if !p.is_empty() {
                    out.push(parse_pattern(&p)?);
                }
                current.clear();
            }
            _ => { current.push(ch); }
        }
    }
    let p = current.trim().to_string();
    if !p.is_empty() {
        out.push(parse_pattern(&p)?);
    }
    if out.is_empty() {
        out.push("*".parse().unwrap());
    }
    Ok(out)
}

fn parse_pattern(p: &str) -> Result<Pattern> {
    let p = clean_pattern(p);
    p.parse()
}

fn clean_pattern(p: &str) -> String {
    let mut p = p.to_string();
    if p.ends_with(":*") { p.truncate(p.len() - 2); }
    while p.ends_with(':') { p.pop(); }
    if p.is_empty() { "*".into() } else { p }
}

/// Split selector into (db_part, table_part) on the last : that's outside /.../ patterns.
fn split_db_table(sel: &str) -> (&str, &str) {
    let mut in_regex = false;
    let mut last_colon = None;
    for (i, ch) in sel.char_indices() {
        match ch {
            '/' => in_regex = !in_regex,
            ':' if !in_regex => last_colon = Some(i),
            _ => {}
        }
    }
    match last_colon {
        Some(pos) => (&sel[..pos], &sel[pos + 1..]),
        None => (sel, "*"),
    }
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
        let s = Selector::parse(Some("db1,db2:*")).unwrap();
        assert!(s.matches_db("db1"));
        assert!(s.matches_db("db2"));
        assert!(s.matches_table("anything"));
    }

    #[test]
    fn parse_db_only() {
        let s = Selector::parse(Some("db1")).unwrap();
        assert!(s.matches_db("db1"));
        assert!(s.matches_table("anything"));
    }

    #[test]
    fn parse_tables() {
        let s = Selector::parse(Some("mydb:users,orders")).unwrap();
        assert!(s.matches_db("mydb"));
        assert!(s.matches_table("users"));
        assert!(s.matches_table("orders"));
        assert!(!s.matches_table("other"));
    }

    #[test]
    fn parse_wildcard() {
        let s = Selector::parse(None).unwrap();
        assert!(s.matches_db("irgendwas"));
        assert!(s.matches_table("egal"));
    }

    #[test]
    fn parse_qualified_wildcards() {
        let s = Selector::parse(Some("db1:*,db2:*")).unwrap();
        assert!(s.matches_db("db1"));
        assert!(s.matches_db("db2"));
    }

    #[test]
    fn regex_slash() {
        // /.../ patterns are treated as regex
        let s = Selector::parse(Some("db1*/:test.*/")).unwrap();
        assert!(s.matches_table("test"));
        assert!(s.matches_table("test_foo"));
    }

    #[test]
    fn glob_uses_regex_conversion() {
        let p: Pattern = "db*".parse().unwrap();
        assert!(p.matches("db1"));
        assert!(p.matches("db123"));
        assert!(p.matches("db"));
        assert!(!p.matches("abc"));
        let p: Pattern = "*foo*".parse().unwrap();
        assert!(p.matches("xfoo_y"));
        assert!(!p.matches("xbar_y"));
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
        let p: Pattern = "db*".parse().unwrap();
        assert!(p.matches("db1"));
        assert!(p.matches("db123"));
        assert!(p.matches("db"));
        assert!(!p.matches("abc"));
        let p: Pattern = "t*_foo".parse().unwrap();
        assert!(p.matches("test_foo"));
        assert!(p.matches("t_foo"));
        assert!(!p.matches("bar_foo"));
    }

    #[test]
    fn glob_match_suffix() {
        let p: Pattern = "*log".parse().unwrap();
        assert!(p.matches("error_log"));
        assert!(p.matches("log"));
        assert!(!p.matches("logs"));
    }

    #[test]
    fn glob_match_middle() {
        let p: Pattern = "db*_t*".parse().unwrap();
        assert!(p.matches("db1_test"));
        assert!(p.matches("db_test"));
        assert!(p.matches("dbx_t"));
        assert!(!p.matches("dbx_x"));
    }

    #[test]
    fn glob_match_question() {
        let p: Pattern = "db?".parse().unwrap();
        assert!(p.matches("db1"));
        assert!(p.matches("dbX"));
        assert!(!p.matches("db12"));
        assert!(!p.matches("db"));
    }

    #[test]
    fn glob_selector() {
        let s = Selector::parse(Some("db*:t*")).unwrap();
        assert!(s.matches_db("db1"));
        assert!(s.matches_db("db_production"));
        assert!(!s.matches_db("abc"));
        assert!(s.matches_table("test"));
        assert!(s.matches_table("t"));
        assert!(!s.matches_table("abc"));
    }

    #[test]
    fn glob_has_glob() {
        let s = Selector::parse(Some("db*:t*")).unwrap();
        assert!(s.has_db_glob());
        assert!(s.has_table_glob());
        let s2 = Selector::parse(Some("mydb:users")).unwrap();
        assert!(!s2.has_db_glob());
        assert!(!s2.has_table_glob());
    }

    #[test]
    fn trailing_commas() {
        let s = Selector::parse(Some("db1*,db2*:foo*,bar*,")).unwrap();
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
        let s = Selector::parse(Some(",db1*:t1*,")).unwrap();
        assert!(s.matches_db("db1"));
        assert!(s.matches_table("t1"));
    }
}
