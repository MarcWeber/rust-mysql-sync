//! Selector A.B: A = DB-Liste (oder *), B = Tabellen-Liste (oder *).
//! Beispiel: "db1,db2.*" oder "*" oder "db1.t1,db1.t2"

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct Selector {
    pub dbs: Vec<String>,
    pub tables: Vec<String>,
}

impl Selector {
    /// None -> "*.*"
    pub fn parse(sel: Option<&str>) -> Result<Selector> {
        let sel = match sel {
            None => return Ok(Selector { dbs: vec!["*".into()], tables: vec!["*".into()] }),
            Some(s) => s.trim(),
        };
        if sel.is_empty() {
            return Ok(Selector { dbs: vec!["*".into()], tables: vec!["*".into()] });
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

    pub fn matches_db(&self, db: &str) -> bool {
        self.dbs.iter().any(|d| d == "*" || d == db)
    }

    pub fn matches_table(&self, table: &str) -> bool {
        self.tables.iter().any(|t| t == "*" || t == table)
    }
}

fn split_list(s: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for part in s.split(',') {
        let mut p = part.trim().to_string();
        if p.is_empty() {
            anyhow::bail!("Leerer Eintrag in Selektor-Liste: '{s}'");
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
        assert_eq!(map_remote("a", "b", &db_map, &t_map), ("x".into(), "y".into()));
        assert_eq!(map_remote("db", "t", &db_map, &t_map), ("target_db".into(), "t".into()));
    }
}
