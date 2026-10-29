//! Konfigurations-Loading (JSON) mit tolerantem Key-Matching:
//! z. B. "transfer-batch-rows" und "transfer_batch_rows" werden akzeptiert;
//! der alte Key "dump_threshold" bleibt als Alias gueltig.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

pub const DEFAULT_QUIC_PORT: u16 = 4433;
pub const PROTOCOL_VERSION: u32 = 3;
pub const ALPN: &[u8] = b"mysql-sync/3";

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MysqlCfg {
    #[serde(default)]
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "d_localhost")]
    pub host: String,
    #[serde(default = "d_mysql_port")]
    pub port: u16,
    /// Optional: Unix-Socket-Pfad (statt host/port)
    #[serde(default)]
    pub socket: Option<String>,
}

fn d_localhost() -> String {
    "127.0.0.1".into()
}
fn d_mysql_port() -> u16 {
    3306
}
fn d_chunked() -> u32 {
    3
}
fn d_transfer_batch_rows() -> usize {
    1000
}
fn d_concurrent() -> usize {
    3
}
fn d_connect_timeout() -> u64 {
    30
}
fn d_max_frame() -> usize {
    8 * 1024 * 1024
}
fn d_max_decompressed() -> usize {
    64 * 1024 * 1024
}
fn d_transport() -> String {
    "quic".to_string()
}
fn d_compression() -> String {
    "none".to_string()
}
fn d_compression_level() -> i32 {
    3
}
fn d_true() -> bool {
    true
}

/// diff-threshold-percent darf als Zahl (10) oder String ("10") kommen.
fn de_percent<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum V {
        N(f64),
        S(String),
    }
    match V::deserialize(deserializer)? {
        V::N(n) => Ok(n),
        V::S(s) => s.trim().trim_end_matches('%').parse().map_err(serde::de::Error::custom),
    }
}

fn d_hash_algo() -> String {
    "xxh3".to_string()
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// CSV-Übertragung erlaubt (sonst SQL-Dump-Modus)
    #[serde(default = "d_true", alias = "allow_csv")]
    pub allow_csv: bool,

    /// Export-Strategien in Try-Order:
    /// "select-stream" | "to_file" / "to_file:DIR" | "mysql-e" | "mysqldump"
    #[serde(default, alias = "dump_strategies")]
    pub dump_strategies: Vec<String>,

    /// Hash-Algorithmus für Chunk/Row-Hashes: xxh3, xxh64, blake3, md5, sha256 (default xxh3)
    #[serde(default = "d_hash_algo", alias = "hash_algorithm", alias = "hash-algo", alias = "hash")]
    pub hash_algo: String,

    /// MySQL-Verbindung (Quelle beim Client, Ziel beim Server)
    #[serde(default)]
    pub mysql: MysqlCfg,

    /// Client: Ziel-Server "host:port"
    #[serde(default)]
    pub server: Option<String>,

    /// Server: Bind-Adresse, z.B. "0.0.0.0"
    #[serde(default)]
    pub listen: Option<String>,

    /// QUIC-Port (Server-Listen-Port bzw. Client-Fallback wenn server ohne Port)
    #[serde(default, alias = "quic_port", alias = "port")]
    pub quic_port: Option<u16>,

    /// Zeilen pro Batch beim SQL-REPLACE-Transfer (kein Diff-Kurzschluss).
    /// Alter Name `dump_threshold` wird als Alias akzeptiert.
    #[serde(default = "d_transfer_batch_rows", alias = "dump_batch_rows", alias = "dump_threshold")]
    pub transfer_batch_rows: usize,

    /// Chunking-Groesse (1–6). Der Modus wird automatisch aus dem PK-Typ
    /// gewaehlt: Range fuer einen unterstuetzten Einzel-PK, sonst Modulo/Suffix.
    /// - range: 10^N Werte pro numerischem Chunk bzw. N Zeichen/Bytes abschneiden
    /// - mod:   letzte N Zeichen des Primary Keys bilden einen Bucket (10^N Buckets)
    #[serde(default = "d_chunked")]
    pub chunked: u32,

    /// Parallel laufende Tabellen-Tasks (je ein QUIC-Stream)
    #[serde(default = "d_concurrent", alias = "concurrent_processes")]
    pub concurrent_processes: usize,

    /// Ab diesem Prozentanteil differierender Buckets: Vollkopie statt Drill-Down
    #[serde(default = "d_percent10", alias = "diff_threshold_percent", deserialize_with = "de_percent")]
    pub diff_threshold_percent: f64,

    /// Zielseitige Umbenennung von Datenbanken: "db" -> "target_db"
    #[serde(default, alias = "database_mappings")]
    pub database_mappings: HashMap<String, String>,

    /// Zielseitige Umbenennung von Tabellen: "db.table" -> "target_db.target_table"
    #[serde(default, alias = "database_table_mappings")]
    pub database_table_mappings: HashMap<String, String>,

    /// Vor Import Triggers auf Zieltabelle droppen und danach wieder anlegen
    #[serde(default, alias = "disable_triggers")]
    pub disable_triggers: bool,

    /// Client: erwarteter Server-Zertifikat-Fingerprint "sha256:<hex>" (Pinning)
    #[serde(default, alias = "server_fingerprint")]
    pub server_fingerprint: Option<String>,

    /// Optionaler gemeinsamer Auth-Token (Hello-Handshake)
    #[serde(default, alias = "auth_token")]
    pub auth_token: Option<String>,

    /// Optionale eigene TLS-Dateien (Server). Ohne Angabe: Self-signed neben der Config.
    #[serde(default)]
    pub tls_cert: Option<String>,
    #[serde(default)]
    pub tls_key: Option<String>,

    /// QUIC-Connect-Timeout in Sekunden
    #[serde(default = "d_connect_timeout", alias = "connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// Max. Framgröße in Bytes (Sicherheit)
    #[serde(default = "d_max_frame")]
    pub max_frame: usize,

    /// Max. Größe nach der Dekompression (Zip-Bomb-Schutz)
    #[serde(default = "d_max_decompressed")]
    pub max_decompressed: usize,

    /// Transport: "quic" (TLS 1.3, Default) oder "tcp" (Klartext, nur mit allow_plaintext)
    #[serde(default = "d_transport")]
    pub transport: String,

    /// Klartext-TCP explizit erlauben (Opt-in, unsicher)
    #[serde(default, alias = "allow_plaintext")]
    pub allow_plaintext: bool,

    /// Kompression: "none" (Default) oder "zstd" (immer, ohne Schwellwert)
    #[serde(default = "d_compression")]
    pub compression: String,

    /// zstd-Level (1–22); nur bei compression="zstd"
    #[serde(default = "d_compression_level", alias = "compression_level")]
    pub compression_level: i32,
}

fn d_percent10() -> f64 {
    10.0
}

impl Config {
    /// Strategie-Liste mit Default.
    pub fn strategies(&self) -> Vec<String> {
        if self.dump_strategies.is_empty() {
            vec!["select-stream".to_string()]
        } else {
            self.dump_strategies.clone()
        }
    }

    pub async fn server_addr(&self) -> Result<std::net::SocketAddr> {
        let raw = self
            .server
            .clone()
            .context("Client-Config braucht \"server\": \"host:port\"")?;
        let addr = if let Some((h, p)) = raw.rsplit_once(':') {
            format!("{h}:{}", p.parse::<u16>().context("Port im server-Feld ungültig")?)
        } else {
            format!("{raw}:{}", self.quic_port.unwrap_or(DEFAULT_QUIC_PORT))
        };
        let resolved = tokio::net::lookup_host(addr.as_str())
            .await
            .with_context(|| format!("DNS-Auflösung fehlgeschlagen: {addr}"))?
            .next()
            .with_context(|| format!("Keine Adresse für {addr}"))?;
        Ok(resolved)
    }

    pub fn listen_addr(&self) -> Result<std::net::SocketAddr> {
        let host = self.listen.clone().unwrap_or_else(|| "0.0.0.0".into());
        Ok(format!("{}:{}", host, self.quic_port.unwrap_or(DEFAULT_QUIC_PORT))
            .parse()?)
    }
}

/// Keys mit Bindestrich auf Unterstrich mappen, damit beide Schreibweisen funktionieren.
fn normalize_keys(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let nk = k.replace('-', "_");
                if let Some(mut val) = map.remove(&k) {
                    normalize_keys(&mut val);
                    map.insert(nk, val);
                }
            }
        }
        serde_json::Value::Array(a) => {
            for item in a.iter_mut() {
                normalize_keys(item);
            }
        }
        _ => {}
    }
}

pub fn load(path: &Path) -> Result<Config> {
    let (raw, source) = if path == Path::new("-") {
        let mut s = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)
            .context("Config von stdin nicht lesbar")?;
        (s, "stdin".to_string())
    } else {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("Konfigdatei nicht lesbar: {}", path.display()))?;
        (s, path.display().to_string())
    };
    parse(&raw, &source)
}

fn parse(raw: &str, source: &str) -> Result<Config> {
    let mut v: serde_json::Value =
        serde_json::from_str(raw).with_context(|| format!("JSON ungültig: {source}"))?;
    normalize_keys(&mut v);
    let cfg: Config = serde_json::from_value(v).context("Config-Schema ungültig")?;
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.chunked == 0 || self.chunked > 6 {
            anyhow::bail!("\"chunked\" muss zwischen 1 und 6 liegen");
        }
        if self.concurrent_processes == 0 {
            anyhow::bail!("\"concurrent_processes\" muss >= 1 sein");
        }
        if self.max_frame < 64 * 1024 {
            anyhow::bail!("\"max_frame\" zu klein (min. 65536)");
        }
        if self.max_decompressed < self.max_frame {
            anyhow::bail!("\"max_decompressed\" muss >= max_frame sein");
        }
        if !matches!(self.transport.as_str(), "quic" | "tcp") {
            anyhow::bail!("\"transport\" muss \"quic\" oder \"tcp\" sein");
        }
        if self.transport == "tcp" && !self.allow_plaintext {
            anyhow::bail!(
                "transport \"tcp\" ist unverschlüsselt; setze explizit \"allow_plaintext\": true"
            );
        }
        if !matches!(self.compression.as_str(), "none" | "zstd") {
            anyhow::bail!("\"compression\" muss \"none\" oder \"zstd\" sein");
        }
        if self.compression == "zstd" && !(1..=22).contains(&self.compression_level) {
            anyhow::bail!("\"compression_level\" muss zwischen 1 und 22 liegen");
        }
        let algo = self
            .hash_algo
            .parse::<crate::hash::HashAlgo>()
            .map_err(|e| anyhow::anyhow!(e))?;
        if !algo.is_enabled() {
            anyhow::bail!(
                "hash_algo \"{algo}\" ist in diesem Build nicht einkompiliert \
                 (Cargo-Features: xxhash, blake3, md5, sha2)"
            );
        }
        Ok(())
    }

    pub fn hash_algo(&self) -> crate::hash::HashAlgo {
        self.hash_algo.parse().unwrap_or_default()
    }

    pub fn plaintext_tcp(&self) -> bool {
        self.transport == "tcp"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_dash_and_underscore_keys() {
        for algo in ["xxh3", "blake3", "sha256"] {
            let raw = format!(
                r#"{{"mysql":{{"user":"u"}},"hash-algo":"{algo}","diff-threshold-percent":42}}"#
            );
            let cfg = parse(&raw, "test").unwrap();
            assert_eq!(cfg.hash_algo.to_string(), algo);
            assert_eq!(cfg.diff_threshold_percent, 42.0);
        }
    }

    #[test]
    fn parse_rejects_unknown_hash_algo() {
        let raw = r#"{"mysql":{"user":"u"},"hash_algo":"nope"}"#;
        assert!(parse(raw, "test").is_err());
    }
}
