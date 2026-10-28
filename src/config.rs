//! Konfigurations-Loading (JSON) mit tolerantem Key-Matching:
//! sowohl "dump-threshold" als auch "dump_threshold" werden akzeptiert.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

pub const DEFAULT_QUIC_PORT: u16 = 4433;
pub const PROTOCOL_VERSION: u32 = 1;
pub const ALPN: &[u8] = b"mysql-sync/1";

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
fn d_dump_threshold() -> u64 {
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

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    /// CSV-Übertragung erlaubt (sonst SQL-Dump-Modus)
    #[serde(default = "d_true", alias = "allow_csv")]
    pub allow_csv: bool,

    /// Export-Strategien in Try-Order:
    /// "select-stream" | "to_file" / "to_file:DIR" | "mysql-e" | "mysqldump"
    #[serde(default, alias = "dump_strategies")]
    pub dump_strategies: Vec<String>,

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

    /// Ab dieser Anzahl Zeilen pro Bucket wird der ganze Bucket kopiert
    /// statt zeilenweise zu diffen ("immer 1000 auf einmal")
    #[serde(default = "d_dump_threshold", alias = "dump_threshold")]
    pub dump_threshold: u64,

    /// Chunking: letzte N Zeichen des Primary Keys bilden einen Bucket (10^N Buckets)
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
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Konfigdatei nicht lesbar: {}", path.display()))?;
    let mut v: serde_json::Value =
        serde_json::from_str(&raw).with_context(|| format!("JSON ungültig: {}", path.display()))?;
    normalize_keys(&mut v);
    let cfg: Config = serde_json::from_value(v).context("Config-Schema ungültig")?;
    cfg.validate()?;
    Ok(cfg)
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.chunked == 0 || self.chunked > 6 {
            anyhow::bail!("\"chunked\" muss zwischen 1 und 6 liegen (10^N Buckets)");
        }
        if self.concurrent_processes == 0 {
            anyhow::bail!("\"concurrent_processes\" muss >= 1 sein");
        }
        if self.max_frame < 64 * 1024 {
            anyhow::bail!("\"max_frame\" zu klein (min. 65536)");
        }
        Ok(())
    }
}
