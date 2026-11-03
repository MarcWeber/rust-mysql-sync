//! Konfigurations-Loading (JSON) mit tolerantem Key-Matching:
//! z. B. "transfer-batch-rows" und "transfer_batch_rows" werden akzeptiert;
//! der alte Key "dump_threshold" bleibt als Alias gueltig.

use anyhow::{bail, Context, Result};
use indexmap::IndexMap;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub const DEFAULT_SERVER_PORT: u16 = 4433;
pub const PROTOCOL_VERSION: u32 = 7;
pub const ALPN: &[u8] = b"mysql-sync/7";

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
fn d_chunk_stream_buffer() -> usize {
    200
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
        V::S(s) => s
            .trim()
            .trim_end_matches('%')
            .parse()
            .map_err(serde::de::Error::custom),
    }
}

fn d_hash_algo() -> String {
    "xxh3".to_string()
}

/// Rundung für FLOAT/DOUBLE-Werte auf Textpfaden (`select`, `dump`, `raw-tsv`).
///
/// `Off` = Rohwert, `Auto` = deklarierter DECIMAL-Anteil (`FLOAT(M,D)` → D,
/// sonst aus), Zahl = feste Nachkommastellen. Gilt nie für DECIMAL/NUMERIC
/// (exakt) und ist auf Binärpfaden immer aus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FloatPrecision {
    Off,
    Auto,
    Fixed(u8),
}

impl Default for FloatPrecision {
    fn default() -> Self {
        Self::Auto
    }
}

impl std::fmt::Display for FloatPrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatPrecision::Off => write!(f, "off"),
            FloatPrecision::Auto => write!(f, "auto"),
            FloatPrecision::Fixed(n) => write!(f, "{n}"),
        }
    }
}

impl FromStr for FloatPrecision {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "false" => Ok(FloatPrecision::Off),
            "auto" | "true" => Ok(FloatPrecision::Auto),
            other => match other.parse::<u8>() {
                Ok(n) if n <= 30 => Ok(FloatPrecision::Fixed(n)),
                _ => bail!("float-precision muss \"off\", \"auto\" oder 0..=30 sein, nicht {s:?}"),
            },
        }
    }
}

impl<'de> Deserialize<'de> for FloatPrecision {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum V {
            N(u8),
            S(String),
        }
        match V::deserialize(deserializer)? {
            V::N(n) if n <= 30 => Ok(FloatPrecision::Fixed(n)),
            V::N(n) => Err(serde::de::Error::custom(format!(
                "float-precision {n} > 30 ist nicht erlaubt"
            ))),
            V::S(s) => FloatPrecision::from_str(&s).map_err(serde::de::Error::custom),
        }
    }
}

/// Per-Spalte-Overrides innerhalb eines Tabellen-Selektors.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldOverride {
    #[serde(default)]
    pub float_precision: Option<FloatPrecision>,
    /// `"trim"`, `"lower"` oder `"ignore"` (Groß-/Kleinschreibung egal).
    /// Jeder Transform macht die Byte-Pfade (`raw-binary`, `raw-tsv`) unzulässig.
    #[serde(default)]
    pub transform: Option<String>,
}

/// Ein Eintrag unter `tables`: `"db.table"` oder `"*.orders"` usw.
///
/// Der erste passende Eintrag in Config-Reihenfolge gewinnt; es wird nicht
/// zwischen mehreren Treffern gemischt.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableOverride {
    /// Erlaubte Hash-Wege in Try-Order, z.B. `["raw-binary-vec", "select"]`.
    #[serde(default)]
    pub strategies: Option<Vec<String>>,
    #[serde(default)]
    pub fields: IndexMap<String, FieldOverride>,
    /// SQL WHERE snippet injected into check queries (e.g. `"created_at > '2025-01-01'"`).
    #[serde(default)]
    pub check_where: Option<String>,
    /// SQL WHERE snippet injected into sync queries.
    #[serde(default)]
    pub sync_where: Option<String>,
}

impl TableOverride {
    pub fn field(&self, name: &str) -> Option<&FieldOverride> {
        self.fields.get(name).or_else(|| {
            self.fields
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| v)
        })
    }
}

/// Einfacher Glob-Vergleich, `*` = beliebige Zeichenfolge.
pub fn glob_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    let (mut pi, mut ti) = (0, 0);
    let mut star: Option<usize> = None;
    let mut star_ti = 0;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == b'?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == b'*' {
            star = Some(pi);
            pi += 1;
            star_ti = ti;
        } else if let Some(sp) = star {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == b'*' {
        pi += 1;
    }
    pi == p.len()
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
    #[serde(
        default = "d_hash_algo",
        alias = "hash_algorithm",
        alias = "hash-algo",
        alias = "hash"
    )]
    pub hash_algo: String,

    /// Hash-Weg in Try-Order fuer alle Tabellen ohne `tables`-Override:
    /// "raw-binary", "raw-binary-vec", "select", "dump", "raw-tsv".
    /// Leer = ["select"]. CLI-Flags (`--hash-*`) haben Vorrang.
    #[serde(default, alias = "hash_strategies_list")]
    pub hash_strategies: Vec<String>,

    /// Standard-Rundung fuer FLOAT/DOUBLE auf Textpfaden: "auto" (Default),
    /// "off" oder 0..=30. Per-Spalte-Overrides haben Vorrang.
    #[serde(default)]
    pub float_precision: FloatPrecision,

    /// Per-Selector-Overrides, z.B. `"*.orders": { "fields": { "price": ... } }`.
    /// Erster Treffer in Config-Reihenfolge gewinnt.
    #[serde(default)]
    pub tables: IndexMap<String, TableOverride>,

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

    /// Optionaler TCP-Port (Server). Wenn gesetzt, lauscht der Server zusätzlich
    /// auf diesem Port im Klartext-Modus (benötigt allow_plaintext: true).
    #[serde(default, alias = "tcp_port")]
    pub tcp_port: Option<u16>,

    /// Zeilen pro Batch beim SQL-REPLACE-Transfer (kein Diff-Kurzschluss).
    /// Alter Name `dump_threshold` wird als Alias akzeptiert.
    #[serde(
        default = "d_transfer_batch_rows",
        alias = "dump_batch_rows",
        alias = "dump_threshold"
    )]
    pub transfer_batch_rows: usize,

    /// Chunking-Groesse (1–6). Der Modus wird automatisch aus dem PK-Typ
    /// gewaehlt: Range fuer einen unterstuetzten Einzel-PK, sonst Modulo/Suffix.
    /// - range: 10^N Werte pro numerischem Chunk bzw. N Zeichen/Bytes abschneiden
    /// - mod:   letzte N Zeichen des Primary Keys bilden einen Bucket (10^N Buckets)
    #[serde(default = "d_chunked")]
    pub chunked: u32,

    /// Lookahead in Chunk-Hashes pro Seite (lokal/remote). Mehr Puffer
    /// ueberbrueckt hoehere Netzwerklatenz, kostet aber Speicher.
    #[serde(
        default = "d_chunk_stream_buffer",
        alias = "chunk_lookahead",
        alias = "chunk_buffer"
    )]
    pub chunk_stream_buffer: usize,

    /// Parallel laufende Tabellen-Tasks (je ein QUIC-Stream)
    #[serde(default = "d_concurrent", alias = "concurrent_processes")]
    pub concurrent_processes: usize,

    /// Ab diesem Prozentanteil differierender Buckets: Vollkopie statt Drill-Down
    #[serde(
        default = "d_percent10",
        alias = "diff_threshold_percent",
        deserialize_with = "de_percent"
    )]
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

    /// Server-Identität als PEM-Inhalt (aus `--generate-configs`).
    #[serde(default)]
    pub tls_cert_pem: Option<String>,
    #[serde(default)]
    pub tls_key_pem: Option<String>,

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

    /// Globale Hash-Weg-Liste (Default: `select`).
    pub fn hash_strategy_list(&self) -> Vec<String> {
        if self.hash_strategies.is_empty() {
            vec!["select".to_string()]
        } else {
            self.hash_strategies.clone()
        }
    }

    /// Erster `tables`-Eintrag, dessen Selektor auf `db.table` passt.
    pub fn table_override(&self, db: &str, table: &str) -> Option<(&str, &TableOverride)> {
        let key = format!("{db}.{table}");
        self.tables
            .iter()
            .find(|(pattern, _)| glob_match(pattern, &key))
            .map(|(pattern, ov)| (pattern.as_str(), ov))
    }

    pub async fn server_addr(&self) -> Result<std::net::SocketAddr> {
        let raw = self
            .server
            .clone()
            .context("Client-Config braucht \"server\": \"host:port\"")?;
        let addr = if let Some((h, p)) = raw.rsplit_once(':') {
            format!(
                "{h}:{}",
                p.parse::<u16>().context("Port im server-Feld ungültig")?
            )
        } else {
            format!("{raw}:{}", self.quic_port.unwrap_or(DEFAULT_SERVER_PORT))
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
        Ok(format!("{}:{}", host, self.quic_port.unwrap_or(DEFAULT_SERVER_PORT)).parse()?)
    }

    pub fn tcp_listen_addr(&self) -> Option<Result<std::net::SocketAddr>> {
        self.tcp_port.map(|port| {
            let host = self.listen.clone().unwrap_or_else(|| "0.0.0.0".into());
            Ok(format!("{}:{}", host, port).parse()?)
        })
    }
}

/// Keys mit Bindestrich auf Unterstrich mappen, damit beide Schreibweisen
/// funktionieren. Tabellen-Selektoren und Spaltennamen bleiben unangetastet
/// (dort sind Bindestriche Teil des Namens).
fn normalize_keys(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            let keys: Vec<String> = map.keys().cloned().collect();
            for k in keys {
                let nk = k.replace('-', "_");
                if let Some(mut val) = map.remove(&k) {
                    if nk == "tables" {
                        normalize_tables(&mut val);
                    } else {
                        normalize_keys(&mut val);
                    }
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

fn normalize_tables(v: &mut serde_json::Value) {
    let serde_json::Value::Object(tables) = v else {
        return;
    };
    for override_value in tables.values_mut() {
        let serde_json::Value::Object(ov) = override_value else {
            continue;
        };
        if let Some(fields) = ov.get_mut("fields") {
            if let serde_json::Value::Object(map) = fields {
                for field_value in map.values_mut() {
                    normalize_keys(field_value);
                }
            }
        }
        if let Some(strategies) = ov.get_mut("strategies") {
            normalize_keys(strategies);
        }
    }
}

pub fn load(path: &Path) -> Result<Config> {
    load_many(&[path.to_path_buf()])
}

/// Lädt mehrere JSON-Configs und merged sie rekursiv (spätere Dateien gewinnen).
pub fn load_many(paths: &[PathBuf]) -> Result<Config> {
    if paths.is_empty() {
        bail!("Mindestens eine Config-Datei mit -C/--config angeben");
    }
    let mut merged = serde_json::Value::Object(serde_json::Map::new());
    for path in paths {
        let (raw, source) = read_config(path)?;
        let mut v: serde_json::Value =
            serde_json::from_str(&raw).with_context(|| format!("JSON ungültig: {source}"))?;
        normalize_keys(&mut v);
        merge_value(&mut merged, v);
    }
    let cfg: Config = serde_json::from_value(merged).context("Config-Schema ungültig")?;
    cfg.validate()?;
    Ok(cfg)
}

fn read_config(path: &Path) -> Result<(String, String)> {
    if path == Path::new("-") {
        let mut s = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut s)
            .context("Config von stdin nicht lesbar")?;
        Ok((s, "stdin".to_string()))
    } else {
        let s = std::fs::read_to_string(path)
            .with_context(|| format!("Konfigdatei nicht lesbar: {}", path.display()))?;
        Ok((s, path.display().to_string()))
    }
}

fn merge_value(dst: &mut serde_json::Value, src: serde_json::Value) {
    match (dst, src) {
        (serde_json::Value::Object(dst_map), serde_json::Value::Object(src_map)) => {
            for (k, v) in src_map {
                match dst_map.get_mut(&k) {
                    Some(existing) => merge_value(existing, v),
                    None => {
                        dst_map.insert(k, v);
                    }
                }
            }
        }
        (dst, src) => *dst = src,
    }
}

/// Erzeugt server.json + client.json inkl. Self-signed-PEM und Fingerprint.
pub fn generate_configs(dir: &Path) -> Result<(PathBuf, PathBuf)> {
    let server_path = dir.join("server.json");
    let client_path = dir.join("client.json");
    for p in [&server_path, &client_path] {
        if p.exists() {
            bail!("{} existiert bereits", p.display());
        }
    }
    std::fs::create_dir_all(dir)
        .with_context(|| format!("Verzeichnis anlegen: {}", dir.display()))?;

    let (cert_pem, key_pem, fp) = crate::tls::generate_identity()?;
    let server = serde_json::json!({
        "mysql": {"host": "127.0.0.1", "port": 3306, "user": "root", "password": ""},
        "listen": "127.0.0.1",
        "quic_port": DEFAULT_SERVER_PORT,
        "tls_cert_pem": cert_pem,
        "tls_key_pem": key_pem,
        "server_fingerprint": fp,
    });
    let client = serde_json::json!({
        "mysql": {"host": "127.0.0.1", "port": 3306, "user": "root", "password": ""},
        "server": format!("127.0.0.1:{DEFAULT_SERVER_PORT}"),
        "quic_port": DEFAULT_SERVER_PORT,
        "server_fingerprint": fp,
    });
    write_private_json(&server_path, &server)?;
    write_private_json(&client_path, &client)?;
    Ok((server_path, client_path))
}

fn write_private_json(path: &Path, value: &serde_json::Value) -> Result<()> {
    let mut text = serde_json::to_string_pretty(value).context("JSON serialisieren")?;
    text.push('\n');
    std::fs::write(path, text).with_context(|| format!("Schreiben {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

pub(crate) fn parse(raw: &str, source: &str) -> Result<Config> {
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
        if self.chunk_stream_buffer == 0 || self.chunk_stream_buffer > 100_000 {
            anyhow::bail!("\"chunk_stream_buffer\" muss zwischen 1 und 100000 liegen");
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
        for s in &self.hash_strategies {
            s.parse::<crate::sync::HashVia>()
                .map_err(|e| anyhow::anyhow!("hash_strategies: {e}"))?;
        }
        for (pattern, ov) in &self.tables {
            if pattern.matches('.').count() != 1 {
                bail!(
                    "tables-Selektor {pattern:?} braucht genau einen Punkt \
                     (z.B. \"shop.orders\", \"*.orders\" oder \"shop.*\")"
                );
            }
            if let Some(list) = &ov.strategies {
                if list.is_empty() {
                    bail!("tables[{pattern:?}].strategies darf nicht leer sein");
                }
                for s in list {
                    s.parse::<crate::sync::HashVia>()
                        .map_err(|e| anyhow::anyhow!("tables[{pattern:?}].strategies: {e}"))?;
                }
            }
            for (name, f) in &ov.fields {
                if let Some(t) = &f.transform {
                    match t.trim().to_ascii_lowercase().as_str() {
                        "trim" | "lower" | "ignore" => {}
                        other => bail!(
                            "tables[{pattern:?}].fields[{name:?}].transform: \
                             unbekannt {other:?} (erlaubt: trim, lower, ignore)"
                        ),
                    }
                }
            }
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

    #[test]
    fn glob_first_match_and_ordered_tables() {
        let raw = r#"{
            "mysql": {"user": "u"},
            "float-precision": "auto",
            "hash-strategies": ["raw-binary", "select"],
            "tables": {
                "*.orders": {
                    "strategies": ["raw-binary-vec", "select"],
                    "fields": {
                        "price": {"float-precision": 2, "transform": "trim"},
                        "my-col": {"transform": "lower"}
                    }
                },
                "shop.*": {"fields": {"price": {"float-precision": 4}}}
            }
        }"#;
        let cfg = parse(raw, "test").unwrap();
        let (pattern, ov) = cfg.table_override("shop", "orders").unwrap();
        assert_eq!(pattern, "*.orders");
        assert_eq!(
            ov.field("price").unwrap().float_precision,
            Some(FloatPrecision::Fixed(2))
        );
        assert!(ov.field("my-col").is_some());
        assert!(ov.field("MY-COL").is_some());
        let (pattern, ov) = cfg.table_override("shop", "customers").unwrap();
        assert_eq!(pattern, "shop.*");
        assert_eq!(
            ov.field("price").unwrap().float_precision,
            Some(FloatPrecision::Fixed(4))
        );
        assert!(cfg.table_override("other", "customers").is_none());
    }

    #[test]
    fn glob_match_wildcards() {
        assert!(glob_match("*.orders", "shop.orders"));
        assert!(!glob_match("*.orders", "shop.order_items"));
        assert!(glob_match("shop.*", "shop.orders"));
        assert!(glob_match("*.*", "a.b"));
        assert!(glob_match("a.?", "a.b"));
        assert!(glob_match("a.*", "a."));
        assert!(!glob_match("a.b", "a.bc"));
    }

    #[test]
    fn parse_rejects_bad_table_override() {
        assert!(parse(
            r#"{"mysql":{"user":"u"},"tables":{"nodot":{"fields":{}}}}"#,
            "test"
        )
        .is_err());
        assert!(parse(
            r#"{"mysql":{"user":"u"},"tables":{"a.b":{"fields":{"x":{"transform":"nope"}}}}}"#,
            "test"
        )
        .is_err());
        assert!(parse(
            r#"{"mysql":{"user":"u"},"hash-strategies":["bogus"]}"#,
            "test"
        )
        .is_err());
    }
}
