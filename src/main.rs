//! mysql-sync — differentieller MySQL-Tabellen-Sync über QUIC.
//!
//! Haupt-Einstiegspunkt: CLI parsen, Config laden, Server- oder Client-Modus.

mod benchmark;
mod client;
mod config;
mod export;
mod hash;
mod hash_binary;
mod hash_tsv;
mod import;
mod mysqlutil;
mod progress;
mod proto;
mod selector;
mod server;
mod sqlhash;
mod sync;
mod tls;
mod transport;

use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;

/// MySQL-Tabellen differentiell über QUIC synchronisieren (chunked Hash-Diff).
#[derive(Parser, Debug)]
#[command(
    name = "mysql-sync",
    version,
    about = "MySQL-Tabellen differentiell über QUIC synchronisieren",
    after_help = "Beispiele:\n  Server:  ./mysql-sync -C config-server.json\n  Client:  ./mysql-sync -C config-client.json --sync db1.*\n           ./mysql-sync -C config-client.json --force --sync db1.*\n           ./mysql-sync -C config-client.json --force-schema db1,db2\n           ./mysql-sync -C config-client.json --check 10 db1.*\n           ./mysql-sync -C config-client.json --benchmark db_bench.big\n\nSelector A.B: A = DB-Liste (kommasepariert) oder *, B = Tabellen-Liste oder *\nExit-Codes: force/sync: 0=ok 1=Fehler | check: 0=gleich 1=unterschiedlich 2=Fehler"
)]
struct Cli {
    /// Pfad zur JSON-Konfigurationsdatei ("-" liest die Config von stdin)
    #[arg(short = 'C', long = "config")]
    config: PathBuf,

    /// Schema der Zieltabellen erzwingen (erstellen / neu erstellen). Optionaler Selector A.B
    #[arg(long, num_args = 0..=1, default_missing_value = "*")]
    force: Option<String>,

    /// Tabelleninhalte abgleichen (differentiell). Optionaler Selector A.B
    #[arg(long, num_args = 0..=1, default_missing_value = "*")]
    sync: Option<String>,

    /// Nur Schema für ganze Datenbanken erzwingen: Liste von DBs oder *
    #[arg(long = "force-schema", num_args = 0..=1, default_missing_value = "*")]
    force_schema: Option<String>,

    /// Anzahl unterschiedlicher Zeilen je Tabelle ermitteln; Wert = Anzahl Beispiel-IDs (z.B. --check 10)
    #[arg(long)]
    check: Option<u32>,

    /// Chunk-Hashes lokal erzeugen und Zeit messen (kein Server, kein Schreiben). Optionaler Selector A.B
    #[arg(long, num_args = 0..=1, default_missing_value = "*")]
    benchmark: Option<String>,

    /// Selector A.B (z.B. "db1.*" oder "db1,db2.t1,t2")
    selector: Option<String>,

    /// Keine Fortschrittsbalken anzeigen (für Cron/CI)
    #[arg(long)]
    no_progress: bool,

    /// Log-Level (error|warn|info|debug|trace)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Diff-/Check-Hashes als gruppierte MD5-Aggregate in MySQL berechnen (Default)
    #[arg(long, alias = "check-select")]
    hash_via_select: bool,

    /// Diff-/Check-Hashes über Export/Parser berechnen (transfer_all-Strategien)
    #[arg(long, alias = "check-dump")]
    hash_via_dump: bool,

    /// Diff-/Check-Hashes über rohe MySQL-Binary-Protokoll-Pakete berechnen (streng versionsgleich)
    #[arg(long, alias = "check-raw-binary", conflicts_with_all = ["hash_via_dump", "hash_via_raw_binary_vec", "hash_via_raw_tsv"])]
    hash_via_raw_binary: bool,

    /// Diff-/Check-Hashes über dekorierte Binary-Protokoll-Zeilen berechnen (versionsübergreifend)
    #[arg(
        long = "hash-raw-binary-vec",
        alias = "hash-raw-binary-row-vec",
        alias = "check-raw-binary-vec",
        conflicts_with_all = ["hash_via_dump", "hash_via_raw_binary", "hash_via_raw_tsv"]
    )]
    hash_via_raw_binary_vec: bool,

    /// Diff-/Check-Hashes über rohe `mysql --batch`-TSV-Zeilen berechnen
    #[arg(long, alias = "check-raw-tsv", conflicts_with_all = ["hash_via_dump", "hash_via_raw_binary", "hash_via_raw_binary_vec"])]
    hash_via_raw_tsv: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    env_logger::Builder::new()
        .filter_level(cli.log_level.parse().unwrap_or(log::LevelFilter::Info))
        .format_timestamp_secs()
        .init();

    let cfg = match config::load(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            log::error!("Config-Fehler: {e:#}");
            return ExitCode::from(2);
        }
    };

    let is_client = cli.force.is_some()
        || cli.sync.is_some()
        || cli.force_schema.is_some()
        || cli.check.is_some();

    let bench_modes: Vec<sync::HashVia> = if cli.hash_via_raw_tsv {
        vec![sync::HashVia::RawTsv]
    } else if cli.hash_via_raw_binary_vec {
        vec![sync::HashVia::RawBinaryVec]
    } else if cli.hash_via_raw_binary {
        vec![sync::HashVia::RawBinary]
    } else if cli.hash_via_dump {
        vec![sync::HashVia::Dump]
    } else if cli.hash_via_select {
        vec![sync::HashVia::Select]
    } else {
        sync::HashVia::ALL.to_vec()
    };

    let result = if let Some(selector) = cli.benchmark.clone() {
        benchmark::run(&cfg, Some(&selector), &bench_modes).await
    } else if is_client {
        client::run(client::ClientArgs {
            cfg,
            force: cli.force,
            sync: cli.sync,
            force_schema: cli.force_schema,
            check: cli.check,
            selector: cli.selector,
            no_progress: cli.no_progress,
            hash_via_select: cli.hash_via_select,
            hash_via_dump: cli.hash_via_dump,
            hash_via_raw_binary: cli.hash_via_raw_binary,
            hash_via_raw_binary_vec: cli.hash_via_raw_binary_vec,
            hash_via_raw_tsv: cli.hash_via_raw_tsv,
        })
        .await
    } else {
        server::run(cfg, &cli.config).await
    };

    match result {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            log::error!("{e:#}");
            ExitCode::from(2)
        }
    }
}
