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
mod testing;
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
    after_help = "Beispiele:\n  Konfigs: ./mysql-sync --generate-configs out/\n  Server:  ./mysql-sync -C server.json\n  Client:  ./mysql-sync -C client.json --sync db1:*\n           ./mysql-sync -C client.json --sync db1:* db2:*\n           ./mysql-sync -C client.json --force --sync db1:*\n           ./mysql-sync -C client.json --force-schema db1,db2\n           ./mysql-sync -C client.json --check 10 db1:*\n           ./mysql-sync -C client.json --benchmark db_bench:big\n  Merge:   ./mysql-sync -C base.json -C local-overrides.json --sync db1:*\n\nSelectoren: A:B — A = DB-Liste (kommasepariert) oder *, B = Tabellen-Liste oder *\nMehrere Selectoren als Positional-Argumente angebar.\nExit-Codes: force/sync: 0=ok 1=Fehler | check: 0=gleich 1=unterschiedlich 2=Fehler"
)]
struct Cli {
    /// JSON-Konfigurationsdatei ("-" liest von stdin); mehrfach angebbar, wird rekursiv gemerged
    #[arg(short = 'C', long = "config", value_name = "FILE")]
    config: Vec<PathBuf>,

    /// Erzeugt DIR/server.json + DIR/client.json inkl. PEM und Fingerprint
    #[arg(long = "generate-configs", value_name = "DIR")]
    generate_configs: Option<PathBuf>,

    /// Schema der Zieltabellen erzwingen (erstellen / neu erstellen)
    #[arg(long)]
    force: bool,

    /// Tabelleninhalte abgleichen (differentiell)
    #[arg(long)]
    sync: bool,

    /// Nur Schema für ganze Datenbanken erzwingen: Liste von DBs oder *
    #[arg(long = "force-schema")]
    force_schema: bool,

    /// Anzahl unterschiedlicher Zeilen je Tabelle ermitteln; Wert = Anzahl Beispiel-IDs (z.B. --check 10)
    #[arg(long)]
    check: Option<u32>,

    /// Chunk-Hashes lokal erzeugen und Zeit messen (kein Server, kein Schreiben)
    #[arg(long)]
    benchmark: bool,

    /// Nur fuer Tests: ab Chunk S die Chunks S..S+3 in der lokalen DB loeschen
    /// (alle / erste / letzte / zufaellige Zeile)
    #[arg(
        long = "testing-drop-chunks",
        alias = "testing-drop-chunks-source",
        alias = "testing-drop-chunks-dest"
    )]
    testing_drop_chunks: Option<u32>,

    /// Selectoren (z.B. "db1:*" oder "db1,db2:t1,t2"); mehrere angebar
    #[arg(value_name = "SELECTOR")]
    selectors: Vec<String>,

    /// Keine Fortschrittsbalken anzeigen (für Cron/CI)
    #[arg(long)]
    no_progress: bool,

    /// Log-Level (error|warn|info|debug|trace)
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Diff-/Check-Hashes als gruppierte SHA2-Aggregate in MySQL berechnen (Default)
    #[arg(
        long = "hash-select-sha2",
        alias = "hash-via-select-sha2",
        alias = "check-select-sha2"
    )]
    hash_via_select_sha2: bool,

    /// Diff-/Check-Hashes als schnelle CRC32-Aggregate in MySQL berechnen
    #[arg(
        long = "hash-select-crc32",
        alias = "hash-via-select-crc32",
        alias = "check-select-crc32"
    )]
    hash_via_select_crc32: bool,

    /// Diff-/Check-Hashes über Export/Parser berechnen (transfer_all-Strategien)
    #[arg(long = "hash-dump", alias = "hash-via-dump", alias = "check-dump")]
    hash_via_dump: bool,

    /// Diff-/Check-Hashes über rohe MySQL-Binary-Protokoll-Pakete berechnen (streng versionsgleich)
    #[arg(long = "hash-raw-binary-row", alias = "hash-via-raw-binary", alias = "check-raw-binary", conflicts_with_all = ["hash_via_dump", "hash_via_raw_binary_vec", "hash_via_raw_tsv"])]
    hash_via_raw_binary: bool,

    /// Diff-/Check-Hashes über dekorierte Binary-Protokoll-Zeilen berechnen (versionsübergreifend)
    #[arg(
        long = "hash-raw-binary-row-vec",
        alias = "hash-raw-binary-vec",
        alias = "hash-via-raw-binary-vec",
        alias = "check-raw-binary-vec",
        conflicts_with_all = ["hash_via_dump", "hash_via_raw_binary", "hash_via_raw_tsv"]
    )]
    hash_via_raw_binary_vec: bool,

    /// Diff-/Check-Hashes über rohe `mysql --batch`-TSV-Zeilen berechnen
    #[arg(long = "hash-raw-tsv", alias = "hash-via-raw-tsv", alias = "check-raw-tsv", conflicts_with_all = ["hash_via_dump", "hash_via_raw_binary", "hash_via_raw_binary_vec"])]
    hash_via_raw_tsv: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();

    env_logger::Builder::new()
        .filter_level(cli.log_level.parse().unwrap_or(log::LevelFilter::Info))
        .format_timestamp_secs()
        .init();

    if let Some(dir) = &cli.generate_configs {
        return match config::generate_configs(dir) {
            Ok((server, client)) => {
                println!("{}", server.display());
                println!("{}", client.display());
                ExitCode::SUCCESS
            }
            Err(e) => {
                log::error!("{e:#}");
                ExitCode::from(2)
            }
        };
    }

    if cli.config.is_empty() {
        log::error!("-C/--config fehlt");
        return ExitCode::from(2);
    }

    let cfg = match config::load_many(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            log::error!("Config-Fehler: {e:#}");
            return ExitCode::from(2);
        }
    };

    let selector_str = if cli.selectors.is_empty() {
        None
    } else {
        Some(cli.selectors.join(","))
    };

    let is_client = cli.force
        || cli.sync
        || cli.force_schema
        || cli.check.is_some()
        || cli.testing_drop_chunks.is_some();

    let bench_modes: Vec<sync::HashVia> = if cli.hash_via_raw_tsv {
        vec![sync::HashVia::RawTsv]
    } else if cli.hash_via_raw_binary_vec {
        vec![sync::HashVia::RawBinaryVec]
    } else if cli.hash_via_raw_binary {
        vec![sync::HashVia::RawBinary]
    } else if cli.hash_via_dump {
        vec![sync::HashVia::Dump]
    } else if cli.hash_via_select_crc32 {
        vec![sync::HashVia::SelectCrc32]
    } else if cli.hash_via_select_sha2 {
        vec![sync::HashVia::SelectSha2]
    } else {
        sync::HashVia::ALL.to_vec()
    };

    let result = if let Some(start) = cli.testing_drop_chunks {
        testing::run(&cfg, selector_str.as_deref(), start).await
    } else if cli.benchmark {
        benchmark::run(&cfg, selector_str.as_deref(), &bench_modes).await
    } else if is_client {
        client::run(client::ClientArgs {
            cfg,
            force: cli.force,
            sync: cli.sync,
            force_schema: cli.force_schema,
            check: cli.check,
            selector: selector_str,
            no_progress: cli.no_progress,
            hash_via_select_sha2: cli.hash_via_select_sha2,
            hash_via_select_crc32: cli.hash_via_select_crc32,
            hash_via_dump: cli.hash_via_dump,
            hash_via_raw_binary: cli.hash_via_raw_binary,
            hash_via_raw_binary_vec: cli.hash_via_raw_binary_vec,
            hash_via_raw_tsv: cli.hash_via_raw_tsv,
        })
        .await
    } else {
        server::run(cfg).await
    };

    match result {
        Ok(code) => ExitCode::from(code),
        Err(e) => {
            log::error!("{e:#}");
            ExitCode::from(2)
        }
    }
}
