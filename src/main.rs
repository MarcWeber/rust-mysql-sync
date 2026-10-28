//! mysql-sync — differentieller MySQL-Tabellen-Sync über QUIC.
//!
//! Haupt-Einstiegspunkt: CLI parsen, Config laden, Server- oder Client-Modus.

mod client;
mod config;
mod export;
mod hash;
mod import;
mod mysqlutil;
mod progress;
mod proto;
mod selector;
mod server;
mod sync;
mod tls;

use clap::Parser;
use std::path::PathBuf;
use std::process::ExitCode;

/// MySQL-Tabellen differentiell über QUIC synchronisieren (chunked Hash-Diff).
#[derive(Parser, Debug)]
#[command(
    name = "mysql-sync",
    version,
    about = "MySQL-Tabellen differentiell über QUIC synchronisieren",
    after_help = "Beispiele:\n  Server:  ./mysql-sync -C config-server.json\n  Client:  ./mysql-sync -C config-client.json --sync db_skywings.*\n           ./mysql-sync -C config-client.json --force --sync db_skywings.*\n           ./mysql-sync -C config-client.json --force-schema db_skywings,db_airwings\n           ./mysql-sync -C config-client.json --check 10 db_skywings.*\n\nSelector A.B: A = DB-Liste (kommasepariert) oder *, B = Tabellen-Liste oder *\nExit-Codes: force/sync: 0=ok 1=Fehler | check: 0=gleich 1=unterschiedlich 2=Fehler"
)]
struct Cli {
    /// Pfad zur JSON-Konfigurationsdatei
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

    /// Selector A.B (z.B. "db_skywings.*" oder "db1,db2.t1,t2")
    selector: Option<String>,

    /// Keine Fortschrittsbalken anzeigen (für Cron/CI)
    #[arg(long)]
    no_progress: bool,

    /// Log-Level (error|warn|info|debug|trace)
    #[arg(long, default_value = "info")]
    log_level: String,
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

    let is_client =
        cli.force.is_some() || cli.sync.is_some() || cli.force_schema.is_some() || cli.check.is_some();

    let result = if is_client {
        client::run(client::ClientArgs {
            cfg,
            force: cli.force,
            sync: cli.sync,
            force_schema: cli.force_schema,
            check: cli.check,
            selector: cli.selector,
            no_progress: cli.no_progress,
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
