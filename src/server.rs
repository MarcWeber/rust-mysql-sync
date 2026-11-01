//! mysql-sync Server: QUIC-Listener mit MySQL-Zugriff.
//!
//! Ein Bidirectional-Stream = state machine:
//!   Normal: Hello/DescribeCount/Recreate/Truncate/Hashes/DeletePks/Triggers
//!   Dump:   DumpBegin (JSON) → BIN-Frames → DumpEnd (JSON) → Ausführen
//!   Csv:    CsvBegin (JSON) → BIN-Frames → CsvEnd (JSON) → Import

use crate::config::Config;
use crate::export;
use crate::hash;
use crate::hash_binary;
use crate::hash_tsv;
use crate::import::{self, TsvBatcher};
use crate::mysqlutil::{self, qualified};
use crate::proto::{self, Framing, Request, Response};
use crate::sqlhash;
use crate::tls;
use crate::transport::{RecvHalf, SendHalf};
use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Pool, Value};
use quinn::Connection;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

const MAX_BUCKETS_PER_BATCH: usize = 512;
const MAX_ROWHASHES_PER_BATCH: usize = 1024;
const MAX_DUMP_BUFFER: u64 = 2 * 1024 * 1024 * 1024; // 2 GB Guard

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashSource {
    Select,
    Dump,
    RawBinary,
    RawBinaryVec,
    RawTsv,
}

impl HashSource {
    fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.unwrap_or("select").to_ascii_lowercase().as_str() {
            "" | "select" => Ok(HashSource::Select),
            "dump" => Ok(HashSource::Dump),
            "raw_binary" | "raw-binary" | "binary" => Ok(HashSource::RawBinary),
            "raw_binary_vec" | "raw-binary-vec" | "binary_vec" | "binary-vec" => {
                Ok(HashSource::RawBinaryVec)
            }
            "raw_tsv" | "raw-tsv" | "tsv" => Ok(HashSource::RawTsv),
            other => anyhow::bail!("Unbekannte hash_source: {other}"),
        }
    }

    fn is_raw(self) -> bool {
        matches!(
            self,
            HashSource::RawBinary | HashSource::RawBinaryVec | HashSource::RawTsv
        )
    }
}

pub async fn run(cfg: Config, config_path: &std::path::Path) -> Result<std::process::ExitCode> {
    // Bei Config von stdin gibt es keinen Dateinamen; Zertifikate landen dann
    // neben dem Arbeitsverzeichnis unter dem üblichen Namen.
    let cert_base = if config_path == std::path::Path::new("-") {
        std::path::PathBuf::from("config-server.json")
    } else {
        config_path.to_path_buf()
    };

    let plaintext = cfg.plaintext_tcp();
    let pool = mysqlutil::new_pool(&cfg.mysql, None)?;
    let state = Arc::new(ServerState { cfg, pool });
    let addr = state.cfg.listen_addr()?;

    log::info!(
        "chunked={} ({} Buckets), transfer_batch_rows={}, diff_threshold_percent={}%",
        state.cfg.chunked,
        10u64.pow(state.cfg.chunked),
        state.cfg.transfer_batch_rows,
        state.cfg.diff_threshold_percent
    );

    if plaintext {
        eprintln!("WARNING: PLAINTEXT transport=tcp — encryption disabled");
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("TCP-Listener auf {addr}"))?;
        eprintln!("mysql-sync Server lauscht auf {addr} (tcp, plaintext)");
        loop {
            let (stream, remote) = listener.accept().await?;
            stream.set_nodelay(true)?;
            let (recv, send) = stream.into_split();
            let state = state.clone();
            let hello_done = Arc::new(AtomicBool::new(false));
            tokio::spawn(async move {
                if let Err(e) =
                    handle_stream(Box::new(send), Box::new(recv), state, hello_done).await
                {
                    log::error!("Verbindungsfehler von {remote}: {e:#}");
                }
            });
        }
    }

    let (certs, key, fingerprint) = tls::server_identity(&state.cfg, &cert_base)?;
    let provider = tls::ring_provider();
    let mut server_crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("TLS-Protokoll-Versionen")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("TLS-Zertifikat/Key ungültig")?;
    server_crypto.alpn_protocols = vec![crate::config::ALPN.to_vec()];

    let mut quinn_cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .context("QUIC-TLS-Konfiguration")?,
    ));
    quinn_cfg.transport_config(Arc::new(crate::transport::quic_transport()?));
    let endpoint = quinn::Endpoint::server(quinn_cfg, addr)
        .with_context(|| format!("QUIC-Listener auf {addr}"))?;

    eprintln!("mysql-sync Server lauscht auf {addr} (quic/tls)");
    eprintln!("TLS-Fingerprint: {fingerprint}");
    eprintln!("→ Fingerprints in config-client.json → \"server_fingerprint\" (Pinning empfohlen)");

    while let Some(incoming) = endpoint.accept().await {
        let state = state.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    if let Err(e) = handle_connection(conn, state).await {
                        log::error!("Verbindungsfehler: {e:#}");
                    }
                }
                Err(e) => log::warn!("Handshake fehlgeschlagen: {e}"),
            }
        });
    }
    Ok(std::process::ExitCode::SUCCESS)
}

struct ServerState {
    cfg: Config,
    pool: Pool,
}

async fn handle_connection(conn: Connection, state: Arc<ServerState>) -> Result<()> {
    let remote = conn.remote_address();
    log::debug!("Verbindung von {remote}");
    let hello_done = Arc::new(AtomicBool::new(false));
    loop {
        let (send, recv) = match conn.accept_bi().await {
            Ok(sr) => sr,
            Err(quinn::ConnectionError::ApplicationClosed(c)) => {
                log::info!("Verbindung geschlossen: {remote} ({c:?})");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let state = state.clone();
        let hello_done = hello_done.clone();
        log::debug!("Stream akzeptiert von {remote}");
        tokio::spawn(async move {
            if let Err(e) = handle_stream(Box::new(send), Box::new(recv), state, hello_done).await {
                log::error!("Stream-Fehler: {e:#}");
            }
        });
    }
}

enum Mode {
    Normal,
    Dump {
        db: String,
        table: String,
        buf: Vec<u8>,
    },
    Csv {
        db: String,
        table: String,
        batcher: TsvBatcher,
        counter: u64,
        err: Option<String>,
    },
}

async fn handle_stream(
    mut send: SendHalf,
    mut recv: RecvHalf,
    state: Arc<ServerState>,
    hello_done: Arc<AtomicBool>,
) -> Result<()> {
    let f = Framing::from_cfg(&state.cfg);
    let mut mode = Mode::Normal;
    let mut trigger_backup: HashMap<(String, String), Vec<import::TriggerDef>> = HashMap::new();

    while let Some(req) = proto::read_request(&mut recv, &f).await? {
        // --- States carrying bulk data ---
        match &mut mode {
            Mode::Dump { db, table, buf } => {
                match req {
                    Request::Bulk { data } => {
                        buf_len_guard(buf)?;
                        buf.extend_from_slice(&data);
                    }
                    Request::DumpEnd { .. } => {
                        let sql = String::from_utf8_lossy(buf).to_string();
                        let resp = match mysqlutil::session_conn(&state.pool).await {
                            Ok(mut conn) => match import::execute_dump(&mut conn, &sql).await {
                                Ok(n) => Response::Ok {
                                    message: "dump executed".into(),
                                    count: Some(n),
                                },
                                Err(e) => Response::err(format!("{e:#}")),
                            },
                            Err(e) => Response::err(format!("{e:#}")),
                        };
                        let failed = matches!(resp, Response::Err { .. });
                        send_response(&mut send, resp, &f).await?;
                        mode = Mode::Normal;
                        if failed {
                            return Ok(());
                        }
                    }
                    _ => {
                        let _ = (db, table);
                        send_response(&mut send, Response::err("Erwartet Bulk/DumpEnd"), &f)
                            .await?;
                        mode = Mode::Normal;
                    }
                }
                continue;
            }
            Mode::Csv {
                db,
                table,
                batcher,
                counter,
                err,
            } => {
                match req {
                    Request::Bulk { data } => {
                        if err.is_none() {
                            match batcher.push(&data) {
                                Ok(rows) => {
                                    if !rows.is_empty() {
                                        match mysqlutil::session_conn(&state.pool).await {
                                            Ok(mut conn) => {
                                                match import::import_rows(
                                                    &mut conn,
                                                    db,
                                                    table,
                                                    &batcher.columns,
                                                    rows,
                                                )
                                                .await
                                                {
                                                    Ok(n) => *counter += n,
                                                    Err(e) => *err = Some(format!("{e:#}")),
                                                }
                                            }
                                            Err(e) => *err = Some(format!("{e:#}")),
                                        }
                                    }
                                }
                                Err(e) => *err = Some(format!("{e:#}")),
                            }
                        }
                    }
                    Request::CsvEnd { .. } => {
                        // Flush the rest
                        let mut resp = if let Some(e) = err.take() {
                            Response::err(e)
                        } else {
                            let rows = batcher.finish()?;
                            match mysqlutil::session_conn(&state.pool).await {
                                Ok(mut conn) => match import::import_rows(
                                    &mut conn,
                                    db,
                                    table,
                                    &batcher.columns,
                                    rows,
                                )
                                .await
                                {
                                    Ok(n) => {
                                        *counter += n;
                                        Response::Ok {
                                            message: "csv imported".into(),
                                            count: Some(*counter),
                                        }
                                    }
                                    Err(e) => Response::err(format!("{e:#}")),
                                },
                                Err(e) => Response::err(format!("{e:#}")),
                            }
                        };
                        let failed = matches!(resp, Response::Err { .. });
                        if let Response::Ok { message, count } = &mut resp {
                            *message = format!("csv imported: {} rows", count.unwrap_or(0));
                        }
                        send_response(&mut send, resp, &f).await?;
                        mode = Mode::Normal;
                        if failed {
                            return Ok(());
                        }
                    }
                    _ => {
                        send_response(&mut send, Response::err("Erwartet Bulk/CsvEnd"), &f).await?;
                        mode = Mode::Normal;
                    }
                }
                continue;
            }
            Mode::Normal => {}
        }

        let is_hello = matches!(req, Request::Hello { .. });
        if !is_hello && !hello_done.load(Ordering::SeqCst) {
            send_response(
                &mut send,
                Response::err("Erster Request muss Hello sein (Auth)"),
                &f,
            )
            .await?;
            return Ok(());
        }

        let resp = match req {
            Request::Hello {
                version,
                token,
                hash_algo,
            } => {
                let r = handle_hello(state.as_ref(), version, &token, hash_algo.as_deref()).await;
                if matches!(r, Response::Ok { .. }) {
                    hello_done.store(true, Ordering::SeqCst);
                }
                r
            }
            Request::MysqlVersion {} => handle_mysql_version(state.as_ref()).await,
            Request::DescribeCount { db, table } => {
                handle_describe_count(state.as_ref(), &db, &table).await
            }
            Request::Recreate { db, sql } => handle_recreate(state.as_ref(), &db, &sql).await,
            Request::Truncate { db, table } => handle_truncate(state.as_ref(), &db, &table).await,
            Request::Hashes {
                db,
                table,
                primary_keys,
                chunked,
                hash_of_chunk,
                buckets,
                hash_source,
                profile,
                schema_hash,
            } => {
                let source = match HashSource::parse(hash_source.as_deref()) {
                    Ok(s) => s,
                    Err(e) => {
                        send_response(&mut send, Response::err(format!("{e:#}")), &f).await?;
                        return Ok(());
                    }
                };
                // Streaming: writes all responses including the final one itself
                if let Err(e) = do_hashes(
                    state.as_ref(),
                    &db,
                    &table,
                    primary_keys,
                    chunked,
                    hash_of_chunk,
                    buckets,
                    source,
                    profile,
                    schema_hash,
                    &mut send,
                    &f,
                )
                .await
                {
                    send_response(&mut send, Response::err(format!("{e:#}")), &f).await?;
                    return Ok(());
                }
                continue;
            }
            Request::DumpBegin { db, table } => {
                mode = Mode::Dump {
                    db,
                    table,
                    buf: Vec::new(),
                };
                continue;
            }
            Request::CsvBegin {
                db,
                table,
                columns,
                hex_columns,
            } => {
                mode = Mode::Csv {
                    db,
                    table,
                    batcher: TsvBatcher::new(columns, &hex_columns),
                    counter: 0,
                    err: None,
                };
                continue;
            }
            Request::DeletePks { db, table, pks } => {
                match mysqlutil::session_conn(&state.pool).await {
                    Ok(mut conn) => match import::delete_pks(&mut conn, &db, &table, &pks).await {
                        Ok(n) => Response::Ok {
                            message: format!("deleted {n}"),
                            count: Some(n),
                        },
                        Err(e) => Response::err(format!("{e:#}")),
                    },
                    Err(e) => Response::err(format!("{e:#}")),
                }
            }
            Request::DisableTriggers { db, table } => {
                match mysqlutil::session_conn(&state.pool).await {
                    Ok(mut conn) => {
                        match import::snapshot_and_drop_triggers(&mut conn, &db, &table).await {
                            Ok(defs) => {
                                trigger_backup.insert((db.clone(), table.clone()), defs);
                                Response::ok("triggers disabled")
                            }
                            Err(e) => Response::err(format!("{e:#}")),
                        }
                    }
                    Err(e) => Response::err(format!("{e:#}")),
                }
            }
            Request::RestoreTriggers { db, table } => {
                let defs = trigger_backup
                    .remove(&(db.clone(), table.clone()))
                    .unwrap_or_default();
                match mysqlutil::session_conn(&state.pool).await {
                    Ok(mut conn) => match import::restore_triggers(&mut conn, &db, &defs).await {
                        Ok(()) => Response::ok("triggers restored"),
                        Err(e) => Response::err(format!("{e:#}")),
                    },
                    Err(e) => Response::err(format!("{e:#}")),
                }
            }
            Request::Bulk { .. } | Request::DumpEnd { .. } | Request::CsvEnd { .. } => {
                Response::err("Unerwarteter Request in diesem Zustand")
            }
        };

        // App-level Err responses are part of the protocol (e.g. "table missing"
        // before --force) — the stream stays open. Only protocol violations
        // terminate the stream early.
        send_response(&mut send, resp, &f).await?;
    }
    Ok(())
}

fn buf_len_guard(buf: &Vec<u8>) -> Result<()> {
    if buf.len() as u64 > MAX_DUMP_BUFFER {
        anyhow::bail!(
            "Dump-Buffer überschritten ({} Bytes Limit)",
            MAX_DUMP_BUFFER
        );
    }
    Ok(())
}

async fn send_response(send: &mut SendHalf, resp: Response, f: &Framing) -> Result<()> {
    proto::write_response(send, &resp, f).await?;
    Ok(())
}

async fn handle_hello(
    state: &ServerState,
    version: u32,
    token: &Option<String>,
    hash_algo: Option<&str>,
) -> Response {
    if version != proto::version() {
        return Response::err(format!(
            "Protokoll-Version {version} nicht unterstützt (Server: {})",
            proto::version()
        ));
    }
    if let Some(expected) = &state.cfg.auth_token {
        if token.as_deref() != Some(expected.as_str()) {
            return Response::err("Auth-Token ungültig");
        }
    }
    if let Some(client_algo) = hash_algo {
        let server_algo = state.cfg.hash_algo.to_string();
        if client_algo != server_algo {
            return Response::err(format!(
                "Hash-Algorithmus weicht ab: Client={client_algo}, Server={server_algo} \
                 (beide configs müssen hash_algo gleich setzen)"
            ));
        }
    }
    Response::ok("welcome")
}

async fn handle_describe_count(state: &ServerState, db: &str, table: &str) -> Response {
    match mysqlutil::session_conn(&state.pool).await {
        Ok(mut conn) => match mysqlutil::table_info(&mut conn, db, table).await {
            Ok(info) => Response::SchemaInfo {
                create_sql: info.create_sql,
                columns: info.columns,
                count: info.count,
            },
            Err(e) => Response::err(format!("{e:#}")),
        },
        Err(e) => Response::err(format!("{e:#}")),
    }
}

async fn handle_mysql_version(state: &ServerState) -> Response {
    match mysqlutil::session_conn(&state.pool).await {
        Ok(mut conn) => match mysqlutil::mysql_profile(&mut conn).await {
            Ok(profile) => Response::MysqlVersion {
                version: profile.version.clone(),
                profile,
            },
            Err(e) => Response::err(format!("{e:#}")),
        },
        Err(e) => Response::err(format!("{e:#}")),
    }
}

async fn handle_recreate(state: &ServerState, db: &str, sql: &str) -> Response {
    match mysqlutil::session_conn(&state.pool).await {
        Ok(mut conn) => {
            // CREATE auf Ziel-DB + Ziel-Tabellennamen qualifizieren
            // (Quelle liefert `CREATE TABLE \`t\` (...)` ohne DB-Präfix;
            //  hier greifen auch database_table_mappings)
            let target_table = table_name_of_create(sql);
            let qualified_sql = rewrite_create_table(sql, db, &target_table);
            let r: Result<()> = async {
                conn.query_drop(format!(
                    "CREATE DATABASE IF NOT EXISTS {} CHARACTER SET utf8mb4",
                    mysqlutil::quote_ident(db)
                ))
                .await
                .context("CREATE DATABASE fehlgeschlagen")?;
                conn.query_drop(format!(
                    "DROP TABLE IF EXISTS {}",
                    qualified(db, &target_table)
                ))
                .await
                .context("DROP TABLE fehlgeschlagen")?;
                conn.query_drop(qualified_sql)
                    .await
                    .context("CREATE TABLE fehlgeschlagen")?;
                Ok(())
            }
            .await;
            match r {
                Ok(()) => Response::ok("table recreated"),
                Err(e) => Response::err(format!("{e:#}")),
            }
        }
        Err(e) => Response::err(format!("{e:#}")),
    }
}

/// CREATE TABLE-Statement auf `db`.`table` umbenennen:
///   "CREATE TABLE `t` (…)" → "CREATE TABLE `db`.`t` (…)"
fn rewrite_create_table(sql: &str, db: &str, table: &str) -> String {
    let lower = sql.to_lowercase();
    let Some(pos) = lower.find("table") else {
        return sql.to_string();
    };
    let rest = sql[pos + 5..].trim_start();
    let rest = if rest.to_lowercase().starts_with("if not exists") {
        rest[13..].trim_start()
    } else {
        rest
    };
    let Some(paren) = rest.find('(') else {
        return sql.to_string();
    };
    // Kopf ("CREATE ") + "TABLE " + qualifizierter Name + Spaltenteil ab "("
    format!(
        "{}TABLE {}{}",
        &sql[..pos],
        qualified(db, table),
        &rest[paren..]
    )
}

/// Tabellennamen aus einem CREATE TABLE Statement extrahieren
/// (für DROP IF EXISTS — der Name kommt von der Quelle).
fn table_name_of_create(sql: &str) -> String {
    let lower = sql.to_lowercase();
    if let Some(pos) = lower.find("table") {
        let rest = sql[pos + 5..].trim();
        // "IF NOT EXISTS" überspringen
        let rest = if rest.to_lowercase().starts_with("if not exists") {
            rest[13..].trim()
        } else {
            rest
        };
        let token: String = rest
            .chars()
            .take_while(|c| !c.is_whitespace() && *c != '(')
            .collect();
        return token.trim_matches('`').to_string();
    }
    String::new()
}

async fn handle_truncate(state: &ServerState, db: &str, table: &str) -> Response {
    match mysqlutil::session_conn(&state.pool).await {
        Ok(mut conn) => {
            let r: Result<()> = async {
                if let Err(e) = conn
                    .query_drop(format!("TRUNCATE TABLE {}", qualified(db, table)))
                    .await
                {
                    log::warn!("TRUNCATE fehlgeschlagen ({e}) — versuche DELETE FROM");
                    conn.query_drop(format!("DELETE FROM {}", qualified(db, table)))
                        .await?;
                }
                Ok(())
            }
            .await;
            match r {
                Ok(()) => Response::ok("truncated"),
                Err(e) => Response::err(format!("{e:#}")),
            }
        }
        Err(e) => Response::err(format!("{e:#}")),
    }
}

/// Wie check_export_strategies auf Client-Seite: binäre Spalten → mysqldump,
/// sonst möglichst Parser-Strategien.
async fn server_export_strategies(
    state: &ServerState,
    db: &str,
    table: &str,
) -> Result<Vec<String>> {
    let mut conn = mysqlutil::session_conn(&state.pool).await?;
    let configured = state.cfg.strategies();
    let bin = mysqlutil::binary_column_indexes(&mut conn, db, table).await?;
    if !bin.is_empty() {
        let mut v = vec!["mysqldump".to_string()];
        for s in configured {
            if !v.iter().any(|x| x == &s) {
                v.push(s);
            }
        }
        return Ok(v);
    }
    let parsing: Vec<String> = configured
        .iter()
        .filter(|s| {
            let t = s.to_lowercase();
            t != "select-stream" && t != "select_stream" && t != "stream"
        })
        .cloned()
        .collect();
    if parsing.is_empty() {
        Ok(configured)
    } else {
        Ok(parsing)
    }
}

/// Iteriert über alle Zeilen aus dem Export-/Parser-Pfad und ruft `f` je Zeile.
async fn export_for_each_row(
    state: &ServerState,
    db: &str,
    table: &str,
    where_clause: Option<String>,
    order_by: Option<String>,
    f: &mut (dyn FnMut(&[Value]) + Send),
) -> Result<()> {
    let strategies = server_export_strategies(state, db, table).await?;
    let mut rx = export::spawn_fetch_with_strategies(
        &state.cfg,
        db.to_string(),
        table.to_string(),
        where_clause,
        order_by,
        strategies,
    );
    while let Some(msg) = rx.recv().await {
        match msg {
            export::RowMsg::Row(values) => f(&values),
            export::RowMsg::Err(e) => anyhow::bail!("Export/Parser-Fehler: {e}"),
            export::RowMsg::Done => break,
        }
    }
    Ok(())
}

async fn write_chunk_stream(
    send: &mut SendHalf,
    mut rx: mpsc::Receiver<hash::ChunkHashEvent>,
    f: &Framing,
    filter: Option<HashSet<String>>,
) -> Result<u64> {
    let mut batch: Vec<proto::ChunkHash> = Vec::with_capacity(MAX_BUCKETS_PER_BATCH);
    let mut total = 0u64;
    while let Some(event) = rx.recv().await {
        match event {
            hash::ChunkHashEvent::Item((bucket, count, hash)) => {
                if filter.as_ref().map(|f| f.contains(&bucket)).unwrap_or(true) {
                    batch.push(proto::ChunkHash {
                        bucket,
                        hash,
                        count,
                    });
                    total += 1;
                    if batch.len() >= MAX_BUCKETS_PER_BATCH {
                        proto::write_response(
                            send,
                            &Response::ChunkHashes {
                                items: std::mem::take(&mut batch),
                            },
                            f,
                        )
                        .await?;
                    }
                }
            }
            hash::ChunkHashEvent::Error(e) => anyhow::bail!("{e:#}"),
        }
    }
    if !batch.is_empty() {
        proto::write_response(send, &Response::ChunkHashes { items: batch }, f).await?;
    }
    Ok(total)
}

#[allow(clippy::too_many_arguments)]
async fn do_hashes(
    state: &ServerState,
    db: &str,
    table: &str,
    primary_keys: Vec<String>,
    chunked: u32,
    hash_of_chunk: bool,
    buckets: Option<Vec<String>>,
    source: HashSource,
    profile: Option<proto::MysqlProfile>,
    schema_hash: Option<String>,
    send: &mut SendHalf,
    f: &Framing,
) -> Result<()> {
    let mut conn = mysqlutil::session_conn(&state.pool).await?;
    let pk_meta = mysqlutil::pk_columns(&mut conn, db, table).await?;
    if pk_meta.is_empty() {
        send_response(
            send,
            Response::err(format!("{db}.{table} hat keinen Primary Key")),
            f,
        )
        .await?;
        return Ok(());
    }
    let server_pks: Vec<String> = pk_meta.iter().map(|p| p.name.clone()).collect();
    if server_pks != primary_keys {
        send_response(
            send,
            Response::err(format!(
                "Primary Keys weichen ab: Server={server_pks:?} Client={primary_keys:?}"
            )),
            f,
        )
        .await?;
        return Ok(());
    }
    let mode = hash::chunk_mode_for_pk(&pk_meta);

    if source.is_raw() {
        let client_profile = match profile {
            Some(p) => p,
            None => {
                send_response(
                    send,
                    Response::err("Raw-Hashing braucht ein MySQL-Profil"),
                    f,
                )
                .await?;
                return Ok(());
            }
        };
        let server_profile = mysqlutil::mysql_profile(&mut conn).await?;
        let diff = if source == HashSource::RawBinaryVec {
            client_profile.difference_ignoring_version(&server_profile)
        } else {
            client_profile.difference(&server_profile)
        };
        if let Some(diff) = diff {
            send_response(
                send,
                Response::err(format!(
                    "Raw-Hashing abgelehnt — MySQL-Profil weicht ab: {diff}"
                )),
                f,
            )
            .await?;
            return Ok(());
        }
        let target_schema = hash::hash_bytes(
            mysqlutil::normalize_schema(&mysqlutil::create_sql(&mut conn, db, table).await?)
                .as_bytes(),
            state.cfg.hash_algo(),
        );
        if schema_hash.as_deref() != Some(target_schema.as_str()) {
            send_response(
                send,
                Response::err("Raw-Hashing abgelehnt — Schema-Hash weicht ab"),
                f,
            )
            .await?;
            return Ok(());
        }
        if source == HashSource::RawTsv {
            let binary = mysqlutil::binary_column_indexes(&mut conn, db, table).await?;
            if !binary.is_empty() {
                send_response(
                    send,
                    Response::err(format!(
                        "raw-tsv unterstützt keine binären Spalten (Indizes {binary:?})"
                    )),
                    f,
                )
                .await?;
                return Ok(());
            }
        }
    }

    let names = mysqlutil::column_names(&mut conn, db, table).await?;
    let mut pk_indexes = Vec::new();
    for pk in &primary_keys {
        match names.iter().position(|n| n == pk) {
            Some(i) => pk_indexes.push(i),
            None => {
                send_response(send, Response::err(format!("PK-Spalte {pk} fehlt")), f).await?;
                return Ok(());
            }
        }
    }

    let filter: Option<HashSet<String>> = buckets.map(|b| b.into_iter().collect());
    let chunked_c = chunked.clamp(1, 6);

    if hash_of_chunk {
        let (tx, rx) = mpsc::channel::<hash::ChunkHashEvent>(200);
        let scan = async {
            match source {
                HashSource::RawBinary => {
                    hash_binary::stream_chunk_hashes(
                        tx,
                        &mut conn,
                        db,
                        table,
                        &pk_meta,
                        &pk_indexes,
                        mode,
                        chunked_c,
                        state.cfg.hash_algo(),
                    )
                    .await
                }
                HashSource::RawBinaryVec => {
                    hash_binary::stream_chunk_hashes_vec(
                        tx,
                        &mut conn,
                        db,
                        table,
                        &pk_meta,
                        &pk_indexes,
                        mode,
                        chunked_c,
                        state.cfg.hash_algo(),
                    )
                    .await
                }
                HashSource::RawTsv => {
                    hash_tsv::stream_chunk_hashes(
                        tx,
                        &state.cfg,
                        db,
                        table,
                        &pk_meta,
                        &pk_indexes,
                        mode,
                        chunked_c,
                    )
                    .await
                }
                HashSource::Dump => {
                    let strategies = server_export_strategies(state, db, table).await?;
                    export::stream_chunk_hashes(
                        tx,
                        &state.cfg,
                        db,
                        table,
                        &pk_meta,
                        &pk_indexes,
                        mode,
                        chunked_c,
                        strategies,
                    )
                    .await
                }
                HashSource::Select => {
                    sqlhash::stream_chunk_hashes(
                        tx, &mut conn, db, table, &pk_meta, mode, chunked_c,
                    )
                    .await
                }
            }
        };
        let writer = write_chunk_stream(send, rx, f, filter);
        let (scan_res, writer_res) = tokio::join!(scan, writer);
        if let Err(e) = scan_res {
            send_response(send, Response::err(format!("{e:#}")), f).await?;
            return Ok(());
        }
        let total = match writer_res {
            Ok(n) => n,
            Err(e) => {
                send_response(send, Response::err(format!("{e:#}")), f).await?;
                return Ok(());
            }
        };
        log::info!("[{db}.{table}] {total} Bucket-Hashes gestreamt");
        send_response(
            send,
            Response::Ok {
                message: format!("{total} buckets"),
                count: Some(total),
            },
            f,
        )
        .await?;
        return Ok(());
    } else {
        // Zeilen-Hashes für bestimmte Buckets — Keyset-Paginierung über PK,
        // damit der Speicher begrenzt bleibt (Page für Page, Schreiben dazwischen).
        let bucket_list: Vec<String> = filter
            .as_ref()
            .map(|f| f.iter().cloned().collect())
            .unwrap_or_default();
        let bucket_filter = if bucket_list.is_empty() {
            None
        } else {
            Some(mysqlutil::chunk_where(
                &pk_meta,
                mode,
                chunked_c,
                &bucket_list,
            )?)
        };
        let names = mysqlutil::column_names(&mut conn, db, table).await?;

        if source == HashSource::RawBinary {
            let rows = hash_binary::row_hashes(
                &mut conn,
                db,
                table,
                &pk_meta,
                &pk_indexes,
                mode,
                chunked_c,
                &bucket_list,
                state.cfg.hash_algo(),
            )
            .await?;
            for chunk in rows.chunks(MAX_ROWHASHES_PER_BATCH) {
                proto::write_response(
                    send,
                    &Response::RowHashes {
                        items: chunk.to_vec(),
                    },
                    f,
                )
                .await?;
            }
            send_response(send, Response::ok("row hashes done"), f).await?;
            return Ok(());
        }

        if source == HashSource::RawBinaryVec {
            let rows = hash_binary::row_hashes_vec(
                &mut conn,
                db,
                table,
                &pk_meta,
                &pk_indexes,
                mode,
                chunked_c,
                &bucket_list,
                state.cfg.hash_algo(),
            )
            .await?;
            for chunk in rows.chunks(MAX_ROWHASHES_PER_BATCH) {
                proto::write_response(
                    send,
                    &Response::RowHashes {
                        items: chunk.to_vec(),
                    },
                    f,
                )
                .await?;
            }
            send_response(send, Response::ok("row hashes done"), f).await?;
            return Ok(());
        }

        if source == HashSource::RawTsv {
            let rows = hash_tsv::row_hashes(
                &state.cfg,
                db,
                table,
                &pk_meta,
                &pk_indexes,
                mode,
                chunked_c,
                &bucket_list,
            )
            .await?;
            for chunk in rows.chunks(MAX_ROWHASHES_PER_BATCH) {
                proto::write_response(
                    send,
                    &Response::RowHashes {
                        items: chunk.to_vec(),
                    },
                    f,
                )
                .await?;
            }
            send_response(send, Response::ok("row hashes done"), f).await?;
            return Ok(());
        }

        if source != HashSource::Dump {
            let selected = filter.as_ref().map(|_| bucket_list.as_slice());
            let rows =
                sqlhash::row_hashes(&mut conn, db, table, &pk_meta, mode, chunked_c, selected)
                    .await?;
            for chunk in rows.chunks(MAX_ROWHASHES_PER_BATCH) {
                let items = chunk
                    .iter()
                    .map(|r| proto::RowHash {
                        bucket: r.bucket.clone(),
                        pk: r.pk.clone(),
                        hash: r.hash.clone(),
                    })
                    .collect();
                proto::write_response(send, &Response::RowHashes { items }, f).await?;
            }
            send_response(send, Response::ok("row hashes done"), f).await?;
            return Ok(());
        }

        if source == HashSource::Dump {
            // Export/Parser-Pfad: ganzer gefilterter Bereich als Stream,
            // Zeilen-Hashes direkt batchweise rausschreiben.
            let algo = state.cfg.hash_algo();
            let mut batch: Vec<proto::RowHash> = Vec::with_capacity(MAX_ROWHASHES_PER_BATCH);
            let mut rx = export::spawn_fetch_with_strategies(
                &state.cfg,
                db.to_string(),
                table.to_string(),
                bucket_filter.clone(),
                Some(mysqlutil::order_by(&pk_meta)),
                server_export_strategies(state, db, table).await?,
            );
            while let Some(msg) = rx.recv().await {
                match msg {
                    export::RowMsg::Row(values) => {
                        let bucket = match hash::chunk_key_for_row(
                            &values,
                            &pk_indexes,
                            &pk_meta,
                            mode,
                            chunked_c,
                        ) {
                            Ok(b) => b,
                            Err(e) => {
                                send_response(send, Response::err(format!("{e:#}")), f).await?;
                                return Ok(());
                            }
                        };
                        if filter.as_ref().map(|f| f.contains(&bucket)).unwrap_or(true) {
                            let pk = hash::pk_canonical(&values, &pk_indexes, &pk_meta);
                            batch.push(proto::RowHash {
                                bucket,
                                pk,
                                hash: hash::row_hash(&values, algo),
                            });
                        }
                        if batch.len() >= MAX_ROWHASHES_PER_BATCH {
                            proto::write_response(
                                send,
                                &Response::RowHashes {
                                    items: std::mem::take(&mut batch),
                                },
                                f,
                            )
                            .await?;
                        }
                    }
                    export::RowMsg::Err(e) => {
                        send_response(send, Response::err(format!("{e}")), f).await?;
                        return Ok(());
                    }
                    export::RowMsg::Done => break,
                }
            }
            if !batch.is_empty() {
                proto::write_response(send, &Response::RowHashes { items: batch }, f).await?;
            }
            send_response(send, Response::ok("row hashes done"), f).await?;
            return Ok(());
        }

        const PAGE_ROWS: usize = 100_000;
        let mut row_err: Option<anyhow::Error> = None;
        let mut last_pk: Option<String> = None;
        loop {
            // WHERE = Bucket-Filter + optional PK-Keyset
            let mut where_parts: Vec<String> = Vec::new();
            if let Some(bf) = &bucket_filter {
                where_parts.push(bf.clone());
            }
            if let Some(lp) = &last_pk {
                where_parts.push(mysqlutil::pk_gt_where(&pk_meta, lp)?);
            }
            let where_clause = if where_parts.is_empty() {
                None
            } else {
                Some(where_parts.join(" AND "))
            };

            let mut batch: Vec<proto::RowHash> = Vec::with_capacity(PAGE_ROWS);
            let mut page_full = false;
            let mut newest_pk: Option<String> = last_pk.clone();

            mysqlutil::scan_table(
                &mut conn,
                db,
                table,
                where_clause.as_deref(),
                true,
                &pk_meta,
                |values| {
                    let pk = hash::pk_canonical(&values, &pk_indexes, &pk_meta);
                    newest_pk = Some(pk.clone());
                    let bucket = match hash::chunk_key_for_row(
                        &values,
                        &pk_indexes,
                        &pk_meta,
                        mode,
                        chunked_c,
                    ) {
                        Ok(b) => b,
                        Err(e) => {
                            row_err = Some(e);
                            return futures::future::ready(false);
                        }
                    };
                    if filter.as_ref().map(|f| f.contains(&bucket)).unwrap_or(true) {
                        let rh = hash::row_hash(&values, state.cfg.hash_algo());
                        if log::log_enabled!(log::Level::Trace) {
                            log::trace!("REMOTE row pk={pk} hash={rh}");
                        }
                        batch.push(proto::RowHash {
                            bucket,
                            pk,
                            hash: rh,
                        });
                        if batch.len() >= PAGE_ROWS {
                            page_full = true;
                            return futures::future::ready(false);
                        }
                    }
                    futures::future::ready(true)
                },
            )
            .await?;
            if let Some(e) = row_err {
                send_response(send, Response::err(format!("{e:#}")), f).await?;
                return Ok(());
            }

            let reached_end = !page_full;
            // Page rausschreiben
            for chunk in batch.chunks(MAX_ROWHASHES_PER_BATCH) {
                proto::write_response(
                    send,
                    &Response::RowHashes {
                        items: chunk.to_vec(),
                    },
                    f,
                )
                .await?;
            }
            match &newest_pk {
                Some(pk) if page_full => {
                    last_pk = Some(pk.clone());
                }
                _ => {
                    break;
                }
            }
            if reached_end {
                break;
            }
            let _ = &names;
        }
        send_response(send, Response::ok("row hashes done"), f).await?;
    }
    Ok(())
}
