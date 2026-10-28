//! mysql-sync Server: QUIC-Listener mit MySQL-Zugriff.
//!
//! Ein Bidirectional-Stream = state machine:
//!   Normal: Hello/DescribeCount/Recreate/Truncate/Hashes/DeletePks/Triggers
//!   Dump:   DumpBegin (JSON) → BIN-Frames → DumpEnd (JSON) → Ausführen
//!   Csv:    CsvBegin (JSON) → BIN-Frames → CsvEnd (JSON) → Import

use crate::config::Config;
use crate::hash;
use crate::import::{self, TsvBatcher};
use crate::mysqlutil::{self, qualified};
use crate::proto::{self, Request, Response};
use crate::tls;
use anyhow::{Context, Result};
use mysql_async::prelude::*;
use mysql_async::Pool;
use quinn::Connection;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const MAX_BUCKETS_PER_BATCH: usize = 512;
const MAX_ROWHASHES_PER_BATCH: usize = 1024;
const MAX_DUMP_BUFFER: u64 = 2 * 1024 * 1024 * 1024; // 2 GB Guard

pub async fn run(cfg: Config) -> Result<std::process::ExitCode> {
    let config_path = std::env::args()
        .position(|a| a == "-C" || a == "--config")
        .and_then(|i| std::env::args().nth(i + 1))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("config-server.json"));

    let (certs, key, fingerprint) = tls::server_identity(&cfg, &config_path)?;
    let pool = mysqlutil::new_pool(&cfg.mysql, None)?;

    let provider = tls::ring_provider();
    let mut server_crypto = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .context("TLS-Protokoll-Versionen")?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("TLS-Zertifikat/Key ungültig")?;
    server_crypto.alpn_protocols = vec![crate::config::ALPN.to_vec()];

    let quinn_cfg = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(server_crypto)
            .context("QUIC-TLS-Konfiguration")?,
    ));
    let addr = cfg.listen_addr()?;
    let endpoint =
        quinn::Endpoint::server(quinn_cfg, addr).with_context(|| format!("QUIC-Listener auf {addr}"))?;

    eprintln!("mysql-sync Server lauscht auf {addr}");
    eprintln!("TLS-Fingerprint: {fingerprint}");
    eprintln!("→ Fingerprints in config-client.json → \"server_fingerprint\" (Pinning empfohlen)");
    log::info!(
        "chunked={} ({} Buckets), dump_threshold={}, diff_threshold_percent={}%",
        cfg.chunked, 10u64.pow(cfg.chunked), cfg.dump_threshold, cfg.diff_threshold_percent
    );

    let state = Arc::new(ServerState { cfg, pool });

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
            if let Err(e) = handle_stream(send, recv, state, hello_done).await {
                log::error!("Stream-Fehler: {e:#}");
            }
        });
    }
}

enum Mode {
    Normal,
    Dump { db: String, table: String, buf: Vec<u8> },
    Csv { db: String, table: String, batcher: TsvBatcher, counter: u64, err: Option<String> },
}

async fn handle_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    state: Arc<ServerState>,
    hello_done: Arc<AtomicBool>,
) -> Result<()> {
    let max_frame = state.cfg.max_frame;
    let mut mode = Mode::Normal;
    let mut trigger_backup: HashMap<(String, String), Vec<import::TriggerDef>> = HashMap::new();

    while let Some(frame) = proto::read_frame(&mut recv, max_frame).await? {
        let (tag, data) = frame;

        // --- Zustände mit Binärdaten ---
        match &mut mode {
            Mode::Dump { db, table, buf } => {
                match tag {
                    proto::TAG_BIN => {
                        buf_len_guard(buf)?;
                        buf.extend_from_slice(&data);
                    }
                    proto::TAG_JSON => {
                        let req: Request = serde_json::from_slice(&data)?;
                        if !matches!(req, Request::DumpEnd { .. }) {
                            let _ = (db, table);
                            send_response(&mut send, Response::err("Erwartet DumpEnd"), max_frame).await?;
                            mode = Mode::Normal;
                            continue;
                        }
                        let sql = String::from_utf8_lossy(buf).to_string();
                        let resp = match mysqlutil::session_conn(&state.pool).await {
                            Ok(mut conn) => match import::execute_dump(&mut conn, &sql).await {
                                Ok(n) => Response::Ok { message: "dump executed".into(), count: Some(n) },
                                Err(e) => Response::err(format!("{e:#}")),
                            },
                            Err(e) => Response::err(format!("{e:#}")),
                        };
                        let failed = matches!(resp, Response::Err { .. });
                        send_response(&mut send, resp, max_frame).await?;
                        mode = Mode::Normal;
                        if failed {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
                continue;
            }
            Mode::Csv { db, table, batcher, counter, err } => {
                match tag {
                    proto::TAG_BIN => {
                        if err.is_none() {
                            match batcher.push(&data) {
                                Ok(rows) => {
                                    if !rows.is_empty() {
                                        match mysqlutil::session_conn(&state.pool).await {
                                            Ok(mut conn) => {
                                                match import::import_rows(&mut conn, db, table, &batcher.columns, rows).await {
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
                    proto::TAG_JSON => {
                        let req: Request = serde_json::from_slice(&data)?;
                        if !matches!(req, Request::CsvEnd { .. }) {
                            send_response(&mut send, Response::err("Erwartet CsvEnd"), max_frame).await?;
                            mode = Mode::Normal;
                            continue;
                        }
                        // Rest flushen
                        let mut resp = if let Some(e) = err.take() {
                            Response::err(e)
                        } else {
                            let rows = batcher.finish()?;
                            match mysqlutil::session_conn(&state.pool).await {
                                Ok(mut conn) => match import::import_rows(&mut conn, db, table, &batcher.columns, rows).await {
                                    Ok(n) => {
                                        *counter += n;
                                        Response::Ok { message: "csv imported".into(), count: Some(*counter) }
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
                        send_response(&mut send, resp, max_frame).await?;
                        mode = Mode::Normal;
                        if failed {
                            return Ok(());
                        }
                    }
                    _ => {}
                }
                continue;
            }
            Mode::Normal => {}
        }

        // --- Normalmodus: JSON-Request ---
        if tag != proto::TAG_JSON {
            send_response(&mut send, Response::err("Erwartete JSON-Request"), max_frame).await?;
            return Ok(());
        }
        let req: Request = serde_json::from_slice(&data).context("Request-JSON ungültig")?;

        let is_hello = matches!(req, Request::Hello { .. });
        if !is_hello && !hello_done.load(Ordering::SeqCst) {
            send_response(&mut send, Response::err("Erster Request muss Hello sein (Auth)"), max_frame).await?;
            return Ok(());
        }

        let resp = match req {
            Request::Hello { version, token } => {
                let r = handle_hello(state.as_ref(), version, &token).await;
                if matches!(r, Response::Ok { .. }) {
                    hello_done.store(true, Ordering::SeqCst);
                }
                r
            }
            Request::DescribeCount { db, table } => {
                handle_describe_count(state.as_ref(), &db, &table).await
            }
            Request::Recreate { db, sql } => {
                handle_recreate(state.as_ref(), &db, &sql).await
            }
            Request::Truncate { db, table } => {
                handle_truncate(state.as_ref(), &db, &table).await
            }
            Request::Hashes { db, table, primary_keys, chunked, hash_of_chunk, buckets } => {
                // Streaming: schreibt alle Responses inkl. Final selbst
                if let Err(e) = do_hashes(
                    state.as_ref(),
                    &db,
                    &table,
                    primary_keys,
                    chunked,
                    hash_of_chunk,
                    buckets,
                    &mut send,
                    max_frame,
                )
                .await
                {
                    send_response(&mut send, Response::err(format!("{e:#}")), max_frame).await?;
                    return Ok(());
                }
                continue;
            }
            Request::DumpBegin { db, table } => {
                mode = Mode::Dump { db, table, buf: Vec::new() };
                continue;
            }
            Request::CsvBegin { db, table, columns, hex_columns } => {
                mode = Mode::Csv { db, table, batcher: TsvBatcher::new(columns, &hex_columns), counter: 0, err: None };
                continue;
            }
            Request::DeletePks { db, table, pks } => {
                match mysqlutil::session_conn(&state.pool).await {
                    Ok(mut conn) => match import::delete_pks(&mut conn, &db, &table, &pks).await {
                        Ok(n) => Response::Ok { message: format!("deleted {n}"), count: Some(n) },
                        Err(e) => Response::err(format!("{e:#}")),
                    },
                    Err(e) => Response::err(format!("{e:#}")),
                }
            }
            Request::DisableTriggers { db, table } => {
                match mysqlutil::session_conn(&state.pool).await {
                    Ok(mut conn) => match import::snapshot_and_drop_triggers(&mut conn, &db, &table).await {
                        Ok(defs) => {
                            trigger_backup.insert((db.clone(), table.clone()), defs);
                            Response::ok("triggers disabled")
                        }
                        Err(e) => Response::err(format!("{e:#}")),
                    },
                    Err(e) => Response::err(format!("{e:#}")),
                }
            }
            Request::RestoreTriggers { db, table } => {
                let defs = trigger_backup.remove(&(db.clone(), table.clone())).unwrap_or_default();
                match mysqlutil::session_conn(&state.pool).await {
                    Ok(mut conn) => match import::restore_triggers(&mut conn, &db, &defs).await {
                        Ok(()) => Response::ok("triggers restored"),
                        Err(e) => Response::err(format!("{e:#}")),
                    },
                    Err(e) => Response::err(format!("{e:#}")),
                }
            }
            Request::DumpEnd { .. } | Request::CsvEnd { .. } => {
                Response::err("Unerwarteter Request in diesem Zustand")
            }
        };

        // App-level Err-Antworten sind Teil des Protokolls (z.B. "Tabelle fehlt"
        // vor --force) — der Stream bleibt offen. Nur Protokollverletzungen
        // (unbekannte Frames, Gate-Failure) beenden den Stream vorzeitig.
        send_response(&mut send, resp, max_frame).await?;
    }
    Ok(())
}

fn buf_len_guard(buf: &Vec<u8>) -> Result<()> {
    if buf.len() as u64 > MAX_DUMP_BUFFER {
        anyhow::bail!("Dump-Buffer überschritten ({} Bytes Limit)", MAX_DUMP_BUFFER);
    }
    Ok(())
}

async fn send_response(send: &mut quinn::SendStream, resp: Response, max_frame: usize) -> Result<()> {
    proto::write_json(send, &resp, max_frame).await?;
    Ok(())
}

async fn handle_hello(state: &ServerState, version: u32, token: &Option<String>) -> Response {
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
                conn.query_drop(format!("DROP TABLE IF EXISTS {}", qualified(db, &target_table)))
                    .await
                    .context("DROP TABLE fehlgeschlagen")?;
                conn.query_drop(qualified_sql).await.context("CREATE TABLE fehlgeschlagen")?;
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
    let Some(pos) = lower.find("table") else { return sql.to_string() };
    let rest = sql[pos + 5..].trim_start();
    let rest = if rest.to_lowercase().starts_with("if not exists") {
        rest[13..].trim_start()
    } else {
        rest
    };
    let Some(paren) = rest.find('(') else { return sql.to_string() };
    // Kopf ("CREATE ") + "TABLE " + qualifizierter Name + Spaltenteil ab "("
    format!("{}TABLE {}{}", &sql[..pos], qualified(db, table), &rest[paren..])
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
        let token: String = rest.chars().take_while(|c| !c.is_whitespace() && *c != '(').collect();
        return token.trim_matches('`').to_string();
    }
    String::new()
}

async fn handle_truncate(state: &ServerState, db: &str, table: &str) -> Response {
    match mysqlutil::session_conn(&state.pool).await {
        Ok(mut conn) => {
            let r: Result<()> = async {
                if let Err(e) = conn.query_drop(format!("TRUNCATE TABLE {}", qualified(db, table))).await {
                    log::warn!("TRUNCATE fehlgeschlagen ({e}) — versuche DELETE FROM");
                    conn.query_drop(format!("DELETE FROM {}", qualified(db, table))).await?;
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

#[allow(clippy::too_many_arguments)]
async fn do_hashes(
    state: &ServerState,
    db: &str,
    table: &str,
    primary_keys: Vec<String>,
    chunked: u32,
    hash_of_chunk: bool,
    buckets: Option<Vec<String>>,
    send: &mut quinn::SendStream,
    max_frame: usize,
) -> Result<()> {
    let mut conn = mysqlutil::session_conn(&state.pool).await?;
    let pk_meta = mysqlutil::pk_columns(&mut conn, db, table).await?;
    if pk_meta.is_empty() {
        send_response(send, Response::err(format!("{db}.{table} hat keinen Primary Key")), max_frame).await?;
        return Ok(());
    }
    let server_pks: Vec<String> = pk_meta.iter().map(|p| p.name.clone()).collect();
    if server_pks != primary_keys {
        send_response(
            send,
            Response::err(format!(
                "Primary Keys weichen ab: Server={server_pks:?} Client={primary_keys:?}"
            )),
            max_frame,
        )
        .await?;
        return Ok(());
    }
    let names = mysqlutil::column_names(&mut conn, db, table).await?;
    let mut pk_indexes = Vec::new();
    for pk in &primary_keys {
        match names.iter().position(|n| n == pk) {
            Some(i) => pk_indexes.push(i),
            None => {
                send_response(send, Response::err(format!("PK-Spalte {pk} fehlt")), max_frame).await?;
                return Ok(());
            }
        }
    }

    let filter: Option<HashSet<String>> = buckets.map(|b| b.into_iter().collect());
    let chunked_c = chunked.clamp(1, 6);

    if hash_of_chunk {
        // Ein Scan über die Tabelle; pro Bucket Hasher + Count.
        let mut acc: HashMap<String, (blake3::Hasher, u64)> = HashMap::new();
        mysqlutil::scan_table(&mut conn, db, table, None, true, &pk_meta, |values| {
            let comps: Vec<String> =
                pk_indexes.iter().map(|&i| hash::value_to_pk_component(&values[i])).collect();
            let bucket = hash::bucket_of_pk(&comps, pk_meta.last().map(|p| p.numeric).unwrap_or(false), chunked_c);
            let rh = hash::row_hash(&values);
            let e = acc.entry(bucket).or_insert_with(|| (blake3::Hasher::new(), 0));
            e.0.update(&hex::decode(&rh).unwrap());
            e.1 += 1;
            futures::future::ready(true)
        })
        .await?;

        let mut items: Vec<proto::ChunkHash> = acc
            .into_iter()
            .filter(|(b, _)| filter.as_ref().map(|f| f.contains(b)).unwrap_or(true))
            .map(|(b, (h, c))| {
                let final_hash = hash::finalize_chunk(&b, c, &h);
                proto::ChunkHash { bucket: b, hash: final_hash, count: c }
            })
            .collect();
        items.sort_by(|a, b| a.bucket.cmp(&b.bucket));
        log::info!("[{db}.{table}] {} Bucket-Hashes berechnet", items.len());

        let total = items.len();
        for chunk in items.chunks(MAX_BUCKETS_PER_BATCH) {
            proto::write_json(send, &Response::ChunkHashes { items: chunk.to_vec() }, max_frame).await?;
        }
        send_response(
            send,
            Response::Ok { message: format!("{total} buckets"), count: Some(total as u64) },
            max_frame,
        )
        .await?;
    } else {
        // Zeilen-Hashes für bestimmte Buckets — Keyset-Paginierung über PK,
        // damit der Speicher begrenzt bleibt (Page für Page, Schreiben dazwischen).
        let bucket_list: Vec<String> =
            filter.as_ref().map(|f| f.iter().cloned().collect()).unwrap_or_default();
        let bucket_filter = if bucket_list.is_empty() {
            None
        } else {
            Some(mysqlutil::bucket_where(&pk_meta, chunked_c, &bucket_list)?)
        };
        let names = mysqlutil::column_names(&mut conn, db, table).await?;

        const PAGE_ROWS: usize = 100_000;
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

            let numeric_last = pk_meta.last().map(|p| p.numeric).unwrap_or(false);
            mysqlutil::scan_table(&mut conn, db, table, where_clause.as_deref(), true, &pk_meta, |values| {
                let pk = hash::pk_canonical(&values, &pk_indexes);
                newest_pk = Some(pk.clone());
                let comps: Vec<String> = pk_indexes
                    .iter()
                    .map(|&i| hash::value_to_pk_component(&values[i]))
                    .collect();
                let bucket = hash::bucket_of_pk(&comps, numeric_last, chunked_c);
                if filter.as_ref().map(|f| f.contains(&bucket)).unwrap_or(true) {
                    let rh = hash::row_hash(&values);
                    if log::log_enabled!(log::Level::Trace) {
                        log::trace!("REMOTE row pk={pk} hash={rh}");
                    }
                    batch.push(proto::RowHash { bucket, pk, hash: rh });
                    if batch.len() >= PAGE_ROWS {
                        page_full = true;
                        return futures::future::ready(false);
                    }
                }
                futures::future::ready(true)
            })
            .await?;

            let reached_end = !page_full;
            // Page rausschreiben
            for chunk in batch.chunks(MAX_ROWHASHES_PER_BATCH) {
                proto::write_json(send, &Response::RowHashes { items: chunk.to_vec() }, max_frame).await?;
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
        send_response(send, Response::ok("row hashes done"), max_frame).await?;
    }
    Ok(())
}
