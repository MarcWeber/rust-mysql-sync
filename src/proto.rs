//! Wire-Protokoll über QUIC-Streams.
//!
//! Framing:  1 Byte Tag | 4 Bytes Länge (BE) | Payload
//!   Tag 1 = JSON-Nachricht (Request / Response)
//!   Tag 2 = Binärdaten (CSV/SQL-Stream-Anteile)
//!
//! Ein Bidirectional-Stream = Sequenz von Request→Response-Paaren.
//! Bulk-Übertragung: DumpBegin/CsvBegin (JSON) → BIN-Frames → DumpEnd/CsvEnd (JSON).

use crate::config::PROTOCOL_VERSION;
use anyhow::{bail, Context, Result};
use quinn::{RecvStream, SendStream};
use serde::{Deserialize, Serialize};

pub const TAG_JSON: u8 = 1;
pub const TAG_BIN: u8 = 2;

/// Größe der BIN-Frames beim CSV/Dump-Upload
pub const BULK_FRAME_SIZE: usize = 256 * 1024;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ChunkHash {
    pub bucket: String,
    pub hash: String,
    pub count: u64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RowHash {
    pub bucket: String,
    pub pk: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Erster Request auf der ersten Stream-Verbindung: Auth + Version
    Hello {
        version: u32,
        token: Option<String>,
    },
    DescribeCount {
        db: String,
        table: String,
    },
    /// CREATE TABLE Statement der Quelle; Server macht DROP IF EXISTS + CREATE
    Recreate {
        db: String,
        sql: String,
    },
    Truncate {
        db: String,
        table: String,
    },
    /// Chunk- oder Zeilen-Hashes vom Server holen.
    /// buckets = Some(filter) -> nur diese Buckets (Drill-Down, ein Scan für alle)
    Hashes {
        db: String,
        table: String,
        primary_keys: Vec<String>,
        chunked: u32,
        hash_of_chunk: bool,
        buckets: Option<Vec<String>>,
    },
    DumpBegin {
        db: String,
        table: String,
    },
    DumpEnd {},
    /// CSV-Import: Spaltenreihenfolge + welche Spalten Hex-kodiert sind
    CsvBegin {
        db: String,
        table: String,
        columns: Vec<String>,
        hex_columns: Vec<String>,
    },
    CsvEnd {},
    /// Primary Keys löschen (kanonische Form, Komponenten mit \x1f joined)
    DeletePks {
        db: String,
        table: String,
        pks: Vec<String>,
    },
    DisableTriggers {
        db: String,
        table: String,
    },
    RestoreTriggers {
        db: String,
        table: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Response {
    Ok {
        message: String,
        #[serde(default)]
        count: Option<u64>,
    },
    Err {
        message: String,
    },
    SchemaInfo {
        create_sql: String,
        columns: Vec<String>,
        count: u64,
    },
    ChunkHashes {
        items: Vec<ChunkHash>,
    },
    RowHashes {
        items: Vec<RowHash>,
    },
}

impl Response {
    pub fn ok(msg: impl Into<String>) -> Response {
        Response::Ok { message: msg.into(), count: None }
    }
    pub fn err(msg: impl Into<String>) -> Response {
        Response::Err { message: msg.into() }
    }
}

pub async fn write_frame(w: &mut SendStream, tag: u8, data: &[u8], max_frame: usize) -> Result<()> {
    if data.len() > max_frame {
        bail!("Frame zu groß: {} > {} Bytes", data.len(), max_frame);
    }
    let mut hdr = [0u8; 5];
    hdr[0] = tag;
    hdr[1..].copy_from_slice(&(data.len() as u32).to_be_bytes());
    w.write_all(&hdr).await?;
    if !data.is_empty() {
        w.write_all(data).await?;
    }
    Ok(())
}

pub async fn write_json(w: &mut SendStream, msg: &impl Serialize, max_frame: usize) -> Result<()> {
    let data = serde_json::to_vec(msg)?;
    write_frame(w, TAG_JSON, &data, max_frame).await
}

/// Liest einen Frame; None = Stream beendet (clean).
pub async fn read_frame(r: &mut RecvStream, max_frame: usize) -> Result<Option<(u8, Vec<u8>)>> {
    let mut hdr = [0u8; 5];
    match r.read_exact(&mut hdr).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let tag = hdr[0];
    let len = u32::from_be_bytes(hdr[1..].try_into().unwrap()) as usize;
    if tag != TAG_JSON && tag != TAG_BIN {
        bail!("Ungültiger Frame-Tag: {tag}");
    }
    if len > max_frame {
        bail!("Frame überschreitet max_frame ({} > {})", len, max_frame);
    }
    let mut data = vec![0u8; len];
    if len > 0 {
        r.read_exact(&mut data)
            .await
            .context("Stream unerwartet beendet mitten im Frame")?;
    }
    Ok(Some((tag, data)))
}

pub async fn read_request(r: &mut RecvStream, max_frame: usize) -> Result<Option<Request>> {
    match read_frame(r, max_frame).await? {
        None => Ok(None),
        Some((TAG_JSON, data)) => Ok(Some(
            serde_json::from_slice(&data).context("Request-JSON ungültig")?,
        )),
        Some((tag, _)) => bail!("Erwartete JSON-Frame, got tag={tag}"),
    }
}

/// Schreibt einen großen Blob als Serie von BIN-Frames.
pub async fn write_bulk(w: &mut SendStream, mut data: &[u8], max_frame: usize) -> Result<()> {
    while !data.is_empty() {
        let n = data.len().min(BULK_FRAME_SIZE);
        write_frame(w, TAG_BIN, &data[..n], max_frame).await?;
        data = &data[n..];
    }
    Ok(())
}

/// Prüft ob die letzte Response eine finale Ok/Err ist.
pub fn is_final(resp: &Response) -> bool {
    matches!(resp, Response::Ok { .. } | Response::Err { .. })
}

pub fn version() -> u32 {
    PROTOCOL_VERSION
}
