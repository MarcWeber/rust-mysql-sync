//! Wire protocol over QUIC streams (or plaintext TCP).
//!
//! Frame: `U<len>\n<body>` or `C<len>\n<body>`
//!   `U` = uncompressed, `C` = zstd-compressed
//!   `<len>` = decimal byte length of the following body
//!   `<body>` = rkyv-archived `Request` (client→server) or `Response` (server→client)
//!
//! One bidirectional stream = sequence of request→response pairs.
//! Bulk transfer: DumpBegin/CsvBegin → `Bulk` frames → DumpEnd/CsvEnd.

use crate::config::{Config, PROTOCOL_VERSION};
use anyhow::{bail, Context, Result};
use rkyv::rancor;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Size of the `Bulk` frames during CSV/dump upload.
pub const BULK_FRAME_SIZE: usize = 256 * 1024;

const MAX_HEADER_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    None,
    Zstd,
}

#[derive(Debug, Clone, Copy)]
pub struct Framing {
    pub max_frame: usize,
    pub max_decompressed: usize,
    pub compression: Compression,
    pub level: i32,
}

impl Framing {
    pub fn from_cfg(cfg: &Config) -> Self {
        Self {
            max_frame: cfg.max_frame,
            max_decompressed: cfg.max_decompressed,
            compression: if cfg.compression.eq_ignore_ascii_case("zstd") {
                Compression::Zstd
            } else {
                Compression::None
            },
            level: cfg.compression_level,
        }
    }
}

/// Per-column hashing option, sent with `Hashes` so client and server apply the
/// same transformation. Ordinals refer to the `SELECT *` column order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub enum FieldTransform {
    Trim,
    Lower,
    Ignore,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct FieldOpt {
    pub ordinal: u32,
    pub transform: Option<FieldTransform>,
    /// Round FLOAT/DOUBLE to this many decimals before hashing.
    pub float_precision: Option<u8>,
}

/// Look up a field option by column ordinal.
pub fn field_opt(opts: &[FieldOpt], ordinal: usize) -> Option<&FieldOpt> {
    opts.iter().find(|o| o.ordinal as usize == ordinal)
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct ChunkHash {
    pub bucket: String,
    pub hash: String,
    pub count: u64,
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub struct RowHash {
    pub bucket: String,
    /// Primary-key components in PK order. Structured (not a joined string) so
    /// rkyv can preserve/expose them without a custom codec.
    pub pk: Vec<String>,
    pub hash: String,
}

/// Representation profile that must match exactly before raw row hashing is
/// allowed. Raw fast paths hash bytes from the MySQL wire protocol, so any
/// difference here can silently change the hashed representation.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
pub struct MysqlProfile {
    pub version: String,
    pub version_comment: String,
    pub version_compile_os: String,
    pub version_compile_machine: String,
    pub character_set_client: String,
    pub character_set_connection: String,
    pub character_set_results: String,
    pub collation_connection: String,
    pub time_zone: String,
    pub sql_mode: String,
}

impl MysqlProfile {
    pub fn from_archived(a: &ArchivedMysqlProfile) -> Self {
        Self {
            version: a.version.as_str().to_owned(),
            version_comment: a.version_comment.as_str().to_owned(),
            version_compile_os: a.version_compile_os.as_str().to_owned(),
            version_compile_machine: a.version_compile_machine.as_str().to_owned(),
            character_set_client: a.character_set_client.as_str().to_owned(),
            character_set_connection: a.character_set_connection.as_str().to_owned(),
            character_set_results: a.character_set_results.as_str().to_owned(),
            collation_connection: a.collation_connection.as_str().to_owned(),
            time_zone: a.time_zone.as_str().to_owned(),
            sql_mode: a.sql_mode.as_str().to_owned(),
        }
    }

    pub fn difference(&self, other: &MysqlProfile) -> Option<String> {
        let mut diffs = Vec::new();
        macro_rules! check {
            ($field:ident, $name:literal) => {
                if self.$field != other.$field {
                    diffs.push(format!(
                        "{}: client={:?}, server={:?}",
                        $name, self.$field, other.$field
                    ));
                }
            };
        }
        check!(version, "VERSION()");
        check!(version_comment, "@@version_comment");
        check!(version_compile_os, "@@version_compile_os");
        check!(version_compile_machine, "@@version_compile_machine");
        self.session_difference_into(other, &mut diffs);
        (!diffs.is_empty()).then(|| diffs.join("; "))
    }

    /// Difference that may be ignored when rows are decoded into canonical
    /// `Value`s before hashing. Version/build fields do not affect the decoded
    /// representation, but session charset/timezone/sql_mode still do.
    pub fn difference_ignoring_version(&self, other: &MysqlProfile) -> Option<String> {
        let mut diffs = Vec::new();
        self.session_difference_into(other, &mut diffs);
        (!diffs.is_empty()).then(|| diffs.join("; "))
    }

    fn session_difference_into(&self, other: &MysqlProfile, diffs: &mut Vec<String>) {
        macro_rules! check {
            ($field:ident, $name:literal) => {
                if self.$field != other.$field {
                    diffs.push(format!(
                        "{}: client={:?}, server={:?}",
                        $name, self.$field, other.$field
                    ));
                }
            };
        }
        check!(character_set_client, "character_set_client");
        check!(character_set_connection, "character_set_connection");
        check!(character_set_results, "character_set_results");
        check!(collation_connection, "collation_connection");
        check!(time_zone, "time_zone");
        check!(sql_mode, "sql_mode");
    }
}

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Request {
    /// First request: auth + version + hash algorithm.
    Hello {
        version: u32,
        token: Option<String>,
        hash_algo: Option<String>,
    },
    /// Server MySQL version/profile. Used to gate byte-oriented raw hashing.
    MysqlVersion {},
    DescribeCount {
        db: String,
        table: String,
    },
    /// CREATE TABLE statement of the source; server does DROP IF EXISTS + CREATE.
    Recreate {
        db: String,
        sql: String,
    },
    Truncate {
        db: String,
        table: String,
    },
    /// Fetch chunk or row hashes from the server.
    Hashes {
        db: String,
        table: String,
        primary_keys: Vec<String>,
        chunked: u32,
        hash_of_chunk: bool,
        buckets: Option<Vec<String>>,
        /// "select-sha2", "select-crc32", "dump", "raw_binary" or "raw_tsv".
        hash_source: Option<String>,
        /// Required for raw sources: exact client-side MySQL representation profile.
        profile: Option<MysqlProfile>,
        /// Required for raw sources: hash of normalized SHOW CREATE TABLE.
        schema_hash: Option<String>,
        /// Per-column transforms/rounding; must be identical on both sides.
        field_opts: Vec<FieldOpt>,
    },
    DumpBegin {
        db: String,
        table: String,
    },
    /// One chunk of dump SQL / CSV bytes.
    Bulk {
        data: Vec<u8>,
    },
    DumpEnd {},
    /// CSV import: column order + which columns are hex-encoded.
    CsvBegin {
        db: String,
        table: String,
        columns: Vec<String>,
        hex_columns: Vec<String>,
    },
    CsvEnd {},
    /// Delete primary keys (each entry = PK components in PK order).
    DeletePks {
        db: String,
        table: String,
        pks: Vec<Vec<String>>,
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

#[derive(Debug, Clone, Archive, Serialize, Deserialize)]
pub enum Response {
    Ok {
        message: String,
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
    MysqlVersion {
        version: String,
        profile: MysqlProfile,
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
        Response::Ok {
            message: msg.into(),
            count: None,
        }
    }
    pub fn err(msg: impl Into<String>) -> Response {
        Response::Err {
            message: msg.into(),
        }
    }
}

async fn write_encoded<W>(w: &mut W, body: AlignedVec, f: &Framing) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    match f.compression {
        Compression::None => write_body(w, b'U', body.as_slice(), f).await,
        Compression::Zstd => {
            let compressed =
                zstd::bulk::compress(body.as_slice(), f.level).context("zstd compress")?;
            write_body(w, b'C', &compressed, f).await
        }
    }
}

async fn write_body<W>(w: &mut W, flag: u8, data: &[u8], f: &Framing) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    if data.len() > f.max_frame {
        bail!("Frame zu groß: {} > {} Bytes", data.len(), f.max_frame);
    }
    let hdr = format!("{}{}\n", flag as char, data.len());
    w.write_all(hdr.as_bytes()).await?;
    if !data.is_empty() {
        w.write_all(data).await?;
    }
    Ok(())
}

pub async fn write_request<W>(w: &mut W, msg: &Request, f: &Framing) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let body = rkyv::to_bytes::<rancor::Error>(msg).context("rkyv serialize")?;
    write_encoded(w, body, f).await
}

pub async fn write_response<W>(w: &mut W, msg: &Response, f: &Framing) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let body = rkyv::to_bytes::<rancor::Error>(msg).context("rkyv serialize")?;
    write_encoded(w, body, f).await
}

async fn read_body<R>(r: &mut R, f: &Framing) -> Result<Option<AlignedVec>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let first = match r.read_u8().await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if first != b'U' && first != b'C' {
        bail!("Ungültiges Frame-Flag: 0x{first:02x}");
    }
    let mut digits = Vec::with_capacity(10);
    loop {
        let b = r.read_u8().await.context("Stream endete im Frame-Header")?;
        if b == b'\n' {
            break;
        }
        if !b.is_ascii_digit() || digits.len() >= MAX_HEADER_LEN {
            bail!("Ungültige Frame-Länge");
        }
        digits.push(b);
    }
    if digits.is_empty() {
        bail!("Leere Frame-Länge");
    }
    let len: usize = std::str::from_utf8(&digits)
        .ok()
        .and_then(|s| s.parse().ok())
        .context("Frame-Länge ungültig")?;
    if len > f.max_frame {
        bail!("Frame überschreitet max_frame ({} > {})", len, f.max_frame);
    }

    let mut raw = AlignedVec::with_capacity(len);
    raw.resize(len, 0);
    if len > 0 {
        r.read_exact(&mut raw[..])
            .await
            .context("Stream unerwartet beendet mitten im Frame")?;
    }

    if first == b'U' {
        return Ok(Some(raw));
    }
    let decompressed =
        zstd::bulk::decompress(&raw, f.max_decompressed).context("zstd decompress")?;
    let mut out = AlignedVec::with_capacity(decompressed.len());
    out.extend_from_slice(&decompressed);
    Ok(Some(out))
}

/// Reads one raw frame. The caller validates it with [`access_request`] and
/// works on the archived view, avoiding an owned copy.
pub async fn read_request_bytes<R>(r: &mut R, f: &Framing) -> Result<Option<AlignedVec>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_body(r, f).await
}

/// See [`read_request_bytes`].
pub async fn read_response_bytes<R>(r: &mut R, f: &Framing) -> Result<Option<AlignedVec>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    read_body(r, f).await
}

pub fn access_request(buf: &[u8]) -> Result<&ArchivedRequest> {
    rkyv::access::<ArchivedRequest, rancor::Error>(buf).context("Request-Binary ungültig")
}

pub fn access_response(buf: &[u8]) -> Result<&ArchivedResponse> {
    rkyv::access::<ArchivedResponse, rancor::Error>(buf).context("Response-Binary ungültig")
}

/// Writes a large blob as a series of `Bulk` frames.
pub async fn write_bulk<W>(w: &mut W, mut data: &[u8], f: &Framing) -> Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    if data.is_empty() {
        write_request(w, &Request::Bulk { data: Vec::new() }, f).await?;
        return Ok(());
    }
    while !data.is_empty() {
        let n = data.len().min(BULK_FRAME_SIZE);
        write_request(
            w,
            &Request::Bulk {
                data: data[..n].to_vec(),
            },
            f,
        )
        .await?;
        data = &data[n..];
    }
    Ok(())
}

/// True if the last response is a final Ok/Err.
pub fn is_final(resp: &Response) -> bool {
    matches!(resp, Response::Ok { .. } | Response::Err { .. })
}

pub fn version() -> u32 {
    PROTOCOL_VERSION
}
