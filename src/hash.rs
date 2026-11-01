//! Kanonische Zeilen-Kodierung + Hashing + TSV-Dialekt (LOAD-DATA-kompatibel).
//!
//! Kanonisch = Typ-Tag + Rohbytes je Feld (mysql_async Value-Repräsentation).
//! Beide Seiten (Client/Server) nutzen denselben Parser (select-stream), dadurch
//! sind Hashes unabhängig von der gewählten Export-Strategie identisch.

use crate::mysqlutil::{PkCol, PkKind};
use anyhow::{anyhow, bail, Context, Result};
use digest::Digest;
use mysql_async::Value;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// Automatisch gewaehlter Chunk-Modus (keine User-Konfiguration).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkMode {
    /// Contiguous PK ranges: numeric `pk / W`, char/binary trim the last N chars/bytes.
    Range,
    /// Fallback: last N chars / ABS(pk) % 10^N buckets.
    Mod,
}

/// Range nur fuer genau einen dafuer unterstuetzten PK; sonst Mod-Fallback.
pub fn chunk_mode_for_pk(pks: &[PkCol]) -> ChunkMode {
    if pks.len() == 1 && pks[0].is_range_supported() {
        ChunkMode::Range
    } else {
        ChunkMode::Mod
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HashAlgo {
    Xxh3,
    Xxh64,
    Blake3,
    Md5,
    Sha256,
}

impl Default for HashAlgo {
    fn default() -> Self {
        HashAlgo::Xxh3
    }
}

impl HashAlgo {
    /// Ob der Algorithmus in diesem Build einkompiliert ist.
    pub fn is_enabled(self) -> bool {
        match self {
            HashAlgo::Xxh3 | HashAlgo::Xxh64 => cfg!(feature = "xxhash"),
            HashAlgo::Blake3 => cfg!(feature = "blake3"),
            HashAlgo::Md5 => cfg!(feature = "md5"),
            HashAlgo::Sha256 => cfg!(feature = "sha2"),
        }
    }
}

impl fmt::Display for HashAlgo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashAlgo::Xxh3 => write!(f, "xxh3"),
            HashAlgo::Xxh64 => write!(f, "xxh64"),
            HashAlgo::Blake3 => write!(f, "blake3"),
            HashAlgo::Md5 => write!(f, "md5"),
            HashAlgo::Sha256 => write!(f, "sha256"),
        }
    }
}

impl FromStr for HashAlgo {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        let t = s.trim().to_lowercase().replace('-', "_");
        match t.as_str() {
            "xxh3" | "xxhash3" | "xxh" => Ok(HashAlgo::Xxh3),
            "xxh64" | "xxhash64" => Ok(HashAlgo::Xxh64),
            "blake3" | "blake" => Ok(HashAlgo::Blake3),
            "md5" => Ok(HashAlgo::Md5),
            "sha256" | "sha_256" | "sha2" => Ok(HashAlgo::Sha256),
            _ => Err(anyhow!(
                "Unbekannter Hash-Algorithmus: {s} (erlaubt: xxh3, xxh64, blake3, md5, sha256)"
            )),
        }
    }
}

pub enum RowHasher {
    #[cfg(feature = "xxhash")]
    Xxh3(xxhash_rust::xxh3::Xxh3),
    #[cfg(feature = "xxhash")]
    Xxh64(xxhash_rust::xxh64::Xxh64),
    #[cfg(feature = "blake3")]
    Blake3(blake3::Hasher),
    #[cfg(feature = "md5")]
    Md5(md5::Md5),
    #[cfg(feature = "sha2")]
    Sha256(sha2::Sha256),
}

impl RowHasher {
    #[allow(unreachable_patterns, unreachable_code)]
    pub fn new(algo: HashAlgo) -> Self {
        match algo {
            #[cfg(feature = "xxhash")]
            HashAlgo::Xxh3 => return RowHasher::Xxh3(xxhash_rust::xxh3::Xxh3::new()),
            #[cfg(feature = "xxhash")]
            HashAlgo::Xxh64 => return RowHasher::Xxh64(xxhash_rust::xxh64::Xxh64::new(0)),
            #[cfg(feature = "blake3")]
            HashAlgo::Blake3 => return RowHasher::Blake3(blake3::Hasher::new()),
            #[cfg(feature = "md5")]
            HashAlgo::Md5 => return RowHasher::Md5(md5::Md5::new()),
            #[cfg(feature = "sha2")]
            HashAlgo::Sha256 => return RowHasher::Sha256(sha2::Sha256::new()),
            _ => {}
        }
        #[cfg(feature = "xxhash")]
        {
            return RowHasher::Xxh3(xxhash_rust::xxh3::Xxh3::new());
        }
        #[cfg(all(not(feature = "xxhash"), feature = "blake3"))]
        {
            return RowHasher::Blake3(blake3::Hasher::new());
        }
        #[cfg(all(not(feature = "xxhash"), not(feature = "blake3"), feature = "md5"))]
        {
            return RowHasher::Md5(md5::Md5::new());
        }
        #[cfg(all(
            not(feature = "xxhash"),
            not(feature = "blake3"),
            not(feature = "md5"),
            feature = "sha2"
        ))]
        {
            return RowHasher::Sha256(sha2::Sha256::new());
        }
        let _ = ();
        #[cfg(all(
            not(feature = "xxhash"),
            not(feature = "blake3"),
            not(feature = "md5"),
            not(feature = "sha2")
        ))]
        {
            panic!("no hash algorithm enabled");
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        match self {
            #[cfg(feature = "xxhash")]
            RowHasher::Xxh3(h) => h.update(data),
            #[cfg(feature = "xxhash")]
            RowHasher::Xxh64(h) => h.update(data),
            #[cfg(feature = "blake3")]
            RowHasher::Blake3(h) => {
                h.update(data);
            }
            #[cfg(feature = "md5")]
            RowHasher::Md5(h) => {
                h.update(data);
            }
            #[cfg(feature = "sha2")]
            RowHasher::Sha256(h) => {
                h.update(data);
            }
        }
    }

    pub fn finalize_hex(self) -> String {
        match self {
            #[cfg(feature = "xxhash")]
            RowHasher::Xxh3(h) => hex::encode(h.digest().to_le_bytes()),
            #[cfg(feature = "xxhash")]
            RowHasher::Xxh64(h) => hex::encode(h.digest().to_le_bytes()),
            #[cfg(feature = "blake3")]
            RowHasher::Blake3(h) => h.finalize().to_hex().to_string(),
            #[cfg(feature = "md5")]
            RowHasher::Md5(h) => hex::encode(h.finalize()),
            #[cfg(feature = "sha2")]
            RowHasher::Sha256(h) => hex::encode(h.finalize()),
        }
    }
}

/// Kanonische Kodierung eines Feldwerts in den Hasher.
pub fn hash_field(h: &mut RowHasher, v: &Value) {
    match v {
        Value::NULL => {
            h.update(&[0u8]);
        }
        Value::Bytes(b) => {
            h.update(&[1u8]);
            h.update(&(b.len() as u32).to_le_bytes());
            h.update(b);
        }
        Value::Int(i) => {
            h.update(&[2u8]);
            h.update(&i.to_le_bytes());
        }
        Value::UInt(u) => {
            h.update(&[3u8]);
            h.update(&u.to_le_bytes());
        }
        Value::Float(f) => {
            h.update(&[4u8]);
            h.update(&f.to_le_bytes());
        }
        Value::Double(d) => {
            h.update(&[5u8]);
            h.update(&d.to_le_bytes());
        }
        Value::Date(y, mo, d, h2, mi, s, us) => {
            h.update(&[6u8]);
            h.update(&y.to_le_bytes());
            h.update(&[*mo, *d, *h2, *mi, *s]);
            h.update(&us.to_le_bytes());
        }
        Value::Time(neg, days, h2, mi, s, us) => {
            h.update(&[7u8]);
            h.update(&[*neg as u8]);
            h.update(&days.to_le_bytes());
            h.update(&[*h2, *mi, *s]);
            h.update(&us.to_le_bytes());
        }
    }
}

fn hash_rounded(h: &mut RowHasher, v: f64, scale: u8) {
    let s = format!("{:.*}", scale as usize, v);
    h.update(&[8u8]);
    h.update(&(s.len() as u32).to_le_bytes());
    h.update(s.as_bytes());
}

fn transform_bytes(v: &Value, trim: bool, lower: bool) -> Option<Vec<u8>> {
    let Value::Bytes(b) = v else {
        return None;
    };
    let mut out = if lower {
        String::from_utf8_lossy(b).to_lowercase().into_bytes()
    } else {
        b.clone()
    };
    if trim {
        let start = out.iter().position(|&c| c != b' ').unwrap_or(out.len());
        let end = out
            .iter()
            .rposition(|&c| c != b' ')
            .map_or(start, |i| i + 1);
        out = out[start..end].to_vec();
    }
    Some(out)
}

/// Kanonische Kodierung mit Per-Spalte-Overrides (Trim/Lower/Ignore/Runden).
pub fn hash_field_opts(h: &mut RowHasher, v: &Value, opt: Option<&crate::proto::FieldOpt>) {
    use crate::proto::FieldTransform;
    let Some(opt) = opt else {
        return hash_field(h, v);
    };
    if opt.transform == Some(FieldTransform::Ignore) {
        return;
    }
    let trim = opt.transform == Some(FieldTransform::Trim);
    let lower = opt.transform == Some(FieldTransform::Lower);
    if (trim || lower) && v != &Value::NULL {
        if let Some(bytes) = transform_bytes(v, trim, lower) {
            h.update(&[1u8]);
            h.update(&(bytes.len() as u32).to_le_bytes());
            h.update(&bytes);
            return;
        }
    }
    if let Some(scale) = opt.float_precision {
        match v {
            Value::Float(f) => return hash_rounded(h, *f as f64, scale),
            Value::Double(d) => return hash_rounded(h, *d, scale),
            _ => {}
        }
    }
    hash_field(h, v);
}

/// Hash einer einzelnen Zeile (Werte inkl. PK).
pub fn row_hash(values: &[Value], algo: HashAlgo) -> String {
    row_hash_opts(values, algo, &[])
}

/// Wie `row_hash`, aber mit Per-Spalte-Overrides (Index = Ordinal).
pub fn row_hash_opts(values: &[Value], algo: HashAlgo, opts: &[crate::proto::FieldOpt]) -> String {
    let mut h = RowHasher::new(algo);
    for (i, v) in values.iter().enumerate() {
        hash_field_opts(&mut h, v, crate::proto::field_opt(opts, i));
    }
    h.finalize_hex()
}

pub fn row_hash_default(values: &[Value]) -> String {
    row_hash(values, HashAlgo::default())
}

/// Hash arbitrary bytes with the configured row-hash algorithm.
pub fn hash_bytes(data: &[u8], algo: HashAlgo) -> String {
    let mut h = RowHasher::new(algo);
    h.update(data);
    h.finalize_hex()
}

/// Add a row hash (hex) to a bucket accumulator.
pub fn add_row_hash(
    acc: &mut std::collections::HashMap<String, (RowHasher, u64)>,
    algo: HashAlgo,
    bucket: String,
    rh: &str,
) {
    let e = acc
        .entry(bucket)
        .or_insert_with(|| (RowHasher::new(algo), 0));
    match hex::decode(rh) {
        Ok(bytes) => e.0.update(&bytes),
        Err(_) => e.0.update(rh.as_bytes()),
    }
    e.1 += 1;
}

/// Running chunk hash for a stream whose rows arrive already grouped by bucket.
///
/// The producer is responsible for the ordering. `push` returns the previous
/// bucket when the key changes, so callers can forward every completed chunk
/// without ever materializing the full table.
pub struct ChunkAccumulator {
    algo: HashAlgo,
    current: Option<(String, RowHasher, u64)>,
}

pub type ChunkHashValue = (String, u64, String);

/// Events on a chunk-hash stream. `Error` is used by the client-side producer
/// task; the server writes its own protocol error after the scan returns.
pub enum ChunkHashEvent {
    Item(ChunkHashValue),
    Error(anyhow::Error),
}

impl ChunkAccumulator {
    pub fn new(algo: HashAlgo) -> Self {
        Self {
            algo,
            current: None,
        }
    }

    pub fn push(&mut self, bucket: String, row_hash: &str) -> Option<ChunkHashValue> {
        let mut completed = None;
        if self
            .current
            .as_ref()
            .map(|(b, _, _)| b != &bucket)
            .unwrap_or(false)
        {
            completed = self.finish();
        }
        if self.current.is_none() {
            self.current = Some((bucket.clone(), RowHasher::new(self.algo), 0));
        }
        let (_, hasher, count) = self.current.as_mut().unwrap();
        match hex::decode(row_hash) {
            Ok(bytes) => hasher.update(&bytes),
            Err(_) => hasher.update(row_hash.as_bytes()),
        }
        *count += 1;
        completed
    }

    pub fn finish(&mut self) -> Option<ChunkHashValue> {
        self.current.take().map(|(bucket, hasher, count)| {
            let hash = finalize_chunk(&bucket, count, hasher);
            (bucket, count, hash)
        })
    }
}

/// Finalize per-bucket rolling hashes into `(count, hash)`.
pub fn finish_chunks(
    acc: std::collections::HashMap<String, (RowHasher, u64)>,
) -> std::collections::HashMap<String, (u64, String)> {
    acc.into_iter()
        .map(|(b, (h, c))| {
            let final_hash = finalize_chunk(&b, c, h);
            (b, (c, final_hash))
        })
        .collect()
}

/// Finaler Bucket-Hash: laufender Zeilen-Hash-Hasher + Bucket + Count.
pub fn finalize_chunk(bucket: &str, count: u64, hasher: RowHasher) -> String {
    let mut h = hasher;
    h.update(bucket.as_bytes());
    h.update(&[0u8]);
    h.update(&count.to_le_bytes());
    h.finalize_hex()
}

/// PK-Komponenten in PRIMARY-KEY-Reihenfolge (typabhaengig kanonisiert).
/// BINARY/VARBINARY werden hex-kodiert, damit keine Bytes verloren gehen.
pub fn pk_components(values: &[Value], pk_indexes: &[usize], pks: &[PkCol]) -> Vec<String> {
    let mut parts: Vec<String> = Vec::with_capacity(pk_indexes.len());
    for (pos, &i) in pk_indexes.iter().enumerate() {
        let part = match pks.get(pos) {
            Some(pk) => value_to_pk_component_for(&values[i], pk),
            None => value_to_pk_component_typed(&values[i], PkKind::Unsupported),
        };
        parts.push(part);
    }
    parts
}

/// Nur fuer Logs/Beispiele: PK-Komponenten lesbar darstellen.
pub fn pk_display(parts: &[String]) -> String {
    format!("{parts:?}")
}

/// Wie `value_to_pk_component`, aber typabhaengig: BINARY als hex, DATE ohne
/// Uhrzeit (passend zu `DATE_FORMAT(..., '%Y-%m-%d')` im Mod-Fallback).
pub fn value_to_pk_component_for(v: &Value, pk: &PkCol) -> String {
    if pk.column_type.eq_ignore_ascii_case("date") {
        if let Value::Date(y, mo, d, ..) = v {
            return format!("{y:04}-{mo:02}-{d:02}");
        }
    }
    value_to_pk_component_typed(v, pk.kind)
}

/// Wie `value_to_pk_component`, aber BINARY als hex (verlustfrei).
pub fn value_to_pk_component_typed(v: &Value, kind: PkKind) -> String {
    match (kind, v) {
        (PkKind::Binary, Value::Bytes(b)) => hex::encode(b),
        _ => value_to_pk_component(v),
    }
}

/// Value -> kanonischer PK-Komponenten-String (für Bucket + DeletePks).
pub fn value_to_pk_component(v: &Value) -> String {
    match v {
        Value::NULL => String::new(),
        Value::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Float(f) => format!("{f}"),
        Value::Double(d) => format!("{d}"),
        Value::Date(y, mo, d, h, mi, s, us) => {
            format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}")
        }
        Value::Time(neg, days, h, mi, s, us) => {
            format!(
                "{}{days:02} {h:02}:{mi:02}:{s:02}.{us:06}",
                if *neg { "-" } else { "" }
            )
        }
    }
}

/// Bucket = letzte N Zeichen der KANONISCHEN LETZTEN PK-Komponente.
/// (Bei numerischen Keys wird das Vorzeichen nicht einbezogen.)
pub fn bucket_of(pk_components: &[String], n: u32) -> String {
    let last = pk_components.last().map(String::as_str).unwrap_or("");
    let s = last.strip_prefix('-').unwrap_or(last);
    last_n_chars(s, n)
}

fn last_n_chars(s: &str, n: u32) -> String {
    let chars: Vec<char> = s.chars().collect();
    let start = chars.len().saturating_sub(n as usize);
    chars[start..].iter().collect()
}

/// Bucket für numerische PKs: nullengefüllte letzte N Ziffern.
/// Wichtig: "43" und "5043" landen dann im SELBEN Bucket "043" — exakt die
/// Modulo-Klasse ABS(pk) % 10^N, die auch die SQL-Drill-Down-Abfrage nutzt.
pub fn bucket_of_pk(pk_components: &[String], numeric_last: bool, n: u32) -> String {
    let last = pk_components.last().map(String::as_str).unwrap_or("");
    let s = last.strip_prefix('-').unwrap_or(last);
    let suffix = last_n_chars(s, n);
    if numeric_last {
        // links mit Nullen auf N Stellen auffüllen
        let mut out: String = suffix;
        while out.chars().count() < n as usize {
            out.insert(0, '0');
        }
        out
    } else {
        suffix
    }
}

/// Entfernt die letzten N Zeichen (Chars) fuer den Range-Chunk-Prefix.
fn remove_last_chars(s: &str, n: u32) -> String {
    let chars: Vec<char> = s.chars().collect();
    let keep = chars.len().saturating_sub(n as usize);
    chars[..keep].iter().collect()
}

fn remove_last_bytes(b: &[u8], n: u32) -> &[u8] {
    let keep = b.len().saturating_sub(n as usize);
    &b[..keep]
}

fn numeric_component(v: &Value) -> Result<i128> {
    let text = match v {
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).trim().to_string(),
        _ => bail!("PK ist nicht numerisch: {v:?}"),
    };
    text.parse::<i128>()
        .map_err(|_| anyhow!("PK ist nicht als Ganzzahl lesbar: {text:?}"))
}

fn binary_component<'a>(v: &'a Value) -> Result<&'a [u8]> {
    match v {
        Value::Bytes(b) => Ok(b),
        _ => bail!("BINARY-PK erwartet Bytes, bekam {v:?}"),
    }
}

/// Chunk-Key fuer Range-Modus aus einem rohen Value.
pub fn chunk_key_from_value(
    kind: PkKind,
    v: &Value,
    mode: ChunkMode,
    chunked: u32,
) -> Result<String> {
    if mode == ChunkMode::Mod {
        let comp = value_to_pk_component_typed(v, kind);
        return chunk_key_from_component(kind, &comp, mode, chunked);
    }
    match kind {
        PkKind::Int | PkKind::UInt => {
            let width = 10i128.pow(chunked.min(6) as u32);
            let n = numeric_component(v)?;
            let low = n.div_euclid(width) * width;
            Ok(low.to_string())
        }
        PkKind::Char => {
            let comp = value_to_pk_component(v);
            Ok(remove_last_chars(&comp, chunked))
        }
        PkKind::Binary => {
            let b = binary_component(v)?;
            Ok(hex::encode(remove_last_bytes(b, chunked)))
        }
        PkKind::Unsupported => bail!("PK-Typ wird von Range-Chunking nicht unterstuetzt"),
    }
}

/// Chunk-Key aus der kanonischen PK-Komponente (z. B. fuer Delete/Send-Mapping).
pub fn chunk_key_from_component(
    kind: PkKind,
    comp: &str,
    mode: ChunkMode,
    chunked: u32,
) -> Result<String> {
    match (mode, kind) {
        (ChunkMode::Mod, _) => Ok(bucket_of_pk(
            &[comp.to_string()],
            matches!(kind, PkKind::Int | PkKind::UInt),
            chunked,
        )),
        (ChunkMode::Range, PkKind::Int | PkKind::UInt) => {
            let width = 10i128.pow(chunked.min(6) as u32);
            let n = comp
                .trim()
                .parse::<i128>()
                .map_err(|_| anyhow!("PK ist nicht als Ganzzahl lesbar: {comp:?}"))?;
            let low = n.div_euclid(width) * width;
            Ok(low.to_string())
        }
        (ChunkMode::Range, PkKind::Char) => Ok(remove_last_chars(comp, chunked)),
        (ChunkMode::Range, PkKind::Binary) => {
            let b = hex::decode(comp)
                .map_err(|_| anyhow!("BINARY-PK ist nicht hex-kodiert: {comp:?}"))?;
            Ok(hex::encode(remove_last_bytes(&b, chunked)))
        }
        (ChunkMode::Range, PkKind::Unsupported) => {
            bail!("PK-Typ wird von Range-Chunking nicht unterstuetzt")
        }
    }
}

/// Chunk-Key einer Zeile (genau eine PK-Spalte im Range-Modus).
pub fn chunk_key_for_pk_values(
    values: &[Value],
    pks: &[crate::mysqlutil::PkCol],
    mode: ChunkMode,
    chunked: u32,
) -> Result<String> {
    if values.len() != pks.len() {
        bail!(
            "PK-Wertanzahl {} passt nicht zu PK-Metadaten {}",
            values.len(),
            pks.len()
        );
    }
    if mode == ChunkMode::Mod {
        let comps: Vec<String> = values
            .iter()
            .zip(pks)
            .map(|(v, pk)| value_to_pk_component_for(v, pk))
            .collect();
        let numeric_last = pks.last().map(|p| p.numeric).unwrap_or(false);
        return Ok(bucket_of_pk(&comps, numeric_last, chunked));
    }
    let pk = pks.first().context("kein PK-Metadatum")?;
    chunk_key_from_value(pk.kind, &values[0], mode, chunked)
}

/// PK-Komponenten aus PK-Werten in PRIMARY-KEY-Reihenfolge.
pub fn pk_components_from_values(values: &[Value], pks: &[PkCol]) -> Vec<String> {
    values
        .iter()
        .zip(pks)
        .map(|(v, pk)| value_to_pk_component_for(v, pk))
        .collect()
}

pub fn chunk_key_for_row(
    values: &[Value],
    pk_indexes: &[usize],
    pks: &[crate::mysqlutil::PkCol],
    mode: ChunkMode,
    chunked: u32,
) -> Result<String> {
    if mode == ChunkMode::Mod {
        let comps: Vec<String> = pk_indexes
            .iter()
            .enumerate()
            .map(|(pos, &i)| match pks.get(pos) {
                Some(pk) => value_to_pk_component_for(&values[i], pk),
                None => value_to_pk_component(&values[i]),
            })
            .collect();
        let numeric_last = pks.last().map(|p| p.numeric).unwrap_or(false);
        return Ok(bucket_of_pk(&comps, numeric_last, chunked));
    }
    let &i = pk_indexes.first().context("kein PK-Index")?;
    let kind = pks.first().map(|p| p.kind).context("kein PK-Metadatum")?;
    chunk_key_from_value(kind, &values[i], mode, chunked)
}

// ---------------------------------------------------------------------------
// TSV-Dialekt: FIELDS TERMINATED BY '\t' ESCAPED BY '\\' LINES TERMINATED BY '\n'
// NULL = "\N", Binärspalten hex-kodiert (ohne 0x-Präfix).
// ---------------------------------------------------------------------------

/// Value -> Text-Darstellung (wie MySQL Text-Protokoll / LOAD DATA).
pub fn value_to_text(v: &Value) -> Vec<u8> {
    match v {
        Value::NULL => b"\\N".to_vec(),
        Value::Bytes(b) => b.clone(),
        Value::Int(i) => i.to_string().into_bytes(),
        Value::UInt(u) => u.to_string().into_bytes(),
        Value::Float(f) => format!("{f}").into_bytes(),
        Value::Double(d) => format!("{d}").into_bytes(),
        Value::Date(y, mo, d, h, mi, s, us) => {
            if *us > 0 {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{us:06}").into_bytes()
            } else {
                format!("{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}").into_bytes()
            }
        }
        Value::Time(neg, days, h, mi, s, us) => {
            let mut out = String::new();
            if *neg {
                out.push('-');
            }
            if *days > 0 {
                out.push_str(&format!("{days} "));
            }
            if *us > 0 {
                out.push_str(&format!("{h:02}:{mi:02}:{s:02}.{us:06}"));
            } else {
                out.push_str(&format!("{h:02}:{mi:02}:{s:02}"));
            }
            out.into_bytes()
        }
    }
}

/// MySQL-LOAD-DATA-Escaping.
pub fn tsv_escape(data: &[u8], out: &mut Vec<u8>) {
    for &b in data {
        match b {
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\t' => out.extend_from_slice(b"\\t"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            0x00 => out.extend_from_slice(b"\\0"),
            0x08 => out.extend_from_slice(b"\\b"),
            0x1A => out.extend_from_slice(b"\\Z"),
            _ => out.push(b),
        }
    }
}

/// Eine Zeile als TSV-Line (inkl. \n) schreiben.
/// hex_indexes: Indizes der Spalten, die als Hex kodiert werden (Binary-Sicherheit).
pub fn row_to_tsv(values: &[Value], hex_indexes: &[usize], out: &mut Vec<u8>) {
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(b'\t');
        }
        if matches!(v, Value::NULL) {
            out.extend_from_slice(b"\\N");
            continue;
        }
        if hex_indexes.contains(&i) {
            if let Value::Bytes(b) = v {
                for hb in hex::encode(b).as_bytes() {
                    out.push(*hb);
                }
                continue;
            }
        }
        let text = value_to_text(v);
        if hex_indexes.contains(&i) {
            // Nicht-Bytes-Wert in Hex-Spalte: Text als ASCII-Hex der Bytes
            for hb in hex::encode(&text).as_bytes() {
                out.push(*hb);
            }
        } else {
            tsv_escape(&text, out);
        }
    }
    out.push(b'\n');
}

/// TSV-Feld unescapen. Returns None wenn das Feld "\N" (NULL) ist.
pub fn tsv_unescape_field(raw: &[u8]) -> Result<Option<Vec<u8>>> {
    if raw == b"\\N" {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == b'\\' && i + 1 < raw.len() {
            let c = raw[i + 1];
            match c {
                b'0' => out.push(0x00),
                b'b' => out.push(0x08),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'Z' => out.push(0x1A),
                other => out.push(other),
            }
            i += 2;
        } else {
            out.push(raw[i]);
            i += 1;
        }
    }
    Ok(Some(out))
}

// ---------------------------------------------------------------------------
// SQL-Literal-Hilfen (für Dump-Modus / WHERE-Klauseln)
// ---------------------------------------------------------------------------

pub fn sql_quote(data: &[u8], out: &mut Vec<u8>) {
    out.push(b'\'');
    for &b in data {
        match b {
            b'\'' => out.extend_from_slice(b"\\'"),
            b'\\' => out.extend_from_slice(b"\\\\"),
            0x00 => out.extend_from_slice(b"\\0"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            0x1A => out.extend_from_slice(b"\\Z"),
            0x08 => out.extend_from_slice(b"\\b"),
            _ => out.push(b),
        }
    }
    out.push(b'\'');
}

pub fn value_to_sql(v: &Value, hex: bool, out: &mut Vec<u8>) {
    match v {
        Value::NULL => out.extend_from_slice(b"NULL"),
        _ if hex => {
            if let Value::Bytes(b) = v {
                out.extend_from_slice(b"0x");
                for hb in hex::encode_upper(b).as_bytes() {
                    out.push(*hb);
                }
            } else {
                let t = value_to_text(v);
                out.extend_from_slice(b"0x");
                for hb in hex::encode_upper(&t).as_bytes() {
                    out.push(*hb);
                }
            }
        }
        Value::Bytes(b) => sql_quote(b, out),
        other => {
            let t = value_to_text(other);
            out.extend_from_slice(&t);
        }
    }
}

/// Prüft ob ein String als Ganzzahl gültig ist (für WHERE-Klauseln).
pub fn is_valid_int_literal(s: &str) -> bool {
    let t = s.strip_prefix('-').unwrap_or(s);
    !t.is_empty() && t.chars().all(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALGOS: [HashAlgo; 5] = [
        HashAlgo::Xxh3,
        HashAlgo::Xxh64,
        HashAlgo::Blake3,
        HashAlgo::Md5,
        HashAlgo::Sha256,
    ];

    fn vals() -> Vec<Value> {
        vec![
            Value::Int(1),
            Value::Bytes(vec![0, 0, 0, 0xff, 0xfe, 0x80]),
            Value::NULL,
            Value::Bytes("äöü\t\\'".as_bytes().to_vec()),
        ]
    }

    #[test]
    fn all_enabled_algos_are_deterministic_hex() {
        for algo in ALGOS {
            if !algo.is_enabled() {
                continue;
            }
            let h = row_hash(&vals(), algo);
            let want = match algo {
                HashAlgo::Xxh3 | HashAlgo::Xxh64 => 16,
                HashAlgo::Md5 => 32,
                HashAlgo::Blake3 | HashAlgo::Sha256 => 64,
            };
            assert_eq!(h.len(), want, "{algo}");
            assert_eq!(h, row_hash(&vals(), algo), "{algo}");
            assert!(h.chars().all(|c| c.is_ascii_hexdigit()), "{algo}");
        }
    }

    #[test]
    fn chunk_hash_matches_streaming_rows() {
        for algo in ALGOS {
            if !algo.is_enabled() {
                continue;
            }
            let mut acc = RowHasher::new(algo);
            for v in vals() {
                acc.update(&hex::decode(row_hash(std::slice::from_ref(&v), algo)).unwrap());
            }
            let got = finalize_chunk("bucket", 4, acc);
            let want = match algo {
                HashAlgo::Xxh3 | HashAlgo::Xxh64 => 16,
                HashAlgo::Md5 => 32,
                HashAlgo::Blake3 | HashAlgo::Sha256 => 64,
            };
            assert_eq!(got.len(), want, "{algo}");
            assert_eq!(
                got,
                finalize_chunk("bucket", 4, {
                    let mut acc = RowHasher::new(algo);
                    for v in vals() {
                        acc.update(&hex::decode(row_hash(std::slice::from_ref(&v), algo)).unwrap());
                    }
                    acc
                })
            );
        }
    }

    fn pk(kind: PkKind, numeric: bool) -> crate::mysqlutil::PkCol {
        crate::mysqlutil::PkCol {
            name: "pk".into(),
            numeric,
            kind,
            column_type: "test".into(),
            sub_part: 0,
        }
    }

    #[test]
    fn range_chunk_keys_numeric_char_binary() {
        let numeric = pk(PkKind::Int, true);
        assert_eq!(
            chunk_key_from_value(PkKind::Int, &Value::Int(1234), ChunkMode::Range, 2).unwrap(),
            "1200"
        );
        assert_eq!(
            chunk_key_from_value(PkKind::Int, &Value::Int(-1), ChunkMode::Range, 2).unwrap(),
            "-100"
        );
        assert_eq!(
            chunk_key_for_row(&[Value::Int(1234)], &[0], &[numeric], ChunkMode::Range, 2).unwrap(),
            "1200"
        );

        let char_pk = pk(PkKind::Char, false);
        assert_eq!(
            chunk_key_from_value(
                PkKind::Char,
                &Value::Bytes(b"abcdef".to_vec()),
                ChunkMode::Range,
                2
            )
            .unwrap(),
            "abcd"
        );
        assert_eq!(
            chunk_key_from_value(
                PkKind::Char,
                &Value::Bytes(b"ab".to_vec()),
                ChunkMode::Range,
                2
            )
            .unwrap(),
            ""
        );
        assert_eq!(
            chunk_key_for_row(
                &[Value::Bytes(b"abcdef".to_vec())],
                &[0],
                &[char_pk],
                ChunkMode::Range,
                2
            )
            .unwrap(),
            "abcd"
        );

        let bin_pk = pk(PkKind::Binary, false);
        assert_eq!(
            chunk_key_from_value(
                PkKind::Binary,
                &Value::Bytes(vec![0x01, 0x02, 0x03, 0x04]),
                ChunkMode::Range,
                2
            )
            .unwrap(),
            "0102"
        );
        assert_eq!(
            chunk_key_for_row(
                &[Value::Bytes(vec![0x01, 0x02, 0x03, 0x04])],
                &[0],
                &[bin_pk],
                ChunkMode::Range,
                2
            )
            .unwrap(),
            "0102"
        );
        assert!(chunk_key_from_value(PkKind::Binary, &Value::Int(1), ChunkMode::Range, 2).is_err());
    }

    #[test]
    fn pk_components_preserve_separators() {
        let pks = vec![
            crate::mysqlutil::PkCol {
                name: "a".into(),
                column_type: "varchar".into(),
                kind: PkKind::Char,
                numeric: false,
                sub_part: 0,
            },
            crate::mysqlutil::PkCol {
                name: "b".into(),
                column_type: "varchar".into(),
                kind: PkKind::Char,
                numeric: false,
                sub_part: 0,
            },
        ];
        let values = vec![
            Value::Bytes(b"a\x1fb".to_vec()),
            Value::Bytes(b"12:34".to_vec()),
        ];
        assert_eq!(
            pk_components(&values, &[0, 1], &pks),
            vec!["a\x1fb".to_string(), "12:34".to_string()]
        );
        assert_eq!(pk_display(&["a".into(), "b".into()]), "[\"a\", \"b\"]");
    }

    #[test]
    fn hash_algo_from_str_aliases() {
        assert_eq!("xxh3".parse::<HashAlgo>().unwrap(), HashAlgo::Xxh3);
        assert_eq!("XXH64".parse::<HashAlgo>().unwrap(), HashAlgo::Xxh64);
        assert_eq!("sha-256".parse::<HashAlgo>().unwrap(), HashAlgo::Sha256);
        assert!("nope".parse::<HashAlgo>().is_err());
    }

    #[test]
    fn row_hash_opts_trim_lower_ignore() {
        use crate::proto::{FieldOpt, FieldTransform};
        let algo = HashAlgo::Xxh3;
        let opt = |ordinal, transform, float_precision| FieldOpt {
            ordinal,
            transform,
            float_precision,
        };
        assert_eq!(
            row_hash_opts(
                &[Value::Bytes(b"  AbC  ".to_vec())],
                algo,
                &[opt(0, Some(FieldTransform::Trim), None)]
            ),
            row_hash(&[Value::Bytes(b"AbC".to_vec())], algo)
        );
        assert_eq!(
            row_hash_opts(
                &[Value::Bytes(b"AbC".to_vec())],
                algo,
                &[opt(0, Some(FieldTransform::Lower), None)]
            ),
            row_hash(&[Value::Bytes(b"abc".to_vec())], algo)
        );
        assert_eq!(
            row_hash_opts(
                &[Value::Int(1), Value::Int(2)],
                algo,
                &[opt(0, Some(FieldTransform::Ignore), None)]
            ),
            row_hash(&[Value::Int(2)], algo)
        );
    }

    #[test]
    fn row_hash_opts_rounds_floats() {
        use crate::proto::FieldOpt;
        let algo = HashAlgo::Xxh3;
        let opt = FieldOpt {
            ordinal: 0,
            transform: None,
            float_precision: Some(2),
        };
        let a = row_hash_opts(&[Value::Double(1.2341)], algo, &[opt]);
        let b = row_hash_opts(&[Value::Double(1.2342)], algo, &[opt]);
        assert_eq!(a, b);
        assert_ne!(
            row_hash(&[Value::Double(1.2341)], algo),
            row_hash(&[Value::Double(1.2342)], algo)
        );
        let f = row_hash_opts(&[Value::Float(3.25)], algo, &[opt]);
        let d = row_hash_opts(&[Value::Double(3.25)], algo, &[opt]);
        assert_eq!(f, d);
    }
}
