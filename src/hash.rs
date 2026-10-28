//! Kanonische Zeilen-Kodierung + Hashing + TSV-Dialekt (LOAD-DATA-kompatibel).
//!
//! Kanonisch = Typ-Tag + Rohbytes je Feld (mysql_async Value-Repräsentation).
//! Beide Seiten (Client/Server) nutzen denselben Parser (select-stream), dadurch
//! sind Hashes unabhängig von der gewählten Export-Strategie identisch.

use anyhow::Result;
use mysql_async::Value;

pub const PK_SEP: char = '\x1f';

/// Kanonische Kodierung eines Feldwerts in den Hasher.
pub fn hash_field(h: &mut blake3::Hasher, v: &Value) {
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

/// Hash einer einzelnen Zeile (Werte inkl. PK).
pub fn row_hash(values: &[Value]) -> String {
    let mut h = blake3::Hasher::new();
    for v in values {
        hash_field(&mut h, v);
    }
    h.finalize().to_hex().to_string()
}

/// Finaler Bucket-Hash: über alle Zeilen-Hashes (in PK-Reihenfolge) + Count.
pub fn finalize_chunk(bucket: &str, count: u64, hasher: &blake3::Hasher) -> String {
    let mut h = hasher.clone();
    h.update(bucket.as_bytes());
    h.update(&[0u8]);
    h.update(&count.to_le_bytes());
    h.finalize().to_hex().to_string()
}

/// Kanonischer PK-String: Komponenten mit \x1f joined.
pub fn pk_canonical(values: &[Value], pk_indexes: &[usize]) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(pk_indexes.len());
    for &i in pk_indexes {
        parts.push(value_to_pk_component(&values[i]));
    }
    parts.join(&PK_SEP.to_string())
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
            format!("{}{days:02} {h:02}:{mi:02}:{s:02}.{us:06}", if *neg { "-" } else { "" })
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
