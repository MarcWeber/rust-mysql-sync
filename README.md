# mysql-sync

**Differentieller MySQL-Tabellen-Sync über QUIC** — ein statisches Rust-Binary, das
Tabellen zwischen zwei MySQL/MariaDB-Servern per gechunkter Hash-Vergleiche
abgleicht und nur die tatsächlichen Differenzen überträgt.

```
QUELLE                                      ZIEL
./mysql-sync -C config-client.json  ──QUIC──▶  ./mysql-sync -C config-server.json
   --sync db_skywings.*                          (MySQL root auf Ziel-DB)
   --force --sync ...
   --check 10 ...
```

## Warum QUIC?

QUIC (Crate `quinn`, TLS 1.3) multiplexiert **viele unabhängige Streams über eine
einzige Verbindung**. mysql-sync öffnet **pro Tabelle einen eigenen Stream** —
`concurrent-processes` Tabellen laufen parallel, ohne Head-of-Line-Blocking und
ohne dass jede Verbindung neu authentifizieren muss. TLS ist in QUIC Pflicht:
der Server generiert beim ersten Start ein Self-signed-Zertifikat und legt den
SHA-256-Fingerprint ab; der Client **pinnt** ihn (`server_fingerprint`).

## So funktioniert der Abgleich

1. `describe/count` von lokaler und entfernter Tabelle
2. Schema-Vergleich: bei Abweichung → mit `--force`/`--force-schema` DROP+CREATE, sonst Fehler
3. **Chunk-Hashes**: Zeilen werden per Primary Key in `10^chunked` Buckets
   („letzte N Zeichen" bzw. bei numerischen Keys die Modulo-Klasse `ABS(pk) % 10^N`)
   gruppiert. Beide Seiten berechnen je Bucket einen Blake3-Hash → nur
   differierende Buckets werden betrachtet.
4. Ist der Diff-Anteil > `diff-threshold-percent` → **Vollkopie** (Truncate + alle Zeilen).
5. Sonst **Drill-Down**: Zeilen-Hashes nur für die differierenden Buckets
   (je ein Scan pro Seite) →
   - Zeilen lokal vorhanden, remote fehlen/abweichen → **sende_csv** (oder sende_dump)
   - Zeilen remote vorhanden, lokal fehlen → **DeletePks**
6. Buckets > `dump-threshold` Zeilen werden komplett kopiert statt zeilenweise gediffed
   („immer 1000 auf einmal", minimiert Sync-Daten).

Die Fortschrittsanzeige nutzt genau diese Counts: Buckets gesamt/differierend,
Zeilen zu senden/löschen.

## Installation

Binaries in `bin/`:

| Datei | für |
|---|---|
| `mysql-sync-linux-x86_64` | jedes AMD64/Intel-64-Linux, **statisch gelinkt (musl)** — keine Abhängigkeiten |
| `mysql-sync-linux-x86_64-glibc` | Ubuntu/Debian 20.04+ (dynamisch, kleinere Datei) |

```bash
chmod +x mysql-sync-linux-x86_64
cp mysql-sync-linux-x86_64 mysql-sync
```

## Quickstart mit Docker (Test)

```bash
docker compose up -d          # 2 MySQL-8-Container (Quelle :3306, Ziel :3307)
python3 test.py               # seedet, startet Server, spielt --force/--sync/--check durch
```

`test.py` führt 7 Szenarien aus (Schema-Force, differentieller Sync, Drift-Detektion
mit exakten Zeilenanzahlen, Reparatur, Force-Recreate, SQL-Dump-Modus) und prüft
u. a. **BLOB-Prüfsummen** (Binärsicherheit: `\0`, `\n`, `\t`, `0xFF` in Daten).
Ohne Docker: `MSYNC_LOCAL=1 MSYNC_SRC_PORT=… MSYNC_DST_PORT=… python3 test.py`
gegen zwei laufende Instanzen.

## CLI

```
./mysql-sync -C config-server.json                       # Server-Modus
./mysql-sync -C config-client.json --sync A.B            # Inhalte abgleichen
./mysql-sync -C config-client.json --force A.B           # nur Schema erzwingen
./mysql-sync -C config-client.json --force --sync A.B    # Recreate + Abgleich
./mysql-sync -C config-client.json --force-schema A      # Schema für ganze DBs
./mysql-sync -C config-client.json --check 10 A.B        # Diff-Report + 10 Beispiel-IDs
```

**Selector `A.B`**: `A` = DB-Liste (kommasepariert) oder `*`, `B` = Tabellen-Liste
oder `*`. Beispiele aus dem Spezialfall-Set:

```
--sync db_skywings,db_airwings.*        # beide DBs, alle Tabellen
--sync db_skywings.*,db_airwings.*      # identische Bedeutung
--force-schema db_skywings,db_airwings  # nur Schema
--check 10 db_skywings.*                # 10 Beispiel-IDs je Tabelle
```

**Exit-Codes**: `sync/force`: 0 = ok, 1 = Fehler · `check`: 0 = identisch,
1 = Unterschiede gefunden, 2 = Fehler. Nützlich für Cron-Monitoring:

```bash
mysql-sync -C config-client.json --check 10 "*" || alert "DB-Drift!"
```

Weitere Optionen: `--no-progress` (für Cron/CI), `--log-level debug|trace`.

## Konfiguration

`config-server.json` (Ziel):

```json
{
  "mysql":  { "user": "root", "password": "…", "host": "127.0.0.1", "port": 3306 },
  "listen": "0.0.0.0",
  "port": 4433,
  "auth_token": "optional-gemeinsames-geheimnis"
}
```

Beim ersten Start erzeugt der Server `config-server-cert.pem` / `-key.pem` und
`config-server.fingerprint` (SHA-256) neben der Config und druckt den Fingerprint.

`config-client.json` (Quelle):

```json
{
  "mysql": { "user": "root", "password": "…", "host": "127.0.0.1", "port": 3306 },
  "server": "ziel.example.com:4433",
  "server_fingerprint": "sha256:FC7A65…(aus config-server.fingerprint)",

  "allow_csv": true,
  "dump_strategies": ["select-stream", "to_file:/tmp", "mysql-e", "mysqldump"],
  "dump_threshold": 1000,
  "chunked": 3,
  "concurrent_processes": 3,
  "diff_threshold_percent": 10,
  "disable_triggers": false,

  "database_mappings":       { "db": "target_db" },
  "database_table_mappings": { "db.table": "target_db.target_table" }
}
```

Alle Keys funktionieren auch mit Bindestrich (`dump-threshold`, `concurrent-processes`,
`diff-threshold-percent`, `database_mappings`, …) — gemischt erlaubt.

### Konfig-Referenz

| Key | Default | Bedeutung |
|---|---|---|
| `mysql` | – | DB-Zugang: Quelle (Client) bzw. Ziel (Server); `socket` statt host/port möglich |
| `server` | – | Client: Ziel-`host:port` (ohne Port → `port`/4433) |
| `listen` / `port` | `0.0.0.0` / `4433` | Server-Bind |
| `allow_csv` | `true` | `false` = SQL-REPLACE-Dumps statt CSV-Stream |
| `dump_strategies` | `["select-stream"]` | Export-Reihenfolge mit Fallback, siehe unten |
| `dump_threshold` | `1000` | Bucket-Größe ab der ganz kopiert wird; auch REPLACE-Batchgröße |
| `chunked` | `3` | Bucket-Exponent: `10^N` Buckets (N = 1–6) |
| `concurrent_processes` | `3` | Parallele Tabellen-Tasks (je 1 QUIC-Stream) |
| `diff_threshold_percent` | `10` | Ab X % differierender Buckets: Vollkopie statt Drill-Down |
| `disable_triggers` | `false` | Vor Import Trigger droppen, danach wiederherstellen |
| `server_fingerprint` | – | Client: erwartetes Server-Zertifikat (`sha256:…`) |
| `auth_token` | – | Beidseitig gleich → Hello-Handshake prüft es |
| `tls_cert` / `tls_key` | – | Server: eigene PEMs statt Self-signed |
| `connect_timeout_secs` | `30` | QUIC-Handshake-Timeout |
| `max_frame` | `8388608` | Max. Framgröße (DoS-Grenze) |

### Dump-Strategien (Export der lokalen Zeilen)

| Name | Weg | Anmerkung |
|---|---|---|
| `select-stream` | in-process Streaming-SELECT (mysql_async) | Default, immer verfügbar, binärsicher, kein Shell-Zugriff nötig |
| `to_file:DIR` | `SELECT … INTO OUTFILE` auf MySQL-Host, Datei wird gelesen | schnellster Server-seitiger Export; braucht `FILE`-Privileg, `secure_file_priv` und lokale Dateisystem-Zugriff; MySQL muss auf demselben Host laufen |
| `mysql-e` | `mysql --batch -e "SELECT …" \|` | TSV-Pipe; `\r` in Daten nicht escapbar (MySQL-Client-Limit) |
| `mysqldump` | Dump parsen (INSERT-Statements) | für Trigger-/Binärfälle; WHERE-Filter clientseitig |

Bei Fallback gilt: Nur wenn eine Strategie **null** Zeilen liefert (z. B. `OUTFILE`
verweigert), wird die nächste probiert. Ein Abbruch mitten im Stream kann nicht
sicher wiederholt werden und bricht den Task ab.

**Trigger:** `mysqldump` führt Trigger aus (Dumps laufen als Statements). Mit
`disable_triggers: true` deaktiviert mysql-sync auf dem **Ziel** die Trigger der
Tabelle (Snapshot → DROP) während des Imports und legt sie danach wieder an.

## Sicherheit

- QUIC erzwingt **TLS 1.3**; Cipher-Suites via rustls/ring.
- **Pinning**: Client prüft den SHA-256 des Server-Zertifikats. Ohne Pinning:
  TOFU mit Warnung (für Produktion pinnen!).
- `auth_token`: erster Request pro Verbindung muss ein gültiges Hello bringen.
- Identifier werden backtick-gequoted; Dump-Statements laufen durch eine
  Allowlist (`INSERT/REPLACE/DELETE/UPDATE/SET/TRUNCATE`).
- `max_frame` begrenzt Frames; der Dump-Buffer ist auf 2 GB gedeckelt.

## Build & Cross-Compile

**Ohne lokale Toolchain (empfohlen)** — nur Docker nötig, erzeugt das statisch
gelinkte musl-Binary (`bin/mysql-sync-linux-x86_64`):

```bash
./compile-linux.sh              # musl static (empfohlen)
./compile-linux.sh --glibc      # zusätzlich glibc-Binary
./compile-linux.sh --clean      # Build-Caches (Named Volumes) löschen
```

Das Skript baut intern das Cross-Image (`Dockerfile.cross`: rust:1-slim +
musl-tools + `rustup target add x86_64-unknown-linux-musl`) und kompiliert in
einem Container; Cargo-Registry und Build-Artefakte liegen in Docker-Named-
Volumes, wiederholte Builds sind daher inkrementell. Alternative Basis via
`--image rust:1` oder `RUST_IMAGE=... ./compile-linux.sh`; Podman via
`DOCKER=podman ./compile-linux.sh`.

**Lokal (ohne Docker)** — benötigt installierte Rust-Toolchain:

```bash
cargo build --release                                  # glibc-Binary
rustup target add x86_64-unknown-linux-musl
CC_x86_64_unknown_linux_musl=musl-gcc \
CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc \
  cargo build --release --target x86_64-unknown-linux-musl   # statisch
./build.sh                                             # beides + bin/ befüllen
```

`.github/workflows/build.yml` baut glibc + musl (und optional macOS) in CI und
hängt die Binaries als Release-Artefakte an.

## Architektur

```
src/
├── main.rs       CLI (clap): Server-/Client-Dispatch
├── config.rs     JSON-Config (Dash-/Underscore-Aliase, Toleranz)
├── proto.rs      QUIC-Framing (1B Tag + u32 BE Len), Request/Response-Enum
├── hash.rs       Kanonische Zeilen-Kodierung → Blake3, TSV-Dialekt, Buckets
├── mysqlutil.rs  Connections, Schema-Infos, PKs, Streaming-Scans, WHERE-Builder
├── selector.rs   A.B-Selektor + DB/Tabellen-Mappings
├── tls.rs        Self-signed-Gen (rcgen), Fingerprint-Pinning (rustls)
├── server.rs     QUIC-Listener, State-Machine (Normal/Dump/Csv), Hash-API
├── import.rs     TSV-Batcher → REPLACE-Import, Dump-Ausführung, Trigger
├── export.rs     4 Export-Strategien mit Fallback (RowMsg-Kanal)
├── sync.rs       Diff-Engine: force / sync / check, Drill-Down, Vollkopie
├── client.rs     Handshake, Task-Enumeration, Semaphore, Summary, Exit-Codes
└── progress.rs   indicatif-Balken je Tabelle (Phasen + Counts)
```

## Bekannte Grenzen

- Tabellen brauchen einen **Primary Key** (sonst Fehler/Hinweis).
- Zero-Dates (`0000-00-00`) und `BIT`-Spalten sind nicht im Fokus.
- Float-Rundung: Hashes basieren auf der Textdarstellung der DB — beide Seiten
  sollten dieselbe MariaDB/MySQL-Hauptversion haben.
- Beide Seiten sollten dieselbe mysql-sync-Version laufen lassen (Protokoll v1).
- `mysql-e`-Strategie: MySQLs `--batch` escapet `\r` nicht.
