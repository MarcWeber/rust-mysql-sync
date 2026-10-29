[English](README.md) | [Deutsch](README.DE.md)

# mysql-sync(1)

## NAME

mysql-sync — MySQL-Tabellen differentiell über QUIC (oder Klartext-TCP) synchronisieren.

## SYNOPSIS

```
mysql-sync -C CONFIG [--force-schema|--force|--sync|--check N] [OPTIONEN] [SELECTOR]
mysql-sync -C -  < config.json
```

`-C -` (bzw. `--config -`) liest die JSON-Config aus **stdin**.

## DESCRIPTION

Vergleicht Quelle (Client) und Ziel (Server) über Bucket- und Zeilen-Hashes und
überträgt nur abweichende Zeilen. Transport ist QUIC/TLS 1.3 mit
Zertifikats-Pinning. **Client und Server müssen denselben `hash_algo` nutzen**
(der Hello-Handshake bricht sonst ab), aber `--hash-via-select` nutzt immer
MySQL-`MD5`.

`SELECTOR` = `A.B` — `A` = DB-Liste oder `*`, `B` = Tabellen-Liste oder `*`.
Beispiele: `db1.*`, `db1,db2.*`, `db1.t1,t2`.

| Modus | Wirkung |
|---|---|
| `--force-schema [SEL]` | Nur Schema/Datenbanken anlegen bzw. angleichen |
| `--force [SEL]` | Zieltabelle neu erstellen (Datenverlust) |
| `--sync [SEL]` | Differentiell abgleichen (INSERT/UPDATE/DELETE) |
| `--check N [SEL]` | Nur Diff-Report + N Beispiel-IDs; kein Schreibzugriff |

| Option | Bedeutung |
|---|---|
| `-C, --config FILE` | JSON-Config (Pflicht); `-` = von stdin |
| `--hash-via-select` | Gruppierte MD5-Fingerprints direkt in MySQL (Default) |
| `--hash-via-dump` | Diff-Hashes über Export/Parser (Alias: `--check-via-dump`) |
| `--hash-via-raw-binary` | Rohe MySQL-Binary-Protokoll-Zeilen hashen (Alias: `--check-via-raw-binary`) |
| `--hash-via-raw-tsv` | Rohe `mysql --batch`-TSV-Zeilen hashen (Alias: `--check-via-raw-tsv`) |
| `--no-progress` | Keine Fortschrittsbalken (Cron/CI) |
| `--log-level LVL` | `error|warn|info|debug|trace` |

## HASHING — EMPFEHLUNG

`--hash-via-select` (Default) berechnet Bucket-Fingerprints direkt in MySQL.
Eine `GROUP BY`-Query pro Tabelle liefert `bucket`, `COUNT(*)` und einen
MD5-Gruppen-Hash: MySQL baut pro Zeile einen null-sicheren, längencodierten
Fingerprint, summiert beide 64-Bit-Hälften des MD5 pro Bucket und hasht erst
danach das Aggregat. Nur Buckets mit abweichendem Hash/Count lösen
Zeilen-Hashes aus; nur Zeilen mit abweichendem PK/Hash werden exportiert und
gesendet.

`--hash-via-dump` streamt Zeilen durch den Export-/Parser-Pfad und hasht sie in
Rust mit `hash_algo` (Default xxh3). Sinnvoll, wenn genau dieser
Export-/Parser-Pfad geprüft werden soll oder Sonderfälle `mysqldump` erzwingen.

`--hash-via-raw-binary` liest Prepared-Statement-Ergebniszeilen über das
gepatchte `mysql_async`-`next_raw()` und hasht das native binäre Row-Paket
direkt. PK-Felder werden nur dekodiert, wenn ein abweichender Bucket
weitergebohrt werden muss. Das ist der schnellste Pfad für saubere Scans und
kleine Diffs.

`--hash-via-raw-tsv` startet auf beiden Seiten
`mysql --batch --quick --skip-column-names --default-character-set=utf8mb4` und
hasht jede escapte Ausgabezeile. Das spart einen zweiten Rust-Zeilenparser,
hängt aber vom externen `mysql`-CLI ab und ist langsamer als raw-binary.

Raw-Modi sind **fail-closed**:

- Das vollständige Session-/Server-Repräsentationsprofil muss exakt gleich sein:
  `VERSION()`, `@@version_comment`, `@@version_compile_os`,
  `@@version_compile_machine`, `character_set_client/connection/results`,
  `collation_connection`, `time_zone` und `sql_mode`. Bei Abweichung wird
  abgelehnt; dann select oder dump nutzen.
- Auch der normalisierte Schema-Hash muss auf beiden Seiten gleich sein.
- `raw-tsv` lehnt zusätzlich Tabellen mit binären Spalten ab und setzt
  identisches `mysql`-Client-Verhalten bzw. dieselbe Version auf beiden Hosts
  voraus. Ein Fallback findet nicht statt.

- `hash_algo` steuert nur den Dump-/Raw-Modus. Der Select-Modus nutzt immer
  MySQL-`MD5`, weil MySQL kein eingebautes xxh3/blake3 hat. Client und Server
  brauchen trotzdem denselben `hash_algo`, weil der Hello-Handshake ihn prüft.
- `--hash-via-select` vermeidet Zeilen-Serialisierung/Parser, aber MySQL-`MD5`
  ist pro Zeile langsamer als Rust-xxh3. Für die eigene Last beide Wege messen.
- `xxh3` ist nicht kryptografisch, für Drift-Erkennung aber völlig ausreichend
  und der schnellste Algorithmus im Dump-Modus.
- `blake3` nur wählen, wenn kryptografische Kollisionsresistenz gefordert ist
  (z. B. nicht vertrauenswürdige Quelle). Rund 1,5× langsamer als xxh3.
- `xxh64`, `md5`, `sha256` primär für Kompatibilität; im Dump-Modus nicht
  schneller als xxh3.

**Testumgebung (Messung vom 2026-10-29, alle Hash-Pfade):**
Apple M1 Ultra (20 Kerne, 64 GB), macOS, Release-Build (`opt-level=2`).
Client und Server laufen nativ auf dem Host, MySQL 8.0 in Docker
(`docker-compose.yml`, Quelle `:3306`, Ziel `:3317`). Datensatz: synthetische
Tabelle `db_bench.big` mit **100.000 Zeilen** (id, VARCHAR-MD5-String, INT,
DATETIME), `chunked=3`, `--check 0`, Median aus 5 Läufen (gesamter Abgleich
inkl. Netzwerk, 2 Seiten).

| Szenario | select (SQL-MD5) | dump (Rust-xxh3) | raw-binary | raw-tsv |
|---|---:|---:|---:|---:|
| clean (0 Diffs) | 867 ms | 258 ms | **142 ms** | 555 ms |
| 10 % der Zeilen abweichend (10 % der Buckets) | 980 ms | 303 ms | **164 ms** | 961 ms |
| Ziel leer (100 % Diff) | 679 ms | **147 ms** | 301 ms | 1096 ms |

Der Dump-Weg ist schnell, weil `select-stream` native Zeilen an den
Rust-Prozess schickt und xxh3 sie hasht. Für `--check` wird kein CSV
geschrieben; CSV wird erst beim tatsächlichen `--sync`-Transfer geänderter
Zeilen genutzt (Binärspalten erzwingen den SQL-Dump-Pfad). Raw-binary vermeidet
`Vec<Value>` und Text-Hashing: Es hasht das binäre Protokoll-Paket direkt und
dekodiert nur die benötigten PK-Felder.

Ist das Ziel leer, unterscheidet sich jeder Bucket; der Check macht daher einen
zweiten Zeilen-Hash-Durchlauf über alle Zeilen. Raw-binary zahlt zwei volle
Scans, während die Zeilen-Hash-Query von select in MySQL bleibt; raw-tsv zahlt
zusätzlich den externen `mysql`-Prozess und TSV-Escaping/Unescaping pro Zeile.
Die Zahlen sind Richtwerte — auf eigener Hardware mit `--check` reproduzieren.

**Benchmark reproduzieren** (Testdaten werden erzeugt, nicht mitgeliefert):

```bash
# 1) 100k-Zeilen-Tabelle auf der Quelle erzeugen
docker compose exec -T mysql-src mysql -uroot -psyncroot -e "
CREATE DATABASE IF NOT EXISTS db_bench;
CREATE TABLE db_bench.big (id INT NOT NULL AUTO_INCREMENT, a VARCHAR(64) NOT NULL,
  b INT NOT NULL, c DATETIME NOT NULL, PRIMARY KEY(id)) ENGINE=InnoDB;
INSERT INTO db_bench.big (a,b,c)
SELECT CONCAT('row-',n,'-',MD5(n)), n%1000, DATE_ADD('2024-01-01', INTERVAL n SECOND)
FROM (SELECT a.N + b.N*10 + c.N*100 + d.N*1000 + e.N*10000 + 1 AS n
      FROM (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
           (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
           (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
           (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d,
           (SELECT 0 N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
            UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) e) s
WHERE n <= 100000;"

# 2) Ziel füllen und beide Datenwege timen (je 5×)
mysql-sync -C .test-run/config-client.json --force --sync db_bench.big
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-dump db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-raw-binary db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-raw-tsv db_bench.big; done

# 3) mit ~10 % abweichenden Zeilen im Ziel wiederholen
docker compose exec -T mysql-dst mysql -uroot -psyncroot -e \
  "UPDATE db_bench.big SET a=CONCAT(a,'-x') WHERE id <= 10000"
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-dump db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-raw-binary db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-raw-tsv db_bench.big; done

# 4) mit leerem Ziel wiederholen (100 % Diff)
docker compose exec -T mysql-dst mysql -uroot -psyncroot -e "TRUNCATE db_bench.big"
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-dump db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-raw-binary db_bench.big; done
for i in 1 2 3 4 5; do time mysql-sync -C .test-run/config-client.json --check 0 --hash-via-raw-tsv db_bench.big; done
```

Alle Algorithmen sind per Default einkompiliert (`blake3`, `xxhash`, `md5`,
`sha2`). Minimal-Build: `cargo build --no-default-features --features xxhash`.

## QUELLEN-SICHERHEIT & EXPORT-NEBENWIRKUNGEN

Der Client liest Quelltabellen ausschließlich: Er führt `SELECT`-/`SHOW`-Queries
aus und niemals `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `LOAD DATA` oder DDL
gegen die Quelle. Alle Schreibvorgänge sind Protokoll-Requests, die der Server
auf dem Ziel ausführt.

`dump_strategies` legt fest, wie Quellzeilen gelesen werden:

| Strategie | Quellzugriff | Voraussetzungen / Nebenwirkungen |
|---|---|---|
| `select-stream` | streamendes `SELECT` | Keine. Default und am schnellsten. |
| `to_file[:DIR]` | `SELECT ... INTO OUTFILE` | Benötigt `FILE`-Privileg und einen erlaubten `secure_file_priv`-Pfad auf dem MySQL-Host. Schreibt eine temporäre TSV-Datei auf dem Quellhost, liest sie danach und löscht sie. **Bei großen Tabellen langsam**: Das komplette Ergebnis wird auf Platte geschrieben und wieder gelesen statt gestreamt. Ein Crash oder ein Pfadproblem bei Remote-Hosts kann die Datei zurücklassen. |
| `mysql-e` | externes `mysql --batch -e "SELECT ..."` | Benötigt den `mysql`-CLI auf dem Client-Host. Nur `SELECT`; keine Tabellen- oder Dateischreibvorgänge. Nutzt `host`/`port`; `mysql.socket` wird ignoriert. |
| `mysqldump` | `mysqldump --skip-lock-tables --single-transaction --skip-triggers` | Nur lesend. `INSERT`-Statements werden geparst, nie lokal ausgeführt. Wird bei binären Spalten automatisch zuerst bevorzugt. |

Ein Fallback erfolgt nur, wenn eine Strategie scheitert, bevor sie eine Zeile
liefert. Um die `to_file`-Nebenwirkung vollständig zu vermeiden, direkt
`mysql-e` verwenden:

```json
{ "dump_strategies": ["mysql-e", "mysqldump"] }
```

Für strikten Schutz dem Client ein Quellkonto mit ausschließlich Leserechten
geben (`SELECT`/`SHOW`; `FILE` nur, wenn `to_file` benötigt wird). Der Server
muss ein separates beschreibbares Konto für das Ziel verwenden. Zeigt die
Server-Config auf die Quellinstanz, würden dessen Schreibvorgänge die Quelle
verändern.

## CHUNK-MODI

Der Chunk-Modus wird pro Tabelle automatisch aus den Primary-Key-Metadaten
gewählt; einen `chunk_mode`-Config-Key gibt es nicht mehr. `chunked` steuert
Bucket-Breite bzw. Suffixlänge.

- **Range** wird für genau eine unterstützte PK-Spalte verwendet: INTEGER,
  CHAR/VARCHAR oder BINARY/VARBINARY (ohne Prefix-Index). Numerische PKs nutzen
  `floor(pk / 10^chunked) * 10^chunked` (`chunked=3` → 1.000er-Bereiche);
  CHAR/BINARY schneiden die letzten `chunked` Zeichen/Bytes ab. Jeder
  Chunk-SELECT/-Export ist nach PK sortiert.
- **Mod** wird automatisch als Fallback für zusammengesetzte PKs und andere
  Einzelspaltentypen gewählt (DATE/TIME, DECIMAL, FLOAT/DOUBLE, ENUM/SET, BIT,
  TEXT/BLOB-Prefix-Keys, …). Es nutzt Modulo-/Suffix-Buckets: die letzten
  `chunked` Zeichen der kanonischen PK-Komponente bzw. `ABS(pk) % 10^N` für
  numerische Komponenten. Bei zusammengesetzten Keys zählt die letzte
  Komponente; DATE-Keys werden als reines Datum behandelt.
- Mod-Buckets sind nicht zusammenhängend, nutzen also in der Regel keine
  PK-Range-Prädikate und können die Tabelle vollständig scannen.

Range-Chunks können groß sein: Eine geänderte Zeile ändert den Hash des ganzen
Chunks. Die Zeilen-Schwellwert-Heuristik gilt daher nur für Mod; im Range-Modus
wird nach einem Chunk-Mismatch immer bis zu exakten Zeilen-Hashes weitergebohrt.

## CONFIG

`config-server.json` (Ziel):

```json
{
  "mysql": { "user": "root", "password": "…", "host": "127.0.0.1", "port": 3306 },
  "listen": "0.0.0.0",
  "port": 4433,
  "auth_token": "optional-gemeinsames-geheimnis",
  "hash_algo": "xxh3",
  "transport": "quic",
  "compression": "none",
  "compression_level": 3
}
```

`config-client.json` (Quelle):

```json
{
  "mysql": { "user": "root", "password": "…", "host": "127.0.0.1", "port": 3306 },
  "server": "ziel-host.example.com:4433",
  "server_fingerprint": "sha256:…",
  "auth_token": "dasselbe-geheimnis-wie-am-server",
  "hash_algo": "xxh3",
  "transport": "quic",
  "compression": "none",
  "compression_level": 3,
  "allow_csv": true,
  "dump_strategies": ["select-stream"],
  "transfer_batch_rows": 1000,
  "chunked": 3,
  "concurrent_processes": 3,
  "diff_threshold_percent": 10,
  "disable_triggers": false,
  "database_mappings": { "db_alt": "db_neu" }
}
```

**Passwort/Auth:** `auth_token` muss auf Client und Server **identisch** sein.
Der Client sendet ihn im ersten Request (`Hello`) über die verschlüsselte
QUIC/TLS-Verbindung; der Server vergleicht ihn und antwortet sonst mit
`Auth-Token ungültig`. Ist am Server kein `auth_token` gesetzt, wird kein Token
geprüft. Für Produktion immer setzen — zusammen mit `server_fingerprint`
(Pinning). Secrets lassen sich per stdin übergeben: `-C -`.

Weitere Keys: `transport` (`quic`/`tcp`), `allow_plaintext`, `compression`
(`none`/`zstd`), `compression_level` (1–22), `max_frame`, `max_decompressed`,
`allow_csv` (`false` = SQL-REPLACE-Dumps), `dump_strategies` (Reihenfolge mit
Fallback), `transfer_batch_rows` (alias `dump_threshold`), `chunked` (1–6), `concurrent_processes`,
`diff_threshold_percent`, `disable_triggers`, `database_mappings`,
`database_table_mappings`. Der Chunk-Modus ist automatisch und kein Config-Key.

Beim ersten Serverstart entstehen Zertifikat + `<config>.fingerprint` neben der
Config; den Fingerprint im Client als `"server_fingerprint": "sha256:…"` pinnen.
Bei `-C -` (stdin) schreibt der Server die TLS-Dateien als
`config-server-*.pem` / `config-server.fingerprint` ins Arbeitsverzeichnis.

## TRANSPORT, VERSCHLÜSSELUNG & KOMPRESSION

- **Default `transport: "quic"`**: QUIC mit TLS 1.3, immer verschlüsselt. Für
  QUIC gibt es keinen Schalter zum Abschalten der Verschlüsselung; sichere
  Identität über `server_fingerprint` (Pinning) und `auth_token`.
- **`transport: "tcp"` ist Klartext** und nur zusammen mit
  `"allow_plaintext": true` auf beiden Seiten erlaubt. Der Prozess warnt laut.
  Nur innerhalb eines Tunnels verwenden, der bereits Vertraulichkeit liefert.
- **Kompression**: `"compression": "none"` (Default) oder `"zstd"`. Mit `zstd`
  wird **jeder** Frame komprimiert — kein Schwellwert, keine Heuristik —
  `compression_level` 1–22 (Default 3). Frames sind selbsterklärend, beide
  Seiten dürfen unterschiedlich konfiguriert sein.
- **Wire-Format v4**: `U<len>\n<body>` (unkomprimiert) oder `C<len>\n<body>`
  (zstd), `<body>` = rkyv-serialisierter `Request`/`Response`; ALPN
  `mysql-sync/4`. Client und Server müssen aus derselben Version gebaut sein.

## TRUST MODEL

Nach erfolgreichem `Hello` (Protokollversion + `auth_token`) gelten die
Wire-Bytes des Peers als korrekt. Die rkyv-Dekodierung prüft noch die
strukturelle Gültigkeit, damit fehlerhafte Frames zu sauberen Fehlern statt zu
undefiniertem Verhalten werden — das ist aber **keine Sicherheitsgrenze**: Ein
korrekt authentifizierter Peer kann ohnehin Schaden anrichten, indem er
**korrekt formatiertes, aber inhaltlich falsches SQL** sendet (z. B.
`SendeDump` mit `DROP TABLE`). Integrität/Confidentiality kommen aus QUIC/TLS
und dem Token, nicht aus der Payload-Prüfung. Nur `Hello` wird vor der
Token-Prüfung geparst.

## EXAMPLES

```bash
mysql-sync -C config-server.json
mysql-sync -C config-client.json --force-schema db1,db2
mysql-sync -C config-client.json --force --sync db1.*,db2.*
mysql-sync -C config-client.json --check 10 "db1.*" || alert "DB-Drift!"
mysql-sync -C config-client.json --check 10 --hash-via-dump "db1.t1"

# Config aus stdin (z. B. aus einem Secret-Store)
cat config-client.json | mysql-sync -C - --check 10 "db1.*"
```

`--check-via-select`, `--check-via-dump`, `--check-via-raw-binary` und
`--check-via-raw-tsv` sind weiterhin als Aliase für ihre `--hash-via-*`-Formen
gültig.

Bei `--sync` den Selector direkt nach dem Flag (oder nach `--check N`) setzen.
`--sync` akzeptiert einen optionalen Selector; steht eine andere Option zwischen
`--sync` und dem Selector, gilt der Default-Selector `*`.

## EXIT STATUS

`--force*`/`--sync`: `0` = ok, `1` = Fehler.  
`--check`: `0` = identisch, `1` = Unterschiede, `2` = Fehler.

## TESTING

```bash
MSYNC_DST_PORT=3317 docker compose up -d
MSYNC_DST_PORT=3317 python3 test.py
```

`test.py` prüft die Szenarien A–L mit 69 Checks: Schema, differentieller Sync,
Drift mit exakten Zeilenanzahlen, Reparatur, Force-Recreate, SQL-Dump,
select-/dump-/raw-binary-/raw-tsv-Hashing (clean, Drift, Sync und gechunkter
Range-/Mod-Fallback), Raw-Profil-Gate, raw-tsv-Ablehnung binärer Spalten,
Hash-Mismatch + blake3, zstd-Kompression, Klartext-TCP sowie automatisches
Range-Chunking für numerische/CHAR/BINARY-PKs, Mod-Fallback für DATE/Composite
und Ablehnung von Tabellen ohne PK — inkl. binärer Giftdaten (`\0\0\0`,
ungültiges UTF-8). Im Docker-Modus legt der Test einen temporären
`mysql`-Wrapper in `.test-run/bin` an, damit raw-tsv durch die Container
scannen kann.
Ohne Docker: `MSYNC_LOCAL=1 MSYNC_SRC_PORT=… MSYNC_DST_PORT=… python3 test.py`.

## ALTERNATIVEN

### pt-table-sync (Percona Toolkit, Perl)

Dokumentation: <https://docs.percona.com/percona-toolkit/pt-table-sync.html> ·
Quelle: <https://github.com/percona/percona-toolkit> ·
Single-File-Download: <https://www.percona.com/get/pt-table-sync>

`pt-table-sync` ist ein Perl-Werkzeug aus dem Percona Toolkit. Es verbindet sich
direkt mit einem oder mehreren MySQL-DSNs, findet Zeilenunterschiede mit
Chunk-/Nibble-/GroupBy-/Stream-Algorithmen und kann die Reparatur-SQL ausgeben
oder `INSERT`-/`REPLACE`-/`UPDATE`-/`DELETE`-Statements auf einem Ziel ausführen.

Verbindungsmodell: `pt-table-sync` ist ein Perl-Prozess mit Netzwerkzugriff auf
beide MySQL-Endpunkte. Er öffnet normale MySQL-Client-Verbindungen zur Quelle
und zum Ziel; MySQL selbst verbindet sich nicht mit MySQL. Es ist weder die
`FEDERATED`-Engine noch ein Datenbank-Link oder ein Unix-`root`-Konto
beteiligt. Die MySQL-Konten müssen Verbindungen vom ausführenden Host erlauben
und benötigen `SELECT` auf der Quelle sowie
`INSERT`/`UPDATE`/`DELETE`/`REPLACE` auf dem Ziel. Replikationsbezogene Optionen
(`--sync-to-source`, `--replicate`) können zusätzlich `SUPER` oder
`REPLICATION CLIENT` erfordern. Das MySQL-`root`-Konto ist normalerweise nicht
nötig und nicht empfohlen.

Unterschiede zu mysql-sync:

- Direkte MySQL-Verbindungen des `pt-table-sync`-Prozesses zu beiden
  Endpunkten statt des Client/Server-QUIC-Transports von mysql-sync. Es nutzt
  die Sicherheit der MySQL-Verbindung; mysql-sync bietet QUIC/TLS,
  Fingerprint-Pinning, Authentifizierung und optional zstd.
- Replikationsbewusst. Es unterstützt Einweg- und Bidirektional-Sync,
  `--sync-to-source`-Reparatur und die Wiederverwendung von
  `pt-table-checksum --replicate`-Ergebnissen. mysql-sync ist
  topologieunabhängig, strikt einweggerichtet Quelle → Ziel und verwaltet weder
  Replikation noch Binlogs.
- Es synchronisiert keine Schemas, Indizes oder anderen Schemaobjekte.
  mysql-sync kann das Schema mit `--force-schema` angleichen.
- Es kann GroupBy/Stream auch für Tabellen ohne geeigneten Index nutzen;
  mysql-sync benötigt einen Primary Key und lehnt Tabellen ohne PK ab.
- Es kann bidirektional synchronisieren; mysql-sync absichtlich nicht.
- Beide unterstützen Bericht versus Ausführung: `--dry-run`/`--print` dort,
  `--check` hier. mysql-sync berechnet im Select-Modus MD5-Gruppen-Hashes in
  MySQL und hasht im Dump-Modus in Rust mit xxh3/blake3/usw.; pt-table-sync
  nutzt MySQL-seitige Checksummen und eigene Chunk-Algorithmen.

pt-table-sync passt, wenn replikationsbewusste Reparatur, bidirektionaler Sync
oder ein direktes DB-zu-DB-Perl-Werkzeug benötigt wird. mysql-sync passt, wenn
ein isolierter Quellenleser, ein authentifizierter Client/Server-Pfad,
einwegige Ziel-Updates und Schema-Abgleich gewünscht sind.

### data-diff (Datafold, Python)

Dokumentation: <https://data-diff.readthedocs.io/> ·
Quelle: <https://github.com/datafold/data-diff>

`data-diff` ist ein Python-CLI und eine Bibliothek zum Vergleichen von Tabellen
innerhalb oder zwischen SQL-Datenbanken. Der Cross-Database-Modus nutzt
Divide-and-Conquer-Checksummen, um abweichende Zeilen effizient zu finden, und
kann abweichende Zeilen samt Statistiken ausgeben.

Unterschiede zu mysql-sync:

- Es ist ein Diff-Werkzeug, kein Synchronisierer. Es meldet Unterschiede, führt
  aber keine Synchronisations-Schreibvorgänge aus; mysql-sync repariert die
  Differenzen.
- Es unterstützt viele SQL-Datenbanken und heterogene Cross-Database-Vergleiche;
  mysql-sync ist derzeit auf MySQL beschränkt.
- Es segmentiert über eine konfigurierte Key-/Update-Spalte. mysql-sync leitet
  Range-/Mod-Chunks automatisch aus dem Primary Key ab und unterstützt
  SELECT- und Dump-basiertes Hashing.
- Es hat keinen Schema-Abgleich und keine Trigger-Behandlung. mysql-sync bietet
  `--force-schema` und optionales Trigger-Aussetzen.
- data-diff eignet sich für Audits heterogener Systeme; mysql-sync für eine
  einwegige MySQL-Reparatur mit explizitem Zielserver.

> Hinweis: Datafold hat im Mai 2024 angekündigt, die aktive Entwicklung des
> Open-Source-Projekts data-diff einzustellen. Vor einem neuen
> Produktionseinsatz den Wartungsstatus prüfen.

## SEE ALSO

Quellcode: `src/sync.rs` (Diff-Engine), `src/sqlhash.rs` (SQL-Gruppen-/Zeilen-Hashes),
`src/hash.rs` (Rust-Hashing/Buckets), `src/hash_binary.rs` (rohe binäre Zeilen),
`src/hash_tsv.rs` (rohe TSV-Zeilen), `src/export.rs` (Export-Strategien),
`src/server.rs` (Zielseite), `vendor/mysql_async` (gepatchtes `next_raw()`).
