[English](README.md) | [Deutsch](README.DE.md)

# mysql-sync(1)

Start mysql-sync on source and target server and tell which tables to
synchronize by glob patterns.

SYNCHRONIZATION:
on target machine build hashes on chunks send to source machine,
if hashes differ hash rows -> send diff of chunk to server which applies
target.

A chunk is defined by int/binary/varchar primary keys eg any last n chars
such as 19.. (thus 1901 1902 1903 ..) or Benjam.. (eg for names such as
Benjamin)

## HOW IT WORKS
The client connects to server.
The client controls the server and tells how and what to sync or check.


## NAME

mysql-sync — differentially synchronize MySQL tables over QUIC (or plaintext
TCP) using multiple ways to process the data (SELECT vs dump as tsv vs ..) so
you can pick the fastest way

## SYNOPSIS

```
# SOURCE (CLIENT)
mysql-sync -C CONFIG [--force-schema|--force|--sync|--check N|--benchmark] [OPTIONS] [SELECTOR]

# TARGET (SERVER)
mysql-sync -C config.json
```

`-C -` (or `--config -`) reads the JSON config from **stdin**.

## DESCRIPTION

Compares source (client) and target (server) via bucket and row hashes and
transfers only differing rows. Transport is QUIC/TLS 1.3 with certificate
pinning. **Client and server must use the same `hash_algo`** (the Hello
handshake aborts otherwise), but `--hash-select` always uses MySQL `MD5`.

`SELECTOR` = `A.B` — `A` = DB list or `*`, `B` = table list or `*`.
Examples: `db1.*`, `db1,db2.*`, `db1.t1,t2`, `db1,db2.t1,t2`

| Mode | Effect |
|---|---|
| `--force-schema [SEL]` | Create/align schema and databases only. | (TODO better name such as --try-update-schema)
| `--force [SEL]` | Recreate the target table (data loss) |
| `--sync [SEL]` | Differential sync (INSERT/UPDATE/DELETE) |
| `--check N [SEL]` | Diff report + N sample IDs only; no writes; shows timings thus could be seen as dry-run option to predict the time to diff |

| `--benchmark [SEL]` | same as check but no need for server basically. For testing how fast it is to build chunked hashes on tables

| `--testing-drop-chunks S [SEL]` | from chunks S, S+1, S+2, S+3 drop all, first, last, random. It must be tetsed that missing chunks source/dest work, this implements dropping them

| Option | Meaning |
|---|---|
| `-C, --config FILE` | JSON config (required); `-` = stdin |

| `--hash-select` | Grouped MD5 fingerprints computed in MySQL (default, no mysqldump or mysql binary required but slower) |
| `--hash-dump`   | Diff hashes via export/parser (alias: `--check-dump`) |
| `--hash-raw-binary-row` | Hash raw MySQL binary-protocol rows (alias: `--check-raw-binary`) |
| `--hash-raw-binary-row-vec` | Hash raw MySQL binary-protocol rows (alias: `--check-raw-binary`) |
| `--hash-raw-tsv` | Hash raw `mysql --batch` TSV lines - no binary columns (alias: `--check-raw-tsv`) |
-> Should try fastest automatically

| `--no-progress` | No progress bars (cron/CI) |
| `--log-level LVL` | `error|warn|info|debug|trace` |

## tutorial
python test.py tutorial_up
python test.py step_1
python test.py step_2
python test.py tutorial_down

For shell usage eg see demo/tutorial.sh

## old immutable data
most projects have old and an auto increment field which no longer changes.

That's why per table sync_where and check_where fields have
bee n introduced to allow to reduce the load even more.

## HASHING — RECOMMENDATION

`--hash-dump` streams rows through the export/parser path and hashes them
in Rust with `hash_algo` (xxh3 by default). Use it when that exact export/parser
path must be exercised or special cases force `mysqldump`.

`--hash-raw-binary-row` reads prepared-statement result rows through a patched
`mysql_async` `next_raw()` accessor and hashes the native binary row packet
directly. It decodes only PK fields, and only when a differing bucket must be
drilled down. This is the fastest path for clean scans and small diffs.

`--hash-raw-tsv` starts `mysql --batch --quick --skip-column-names
--default-character-set=utf8mb4` on each side and hashes each escaped output
line. It avoids a second Rust row parser, but depends on an external `mysql`
CLI and is slower than raw-binary.

`--hash-select` (default) 
NOTE: happens to be slower than raw-binary/tsv due to nested function calls to
compute hashes when running on same host.

computes bucket fingerprints inside MySQL. One
`GROUP BY` query per table returns `bucket`, `COUNT(*)` and an MD5 group hash:
MySQL builds a null-safe, length-prefixed fingerprint per row, sums both 64-bit
MD5 halves per bucket, then hashes the aggregate. Only buckets with a different
hash/count trigger row hashes; only rows with a different PK/hash are exported
and sent.


Raw modes are **fail-closed**:

- The full session/server representation profile must match exactly: `VERSION()`,
  `@@version_comment`, `@@version_compile_os`, `@@version_compile_machine`,
  `character_set_client/connection/results`, `collation_connection`,
  `time_zone`, and `sql_mode`. A mismatch is rejected; use select or dump.
- The normalized schema hash must also match on both sides.
- `raw-tsv` additionally rejects tables with binary columns and requires the
  same `mysql` client behavior/version on both hosts. It does not fall back.

- `hash_algo` controls dump/raw mode only. Select mode always uses MySQL
  `MD5`, because MySQL has no built-in xxh3/blake3. Client and server still need
  the same `hash_algo` because the Hello handshake validates it.
- `--hash-select` avoids serializing/parsing rows, but MySQL `MD5` is slower
  per row than Rust xxh3. Measure both paths for your workload.
- `xxh3` is not cryptographic, but perfectly sufficient for drift detection
  and the fastest algorithm in dump mode.
- Choose `blake3` only when cryptographic collision resistance is required
  (e.g. an untrusted source). About 1.5× slower than xxh3.
- `xxh64`, `md5`, `sha256` are primarily for compatibility; not faster than
  xxh3 in dump mode.

**Test environment (measurement 2026-10-31, all hash paths):**
Linux x86_64, release build, MySQL 8.0 in Docker (`docker-compose.yml`,
source `:7012`). Dataset: synthetic table `db_bench.big` with **100,000 rows**
(id, VARCHAR MD5 string, INT, DATETIME), `chunked=3`.

`--benchmark` generates the chunk hashes locally and discards them. It needs no
server and performs no writes; the measured time is the local scan + hashing
work that dominates a clean synchronization.

| mode | time (median of 5) | rows/s |
|---|---:|---:|
| select (SQL MD5) | 871 ms | 114,850 |
| dump (Rust xxh3) | 489 ms | 204,624 |
| raw-binary | **100 ms** | 998,004 |
| raw-tsv | 185 ms | 540,000 |

The dump path is fast because `select-stream` sends native rows to the Rust
process and xxh3 hashes them. Raw-binary avoids building `Vec<Value>` and
hashing text: it hashes the binary protocol packet directly and decodes only
the PK fields it needs. Raw-tsv additionally pays for the external `mysql`
process and TSV escaping/unescaping per row. The numbers are indicative —
reproduce them on your own hardware.

**Reproducing the benchmark** (test data is generated, not shipped):

```bash
# 1) create the 100k-row table on the source (see test.py TableSchema or your own data)
# 2) benchmark all hash paths locally (no server required)
mysql-sync -C config-client.json --benchmark db_bench.big

# one path only (selector directly after --benchmark)
mysql-sync -C config-client.json --benchmark db_bench.big --hash-raw-binary-row
```

Output is tab-separated: `table`, `mode`, `sync_ms`, `rows`, `buckets`,
`rows_per_s`, so it can be sorted with standard tools. Use
`--hash-select|dump|raw-binary|raw-tsv` to restrict the run to one path.

All algorithms are compiled in by default (`blake3`, `xxhash`, `md5`, `sha2`).
Minimal build: `cargo build --no-default-features --features xxhash`.

## SOURCE SAFETY & EXPORT SIDE EFFECTS

The client reads source tables only: it issues `SELECT`/`SHOW` queries and never
executes `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `LOAD DATA`, or DDL against the
source. All writes are protocol requests executed by the server on the target.

`dump_strategies` controls how source rows are fetched:

| Strategy | Source access | Requirements / side effects |
|---|---|---|
| `select-stream` | streaming `SELECT` | None. Default and fastest. |
| `to_file[:DIR]` | `SELECT ... INTO OUTFILE` | Requires `FILE` privilege and a permitted `secure_file_priv` path on the MySQL host. Writes a temporary TSV on the source host, then reads and removes it. **Slow for large tables**: the complete result is written to disk and read back instead of streaming. A crash or remote-host path problem can leave the file behind. |
| `mysql-e` | external `mysql --batch -e "SELECT ..."` | Requires the `mysql` CLI on the client host. `SELECT`-only; no table or file writes. Uses `host`/`port`; `mysql.socket` is ignored. |
| `mysqldump` | `mysqldump --skip-lock-tables --single-transaction --skip-triggers` | Read-only. `INSERT` statements are parsed, never executed locally. Automatically preferred first for binary columns. |

Fallback only happens if a strategy fails before producing any row. To avoid the
`to_file` side effect entirely, use `mysql-e` directly:

```json
{ "dump_strategies": ["mysql-e", "mysqldump"] }
```

For strict protection, give the client a source account with only read
privileges (`SELECT`/`SHOW`; add `FILE` only if `to_file` is required). The
server must use a separate writable account for the target. If the server config
points at the source instance, its writes would modify the source.

## CHUNK MODES

Most database tables have a very simlpe order.
primary id fields evenly distirbuted incrementing.
-> That's what' range is for
-> See **Range**

If you think about arbitrary keys such as dates/floats whatever :-/
there is no real structure to divide the data into.
Maybe it would be best to find biggest/smallest and divide into 2 buckets
keep dividing till buckets are reasonably small or just scan and hash
-> This case I honestly don't care about
-> **Mod** might go await/be rewritten changed whatever


The chunk mode is selected automatically per table from the primary-key metadata;
there is no `chunk_mode` config key. `chunked` controls the bucket width/suffix.

- **Range** is used for exactly one supported PK column: INTEGER, CHAR/VARCHAR, or
  BINARY/VARBINARY (without prefix index). Numeric PKs use
  `floor(pk / 10^chunked) * 10^chunked` (`chunked=3` → 1,000-value ranges);
  CHAR/BINARY keys remove the last `chunked` characters/bytes. Every chunk
  SELECT/export is ordered by PK.

  In primary key tuples the last field is chunked only right now

- **Mod** is chosen automatically as the fallback for composite PKs and other
  single-column types (DATE/TIME, DECIMAL, FLOAT/DOUBLE, ENUM/SET, BIT,
  TEXT/BLOB prefix keys, …). It uses modulo/suffix buckets: the last `chunked`
  characters of the canonical PK component, or `ABS(pk) % 10^N` for numeric
  components. Composite keys bucket by their last component. Date keys use the
  date-only form.

- Mod buckets are not contiguous, so they generally cannot use PK range
  predicates and may scan the table.

Range chunks can be large: a single changed row changes the whole chunk hash.
The row-count threshold heuristic therefore only applies to mod; range mode
always drills down to exact row hashes after a chunk mismatch. (The target
design drops this heuristic entirely — see *SYNC ALGORITHM — TARGET DESIGN*.)

## SYNC ALGORITHM — TARGET DESIGN

This section describes how `--sync` is supposed to work.

Current state: the *compare* phase is still multi-pass (compute all local chunk
hashes, stream all remote chunk hashes, collect `diff_buckets`, then re-read
the differing chunks once for row hashes and once for the row data). The
*write* phase is already streaming and batched: changes of a differing bucket
are pushed as multi-row `REPLACE` statements (500 rows per statement) inside
one transaction per dump, so writes no longer cost a round trip and a commit
per row. The truncate/full-copy fallback has been removed — a table with a high
diff ratio is always resolved row-wise.

The target is a single forward streaming pass per table:

```
for chunk in chunks:                        # deterministic order, one scan
    lh = source.chunk_hash(chunk)           # local side
    rh = target.chunk_hash(chunk)           # remote side, same chunk

    if lh == rh:
        continue                            # identical chunk, nothing to do

    for row in source.rows(chunk):          # only this chunk is read again
        rh_row = target.row_hash(row.pk)
        if rh_row == None:        emit INSERT(row)
        elif rh_row != row.hash:  emit UPDATE(row)
        else:                     skip
    for pk in target_only(chunk):
        emit DELETE(pk)

    push(batch)                             # apply now, then continue scanning
```

Properties that follow from this:

- **One forward pass per side.** Chunks are compared as they are scanned; there
  is no global `diff_buckets` list and no second full-table pass.
- **Drill-down is local to the chunk.** A differing chunk re-reads only its own
  rows on both sides. Row-level comparison happens for that chunk only.
- **Writes overlap scanning.** Changes of chunk *N* are pushed while chunk *N+1*
  is already being scanned; they are batched (one multi-row `INSERT … ON
  DUPLICATE KEY UPDATE` / one `DELETE … IN (…)` per batch), not one round trip
  and not one fsync per row.
- **No truncate / full-copy fallback.** A chunk with many differing rows is
  still resolved row-wise (implemented). `diff_threshold_percent` only survives
  as an early-exit estimate in `--check`, where it never writes anything.
- **Bounded memory.** Only one chunk's rows and one write batch are buffered,
  not the whole table.
- **`--check N` is the same loop** with the `push(batch)` step removed and the
  first `N` differing PKs collected as examples.

Composite primary keys must use range chunks via row-value comparison
(`(id, id2) > (?, ?)`), so they do not fall back to `mod` and do not pay the
mod-scan cost.

### Row sources — four separate implementations

The four `--hash-*` modes are four different ways to produce the *same*
information (chunk hash, row hash, PK, row payload for changed rows). They
differ in **where hashing happens** and **whether row bytes cross the wire**,
so they are kept as four independent implementations, all consumed through one
thin engine interface — not merged into one "smart" path and not duplicated
engines.

| Mode | Hash computed by | Row bytes transferred | File |
|---|---|---|---|
| `select` (default) | MySQL: `GROUP BY` over a null-safe, length-prefixed row fingerprint (MD5) | no row data, only bucket/row hashes | `src/sqlhash.rs` |
| `dump` | Rust, over the export/parser stream (`select-stream`, `INTO OUTFILE`, `mysql -e`, `mysqldump`) | row values for compared chunks | `src/export.rs` + `src/hash.rs` |
| `raw-binary` | Rust, over native MySQL binary-protocol row packets | raw row packets | `src/hash_binary.rs` |
| `raw-tsv` | Rust, over `mysql --batch` TSV lines | escaped text lines (no binary columns) | `src/hash_tsv.rs` |

Keeping them separate is deliberate:

- `select` never moves row data; MySQL hashes in place. It cannot be expressed
  through the dump/CSV path without moving data that the select path does not
  move.
- `dump`/`raw-tsv` read *text* (escaping, locales, NULL as `\N`); `raw-binary`
  reads the binary protocol (length-encoded, NULL bitmap). Sharing one parser
  would force one representation onto the other.
- `raw-tsv` cannot represent binary columns at all and must reject such tables;
  that constraint belongs with its implementation.

The engine (`src/sync.rs`) owns chunk order, comparison, batching and
transport; a row source only answers "chunk hash" and "rows of this chunk".
Proposed layout, one file per strategy:

```
src/hashvia/mod.rs          trait RowSource { chunk_hash(chunk); rows(chunk) }
src/hashvia/select.rs       = today's src/sqlhash.rs
src/hashvia/dump.rs         = today's src/export.rs + hash glue
src/hashvia/raw_binary.rs   = today's src/hash_binary.rs
src/hashvia/raw_tsv.rs      = today's src/hash_tsv.rs
```

The target-side writer (`src/server.rs`, `src/import.rs`) stays a single
implementation; it is not duplicated per strategy.

## CONFIG

`config-server.json` (target):

```json
{
  "mysql": { "user": "root", "password": "…", "host": "127.0.0.1", "port": 3306 },
  "listen": "0.0.0.0",
  "port": 4433,
  "auth_token": "optional-shared-secret",
  "hash_algo": "xxh3",
  "transport": "quic",
  "compression": "none",
  "compression_level": 3
}
```

`config-client.json` (source):

```json
{
  "mysql": { "user": "root", "password": "…", "host": "127.0.0.1", "port": 3306 },
  "server": "target-host.example.com:4433",
  "server_fingerprint": "sha256:…",
  "auth_token": "same-secret-as-server",
  "hash_algo": "xxh3",
  "transport": "quic",
  "compression": "none",
  "compression_level": 3,
  "allow_csv": true,
  "dump_strategies": ["select-stream"],
  "transfer_batch_rows": 1000,
  "chunked": 4,
  "concurrent_processes": 3,
  "diff_threshold_percent": 10,
  "disable_triggers": false,
  "database_mappings": { "db_old": "db_new" }
}
```

**Password/auth:** `auth_token` must be **identical** on client and server.
The client sends it in the first request (`Hello`) over the encrypted QUIC/TLS
connection; the server compares it and otherwise replies with
`Auth-Token ungültig`. If no `auth_token` is set on the server, no token is
checked. Always set it in production — together with `server_fingerprint`
(pinning). Secrets can be passed via stdin: `-C -`.

Other keys: `transport` (`quic`/`tcp`), `allow_plaintext`, `compression`
(`none`/`zstd`), `compression_level` (1–22), `max_frame`, `max_decompressed`,
`allow_csv` (`false` = SQL REPLACE dumps), `dump_strategies` (ordered with
fallback), `transfer_batch_rows` (alias `dump_threshold`), `chunked` (1–6), `concurrent_processes`,
`diff_threshold_percent`, `disable_triggers`, `database_mappings`,
`database_table_mappings`, `hash_strategies`, `float_precision`, `tables`.
Chunk mode is automatic and is not a config key.

### Hash path, tables and fields

`hash_strategies` is an ordered try-list; the first usable path wins. A CLI
`--hash-*` flag forces exactly one path and skips the list. Valid names:
`raw-binary`, `raw-binary-vec`, `select`, `dump`, `raw-tsv`. Default is
`["select"]`.

`tables` overrides apply per table. The key is a glob over `"db.table"`:
`*` matches any run of characters, `?` exactly one. The **first matching entry
in config order wins** — entries are not merged. Format the JSON so the more
specific selectors come first.

```json
{
  "hash-strategies": ["raw-binary", "raw-binary-vec", "select"],
  "float-precision": "auto",
  "tables": {
    "*.orders": {
      "strategies": ["raw-binary-vec", "select"],
      "fields": {
        "price":    { "float-precision": 2 },
        "currency": { "transform": "ignore" },
        "email":    { "transform": "lower" }
      }
    },
    "shop.*": {
      "fields": { "ratio": { "float-precision": 6 } }
    }
  }
}
```

Field options are looked up by column name (case-insensitive):

- `float-precision`: `"off"`, `"auto"` (default) or `0..=30`.
- `transform`: `"trim"`, `"lower"` or `"ignore"`. Transforms change row
  equality, so they restrict the table to paths that decode values:
  `select`, `dump` or `raw-binary-vec`. `raw-binary`/`raw-tsv` are skipped
  automatically (forced CLI paths then fail with an error).
- Overrides on primary-key columns are rejected.

Precedence: field override > global `float_precision` > built-in default
(`auto`). DECIMAL/NUMERIC is exact and is never rounded; an explicit numeric
`float-precision` on a non-FLOAT/DOUBLE column is an error.

The resolved path, field options and float precision are logged per table, e.g.
`[shop.orders] Feld-Optionen: 2 fuer price (Feld), lower fuer email`.

#### DETAILS ABOUT TYPES

| Column type | `auto` on text paths | Explicit `float-precision` | `trim`/`lower` |
|---|---|---|---|
| `FLOAT(M,D)`, `DOUBLE(M,D)` | round to `D` | round to `N` | rejected |
| `FLOAT`, `DOUBLE` without `(M,D)` | off | round to `N` | rejected |
| `DECIMAL`, `NUMERIC` | exact, never rounded | rejected (error) | rejected |
| `CHAR`, `VARCHAR`, `TEXT`, `ENUM`, `SET` | unchanged | rejected (error) | allowed |
| `BINARY`, `VARBINARY`, `BLOB` | unchanged | rejected (error) | rejected |
| integers, `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, JSON, … | unchanged | rejected (error) | rejected |

Rounding applies to text rendering only. `raw-binary` and `raw-tsv` hash raw
bytes and ignore `float-precision`; `raw-binary-vec` decodes values and applies
it like `select`/`dump`. Use `"off"` for bit-exact FLOAT/DOUBLE comparison on
text paths. Two servers with the same schema otherwise format floats
identically, so for same-version migrations prefer `raw-binary` and leave
precision at `auto`.

On first server start, the certificate and `<config>.fingerprint` are created
next to the config; pin the fingerprint in the client as
`"server_fingerprint": "sha256:…"`. With `-C -` (stdin), the server writes the
TLS files as `config-server-*.pem` / `config-server.fingerprint` into the
working directory.

## TRANSPORT, ENCRYPTION & COMPRESSION

- **Default `transport: "quic"`**: QUIC with TLS 1.3, always encrypted. There is
  no switch to disable encryption on QUIC; use `server_fingerprint` pinning and
  `auth_token`.
- **`transport: "tcp"` is plaintext** and only allowed together with
  `"allow_plaintext": true` on both sides. The process logs a loud warning. Use
  it only inside a tunnel that already provides confidentiality.
- **Compression**: `"compression": "none"` (default) or `"zstd"`. With `zstd`,
  every frame is compressed — no size threshold, no heuristics —
  `compression_level` 1–22 (default 3). Frames are self-describing, so each
  side may choose its own setting.
- **Wire format v7**: `U<len>\n<body>` (uncompressed) or `C<len>\n<body>`
  (zstd), `<body>` = rkyv-serialized `Request`/`Response`; ALPN
  `mysql-sync/7`. Client and server must be built from the same version.
  Primary keys travel as structured component lists (`Vec<String>`), never as
  separator-joined strings.

## TRUST MODEL

After a successful `Hello` (protocol version + `auth_token`), the peer's wire
bytes are considered correct. rkyv decoding still checks structural validity so
malformed frames become clean errors instead of undefined behavior, but this is
**not a security boundary**: a correctly authenticated peer can cause damage
anyway by sending **correctly formatted but semantically wrong SQL** (e.g.
`SendeDump` with `DROP TABLE`). Integrity/confidentiality come from QUIC/TLS and
the token, not from payload validation. Only `Hello` is parsed before token
verification.

## EXAMPLES

```bash
mysql-sync -C config-server.json
mysql-sync -C config-client.json --force-schema db1,db2
mysql-sync -C config-client.json --force --sync db1.*,db2.*
mysql-sync -C config-client.json --check 10 "db1.*" || alert "DB drift!"
mysql-sync -C config-client.json --check 10 --hash-dump "db1.t1"

# config from stdin (e.g. from a secret store)
cat config-client.json | mysql-sync -C - --check 10 "db1.*"
```

`--check-select`, `--check-dump`, `--check-raw-binary`, and
`--check-raw-tsv` remain valid aliases for their `--hash-*` forms.

For `--sync`, put the selector directly after the flag (or after `--check N`).
`--sync` accepts an optional selector; if another option sits between `--sync`
and the selector, the default selector `*` is used.

## EXIT STATUS

`--force*`/`--sync`: `0` = ok, `1` = error.  
`--check`: `0` = identical, `1` = differences, `2` = error.

## TESTING

Docker only. `test.py` starts two MySQL containers and runs the matrix
(source vs target, rust modes vs pt-table-sync, with chunks deliberately
removed from both sides).

```bash
python3 test.py up matrix
```

Everything needed is set in `test.py`; no environment variables have to be
exported.

## ALTERNATIVES

### pt-table-sync (Percona Toolkit, Perl)

Documentation: <https://docs.percona.com/percona-toolkit/pt-table-sync.html> ·
Source: <https://github.com/percona/percona-toolkit> ·
Single-file download: <https://www.percona.com/get/pt-table-sync>

`pt-table-sync` is a Perl utility shipped with Percona Toolkit. It connects
directly to one or more MySQL DSNs, finds row differences with chunk/nibble/
group-by/stream algorithms, and can either print the repair SQL or execute
`INSERT`/`REPLACE`/`UPDATE`/`DELETE` statements on a destination.

Connection model: `pt-table-sync` is a Perl process with network reachability
to both MySQL endpoints. It opens ordinary MySQL client connections to the
source and destination; MySQL itself does not connect to MySQL. No `FEDERATED`
engine, database link, or Unix `root` account is involved. The MySQL accounts
must allow connections from the host running `pt-table-sync` and need `SELECT`
on the source and `INSERT`/`UPDATE`/`DELETE`/`REPLACE` on the target.
Replication-related options (`--sync-to-source`, `--replicate`) may additionally
require `SUPER` or `REPLICATION CLIENT`. Using the MySQL `root` account is
normally unnecessary and not recommended.

Differences from mysql-sync:

- Direct MySQL connections from the `pt-table-sync` process to both endpoints,
  instead of mysql-sync's client/server QUIC transport. It relies on MySQL's own
  connection security; mysql-sync provides QUIC/TLS, fingerprint pinning,
  authentication, and optional zstd.
- Replication-aware. It supports one-way and bidirectional synchronization,
  `--sync-to-source` repair, and reusing `pt-table-checksum --replicate` results.
  mysql-sync is topology-agnostic, strictly one-way source → target, and does
  not manage replication or binlogs.
- It does not synchronize schemas, indexes, or other schema objects. mysql-sync
  can align the schema with `--force-schema`.
- It can use GroupBy/Stream algorithms for tables without a suitable index;
  mysql-sync requires a primary key and rejects tables without one.
- It can run bidirectional sync; mysql-sync intentionally cannot.
- Both support reporting versus execution: `--dry-run`/`--print` there,
  `--check` here. mysql-sync computes MD5 group hashes in MySQL for select mode
  and hashes in Rust with xxh3/blake3/etc. for dump mode; pt-table-sync uses
  MySQL-side checksums and its own chunking algorithms.

Use pt-table-sync when you need replication-aware repair, bidirectional sync,
or a direct DB-to-DB Perl utility. Use mysql-sync when you want an isolated
source reader, an authenticated client/server path, one-way target updates, and
schema alignment.

### data-diff (Datafold, Python)

Documentation: <https://data-diff.readthedocs.io/> ·
Source: <https://github.com/datafold/data-diff>

`data-diff` is a Python CLI and library for comparing tables within or across
SQL databases. Its cross-database mode uses divide-and-conquer checksums to find
differing rows efficiently and can output differing rows plus statistics.

Differences from mysql-sync:

- It is a diff tool, not a synchronizer. It reports differences but does not
  perform synchronization writes; mysql-sync performs the repair.
- It supports many SQL databases and heterogeneous cross-database comparisons;
  mysql-sync currently targets MySQL only.
- It segments by a configured key/update column. mysql-sync derives automatic
  range/mod chunks from the primary key and supports select- and dump-based
  hashing.
- It has no schema synchronization or trigger handling. mysql-sync provides
  `--force-schema` and optional trigger suspension.
- data-diff is useful for auditing heterogeneous estates; mysql-sync is for
  applying a one-way MySQL repair with an explicit target server.

> Note: Datafold announced that it stopped active development of the open-source
> data-diff project in May 2024. Check its maintenance status before adopting it
> for new production use.

## FUTURE IMPROVEMENTS

TODO: Maybe its not very smart to allow the client to
control the server, maybe it should be the other way
round.

Maybe concurrent processing within a table in case if all data are in
RAM that would be even faster.

## SEE ALSO

Source: `src/sync.rs` (diff engine), `src/sqlhash.rs` (SQL group/row hashes),
`src/hash.rs` (Rust hashing/buckets), `src/hash_binary.rs` (raw binary rows),
`src/hash_tsv.rs` (raw TSV lines), `src/export.rs` (export strategies),
`src/server.rs` (target side), `vendor/mysql_async` (patched `next_raw()`).



## BENCHMARKS
TC2* -> more columns thus somewhat realistic


10.000 rows on XEON intel CPU within incus->docker

test mysqls sync  details

SLOW mysqlump and perl


HEADERS:
table-schema, mysql-versions, sync time, test setup description

TC201-mixed-tuple-no_binary	8.0:9.7	1785	pt clean=- drift=1075 total=2860 ok
TC201-mixed-tuple-no_binary	8.0:8.0	1577	pt clean=- drift=1240 total=2818 ok
TC201-mixed-tuple-no_binary	8.0:8.0	1240	mysqldump/none clean=- drift=- total=1240 ok
TC201-mixed-tuple-no_binary	8.0:8.0	1215	mysqldump/zstd clean=- drift=- total=1215 ok
TC201-mixed-tuple-no_binary	8.0:9.7	1121	mysqldump/none clean=- drift=- total=1121 ok
TC201-mixed-tuple-no_binary	8.0:9.7	1108	mysqldump/zstd clean=- drift=- total=1108 ok

SAME FAST -> rust ...
TC201-mixed-tuple-no_binary	8.0:8.0	215	rust/dump clean=182 drift=233 total=630 ok
TC201-mixed-tuple-no_binary	8.0:8.0	243	rust/raw-binary-vec clean=114 drift=175 total=532 ok
TC201-mixed-tuple-no_binary	8.0:8.0	297	rust/select-crc32 clean=79 drift=125 total=501 ok
TC201-mixed-tuple-no_binary	8.0:8.0	299	rust/raw-binary clean=128 drift=110 total=537 ok
TC201-mixed-tuple-no_binary	8.0:8.0	362	rust/select-sha2 clean=149 drift=170 total=681 ok
TC201-mixed-tuple-no_binary	8.0:8.0	793	rust/raw-tsv clean=367 drift=957 total=2117 ok
TC201-mixed-tuple-no_binary	8.0:9.7	180	rust/select-crc32 clean=128 drift=124 total=432 ok
TC201-mixed-tuple-no_binary	8.0:9.7	212	rust/select-sha2 clean=209 drift=159 total=579 ok
TC201-mixed-tuple-no_binary	8.0:9.7	323	rust/raw-binary-vec clean=101 drift=180 total=604 ok
TC201-mixed-tuple-no_binary	8.0:9.7	374	rust/dump clean=159 drift=186 total=720 ok



Other table
same are slow (dump and perl)
TC202-mixed-tuple-bin	8.0:8.0	2612	mysqldump/none clean=- drift=- total=2612 ok
TC202-mixed-tuple-bin	8.0:8.0	2474	mysqldump/zstd clean=- drift=- total=2474 ok
TC202-mixed-tuple-bin	8.0:8.0	2414	pt clean=- drift=1271 total=3685 ok
TC202-mixed-tuple-bin	8.0:9.7	1652	pt clean=- drift=1227 total=2878 ok
TC202-mixed-tuple-bin	8.0:9.7	1550	mysqldump/none clean=- drift=- total=1550 ok
TC202-mixed-tuple-bin	8.0:9.7	1504	mysqldump/zstd clean=- drift=- total=1504 ok

rust are al fast 2 outliers expected because .. well I ran 12 in parallel

TC202-mixed-tuple-bin	8.0:8.0	1449	rust/select-sha2 clean=301 drift=307 total=2056 ok
TC202-mixed-tuple-bin	8.0:8.0	1361	rust/select-crc32 clean=135 drift=139 total=1636 ok
TC202-mixed-tuple-bin	8.0:9.7	279 Y	rust/select-crc32 clean=79 drift=285 total=642 ok
TC202-mixed-tuple-bin	8.0:9.7	388 Y	rust/select-sha2 clean=202 drift=328 total=917 ok

TC202-mixed-tuple-bin	8.0:8.0	1117	rust/raw-binary clean=97 drift=210 total=1423 ok
TC202-mixed-tuple-bin	8.0:8.0	146	rust/raw-binary-vec clean=59 drift=114 total=319 ok
TC202-mixed-tuple-bin	8.0:8.0	765	rust/dump clean=249 drift=537 total=1551 ok
TC202-mixed-tuple-bin	8.0:9.7	192	rust/raw-binary-vec clean=124 drift=116 total=431 ok
TC202-mixed-tuple-bin	8.0:9.7	718	rust/dump clean=333 drift=1892 total=2943 ok


Example run 100000 rows
TC201-mixed-tuple-bin   8.0:8.0 -       rust/raw-tsv(xxh3,c3,none,quic) clean=- drift=- total

TC201-mixed-tuple-bin   8.0:8.0 22600   pt clean=- drift=7524 total=30124 ok    8745da64
TC201-mixed-tuple-bin   8.0:9.7 11663   pt clean=- drift=8092 total=19755 ok    1f422a98
TC201-mixed-tuple-bin   8.0:8.0 10473   mysqldump/none clean=- drift=- total=10473 ok   32759
TC201-mixed-tuple-bin   8.0:9.7 11663   pt clean=- drift=8092 total=19755 ok    1f422a98
TC201-mixed-tuple-bin   8.0:8.0 10473   mysqldump/none clean=- drift=- total=10473 ok   32759
TC201-mixed-tuple-bin   8.0:8.0 5842    rust/dump(xxh3,c3,none,quic) clean=1915 drift=7487 to
TC201-mixed-tuple-bin   8.0:9.7 5270    rust/dump(xxh3,c3,none,quic) clean=881 drift=9471 tot
TC201-mixed-tuple-bin   8.0:8.0 4701    mysqldump/zstd clean=- drift=- total=4701 ok    ed988
TC201-mixed-tuple-bin   8.0:9.7 3756    mysqldump/zstd clean=- drift=- total=3756 ok    a0d95
TC201-mixed-tuple-bin   8.0:9.7 2912    mysqldump/none clean=- drift=- total=2912 ok    0b2ba
TC201-mixed-tuple-bin   8.0:8.0 2438    rust/raw-binary(xxh3,c3,none,quic) clean=423 drift=90
TC201-mixed-tuple-bin   8.0:8.0 2427    rust/select-crc32(xxh3,c3,none,quic) clean=648 drift=
TC201-mixed-tuple-bin   8.0:8.0 2438    rust/raw-binary(xxh3,c3,none,quic) clean=423 drift=90
TC201-mixed-tuple-bin   8.0:8.0 2427    rust/select-crc32(xxh3,c3,none,quic) clean=648 drift=
TC201-mixed-tuple-bin   8.0:9.7 1737    rust/select-sha2(xxh3,c3,none,quic) clean=1412 drift=
TC201-mixed-tuple-bin   8.0:9.7 1737    rust/select-sha2(xxh3,c3,none,quic) clean=1412 drift=
TC201-mixed-tuple-bin   8.0:8.0 1244    rust/select-sha2(xxh3,c3,none,quic) clean=1121 drift=
TC201-mixed-tuple-bin   8.0:8.0 1277    rust/raw-binary-vec(xxh3,c3,none,quic) clean=475 drif
TC201-mixed-tuple-bin   8.0:8.0 1244    rust/select-sha2(xxh3,c3,none,quic) clean=1121 drift=
TC201-mixed-tuple-bin   8.0:8.0 1277    rust/raw-binary-vec(xxh3,c3,none,quic) clean=475 drif
TC201-mixed-tuple-bin   8.0:9.7 941     rust/select-crc32(xxh3,c3,none,quic) clean=705 drift=
TC201-mixed-tuple-bin   8.0:9.7 669     rust/raw-binary-vec(xxh3,c3,none,quic) clean=353 drif
TC201-mixed-tuple-bin   8.0:9.7 941     rust/select-crc32(xxh3,c3,none,quic) clean=705 drift=
TC201-mixed-tuple-bin   8.0:9.7 669     rust/raw-binary-vec(xxh3,c3,none,quic) clean=353 drif

bin expected can't be run through TSV export
TC201-mixed-tuple-bin   8.0:9.7 -       rust/raw-binary(xxh3,c3,none,quic) clean=- drift=- to
TC201-mixed-tuple-bin   8.0:9.7 -       rust/raw-tsv(xxh3,c3,none,quic) clean=- drift=- total
TC201-mixed-tuple-bin   8.0:9.7 -       rust/raw-binary(xxh3,c3,none,quic) clean=- drift=- to
TC201-mixed-tuple-bin   8.0:8.0 -       rust/raw-tsv(xxh3,c3,none,quic) clean=- drift=- total
TC201-mixed-tuple-bin   8.0:9.7 -       rust/raw-tsv(xxh3,c3,none,quic) clean=- drift=- total

### Why select modes are faster on 8.0→9.7

With only 10,000 rows, the select modes' sync time is dominated by the time to
start the hash query and wait for the first results — not by the actual hashing
computation. Both source and destination must compute checksums for all chunks,
so the total time is mostly waiting for the slower side to finish.

The `select-sha2` and `select-crc32` modes run SQL on both servers:
`SHA2(CONCAT(...))` or `CRC32()` chains with `GROUP BY` aggregation. MySQL 9.7
has a faster query optimizer for these, so on 8.0→9.7 the destination side
returns hashes quickly. On 8.0→8.0 both sides use the slower optimizer, making
select modes 10× slower than `raw-binary-vec`.

To see real differences between hash algorithms, the table needs millions of
rows so the actual computation dominates over query startup latency.
