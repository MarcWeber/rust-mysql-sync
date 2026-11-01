#!/usr/bin/env python3
"""

Docker only. test.py starts two MySQL containers, creates/fills tables from
the Python TableSchema class, deliberately removes chunks on both sides and
runs the rust-sync vs pt-table-sync matrix.

Each case gets its own table in db_matrix, derived from its dimension hash.

Actions:
  up     : start the Docker MySQL containers and wait until they answer
  matrix : run the comparison matrix (rust modes vs pt-table-sync)
  down   : stop/remove the containers
  clean  : stop/remove the containers and their volumes

python3 test.py up matrix
Exit 0 = all executed actions passed.
"""

from typing import Literal, Tuple
from dataclasses import dataclass
from itertools import product
from random import random
import asyncio
from asyncio import Semaphore, gather
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import traceback
import yaml
from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime
from pathlib import Path
from yachalk import chalk

from u_asyncio import run_show_out_tail_on_error, task
from u_no_proc_zombies import ProcManager
from u_benchmark import context_benchmark
from u_redirect_logging import with_logging
from u_systeminfo import system_info

LOCK_FROM_TABLES = defaultdict(asyncio.Lock)

ProcManager()  # kill child processes on exit

ROOT_PW = "syncroot"
SERVER_PORT_OFFSET = 100
MATRIX_DB = "db_matrix"
MATRIX_ROWS = 100000
CHUNK_SIZE = 2
MATRIX_JOBS = 4
FAIL_EARLY = True
QUICK = False

TEST_TEXT = "A\nB\n\\BinärmüllÄß"

MYSQL_INSTANCES_CONFIG = [
    # name version port
    ["mysql80", "8.0", 7012],
    ["mysql97", "9.7", 7013]
]

TABLE_SCHEMA_CASES = {
    # number of primary keys

    # these cases should represent all primary key cases
    # ond the left side and all potential data cases on the right side

    # single primary key types
    "TC101": "1:int(10):varchar(50)",
    "TC102": "1:varchar(10):int(10)",
    "TC103": "1:blob:varchar(10)",
    "TC104": "1:int(10):varchar(10):int(10):blob:float(2):float(0):text:currency", # all fields

    # primary key tuples
    "TC201": "2:int(10):varchar(10):int(10):blob:float(2):float(0)",
    "TC202": "2:varchar(10):int(10):int(10):blob:float(2):float(0)"
}


HERE = Path(__file__).resolve().parent
WORK = HERE / ".test-run"


def _cargo_target_dir():
    override = os.environ.get("CARGO_TARGET_DIR")
    if override:
        path = Path(override).expanduser()
        return path if path.is_absolute() else HERE / path
    if shutil.which("cargo"):
        try:
            meta = subprocess.check_output(
                ["cargo", "metadata", "--format-version", "1", "--no-deps"],
                cwd=HERE, text=True, stderr=subprocess.DEVNULL,
            )
            return Path(json.loads(meta)["target_directory"])
        except (OSError, subprocess.CalledProcessError, KeyError, ValueError):
            pass
    return HERE / "target"

def _default_bin():
    return _cargo_target_dir() / "release" / "mysql-sync"


if os.environ.get("MSYNC_BIN"):
    BIN = Path(os.environ["MSYNC_BIN"]).expanduser().resolve()
else:
    BIN = _default_bin()

PASS, FAIL = chalk.green("PASS"), chalk.red("FAIL")
CONCURRENCY = 1

@dataclass()
class TableSchemaField:
    type: str

    def sql_type(self):
        t = self.type.lower()
        if t.startswith("int"):
            return t.upper()
        if t.startswith("varchar"):
            return t.upper()
        if t.startswith("text"):
            return "TEXT"
        if t.startswith("blob"):
            return "VARBINARY(128)"
        if t.startswith("float"):
            return "FLOAT"
        if t.startswith("currency"):
            return "DECIMAL(10,2)"
        raise ValueError(f"unbekannter Feldtyp: {self.type}")

    def sql_value_from_n(self, n="n"):
        t = self.type.lower()
        if t.startswith("int"):
            return n
        if t.startswith("varchar"):
            return f"CONCAT('LINE', {n})"
        if t.startswith("text"):
            text = (TEST_TEXT + " ") * 150
            return f"CONCAT(CONVERT(UNHEX('{text.encode().hex()}') USING utf8mb4), {n})"
        if t.startswith("blob"):
            return f"BIN({n})"
        if t.startswith("float"):
            return f"({n} / 10)"
        if t.startswith("currency"):
            return f"({n} / 10)"
        raise ValueError(f"unbekannter Feldtyp: {self.type}")




@dataclass()
class TableSchema:
    primary_key_fields: int
    fields: list[TableSchemaField]

    @staticmethod
    def from_str(s):
        x = s.split(':')
        pkf = int(x[0])
        return TableSchema(pkf, [TableSchemaField(t) for t in x[1:]])

    def drop_recreate_sql(self, name, row_count):
        """
        assume all fields are called a,b,c,..

        DROP {name} IF EXISTS
        CREATE {name}

        INSERT INTO {name} (flight_no, origin, dest, price, seats, notes, created)
        SELECT
          CONCAT('SKW', LPAD(n, 5, '0')),
          CONCAT('City-', n % 97, CASE WHEN n % 13 = 0 THEN '''s End' ELSE '' END),
          CONCAT('Dest-', (n * 7) % 89),
          CASE WHEN n % 11 = 0 THEN NULL ELSE ROUND(19.99 + (n % 400) * 1.37, 2) END,
          60 + (n % 240),
          CASE
            WHEN n % 23 = 0 THEN NULL
            WHEN n % 23 = 1 THEN CONCAT('Zeile ', n, '\tMit\tTabs')
            WHEN n % 23 = 2 THEN CONCAT('Zeile ', n, '\nMit\nNewlines')
            WHEN n % 23 = 3 THEN CONCAT('Backslash ', n, '\\')
            ELSE CONCAT('Notiz äöüß Nr. ', n)
          END,
          DATE_ADD('2024-01-01 00:00:00', INTERVAL n MINUTE)
        FROM (
          SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 + 1 AS n
          FROM (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
               (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
               (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
               (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5) d
        ) nums
        WHERE n <= 5000;

        """
        letters = "abcdefghijklmnopqrstuvwxyz"
        cols = ", ".join(letters[:len(self.fields)])
        col_defs = ", ".join(
            f"`{letters[i]}` {self.fields[i].sql_type()}"
            for i in range(len(self.fields))
        )
        pk_cols = ", ".join(letters[:self.primary_key_fields])
        values = ",\n  ".join(f.sql_value_from_n("n") for f in self.fields)
        # digit-series generator: n ranges 1 .. row_count
        digits = max(1, (row_count // 10 + 1).bit_length())  # enough single-digit factors
        factors = ",\n               ".join(
            "(SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4"
            " UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9)"
            f" {letters[i]}"
            for i in range(digits)
        )
        terms = " + ".join(f"{letters[i]}.N * {10 ** i}" for i in range(digits)) + " + 1"
        return [f"DROP TABLE IF EXISTS {name}",
f"""
CREATE TABLE {name} ({col_defs}, PRIMARY KEY ({pk_cols})) ENGINE=InnoDB;
INSERT INTO {name} ({cols})
SELECT
  {values}
FROM (
  SELECT {terms} AS n
  FROM {factors}
) nums
WHERE n <= {row_count};"""]

TABLE_SCHEMAS = {k: TableSchema.from_str(s) for k, s in TABLE_SCHEMA_CASES.items()}

@dataclass(frozen=True)
class Mysql:
    name: str
    version: str
    port: int

    async def sql(self, *queries, database=None, check=True, quiet=False):
        """SQL auf die MySQL-Instanz dieses Objekts ausführen."""
        cli = os.environ.get("MSYNC_MYSQL_CLI", "/usr/bin/mysql")
        base = [cli, "-h", "127.0.0.1", "-P", str(self.port), "-uroot"]
        if ROOT_PW:
            base.append(f"-p{ROOT_PW}")
        if database:
            base.append(database)
        out_parts = []
        for query in queries:
            cmd = base + ["-e", query]
            proc = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if check and proc.returncode != 0:
                err = stderr.decode(errors="replace") if stderr else ""
                raise RuntimeError(f"mysql CLI failed: {' '.join(str(c) for c in cmd)[:120]}\n{err[-500:]}" if not quiet else err)
            out_parts.append(stdout.decode(errors="replace") if stdout else "")
        return "".join(out_parts)

    async def scalar(self, query, database=None, quiet=False):
        out = (await self.sql(query, database=database, quiet=quiet)).strip().splitlines()
        return out[-1] if out else None

    async def table_exists(self, table):
        return await self.scalar(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA='{MATRIX_DB}' AND TABLE_NAME='{table}'",
            quiet=True,
        ) == "1"

    @property
    def server_port(self):
        return self.port + SERVER_PORT_OFFSET

    def server_config(self):
        cfg = WORK / f"config-server-{self.name}.json"
        cfg.write_text(json.dumps({
            "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": self.port},
            "listen": "127.0.0.1",
            "port": self.server_port,
            "tcp_port": self.server_port + 1,
            "allow_plaintext": True,
        }, indent=2))
        return cfg

    @property
    def tcp_server_port(self):
        return self.server_port + 1

    @asynccontextmanager
    async def server_running(self):
        cfg = self.server_config()
        fp_file = cfg.with_suffix(".fingerprint")
        fp_file.unlink(missing_ok=True)
        log = open(WORK / f"server-{self.name}.log", "w")
        env = os.environ.copy()
        env["RUST_LOG"] = "warn"
        proc = await asyncio.create_subprocess_exec(
            str(BIN), "-C", str(cfg), stdout=log, stderr=log, env=env
        )
        log.close()
        try:
            fingerprint = None
            for _ in range(50):
                if fp_file.exists():
                    fingerprint = fp_file.read_text().strip()
                    break
                await asyncio.sleep(0.2)
            if not fingerprint:
                raise RuntimeError(f"{self.name}: kein Server-Fingerprint")
            yield fingerprint
        finally:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except (asyncio.TimeoutError, subprocess.TimeoutExpired):
                proc.kill()
                await proc.wait()

    def _pt_dsn(self, table):
        parts = [
            "h=127.0.0.1",
            f"P={self.port}",
            "u=root",
            f"D={MATRIX_DB}",
            f"t={table}",
        ]
        if ROOT_PW:
            parts.append(f"p={ROOT_PW}")
        return ",".join(parts)

MYSQL_INSTANCES = {a[0]: Mysql(*a) for a in MYSQL_INSTANCES_CONFIG}


def write_docker_compose():
    services = {}
    for name, version, port in MYSQL_INSTANCES_CONFIG:
        services[name] = {
            "image": f"mysql:{version}",
            "container_name": name,
            "environment": {"MYSQL_ROOT_PASSWORD": ROOT_PW},
            "command": "--character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci --local_infile=1",
            "ports": [f"{port}:3306"],
            "healthcheck": {
                "test": ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", f"-p{ROOT_PW}"],
                "interval": "3s",
                "timeout": "3s",
                "retries": 30,
            },
        }
    (HERE / "docker-compose.yml").write_text(
        yaml.safe_dump({"services": services}, sort_keys=False)
    )


def _matrix_mysql_json(m):
    return json.dumps({
        "mysql": {"user": "root", "password": ROOT_PW,
                  "host": "127.0.0.1", "port": m.port},
    })




def run(cmd, check=True, input=None, capture=True, quiet=False):
    if not quiet:
        print(f"    $ {' '.join(str(c) for c in cmd)[:160]}")
    r = subprocess.run([str(c) for c in cmd], input=input, text=True,
                       capture_output=capture)
    if check and r.returncode != 0:
        if not quiet:
            print(r.stdout[-3000:])
            print(r.stderr[-3000:])
        raise SystemExit(f"Befehl fehlgeschlagen: {cmd}")
    return r


async def ensure_binary(bin):
    try:
        await run_show_out_tail_on_error([bin, "--help"])
    except Exception:
        await run_show_out_tail_on_error(["cargo", "+nightly", "build", "--release"], cwd=HERE)


def mysql_sync(args, expect_rc=0, label=""):
    r = subprocess.run([str(BIN), *args], text=True, capture_output=True, cwd=WORK)
    tail = (r.stdout + r.stderr).strip().splitlines()
    tail_str = "\n    ".join(tail[-6:])
    ok = r.returncode == expect_rc
    print(f"    $ mysql-sync {' '.join(str(a) for a in args)[:120]}  → rc={r.returncode} (erwartet {expect_rc})")
    print(f"    {tail_str}")
    return ok


def run_bin(args):
    r = subprocess.run([str(BIN), *args], text=True, capture_output=True, cwd=WORK)
    return r.returncode, r.stdout + r.stderr


if random() > 0.5:
    CHUNK_DROP_START_SOURCE = 0
    CHUNK_DROP_START_DEST   = 4
else:
    CHUNK_DROP_START_SOURCE = 4
    CHUNK_DROP_START_DEST   = 0

MATRIX_TIMEOUT = 300
MATRIX_DRIFT_OFFSET = MATRIX_ROWS
MATRIX_EXTRA_OFFSET = 9 * MATRIX_ROWS
PT_BIN = os.environ.get("MSYNC_PT_TABLE_SYNC") or shutil.which("pt-table-sync")

RUST_MODES = (
    ("select-sha2",  "--hash-select-sha2"),
    ("select-crc32", "--hash-select-crc32"),
    ("dump", "--hash-dump"),
    ("raw-binary", "--hash-raw-binary-row"),
    ("raw-binary-vec", "--hash-raw-binary-row-vec"),
    ("raw-tsv", "--hash-raw-tsv"),
)

# Rust-side hash algorithm for raw modes (xxh3 is default)
RUST_HASH_ALGOS = ("xxh3", "blake3", "sha256")

# Chunk size exponent (1-6); 3 is the default
RUST_CHUNKED = (3,)

# Wire compression: none (default) or zstd
RUST_COMPRESSION = ("none", "zstd")

# Transport: quic (TLS 1.3 encrypted, default) or tcp (plaintext)
RUST_TRANSPORT = ("quic", "tcp")

Tool = Literal['pt', 'rust'] # rust is rust-mysql-sync

@dataclass()
class Mysqls:
    mysqls: str
    def versions(self):
        return self.mysqls.split(':')

@dataclass()
class MatrixCasePTOpts:
    pass

@dataclass()
class MatrixCaseMysqlDumpOpts:
    compression: Literal['none','zstd'] = "none"

@dataclass()
class MatrixCaseRustOpts:
    mode: str
    hash_algo: str = "xxh3"
    chunked: int = 3
    compression: str = "none"
    transport: str = "quic"

Binary = Tuple["binary", "no_binary"]

@dataclass()
class MatrixCase:
    table_schema_case: str  # key of TABLE_SCHEMA_CASES
    schema: TableSchema
    binary: Binary
    mysqls: Tuple[Mysql, Mysql]  # from, to
    tool_opts: MatrixCasePTOpts | MatrixCaseMysqlDumpOpts | MatrixCaseRustOpts

    def __post_init__(self):
        raw = f"{self.table_schema_case}|{self.binary}|{self.mysqls}|{self.tool_opts}"
        self.hash = hashlib.sha256(raw.encode()).hexdigest()[:8]

    def key(self):
        ts = self.schema
        first = ts.fields[0].type
        kind = "int" if first.startswith("int") else "str"
        if ts.primary_key_fields < 2:
            return kind
        second = ts.fields[1].type
        kind2 = "int" if second.startswith("int") else "str"
        if kind == kind2:
            return f"{kind}-tuple"
        return "mixed-tuple"

    def table_names(self):
        s = f"src_{CHUNK_SIZE}_{MATRIX_ROWS}_{self.table_schema_case}_{self.binary}"
        d = f"dst_{CHUNK_SIZE}_{MATRIX_ROWS}_{self.table_schema_case}_{self.binary}_{self.hash}"
        return s, d



def matrix_cases():
    cases = []
    # Source → Dest MySQL instance pairs.
    M = MYSQL_INSTANCES
    mysql_pairs = [
        (M["mysql80"], M["mysql97"]),
        (M["mysql80"], M["mysql80"]),
    ]

    # Representative schemas for extended dimension testing (hash_algo, chunked, compression)
    # to avoid exploding the full matrix. Full matrix uses default config.
    tables_schemas = TABLE_SCHEMAS.items()
    binary_cases = ("no_binary", "bin")
    rust_modes = RUST_MODES
    rust_algos = RUST_HASH_ALGOS
    rust_compression = RUST_COMPRESSION
    rust_transport = RUST_TRANSPORT
    rust_hash_algos = RUST_HASH_ALGOS[:1] # if one works all work

    if QUICK:
        tables_schemas = tables_schemas[:1]
        binary_cases = binary_cases[:1]
        rust_algos = rust_algos[:1]
        rust_compression = rust_compression[:1]
        rust_transport = rust_transport[:1]
        rust_modes = [
            # ("select-sha2", "--hash-select-sha2"),
            ("select-crc32", "--hash-select-crc32"),
            # ("dump", "--hash-dump"),
            # ("raw-binary", "--hash-raw-binary-row"),
            ("raw-binary-vec", "--hash-raw-binary-row-vec"),
            ("raw-tsv", "--hash-raw-tsv"),
        ]

    extended_done = False

    for (table_schema_case, schema), binary, mysqls in product(
            tables_schemas,
            binary_cases,
            mysql_pairs):
        common = {"table_schema_case": table_schema_case,
                  "schema": schema,
                  "binary": binary, "mysqls": mysqls}
        cases.append(MatrixCase(**common, tool_opts=MatrixCasePTOpts()))

        # mysqldump | mysql baseline (plain and zstd)
        for comp in ("none", "zstd"):
            cases.append(MatrixCase(**common, tool_opts=MatrixCaseMysqlDumpOpts(compression=comp)))

        # Default rust cases: all modes with default config
        for mode, _ in rust_modes:
            cases.append(MatrixCase(**common, tool_opts=MatrixCaseRustOpts(mode)))

        # Extended rust cases: vary hash_algo, chunked, compression, transport for representative schemas
        if not extended_done:
            # chunking options compression and transport
            # does (hopefully) not depend on the table schema binary case or
            # mysql version pair
            # so its ok to add only once
            extended_done = True
            for (mode, _), hash_algo, chunked, compression, transport in product(
                    # one value only
                    rust_modes[:1], rust_hash_algos, RUST_CHUNKED, 

                    # only testing these
                    RUST_COMPRESSION, RUST_TRANSPORT
                ):
                # Skip if this is the default config (already covered above)
                cases.append(MatrixCase(**common, tool_opts=MatrixCaseRustOpts(
                    mode, hash_algo=hash_algo, chunked=chunked, compression=compression,
                    transport=transport)))

    print(f"cases: {len(cases)}:")
    return cases

def _matrix_ddl(case, table):
    fields = case.schema.fields
    key = fields[0].sql_type()
    id2_type = fields[1].sql_type() if case.schema.primary_key_fields > 1 else None
    tuple_col = f"  id2 {id2_type} NOT NULL,\n" if id2_type else ""
    pk = "(id, id2)" if id2_type else "(id)"
    bin_col = "  bin_data VARBINARY(128) NULL,\n" if case.binary == "bin" else ""
    return (
        f"CREATE TABLE {MATRIX_DB}.{table} (\n"
        f"  id {key} NOT NULL,\n"
        f"{tuple_col}"
        f"{bin_col}"
        "  txt VARCHAR(64) NOT NULL,\n"
        "  n INT NOT NULL,\n"
        f"  PRIMARY KEY {pk}\n"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def _matrix_id_expr(case, n="n"):
    id_type = case.schema.fields[0].type
    base = f"CEIL({n} / 1000)" if case.schema.primary_key_fields > 1 else n
    if id_type.startswith("int"):
        return base
    if id_type.startswith("blob"):
        return f"UNHEX(CONCAT(LPAD(HEX({base}), 8, '0'), '000000'))"
    return f"CONCAT('k', LPAD({base}, 8, '0'))"


def _matrix_id2_expr(case, n="n"):
    if case.schema.fields[1].type.startswith("int"):
        return f"MOD({n}, 1000)"
    return f"CONCAT('c', LPAD(MOD({n}, 1000), 3, '0'))"


def _matrix_seed(case, rows, table):
    table = f"{MATRIX_DB}.{table}"
    id_expr = _matrix_id_expr(case)
    txt_expr = "CONCAT('row-', n, CASE WHEN n % 7 = 0 THEN '\\tTab' ELSE '' END)"
    cols = ["id"]
    vals = [id_expr]
    if case.schema.primary_key_fields > 1:
        cols.append("id2")
        vals.append(_matrix_id2_expr(case))
    if case.binary == "bin":
        cols.append("bin_data")
        vals.append(
            "CASE WHEN n % 17 = 0 THEN NULL "
            "WHEN n % 19 = 0 THEN UNHEX('000000') "
            "WHEN n % 23 = 0 THEN UNHEX('FFFE80C0AF') "
            "ELSE UNHEX(CONCAT('00FF0A09', LPAD(HEX(n), 8, '0'), "
            "REPEAT('AB', 8 + (n % 16)))) END"
        )
    cols += ["txt", "n"]
    vals += [txt_expr, "n"]
    cols, vals = ", ".join(cols), ", ".join(vals)
    digits = max(1, len(str(rows - 1)))
    digit = ("(SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 "
             "UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 "
             "UNION SELECT 8 UNION SELECT 9)")
    from_clause = ",\n       ".join(f"{digit} d{i}" for i in range(digits))
    n_expr = " + ".join(f"d{i}.N * {10 ** i}" for i in range(digits))
    return f"""INSERT INTO {table} ({cols})
SELECT {vals} FROM (
  SELECT {n_expr} + 1 AS n
  FROM {from_clause}
) nums WHERE n <= {rows}"""


def _matrix_drift(case, table):
    table = f"{MATRIX_DB}.{table}"
    update_bin = ", bin_data = UNHEX('DEADBEEF')" if case.binary == "bin" else ""
    id_type = case.schema.fields[0].type
    if id_type.startswith("int"):
        extra_id = f"{MATRIX_EXTRA_OFFSET} + n"
    elif id_type.startswith("blob"):
        extra_id = f"UNHEX(CONCAT('E', LPAD(HEX(n), 7, '0'), '000000'))"
    else:
        extra_id = "CONCAT('extra-', LPAD(n, 4, '0'))"
    tuple_col = ", id2" if case.schema.primary_key_fields > 1 else ""
    tuple_val = ", " + _matrix_id2_expr(case) if case.schema.primary_key_fields > 1 else ""
    bin_col = ", bin_data" if case.binary == "bin" else ""
    bin_val = ", UNHEX('00FF00FF')" if case.binary == "bin" else ""
    return f"""
DELETE FROM {table} WHERE MOD(n, 20) = 0;
UPDATE {table} SET txt = CONCAT(txt, ' [drift]'), n = n + {MATRIX_DRIFT_OFFSET}{update_bin} WHERE MOD(n, 20) = 1;
INSERT INTO {table} (id{tuple_col}{bin_col}, txt, n)
SELECT {extra_id}{tuple_val}{bin_val}, CONCAT('extra-', n), {MATRIX_EXTRA_OFFSET} + n
FROM (SELECT 1 AS n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
      UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10) x;
"""


def _matrix_checksum_sql(case, table):
    cols = ["HEX(id)"]
    if case.schema.primary_key_fields > 1:
        cols.append("CAST(id2 AS CHAR)")
    cols += ["HEX(txt)", "CAST(n AS CHAR)"]
    if case.binary == "bin":
        cols.append("IFNULL(HEX(bin_data), 'NULL')")
    expr = "CONCAT_WS('|', " + ", ".join(cols) + ")"
    return f"SELECT CONCAT(COUNT(*), ':', COALESCE(SUM(CRC32({expr})), 0)) FROM {MATRIX_DB}.{table}"


async def _timed(cmd, stdin_data=None):
    """Run a command asynchronously; returns (elapsed_ms, (stdout_bytes, stderr_bytes, returncode))."""
    started = time.perf_counter()
    proc = await asyncio.create_subprocess_exec(
        *[str(c) for c in cmd],
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
        cwd=WORK,
    )
    stdout, _ = await asyncio.wait_for(
        proc.communicate(input=stdin_data), timeout=MATRIX_TIMEOUT
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return elapsed_ms, (stdout, b"", proc.returncode)


@dataclass
class MatrixResult:
    case: MatrixCase
    status: Literal['ok', 'waiting', 'aborted', 'skip', 'reject'] | BaseException
    detail: str = ""
    clean_ms: float | None = None
    drift_ms: float | None = None
    sync_ms: float | None = None
    total_ms: float | None = None

    def columns(self):
        c = self.case
        if isinstance(c.tool_opts, MatrixCasePTOpts):
            tool, mode = "pt", None
            algo, chunk, comp, trans = "-", "-", "-", "-"
            label = tool
        elif isinstance(c.tool_opts, MatrixCaseMysqlDumpOpts):
            tool = "mysqldump"
            mode = c.tool_opts.compression
            algo, chunk, comp, trans = "-", "-", mode, "-"
            label = f"{tool}/{mode}"
        else:
            tool = "rust"
            mode = c.tool_opts.mode
            algo, chunk, comp, trans = c.tool_opts.hash_algo, c.tool_opts.chunked, c.tool_opts.compression, c.tool_opts.transport
            cfg = f"({algo},c{chunk},{comp},{trans})"
            label = f"{tool}/{mode or ''}{cfg}"
        st = 'error' if isinstance(self.status, BaseException) else self.status
        return {
            'case_hash': c.hash,
            'schema': c.table_schema_case,
            'key': c.key(), 'binary': c.binary,
            'mysqls': f"{c.mysqls[0].version}:{c.mysqls[1].version}",
            'sync_ms': self.sync_ms, 'tool': tool, 'mode': mode or '-',
            'hash_algo': algo, 'chunked': chunk, 'compression': comp, 'transport': trans,
            'label': label,
            'clean_ms': self.clean_ms, 'drift_ms': self.drift_ms, 'total_ms': self.total_ms,
            'status': st, 'detail': self.detail,
        }

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _matrix_base(case):
    return MatrixResult(case=case, status="waiting")


async def _run_rust_case(cfg_json, case):
    out = _matrix_base(case)
    src_table, dst_table = case.table_names()
    src, dst = case.mysqls
    selector = f"{MATRIX_DB}.{src_table}"
    mode = case.tool_opts.mode
    flag = dict(RUST_MODES)[mode]
    base = [str(BIN), "-C", "-", "--no-progress"] + ([flag] if flag else [])
    stdin_data = cfg_json.encode()

    if mode == "raw-tsv" and case.binary == "bin":
        ms, (stdout, _, rc) = await _timed([*base, "--check", "0", selector], stdin_data)
        if rc != 0:
            out.update(total_ms=ms, status="reject", detail=f"rc={rc}")
            return out
        raise RuntimeError(f"raw-tsv with binary should have failed: rc={rc}")

    # prepare_tables has already removed chunks from both sides. With the
    # default CHUNK_SIZE=2 and 10k rows there are 100 chunks, so the drops are
    # real differences and the first check must report drift.
    drift_ms, (stdout, _, rc) = await _timed([*base, "--check", "0", selector], stdin_data)
    if rc != 1:
        raise RuntimeError(f"drift check: expected rc=1, got rc={rc}: {stdout.decode('utf8','replace')[-300:]}")

    sync_ms, (stdout, _, rc) = await _timed([*base, "--sync", selector], stdin_data)
    if rc != 0:
        raise RuntimeError(f"sync: expected rc=0, got rc={rc}: {stdout.decode('utf8','replace')[-300:]}")

    clean_ms, (stdout, _, rc) = await _timed([*base, "--check", "0", selector], stdin_data)
    if rc != 0:
        raise RuntimeError(f"clean check: expected rc=0, got rc={rc}: {stdout.decode('utf8','replace')[-300:]}")

    a = await src.scalar(_matrix_checksum_sql(case, src_table), quiet=True)
    b = await dst.scalar(_matrix_checksum_sql(case, dst_table), quiet=True)
    if a != b:
        raise RuntimeError(f"data mismatch: {a} != {b}")

    out.clean_ms = clean_ms
    out.drift_ms = drift_ms
    out.sync_ms = sync_ms
    out.total_ms = clean_ms + drift_ms + sync_ms
    out.status = "ok"
    return out


async def _run_mysqldump_case(cfg_json, case):
    """mysqldump src | [zstd] | mysql dst — simple dump-and-pipe baseline."""
    out = _matrix_base(case)
    src_table, dst_table = case.table_names()
    src, dst = case.mysqls
    compression = case.tool_opts.compression

    # Apply drift to dest so there's something to sync
    await dst.sql(_matrix_drift(case, dst_table), quiet=True)

    # Use shell pipes for the pipeline; sed rewrites the table name so
    # the dump's CREATE/INSERT statements target the destination table.
    dump = f"mysqldump -h127.0.0.1 -P{src.port} -uroot -p{ROOT_PW} " \
           f"--single-transaction --quick --set-gtid-purged=OFF " \
           f"--routines --triggers {MATRIX_DB} {src_table}"
    rename = f"sed 's/{src_table}/{dst_table}/g'"
    load = f"mysql -h127.0.0.1 -P{dst.port} -uroot -p{ROOT_PW} {MATRIX_DB}"

    if compression == "zstd":
        shell_cmd = f"{dump} | {rename} | zstd -q -c | zstd -d -q -c | {load}"
    else:
        shell_cmd = f"{dump} | {rename} | {load}"

    started = time.perf_counter()
    proc = await asyncio.create_subprocess_shell(
        shell_cmd, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    _, stderr = await asyncio.wait_for(proc.communicate(), timeout=MATRIX_TIMEOUT)
    sync_ms = (time.perf_counter() - started) * 1000.0

    if proc.returncode != 0:
        raise RuntimeError(f"mysqldump|mysql failed rc={proc.returncode}: {stderr.decode('utf8','replace')[-300:]}")

    # Verify data matches
    a = await src.scalar(_matrix_checksum_sql(case, src_table), quiet=True)
    b = await dst.scalar(_matrix_checksum_sql(case, dst_table), quiet=True)
    if a != b:
        raise RuntimeError(f"data mismatch: {a} != {b}")

    out.sync_ms = sync_ms
    out.total_ms = sync_ms
    out.status = "ok"
    return out


async def _run_pt_case(cfg_json, case):
    # pt-table-sync exit 2 means "Differences found" (also after a successful
    # --execute), 0 means "Nothing to do", 1 means error. Verify the outcome
    # with our own check instead of trusting the exit code.
    out = _matrix_base(case)
    if not PT_BIN:
        out.update(status="skip", detail="pt-table-sync nicht im PATH")
        return out
    src_table, dst_table = case.table_names()
    src, dst = case.mysqls
    await dst.sql(_matrix_drift(case, dst_table), quiet=True)
    src_dsn, dst_dsn = src._pt_dsn(src_table), dst._pt_dsn(dst_table)
    common = [PT_BIN, "--no-check-slave", "--no-check-triggers", "--chunk-size=1000"]

    # pt always runs against the drifted tables (no clean pass):
    # --print is the drift detection, --execute applies the differences.
    drift_ms, (stdout, _, rc) = await _timed([*common, "--print", src_dsn, dst_dsn])
    if rc not in (0, 2):
        raise RuntimeError(f"pt --print: expected rc=0|2, got rc={rc}: {stdout.decode('utf8','replace')[-300:]}")

    sync_ms, (stdout, _, rc) = await _timed([*common, "--execute", src_dsn, dst_dsn])
    if rc not in (0, 2):
        raise RuntimeError(f"pt --execute: expected rc=0|2, got rc={rc}: {stdout.decode('utf8','replace')[-300:]}")

    _, (stdout, _, rc) = await _timed(
        [str(BIN), "-C", "-", "--no-progress", "--check", "0", f"{MATRIX_DB}.{src_table}"],
        cfg_json.encode()
    )
    out_text = stdout.decode('utf8','replace')
    a = await src.scalar(_matrix_checksum_sql(case, src_table), quiet=True)
    b = await dst.scalar(_matrix_checksum_sql(case, dst_table), quiet=True)
    if rc != 0 or a != b:
        raise RuntimeError(f"rust --check rc={rc} | data mismatch: {a} != {b} | {out_text[-200:]}")

    out.drift_ms = drift_ms
    out.sync_ms = sync_ms
    out.total_ms = drift_ms + sync_ms
    out.status = "ok"
    return out

async def _prepare_matrix_table(m, case, table):
    await m.sql(f"DROP TABLE IF EXISTS {MATRIX_DB}.{table}", quiet=True)
    await m.sql(_matrix_ddl(case, table), quiet=True)
    await m.sql(_matrix_seed(case, MATRIX_ROWS, table), quiet=True)


async def _drop_matrix_chunks(m, table, start):
    cmd = [str(BIN), "-C", "-", f"--testing-drop-chunks={start}",
           f"{MATRIX_DB}.{table}"]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        cwd=WORK
    )
    stdout, stderr = await proc.communicate(input=_matrix_mysql_json(m).encode())
    if proc.returncode != 0:
        err = stderr.decode(errors="replace") if stderr else ""
        raise RuntimeError(f"drop-chunks failed rc={proc.returncode}: {err[-300:]}")


async def prepare_tables(case: MatrixCase):
    src_table, dst_table = case.table_names()
    src, dst = case.mysqls

    async def prepare_source():
        async with LOCK_FROM_TABLES[(src.name, src_table)]:
            if not await src.table_exists(src_table):
                with context_benchmark(f"prepare_source {src_table}"):
                    await _prepare_matrix_table(src, case, src_table)
                    await _drop_matrix_chunks(src, src_table, CHUNK_DROP_START_SOURCE)

    async def prepare_dest():
        with context_benchmark(f"prepare_dest {dst_table}"):
            await _prepare_matrix_table(dst, case, dst_table)
            await _drop_matrix_chunks(dst, dst_table, CHUNK_DROP_START_DEST)

    return await gather(prepare_source(), prepare_dest())

async def _matrix_runner(cfg, case: MatrixCase):
    out = _matrix_base(case)
    try:
        await prepare_tables(case)
        if isinstance(case.tool_opts, MatrixCasePTOpts):
            return await _run_pt_case(cfg, case)
        elif isinstance(case.tool_opts, MatrixCaseMysqlDumpOpts):
            return await _run_mysqldump_case(cfg, case)
        else:
            return await _run_rust_case(cfg, case)

    except asyncio.exceptions.CancelledError as exc:
        out.update(status="aborted")
    except Exception as exc:
        msg = str(exc)
        # Raw mode cross-version profile mismatches are expected skips
        if "Profil weicht ab" in msg or "Profile" in msg and "differs" in msg:
            out.update(status="skip", detail=msg[:300])
        elif "kein Hash-Weg möglich" in msg:
            out.update(status="skip", detail=msg[:300])
        else:
            out.update(status=exc)
    return out

async def _matrix_async(cases, fingerprints):
    global CONCURRENCY
    sem = Semaphore(CONCURRENCY)

    async def one(case):
        async with sem:
            client_cfg_json = make_client_cfg(case, fingerprints[case.mysqls[1].name])
            return await _matrix_runner(client_cfg_json, case)

    tasks = [asyncio.create_task(one(case)) for case in cases]
    return await asyncio.gather(*tasks)


def _report_matrix(rows, wall_ms):
    global CONCURRENCY
    print("\n== Matrix: rust-sync vs pt-table-sync ==")
    print(f"concurrency={CONCURRENCY} rows={MATRIX_ROWS} "
          f"wall={wall_ms:.0f} ms")
    print("  Sorted by case, mysqls, sync. Errors grouped by case below.")
    print("  Legend (all times in ms):")
    print("    drift = ms spent hashing chunks and finding which differ")
    print("    sync  = ms spent copying the differing chunks")
    print("    clean = ms spent re-hashing chunks and confirming none differ")
    print("    total = drift + sync + clean")
    cols_list = [r.columns() for r in rows]
    def ms(v):
        return "-" if v is None else f"{v:.0f}"
    def case_key(d):
        return f"{d['schema']}-{d['key']}-{d['binary']}"

    # Sort by case, then mysqls, then sync time descending
    cols_list.sort(key=lambda d: (case_key(d), -(d['sync_ms'] or 0), d['mysqls'], d['label']))

    print("case\tmysqls\tsync\tdetails\tcase_hash")
    for d in cols_list:
        parts = [d['label'], f"clean={ms(d['clean_ms'])}", f"drift={ms(d['drift_ms'])}", f"total={ms(d['total_ms'])}", d['status']]
        detail = d['detail'].replace('\n', ' ').replace('\r', ' ')
        if detail:
            parts.append(detail[:100])
        print(f"{case_key(d)}\t{d['mysqls']}\t{ms(d['sync_ms'])}\t{' '.join(parts)}\t{d['case_hash']}")

    # Print errors grouped by case, with full traceback for exceptions
    errors = [(r, r.columns()) for r in rows if r.status not in ('ok', 'skip', 'reject')]
    if errors:
        print("\n  Errors by case:")
        by_case = {}
        for r, d in errors:
            ck = case_key(d)
            by_case.setdefault(ck, []).append((r, d))
        for ck in sorted(by_case):
            for r, d in by_case[ck]:
                exc_info = ""
                if isinstance(r.status, BaseException):
                    exc_info = "\n".join(traceback.format_exception(r.status))
                print(f"  {ck} | {d['case_hash']} | {d['mysqls']} | {d['label']} | {type(r.status).__name__}: {d['detail'][:200]}")
                if exc_info:
                    print(f"    {exc_info.strip()}")

    ok = sum(1 for d in cols_list if d['status'] == 'ok')
    skip = sum(1 for d in cols_list if d['status'] in ('skip', 'reject'))
    err = len(cols_list) - ok - skip
    print(f"  [{chalk.green('ok') if err == 0 else chalk.red('FAIL')}] Matrix: {ok} ok, {skip} skip, {err} error")


@task
async def prepare_workdir():
    WORK.mkdir(exist_ok=True)

    # raw-tsv starts the `mysql` CLI on the host (client and server side).
    # The Docker databases are exposed via published ports, so provide a
    # small mysql wrapper that forwards the scan into the containers.
    bin_dir = WORK / "bin"
    bin_dir.mkdir(exist_ok=True)
    wrapper = bin_dir / "mysql"
    wrapper.write_text(f"""#!/usr/bin/env python3
import subprocess, sys

COMPOSE = {str(HERE / "docker-compose.yml")!r}
SERVICES = { {str(m.port): m.name for m in MYSQL_INSTANCES.values()}!r}
ROOT_PW = {ROOT_PW!r}

args = sys.argv[1:]
port = None
out = []
i = 0
while i < len(args):
    a = args[i]
    if a in ("-h", "--host", "-P", "--port", "-u", "--user", "--socket"):
        if a in ("-P", "--port") and i + 1 < len(args):
            port = args[i + 1]
        i += 2
        continue
    if a.startswith("--port="):
        port = a.split("=", 1)[1]
        i += 1
        continue
    if a.startswith(("--host=", "--user=", "--socket=")):
        i += 1
        continue
    if a.startswith("-P") and len(a) > 2:
        port = a[2:]
        i += 1
        continue
    if a.startswith("-u") and len(a) > 2:
        i += 1
        continue
    if a.startswith("-p"):
        i += 1
        continue
    out.append(a)
    i += 1

service = SERVICES.get(port)
if service is None:
    sys.exit(f"unknown mysql port {{port}}")
cmd = ["docker", "compose", "-f", COMPOSE, "exec", "-T", service, "mysql", "-uroot"]
if ROOT_PW:
    cmd.append("-p" + ROOT_PW)
cmd.extend(out)
sys.exit(subprocess.call(cmd))
""")
    wrapper.chmod(0o755)
    os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")

@task
async def docker_db_up():
    await compose("up", "-d", "--wait")
    await ensure_dbs_up()

async def ensure_dbs_up(attempts=20):
    for _ in range(attempts):
        try:
            for m in MYSQL_INSTANCES.values():
                await m.sql("SELECT 1", quiet=True)
            break
        except Exception:
            await asyncio.sleep(2)
            print(".. waiting for dbs")
    else:
        assert False, "dbs not up"
    with context_benchmark("CREATE DATABASE"):
        for m in MYSQL_INSTANCES.values():
            await m.sql(f"CREATE DATABASE IF NOT EXISTS {MATRIX_DB} CHARACTER SET utf8mb4", quiet=True)

def make_client_cfg(case, fingerprint):
    src, dst = case.mysqls
    src_table, dst_table = case.table_names()
    opts = case.tool_opts
    if isinstance(opts, MatrixCaseRustOpts):
        hash_algo = opts.hash_algo
        chunked = opts.chunked
        compression = opts.compression
        transport = opts.transport
    else:
        hash_algo = "xxh3"
        chunked = 3
        compression = "none"
        transport = "quic"
    server_port = dst.tcp_server_port if transport == "tcp" else dst.server_port
    cfg_dict = {
        "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": src.port},
        "server": f"127.0.0.1:{server_port}",
        "server_fingerprint": fingerprint,
        "allow_csv": True,
        "dump_strategies": ["select-stream"],
        "hash_algo": hash_algo,
        "transfer_batch_rows": 1000,
        "chunked": chunked,
        "concurrent_processes": 3,
        "diff_threshold_percent": 10,
        "compression": compression,
        "transport": transport,
        "database_table_mappings": {
            f"{MATRIX_DB}.{src_table}": f"{MATRIX_DB}.{dst_table}",
        },
    }
    if transport == "tcp":
        cfg_dict["allow_plaintext"] = True
    return json.dumps(cfg_dict)


def json_variant(base, filename, **changes):
    path = WORK / filename
    cfg = json.loads(Path(base).read_text())
    cfg.update(changes)
    path.write_text(json.dumps(cfg, indent=2))
    return path


def update_json(path, **changes):
    return json_variant(path, Path(path).name, **changes)


def print_env():
    print("== Testumgebung ==")
    print("  Modus: Docker")
    for m in MYSQL_INSTANCES.values():
        print(f"  {m.name}: MySQL 127.0.0.1:{m.port}, QUIC 127.0.0.1:{m.server_port}, TCP 127.0.0.1:{m.tcp_server_port}")


def start_server(server_cfg, log_name):
    fp_file = server_cfg.with_suffix(".fingerprint")
    fp_file.unlink(missing_ok=True)
    log = open(WORK / log_name, "w")
    proc = subprocess.Popen([str(BIN), "-C", str(server_cfg)], stdout=log, stderr=log)
    fingerprint = None
    for _ in range(50):
        if fp_file.exists():
            fingerprint = fp_file.read_text().strip()
            break
        time.sleep(0.2)
    return proc, fingerprint


def stop_server(server):
    if server is None:
        return
    server.terminate()
    try:
        server.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()


def restart_server(server, base_cfg, filename, log_name, **changes):
    stop_server(server)
    cfg = json_variant(base_cfg, filename, **changes)
    new_server, fingerprint = start_server(cfg, log_name)
    return new_server, fingerprint, cfg


def wait_tcp(port, attempts=50):
    for _ in range(attempts):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.1)
    return False


async def matrix():
    await ensure_binary(str(BIN))
    await prepare_workdir()
    await ensure_dbs_up()
    print_env()

    cases = matrix_cases()
    async with AsyncExitStack() as stack:
        fingerprints = {}
        for m in MYSQL_INSTANCES.values():
            fingerprints[m.name] = await stack.enter_async_context(m.server_running())
        started = time.perf_counter()
        rows = await _matrix_async(cases, fingerprints)
        wall_ms = (time.perf_counter() - started) * 1000.0
    _report_matrix(rows, wall_ms)
    bad = [r for r in rows if r.columns()['status'] not in ("ok", "reject", "skip")]
    return not bad

async def compose(*args):
    write_docker_compose()
    cmd = ["docker", "compose", "-f", str(HERE / "docker-compose.yml"), *args]
    proc = await asyncio.create_subprocess_exec(*cmd)
    rc = await proc.wait()
    if rc != 0:
        raise subprocess.CalledProcessError(rc, cmd)

async def up():
    await prepare_workdir()
    await docker_db_up()


async def down():
    await compose("down", "--remove-orphans")


async def clean():
    await compose("down", "--remove-orphans", "--volumes")


async def dump_table_create_sql():
    """Dump CREATE TABLE + INSERT SQL for one matrix case to create-table.sql and time loading it."""
    await ensure_binary(str(BIN))
    await prepare_workdir()
    await ensure_dbs_up()
    cases = matrix_cases()
    case = cases[0]
    _, dst_table = case.table_names()
    ddl = _matrix_ddl(case, dst_table)
    seed = _matrix_seed(case, MATRIX_ROWS, dst_table)
    sql = f"DROP TABLE IF EXISTS {MATRIX_DB}.{dst_table};\n{ddl};\n{seed};\n"
    path = HERE / "create-table.sql"
    path.write_text(sql)
    print(f"Written {path} ({len(sql)} bytes)")
    # Time loading it
    import time as _time
    started = _time.perf_counter()
    for m in MYSQL_INSTANCES.values():
        await m.sql(sql)
        elapsed_ms = (_time.perf_counter() - started) * 1000
        print(f"Loaded on {m.name} in {elapsed_ms:.0f} ms")
        started = _time.perf_counter()

ACTIONS = {
    "write_docker_compose": write_docker_compose,
    "up": up,
    "matrix": matrix,
    "down": down,
    "clean": clean,
    "dump-table-create-sql": dump_table_create_sql,
}

async def main():
    global CONCURRENCY
    args = sys.argv[1:]
    print(args)

    # --concurrency[N] (can appear anywhere in args)
    c = "--concurrency"
    for i, arg in enumerate(args):
        if arg.startswith(c):
            CONCURRENCY = int(arg[len(c):]) or 1
            args = args[:i] + args[i+1:]
            break

    print("\n\n")
    print(f"CONCURRENCY {CONCURRENCY}")

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"===== cpu={system_info()} | concurrency={CONCURRENCY or 'MISSING'} | "
    with with_logging(header = header):
        print(f"CONCURRENCY={CONCURRENCY or 'MISSING'} matrix_jobs={CONCURRENCY}")

        # python test.py [--concurrencyN] up matrix down [clean]
        actions = args or ["up", "matrix", "down"]
        for action in actions:
            print(f"==> STARTING ACTION {action}")
            assert action in ACTIONS
            await ACTIONS[action]()

if __name__ == "__main__":
    asyncio.run(main())
