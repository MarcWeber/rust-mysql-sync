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

from typing import TypeVar, Generic, Union, Callable, Any, IO, cast, Protocol, overload, Awaitable, Callable, Iterable, Optional, List, Dict, Literal, TypedDict, Unpack, Concatenate, Tuple, NotRequired
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from itertools import product
from random import random
import hashlib
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import time
import yaml
import traceback
import sys
from u_systeminfo import system_info
from u_dict import DictFactory
from datetime import datetime
from pathlib import Path
from yachalk import chalk
from asyncio import create_task, to_thread, sleep, timeout, timeout_at, Semaphore, Future, gather
import asyncio

from collections import defaultdict
from contextlib import AsyncExitStack, asynccontextmanager
import threading

from u_redirect_logging import with_logging
from u_asyncio import run_show_out_tail_on_error, task

ROOT_PW = "syncroot"
SERVER_PORT_OFFSET = 100
MATRIX_DB = "db_matrix"
MATRIX_ROWS = 10000

TEST_TEXT = "A\nB\n\\\BinärmüllÄß"

MYSQL_INSTANCES_CONFIG = [
    # name version port
    ["mysql80", "8.0", 7012],
    ["mysql80b", "8.0", 7014],  # Second 8.0 instance for 8.0→8.0 sync tests
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
SKIP = chalk.yellow("SKIP")
results = []
CONCURRENCY = 1

LOG_PATH = Path(os.environ.get("MSYNC_LOG") or (HERE / "test.log"))


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
        cli = os.environ.get("MSYNC_MYSQL_CLI", "mysql")
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
            "allow_plaintext": True,
        }, indent=2))
        return cfg

    @asynccontextmanager
    async def server_running(self):
        cfg = self.server_config()
        fp_file = cfg.with_suffix(".fingerprint")
        fp_file.unlink(missing_ok=True)
        log = open(WORK / f"server-{self.name}.log", "w")
        proc = await asyncio.create_subprocess_exec(
            str(BIN), "-C", str(cfg), stdout=log, stderr=log
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
            proc.terminate()
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


def step(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{PASS if ok else FAIL}] {name}" + (f" — {detail}" if detail else ""))
    return ok


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


async def ensure_binary():
    try:
        await run_show_out_tail_on_error([str(BIN), "--help"])
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

CHUNK_SIZE = 2
MATRIX_JOBS = 4
MATRIX_TIMEOUT = 300
MATRIX_DRIFT_OFFSET = MATRIX_ROWS
MATRIX_EXTRA_OFFSET = 9 * MATRIX_ROWS
PT_BIN = os.environ.get("MSYNC_PT_TABLE_SYNC") or shutil.which("pt-table-sync")

RUST_MODES = (
    ("select-sha2", "--hash-select-sha2"),
    ("select-crc32", "--hash-select-crc32"),
    ("dump", "--hash-dump"),
    ("raw-binary", "--hash-raw-binary-row"),
    ("raw-tsv", "--hash-raw-tsv"),
)

# Rust-side hash algorithm for raw modes (xxh3 is default)
RUST_HASH_ALGOS = ("xxh3", "blake3", "sha256")

# Chunk size exponent (1-6); 3 is the default
RUST_CHUNKED = (1, 3, 6)

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
    tool_opts: MatrixCasePTOpts | MatrixCaseRustOpts

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
    mysql_pairs = [
        (MYSQL_INSTANCES["mysql80"], MYSQL_INSTANCES["mysql97"]),
        (MYSQL_INSTANCES["mysql80"], MYSQL_INSTANCES["mysql80b"]),
    ]

    # Representative schemas for extended dimension testing (hash_algo, chunked, compression)
    # to avoid exploding the full matrix. Full matrix uses default config.
    extended_schemas = {"TC101", "TC201"}

    for (table_schema_case, schema), binary, (src_mysql, dst_mysql) in product(
            TABLE_SCHEMAS.items(),
            ("no_binary", "bin"),
            mysql_pairs):
        mysqls = (src_mysql, dst_mysql)
        common = {"table_schema_case": table_schema_case,
                  "schema": schema,
                  "binary": binary, "mysqls": mysqls}
        cases.append(MatrixCase(**common, tool_opts=MatrixCasePTOpts()))

        # Default rust cases: all modes with default config
        for mode, _ in RUST_MODES:
            cases.append(MatrixCase(**common, tool_opts=MatrixCaseRustOpts(mode)))

        # Extended rust cases: vary hash_algo, chunked, compression, transport for representative schemas
        if table_schema_case in extended_schemas:
            for (mode, _), hash_algo, chunked, compression, transport in product(
                    RUST_MODES, RUST_HASH_ALGOS, RUST_CHUNKED, RUST_COMPRESSION, RUST_TRANSPORT):
                # Skip if this is the default config (already covered above)
                if (hash_algo == "xxh3" and chunked == 3 and compression == "none"
                        and transport == "quic"):
                    continue
                cases.append(MatrixCase(**common, tool_opts=MatrixCaseRustOpts(
                    mode, hash_algo=hash_algo, chunked=chunked, compression=compression,
                    transport=transport)))

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
        stderr=asyncio.subprocess.PIPE,
        cwd=WORK,
    )
    stdout, stderr = await asyncio.wait_for(
        proc.communicate(input=stdin_data), timeout=MATRIX_TIMEOUT
    )
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return elapsed_ms, (stdout, stderr, proc.returncode)


@dataclass
class MatrixResult:
    """Holds dimension labels (from the case) and timing/status fields (from the run)."""
    table_schema_case: str
    key: str
    binary: str
    tool: str
    mode: str | None
    mysqls: Tuple[Mysql, Mysql]
    hash_algo: str = "xxh3"
    chunked: int = 3
    compression: str = "none"
    transport: str = "quic"
    clean_ms: float | None = None
    drift_ms: float | None = None
    sync_ms: float | None = None
    total_ms: float | None = None
    status: str = "error"
    detail: str = ""

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def _matrix_base(case):
    tool = "pt" if isinstance(case.tool_opts, MatrixCasePTOpts) else "rust"
    mode = None if tool == "pt" else case.tool_opts.mode
    r = MatrixResult(
        table_schema_case=case.table_schema_case,
        key=case.key(),
        binary=case.binary,
        tool=tool,
        mode=mode,
        mysqls=case.mysqls,
    )
    if isinstance(case.tool_opts, MatrixCaseRustOpts):
        r.hash_algo = case.tool_opts.hash_algo
        r.chunked = case.tool_opts.chunked
        r.compression = case.tool_opts.compression
        r.transport = case.tool_opts.transport
    return r


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
        ms, (stdout, stderr, rc) = await _timed([*base, "--check", "0", selector], stdin_data)
        out.update(total_ms=ms, status="reject" if rc != 0 else "unexpected-ok",
                   detail=f"rc={rc}")
        return out

    # prepare_tables has already removed chunks from both sides. With the
    # default CHUNK_SIZE=2 and 10k rows there are 100 chunks, so the drops are
    # real differences and the first check must report drift.
    drift_ms, (stdout, stderr, rc) = await _timed([*base, "--check", "0", selector], stdin_data)
    if rc != 1:
        out.update(drift_ms=drift_ms,
                   detail=f"drift rc={rc}: {(stdout.decode('utf8','replace') + stderr.decode('utf8','replace'))[-300:]}")
        return out

    sync_ms, (stdout, stderr, rc) = await _timed([*base, "--sync", selector], stdin_data)
    if rc != 0:
        out.update(drift_ms=drift_ms, sync_ms=sync_ms,
                   detail=f"sync rc={rc}: {(stdout.decode('utf8','replace') + stderr.decode('utf8','replace'))[-300:]}")
        return out

    clean_ms, (stdout, stderr, rc) = await _timed([*base, "--check", "0", selector], stdin_data)
    if rc != 0:
        out.update(drift_ms=drift_ms, sync_ms=sync_ms, clean_ms=clean_ms,
                   detail=f"clean rc={rc}: {(stdout.decode('utf8','replace') + stderr.decode('utf8','replace'))[-300:]}")
        return out

    a = await src.scalar(_matrix_checksum_sql(case, src_table), quiet=True)
    b = await dst.scalar(_matrix_checksum_sql(case, dst_table), quiet=True)
    out.update(clean_ms=clean_ms, drift_ms=drift_ms, sync_ms=sync_ms,
               total_ms=clean_ms + drift_ms + sync_ms,
               status="ok" if a == b else "mismatch",
               detail="" if a == b else f"{a} != {b}")
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
    drift_ms, (stdout, stderr, rc) = await _timed([*common, "--print", src_dsn, dst_dsn])
    if rc not in (0, 2):
        out.update(drift_ms=drift_ms,
                   detail=f"print rc={rc}: {(stdout.decode('utf8','replace') + stderr.decode('utf8','replace'))[-300:]}")
        return out

    sync_ms, (stdout, stderr, rc) = await _timed([*common, "--execute", src_dsn, dst_dsn])
    if rc not in (0, 2):
        out.update(drift_ms=drift_ms, sync_ms=sync_ms,
                   detail=f"execute rc={rc}: {(stdout.decode('utf8','replace') + stderr.decode('utf8','replace'))[-300:]}")
        return out

    _, (stdout, stderr, rc) = await _timed(
        [str(BIN), "-C", "-", "--no-progress", "--check", "0", f"{MATRIX_DB}.{src_table}"],
        cfg_json.encode()
    )
    out_text = stdout.decode('utf8','replace') + stderr.decode('utf8','replace')
    a = await src.scalar(_matrix_checksum_sql(case, src_table), quiet=True)
    b = await dst.scalar(_matrix_checksum_sql(case, dst_table), quiet=True)
    ok = rc == 0 and a == b
    out.update(drift_ms=drift_ms, sync_ms=sync_ms, total_ms=drift_ms + sync_ms,
               status="ok" if ok else "mismatch",
               detail="" if ok else f"rust --check rc={rc}: {out_text[-200:]} | {a} != {b}")
    return out

LOCK_FROM_TABLES = defaultdict(asyncio.Lock)


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
                await _prepare_matrix_table(src, case, src_table)
                await _drop_matrix_chunks(src, src_table, CHUNK_DROP_START_SOURCE)

    async def prepare_dest():
        await _prepare_matrix_table(dst, case, dst_table)
        await _drop_matrix_chunks(dst, dst_table, CHUNK_DROP_START_DEST)

    return await gather(prepare_source(), prepare_dest())

async def _matrix_runner(cfg, case: MatrixCase):
    try:
        await prepare_tables(case)
        if isinstance(case.tool_opts, MatrixCasePTOpts):
            return await _run_pt_case(cfg, case)
        else:
            return await _run_rust_case(cfg, case)
    except Exception as exc:
        out = _matrix_base(case)
        out.update(status="error", detail=str(exc)[-300:])
        return out

async def _matrix_async(cases, fingerprints):
    global CONCURRENCY
    sem = Semaphore(CONCURRENCY)

    async def one(case):
        async with sem:
            client_cfg_json = make_client_cfg(case, fingerprints[case.mysqls[1].name])
            return await _matrix_runner(client_cfg_json, case)

    return await gather(*(one(case) for case in cases))


def _report_matrix(rows, wall_ms):
    global CONCURRENCY
    print("\n== Matrix: rust-sync vs pt-table-sync ==")
    print(f"concurrency={CONCURRENCY} rows={MATRIX_ROWS} "
          f"wall={wall_ms:.0f} ms")
    print("  Legend (all times in ms):")
    print("    clean = check while tables are identical (rust only; detection, no writes)")
    print("    drift = check while tables differ (detection only, no writes)")
    print("    sync  = apply the differences (this is the real sync time)")
    print("    total = clean + drift + sync = total time for this table")
    print("    pt-table-sync: drift = --print, sync = --execute (no clean run)")
    print("    mode: select-sha2/select-crc32/dump/raw-binary/raw-tsv = rust-sync hashing method")
    print("    algo: xxh3/blake3/sha256 = Rust-side hash algorithm (raw modes)")
    print("    chunked: 1-6 = chunk size exponent")
    print("    comp: none/zstd = wire compression")
    print("    trans: quic (TLS 1.3 encrypted) / tcp (plaintext)")
    print("    mysqls: source→dest MySQL version pair")
    print("key\tbin\tmysqls\tsync\ttool\tmode\talgo\tchunked\tcomp\ttrans\tclean\tdrift\ttotal\tstatus")
    for r in sorted(rows, key=lambda x: (x.key, x.binary, x.tool, x.mode or "")):
        def ms(v):
            return "-" if v is None else f"{v:.0f}"
        mysqls_str = f"{r.mysqls[0].version}:{r.mysqls[1].version}"
        print(f"{r.key}\t{r.binary}\t{mysqls_str}\t{ms(r.sync_ms)}\t{r.tool}\t"
              f"{r.mode or '-'}\t{r.hash_algo}\t{r.chunked}\t{r.compression}\t{r.transport}\t"
              f"{ms(r.clean_ms)}\t{ms(r.drift_ms)}\t"
              f"{ms(r.total_ms)}\t{r.status}"
              + (f" ({r.detail})" if r.detail else ""))

    print("  Totals:")
    groups = {}
    for r in rows:
        if r.tool == "pt":
            label = "pt"
        else:
            cfg = ""
            if r.hash_algo != "xxh3" or r.chunked != 3 or r.compression != "none" or r.transport != "quic":
                cfg = f"({r.hash_algo},c{r.chunked},{r.compression},{r.transport})"
            label = f"rust/{r.mode or ''}{cfg}"
        g = groups.setdefault(label, {"n": 0, "clean": 0.0, "drift": 0.0, "sync": 0.0, "total": 0.0})
        g["n"] += 1
        for k in ("clean", "drift", "sync", "total"):
            val = getattr(r, f"{k}_ms")
            if val is not None:
                g[k] += val
    print("tool\tn\tclean\tdrift\tsync\ttotal")
    for label in sorted(groups):
        g = groups[label]
        print(f"{label}\t{g['n']}\t{g['clean']:.0f}\t{g['drift']:.0f}\t"
              f"{g['sync']:.0f}\t{g['total']:.0f}")
    grand = sum(r.total_ms or 0.0 for r in rows)
    print(f"TOTAL\t{len(rows)}\t-\t-\t-\t{grand:.0f}")


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
            return True
        except Exception:
            await asyncio.sleep(2)
            print(".. waiting for dbs")
    assert False, "dbs not up"

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
    cfg_dict = {
        "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": src.port},
        "server": f"127.0.0.1:{dst.server_port}",
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
        print(f"  {m.name}: MySQL 127.0.0.1:{m.port}, QUIC 127.0.0.1:{m.server_port}")


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
    await ensure_binary()
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
    bad = [r for r in rows if r.status not in ("ok", "reject", "skip")]
    skipped = [r for r in rows if r.status == "skip"]
    return step("Matrix: alle Fälle", not bad,
                f"{len(rows)-len(bad)-len(skipped)} ok, {len(skipped)} skip, {len(bad)} Fehler")

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


ACTIONS = {
    "write_docker_compose": write_docker_compose,
    "up": up,
    "matrix": matrix,
    "down": down,
    "clean": clean,
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
