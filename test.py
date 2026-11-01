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

TEST_TEXT = "A\nB\n\\\BinärmüllÄß"

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

# @dataclass()
# # slots=True => packed
# class MFieldTypeInt:
#     len: Int
# class MFieldTypeVarchar:
#     len: Int
# class MFieldTypeBinary:
#     len: Int
# class MFieldTypeDateTime:
#     len: Int

# @dataclass()
# # slots=True => packed
# class MField:
#     name: str
#     type: MFieldType
#     nullable: bool = False

@dataclass(frozen=True)
class Mysql:
    name: str
    version: str
    port: int

    def sql(self, *queries, database=None, check=True, quiet=False):
        """SQL auf die MySQL-Instanz dieses Objekts ausführen."""
        cli = os.environ.get("MSYNC_MYSQL_CLI", "mysql")
        cmd = [cli, "-h", "127.0.0.1", "-P", str(self.port), "-uroot"]
        if ROOT_PW:
            cmd.append(f"-p{ROOT_PW}")
        if database:
            cmd.append(database)
        r = None
        for query in queries:
            r = run(cmd + ["-e", query], check=check, quiet=quiet)
        assert r
        return r.stdout


    def scalar(self, query, database=None, quiet=False):
        out = self.sql(query, database = database, quiet=quiet).strip().splitlines()
        return out[-1] if out else None

    def table_exists(self, table):
        return self.scalar(
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
        }, indent=2))
        return cfg

    @asynccontextmanager
    async def server_running(self):
        cfg = self.server_config()
        fp_file = cfg.with_suffix(".fingerprint")
        fp_file.unlink(missing_ok=True)
        log = open(WORK / f"server-{self.name}.log", "w")
        proc = subprocess.Popen([str(BIN), "-C", str(cfg)], stdout=log, stderr=log)
        log.close()
        try:
            fingerprint = None
            for _ in range(50):
                if fp_file.exists():
                    fingerprint = fp_file.read_text().strip()
                    break
                await sleep(0.2)
            if not fingerprint:
                raise RuntimeError(f"{self.name}: kein Server-Fingerprint")
            yield fingerprint
        finally:
            proc.terminate()
            try:
                await to_thread(proc.wait, 5)
            except subprocess.TimeoutExpired:
                proc.kill()
                await to_thread(proc.wait)

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


def _binary_kind(path):
    try:
        head = path.read_bytes()[:4]
    except OSError:
        return None
    if head == b"\x7fELF":
        return "linux"
    if head in (b"\xcf\xfa\xed\xfe", b"\xce\xfa\xed\xfe",
                b"\xfe\xed\xfa\xce", b"\xfe\xed\xfa\xcf", b"\xca\xfe\xba\xbe"):
        return "macos"
    if head[:2] == b"MZ":
        return "windows"
    return None


def ensure_binary():
    if BIN.exists():
        kind = _binary_kind(BIN)
        if kind == "macos":
            raise SystemExit(
                f"Binary ist für macOS gebaut: {BIN}\n"
                "Auf Linux neu bauen: cargo build --release"
            )
        if not os.access(BIN, os.X_OK):
            raise SystemExit(f"Binary nicht ausführbar: {BIN}")
        return
    if os.environ.get("MSYNC_BIN"):
        raise SystemExit(f"MSYNC_BIN gesetzt, aber Binary fehlt: {BIN}")
    if os.environ.get("MSYNC_NO_BUILD"):
        raise SystemExit(f"Binary fehlt: {BIN} (MSYNC_NO_BUILD gesetzt)")
    if shutil.which("cargo") is None:
        raise SystemExit(f"Binary fehlt: {BIN}; cargo nicht im PATH")
    print(f"== Build ==\n  Binary fehlt: {BIN}\n  cargo +nightly build --release")
    r = subprocess.run(["cargo", "+nightly", "build", "--release"], cwd=HERE)
    if r.returncode != 0:
        raise SystemExit("cargo build --release fehlgeschlagen")
    if not BIN.exists():
        raise SystemExit(f"Binary auch nach dem Build nicht gefunden: {BIN}")


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


MATRIX_DB = "db_matrix"
MATRIX_ROWS = 10000

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
    ("select", None),
    ("dump", "--hash-dump"),
    ("raw-binary", "--hash-raw-binary-row"),
    ("raw-tsv", "--hash-raw-tsv"),
)

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

    def source_table(self):
        return f"src_{CHUNK_SIZE}_{MATRIX_ROWS}_{self.table_schema_case}_{self.binary}"

    def dest_table(self):
        return f"dst_{CHUNK_SIZE}_{MATRIX_ROWS}_{self.table_schema_case}_{self.binary}_{self.hash}"

    def table_name(self):
        return self.dest_table()

    def table_names(self):
        return self.source_table(), self.dest_table()

    def _dims(self):
        tool = "pt" if isinstance(self.tool_opts, MatrixCasePTOpts) else "rust"
        mode = None if tool == "pt" else self.tool_opts.mode
        return {
            "table_schema_case": self.table_schema_case,
            "key": self.key(),
            "binary": self.binary,
            "table": self.table_name(),
            "tool": tool,
            "mode": mode,
            "mysqls": self.mysqls,
        }

    def keys(self):
        return self._dims().keys()

    def __getitem__(self, key):
        return self._dims()[key]

    def __iter__(self):
        return iter(self._dims())

    def __len__(self):
        return len(self._dims())


def _mysql_by_version(version):
    for m in MYSQL_INSTANCES.values():
        if m.version == version:
            return m
    raise SystemExit(f"Keine MySQL-Instanz fuer Version {version} konfiguriert")


def matrix_cases():
    cases = []
    # Nur Paare, fuer die tatsaechlich Instanzen laufen. Weitere Versionen
    # (z.B. 8.0:8.0) brauchen zusaetzliche Container in docker-compose.yml.
    version_pairs = [("8.0", "9.7"), ("8.0", "8.0")]
    for (table_schema_case, schema), binary, (v_src, v_dst) in product(
            TABLE_SCHEMAS.items(),
            ("no_binary", "bin"),
            version_pairs):
        mysqls = (_mysql_by_version(v_src), _mysql_by_version(v_dst))
        common = {"table_schema_case": table_schema_case,
                  "schema": schema,
                  "binary": binary, "mysqls": mysqls}
        cases.append(MatrixCase(**common, tool_opts=MatrixCasePTOpts()))
        for mode, _ in RUST_MODES:
            cases.append(MatrixCase(**common, tool_opts=MatrixCaseRustOpts(mode)))
    return cases

def _matrix_ddl(case, table):
    fields = case.schema.fields
    key = fields[0].sql_type()
    id2_type = fields[1].sql_type() if case.schema.primary_key_fields > 1 else None
    tuple_col = f"  id2 {id2_type} NOT NULL,\n" if id2_type else ""
    pk = "(id, id2)" if id2_type else "(id)"
    bin_col = "  bin_data VARBINARY(128) NULL,\n" if case["binary"] == "bin" else ""
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
    if case["binary"] == "bin":
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
    update_bin = ", bin_data = UNHEX('DEADBEEF')" if case["binary"] == "bin" else ""
    id_type = case.schema.fields[0].type
    if id_type.startswith("int"):
        extra_id = f"{MATRIX_EXTRA_OFFSET} + n"
    elif id_type.startswith("blob"):
        extra_id = f"UNHEX(CONCAT('E', LPAD(HEX(n), 7, '0'), '000000'))"
    else:
        extra_id = "CONCAT('extra-', LPAD(n, 4, '0'))"
    tuple_col = ", id2" if case.schema.primary_key_fields > 1 else ""
    tuple_val = ", " + _matrix_id2_expr(case) if case.schema.primary_key_fields > 1 else ""
    bin_col = ", bin_data" if case["binary"] == "bin" else ""
    bin_val = ", UNHEX('00FF00FF')" if case["binary"] == "bin" else ""
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
    if case["binary"] == "bin":
        cols.append("IFNULL(HEX(bin_data), 'NULL')")
    expr = "CONCAT_WS('|', " + ", ".join(cols) + ")"
    return f"SELECT CONCAT(COUNT(*), ':', COALESCE(SUM(CRC32({expr})), 0)) FROM {MATRIX_DB}.{table}"


def _timed(cmd):
    started = time.perf_counter()
    r = subprocess.run([str(c) for c in cmd], text=True, capture_output=True,
                       cwd=WORK, timeout=MATRIX_TIMEOUT)
    return (time.perf_counter() - started) * 1000.0, r


def _matrix_base(case):
    out = {**case._dims(), "clean_ms": None, "drift_ms": None, "sync_ms": None,
           "total_ms": None, "status": "error", "detail": ""}
    return out


def _case_scalar(case, which, query):
    m = case.mysqls[0] if which == "src" else case.mysqls[1]
    return m.scalar(query, quiet=True)


def _run_rust_case(cfg, case):
    out = _matrix_base(case)
    src_table, dst_table = case.table_names()
    selector = f"{MATRIX_DB}.{src_table}"
    flag = dict(RUST_MODES)[case["mode"]]
    base = [str(BIN), *cfg, "--no-progress"] + ([flag] if flag else [])

    if case["mode"] == "raw-tsv" and case["binary"] == "bin":
        ms, r = _timed([*base, "--check", "0", selector])
        out.update(total_ms=ms, status="reject" if r.returncode != 0 else "unexpected-ok",
                   detail=f"rc={r.returncode}")
        return out

    # prepare_tables has already removed chunks from both sides. With the
    # default CHUNK_SIZE=2 and 10k rows there are 100 chunks, so the drops are
    # real differences and the first check must report drift.
    drift_ms, r = _timed([*base, "--check", "0", selector])
    if r.returncode != 1:
        out.update(drift_ms=drift_ms,
                   detail=f"drift rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    sync_ms, r = _timed([*base, "--sync", selector])
    if r.returncode != 0:
        out.update(drift_ms=drift_ms, sync_ms=sync_ms,
                   detail=f"sync rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    clean_ms, r = _timed([*base, "--check", "0", selector])
    if r.returncode != 0:
        out.update(drift_ms=drift_ms, sync_ms=sync_ms, clean_ms=clean_ms,
                   detail=f"clean rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    a = _case_scalar(case, "src", _matrix_checksum_sql(case, src_table))
    b = _case_scalar(case, "dst", _matrix_checksum_sql(case, dst_table))
    out.update(clean_ms=clean_ms, drift_ms=drift_ms, sync_ms=sync_ms,
               total_ms=clean_ms + drift_ms + sync_ms,
               status="ok" if a == b else "mismatch",
               detail="" if a == b else f"{a} != {b}")
    return out


def _run_pt_case(cfg, case):
    # pt-table-sync exit 2 means "Differences found" (also after a successful
    # --execute), 0 means "Nothing to do", 1 means error. Verify the outcome
    # with our own check instead of trusting the exit code.
    out = _matrix_base(case)
    if not PT_BIN:
        out.update(status="skip", detail="pt-table-sync nicht im PATH")
        return out
    src_table, dst_table = case.table_names()
    src, dst = case.mysqls
    dst.sql(_matrix_drift(case, dst_table), quiet=True)
    src_dsn, dst_dsn = src._pt_dsn(src_table), dst._pt_dsn(dst_table)
    common = [PT_BIN, "--no-check-slave", "--no-check-triggers", "--chunk-size=1000"]

    # pt always runs against the drifted tables (no clean pass):
    # --print is the drift detection, --execute applies the differences.
    drift_ms, r = _timed([*common, "--print", src_dsn, dst_dsn])
    if r.returncode not in (0, 2):
        out.update(drift_ms=drift_ms,
                   detail=f"print rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    sync_ms, r = _timed([*common, "--execute", src_dsn, dst_dsn])
    if r.returncode not in (0, 2):
        out.update(drift_ms=drift_ms, sync_ms=sync_ms,
                   detail=f"execute rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    rc, out_text = run_bin([*cfg, "--no-progress", "--check", "0",
                            f"{MATRIX_DB}.{src_table}"])
    a = _case_scalar(case, "src", _matrix_checksum_sql(case, src_table))
    b = _case_scalar(case, "dst", _matrix_checksum_sql(case, dst_table))
    ok = rc == 0 and a == b
    out.update(drift_ms=drift_ms, sync_ms=sync_ms, total_ms=drift_ms + sync_ms,
               status="ok" if ok else "mismatch",
               detail="" if ok else f"rust --check rc={rc}: {out_text[-200:]} | {a} != {b}")
    return out

LOCK_FROM_TABLES = defaultdict(threading.Lock)


def _prepare_matrix_table(m, case, table):
    m.sql(f"DROP TABLE IF EXISTS {MATRIX_DB}.{table}", quiet=True)
    m.sql(_matrix_ddl(case, table), quiet=True)
    m.sql(_matrix_seed(case, MATRIX_ROWS, table), quiet=True)


def _drop_matrix_chunks(m, table, start):
    return run([str(BIN), "-C", "-", f"--testing-drop-chunks={start}",
                f"{MATRIX_DB}.{table}"], input=_matrix_mysql_json(m))


async def prepare_tables(case: MatrixCase):
    src_table, dst_table = case.table_names()
    src, dst = case.mysqls

    def prepare_source():
        with LOCK_FROM_TABLES[(src.name, src_table)]:
            if not src.table_exists(src_table):
                _prepare_matrix_table(src, case, src_table)
                _drop_matrix_chunks(src, src_table, CHUNK_DROP_START_SOURCE)

    def prepare_dest():
        _prepare_matrix_table(dst, case, dst_table)
        _drop_matrix_chunks(dst, dst_table, CHUNK_DROP_START_DEST)

    return await gather(to_thread(prepare_source), to_thread(prepare_dest))

async def _matrix_runner(cfg, case: MatrixCase):
    try:
        src, dst = await prepare_tables(case)
        if case.tool_opts is MatrixCasePTOpts:
            return _run_pt_case(cfg, case)
        else:
            return _run_rust_case(cfg, case)
    except Exception as exc:
        out = _matrix_base(case)
        out.update(status="error", detail=str(exc)[-300:])
        return out

async def _matrix_async(cases, fingerprints):
    global CONCURRENCY
    sem = Semaphore(CONCURRENCY)

    async def one(case):
        async with sem:
            client_cfg = make_client_cfg(case, fingerprints[case.mysqls[1].name])
            return await _matrix_runner(["-C", str(client_cfg)], case)

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
    print("    mode: select/dump/raw-binary/raw-tsv = rust-sync hashing method")
    print("key\tbin\tsync\ttool\tmode\tclean\tdrift\ttotal\tstatus")
    for r in sorted(rows, key=lambda x: (x["key"], x["binary"], x["tool"], x["mode"] or "")):
        def ms(v):
            return "-" if v is None else f"{v:.0f}"
        print(f"{r['key']}\t{r['binary']}\t{ms(r['sync_ms'])}\t{r['tool']}\t"
              f"{r['mode'] or '-'}\t{ms(r['clean_ms'])}\t{ms(r['drift_ms'])}\t"
              f"{ms(r['total_ms'])}\t{r['status']}"
              + (f" ({r['detail']})" if r["detail"] else ""))

    print("  Totals:")
    groups = {}
    for r in rows:
        label = r["tool"] if r["tool"] == "pt" else f"rust/{r['mode']}"
        g = groups.setdefault(label, {"n": 0, "clean": 0.0, "drift": 0.0, "sync": 0.0, "total": 0.0})
        g["n"] += 1
        for k in ("clean", "drift", "sync", "total"):
            if r[f"{k}_ms"] is not None:
                g[k] += r[f"{k}_ms"]
    print("tool\tn\tclean\tdrift\tsync\ttotal")
    for label in sorted(groups):
        g = groups[label]
        print(f"{label}\t{g['n']}\t{g['clean']:.0f}\t{g['drift']:.0f}\t"
              f"{g['sync']:.0f}\t{g['total']:.0f}")
    grand = sum(r["total_ms"] or 0.0 for r in rows)
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
                m.sql("SELECT 1", quiet=True)
            return True
        except SystemExit:
            time.sleep(2)
            print(".. waiting for dbs")
    assert False, "dbs not up"

def make_client_cfg(case, fingerprint):
    src, dst = case.mysqls
    src_table, dst_table = case.table_names()
    cfg = WORK / f"config-client-{case.hash}.json"
    cfg.write_text(json.dumps({
        "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": src.port},
        "server": f"127.0.0.1:{dst.server_port}",
        "server_fingerprint": fingerprint,
        "allow_csv": True,
        "dump_strategies": ["select-stream"],
        "transfer_batch_rows": 1000,
        "chunked": 3,
        "concurrent_processes": 3,
        "diff_threshold_percent": 10,
        "database_table_mappings": {
            f"{MATRIX_DB}.{src_table}": f"{MATRIX_DB}.{dst_table}",
        },
    }, indent=2))
    return cfg


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
    ensure_binary()
    prepare_workdir()
    await ensure_dbs_up()
    print_env()

    for m in MYSQL_INSTANCES.values():
        m.sql(f"CREATE DATABASE IF NOT EXISTS {MATRIX_DB} CHARACTER SET utf8mb4", quiet=True)

    cases = matrix_cases()
    async with AsyncExitStack() as stack:
        fingerprints = {}
        for m in MYSQL_INSTANCES.values():
            fingerprints[m.name] = await stack.enter_async_context(m.server_running())
        started = time.perf_counter()
        rows = await _matrix_async(cases, fingerprints)
        wall_ms = (time.perf_counter() - started) * 1000.0
    _report_matrix(rows, wall_ms)
    bad = [r for r in rows if r["status"] not in ("ok", "reject", "skip")]
    skipped = [r for r in rows if r["status"] == "skip"]
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
    prepare_workdir()
    await docker_db_up()


async def down():
    await compose("down", "--remove-orphans")


async def clean():
    await compose("down", "--remove-orphans", "--volumes")


ACTIONS = {
    "up": up,
    "matrix": matrix,
    "down": down,
    "clean": clean,
}

async def main():
    global CONCURRENCY
    args = sys.argv[1:]
    print(args)

    # --concurrency[N]
    c = "--concurrency"
    if args and args[0].startswith(c):
        CONCURRENCY = int(args[0][len(c):]) or 1
        args = args[1:]

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
