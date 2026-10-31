#!/usr/bin/env python3
"""

Docker only so that test is portable.
table names have prefixes

src_XXXX -> can be reused
dst_YYYY -> where YYYY is a specific test case so each test case gets its own rw target table

The docker mysql instances are both read and write targets

to keep it simpsle the database is called 
rust_mysql_sync.* for now

mysql-sync End-to-End-Test.

Aufruf:
Modi:
  1. Docker (Standard):  startet 2 MySQL-8-Container via docker compose,
     baut/startet den Server-Binary auf dem Host und spielt alle Szenarien durch.

Szenarien (Aktion old):
  A) --force-schema  : Schema auf Ziel erzwingen
  B) --sync          : differentieller Abgleich, danach Zähler + BLOB-Prüfsummen gleich
  C) --check 10      : Exit 0 wenn identisch (select/dump/raw-binary/raw-tsv)
  D) Drift + --check 5  : Exit 1, erwartete Diff-Anzahl (49 plain / 19 binary) gemeldet
  E) --sync          : Drift behoben (raw-tsv + raw-binary), wieder identisch
  F) --force --sync  : Tabelle neu erstellen + Vollkopie, wieder identisch
  G) SQL-Dump-Modus  : allow_csv=false, binärsicher
  H) hash_algo       : Mismatch wird abgelehnt, blake3 funktioniert
  I) zstd            : Kompression auf beiden Seiten
  J) TCP             : Klartext-Transport (Opt-in) + Ablehnung ohne allow_plaintext
  K) Chunking        : Range fuer INTEGER/CHAR/BINARY, automatischer Mod-Fallback fuer
                       DATE/Composite, select/dump/raw, exakte Drift-Zahlen, Fehler ohne PK
  L) Raw-Profil-Gate : raw-binary/raw-tsv lehnen abweichendes MySQL-Profil ab

Matrix (Aktion matrix):
  Produkt aus Tool (rust-sync, pt-table-sync), Key (int, str, int-tuple,
  str-tuple, mixed-tuple) und Binärspalte (nein, ja). "-tuple" heißt
  zusammengesetzter Primary Key (id, id2); mixed-tuple ist str-int
  (id VARCHAR, id2 INT). rust-sync zusätzlich pro Read-Mode (select, dump,
  raw-binary, raw-tsv). Jeder Fall bekommt aus seinem Dimensions-Hash eine eigene Tabelle
  in db_matrix, damit parallele Fälle sich nicht beeinflussen. Gemessen werden
  Clean-Check, Drift-Check und Sync (pt-table-sync: --print und --execute).
  Globale Matrix-Parameter: MATRIX_ROWS=100000, MATRIX_JOBS=4, MATRIX_TIMEOUT=300.
  MSYNC_PT_TABLE_SYNC (sonst PATH-Suche) steuert den pt-table-sync-Pfad.

Exit 0 = alle aufgerufenen Aktionen bestanden.
"""

import asyncio
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
from datetime import datetime
from pathlib import Path
from yachalk import chalk
from asyncio import create_task, to_thread, sleep, timeout, timeout_at, Semaphore, Future
import asyncio

from u_redirect_logging import with_logging
from u_asyncio import run_show_out_tail_on_error, task

START_PORT = 7012

DOCKER_MYSQL_INSTANCES = [
    {"name": "mysql80", "version": 8.0, "port": 7012},
    {"name": "mysql97", "version": 9.7, "port": 7013}
]

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

E = os.environ

DOCKER = E["MSYNC_DOCKER"]
SRC_PORT = int(E["MSYNC_SRC_PORT"])
DST_PORT = int(E["MSYNC_DST_PORT"])
ROOT_PW = E["MSYNC_ROOT_PW"] # "syncroot"
MYSQL_SYNC_SERVER_PORT = int(E["MYSQL_SYNC_SERVER_PORT"])

PASS, FAIL = chalk.green("PASS"), chalk.red("FAIL")
SKIP = chalk.yellow("SKIP")
results = []
CONCURRENCY = 1

LOG_PATH = Path(os.environ.get("MSYNC_LOG") or (HERE / "test.log"))


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
    # if BIN.exists():
    #     kind = _binary_kind(BIN)
    #     if kind == "macos":
    #         raise SystemExit(
    #             f"Binary ist für macOS gebaut: {BIN}\n"
    #             "Auf Linux neu bauen: cargo build --release"
    #         )
    #     if not os.access(BIN, os.X_OK):
    #         raise SystemExit(f"Binary nicht ausführbar: {BIN}")
    #     return
    if os.environ.get("MSYNC_BIN"):
        raise SystemExit(f"MSYNC_BIN gesetzt, aber Binary fehlt: {BIN}")
    if os.environ.get("MSYNC_NO_BUILD"):
        raise SystemExit(f"Binary fehlt: {BIN} (MSYNC_NO_BUILD gesetzt)")
    if shutil.which("cargo") is None:
        raise SystemExit(f"Binary fehlt: {BIN}; cargo nicht im PATH")
    print(f"== Build ==\n  Binary fehlt: {BIN}\n  cargo build --release")
    r = subprocess.run(["cargo", "build", "--release"], cwd=HERE)
    if r.returncode != 0:
        raise SystemExit("cargo build --release fehlgeschlagen")
    if not BIN.exists():
        raise SystemExit(f"Binary auch nach dem Build nicht gefunden: {BIN}")

def sql(which, query, database=None, check=True, quiet=False):
    """SQL auf Quelle (which='src') oder Ziel (which='dst') ausführen."""
    if DOCKER:
        svc = "mysql-src" if which == "src" else "mysql-dst"
        cmd = ["docker", "compose", "-f", HERE / "docker-compose.yml", "exec", "-T", svc,
               "mysql", "-uroot", f"-p{ROOT_PW}"]
    else:
        cli = os.environ.get("MSYNC_MYSQL_CLI", "mysql")
        cmd = [cli, "--no-defaults", "-h", "127.0.0.1",
               "-P", str(SRC_PORT if which == "src" else DST_PORT), "-uroot"]
        if ROOT_PW:
            cmd.append(f"-p{ROOT_PW}")
    if database:
        cmd.append(database)
    r = run(cmd + ["-e", query], check=check, quiet=quiet)
    return r.stdout


def scalar(which, query, database=None, quiet=False):
    out = sql(which, query, database, quiet=quiet).strip().splitlines()
    return out[-1] if out else None


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
MATRIX_ROWS = 100000
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

def _matrix_case(tool, key, binary, mode=None):
    dims = {"tool": tool, "key": key, "binary": binary, "mode": mode}
    payload = json.dumps(dims, sort_keys=True, separators=(",", ":"))
    table = "mx_" + hashlib.sha256(payload.encode()).hexdigest()[:16]
    return {**dims, "table": table}


def matrix_cases():
    cases = []
    # different mysql versions !
    # the binary-via-
    for key, binary, mysql_versions in product(
           # primary key  variations
           ("int", "str", "int-tuple", "str-tuple", "mixed-tuple"),

           # with or without binary
           ("none", "bin"),

           # mysql versions differ
           # --hash-raw-binary-row -> expected to fail if mysql versions differ
           # --hash-raw-binary-row-vec -> expected to always work
           ("8.0:8.0", "8.0:9.7")
        ):
        cases.append(_matrix_case("pt", key, binary))
        for mode, _ in RUST_MODES:
            cases.append(_matrix_case("rust", key, binary, mode))
    return cases

def _key_kind(case):
    return case["key"].split("-")[0]


def _is_tuple(case):
    return case["key"].endswith("-tuple")


def _key_types(case):
    """(id_type, id2_type) — id2_type ist None ohne zusammengesetzten Key."""
    id_type = "BIGINT" if _key_kind(case) == "int" else "VARCHAR(32)"
    if not _is_tuple(case):
        return id_type, None
    return id_type, "VARCHAR(32)" if _key_kind(case) == "str" else "INT"


def _matrix_ddl(case):
    key, id2_type = _key_types(case)
    tuple_col = f"  id2 {id2_type} NOT NULL,\n" if id2_type else ""
    pk = "(id, id2)" if id2_type else "(id)"
    bin_col = "  bin_data VARBINARY(128) NULL,\n" if case["binary"] == "bin" else ""
    return (
        f"CREATE TABLE {MATRIX_DB}.{case['table']} (\n"
        f"  id {key} NOT NULL,\n"
        f"{tuple_col}"
        f"{bin_col}"
        "  txt VARCHAR(64) NOT NULL,\n"
        "  n INT NOT NULL,\n"
        f"  PRIMARY KEY {pk}\n"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )


def _matrix_id_expr(case, n="n"):
    base = f"CEIL({n} / 3)" if _is_tuple(case) else n
    if _key_kind(case) == "int":
        return base
    return f"CONCAT('k', LPAD({base}, 8, '0'))"


def _matrix_id2_expr(case, n="n"):
    if _key_kind(case) == "str":
        return f"CONCAT('c', MOD({n}, 3))"
    return f"MOD({n}, 3)"


def _matrix_seed(case, rows):
    table = f"{MATRIX_DB}.{case['table']}"
    id_expr = _matrix_id_expr(case)
    txt_expr = "CONCAT('row-', n, CASE WHEN n % 7 = 0 THEN '\\tTab' ELSE '' END)"
    cols = ["id"]
    vals = [id_expr]
    if _is_tuple(case):
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


def _matrix_drift(case):
    table = f"{MATRIX_DB}.{case['table']}"
    update_bin = ", bin_data = UNHEX('DEADBEEF')" if case["binary"] == "bin" else ""
    extra_id = (f"{MATRIX_EXTRA_OFFSET} + n" if _key_kind(case) == "int"
                else "CONCAT('extra-', LPAD(n, 4, '0'))")
    tuple_col = ", id2" if _is_tuple(case) else ""
    tuple_val = ", " + _matrix_id2_expr(case) if _is_tuple(case) else ""
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


def _matrix_checksum_sql(case):
    cols = ["HEX(id)"]
    if _is_tuple(case):
        cols.append("CAST(id2 AS CHAR)")
    cols += ["HEX(txt)", "CAST(n AS CHAR)"]
    if case["binary"] == "bin":
        cols.append("IFNULL(HEX(bin_data), 'NULL')")
    expr = "CONCAT_WS('|', " + ", ".join(cols) + ")"
    return f"SELECT CONCAT(COUNT(*), ':', COALESCE(SUM(CRC32({expr})), 0)) FROM {MATRIX_DB}.{case['table']}"


def _matrix_setup(case):
    for which in ("src", "dst"):
        sql(which, f"DROP TABLE IF EXISTS {MATRIX_DB}.{case['table']}", quiet=True)
        sql(which, _matrix_ddl(case), quiet=True)
        sql(which, _matrix_seed(case, MATRIX_ROWS), quiet=True)
    a = scalar("src", _matrix_checksum_sql(case), quiet=True)
    b = scalar("dst", _matrix_checksum_sql(case), quiet=True)
    if a != b:
        raise RuntimeError(f"Matrix-Setup ungleich: {a} != {b}")


def _timed(cmd):
    started = time.perf_counter()
    r = subprocess.run([str(c) for c in cmd], text=True, capture_output=True,
                       cwd=WORK, timeout=MATRIX_TIMEOUT)
    return (time.perf_counter() - started) * 1000.0, r


def _matrix_base(case):
    out = {**case, "clean_ms": None, "drift_ms": None, "sync_ms": None,
           "total_ms": None, "status": "error", "detail": ""}
    return out


def _run_rust_case(cfg, case):
    out = _matrix_base(case)
    _matrix_setup(case)
    selector = f"{MATRIX_DB}.{case['table']}"
    flag = dict(RUST_MODES)[case["mode"]]
    base = [str(BIN), *cfg, "--no-progress"] + ([flag] if flag else [])

    if case["mode"] == "raw-tsv" and case["binary"] == "bin":
        ms, r = _timed([*base, "--check", "0", selector])
        out.update(total_ms=ms, status="reject" if r.returncode != 0 else "unexpected-ok",
                   detail=f"rc={r.returncode}")
        return out

    clean_ms, r = _timed([*base, "--check", "0", selector])
    if r.returncode != 0:
        out.update(clean_ms=clean_ms, detail=f"clean rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    sql("dst", _matrix_drift(case), quiet=True)
    drift_ms, r = _timed([*base, "--check", "0", selector])
    if r.returncode != 1:
        out.update(clean_ms=clean_ms, drift_ms=drift_ms,
                   detail=f"drift rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    sync_ms, r = _timed([*base, "--sync", selector])
    if r.returncode != 0:
        out.update(clean_ms=clean_ms, drift_ms=drift_ms, sync_ms=sync_ms,
                   detail=f"sync rc={r.returncode}: {(r.stdout + r.stderr)[-300:]}")
        return out

    a = scalar("src", _matrix_checksum_sql(case), quiet=True)
    b = scalar("dst", _matrix_checksum_sql(case), quiet=True)
    out.update(clean_ms=clean_ms, drift_ms=drift_ms, sync_ms=sync_ms,
               total_ms=clean_ms + drift_ms + sync_ms,
               status="ok" if a == b else "mismatch",
               detail="" if a == b else f"{a} != {b}")
    return out


def _pt_dsn(which, case):
    parts = [
        "h=127.0.0.1",
        f"P={SRC_PORT if which == 'src' else DST_PORT}",
        "u=root",
        f"D={MATRIX_DB}",
        f"t={case['table']}",
    ]
    if ROOT_PW:
        parts.append(f"p={ROOT_PW}")
    return ",".join(parts)


def _run_pt_case(cfg, case):
    # pt-table-sync exit 2 means "Differences found" (also after a successful
    # --execute), 0 means "Nothing to do", 1 means error. Verify the outcome
    # with our own check instead of trusting the exit code.
    out = _matrix_base(case)
    if not PT_BIN:
        out.update(status="skip", detail="pt-table-sync nicht im PATH")
        return out
    _matrix_setup(case)
    sql("dst", _matrix_drift(case), quiet=True)
    src_dsn, dst_dsn = _pt_dsn("src", case), _pt_dsn("dst", case)
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
                            f"{MATRIX_DB}.{case['table']}"])
    a = scalar("src", _matrix_checksum_sql(case), quiet=True)
    b = scalar("dst", _matrix_checksum_sql(case), quiet=True)
    ok = rc == 0 and a == b
    out.update(drift_ms=drift_ms, sync_ms=sync_ms, total_ms=drift_ms + sync_ms,
               status="ok" if ok else "mismatch",
               detail="" if ok else f"rust --check rc={rc}: {out_text[-200:]} | {a} != {b}")
    return out


def _matrix_runner(cfg, case):
    try:
        if case["tool"] == "pt":
            return _run_pt_case(cfg, case)
        return _run_rust_case(cfg, case)
    except Exception as exc:
        out = _matrix_base(case)
        out.update(status="error", detail=str(exc)[-300:])
        return out

async def _matrix_async(cfg, cases):
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(case):
        async with sem:
            return await asyncio.to_thread(_matrix_runner, cfg, case)

    return await asyncio.gather(*(one(case) for case in cases))


def _report_matrix(rows, wall_ms):
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
    if not DOCKER:
        return

    # raw-tsv starts the `mysql` CLI on the host (client and server side).
    # Docker mode exposes the databases via published ports, so provide a
    # small mysql wrapper that forwards the scan into the containers.
    bin_dir = WORK / "bin"
    bin_dir.mkdir(exist_ok=True)
    wrapper = bin_dir / "mysql"
    wrapper.write_text(f"""#!/usr/bin/env python3
import subprocess, sys

COMPOSE = {str(HERE / "docker-compose.yml")!r}
SRC_PORT = {str(SRC_PORT)!r}
DST_PORT = {str(DST_PORT)!r}
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

service = "mysql-dst" if port == DST_PORT else "mysql-src"
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
            sql("src", "SELECT 1", quiet=True)
            sql("dst", "SELECT 1", quiet=True)
            return True
        except SystemExit:
            time.sleep(2)
            print(".. waiting for dbs")
    assert False, "dbs not up"

def make_server_cfg():
    cfg = WORK / "config-server.json"
    cfg.write_text(json.dumps({
        "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": DST_PORT},
        "listen": "127.0.0.1",
        "port": MYSQL_SYNC_SERVER_PORT,
    }, indent=2))
    return cfg

def make_client_cfg(fingerprint):
    cfg = WORK / "config-client.json"
    cfg.write_text(json.dumps({
        "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": SRC_PORT},
        "server": f"127.0.0.1:{MYSQL_SYNC_SERVER_PORT}",
        "server_fingerprint": fingerprint,
        "allow_csv": True,
        "dump_strategies": ["select-stream"],
        "transfer_batch_rows": 1000,
        "chunked": 3,
        "concurrent_processes": 3,
        "diff_threshold_percent": 10,
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
    print(f"  Modus: {'Docker (2 MySQL-8-Container)' if DOCKER else 'lokal (vorab gestartete Instanzen)'}")
    print(f"  Quelle : 127.0.0.1:{SRC_PORT}\n  Ziel   : 127.0.0.1:{DST_PORT}\n  QUIC   : 127.0.0.1:{MYSQL_SYNC_SERVER_PORT}")


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


class OldContext:
    def __init__(self, server_cfg, client_cfg, server):
        self.server_cfg = server_cfg
        self.client_cfg = client_cfg
        self.server = server
        self.cfg = ["-C", str(client_cfg)]


def test_force_schema(ctx):
    print("== A: --force-schema ==")
    ok = mysql_sync(ctx.cfg + ["--force-schema", "db1,db2"])
    tbls = scalar("dst", "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='db1'")
    ok &= step("Tabellen auf Ziel erstellt", tbls == "1")
    return step("A: force-schema", ok)


def test_initial_sync(ctx):
    print("== B: --sync (differentiell) ==")
    ok = mysql_sync(ctx.cfg + ["--sync", "db1.*,db2.*"])
    for db, table in [("db1", "flights"), ("db2", "payloads")]:
        s = scalar("src", f"SELECT COUNT(*) FROM {table}", db)
        d = scalar("dst", f"SELECT COUNT(*) FROM {table}", db)
        ok &= step(f"Count gleich: {db}.{table}", s == d, f"src={s} dst={d}")
    cs = scalar("src", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,'')))+SUM(CRC32(label)) FROM payloads", "db2")
    cd = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,'')))+SUM(CRC32(label)) FROM payloads", "db2")
    ok &= step("BLOB-Prüfsummen identisch (binärsicher!)", cs == cd and cs != "0", f"{cs} vs {cd}")
    fs = scalar("src", "SELECT SUM(CRC32(COALESCE(notes,'')))+SUM(CRC32(flight_no))+SUM(CRC32(origin)) FROM flights", "db1")
    fd = scalar("dst", "SELECT SUM(CRC32(COALESCE(notes,'')))+SUM(CRC32(flight_no))+SUM(CRC32(origin)) FROM flights", "db1")
    ok &= step("Inhalts-Prüfsummen identisch", fs == fd, f"{fs} vs {fd}")
    return step("B: sync", ok)


def test_check_identical(ctx):
    print("== C: --check (select/dump/raw-binary/raw-tsv) 10 (identisch) ==")
    ok = mysql_sync(ctx.cfg + ["--check", "10", "db1.*,db2.*"], expect_rc=0)
    ok &= mysql_sync(ctx.cfg + ["--check", "10", "--check-dump", "db1.*,db2.*"], expect_rc=0)
    ok &= mysql_sync(ctx.cfg + ["--check", "10", "--hash-raw-binary-row", "db1.*,db2.*"], expect_rc=0)
    ok &= mysql_sync(ctx.cfg + ["--check", "10", "--hash-raw-tsv", "db1.*"], expect_rc=0)
    rc, out = run_bin(ctx.cfg + ["--check", "3", "--hash-raw-tsv", "db2.payloads"])
    ok &= step("raw-tsv lehnt binäre Spalten ab",
               rc != 0 and "keine binären Spalten" in out, f"rc={rc}")
    return step("C: check (select/dump/raw) → Exit 0", ok)


def test_drift_check(ctx):
    print("== D: Drift erzeugen + --check (select/dump/raw) 5 ==")
    sql("dst", (HERE / "deploy/drift.sql").read_text())

    def capture_check(extra=None, selector="db1.*,db2.*"):
        return run_bin(ctx.cfg + ["--check", "5", *([extra] if extra else []), selector])

    ok = True
    for name, extra in (("--check", None), ("--check-dump", "--check-dump"),
                        ("--hash-raw-binary-row", "--hash-raw-binary-row")):
        rc, joined = capture_check(extra)
        ok &= step(f"{name}: Exit 1", rc == 1, f"rc={rc}")
        ok &= step(f"{name}: 49 Diffs in flights erkannt", "49" in joined)
        ok &= step(f"{name}: 19 Diffs in payloads erkannt", "19" in joined)
        ok &= step(f"{name}: Beispiel-IDs angegeben", "Beispiele:" in joined)
    rc, joined = capture_check("--hash-raw-tsv", "db1.*")
    ok &= step("--hash-raw-tsv: Exit 1", rc == 1, f"rc={rc}")
    ok &= step("--hash-raw-tsv: 49 Diffs in flights erkannt", "49" in joined)
    ok &= step("--hash-raw-tsv: Beispiel-IDs angegeben", "Beispiele:" in joined)
    return step("D: check mit Drift (select/dump/raw)", ok)


def test_drift_repair(ctx):
    print("== E: --sync (nur Differenzen, raw-tsv + raw-binary) ==")
    ok = mysql_sync(ctx.cfg + ["--sync", "db1.*", "--hash-raw-tsv"])
    e1 = scalar("src", "SELECT SUM(CRC32(CONCAT(flight_no,'|',origin,'|',COALESCE(notes,'')))) FROM flights", "db1")
    e2 = scalar("dst", "SELECT SUM(CRC32(CONCAT(flight_no,'|',origin,'|',COALESCE(notes,'')))) FROM flights", "db1")
    ok &= step("raw-tsv: Flights-Prüfsummen nach Drift-Fix identisch", e1 == e2)
    ok &= mysql_sync(ctx.cfg + ["--sync", "db1.*,db2.*", "--hash-raw-binary-row"])
    e1 = scalar("src", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db2")
    e2 = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db2")
    ok &= step("raw-binary: BLOB-Prüfsummen nach Drift-Fix identisch", e1 == e2)
    rc, _ = run_bin(ctx.cfg + ["--check", "10", "--hash-raw-binary-row",
                              "db1.*,db2.*"])
    ok &= step("Erneuter raw-binary --check → Exit 0", rc == 0)
    rc, _ = run_bin(ctx.cfg + ["--check", "10", "--hash-raw-tsv", "db1.*"])
    ok &= step("Erneuter raw-tsv --check → Exit 0", rc == 0)
    return step("E: drift behoben (raw)", ok)


def test_force_resync(ctx):
    print("== F: --force --sync (Recreate + Vollkopie) ==")
    ok = mysql_sync(ctx.cfg + ["--force", "--sync", "db2.payloads"])
    f1 = scalar("src", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db2")
    f2 = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db2")
    ok &= step("Nach Force-Recreate identisch (Binary!)", f1 == f2)
    return step("F: force+sync", ok)


def test_chunking(ctx):
    print("== K: Chunking (Range + automatischer Mod-Fallback) ==")
    range_tables = "db_range.char_keys,binary_keys,date_keys,composite_keys"
    range_tsv_tables = "db_range.char_keys,date_keys,composite_keys"
    ok = mysql_sync(ctx.cfg + ["--force-schema", range_tables])
    ok &= step("Chunking-Tabellen auf Ziel erstellt",
               scalar("dst", "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='db_range'") == "4")
    ok &= mysql_sync(ctx.cfg + ["--sync", range_tables])
    checks = (
        ("CHAR-PK", "SELECT SUM(CRC32(CONCAT(code,'|',label,'|',n))) FROM char_keys"),
        ("BINARY-PK", "SELECT SUM(CRC32(CONCAT(HEX(id),'|',label,'|',n))) FROM binary_keys"),
        ("DATE-PK (Mod-Fallback)", "SELECT SUM(CRC32(CONCAT(d,'|',COALESCE(v,-1)))) FROM date_keys"),
        ("Composite-PK (Mod-Fallback)", "SELECT SUM(CRC32(CONCAT(a,'|',b,'|',COALESCE(v,'')))) FROM composite_keys"),
    )
    for label, query in checks:
        a, b = scalar("src", query, "db_range"), scalar("dst", query, "db_range")
        ok &= step(f"{label} Inhalt identisch", a == b and a != "0", f"{a} vs {b}")

    raw_checks = (
        ("--hash-select", None, range_tables),
        ("--hash-dump", "--hash-dump", range_tables),
        ("--hash-raw-binary-row", "--hash-raw-binary-row", range_tables),
        ("--hash-raw-tsv", "--hash-raw-tsv", range_tsv_tables),
    )
    for name, extra, selector in raw_checks:
        ok &= mysql_sync(ctx.cfg + ["--check", "5", *([extra] if extra else []), selector],
                         expect_rc=0, label=name)

    rc, out = run_bin(ctx.cfg + ["--check", "3", "--hash-raw-tsv", "db_range.binary_keys"])
    ok &= step("Chunking raw-tsv lehnt BINARY-Spalten ab",
               rc != 0 and "keine binären Spalten" in out, f"rc={rc}")

    sql("dst", (HERE / "deploy/drift-range.sql").read_text())
    sql("dst", (HERE / "deploy/drift-fallback.sql").read_text())

    def capture_range_check(extra=None, selector=range_tables):
        return run_bin(ctx.cfg + ["--check", "5", *([extra] if extra else []), selector])

    for name, extra, selector in raw_checks:
        rc, joined = capture_range_check(extra, selector)
        ok &= step(f"Chunking {name}: Exit 1", rc == 1, f"rc={rc}")
        expected_six = 2 if "binary_keys" in selector else 1
        ok &= step(f"Chunking {name}: Range je 6 Diffs",
                   joined.count("6 Zeilen differieren") >= expected_six)
        ok &= step(f"Chunking {name}: Fallback je 3 Diffs",
                   joined.count("3 Zeilen differieren") >= 2)

    ok &= mysql_sync(ctx.cfg + ["--sync", range_tsv_tables, "--hash-raw-tsv"])
    ok &= mysql_sync(ctx.cfg + ["--sync", range_tables, "--hash-raw-binary-row"])
    for name, extra, selector in raw_checks:
        ok &= mysql_sync(ctx.cfg + ["--check", "10", *([extra] if extra else []), selector],
                         expect_rc=0, label=f"Chunking sync {name}")

    for name, extra in (("select", None), ("dump", "--hash-dump"),
                        ("raw-binary", "--hash-raw-binary-row"), ("raw-tsv", "--hash-raw-tsv")):
        rc, out = run_bin(ctx.cfg + ["--check", "3", *([extra] if extra else []), "db_range.no_pk"])
        ok &= step(f"Sync ohne Primary Key wird abgelehnt ({name})",
                   rc != 0 and "keinen Primary Key" in out, f"rc={rc}")
    return step("K: Chunking Range/Mod-Fallback", ok)


def test_raw_profile_gate(ctx):
    print("== L: raw profile gate ==")
    orig_mode = scalar("dst", "SELECT @@GLOBAL.sql_mode")
    sql("dst", "SET GLOBAL sql_mode='ANSI_QUOTES'")
    ctx.server, fp_l, _ = restart_server(ctx.server, ctx.server_cfg, "config-server.json",
                                         "server-profile-mismatch.log")
    update_json(ctx.client_cfg, server_fingerprint=fp_l)
    rc, out = run_bin(ctx.cfg + ["--check", "3", "--hash-raw-binary-row", "db1.*"])
    ok = step("raw-binary lehnt abweichendes MySQL-Profil ab",
              rc != 0 and "MySQL-Profil weicht ab" in out, f"rc={rc}")

    escaped_mode = orig_mode.replace("'", "''")
    sql("dst", f"SET GLOBAL sql_mode='{escaped_mode}'")
    ctx.server, fp_l, _ = restart_server(ctx.server, ctx.server_cfg, "config-server.json",
                                         "server-profile-restored.log")
    update_json(ctx.client_cfg, server_fingerprint=fp_l)
    ok &= mysql_sync(ctx.cfg + ["--check", "5", "--hash-raw-binary-row", "db1.*"], expect_rc=0)
    ok &= mysql_sync(ctx.cfg + ["--check", "5", "--hash-raw-tsv", "db1.*"], expect_rc=0)
    return step("L: raw profile gate", ok)


def test_dump_mode(ctx):
    print("== G: SQL-Dump-Modus (allow_csv=false) ==")
    dump_cfg = json_variant(ctx.client_cfg, "config-client-dump.json", allow_csv=False)
    ok = mysql_sync(["-C", str(dump_cfg), "--sync", "db2.payloads"])
    g1 = scalar("src", "SELECT SUM(CRC32(COALESCE(data,''))) FROM payloads", "db2")
    g2 = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,''))) FROM payloads", "db2")
    ok &= step("Dump-Modus binärsicher", g1 == g2)
    return step("G: Dump-Modus", ok)


def test_hash_algo(ctx):
    print("== H: hash_algo ==")
    bad_cfg = json_variant(ctx.client_cfg, "config-client-md5.json", hash_algo="md5")
    rc, out = run_bin(["-C", str(bad_cfg), "--check", "3", "db1.*"])
    ok = step("Mismatch (md5 vs xxh3) wird abgelehnt",
              rc != 0 and "Hash-Algorithmus weicht ab" in out, f"rc={rc}")

    ctx.server, fp_b, _ = restart_server(ctx.server, ctx.server_cfg,
                                         "config-server-blake3.json", "server-blake3.log",
                                         hash_algo="blake3")
    blake_cfg = json_variant(ctx.client_cfg, "config-client-blake3.json",
                             hash_algo="blake3", server_fingerprint=fp_b)
    ok &= step("Server mit blake3 gestartet", bool(fp_b))
    ok &= mysql_sync(["-C", str(blake_cfg), "--check", "5", "db1.*,db2.*"], expect_rc=0)
    return step("H: hash_algo", ok)


def test_zstd(ctx):
    print("== I: zstd compression ==")
    ctx.server, fp_z, _ = restart_server(ctx.server, ctx.server_cfg,
                                         "config-server-zstd.json", "server-zstd.log",
                                         compression="zstd", compression_level=3)
    client_cfg_z = json_variant(ctx.client_cfg, "config-client-zstd.json",
                                compression="zstd", compression_level=3,
                                server_fingerprint=fp_z)
    ok = step("Server mit zstd gestartet", bool(fp_z))
    ok &= mysql_sync(["-C", str(client_cfg_z), "--check", "5", "db1.*,db2.*"], expect_rc=0)
    return step("I: zstd compression", ok)


def test_plaintext_tcp(ctx):
    print("== J: plaintext TCP ==")
    ctx.server, _, _ = restart_server(ctx.server, ctx.server_cfg,
                                      "config-server-tcp.json", "server-tcp.log",
                                      transport="tcp", allow_plaintext=True)
    wait_tcp(MYSQL_SYNC_SERVER_PORT)
    client_cfg_t = json_variant(ctx.client_cfg, "config-client-tcp.json",
                                transport="tcp", allow_plaintext=True)
    ok = mysql_sync(["-C", str(client_cfg_t), "--check", "5", "db1.*,db2.*"], expect_rc=0)
    step("J: plaintext TCP", ok)

    bad_tcp_cfg = json_variant(ctx.client_cfg, "config-client-tcp-bad.json", transport="tcp")
    rc, out = run_bin(["-C", str(bad_tcp_cfg), "--check", "3", "db1.*"])
    return step("tcp ohne allow_plaintext wird abgelehnt",
                rc != 0 and "allow_plaintext" in out)


async def down():
    await compose("down", "--remove-orphans")

async def clean():
    await compose("down", "--remove-orphans", "--volumes")

async def old():
    ensure_binary()
    prepare_workdir()
    await ensure_dbs_up()
    print_env()
    before = len(results)

    print("== Seed-Daten ==")
    sql("src", "DROP DATABASE IF EXISTS db1")
    sql("src", "DROP DATABASE IF EXISTS db2")
    sql("src", (HERE / "deploy/seed.sql").read_text())
    n_flights = scalar("src", "SELECT COUNT(*) FROM db1.flights")
    n_payloads = scalar("src", "SELECT COUNT(*) FROM db2.payloads")
    step("Seed geladen", n_flights == "5000" and n_payloads == "800",
         f"flights={n_flights} payloads={n_payloads}")
    n_nullbytes = scalar("src", "SELECT COUNT(*) FROM payloads WHERE HEX(data) = '000000'", "db2")
    n_badutf = scalar("src", "SELECT COUNT(*) FROM payloads WHERE HEX(data) = 'FFFE80C0AF'", "db2")
    step("Binäre Giftdaten vorhanden (\\0\\0\\0, invalid UTF-8)",
         int(n_nullbytes or 0) > 0 and int(n_badutf or 0) > 0,
         f"nullbytes={n_nullbytes} badutf={n_badutf}")

    print("== mysql-sync Server ==")
    server_cfg = make_server_cfg()
    server, fingerprint = start_server(server_cfg, "server.log")
    if not step("Server gestartet + Fingerprint", bool(fingerprint),
                fingerprint or "kein Fingerprint"):
        stop_server(server)
        raise SystemExit("Server-Fingerprint fehlt")

    ctx = OldContext(server_cfg, make_client_cfg(fingerprint), server)
    try:
        test_force_schema(ctx)
        test_initial_sync(ctx)
        test_check_identical(ctx)
        test_drift_check(ctx)
        test_drift_repair(ctx)
        test_force_resync(ctx)
        test_chunking(ctx)
        test_raw_profile_gate(ctx)
        test_dump_mode(ctx)
        test_hash_algo(ctx)
        test_zstd(ctx)
        test_plaintext_tcp(ctx)
    finally:
        stop_server(ctx.server)

    failed = [r for r in results[before:] if not r[1]]
    return not failed


async def matrix():
    ensure_binary()
    prepare_workdir()
    await ensure_dbs_up()
    print_env()

    server = None
    try:
        sql("src", f"CREATE DATABASE IF NOT EXISTS {MATRIX_DB} CHARACTER SET utf8mb4", quiet=True)
        sql("dst", f"CREATE DATABASE IF NOT EXISTS {MATRIX_DB} CHARACTER SET utf8mb4", quiet=True)

        server_cfg = make_server_cfg()
        server, fingerprint = start_server(server_cfg, "server-matrix.log")
        if not step("Matrix: Server gestartet + Fingerprint", bool(fingerprint),
                    fingerprint or "kein Fingerprint"):
            return False

        client_cfg = make_client_cfg(fingerprint)
        cfg = ["-C", str(client_cfg)]
        cases = matrix_cases()
        started = time.perf_counter()
        rows = asyncio.run(_matrix_async(cfg, cases))
        wall_ms = (time.perf_counter() - started) * 1000.0
        _report_matrix(rows, wall_ms)
        bad = [r for r in rows if r["status"] not in ("ok", "reject", "skip")]
        skipped = [r for r in rows if r["status"] == "skip"]
        return step("Matrix: alle Fälle", not bad,
                    f"{len(rows)-len(bad)-len(skipped)} ok, {len(skipped)} skip, {len(bad)} Fehler")
    finally:
        stop_server(server)

async def compose(*args):
    await run_show_out_tail_on_error(["docker", "compose", "-f", HERE / "docker-compose.yml", *args])

async def up():
    prepare_workdir()
    await docker_db_up()

async def write_docker_compose():
    def kv(name, version, port):
        y = f"""
        {name}:
            image: mysql:{version}
            container_name: mysqlsync-src
            environment:
              MYSQL_ROOT_PASSWORD: syncroot
              MYSQL_DATABASE: db1
            command: --character-set-server=utf8mb4 --collation-server=utf8mb4_0900_ai_ci --local_infile=1
            ports:
              - "${{MSYNC_SRC_PORT:-{port}}}:3306"
            volumes:
              - ./deploy/seed.sql:/docker-entrypoint-initdb.d/10-seed.sql:ro
            healthcheck:
              test: ["CMD", "mysqladmin", "ping", "-h", "127.0.0.1", "-uroot", "-psyncroot"]
              interval: 3s
              timeout: 3s
              retries: 30
        """
        return yaml.parse(y)

    dcy = {}
    for m in DOCKER_MYSQL_INSTANCES:
        dcy = {**docker, **kv(m["name"], m["version"], m['port'])}
    Path("docker-compose.yml").write_text(yaml.dumps(dcy))

ACTIONS = {
    "write_docker_compose": write_docker_compose,
    "up": up,
    "old": old,
    "matrix": matrix,
    "down": down,
    "clean": clean,
}

def cpu_type():
    """
    cleanup?
    """
    if sys.platform == "darwin":
        try:
            return subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                text=True, stderr=subprocess.DEVNULL,
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine() or "unknown"


async def main():
    global CONCURRENCY
    args = sys.argv[1:]

    # --concurrency[N]
    c = "--concurrency"
    if args[0].startswith(c):
        CONCURRENCY = int(args[0][len(c):])
        args = sys.argv[1:]
    print("\n\n")
    print(f"CONCURRENCY {CONCURRENCY}")

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = f"===== cpu={cpu_type()} | concurrency={CONCURRENCY or 'MISSING'} | "
    with with_logging(header = header):
        print(f"CONCURRENCY={CONCURRENCY or 'MISSING'} matrix_jobs={CONCURRENCY}")

        # python test.py [--concurrencyN] up old matrix down [clean]
        actions = args[1:] or ["up", "old", "matrix", "down"]
        for action in actions:
            assert action in ACTIONS
            await ACTIONS[action]()

if __name__ == "__main__":
    asyncio.run(main())
