#!/usr/bin/env python3
"""
mysql-sync End-to-End-Test.

Modi:
  1. Docker (Standard):  startet 2 MySQL-8-Container via docker compose,
     baut/startet den Server-Binary auf dem Host und spielt alle Szenarien durch.
  2. Lokal (MSYNC_LOCAL=1): erwartet 2 laufende MySQL/MariaDB-Instanzen
     (Quelle MSYNC_SRC_PORT=33061, Ziel MSYNC_DST_PORT=33062, User root,
     Passwort MSYNC_ROOT_PW=''), plus mysql-CLI im PATH (MSYNC_MYSQL_CLI).

Szenarien:
  A) --force-schema  : Schema auf Ziel erzwingen
  B) --sync          : differentieller Abgleich, danach Zähler + BLOB-Prüfsummen gleich
  C) --check 10      : Exit 0 wenn identisch
  D) Drift + --check 5  : Exit 1, erwartete Diff-Anzahl (49 plain / 19 binary) gemeldet
  E) --sync          : Drift behoben, wieder identisch
  F) --force --sync  : Tabelle neu erstellen + Vollkopie, wieder identisch
  G) SQL-Dump-Modus  : allow_csv=false, binärsicher
  H) hash_algo       : Mismatch wird abgelehnt, blake3 funktioniert
  I) zstd            : Kompression auf beiden Seiten
  J) TCP             : Klartext-Transport (Opt-in) + Ablehnung ohne allow_plaintext
  K) Chunking        : Range fuer INTEGER/CHAR/BINARY, automatischer Mod-Fallback fuer
                       DATE/Composite, select+dump, exakte Drift-Zahlen, Fehler ohne PK

Exit 0 = alle Szenarien bestanden.
"""

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
WORK = HERE / ".test-run"
BIN = Path(os.environ.get("MSYNC_BIN", HERE / "target/release/mysql-sync"))

DOCKER = shutil.which("docker") is not None and not os.environ.get("MSYNC_LOCAL")
SRC_PORT = int(os.environ.get("MSYNC_SRC_PORT", 3306 if DOCKER else 33061))
DST_PORT = int(os.environ.get("MSYNC_DST_PORT", 3307 if DOCKER else 33062))
ROOT_PW = os.environ.get("MSYNC_ROOT_PW", "syncroot" if DOCKER else "")
QUIC_PORT = int(os.environ.get("MSYNC_QUIC_PORT", 44330))

PASS, FAIL = "\033[32mPASS\033[0m", "\033[31mFAIL\033[0m"
results = []


def step(name, ok, detail=""):
    results.append((name, ok, detail))
    print(f"  [{PASS if ok else FAIL}] {name}" + (f" — {detail}" if detail else ""))
    return ok


def run(cmd, check=True, input=None, capture=True):
    print(f"    $ {' '.join(str(c) for c in cmd)[:160]}")
    r = subprocess.run([str(c) for c in cmd], input=input, text=True,
                       capture_output=capture)
    if check and r.returncode != 0:
        print(r.stdout[-3000:])
        print(r.stderr[-3000:])
        raise SystemExit(f"Befehl fehlgeschlagen: {cmd}")
    return r


def sql(which, query, database=None, check=True):
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
    r = run(cmd + ["-e", query], check=check)
    return r.stdout


def scalar(which, query, database=None):
    out = sql(which, query, database).strip().splitlines()
    return out[-1] if out else None


def mysql_sync(args, expect_rc=0, label=""):
    r = subprocess.run([str(BIN), *args], text=True, capture_output=True, cwd=WORK)
    tail = (r.stdout + r.stderr).strip().splitlines()
    tail_str = "\n    ".join(tail[-6:])
    ok = r.returncode == expect_rc
    print(f"    $ mysql-sync {' '.join(str(a) for a in args)[:120]}  → rc={r.returncode} (erwartet {expect_rc})")
    print(f"    {tail_str}")
    return ok


def main():
    if not BIN.exists():
        raise SystemExit(f"Binary fehlt: {BIN} (erst cargo build --release)")
    WORK.mkdir(exist_ok=True)

    print("== Testumgebung ==")
    if DOCKER:
        print("  Modus: Docker (2 MySQL-8-Container)")
        run(["docker", "compose", "-f", HERE / "docker-compose.yml", "up", "-d", "--wait"],
            check=False)
    else:
        print("  Modus: lokal (vorab gestartete Instanzen)")
    for i in range(60):
        try:
            sql("src", "SELECT 1")
            sql("dst", "SELECT 1")
            break
        except SystemExit:
            time.sleep(2)
    else:
        raise SystemExit("Datenbanken nicht erreichbar")
    print(f"  Quelle : 127.0.0.1:{SRC_PORT}\n  Ziel   : 127.0.0.1:{DST_PORT}\n  QUIC   : 127.0.0.1:{QUIC_PORT}")

    # ---------- Seed (frisch) ----------
    print("== Seed-Daten ==")
    sql("src", "DROP DATABASE IF EXISTS db_skywings")
    sql("src", "DROP DATABASE IF EXISTS db_airwings")
    seed = (HERE / "deploy/seed.sql").read_text()
    sql("src", seed)
    n_flights = scalar("src", "SELECT COUNT(*) FROM db_skywings.flights")
    n_payloads = scalar("src", "SELECT COUNT(*) FROM db_airwings.payloads")
    step("Seed geladen", n_flights == "5000" and n_payloads == "800", f"flights={n_flights} payloads={n_payloads}")
    # Böse Binärdaten müssen wirklich vorhanden sein (3 Nullbytes + ungültiges UTF-8)
    n_nullbytes = scalar("src", "SELECT COUNT(*) FROM payloads WHERE HEX(data) = '000000'", "db_airwings")
    n_badutf = scalar("src", "SELECT COUNT(*) FROM payloads WHERE HEX(data) = 'FFFE80C0AF'", "db_airwings")
    step("Binäre Giftdaten vorhanden (\\0\\0\\0, invalid UTF-8)",
         int(n_nullbytes or 0) > 0 and int(n_badutf or 0) > 0,
         f"nullbytes={n_nullbytes} badutf={n_badutf}")

    # ---------- mysql-sync Server starten ----------
    print("== mysql-sync Server ==")
    server_cfg = WORK / "config-server.json"
    server_cfg.write_text(json.dumps({
        "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": DST_PORT},
        "listen": "127.0.0.1",
        "port": QUIC_PORT,
    }, indent=2))
    log = open(WORK / "server.log", "w")
    server = subprocess.Popen([str(BIN), "-C", str(server_cfg)], stdout=log, stderr=log)
    fp_file = server_cfg.with_suffix(".fingerprint")
    fingerprint = None
    for _ in range(50):
        if fp_file.exists():
            fingerprint = fp_file.read_text().strip()
            break
        time.sleep(0.2)
    try:
        step("Server gestartet + Fingerprint", bool(fingerprint), fingerprint or "kein Fingerprint")
    except SystemExit:
        server.terminate()
        raise

    try:
        client_cfg = WORK / "config-client.json"
        client_cfg.write_text(json.dumps({
            "mysql": {"user": "root", "password": ROOT_PW, "host": "127.0.0.1", "port": SRC_PORT},
            "server": f"127.0.0.1:{QUIC_PORT}",
            "server_fingerprint": fingerprint,
            "allow_csv": True,
            "dump_strategies": ["select-stream"],
            "transfer_batch_rows": 1000,
            "chunked": 3,
            "concurrent_processes": 3,
            "diff_threshold_percent": 10,
        }, indent=2))
        cfg = ["-C", str(client_cfg)]

        # ---------- A: --force-schema ----------
        print("== A: --force-schema ==")
        ok = mysql_sync(cfg + ["--force-schema", "db_skywings,db_airwings"])
        tbls = scalar("dst", "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='db_skywings'")
        ok &= step("Tabellen auf Ziel erstellt", tbls == "1")
        step("A: force-schema", ok)

        # ---------- B: --sync ----------
        print("== B: --sync (differentiell) ==")
        ok = mysql_sync(cfg + ["--sync", "db_skywings.*,db_airwings.*"])
        for db, t in [("db_skywings", "flights"), ("db_airwings", "payloads")]:
            s = scalar("src", f"SELECT COUNT(*) FROM {t}", db)
            d = scalar("dst", f"SELECT COUNT(*) FROM {t}", db)
            ok &= step(f"Count gleich: {db}.{t}", s == d, f"src={s} dst={d}")
        cs = scalar("src", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,'')))+SUM(CRC32(label)) FROM payloads", "db_airwings")
        cd = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,'')))+SUM(CRC32(label)) FROM payloads", "db_airwings")
        ok &= step("BLOB-Prüfsummen identisch (binärsicher!)", cs == cd and cs != "0", f"{cs} vs {cd}")
        fs = scalar("src", "SELECT SUM(CRC32(COALESCE(notes,'')))+SUM(CRC32(flight_no))+SUM(CRC32(origin)) FROM flights", "db_skywings")
        fd = scalar("dst", "SELECT SUM(CRC32(COALESCE(notes,'')))+SUM(CRC32(flight_no))+SUM(CRC32(origin)) FROM flights", "db_skywings")
        ok &= step("Inhalts-Prüfsummen identisch", fs == fd, f"{fs} vs {fd}")
        step("B: sync", ok)

        # ---------- C: --check (Select + Dump) ----------
        print("== C: --check / --check-via-dump 10 (identisch) ==")
        ok = mysql_sync(cfg + ["--check", "10", "db_skywings.*,db_airwings.*"], expect_rc=0)
        ok &= mysql_sync(cfg + ["--check", "10", "--check-via-dump", "db_skywings.*,db_airwings.*"], expect_rc=0)
        step("C: check (select+dump) → Exit 0", ok)

        # ---------- D: Drift + --check (Select + Dump) ----------
        print("== D: Drift erzeugen + --check / --check-via-dump 5 ==")
        sql("dst", (HERE / "deploy/drift.sql").read_text())

        def capture_check(extra=None):
            r = subprocess.run([str(BIN), *cfg, "--check", "5", *([extra] if extra else []),
                                "db_skywings.*,db_airwings.*"],
                               text=True, capture_output=True, cwd=WORK)
            return r.returncode, r.stdout + r.stderr

        ok = True
        for name, extra in (("--check", None), ("--check-via-dump", "--check-via-dump")):
            rc, joined = capture_check(extra)
            ok &= step(f"{name}: Exit 1", rc == 1, f"rc={rc}")
            ok &= step(f"{name}: 49 Diffs in flights erkannt", "49" in joined)
            ok &= step(f"{name}: 19 Diffs in payloads erkannt", "19" in joined)
            ok &= step(f"{name}: Beispiel-IDs angegeben", "Beispiele:" in joined)
        step("D: check mit Drift (select+dump)", ok)

        # ---------- E: --sync behebt Drift ----------
        print("== E: --sync (nur Differenzen) ==")
        ok = mysql_sync(cfg + ["--sync", "db_skywings.*,db_airwings.*"])
        e1 = scalar("src", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db_airwings")
        e2 = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db_airwings")
        ok &= step("BLOB-Prüfsummen nach Drift-Fix identisch", e1 == e2)
        diff = subprocess.run([str(BIN), *cfg, "--check", "10", "db_skywings.*,db_airwings.*"],
                              text=True, capture_output=True, cwd=WORK).returncode
        ok &= step("Erneuter --check → Exit 0", diff == 0)
        step("E: drift behoben", ok)

        # ---------- F: --force --sync ----------
        print("== F: --force --sync (Recreate + Vollkopie) ==")
        ok = mysql_sync(cfg + ["--force", "--sync", "db_airwings.payloads"])
        f1 = scalar("src", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db_airwings")
        f2 = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,'')))+SUM(CRC32(COALESCE(meta,''))) FROM payloads", "db_airwings")
        ok &= step("Nach Force-Recreate identisch (Binary!)", f1 == f2)
        step("F: force+sync", ok)

        # ---------- K: automatisches Range/Mod-Fallback-Chunking ----------
        print("== K: Chunking (Range + automatischer Mod-Fallback) ==")
        range_tables = "db_range.char_keys,binary_keys,date_keys,composite_keys"
        ok = mysql_sync(cfg + ["--force-schema", range_tables])
        ok &= step("Chunking-Tabellen auf Ziel erstellt",
                   scalar("dst", "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='db_range'") == "4")
        ok &= mysql_sync(cfg + ["--sync", range_tables])
        checks = (
            ("CHAR-PK", "SELECT SUM(CRC32(CONCAT(code,'|',label,'|',n))) FROM char_keys"),
            ("BINARY-PK", "SELECT SUM(CRC32(CONCAT(HEX(id),'|',label,'|',n))) FROM binary_keys"),
            ("DATE-PK (Mod-Fallback)", "SELECT SUM(CRC32(CONCAT(d,'|',COALESCE(v,-1)))) FROM date_keys"),
            ("Composite-PK (Mod-Fallback)", "SELECT SUM(CRC32(CONCAT(a,'|',b,'|',COALESCE(v,'')))) FROM composite_keys"),
        )
        for label, q in checks:
            a, b = scalar("src", q, "db_range"), scalar("dst", q, "db_range")
            ok &= step(f"{label} Inhalt identisch", a == b and a != "0", f"{a} vs {b}")

        for name, extra in (("--hash-via-select", None), ("--hash-via-dump", "--hash-via-dump")):
            ok &= mysql_sync(cfg + (["--check", "5", *([extra] if extra else []), range_tables]),
                             expect_rc=0, label=name)

        sql("dst", (HERE / "deploy/drift-range.sql").read_text())
        sql("dst", (HERE / "deploy/drift-fallback.sql").read_text())

        def capture_range_check(extra=None):
            r = subprocess.run([str(BIN), *cfg, "--check", "5", *([extra] if extra else []), range_tables],
                               text=True, capture_output=True, cwd=WORK)
            return r.returncode, r.stdout + r.stderr

        for name, extra in (("select", None), ("dump", "--hash-via-dump")):
            rc, joined = capture_range_check(extra)
            ok &= step(f"Chunking {name}: Exit 1", rc == 1, f"rc={rc}")
            ok &= step(f"Chunking {name}: Range je 6 Diffs",
                       joined.count("6 Zeilen differieren") >= 2)
            ok &= step(f"Chunking {name}: Fallback je 3 Diffs",
                       joined.count("3 Zeilen differieren") >= 2)

        ok &= mysql_sync(cfg + ["--sync", range_tables])
        for name, extra in (("select", None), ("dump", "--hash-via-dump")):
            ok &= mysql_sync(cfg + (["--check", "10", *([extra] if extra else []), range_tables]),
                             expect_rc=0, label=f"Chunking sync {name}")

        r = subprocess.run([str(BIN), *cfg, "--check", "3", "db_range.no_pk"],
                           text=True, capture_output=True, cwd=WORK)
        out = r.stdout + r.stderr
        ok &= step("Sync ohne Primary Key wird abgelehnt",
                   r.returncode != 0 and "keinen Primary Key" in out, f"rc={r.returncode}")
        step("K: Chunking Range/Mod-Fallback", ok)

        # ---------- G: Dump-Modus (allow_csv=false) ----------
        print("== G: SQL-Dump-Modus (allow_csv=false) ==")
        dump_cfg = WORK / "config-client-dump.json"
        c = json.loads(client_cfg.read_text())
        c["allow_csv"] = False
        dump_cfg.write_text(json.dumps(c, indent=2))
        ok = mysql_sync(["-C", str(dump_cfg), "--sync", "db_airwings.payloads"])
        g1 = scalar("src", "SELECT SUM(CRC32(COALESCE(data,''))) FROM payloads", "db_airwings")
        g2 = scalar("dst", "SELECT SUM(CRC32(COALESCE(data,''))) FROM payloads", "db_airwings")
        ok &= step("Dump-Modus binärsicher", g1 == g2)
        step("G: Dump-Modus", ok)

        # ---------- H: hash_algo (Mismatch + blake3) ----------
        print("== H: hash_algo ==")
        bad = json.loads(client_cfg.read_text())
        bad["hash_algo"] = "md5"
        bad_cfg = WORK / "config-client-md5.json"
        bad_cfg.write_text(json.dumps(bad, indent=2))
        r = subprocess.run([str(BIN), "-C", str(bad_cfg), "--check", "3", "db_skywings.*"],
                           text=True, capture_output=True, cwd=WORK)
        out = r.stdout + r.stderr
        ok = step("Mismatch (md5 vs xxh3) wird abgelehnt",
                  r.returncode != 0 and "Hash-Algorithmus weicht ab" in out,
                  f"rc={r.returncode}")

        server.terminate()
        server.wait(timeout=5)
        server_cfg_b = WORK / "config-server-blake3.json"
        sc = json.loads(server_cfg.read_text())
        sc["hash_algo"] = "blake3"
        server_cfg_b.write_text(json.dumps(sc, indent=2))
        log_b = open(WORK / "server-blake3.log", "w")
        server = subprocess.Popen([str(BIN), "-C", str(server_cfg_b)], stdout=log_b, stderr=log_b)
        fp_b = None
        for _ in range(50):
            f = server_cfg_b.with_suffix(".fingerprint")
            if f.exists():
                fp_b = f.read_text().strip()
                break
            time.sleep(0.2)
        cc = json.loads(client_cfg.read_text())
        cc["hash_algo"] = "blake3"
        cc["server_fingerprint"] = fp_b
        blake_cfg = WORK / "config-client-blake3.json"
        blake_cfg.write_text(json.dumps(cc, indent=2))
        ok &= step("Server mit blake3 gestartet", bool(fp_b))
        ok &= mysql_sync(["-C", str(blake_cfg), "--check", "5", "db_skywings.*,db_airwings.*"], expect_rc=0)
        step("H: hash_algo", ok)

        # ---------- I: zstd compression ----------
        print("== I: zstd compression ==")
        server.terminate()
        server.wait(timeout=5)
        server_cfg_z = WORK / "config-server-zstd.json"
        sz = json.loads(server_cfg.read_text())
        sz["compression"] = "zstd"
        sz["compression_level"] = 3
        server_cfg_z.write_text(json.dumps(sz, indent=2))
        log_z = open(WORK / "server-zstd.log", "w")
        server = subprocess.Popen([str(BIN), "-C", str(server_cfg_z)], stdout=log_z, stderr=log_z)
        fp_z = None
        for _ in range(50):
            f = server_cfg_z.with_suffix(".fingerprint")
            if f.exists():
                fp_z = f.read_text().strip()
                break
            time.sleep(0.2)
        cz = json.loads(client_cfg.read_text())
        cz["compression"] = "zstd"
        cz["compression_level"] = 3
        cz["server_fingerprint"] = fp_z
        client_cfg_z = WORK / "config-client-zstd.json"
        client_cfg_z.write_text(json.dumps(cz, indent=2))
        ok = step("Server mit zstd gestartet", bool(fp_z))
        ok &= mysql_sync(["-C", str(client_cfg_z), "--check", "5", "db_skywings.*,db_airwings.*"], expect_rc=0)
        step("I: zstd compression", ok)

        # ---------- J: plaintext TCP (opt-in) ----------
        print("== J: plaintext TCP ==")
        server.terminate()
        server.wait(timeout=5)
        server_cfg_t = WORK / "config-server-tcp.json"
        st = json.loads(server_cfg.read_text())
        st["transport"] = "tcp"
        st["allow_plaintext"] = True
        server_cfg_t.write_text(json.dumps(st, indent=2))
        log_t = open(WORK / "server-tcp.log", "w")
        server = subprocess.Popen([str(BIN), "-C", str(server_cfg_t)], stdout=log_t, stderr=log_t)
        for _ in range(50):
            try:
                with socket.create_connection(("127.0.0.1", QUIC_PORT), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.1)
        ct = json.loads(client_cfg.read_text())
        ct["transport"] = "tcp"
        ct["allow_plaintext"] = True
        client_cfg_t = WORK / "config-client-tcp.json"
        client_cfg_t.write_text(json.dumps(ct, indent=2))
        ok = mysql_sync(["-C", str(client_cfg_t), "--check", "5", "db_skywings.*,db_airwings.*"], expect_rc=0)
        step("J: plaintext TCP", ok)

        ct_bad = json.loads(client_cfg.read_text())
        ct_bad["transport"] = "tcp"
        bad_tcp_cfg = WORK / "config-client-tcp-bad.json"
        bad_tcp_cfg.write_text(json.dumps(ct_bad, indent=2))
        r = subprocess.run([str(BIN), "-C", str(bad_tcp_cfg), "--check", "3", "db_skywings.*"],
                           text=True, capture_output=True, cwd=WORK)
        step("tcp ohne allow_plaintext wird abgelehnt",
             r.returncode != 0 and "allow_plaintext" in (r.stdout + r.stderr))

    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()

    print("\n═══════════════ GESAMTERGEBNIS ═══════════════")
    failed = [r for r in results if not r[1]]
    for name, ok, detail in results:
        print(f"  [{PASS if ok else FAIL}] {name}" + (f" — {detail}" if detail else ""))
    print(f"═══════════════ {len(results)-len(failed)}/{len(results)} bestanden ═══════════════")
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
