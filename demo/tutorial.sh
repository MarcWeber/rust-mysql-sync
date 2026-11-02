#!/usr/bin/env bash
# basic demo how to use eg from shell scripting
#
# mysql-sync Tutorial (Shell) — Learn how to use the tool step by step.
#
# Prerequisites:
#   python test.py up          # Start MySQL containers first
#
# Usage:
#   bash tutorial.sh           # Run all steps (from anywhere)
#   # Or comment out steps you don't want to run
#
# The script uses QUIC (TLS 1.3 encrypted) transport.
# Config is passed via stdin (-C -) so no temp files are needed.
#

set -euo pipefail

# Resolve script directory so BIN works from anywhere
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Connection details
SRC_HOST="127.0.0.1"
SRC_PORT=7012
DST_HOST="127.0.0.1"
DST_PORT=7013
SERVER_PORT=7112
ROOT_PW="syncroot"
DB="db_tutorial"
SRC_TABLE="src_manual"
DST_TABLE="dest_manual"
BIN="${CARGO_TARGET_DIR}/release/mysql-sync"
FINGERPRINT=""

# Cleanup handler
cleanup() {
    echo ""
    echo "Cleaning up mysql-sync processes..."
    pkill -f "mysql-sync" 2>/dev/null || true
    exit 0
}
trap cleanup SIGINT SIGTERM

info() {
    echo ""
    echo ">>> $1"
}

step_1_start_server() {
    info "Step 1: Start server on source MySQL (port ${SRC_PORT})"
    info "  The server reads data from the source and sends hashes to the client."

    "$BIN" -C - <<EOF &
{
  "mysql": {
    "host": "${SRC_HOST}",
    "port": ${SRC_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "listen": "127.0.0.1",
  "quic_port": ${SERVER_PORT}
}
EOF
    SERVER_PID=$!
    echo "  Server started (PID ${SERVER_PID})"

    # Wait for fingerprint file
    for i in $(seq 1 50); do
        if [ -f ".fingerprint" ]; then
            FINGERPRINT=$(cat .fingerprint)
            echo "  Fingerprint: ${FINGERPRINT}"
            break
        fi
        sleep 0.2
    done
    if [ -z "${FINGERPRINT}" ]; then
        echo "FATAL: Server fingerprint not found"
        exit 1
    fi
}

step_2_benchmark() {
    info "Step 2: Run --benchmark"
    info "  Measures how fast the tool can hash and compare all chunks."
    info "  Expected: fast, no differences yet (dest is empty)."

    "$BIN" -C - --benchmark "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}" <<EOF
{
  "mysql": {
    "host": "${DST_HOST}",
    "port": ${DST_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "server": "127.0.0.1:${SERVER_PORT}",
  "server_fingerprint": "${FINGERPRINT}",
  "chunked": 2,
  "hash-strategies": ["select-sha2"]
}
EOF
}

step_3_check() {
    info "Step 3: Run --check"
    info "  Compares chunk hashes on both sides."
    info "  Expected: exit code 1 (differences found, dest is empty)."

    if "$BIN" -C - --check 0 "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}" <<EOF
{
  "mysql": {
    "host": "${DST_HOST}",
    "port": ${DST_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "server": "127.0.0.1:${SERVER_PORT}",
  "server_fingerprint": "${FINGERPRINT}",
  "chunked": 2,
  "hash-strategies": ["select-sha2"]
}
EOF
    then
        echo "  UNEXPECTED: no differences found"
    else
        echo "  EXPECTED: differences detected (exit code 1)"
    fi
}

step_4_sync_full() {
    info "Step 4: Run --sync (full sync)"
    info "  Copies all differing chunks from source to dest."
    info "  Expected: all 1000 rows transferred."

    "$BIN" -C - --sync "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}" <<EOF
{
  "mysql": {
    "host": "${DST_HOST}",
    "port": ${DST_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "server": "127.0.0.1:${SERVER_PORT}",
  "server_fingerprint": "${FINGERPRINT}",
  "chunked": 2,
  "hash-strategies": ["select-sha2"]
}
EOF
}

step_5_check_after_sync() {
    info "Step 5: Run --check after sync"
    info "  Verifies all chunks now match."
    info "  Expected: exit code 0 (no differences)."

    "$BIN" -C - --check 0 "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}" <<EOF
{
  "mysql": {
    "host": "${DST_HOST}",
    "port": ${DST_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "server": "127.0.0.1:${SERVER_PORT}",
  "server_fingerprint": "${FINGERPRINT}",
  "chunked": 2,
  "hash-strategies": ["select-sha2"]
}
EOF
    echo "  SUCCESS: no differences, tables are identical"
}

step_6_sync_with_where() {
    info "Step 6: Run --sync with sync_where"
    info "  Deletes rows with id < 100 on source, then syncs with WHERE id >= 100."
    info "  Only rows matching the WHERE clause are checked and synced."

    mysql -h "$SRC_HOST" -P "$SRC_PORT" -u root -p"$ROOT_PW" \
        -e "DELETE FROM ${DB}.${SRC_TABLE} WHERE id < 100"
    echo "  Deleted rows id < 100 on source"

    "$BIN" -C - --sync "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}" <<EOF
{
  "mysql": {
    "host": "${DST_HOST}",
    "port": ${DST_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "server": "127.0.0.1:${SERVER_PORT}",
  "server_fingerprint": "${FINGERPRINT}",
  "chunked": 2,
  "hash-strategies": ["select-sha2"],
  "tables": {
    "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}": {
      "sync_where": "id >= 100"
    }
  }
}
EOF
    echo "  Synced only rows with id >= 100"
}

step_7_final_check() {
    info "Step 7: Final --check"
    info "  No problems expected — all chunks should match."

    "$BIN" -C - --check 0 "${DB}.${SRC_TABLE}:${DB}.${DST_TABLE}" <<EOF
{
  "mysql": {
    "host": "${DST_HOST}",
    "port": ${DST_PORT},
    "user": "root",
    "password": "${ROOT_PW}"
  },
  "server": "127.0.0.1:${SERVER_PORT}",
  "server_fingerprint": "${FINGERPRINT}",
  "chunked": 2,
  "hash-strategies": ["select-sha2"]
}
EOF
    echo "  SUCCESS: tutorial complete, no differences found"
}

# Run all steps in order — comment out any you don't want
step_1_start_server
step_2_benchmark
step_3_check
step_4_sync_full
step_5_check_after_sync
step_6_sync_with_where
step_7_final_check

cleanup
