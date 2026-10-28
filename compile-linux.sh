#!/usr/bin/env bash
# ============================================================================
# compile-linux.sh — Cross-Compile von mysql-sync für Linux x86_64 via Docker
#
# Erzeugt ein STATISCH gelinktes musl-Binary (kein glibc/openssl auf dem
# Zielsystem nötig — läuft auf jedem Linux-Kernel mit x86_64).
#
# Benötigt wird nur Docker (oder Podman) — keine lokale Rust-/musl-Toolchain:
#
#   ./compile-linux.sh                 # statisches musl-Binary  -> bin/mysql-sync-linux-x86_64
#   ./compile-linux.sh --glibc         # zusätzlich glibc-Binary -> bin/mysql-sync-linux-x86_64-glibc
#   ./compile-linux.sh --no-cache      # Docker-Layer-Cache der Cross-Image-Builds ignorieren
#   ./compile-linux.sh --image rust:1  # andere Rust-Basisversion
#
# Caching: Cargo-Registry und Build-Artefakte liegen in Docker-Named-Volumes
# (mysql-sync-cargo-home / mysql-sync-target-*), wiederholte Builds sind daher
# inkrementell schnell. Rücksetzen: ./compile-linux.sh --clean
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"

BASE_IMAGE="${RUST_IMAGE:-rust:1-slim-bookworm}"
CROSS_IMAGE="mysql-sync-cross:latest"
MUSL_TARGET="x86_64-unknown-linux-musl"
GNU_TARGET="x86_64-unknown-linux-gnu"
OUT="bin"
BIN="mysql-sync"
DOCKER="${DOCKER:-docker}"

usage() {
    sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
}

BUILD_GLIBC=0
NO_CACHE=0
CLEAN=0
while [ $# -gt 0 ]; do
    case "$1" in
        --glibc)     BUILD_GLIBC=1 ;;
        --no-cache)  NO_CACHE=1 ;;
        --clean)     CLEAN=1 ;;
        --image)     BASE_IMAGE="$2"; shift ;;
        -h|--help)   usage; exit 0 ;;
        *) echo "Unbekannte Option: $1 (—help zeigt alle)"; exit 1 ;;
    esac
    shift
done

if ! command -v "$DOCKER" >/dev/null 2>&1; then
    echo "Fehler: '$DOCKER' nicht gefunden. Docker installieren oder DOCKER=podman setzen." >&2
    exit 1
fi

RUN_IMAGE_REF="$CROSS_IMAGE"

# ----------------------------------------------------------------------------
# Schritt 0: Aufräumen?
# ----------------------------------------------------------------------------
if [ "$CLEAN" -eq 1 ]; then
    echo "==> Entferne Build-Caches (Named Volumes)"
    $DOCKER volume rm -f mysql-sync-cargo-home \
                        "mysql-sync-target-$MUSL_TARGET" \
                        "mysql-sync-target-$GNU_TARGET" >/dev/null 2>&1 || true
    echo "==> Fertig."
    exit 0
fi

mkdir -p "$OUT"

# ----------------------------------------------------------------------------
# Schritt 1: Cross-Image bauen (Rust + musl-Tools + Target, im Layer-Cache)
# ----------------------------------------------------------------------------
echo "==> [1/3] Baue Cross-Image '$CROSS_IMAGE' (Basis: $BASE_IMAGE)"
BUILD_ARGS=(-f Dockerfile.cross --build-arg "BASE=$BASE_IMAGE" -t "$CROSS_IMAGE")
[ "$NO_CACHE" -eq 1 ] && BUILD_ARGS+=(--no-cache)
$DOCKER build "${BUILD_ARGS[@]}" .

# ----------------------------------------------------------------------------
# Schritt 2: Statisches musl-Binary kompilieren
# ----------------------------------------------------------------------------
echo "==> [2/3] Kompiliere statisches Binary ($MUSL_TARGET)"
$DOCKER run --rm \
    -v "$PWD":/work -w /work \
    -v mysql-sync-cargo-home:/usr/local/cargo \
    -v "mysql-sync-target-$MUSL_TARGET":/work/target \
    -e "CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc" \
    -e "CC_x86_64_unknown_linux_musl=musl-gcc" \
    -e "AR_x86_64_unknown_linux_musl=ar" \
    "$RUN_IMAGE_REF" \
    sh -eu -c '
        rustup target add '"$MUSL_TARGET"' >/dev/null 2>&1 || true
        cargo build --release --target '"$MUSL_TARGET"'
        cp target/'"$MUSL_TARGET"'/release/'"$BIN"' bin/'"$BIN"'-linux-x86_64
        echo "--- Binary-Info ---"
        file bin/'"$BIN"'-linux-x86_64
        sha256sum bin/'"$BIN"'-linux-x86_64
    '

# ----------------------------------------------------------------------------
# Schritt 3 (optional): glibc-Binary
# ----------------------------------------------------------------------------
if [ "$BUILD_GLIBC" -eq 1 ]; then
    echo "==> [3/3] Kompiliere glibc-Binary ($GNU_TARGET)"
    $DOCKER run --rm \
        -v "$PWD":/work -w /work \
        -v mysql-sync-cargo-home:/usr/local/cargo \
        -v "mysql-sync-target-$GNU_TARGET":/work/target \
        "$RUN_IMAGE_REF" \
        sh -eu -c '
            cargo build --release --target '"$GNU_TARGET"'
            cp target/'"$GNU_TARGET"'/release/'"$BIN"' bin/'"$BIN"'-linux-x86_64-glibc
            file bin/'"$BIN"'-linux-x86_64-glibc
            sha256sum bin/'"$BIN"'-linux-x86_64-glibc
        '
else
    echo "==> [3/3] glibc-Build übersprungen (--glibc zum Aktivieren)"
fi

echo
echo "==> Fertig. Artefakte:"
ls -la "$OUT"
