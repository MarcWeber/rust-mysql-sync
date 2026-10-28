#!/usr/bin/env bash
# Baut mysql-sync für Linux x86_64: glibc (dynamisch) + musl (statisch).
# Voraussetzungen: cargo, für musl: musl-gcc (apt install musl-tools) und
#   rustup target add x86_64-unknown-linux-musl
set -euo pipefail
cd "$(dirname "$0")"

export PATH="$HOME/.cargo/bin:$PATH"
OUT="bin"
mkdir -p "$OUT"

echo "==> Build (glibc, x86_64)"
cargo build --release
cp target/release/mysql-sync "$OUT/mysql-sync-linux-x86_64-glibc"

echo "==> Build (musl static, x86_64)"
if rustup target list --installed | grep -q x86_64-unknown-linux-musl; then
    export CC_x86_64_unknown_linux_musl="${CC_x86_64_unknown_linux_musl:-musl-gcc}"
    export AR_x86_64_unknown_linux_musl="${AR_x86_64_unknown_linux_musl:-ar}"
    export CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER="${CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER:-musl-gcc}"
    cargo build --release --target x86_64-unknown-linux-musl
    cp target/x86_64-unknown-linux-musl/release/mysql-sync "$OUT/mysql-sync-linux-x86_64"
else
    echo "   (musl-Target nicht installiert — übersprungen: rustup target add x86_64-unknown-linux-musl)"
fi

echo "==> Tests"
cargo test --quiet || true

echo "==> Fertig:"
ls -la "$OUT"
