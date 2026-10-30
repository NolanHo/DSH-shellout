#!/usr/bin/env bash
# Build the runner as a static musl binary.
#
# The remote execution plane pushes this one artifact to every host it serves,
# so it must not depend on the host's libc: a glibc build fails on Alpine and on
# any distro older than the machine that produced it. Zig supplies the musl C
# toolchain (SQLite is vendored C), so no system package is required.
#
# Usage: scripts/build-static.sh [--target x86_64-unknown-linux-musl]
set -euo pipefail

target="${TARGET:-x86_64-unknown-linux-musl}"
if [ "${1:-}" = "--target" ]; then target="${2:?target}"; fi
triple_arch="${target%%-*}"
zig_triple="${triple_arch}-linux-musl"

here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/.." && pwd)"

if ! command -v zig >/dev/null 2>&1; then
  echo "zig is required as the musl C toolchain" >&2
  exit 1
fi

# rustc links its own bundled musl CRT, which collides with the one zig
# provides; let zig own the whole link instead.
# cc-rs wants the lowercase triple with dashes as underscores; cargo wants it
# uppercased. Mixing these up silently falls back to the host compiler.
cc_var="CC_$(printf '%s' "$target" | tr '-' '_')"
upper="$(printf '%s' "$target" | tr '[:lower:]-' '[:upper:]_')"
env \
  "${cc_var}=$here/zig-musl-cc.sh" \
  "CARGO_TARGET_${upper}_LINKER=$here/zig-musl-ld.sh" \
  RUSTFLAGS="-C link-self-contained=no" \
  cargo build --release --target "$target" --manifest-path "$repo/runner-rs/Cargo.toml"

artifact="$repo/runner-rs/target/$target/release/dsh-shellout-runner"
printf 'built %s\n' "$artifact"
file "$artifact"
