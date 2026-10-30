#!/usr/bin/env bash
# Zig as the C compiler for the static musl build.
#
# `cc-rs` appends the rust triple (`--target=x86_64-unknown-linux-musl`), which
# zig rejects; zig names its own triples. Drop the rust triple and state zig's.
set -euo pipefail
kept=()
for arg in "$@"; do
  case "$arg" in
    --target=*) continue ;;
  esac
  kept+=("$arg")
done
exec zig cc -target "${ZIG_TARGET:-x86_64-linux-musl}" "${kept[@]}"
