#!/usr/bin/env bash
# Zig as the linker for the static musl build (rustc takes one executable path).
#
# rustc passes GNU ld flags straight through for the musl target
# (`--as-needed`, `--eh-frame-hdr`, `-z …`), and zig's clang front end rejects
# them as unknown options. A static binary neither needs nor can honour those
# request, so drop them instead of teaching zig a GNU ld spelling.
set -euo pipefail
kept=()
skip_next=0
for arg in "$@"; do
  if [ "$skip_next" = 1 ]; then skip_next=0; continue; fi
  case "$arg" in
    -Wl,--as-needed|--as-needed|-Wl,--no-as-needed|--no-as-needed) continue ;;
    --eh-frame-hdr*|--build-id*|--hash-style=*|--gc-sections|--no-gc-sections) continue ;;
    --no-dynamic-linker|-Wl,--no-dynamic-linker) continue ;;
    --strip-all) kept+=("-s"); continue ;;
    -z) skip_next=1; continue ;;
  esac
  kept+=("$arg")
done
exec zig cc -target "${ZIG_TARGET:-x86_64-linux-musl}" "${kept[@]}"
