#!/usr/bin/env bash
# Stand-in for an ssh hop whose "remote" is this machine.
#
# `--mode attach` runs `<ssh> <host> -- <remote command>`; this script ignores
# the host and executes the command locally, so the bootstrap sequence (probe,
# push, verify, exec) is exercised end to end without an sshd.
set -euo pipefail
shift # host
[ "${1:-}" = "--" ] && shift
exec "$@"
