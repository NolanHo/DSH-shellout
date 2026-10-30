#!/usr/bin/env bash
# Stand-in for an ssh hop whose "remote" is this machine.
#
# `--mode attach` runs `<ssh> <host> -- <remote command>`; this script ignores
# the host and executes the command locally, so the bootstrap sequence (probe,
# push, verify, exec) is exercised end to end without an sshd. Like ssh, it
# joins the remaining arguments with spaces and lets the login shell parse the
# result — a transport that exec'd them as separate argv entries would hide
# exactly the quoting bugs this test exists to catch.
set -euo pipefail
shift # host
[ "${1:-}" = "--" ] && shift
exec sh -c "$*"
