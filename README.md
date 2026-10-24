# DSH-shellout

**Out-of-process shell execution for [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness).**
Agent-issued commands run in a supervised runner process with its own event loop, so a flood, a
leak, or a defect in the execution path can no longer stall sessions, streaming, or the web UI.

```
host process                          runner process
  agent / sessions / UI                 shell executor (the only implementation)
  ctx.jobs: records, ring, cursors      managed process groups + liveness observation
  shellout client  ─── unix socket ──▶  shellout server
```

## Why

A harness owns every process it spawns: it captures output, observes exits, and decides when a
process **group** is empty — a command that leaves a descendant behind keeps the group alive. That
work shares one event loop with everything users see. When it goes wrong, the whole product goes
wrong: a liveness check that rescanned the process table on every heartbeat pinned a core for
hours and turned sub-second history requests into tens of seconds.

`DSH-shellout` moves that work into a separate process. The host keeps job records, output rings,
and cursors — and talks to the runner over a small request/response protocol. Nothing about how the
engine calls a shell changes.

## What it is not

Interactive terminals (PTY), MCP servers, language servers, subagent CLIs, and package-manager runs
stay in the host: they do not poll process liveness, and moving them is a separate decision. See
[the spec](docs/spec.md) for the exact boundary.

## Status

Design complete, implementation in progress. The contract lives in [docs/spec.md](docs/spec.md).

## Layout

| Package | Role |
|---|---|
| `packages/protocol` | Wire types, framing, version negotiation (no harness dependency) |
| `packages/host` | Host plugin: implements the harness shell seam over the socket |
| `packages/runner` | Runner plugin: serves the protocol, owns process groups |

## Development

```sh
pnpm install
pnpm test          # unit + contract suites
pnpm typecheck
```

## License

Apache-2.0.
