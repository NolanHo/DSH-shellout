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

Implementation complete for the shell path; the contract lives in [docs/spec.md](docs/spec.md).
An isolated end-to-end check (`pnpm test:e2e`) boots a runner and a host in a temporary
`DSH_HOME` and asserts the properties below.

## Enable it

1. **Runner profile** — create a profile that mounts a local shell executor plus the runner
   service: [examples/runner-profile.cordis.patch.yml](examples/runner-profile.cordis.patch.yml).
   Start it with `dsh --profile <runner-profile>`; the process stays up on its socket.
2. **Supervised service** — run that profile under your process manager so a runner restart is
   automatic: [examples/dsh-runner.supervisor.conf](examples/dsh-runner.supervisor.conf).
3. **Host profile** — disable the local executor row and mount the client:
   [examples/host-profile.cordis.patch.yml](examples/host-profile.cordis.patch.yml).
4. **Restart the host once.** Every command the agent runs now executes in the runner.

## Verify

```sh
pnpm test         # protocol, registry, server, client, mirror, seam behaviour
pnpm typecheck
pnpm -r build
pnpm test:e2e     # isolated harness instance: runner + host, seven checks
```

The end-to-end script creates its own `DSH_HOME`, socket, and profiles under the system temp
directory; it never touches a running harness. It asserts:

| Check | What it proves |
|---|---|
| basic | stdout, stderr and exit code match a local run |
| lag | a core-burning command in the runner leaves the host event loop idle (max lag in ms) |
| background | a command that leaves a descendant still reports its own exit |
| isolation | the host process has no child in the execution's group |
| lease | after the host dies, the runner terminates the group when the boot lease expires |
| runner crash | a live execution settles as `killed` with a `shellout: runner lost` note |

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
