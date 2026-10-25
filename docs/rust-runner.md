# Rust runner

The execution plane, written in Rust. It speaks the same wire protocol as the Node runner
(`protocol 2.0`), so the host plugin does not know or care which one is on the other end of the
socket.

## What it does

The host resolves a command into a complete spawn spec — `argv`, the final child environment,
`cwd`, deadline policy, capture budgets — and sends it. The runner owns everything that is
operating-system work:

| Concern | Implementation |
|---|---|
| Process creation | executor spawns a detached shim; shim starts the command with `setsid` |
| Output capture | shim drains stdout/stderr into append-only files in the execution state directory |
| Capture limits | `spillMaxBytes` caps each file; zero uses each stream's capture budget; retains the prefix |
| Spill | only a stream beyond its in-memory cap is copied into the host's `spillDir`, atomically, and reclaimed by `--spill-retention-ms`/`--spill-max-bytes` |
| Lossy reads | per-stream truncation and invalid UTF-8 are reported, never hidden |
| Deadline | shim enforces `timeoutMs` independently of the executor |
| Termination | shim sends `SIGTERM` to the group, then `SIGKILL` after `graceMs` |
| Settlement | atomic `status.json`, retried history write, and one `settled` event |
| Adoption | a restarted executor re-adopts a live shim and settles it from `status.json` |
| Leases | a host boot's executions are terminated when its lease expires |
| Ownership | one executor per state directory, claimed with `flock` on `<state>/.lock` |
| Retention | a settled execution's directory is deleted once its history row is durable (`--state-retention-ms`, default 15 s); host spills follow `--spill-retention-ms` |
| Spawn admission | the executor claims the spawn (`O_EXCL`); the shim starts only after `go`; a pre-verdict timeout refuses the command, a post-verdict one reports the execution and settles it for real |
| Deployment | the shim runs the image through `/proc/self/exe`, so replacing the binary file does not break later executions |

The executor holds no sessions, model connection, HTTP surface or harness dependency.
The separate `--mode hub` owns the dashboard and opens history read-only.
See [execution-plane.md](execution-plane.md) for paths, flags and restart guarantees.

## Build and run

```sh
cargo build --release --manifest-path runner-rs/Cargo.toml
DSH_HOME="$PWD/.runtime" ./runner-rs/target/release/dsh-shellout-runner \
  --cwd "$PWD" --lease-ttl-ms 60000 --max-concurrent 64
DSH_HOME="$PWD/.runtime" ./runner-rs/target/release/dsh-shellout-runner \
  --mode hub --listen 127.0.0.1:3001
```

Supervisor unit:

```ini
[program:dsh-runner]
command=/opt/dsh-shellout/dsh-shellout-runner --socket /srv/dsh/shellout/control.sock --state /srv/dsh/shellout/state --history /srv/dsh/shellout/history.sqlite --cwd /srv/dsh/app
autostart=true
autorestart=true
startsecs=1
; The supervisor signals the executor process only. Supervised commands live
; in shims the executor started; a group signal would kill them with it.
stopasgroup=false
killasgroup=false
```

[execution-plane.md](execution-plane.md#deployment-prerequisites) owns why the
group must be left alone and what else the deployment has to guarantee.

## Parity with the Node runner

Both implementations speak the host protocol. `scripts/e2e-rust.mjs` runs an
isolated wire-level suite without a harness checkout or a running service:

- stdout, stderr, offset reads and real exit code
- byte caps and durable per-stream flags
- deadline TERM-to-KILL escalation and background descendant cleanup
- executor SIGKILL while a command writes one marker per second; continued
  capture, adoption by the replacement executor, and the real terminal exit code
- executor SIGTERM leaves the command running
- a settlement emitted while no host was connected is replayed on reconnect
- a second executor on the same state directory refuses to start
- a detached descendant holding a pipe cannot stall settlement
- a reaped child with a live shim survives an executor restart
- a spawn failure is answered by the `exec` reply itself
- mismatched supervisor start times become `lost`
- read-only hub observation, summary/detail split, percent-decoded IDs, and
  history-only/missing-history degradation

Fixtures are created and removed inside the worktree. Existing services are not
used. The TypeScript host suite remains `pnpm test`; the Node runner's separate
harness suite remains `scripts/e2e.mjs`.

## Boundaries

- **Linux only.** The session/process-group model is POSIX. Windows keeps the Node runner.
- **No confinement.** The runner executes the argv it is given; sandbox policy is the host's
  concern and is not implemented here. A composition that needs confinement must keep the local
  executor.
- **Single machine.** The socket and the process tree are local by design; there is no remote
  transport and none is planned.
- **Reserved: command inspection.** `SettledEvent.notices` and the `warn` event exist so a future
  runner can report what it observed about a command before executing it. Nothing emits them yet.

## Testing

```sh
cargo test --manifest-path runner-rs/Cargo.toml      # unit tests
node scripts/e2e-rust.mjs                            # isolation suite against the binary
```
