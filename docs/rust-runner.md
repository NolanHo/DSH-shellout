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
| Process creation | `exec` in a new session (`setsid`), so the process group id equals the pid |
| Output capture | one reader thread per stream into a bounded in-memory tail |
| Spill | per-execution, per-stream file capped by `capture.spillMaxBytes`; dropped when exceeded |
| Lossy reads | a read below the retained window reports `lossy` and returns the tail |
| Deadline | `timeoutMs` watchdog; `onExpiry: "kill"` terminates at expiry |
| Termination | `SIGTERM` to the group, then `SIGKILL` after `graceMs` if anything survives |
| Settlement | one `settled` event carrying `status`, `exitCode`, `signal` |
| Leases | a host boot's executions are terminated when its lease expires |

It holds no sessions, no model connection, no web surface and no harness dependency.

## Build and run

```sh
cargo build --release --manifest-path runner-rs/Cargo.toml
./runner-rs/target/release/dsh-shellout-runner \
  --socket /root/.dsh/shellout/control.sock \
  --cwd /root/dsh-web/app \
  --lease-ttl-ms 60000 \
  --max-concurrent 64
```

Supervisor unit:

```ini
[program:dsh-runner]
command=/opt/dsh-shellout/dsh-shellout-runner --socket /root/.dsh/shellout/control.sock --cwd /root/dsh-web/app
autostart=true
autorestart=true
startsecs=1
stopasgroup=true
killasgroup=true
```

## Parity with the Node runner

Both implementations satisfy the same host contract; `scripts/e2e-rust.mjs` runs the isolation
suite against the binary and `scripts/e2e.mjs` runs it against the Node runner. The checks that
must hold for either:

- stdout, stderr and exit code reach the host unchanged
- a core-burning command in the runner leaves the host event loop idle
- a command that leaves a descendant still reports its own exit
- the host process has no child in the execution's group
- after the host dies, lease expiry terminates the group
- a lost runner settles its live executions as `killed` with a provider note

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
