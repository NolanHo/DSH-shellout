> Architecture, ownership and guarantees across the hub, executor and shim
> processes live in [execution-plane.md](execution-plane.md).

# DSH-shellout — Specification

Status: **implemented (shell path)** · Version: 0.2 · Date: 2026-10-24

This document is the contract for `DSH-shellout`. It is written to be executed by an agent with
no prior conversation: everything needed to implement, verify, and review the work is either in
this file or reachable from it.

---

## 1. Problem

An agent harness runs agent-issued shell commands inside its own Node process. Every such command
creates a managed process group that the harness must own: it spawns it, captures its output,
observes when it exits, and — crucially — decides when the **group** is empty, because a command
that leaves a descendant behind keeps the group alive after the direct child exits.

That ownership work shares one event loop with everything the product does: sessions, model
streaming, the web server, and every user-facing request. A defect or a heavy workload in the
execution path therefore degrades the whole product. Observed failure class: a liveness check
that walked the process table on every heartbeat pinned one core for hours, and history requests
that normally take a fraction of a second took tens of seconds. The heartbeat itself is fixable
in place; the structural problem is that execution and UI share a fate.

## 2. Goals

1. **Isolation.** The harness process owns no agent-issued OS processes. Spawning, output capture,
   exit observation, process-group liveness, and termination all happen in a separate runner
   process with its own event loop. An execution-side bug, flood, or leak cannot block the host.
2. **Behavioural equivalence.** A command executed through the runner behaves, from the engine's
   point of view, like one executed locally: same output bytes, same exit facts, same timeout and
   cancellation semantics, same spill paths, same error shapes.
3. **One switch.** The host enables the runner by replacing one composition row; disabling it
   restores local execution. There is never a moment where both paths run for the same command.
4. **Small surface.** No edits to harness packages: this project is a plugin (two packages plus a
   runner composition). The only host-side change is composition configuration.

## 3. Non-goals

- **PTY terminals.** Interactive terminal sessions stay in the host. They have a different risk
  profile (bidirectional, latency-sensitive, screen state) and are out of scope.
- **Non-shell process sources.** MCP servers, language servers, ACP/Codex/Claude subagent CLIs,
  the PTC runtime, and package-manager runs stay where they are. They do not perform process-table
  liveness polling; moving them is a separate decision.
- **Cross-host-restart durability.** An execution survives a restart of the process that
  supervises it: the shim owns the command, and a restarted supervisor re-adopts a live shim from
  the state directory (see [execution-plane.md](execution-plane.md)). It does not survive a restart
  of the harness host: a new boot cannot address the previous boot's executions, which are
  terminated at lease expiry.
- **Scheduling, containers, remote hosts.** One runner serves one host on one machine.
- **Windows.** First release targets Linux/macOS POSIX semantics. Windows support is a follow-up.

## 4. Architecture

```
host process                                 runner process
  agent loop, sessions, tools                  shell executor (the only implementation)
  ctx.jobs (records, ring, cursors)            managed process groups + liveness observation
  ctx.shell = shellout client  ── socket ──▶   shellout server
  local mirror of each execution's output      per-execution output retention + spill files
```

Two packages plus a composition:

| Package | Role |
|---|---|
| `dsh-shellout-protocol` | Wire types, framing, version negotiation. No harness dependency. |
| `dsh-shellout` | Host plugin. Implements the harness `ShellExecutor` seam over the wire. |
| `dsh-shellout-runner` | Runner plugin. Serves the wire protocol, delegating to a real shell executor inside the runner process. |

The runner is an ordinary harness profile whose composition mounts a shell executor (the stock
local one), the sandbox policy, and `dsh-shellout-runner`. It runs `dsh --profile shellout-runner`
and stays alive while its socket server is listening.

## 5. Contracts

### 5.1 Host seam fidelity

The host plugin subclasses the harness `ShellExecutor` service and must satisfy the seam's
documented behaviour:

- `sandboxMode` reports the mode the runner's executor reports (learned at handshake), or
  `undefined` when it does not sandbox.
- `resolve(request) → ShellExecSpec` is **synchronous**. It applies this executor's defaults and
  caps from the handshake descriptor, using the formula in §5.2. It never performs I/O.
- `execute(spec) → Promise<ShellExecution>` publishes a live handle. It throws only for a
  preparation failure or a caller cancellation before process publication; a returned handle's
  `done` never rejects.
- `ShellProcess`:
  - `readOutput()` — consuming delta over stdout (with a stderr section), `lossy` flags, and
    `stdoutSpillPath`/`stderrSpillPath` when the runner keeps a complete stream file.
  - `observed.stdout` / `observed.stderr` — non-consuming, offset-based readers over the same
    captured streams.
  - both are **synchronous** and are served from a local mirror (§5.4).
  - `kill()` — idempotent; `false` when the execution already finished.
  - `done` — resolves once, after the terminal facts are available.
  - `status`, `exitCode`, `signal`, `sandbox` — mirror the runner's terminal facts.
- `ShellExecution.result()` — the foreground projection: split streams, exit facts, and the
  `timedOut`/`aborted` classification.

### 5.2 Resolution formula

Given the runner descriptor `{ cwd, timeoutMs, maxTimeoutMs, maxOutputBytes }` and a request:

```
workdir        = request.workdir ?? descriptor.cwd
timeoutMs      = request.timeoutMs ?? descriptor.timeoutMs
timeoutMs      = min(timeoutMs, descriptor.maxTimeoutMs)        // invalid values throw
onExpiry       = request.onExpiry ?? 'kill'
stdoutMaxBytes = request.stdoutMaxBytes ?? descriptor.maxOutputBytes
```

The runner re-applies its own authoritative resolution before spawning; the host's value is a
caller-visible default, and the two must agree for every request that does not carry an override.
A request field that is not a positive finite number must throw the same way the local executor
throws.

### 5.3 Wire protocol

Transport: a Unix domain socket (`$DSH_HOME/shellout/control.sock`, mode `0600`), newline-free
length-prefixed JSON frames: `[u32 big-endian length][UTF-8 JSON]`. No binary frames: every value
crossing the wire is text, so a single JSON encoding is sufficient. Maximum frame size is
negotiated (default 8 MiB); an oversized frame is a protocol error and closes the connection.

Envelope:

```jsonc
{ "id": 17, "kind": "req" | "res" | "err" | "evt", "method": "exec", "payload": { } }
```

- `req` — host → runner; `id` is unique per connection.
- `res` / `err` — runner → host, echoing `id`; `err` carries `{ code, message, detail? }`.
- `evt` — runner → host, unsolicited; `method` names the event.

Methods (all host → runner unless marked event):

| Method | Payload | Reply | Notes |
|---|---|---|---|
| `hello` | `{ protocol, clientBuild, bootId, capabilities }` | `{ protocol, runnerBuild, runnerBootId, descriptor, maxFrameBytes, heartbeatMs, leaseTtlMs, executions }` | First frame on every connection. Version mismatch is fatal. `executions` lists what this boot still owns, with each execution's terminal facts. |
| `resolve` | `{ request }` | `{ spec }` | Optional: used by diagnostics and tests; the host normally resolves locally. |
| `exec` | `{ operationId, executionId, spec }` | `{ executionId }` | Idempotent per `operationId`: a repeated `exec` with the same `(operationId, specDigest)` returns the same execution; a different digest is rejected. |
| `readOutput` | `{ executionId }` | `ShellProcessRead` | Consuming; the runner advances its cursor. |
| `readStream` | `{ executionId, stream, offset, limit }` | `{ text, nextOffset, lossy, spillPath? }` | Non-consuming offset read, bounded by `limit` bytes. |
| `kill` | `{ executionId }` | `{ requested: boolean }` | Idempotent. |
| `state` | `{ executionId }` | `{ state, exitCode, signal, sandbox? }` | Reconciliation after a reconnect. |
| `ping` | `{ bootId }` | `{ ok: true }` | Refreshes the boot lease. |
| `drain` | `{ timeoutMs }` | `{ live: number }` | Stops accepting `exec`; terminates or waits for live executions. |
| `shutdown` | `{}` | `{}` | Clean stop after drain. |
| `settled` (event) | `{ executionId, status, exitCode, signal, termination?, sandbox?, detail? }` | — | Sent once per execution, after its final output is readable. A settlement nobody was subscribed for is recovered from the `hello` reply. |

Error codes: `protocol`, `version`, `unknown-execution`, `busy`, `rejected` (admission refused),
`internal`. An `err` frame for a request the host no longer tracks is logged and ignored.

Ordering: the runner sends `settled` **after** the final output bytes are readable through
`readOutput`/`readStream`, so a host that reads until `settled` never misses a tail.

### 5.4 Local mirror (host)

`readOutput()` and `observed.*` are synchronous, so the host keeps a bounded mirror per execution:

- A poller requests `readStream` at the mirror's high-water offset every `mirrorPollMs`
  (default 150 ms) and appends to a bounded buffer (default 1 MiB, configurable).
- `observed` reads are served from the retained window. An offset below the retained head yields
  `lossy: true` with the spill path — which the seam already permits.
- `spillPath` names the complete stream in the host's configured `spillDir`, which outlives the
  runner's ephemeral state directory; a capture that is gone is reported as `lossy`, never as a
  clean empty read. A stream that did not exceed its in-memory cap has no file, and the runner
  reclaims the files it owns after `--spill-retention-ms` or its `--spill-max-bytes` budget.
- `readOutput()` consumes from a cursor over the same bytes, appending a stderr section exactly as
  the local executor does.
- Before `done` resolves, the poller performs one final drain so no tail is lost; `settled` is only
  handled after that drain.

### 5.5 Lifecycle

- **Host boot** → connect → `hello{bootId}` → the runner starts a boot lease (default TTL 60 s,
  refreshed by `ping` every 5 s).
- **Normal host shutdown** → `drain` (SIGTERM to live groups, wait `disposeGraceMs`), then
  `shutdown`. Executions still running are terminated and their fates recorded.
- **Host crash / connection loss** → lease expiry → the runner terminates every execution of that
  boot, verifies each process group is empty, and records the outcome as `killed` with
  `termination: "lease"`. A reconnect within the lease resumes the same boot; a reconnect after
  expiry starts a new lease and cannot address the old executions.
- **Executor restart** → supervised commands keep running. A replacement executor re-adopts every
  shim whose recorded start time still matches, and the host reconciles from the handshake: an
  execution the executor still holds is kept (settling with the real exit code when it ends), and
  one it no longer holds settles as `killed` with the stderr note `shellout: runner lost`. A
  process-group id alone is never treated as proof of ownership — only the recorded supervisor
  identity is.
- **Protocol/version mismatch** → the host refuses to start and reports an actionable message.
- **Runner unavailable at startup** → after `connectRetryMs` (default 10 s) the plugin logs one
  warning naming the socket and mounts the stock in-process executor, so the agent keeps a working
  shell. `fallback: 'none'` restores fail-loud behaviour. The fallback is a startup decision and
  is never silent.
- **Connection lost at runtime** → `disconnectGraceMs` (default 10 s) to recover. A reconnect keeps
  the executions the runner still holds (reconciled against its handshake list, which carries each
  execution's terminal facts so a settlement announced while the host was away is not lost);
  otherwise they settle as `killed` with the provider note and later commands run in-process until
  reconnected. The window must exceed the worst-case executor restart, or a merely slow restart is
  reported as a lost runner.
- **Concurrency limit** → the runner admits at most `maxConcurrent` executions (default 64) and
  answers `busy` beyond that; the host surfaces it as a preparation failure.

### 5.6 Security

- The socket lives under `$DSH_HOME` (already `0700`), is created `0600`, and is refused if it
  exists with a different owner.
- The same user id is **not** a security boundary between host and runner; the protocol assumes a
  single trusted user, exactly like the harness's local executor. Document this explicitly.
- The runner spawns with the same environment scrub, executable resolution, and sandbox policy the
  harness applies locally. It never widens them.
- No new listening TCP socket; no network egress.

### 5.7 Configuration (host plugin)

| Key | Default | Meaning |
|---|---|---|
| `socketPath` | `$DSH_HOME/shellout/control.sock` | Runner socket |
| `connectTimeoutMs` | `5000` | Startup handshake bound; failure is fatal at load |
| `mirrorPollMs` | `150` | Output poll cadence |
| `mirrorRetainBytes` | `1048576` | Local mirror window |
| `heartbeatMs` | `5000` | Boot-lease ping cadence |
| `maxFrameBytes` | `8388608` | Frame cap the host accepts |

Runner plugin config: `socketPath`, `leaseTtlMs`, `maxConcurrent`, `settleRetentionMs`,
`disposeGraceMs`, `spillDir`.

## 6. Behaviour table

| Situation | Required behaviour |
|---|---|
| Command exits 0 | `status: 'exited'`, `exitCode: 0`; `result()` resolves with split streams |
| Command exits nonzero | Same, with the code as `exitCode`; not an error |
| Killed by signal | `status: 'killed'`, `signal` set; `result()` resolves (not rejects) |
| Timeout with `onExpiry: 'kill'` | Runner kills at the deadline; `result().timedOut === true` |
| Timeout with `onExpiry: 'none'` | Execution keeps running; no kill |
| `signal` aborted before spawn | `execute` throws without creating a process |
| `signal` aborted after publication | The runner kills the group; `result().aborted === true` |
| Spawn failure (bad cwd, missing exe) | `execute` throws the preparation error |
| Spawn still starting after the `go` verdict | `execute` returns the handle; the command runs once and settles with its real status (never a refusal the host would retry) |
| Output exceeds the cap | Non-lossy reads up to the cap; beyond it `lossy: true` (a truncated capture has no complete file, so no `spillPath`) |
| Executor restarts mid-execution | The command keeps running; the host settles it with its real exit code |
| Executor is replaced and no longer holds the execution | `done` resolves, `status: 'killed'`, stderr carries `shellout: runner lost` |
| Host dies mid-execution | Runner terminates the group at lease expiry; history records `killed` with `termination: "lease"` |
| Socket reconnect inside lease | Executions continue; host reconciles via `state` and resumes mirroring |
| Duplicate `exec` (same operation) | One process; same `executionId` returned |
| `kill` after settlement | Returns `{ requested: false }` |

## 7. Acceptance criteria

Each item is verified by a command in the repository.

1. **Unit: protocol.** `pnpm test --filter dsh-shellout-protocol` — frame round-trip, oversized
   frame rejection, version mismatch, malformed JSON.
2. **Unit: mirror.** `pnpm test --filter dsh-shellout` — consuming vs non-consuming reads, lossy
   boundary, stderr section format, final drain before settlement.
3. **Contract: host seam.** `pnpm test --filter dsh-shellout` runs the seam behaviour table
   (§6) against a fake runner: exit codes, signals, timeout policies, abort, spawn failure,
   duplicate exec, kill-after-settle.
4. **Integration: real runner.** `pnpm test:e2e` starts a real runner process and asserts:
   a command's stdout/spill/exit facts match the local executor's for the same request; a
   long-running command survives a host reconnect; killing the runner settles live executions as
   `killed` with the documented note.
5. **Isolation.** `pnpm test:isolation` starts a runner, issues a command that leaves a background
   descendant (`sleep 600 &`), and asserts: the host process has no child in that group, its event
   loop delay stays under the configured bound while the descendant lives, and the runner's lease
   expiry terminates the group after the host disappears.
6. **No harness edits.** `git -C <harness> status --porcelain` is unchanged by installing and
   enabling this plugin; enabling is one composition row.

## 8. Out of scope (explicit)

PTY terminals, non-shell process sources, Windows, cross-restart durability, remote runners,
resource quotas (cgroups), metrics/tracing endpoints. Each is a follow-up with its own spec.

## 9. Context snapshot (volatile — re-verify before relying on it)

Snapshot taken 2026-10-24 against DeepSeek Harness `0.2.1-rc.2`.

- **Seam to implement** — the harness shell service definition exposes an abstract executor with
  `resolve(request) → spec`, `execute(spec) → execution`, a `sandboxMode` getter, and the
  `ShellProcess`/`ShellExecution` types used above. The local concrete executor is the reference
  implementation of those semantics; its tests are the closest thing to a contract suite.
- **Composition mechanism** — profiles stack patch layers; entries address existing rows by `id`
  and may override `name`/`config` or disable them. Replacing the shell executor is therefore one
  row override plus one disable, with no harness code change.
- **Runner launch** — the CLI boots any profile and, once the plugin tree is active, holds the
  process open; a profile containing only execution plugins plus this project's server plugin
  stays alive on its listening socket without needing an agent or an app entry point.
- **Dependency availability** — the harness packages targeted here are published; the plugin
  declares them as peer dependencies and pins a compatible range.
- **Environment** — development happens on Linux with Node 22+/24+ and pnpm; the runner profile is
  supervised by the deployment's process manager as a separate service.

### 9.1 Deliverables

- [x] `dsh-shellout-protocol`: frames, envelope, version negotiation, error codes
- [x] `dsh-shellout`: host provider (resolve/execute/mirror/lifecycle)
- [x] `dsh-shellout-runner`: server, execution registry, lease, drain
- [x] Runner composition + supervisor unit documentation (`examples/`)
- [x] Unit, contract, integration, and isolation suites (§7) — `pnpm test`, `pnpm test:e2e`
- [x] README install/enable instructions (one row, one disable)
- [x] Model-driven turn — an isolated web instance ran an agent turn whose `bash` command executed as a child of the runner while the host process had no children (§7.5 evidence)
