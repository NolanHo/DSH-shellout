# Execution plane

Three processes with distinct lifetimes. Each one can be restarted without
taking the others' guarantees down.

```
hub        service face: HTTP dashboard, read-only history, no child processes
 └ executor  supervision: speaks the host protocol, admits and reports executions
    └ shim     one per execution: parent of the command, owns its lifetime
```

## Why a shim

The host's sockets, the executor's memory and the dashboard are all replaceable.
A running command is not: detaching it with `setsid` only orphans it, because the
process group outlives whoever held the pipes and the deadline.

So every execution gets a **shim** — the same binary in `--mode shim`. The shim is
the command's parent, so it survives the executor, and it keeps everything that
matters about the execution in files:

| Concern | Owner |
|---|---|
| process group, `TERM`→`KILL`, deadline | shim |
| stdout/stderr bytes | shim → files, append-only |
| exit facts | shim → `status.json`, written atomically |
| admission, reporting to the host, history rows | executor |
| dashboard, history reader, availability state | hub |

## State directory

`$DSH_SHELLOUT_STATE` (default `<DSH_HOME>/shellout/state`), one directory per execution:

```
<state>/.lock      exclusive claim held by the one executor that owns this directory
<state>/<executionId>/
  meta.json     argv, cwd, complete env, stdin, deadline and capture policy, boot/operation identity
  status.json   command pid/pgid/start time, shim identity, timestamps, terminal and stream facts
  stdout.log    append-only, byte-capped capture
  stderr.log    append-only, byte-capped capture
  kill.request  durable termination request, consumed by the shim
  spawn.claim   the party that arbitrates this spawn's outcome
  spawn.verdict go / fail — the claim owner's one decision
```

`status.json` is replaced with `write`+`rename`, so a reader never sees a torn
document. The shim publishes its own identity (pid and start time) as its first
act, then the command's pid/pgid once spawned, and the terminal facts after the
child exits; the executor writes the same record to history. It never rewrites
an unchanged record, and it clears `meta.json`'s environment once the execution
is terminal.

The spawn outcome has exactly one arbiter. Before starting a shim the executor
creates `spawn.claim` with `O_EXCL`; a shim that finds a claim it did not create
publishes its identity and waits for `spawn.verdict`, and only `go` lets it start
the command. Until the executor writes `go`, a wait that expires writes `fail`,
records a `failed` history row, and keeps the execution addressable: a shim that
starts even later refuses the command, so an `exec` answered with a failure can
never run it. Once `go` is written the command may already be starting, so the
`exec` reply names the execution and the outcome arrives as a settlement: the
executor never answers that `exec` with a failure, never stops the command, and
never records a fixed `failed` row for a slow spawn. A published `spawnError` is
the one exception — it proves the command never started, so a still-waiting
`exec` fails synchronously.

Terminal facts other than the exit status:

| Field | Meaning |
|---|---|
| `termination` | `deadline`, `request`, `lease` or `drain` — who ended the execution |
| `spawnError` | why the command could not be started; its presence fails a still-waiting `exec` synchronously |

An execution whose stream a detached descendant keeps open is published as
`lossy` rather than blocking: the shim bounds the join, records the loss, and
publishes terminal facts regardless.

A settled execution's directory is deleted once its history row is durable and
a retention window (`--state-retention-ms`, default 15 s) has passed, so
readers that follow the settlement event can still fetch the tail. Keep that
window longer than the host's `disconnectGraceMs`: a host that reconnects
inside its grace still has to read the execution it missed. Before the deletion
the complete capture is hard-linked into the host's `capture.spillDir` as
`<spillDir>/<executionId>-<stream>.log`, so the `spillPath` a reader was given
keeps working; a read whose capture is gone reports `lossy` instead of an
empty stream.

## Executor lifetime

* **One executor per state directory.** It takes `flock(LOCK_EX|LOCK_NB)` on
  `<state>/.lock` before it unlinks or binds the socket, and exits with an
  error naming the lock when another executor holds it. Two executors would
  each adopt the other's shims, and the older lease would terminate the newer
  one's commands.
* On start, the executor scans the state directory and **adopts** every shim
  that is still alive: it checks the recorded supervisor start time (pid +
  `/proc/<pid>/stat` field 22) and re-registers the execution, serving its
  output from the files.
* Liveness is a property of the **shim**, not of the command: a shim that has
  already reaped its direct child is still terminating the group and draining
  the pipes, and the execution stays `running` until the shim publishes its
  terminal facts. Loading and polling use the same predicate, so a restart
  can never disagree with a running executor about the same file.
* When the supervisor is gone, or its start time differs (pid reuse), the
  execution is marked `lost` — a fact, not a guess.
* A directory whose shim has not published `status.json` yet is adopted as
  `running`, because the shim writes its identity as its first act. If nothing
  is published within the startup grace, the execution is marked `lost`.
* History is a projection of the state directory. A `running` row with no
  directory behind it becomes `lost` at startup, and a row only ever moves
  from `running` to a terminal state: a terminal row is never rewritten or
  regressed. An execution id whose row is already terminal is refused, never
  silently reused.
* Admission does not block the runtime: the executor waits for the shim's
  first report on a blocking thread and holds no admission lock while it
  waits, so an unrelated request is answered while a spawn is in flight.
* SIGTERM: the executor stops serving and exits without signalling shims or commands.
  On adoption, a host boot receives a fresh lease window to reconnect; deadlines
  remain owned by the shim while the executor is absent.

Settlement is replayable. A `settled` event is sent once; if it is emitted while
no connection is subscribed, the execution would stay `running` on every host
that missed it. The `hello` reply therefore carries each held execution's
terminal facts (`state`, `exitCode`, `signal`, `termination`, `detail`), and the
executor replays the retained settlements of that boot on the connection that
just handshaked for it — never by broadcast, so an unrelated host's stream stays
its own. A connection that falls behind the settlement channel is dropped so its
host re-handshakes and recovers the facts; events are never silently skipped.

## Hub lifetime

* Opens history **read-only**. Only the executor writes rows.
* Live state comes from an `observe` RPC: no `hello`, no lease, no event
  subscription, a total deadline, and a cap on concurrent calls.
* `/` and `/health` never call the executor; `/api/*` degrade to history-only
  and report `executor: "unreachable"` instead of inventing zeroes.
* Dashboard counters are derived from history, so an executor restart cannot
  reset them and a refused spawn is counted like any other failure.

## Deployment prerequisites

The shim is the command's parent, so anything that signals a process **group**
terminates supervised commands with it:

* The service manager must signal the executor process only: `KillMode=process`
  under systemd, `killasgroup=false`/`stopasgroup=false` under supervisor, or
  shims placed in their own scope. A control-group kill reaches every shim the
  executor started.
* The executor must not be PID 1 in a container. PID 1 inherits orphaned
  children, so a signalled init takes the shims — and their commands — with it.
* `disconnectGraceMs` on the host must exceed the worst-case executor restart,
  including process start, the state-directory scan and the handshake. A
  restart that is merely slower than the window is indistinguishable from a
  lost runner, and its executions settle as `killed`.
* Deployment replaces the binary file and restarts the executor. A running
  executor starts each shim through `/proc/self/exe`, the image it is already
  executing, so unlinking or renaming the old path does not break later
  executions; the restart is what moves the service to the new build.

## Wire budget

A list reply carries summaries only. Output is read per execution and per
offset, so a single large stream can never make a list reply exceed the frame
cap.

## Guarantees

| Event | Running commands | History | Dashboard |
|---|---|---|---|
| hub restart | untouched | untouched | ~1 s gap |
| executor restart | **survive**, settle with the real exit code | preserved | live rows return on reconnect |
| shim crash | that execution is lost | row marked `lost` | visible |
| history write failure | unaffected | the failure is reported in the API and logged, never silent | visible |

## Configuration and capture

Both modes resolve paths through the same resolver. Flags override environment:

| Flag | Environment | Default |
|---|---|---|
| `--state` | `DSH_SHELLOUT_STATE` | `<DSH_HOME>/shellout/state` |
| `--history` | `DSH_SHELLOUT_HISTORY` | `<DSH_HOME>/shellout/history.sqlite` |
| `--socket` | `DSH_SHELLOUT_SOCKET` | `<DSH_HOME>/shellout/control.sock` |
| `--executor-socket` | `DSH_SHELLOUT_EXECUTOR_SOCKET` | resolved `--socket` |
| `--listen` | `DSH_SHELLOUT_LISTEN` | `127.0.0.1:3001` (hub HTTP only) |

Absent `DSH_HOME`, use `$HOME/.dsh`. The executor has no HTTP listener;
`--dashboard` and `DSH_SHELLOUT_DASHBOARD` are rejected. Start the hub with `--mode hub --listen <address>`.
Shim inputs are only `--mode shim --state <directory> --id <executionId>`;
the executor writes private metadata before admission and spawn.

Capture keeps a **prefix**, not the old in-memory tail. Each stream's file cap
is `capture.spillMaxBytes` when nonzero, otherwise its `stdoutMaxBytes` or
`stderrMaxBytes`. When an execution is terminal and its capture is complete, the
executor links the log into the host's `capture.spillDir` as
`<spillDir>/<executionId>-<stream>.log`, and `spillPath` names that host-owned
copy, which outlives the state directory. A truncated capture has no complete
file and reports no `spillPath`. Readers continue draining pipes after the cap;
`bytes` counts observed bytes, and `truncated`/`lossy` record loss. `readStream`
offsets are log byte offsets, with replies capped at 256 KiB and the connection's
frame budget; a read whose state directory has already been deleted falls back to
the spill, and reports `lossy` when even that is gone. On direct-child exit the
shim terminates remaining group members before publishing terminal status, so a
background descendant cannot keep the capture pipes open.

The hub allows eight concurrent observations, with one 500 ms total deadline
including semaphore queueing, connection, and all frame I/O. A request uses
`kind: req`, `method: observe`, and optional `payload.executionId` for live detail.
Observer connections cannot become host connections; only host `hello` registers
a lease and subscribes to events. List/snapshot replies exclude output. History
lists have at most 50 summaries and 256 KiB total; oversized metadata or invalid
stored argv becomes an explicit error summary, never a silently skipped row.
Individual summaries are limited to 5 KiB; oversized IDs have `idTruncated: true`.
History detail uses a primary-key lookup, not a recent-row scan.

Admission is refused if its history row cannot be written. Settlement writes are
retried on the polling loop and logged with the execution ID. Executor observation
and hub statistics expose `historyWritable` and `historyError`. When the executor
is unavailable, counters are absent rather than zero and `executor` is
`unreachable`; missing/unreadable history yields `history: unavailable` and a
minimal page instead of a hub panic. The hub never reconciles or writes rows.
