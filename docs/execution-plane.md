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
<state>/<executionId>/
  meta.json     argv, cwd, complete env, stdin, deadline and capture policy, boot/operation identity
  status.json   command pid/pgid/start time, shim identity, timestamps, terminal and stream facts
  stdout.log    append-only, byte-capped capture
  stderr.log    append-only, byte-capped capture
  kill.request  durable termination request, consumed by the shim
```

`status.json` is replaced with `write`+`rename`, so a reader never sees a torn
document. The shim writes `state: running` before `exec` and the terminal facts
after the child is reaped; the executor writes the same record to history.

## Executor lifetime

* On start, the executor scans the state directory. For each shim whose
  `status.json` says running, it checks the recorded pid's start time (pid +
  `/proc/<pid>/stat` field 22) and, when it matches, **adopts** the execution:
  it re-registers it, serves its output from the files, and settles it from
  `status.json` when the shim finishes.
* When the pid is gone or the start time differs (pid reuse), the execution is
  marked `lost` — a fact, not a guess.
* SIGTERM: the executor stops serving and exits without signalling shims or commands.
  On adoption, a host boot receives a fresh lease window to reconnect; deadlines
  remain owned by the shim while the executor is absent.

## Hub lifetime

* Opens history **read-only**. Only the executor writes rows.
* Live state comes from an `observe` RPC: no `hello`, no lease, no event
  subscription, a total deadline, and a cap on concurrent calls.
* `/` and `/health` never call the executor; `/api/*` degrade to history-only
  and report `executor: "unreachable"` instead of inventing zeroes.

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
`stderrMaxBytes`. `spillDir` remains accepted as protocol input but never redirects
durable capture outside the execution directory. Readers continue draining pipes
after the cap; `bytes` counts observed bytes, and `truncated`/`lossy` record loss.
`readStream` offsets are log byte offsets, with replies capped at 256 KiB and the
connection's frame budget. A complete log may be returned as `spillPath`.
On direct-child exit the shim terminates remaining group members before publishing
terminal status, so a background descendant cannot keep the capture pipes open.

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
