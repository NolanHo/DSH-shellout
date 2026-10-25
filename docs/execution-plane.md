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
  meta.json     argv, cwd, env keys, deadline policy, capture budgets  (written before spawn)
  status.json   pid, pgid, startedAt, finishedAt, exitCode, signal, state
  stdout.log    append-only capture
  stderr.log    append-only capture
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
* SIGTERM: the executor tells live shims to stop waiting (they keep the command
  running), then exits. Nothing is killed by the executor's own shutdown.

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
