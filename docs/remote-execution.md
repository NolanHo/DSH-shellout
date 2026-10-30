# Remote execution

The runner already supervises commands on the machine that hosts DSH. This
document defines the second plane: running the same protocol against a host
reached over SSH, with the same supervision, the same settlement facts and the
same durability — while `rexec` keeps doing what it does well.

## Boundary with rexec

| Concern | Owner |
|---|---|
| Host resolution, `ProxyJump`, keys, port | ssh (and rexec's own resolution when it drives the call) |
| File synchronization | **rexec** — `rsync` semantics are not reimplemented here |
| Process supervision, output capture, settlement, history | **runner** |

Nothing in this design changes rexec's behaviour. rexec's contract is that a
remote command survives a dropped connection, that its output can be resumed,
and that sync is explicit and reversible; the runner supplies the same
guarantees from the other side, so either can drive the other without changing
what either promises.

## What goes wrong today, and why

`rexec` deploys a worker per host. Its two paths have distinct failure classes,
both visible in its source:

1. **Same-platform deploy uploads the locally running binary** (`ensure_remote_binary_traced`,
   `src/ssh.rs:1065`). If that binary is a glibc build, it fails on a musl host
   (Alpine) and on any distro older than the machine that built it — the host
   reports a loader error, not an arch mismatch.
2. **Cross-platform deploy downloads a release asset** (`download_worker`,
   `src/ssh.rs:1294`, URL built at `src/main.rs:5160`). The released Linux
   assets are musl-static (`release.yml:40-44`), so libc is not the problem
   there; **egress to `github.com` is**. A host or controller behind a proxy, or
   in an air-gapped network, cannot take this path at all.
3. **Platform detection is `uname -sm`** (`uname_asset`, `src/ssh.rs:1199`),
   which has no answer for Git-Bash/MSYS and refuses anything not Linux, macOS
   or Windows — by design, and clearly reported.
4. **`rsync` must exist on the remote** for any `--sync` work; `init` installs
   it when the host has a package manager.

Only (1) and (2) are defects in the sense of "works here, not there"; (3) and
(4) are deliberate scope.

## How this design avoids them

- **One artifact for every host.** `scripts/build-static.sh` produces a
  `static-pie` musl binary (~3 MB) with `zig` as the C toolchain for vendored
  SQLite, so no system package is needed to build it and no libc is needed to
  run it. The same file serves the local plane and every remote host.
- **The binary travels over the channel it will use.** The first attach pushes
  the bytes through the same ssh connection (`cat > …`), verifies `sha256`, and
  renames into place. No release host, no egress requirement, no version skew
  between "what we downloaded" and "what we run".
- **Versioned cache per host.** `~/.dsh-shellout/bin/<version>-<arch>/runner`,
  so an upgrade is a new directory and a rollback is the previous one.
- **No new dependencies.** Execution needs `sh`, `mkdir`, `cat`, `chmod`,
  `sha256sum` (or `shasum`), all POSIX. Sync stays with rexec.

## Transport

`--mode stdio` runs the executor protocol on stdin/stdout instead of a unix
socket. Frames are the identical `[u32 length][JSON]` contract.

```
dsh host plugin ──unix socket──▶ local executor
                                     │
                                     └──ssh──▶ dsh-shellout-runner --mode stdio --state ~/.dsh-shellout/state
                                                   └──▶ shim ──▶ command
```

`ssh` supplies identity, `ProxyJump`, keepalives and host keys; the runner never
listens on a TCP port, so there is nothing new to authenticate.

**Bridge or become.** On start, stdio mode asks the host's unix socket
(`<state>/control.sock`) whether an executor already owns this state directory:

- it answers → the connection is bridged onto it, so several sessions share one
  supervisor and one history;
- nobody answers → this process takes the state lock and becomes the executor.

This reuses the existing single-owner rule: a second executor on the same state
directory is refused, and a bridge is just another client.

## Durability across the connection

The shim is the parent of the command, so the guarantees already documented for
the local plane hold unchanged:

| Event | Command | History | Dashboard |
|---|---|---|---|
| ssh connection drops | keeps running (shim owns it) | intact | remote rows resume on the next attach |
| remote executor killed | keeps running; the next attach adopts it by pid+start time | intact | idem |
| remote host reboots | lost (processes are gone) | rows reconcile to `lost` on the next attach | visible as `lost` |

The reconnect path is the existing settlement replay plus the handshake's
terminal facts — no new state machine.

## Attaching from a controller

```sh
dsh-shellout-runner --mode attach --host <alias> [--ssh "ssh -J jump"] [--remote-root <path>]
```

`--mode attach` performs the bootstrap below and then bridges its own stdin and
stdout to the remote executor, so any client that could speak to a local socket
can speak to a remote host by spawning this command instead. The transport is a
plain argv (`ssh` by default, `{host}` substituted where it appears), which also
makes the whole sequence testable without an sshd: `scripts/fake-remote.sh`
executes the remote command locally, and `tests/attach.rs` drives the real
binary through it.

The protocol answers `history { limit }` with the host's recent settlements, so
a controller that attaches after the fact still sees what ran there. The rows
live with the host — that is where the state directory and the shim facts are —
and a dashboard merges them by asking each attached host.

## Bootstrap protocol

Attach performs, in order, each step separately diagnosable:

1. `uname -sm` → `linux-x86_64` / `linux-aarch64`; anything else is a clear
   refusal, never a silent fallback.
2. Probe the state root: `$DSH_SHELLOUT_HOME`, else `~/.dsh-shellout`, else
   `/tmp/dsh-shellout-<uid>` when the home is read-only.
3. Probe executability: run the cached binary with `--mode status`; a `noexec`
   mount or a wrong arch shows up here rather than at the first real command.
4. If the cached version does not answer, push the local static binary, verify
   `sha256`, `chmod 0700`, atomic rename, then re-probe.
5. Exec `… --mode stdio --state <root>/state` and speak frames.

Every step reports what it found, so a host that fails says which precondition
failed instead of "worker died".

## Degradations, stated rather than assumed

| Condition | Behaviour |
|---|---|
| No `/proc` (unusual containers) | adoption cannot verify identity; executions settle from `status.json` but are never adopted — reported as `lost` on attach |
| Read-only home | state root moves to `/tmp/dsh-shellout-<uid>` (0700, sticky parent); history warns it is not durable across reboot |
| `noexec` home | detected at the probe; state root falls back to `/tmp` with the same warning |
| No `sha256sum`/`shasum` | attach refuses: an unverified binary is not executed |
| Non-Linux remote | refused with the detected platform, by design |

## Security

- Identity is ssh's; the runner adds no credentials and opens no port.
- State root is `0700`, socket `0600`, `meta.json` and `status.json` carry the
  command's argv and cwd but never its environment values.
- The pushed binary is pinned by `sha256` of the bytes actually sent.
- A remote host can always run anything the ssh account can run; the runner does
  not pretend to sandbox beyond the policy the host already passes.

## Test plan

- `--mode stdio` over a pipe pair: framing, request/response, settlement replay.
- Bridge: two connections to one state directory share one executor; the second
  does not take the lock.
- Bootstrapped attach to `localhost` over ssh: probe, push, verify, run a
  command, compare its exit facts with the local plane.
- Kill the remote executor mid-command over ssh; the next attach adopts and
  settles with the real exit code.
- Simulate a reboot (delete shims, keep the state directory); rows reconcile to
  `lost`.
- Static binary runs with an empty `PATH` and no `/etc/ld.so` entry.

## Non-goals

- Replacing `rsync` or rexec's sync semantics.
- Windows remotes.
- Multi-host fan-out schedulers; one attach serves one host.
- Changing any rexec behaviour.
