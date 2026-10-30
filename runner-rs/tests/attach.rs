//! Attach over a transport command: probe, push, verify, then speak frames.
//!
//! The transport here is `scripts/fake-remote.sh`, which executes the remote
//! command locally. Everything the attach adds — platform probe, versioned
//! cache, sha256-pinned push, run verification — is exercised unchanged; only
//! the ssh hop itself is substituted, because ssh is not ours to reimplement.

mod support;

use std::io::{Read, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use support::Scratch;

/// The controller must be the static build: attach refuses to push a binary
/// that carries a dynamic loader, because that is the failure this plane exists
/// to avoid. Build it with `scripts/build-static.sh`.
fn controller() -> Option<PathBuf> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/x86_64-unknown-linux-musl/release/dsh-shellout-runner");
    if path.exists() {
        return Some(path);
    }
    // A silently skipped remote test looks exactly like a passing one, so the
    // gate sets this to refuse that trade instead of accepting it.
    assert!(
        std::env::var_os("DSH_REQUIRE_STATIC").is_none(),
        "the static controller is missing: run scripts/build-static.sh"
    );
    None
}

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("repo root")
        .to_path_buf()
}

/// A remote root short enough for the unix socket it contains.
///
/// A real host uses `$HOME/.dsh-shellout`; the repo-relative scratch is long
/// enough to hit `SUN_LEN`, which is a property of the path rather than of the
/// transport, so the test keeps the path realistic.
fn remote_root(scratch: &Scratch) -> PathBuf {
    let slug = scratch
        .root
        .file_name()
        .expect("scratch name")
        .to_string_lossy()
        .into_owned();
    PathBuf::from("/tmp").join(format!("dsh-attach-{slug}"))
}

struct Attached {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    next_id: u64,
}

impl Attached {
    fn start(scratch: &Scratch, remote_root: &Path) -> Self {
        let transport = repo().join("scripts/fake-remote.sh");
        let binary = controller().expect("static controller");
        let mut child = Command::new(binary)
            .args(["--mode", "attach", "--host", "fake-host", "--ssh"])
            .arg(&transport)
            .arg("--remote-root")
            .arg(remote_root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("attach starts");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        let _ = scratch;
        Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    fn request(&mut self, method: &str, payload: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        write_frame(
            &mut self.stdin,
            &json!({ "id": id, "kind": "req", "method": method, "payload": payload }),
        );
        loop {
            let reply = read_frame(&mut self.stdout);
            if reply["kind"] == "evt" {
                continue;
            }
            assert_eq!(reply["id"], id, "{reply}");
            return reply;
        }
    }

    fn settled(&mut self, execution_id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                Instant::now() < deadline,
                "no settlement for {execution_id}"
            );
            let frame = read_frame(&mut self.stdout);
            if frame["kind"] == "evt" && frame["payload"]["executionId"] == execution_id {
                return frame["payload"].clone();
            }
        }
    }
}

impl Drop for Attached {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_frame(stdin: &mut ChildStdin, value: &Value) {
    let body = serde_json::to_vec(value).expect("encode");
    stdin
        .write_all(&(body.len() as u32).to_be_bytes())
        .expect("length");
    stdin.write_all(&body).expect("body");
    stdin.flush().expect("flush");
}

fn read_frame(stdout: &mut ChildStdout) -> Value {
    let mut header = [0u8; 4];
    stdout.read_exact(&mut header).expect("frame length");
    let mut body = vec![0u8; u32::from_be_bytes(header) as usize];
    stdout.read_exact(&mut body).expect("frame body");
    serde_json::from_slice(&body).expect("frame json")
}

fn spec(command: &str) -> Value {
    json!({
        "argv": ["/bin/sh", "-c", command],
        "cwd": "/tmp",
        "env": { "PATH": "/usr/local/bin:/usr/bin:/bin" },
        "timeoutMs": 30000,
        "onExpiry": "kill",
        "graceMs": 500,
        "capture": { "stdoutMaxBytes": 65536, "stderrMaxBytes": 65536, "spillMaxBytes": 0, "spillDir": "/tmp" }
    })
}

#[test]
fn bootstraps_a_host_and_runs_a_command_over_the_transport() {
    let Some(_controller) = controller() else {
        eprintln!("skipped: run scripts/build-static.sh to provide the static controller");
        return;
    };
    let scratch = Scratch::new("attach-run");
    let remote_root = remote_root(&scratch);
    let mut attached = Attached::start(&scratch, &remote_root);

    let hello = attached.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-attach" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");
    assert_eq!(hello["payload"]["protocol"], "2.0", "{hello}");

    let exec = attached.request(
        "exec",
        json!({ "operationId": "op-attach", "executionId": "attach-1", "specDigest": "d", "spec": spec("printf 'remote-ok\\n'") }),
    );
    assert_eq!(exec["kind"], "res", "{exec}");
    let settled = attached.settled("attach-1");
    assert_eq!(settled["status"], "exited", "{settled}");
    assert_eq!(settled["exitCode"], 0, "{settled}");

    let read = attached.request(
        "readStream",
        json!({ "executionId": "attach-1", "stream": "stdout", "offset": 0, "limit": 4096 }),
    );
    assert_eq!(read["payload"]["text"], "remote-ok\n", "{read}");

    // The bootstrap landed the artifact where the versioned cache says it
    // should, and it is executable.
    let binary = remote_root
        .join("bin")
        .join(format!("{VERSION}-x86_64"))
        .join("runner");
    assert!(binary.exists(), "pushed binary at {binary:?}");
    let mode = std::fs::metadata(&binary).expect("metadata").permissions();
    assert!(mode.mode() & 0o100 != 0, "the pushed binary is executable");
}

/// Simulate a host reboot: every process of the plane on that host disappears
/// while the state directory stays behind.
#[test]
fn a_rebooted_host_reconciles_its_running_rows_as_lost() {
    let Some(_controller) = controller() else {
        eprintln!("skipped: run scripts/build-static.sh to provide the static controller");
        return;
    };
    let scratch = Scratch::new("attach-reboot");
    let remote_root = remote_root(&scratch);
    let state = remote_root.join("state");
    let history = remote_root.join("history.sqlite");

    let mut attached = Attached::start(&scratch, &remote_root);
    let hello = attached.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-reboot" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");
    attached.request(
        "exec",
        json!({ "operationId": "op-reboot", "executionId": "reboot-1", "specDigest": "d", "spec": spec("sleep 120") }),
    );

    // Wait until the plane really started the command.
    let status_path = state.join("reboot-1").join("status.json");
    let running = support::until(
        || {
            let status: Value = support::read_json(&status_path);
            (status["state"] == "running").then_some(status)
        },
        Duration::from_secs(20),
        "the command starts",
    );
    let shim = running["shimPid"].as_i64().expect("shim pid") as i32;
    let command = running["pid"].as_i64().expect("command pid") as i32;

    // The reboot: the client goes away, then every process of the plane.
    drop(attached);
    for pid in [command, shim] {
        // SAFETY: pids just read from the state document; ESRCH is tolerated.
        unsafe {
            libc::kill(pid, libc::SIGKILL);
        }
    }
    std::thread::sleep(Duration::from_millis(500));

    // A fresh attach is what a later session does after the host came back.
    let mut after = Attached::start(&scratch, &remote_root);
    let hello = after.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-after-reboot" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");

    // History is a projection of the state directory: a row whose process is
    // gone must read `lost`, not `running`, without anyone settling it.
    let rows = support::until(
        || {
            let connection = rusqlite::Connection::open_with_flags(
                &history,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .ok()?;
            let state: String = connection
                .query_row(
                    "select state from executions where id = 'reboot-1'",
                    [],
                    |row| row.get(0),
                )
                .ok()?;
            (state == "lost").then_some(state)
        },
        Duration::from_secs(20),
        "the rebooted row reconciles to lost",
    );
    assert_eq!(rows, "lost");
}

/// The executor may die while a command runs; the command and its facts must
/// survive, exactly as on the local plane.
#[test]
fn a_killed_remote_executor_is_adopted_and_still_reports_the_real_exit_code() {
    let Some(_controller) = controller() else {
        eprintln!("skipped: run scripts/build-static.sh to provide the static controller");
        return;
    };
    let scratch = Scratch::new("attach-adopt");
    let remote_root = remote_root(&scratch);
    let state = remote_root.join("state");
    let history = remote_root.join("history.sqlite");

    let mut attached = Attached::start(&scratch, &remote_root);
    let hello = attached.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-adopt" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");
    attached.request(
        "exec",
        json!({
            "operationId": "op-adopt",
            "executionId": "adopt-1",
            "specDigest": "d",
            "spec": spec("i=0; while [ $i -lt 4 ]; do echo tick-$i; i=$((i+1)); sleep 1; done; exit 7"),
        }),
    );
    let status_path = state.join("adopt-1").join("status.json");
    support::until(
        || {
            let status: Value = support::read_json(&status_path);
            (status["state"] == "running").then_some(())
        },
        Duration::from_secs(20),
        "the command starts",
    );

    // Kill the executor only: the shim owns the command and must keep it alive.
    let identity = support::read_json(&state.join("executor.json"));
    let executor = identity["pid"].as_i64().expect("executor pid") as i32;
    // SAFETY: the pid this plane just published; ESRCH is tolerated.
    unsafe {
        libc::kill(executor, libc::SIGKILL);
    }
    drop(attached);
    std::thread::sleep(Duration::from_millis(800));

    // A later attach adopts the shim and reports the settlement it observed.
    let mut after = Attached::start(&scratch, &remote_root);
    let hello = after.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-adopt-after" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");
    let settled = after.settled("adopt-1");
    assert_eq!(settled["exitCode"], 7, "{settled}");

    let stdout = after.request(
        "readStream",
        json!({ "executionId": "adopt-1", "stream": "stdout", "offset": 0, "limit": 4096 }),
    );
    assert_eq!(
        stdout["payload"]["text"], "tick-0\ntick-1\ntick-2\ntick-3\n",
        "{stdout}"
    );

    // History agrees with the settlement the host received.
    let row = support::until(
        || {
            let connection = rusqlite::Connection::open_with_flags(
                &history,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )
            .ok()?;
            let row: (String, Option<i64>) = connection
                .query_row(
                    "select state, exit_code from executions where id = 'adopt-1'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .ok()?;
            (row.0 == "exited").then_some(row)
        },
        Duration::from_secs(20),
        "the adopted execution reaches history",
    );
    assert_eq!(row.1, Some(7), "{row:?}");
}

/// A controller that attaches later still gets the host's settlements: the
/// history lives with the host, so the protocol has to return it.
#[test]
fn a_late_attach_can_read_the_hosts_history() {
    let Some(_controller) = controller() else {
        eprintln!("skipped: run scripts/build-static.sh to provide the static controller");
        return;
    };
    let scratch = Scratch::new("attach-history");
    let remote_root = remote_root(&scratch);

    let mut first = Attached::start(&scratch, &remote_root);
    let hello = first.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-history-1" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");
    first.request(
        "exec",
        json!({ "operationId": "op-history", "executionId": "history-1", "specDigest": "d", "spec": spec("printf 'kept\\n'") }),
    );
    let _ = first.settled("history-1");
    drop(first);

    let mut second = Attached::start(&scratch, &remote_root);
    let hello = second.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": "boot-history-2" }),
    );
    assert_eq!(hello["kind"], "res", "{hello}");
    let history = second.request("history", json!({ "limit": 10 }));
    assert_eq!(history["kind"], "res", "{history}");
    let rows = history["payload"]["rows"].as_array().expect("rows");
    let row = rows
        .iter()
        .find(|row| row["id"] == "history-1")
        .unwrap_or_else(|| panic!("history-1 in {history}"));
    assert_eq!(row["state"], "exited", "{row}");
    assert_eq!(row["exitCode"], 0, "{row}");
}
