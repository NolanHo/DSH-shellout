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
        let mut child = Command::new(support::BINARY)
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
