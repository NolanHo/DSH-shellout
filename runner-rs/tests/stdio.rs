//! The ssh transport: the same protocol, carried by a child process's stdio.
//!
//! This is what `ssh host -- dsh-shellout-runner --mode stdio` relies on, so the
//! test drives the binary exactly that way: frames on the child's stdin and
//! stdout, no socket involved.

mod support;

use std::io::{Read, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use support::{until, Executor, Scratch};

/// One stdio-attached runner.
struct Attached {
    child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    next_id: u64,
}

impl Attached {
    /// Start the runner in stdio mode against `scratch`'s state directory.
    ///
    /// The socket is passed explicitly: a stdio session must look for the owner
    /// where the deployment actually put it, not at a default it guessed.
    fn start(scratch: &Scratch) -> Self {
        let mut child = Command::new(support::BINARY)
            .args(["--mode", "stdio", "--state"])
            .arg(scratch.state())
            .arg("--history")
            .arg(scratch.history())
            .arg("--socket")
            .arg(scratch.socket())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("stdio runner starts");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = child.stdout.take().expect("stdout");
        Self {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    /// Send one request and return its reply, skipping unsolicited events.
    fn request(&mut self, method: &str, payload: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let frame = json!({ "id": id, "kind": "req", "method": method, "payload": payload });
        write_frame(&mut self.stdin, &frame);
        loop {
            let reply = read_frame(&mut self.stdout);
            if reply["kind"] == "evt" {
                continue;
            }
            assert_eq!(reply["id"], id, "reply for our request: {reply}");
            return reply;
        }
    }

    /// Wait for one settlement event for `execution_id`.
    fn settled(&mut self, execution_id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(20);
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
    let length = u32::from_be_bytes(header) as usize;
    let mut body = vec![0u8; length];
    stdout.read_exact(&mut body).expect("frame body");
    serde_json::from_slice(&body).expect("frame json")
}

fn hello(attached: &mut Attached, boot: &str) -> Value {
    attached.request(
        "hello",
        json!({ "protocol": "2.0", "clientBuild": "test", "bootId": boot }),
    )
}

#[test]
fn runs_a_command_over_stdin_and_stdout() {
    let scratch = Scratch::new("stdio-run");
    let mut attached = Attached::start(&scratch);
    let reply = hello(&mut attached, "boot-stdio");
    assert_eq!(reply["kind"], "res", "{reply}");
    assert_eq!(reply["payload"]["protocol"], "2.0", "{reply}");

    let exec = attached.request(
        "exec",
        json!({
            "operationId": "op-stdio",
            "executionId": "stdio-1",
            "specDigest": "digest",
            "spec": {
                "argv": ["/bin/sh", "-c", "printf 'over-stdio\\n'; exit 0"],
                "cwd": scratch.root.display().to_string(),
                "env": { "PATH": "/usr/local/bin:/usr/bin:/bin" },
                "timeoutMs": 20000,
                "onExpiry": "kill",
                "graceMs": 500,
                "capture": { "stdoutMaxBytes": 4096, "stderrMaxBytes": 4096, "spillMaxBytes": 0, "spillDir": scratch.root.display().to_string() }
            }
        }),
    );
    assert_eq!(exec["kind"], "res", "{exec}");

    let settled = attached.settled("stdio-1");
    assert_eq!(settled["status"], "exited", "{settled}");
    assert_eq!(settled["exitCode"], 0, "{settled}");

    let read = attached.request(
        "readStream",
        json!({ "executionId": "stdio-1", "stream": "stdout", "offset": 0, "limit": 4096 }),
    );
    assert_eq!(read["payload"]["text"], "over-stdio\n", "{read}");
}

#[test]
fn a_second_session_bridges_instead_of_taking_the_state_root() {
    let scratch = Scratch::new("stdio-bridge");
    // The socket executor owns this state root first.
    let executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-owner");
    host.exec("owner-exec", "true", json!({}))
        .expect("admitted");

    // The stdio session must attach to it rather than start a second registry:
    // a second owner would be refused by the state lock.
    let mut attached = Attached::start(&scratch);
    let reply = hello(&mut attached, "boot-bridge");
    assert_eq!(reply["kind"], "res", "{reply}");

    let exec = attached.request(
        "exec",
        json!({
            "operationId": "op-bridge",
            "executionId": "bridge-1",
            "specDigest": "digest",
            "spec": {
                "argv": ["/bin/sh", "-c", "echo bridged"],
                "cwd": scratch.root.display().to_string(),
                "env": { "PATH": "/usr/bin:/bin" },
                "timeoutMs": 20000,
                "onExpiry": "kill",
                "graceMs": 500,
                "capture": { "stdoutMaxBytes": 4096, "stderrMaxBytes": 4096, "spillMaxBytes": 0, "spillDir": scratch.root.display().to_string() }
            }
        }),
    );
    assert_eq!(exec["kind"], "res", "{exec}");

    // The owner is the supervisor: the bridged execution shows up in its state.
    let path = scratch.state().join("bridge-1").join("status.json");
    let status = until(
        || {
            let status: Value = support::read_json(&path);
            (status["state"] != "running").then_some(status)
        },
        Duration::from_secs(20),
        "the owner settles the bridged execution",
    );
    assert_eq!(status["state"], "exited", "{status}");
    assert_eq!(status["exitCode"], 0, "{status}");
}
