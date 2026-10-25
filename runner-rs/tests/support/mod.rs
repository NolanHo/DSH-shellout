//! Shared helpers for the execution-plane integration tests: a real executor
//! process, a framed protocol client, and bounded waiting without a runtime.
#![allow(dead_code)]

use std::io::{self, Read, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

pub const BINARY: &str = env!("CARGO_BIN_EXE_dsh-shellout-runner");

/// A scratch directory owned by one test.
pub struct Scratch {
    pub root: PathBuf,
}

impl Scratch {
    pub fn new(name: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(".task-tmp")
            .join(format!(
                "{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("scratch directory");
        Self { root }
    }
    pub fn state(&self) -> PathBuf {
        self.root.join("state")
    }
    pub fn history(&self) -> PathBuf {
        self.root.join("history.sqlite")
    }
    pub fn socket(&self) -> PathBuf {
        self.root.join("control.sock")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// An executor process, killed when the test ends.
pub struct Executor {
    pub child: Child,
    pub stderr: PathBuf,
    socket: PathBuf,
}

impl Executor {
    pub fn start(scratch: &Scratch) -> Self {
        Self::start_named(scratch, "executor", &[])
    }

    pub fn start_named(scratch: &Scratch, name: &str, extra: &[&str]) -> Self {
        let stderr = scratch.root.join(format!("{name}.stderr"));
        let file = std::fs::File::create(&stderr).expect("stderr file");
        let mut command = Command::new(BINARY);
        command
            .arg("--state")
            .arg(scratch.state())
            .arg("--history")
            .arg(scratch.history())
            .arg("--socket")
            .arg(scratch.socket())
            .arg("--lease-ttl-ms")
            .arg("60000")
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(file));
        Self {
            child: command.spawn().expect("executor starts"),
            stderr,
            socket: scratch.socket(),
        }
    }

    /// Wait until the socket answers a request.
    pub fn ready(&self) -> Client {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Ok(mut client) = Client::connect(&self.socket) {
                // Any answer, including a wire error for the unknown probe id,
                // proves the server is accepting and dispatching.
                let _ = client.request("state", json!({"executionId": "probe"}));
                return client;
            }
            assert!(Instant::now() < deadline, "executor did not become ready");
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Give the executor SIGKILL: its shims must survive it.
    pub fn diagnose(&self) -> String {
        std::fs::read_to_string(&self.stderr).unwrap_or_default()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// One framed connection speaking the executor protocol.
pub struct Client {
    stream: UnixStream,
    buffer: Vec<u8>,
    next_id: u64,
    pub events: Vec<Value>,
}

impl Client {
    pub fn connect(path: &Path) -> io::Result<Self> {
        let stream = UnixStream::connect(path)?;
        stream.set_read_timeout(Some(Duration::from_millis(50)))?;
        Ok(Self {
            stream,
            buffer: Vec::new(),
            next_id: 0,
            events: Vec::new(),
        })
    }

    pub fn hello(&mut self, boot: &str) -> Value {
        self.request(
            "hello",
            json!({"protocol": "2.0", "clientBuild": "test", "bootId": boot,
                "maxFrameBytes": 8 * 1024 * 1024}),
        )
        .unwrap_or_else(|| panic!("hello answered"))
    }

    pub fn exec(&mut self, id: &str, command: &str, overrides: Value) -> Option<Value> {
        let mut spec = json!({
            "argv": ["/bin/sh", "-c", command],
            "cwd": "/tmp",
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 10000,
            "onExpiry": "kill",
            "graceMs": 100,
            "capture": {"stdoutMaxBytes": 64000, "stderrMaxBytes": 64000,
                "spillMaxBytes": 1048576, "spillDir": "/tmp"},
        });
        if let Some(object) = overrides.as_object() {
            for (key, value) in object {
                spec[key] = value.clone();
            }
        }
        self.request(
            "exec",
            json!({"executionId": id, "operationId": format!("op-{id}"),
                "specDigest": command, "spec": spec}),
        )
    }

    /// Send a request and answer its reply, buffering events that arrive first.
    pub fn request(&mut self, method: &str, payload: Value) -> Option<Value> {
        let frame = self.call(method, payload)?;
        (frame["kind"] == "res").then(|| frame["payload"].clone())
    }

    /// Send a request and answer its whole reply frame, error frames included.
    pub fn call(&mut self, method: &str, payload: Value) -> Option<Value> {
        self.next_id += 1;
        let id = self.next_id;
        let body = serde_json::to_vec(
            &json!({"id": id, "kind": "req", "method": method, "payload": payload}),
        )
        .ok()?;
        let mut frame = u32::try_from(body.len()).ok()?.to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        self.stream.write_all(&frame).ok()?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let Some(frame) = self.read_frame() else {
                // A read timeout is not an answer; the reply may still arrive.
                assert!(Instant::now() < deadline, "no reply to {method}");
                std::thread::sleep(Duration::from_millis(5));
                continue;
            };
            if frame["kind"] == "evt" {
                self.events.push(frame);
                continue;
            }
            if frame["id"].as_u64() == Some(id) {
                return Some(frame);
            }
            if Instant::now() >= deadline {
                return None;
            }
        }
    }

    /// Wait for one event matching `predicate`, reading frames meanwhile.
    pub fn wait_event(
        &mut self,
        predicate: impl Fn(&Value) -> bool,
        timeout: Duration,
    ) -> Option<Value> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(position) = self.events.iter().position(&predicate) {
                return Some(self.events.remove(position));
            }
            if Instant::now() >= deadline {
                return None;
            }
            if let Some(frame) = self.read_frame() {
                if frame["kind"] == "evt" {
                    self.events.push(frame);
                }
            }
        }
    }

    /// State of one execution, or `None` on a wire error.
    pub fn state(&mut self, id: &str) -> Option<Value> {
        self.request("state", json!({"executionId": id}))
    }

    fn read_frame(&mut self) -> Option<Value> {
        loop {
            if self.buffer.len() >= 4 {
                let length = u32::from_be_bytes(self.buffer[..4].try_into().ok()?) as usize;
                if self.buffer.len() >= 4 + length {
                    let frame = serde_json::from_slice(&self.buffer[4..4 + length]).ok();
                    self.buffer.drain(..4 + length);
                    return frame;
                }
            }
            let mut chunk = [0u8; 8192];
            match self.stream.read(&mut chunk) {
                Ok(0) => return None,
                Ok(count) => self.buffer.extend_from_slice(&chunk[..count]),
                Err(error)
                    if matches!(
                        error.kind(),
                        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                    ) =>
                {
                    return None
                }
                Err(_) => return None,
            }
        }
    }
}

/// Poll `probe` until it answers or the deadline passes.
pub fn until<T>(mut probe: impl FnMut() -> Option<T>, timeout: Duration, label: &str) -> T {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        if Instant::now() >= deadline {
            panic!("timeout: {label}");
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Read one JSON document, retrying while it is being replaced.
pub fn read_json(path: &Path) -> Value {
    until(
        || {
            std::fs::read_to_string(path)
                .ok()
                .and_then(|text| serde_json::from_str(&text).ok())
        },
        Duration::from_secs(5),
        &format!("json at {}", path.display()),
    )
}

/// Write one execution's metadata, standing in for an executor that admitted it.
pub fn write_meta(state: &Path, id: &str, spec: Value) {
    let dir = state.join(id);
    std::fs::create_dir_all(&dir).expect("execution directory");
    let mut meta =
        json!({"id": id, "operationId": format!("op-{id}"), "digest": "d", "bootId": "test-boot"});
    for (key, value) in spec.as_object().expect("spec object") {
        meta[key] = value.clone();
    }
    std::fs::write(
        dir.join("meta.json"),
        serde_json::to_vec(&meta).expect("meta json"),
    )
    .expect("meta write");
}
