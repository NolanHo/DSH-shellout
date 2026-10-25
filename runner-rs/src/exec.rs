//! Executor-side file-backed execution handles. The shim owns all child I/O.
use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use crate::capture;
use crate::shim::{self, Meta, Status, StreamStatus};
use crate::store::{Record, Store};

pub struct Execution {
    pub meta: Meta,
    pub dir: PathBuf,
    status: Mutex<Status>,
    pub dirty: AtomicBool,
    pub announced: AtomicBool,
}

#[derive(Default)]
pub struct Counters {
    pub spawned: AtomicU64,
    pub settled: AtomicU64,
    pub killed: AtomicU64,
    pub failed: AtomicU64,
}

impl Execution {
    pub fn state(&self) -> Status {
        self.status
            .lock()
            .expect("execution status poisoned")
            .clone()
    }

    pub fn running(&self) -> bool {
        self.state().state == "running"
    }

    pub fn kill(&self) -> io::Result<bool> {
        if !self.running() {
            return Ok(false);
        }
        // A durable request, consumed by the shim; executor never owns signals.
        fs::write(self.dir.join("kill.request"), [])?;
        Ok(true)
    }

    pub fn refresh(&self) {
        if !self.running() {
            return;
        }
        match shim::read_json::<Status>(&self.dir.join("status.json")) {
            Ok(mut next) => {
                if next.state == "running" && !shim::is_live(&next) {
                    // Re-read after the liveness check: publication may have
                    // raced the command's exit and removal from /proc.
                    if let Ok(latest) = shim::read_json::<Status>(&self.dir.join("status.json")) {
                        next = latest;
                    }
                    if next.state == "running"
                        && !shim::matches_process(next.shim_pid, next.shim_start_time)
                    {
                        next.state = "lost".into();
                        next.finished_at = Some(shim::now_ms());
                        next.error = Some("shim process disappeared".into());
                    }
                }
                if next.state != "running" {
                    self.dirty.store(true, Ordering::Relaxed);
                }
                *self.status.lock().expect("execution status poisoned") = next;
            }
            Err(error) => eprintln!("execution {}: status read failed: {error}", self.meta.id),
        }
    }

    pub fn summary(&self) -> Value {
        let status = self.state();
        // Keep every individual summary bounded, even with very large argv.
        let command: String = self
            .meta
            .spec
            .argv
            .first()
            .map(String::as_str)
            .unwrap_or("")
            .chars()
            .take(256)
            .collect();
        json!({"id": self.meta.id, "command": command, "state":status.state,
            "exitCode":status.exit_code,"signal":status.signal,
            "startedAtMs":status.started_at,"finishedAtMs":status.finished_at,
            "ageMs":shim::now_ms().saturating_sub(status.started_at),
            "durationMs":status.finished_at.map(|end| end.saturating_sub(status.started_at)),
            "lossy":status.stdout.lossy || status.stderr.lossy,
            "stdoutLossy":status.stdout.lossy,"stderrLossy":status.stderr.lossy,
            "stdoutTruncated":status.stdout.truncated,"stderrTruncated":status.stderr.truncated})
    }

    pub fn read_stream(&self, stream: &str, offset: u64, limit: usize) -> io::Result<Value> {
        let status = self.state();
        let truncated = if stream == "stderr" {
            status.stderr.truncated
        } else {
            status.stdout.truncated
        };
        capture::read_stream(&self.dir, stream, offset, limit, truncated)
    }

    pub fn detail(&self) -> Value {
        let mut value = self.summary();
        value["argv"] = json!(self.meta.spec.argv);
        value["cwd"] = json!(self.meta.spec.cwd);
        for stream in ["stdout", "stderr"] {
            match self.read_stream(stream, 0, capture::READ_LIMIT) {
                Ok(read) => {
                    value[stream] = read["text"].clone();
                    value[format!("{stream}Lossy")] = read["lossy"].clone();
                    value[format!("{stream}SpillPath")] = read["spillPath"].clone();
                }
                Err(error) => {
                    value["error"] = json!(error.to_string());
                }
            }
        }
        value["lossy"] = json!(value["stdoutLossy"] == true || value["stderrLossy"] == true);
        value
    }

    pub fn record(&self) -> io::Result<Record> {
        let status = self.state();
        let read = |stream: &str| -> io::Result<(String, bool)> {
            let bytes = match fs::read(self.dir.join(format!("{stream}.log"))) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
                Err(error) => return Err(error),
            };
            let text = String::from_utf8_lossy(&bytes);
            let invalid = matches!(&text, std::borrow::Cow::Owned(_));
            Ok((text.into_owned(), invalid))
        };
        let (stdout, out_invalid) = read("stdout")?;
        let (stderr, err_invalid) = read("stderr")?;
        let stdout_lossy = status.stdout.lossy || status.stdout.truncated || out_invalid;
        let stderr_lossy = status.stderr.lossy || status.stderr.truncated || err_invalid;
        Ok(Record {
            id: self.meta.id.clone(),
            argv: serde_json::to_string(&self.meta.spec.argv)?,
            cwd: self.meta.spec.cwd.clone(),
            started_at: status.started_at,
            finished_at: status.finished_at,
            state: status.state,
            exit_code: status.exit_code.map(i64::from),
            signal: status.signal.map(i64::from),
            stdout,
            stderr,
            lossy: stdout_lossy || stderr_lossy,
            stdout_lossy,
            stderr_lossy,
            stdout_truncated: status.stdout.truncated,
            stderr_truncated: status.stderr.truncated,
        })
    }

    pub fn persist(&self, store: &Store) -> Result<(), String> {
        store.save(&self.record().map_err(|error| error.to_string())?)?;
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }
}

pub fn admit(meta: Meta, state: &Path, store: &Store) -> io::Result<Arc<Execution>> {
    let dir = state.join(&meta.id);
    fs::create_dir(&dir)?;
    shim::atomic_json(&dir.join("meta.json"), &meta)?;
    let status = Status {
        state: "running".into(),
        pid: None,
        pgid: None,
        process_start_time: None,
        shim_pid: 0,
        shim_start_time: 0,
        started_at: shim::now_ms(),
        finished_at: None,
        exit_code: None,
        signal: None,
        stdout: StreamStatus::default(),
        stderr: StreamStatus::default(),
        error: None,
    };
    let execution = Arc::new(Execution {
        meta,
        dir,
        status: Mutex::new(status),
        dirty: AtomicBool::new(false),
        announced: AtomicBool::new(false),
    });
    if let Err(error) = execution.persist(store) {
        // No child has been spawned. Remove only the admission metadata we own
        // so a corrected request can retry the same execution id.
        fs::remove_file(execution.dir.join("meta.json"))?;
        fs::remove_dir(&execution.dir)?;
        return Err(io::Error::other(format!(
            "history admission write failed: {error}"
        )));
    }
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--mode")
        .arg("shim")
        .arg("--state")
        .arg(state)
        .arg("--id")
        .arg(&execution.meta.id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // SAFETY: detach the shim from executor's session and controlling terminal.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    match command.spawn() {
        Ok(mut child) => {
            std::thread::spawn(move || {
                let _ = child.wait();
            });
        }
        Err(error) => {
            let mut status = execution.status.lock().expect("execution status poisoned");
            status.state = "failed".into();
            status.finished_at = Some(shim::now_ms());
            status.error = Some(error.to_string());
            shim::atomic_json(&execution.dir.join("status.json"), &*status)?;
            execution.dirty.store(true, Ordering::Relaxed);
        }
    }
    Ok(execution)
}

pub fn load(dir: PathBuf) -> io::Result<Arc<Execution>> {
    let meta: Meta = shim::read_json(&dir.join("meta.json"))?;
    let mut status: Status = match shim::read_json(&dir.join("status.json")) {
        Ok(status) => status,
        Err(error) if error.kind() == io::ErrorKind::NotFound => Status {
            state: "lost".into(),
            pid: None,
            pgid: None,
            process_start_time: None,
            shim_pid: 0,
            shim_start_time: 0,
            started_at: shim::now_ms(),
            finished_at: Some(shim::now_ms()),
            exit_code: None,
            signal: None,
            stdout: StreamStatus::default(),
            stderr: StreamStatus::default(),
            error: Some("admission interrupted before shim status".into()),
        },
        Err(error) => return Err(error),
    };
    if status.state == "running" && !shim::is_live(&status) {
        let latest: Status = shim::read_json(&dir.join("status.json"))?;
        status = latest;
        if status.state == "running" && !shim::is_live(&status) {
            status.state = "lost".into();
            status.finished_at = Some(shim::now_ms());
            status.error = Some("recorded process identity no longer exists".into());
        }
    }
    Ok(Arc::new(Execution {
        meta,
        dir,
        status: Mutex::new(status),
        dirty: AtomicBool::new(true),
        announced: AtomicBool::new(false),
    }))
}
