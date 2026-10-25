//! Executor-side file-backed execution handles. The shim owns all child I/O.
use std::fs;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::capture;
use crate::shim::{self, Meta, Status};
use crate::store::{Record, Store};

/// How long `exec` waits for the shim to report that the command started.
/// A spawn failure is a preparation failure, so it belongs in the exec reply.
const SPAWN_GRACE: Duration = Duration::from_secs(5);

pub struct Execution {
    pub meta: Meta,
    pub dir: PathBuf,
    status: Mutex<Status>,
    pub dirty: AtomicBool,
    pub announced: AtomicBool,
    cleanup_at: Mutex<Option<Instant>>,
    removed: AtomicBool,
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

    /// Ask the shim to terminate the group, naming the asking party so the
    /// terminal record can say who ended the execution.
    pub fn kill(&self, reason: &str) -> io::Result<bool> {
        if !self.running() {
            return Ok(false);
        }
        // A durable request, consumed by the shim; executor never owns signals.
        fs::write(self.dir.join("kill.request"), reason)?;
        Ok(true)
    }

    pub fn refresh(&self) {
        if !self.running() {
            return;
        }
        let previous = self.state();
        let next = match shim::read_status(&self.dir) {
            Ok(next) => next,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if !shim::starting(&previous, shim::now_ms()) {
                    self.lose("the shim never published its status");
                }
                return;
            }
            Err(error) => {
                eprintln!("execution {}: status read failed: {error}", self.meta.id);
                return;
            }
        };
        // Only terminal facts belong in history; a running row is already there.
        if next.state != "running" && next != previous {
            self.dirty.store(true, Ordering::Relaxed);
        }
        *self.status.lock().expect("execution status poisoned") = next;
    }

    /// Record a fact the shim never published: this execution is lost.
    fn lose(&self, reason: &str) {
        let mut status = self.status.lock().expect("execution status poisoned");
        status.state = "lost".into();
        status.finished_at = Some(shim::now_ms());
        status.error = Some(reason.into());
        self.dirty.store(true, Ordering::Relaxed);
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

    /// Terminal facts other processes need to settle this execution without an
    /// event: whether it still runs, and how it ended.
    pub fn terminal_facts(&self) -> Value {
        let status = self.state();
        json!({"executionId":self.meta.id,"state":status.state,
            "exitCode":status.exit_code,"signal":status.signal,
            "termination":status.termination,
            "detail":status.error})
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

    /// Delete the execution's files once its terminal row is durable and the
    /// reads that follow a settlement had their window.
    pub fn schedule_cleanup(&self, retention: Duration) {
        let mut at = self.cleanup_at.lock().expect("cleanup poisoned");
        if at.is_none() {
            *at = Some(Instant::now() + retention);
        }
    }

    pub fn cleanup_due(&self) -> bool {
        self.cleanup_at
            .lock()
            .expect("cleanup poisoned")
            .is_some_and(|at| at <= Instant::now())
    }

    /// True while the files a settlement refers to are still readable.
    pub fn retained(&self) -> bool {
        self.cleanup_at.lock().expect("cleanup poisoned").is_some()
            && !self.removed.load(Ordering::Relaxed)
    }

    pub fn remove_state(&self) -> io::Result<()> {
        if self.removed.swap(true, Ordering::Relaxed) {
            return Ok(());
        }
        match fs::remove_dir_all(&self.dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => {
                self.removed.store(false, Ordering::Relaxed);
                Err(error)
            }
        }
    }
}

pub fn admit(meta: Meta, state: &Path, store: &Store) -> io::Result<Arc<Execution>> {
    let dir = state.join(&meta.id);
    fs::create_dir(&dir)?;
    shim::atomic_json(&dir.join("meta.json"), &meta)?;
    let execution = Arc::new(Execution {
        meta,
        dir,
        status: Mutex::new(Status::admitted(shim::now_ms())),
        dirty: AtomicBool::new(false),
        announced: AtomicBool::new(false),
        cleanup_at: Mutex::new(None),
        removed: AtomicBool::new(false),
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
            let message = persist_failure(&execution, store, &error.to_string());
            let _ = fs::remove_dir_all(&execution.dir);
            return Err(io::Error::other(message));
        }
    }
    execution.await_spawn(store)?;
    Ok(execution)
}

/// Record a shim that never started as a settled failure, then report it.
fn persist_failure(execution: &Execution, store: &Store, error: &str) -> String {
    {
        let mut status = execution.status.lock().expect("execution status poisoned");
        status.state = "failed".into();
        status.finished_at = Some(shim::now_ms());
        status.error = Some(error.into());
        status.spawn_error = Some(error.into());
    }
    execution.dirty.store(true, Ordering::Relaxed);
    match execution.persist(store) {
        Ok(()) => format!("cannot start execution: {error}"),
        Err(persist) => format!("cannot start execution: {error} (history: {persist})"),
    }
}

impl Execution {
    /// The command is spawned by the shim, so the admission result is the
    /// shim's report: a recorded pid, or why it could not start one.
    fn await_spawn(&self, store: &Store) -> io::Result<()> {
        let path = self.dir.join("status.json");
        let deadline = Instant::now() + SPAWN_GRACE;
        loop {
            match shim::read_json::<Status>(&path) {
                Ok(status) => {
                    if status.spawn_error.is_some() {
                        let message = persist_failure(
                            self,
                            store,
                            status.spawn_error.as_deref().unwrap_or("spawn failed"),
                        );
                        let _ = fs::remove_dir_all(&self.dir);
                        return Err(io::Error::other(message));
                    }
                    if status.pid.is_some() {
                        return Ok(());
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("execution {}: status read failed: {error}", self.meta.id),
            }
            if Instant::now() >= deadline {
                // Nothing is known about the command; make sure it cannot keep
                // running unattended while the caller is told admission failed.
                let _ = fs::write(self.dir.join("kill.request"), shim::TERMINATION_REQUEST);
                return Err(io::Error::other("the shim did not report a spawn outcome"));
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// An admitted execution whose shim has not published a status yet. The shim
/// writes its identity as its first act, so this is a starting execution, not
/// evidence that it was lost.
fn unpublished(dir: &Path) -> Status {
    let started_at = fs::metadata(dir)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| modified.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or_else(shim::now_ms, |elapsed| elapsed.as_millis() as u64);
    Status::admitted(started_at)
}

/// Only a terminal fact the row does not carry yet needs a rewrite: restarting
/// the executor must not rewrite the whole history.
fn needs_persist(store: Option<&Store>, id: &str, status: &Status) -> bool {
    let Some(store) = store else {
        return true;
    };
    match store.facts(id) {
        Ok(Some(facts)) => {
            facts.state != status.state
                || facts.exit_code != status.exit_code.map(i64::from)
                || facts.signal != status.signal.map(i64::from)
                || facts.finished_at != status.finished_at.map(|at| at as i64)
        }
        Ok(None) => true,
        Err(error) => {
            eprintln!("execution {id}: history lookup failed: {error}");
            true
        }
    }
}

pub fn load(dir: PathBuf, store: Option<&Store>) -> io::Result<Arc<Execution>> {
    let meta: Meta = shim::read_json(&dir.join("meta.json"))?;
    let status = match shim::read_status(&dir) {
        Ok(status) => status,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let mut status = unpublished(&dir);
            shim::reconcile(&mut status, shim::now_ms());
            status
        }
        Err(error) => return Err(error),
    };
    let dirty = needs_persist(store, &meta.id, &status);
    Ok(Arc::new(Execution {
        meta,
        dir,
        status: Mutex::new(status),
        dirty: AtomicBool::new(dirty),
        announced: AtomicBool::new(false),
        cleanup_at: Mutex::new(None),
        removed: AtomicBool::new(false),
    }))
}
