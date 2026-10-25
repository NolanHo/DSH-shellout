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

/// The admission wait, shortened by a test seam so the timeout path is testable
/// without delaying `execve` for the production five seconds.
fn spawn_grace() -> Duration {
    std::env::var("DSH_SHELLOUT_SPAWN_GRACE_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map_or(SPAWN_GRACE, Duration::from_millis)
}

/// What admission decided about one execution.
pub struct Admission {
    pub execution: Arc<Execution>,
    pub outcome: SpawnOutcome,
}

/// The spawn outcome the `exec` reply and history must agree about.
pub enum SpawnOutcome {
    /// The shim reported the command's pid; the execution is live.
    Admitted,
    /// The executor published `go`, but the shim had not reported the pid when
    /// the wait expired. The command may already be starting, so the reply
    /// names the execution and its outcome arrives as a settlement.
    Accepted,
    /// The command can never start. `tracked` keeps a pre-verdict timeout
    /// addressable for its settlement; a reported spawn error has no files left.
    Refused { message: String, tracked: bool },
}

/// The binary to run as a shim: the running image, never the path it was
/// started from. Deployment replaces the file in place, which unlinks the old
/// inode; `/proc/self/exe` addresses the image this process is already running.
fn shim_program() -> io::Result<PathBuf> {
    let proc_self = PathBuf::from("/proc/self/exe");
    if proc_self.exists() {
        Ok(proc_self)
    } else {
        std::env::current_exe()
    }
}

/// The spill files this executor created, with the policy that reclaims them.
///
/// A spill is host-owned storage for one complete stream, so it is not deleted
/// with the state directory. Nothing else would ever remove it: the host only
/// reads the path. The executor therefore keeps the inventory and deletes a
/// spill once it is older than the retention window, or oldest-first when the
/// total exceeds the budget, so a host spill directory stays bounded.
pub struct SpillBook {
    records: Mutex<Vec<SpillRecord>>,
    retention: Duration,
    budget: u64,
}

struct SpillRecord {
    path: PathBuf,
    bytes: u64,
    created: Instant,
}

impl SpillBook {
    pub fn new(retention: Duration, budget: u64) -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            retention,
            budget,
        }
    }

    fn record(&self, path: PathBuf, bytes: u64) {
        let mut records = self.records.lock().expect("spill records poisoned");
        if !records.iter().any(|record| record.path == path) {
            records.push(SpillRecord {
                path,
                bytes,
                created: Instant::now(),
            });
        }
    }

    /// Delete the spills this executor owns that outlived the retention window
    /// or no longer fit the budget. Called from the executor's sweep.
    pub fn prune(&self) {
        let mut records = self.records.lock().expect("spill records poisoned");
        if records.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut ordered: Vec<SpillRecord> = std::mem::take(&mut *records);
        // Newest first: the newest spills are the ones a just-finished result
        // is most likely to reference, so the budget evicts the oldest.
        ordered.sort_by_key(|record| std::cmp::Reverse(record.created));
        let mut total = 0u64;
        let mut kept = Vec::with_capacity(ordered.len());
        for record in ordered {
            let fresh = now.duration_since(record.created) < self.retention;
            if fresh && total.saturating_add(record.bytes) <= self.budget {
                total += record.bytes;
                kept.push(record);
                continue;
            }
            match fs::remove_file(&record.path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    eprintln!("spill {}: cannot remove: {error}", record.path.display());
                    // Keep it on the inventory so a later sweep retries.
                    kept.push(record);
                }
            }
        }
        *records = kept;
    }
}

pub struct Execution {
    pub meta: Meta,
    pub dir: PathBuf,
    status: Mutex<Status>,
    pub dirty: AtomicBool,
    pub announced: AtomicBool,
    cleanup_at: Mutex<Option<Instant>>,
    removed: AtomicBool,
    /// Set once the terminal capture's spill decision is final, so a pruned
    /// spill is not recreated by the next poll.
    spills_ready: AtomicBool,
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

    /// The host-owned spill path for one stream; `None` when the spec names none.
    fn spill_path(&self, stream: &str) -> Option<PathBuf> {
        let directory = self.meta.spec.capture.spill_dir.trim();
        (!directory.is_empty())
            .then(|| Path::new(directory).join(format!("{}-{stream}.log", self.meta.id)))
    }

    /// Link the finished capture into the host's spill directory.
    ///
    /// The state directory is a working copy that retention deletes; the spill
    /// is the path a reader (and the model) was told to open, so it must outlive
    /// it. Only a stream that outgrew the host's in-memory cap needs one: the
    /// host already retains a smaller capture in its own mirror, so an empty or
    /// small stream leaves no file behind. A hard link keeps the bytes without
    /// copying; a cross-device spill copies through a temporary file and renames
    /// it, so the published path only ever names a complete capture.
    pub fn ensure_spills(&self, book: &SpillBook) {
        let status = self.state();
        // A settled capture's spill decision is final: retention may delete the
        // file later, and a poll must not recreate it.
        if status.state == "running" || self.spills_ready.load(Ordering::Relaxed) {
            return;
        }
        let mut settled = true;
        for (stream, facts, cap) in [
            (
                "stdout",
                &status.stdout,
                self.meta.spec.capture.stdout_max_bytes,
            ),
            (
                "stderr",
                &status.stderr,
                self.meta.spec.capture.stderr_max_bytes,
            ),
        ] {
            if facts.truncated || facts.bytes <= cap as u64 {
                continue;
            }
            let Some(target) = self.spill_path(stream) else {
                continue;
            };
            // A file of the wrong size is the residue of an interrupted copy,
            // not a complete capture: replace it instead of serving it.
            if let Ok(metadata) = fs::metadata(&target) {
                if metadata.len() == facts.bytes {
                    book.record(target, metadata.len());
                    continue;
                }
                if let Err(error) = fs::remove_file(&target) {
                    if error.kind() != io::ErrorKind::NotFound {
                        eprintln!(
                            "execution {}: cannot replace the partial {stream} spill: {error}",
                            self.meta.id
                        );
                        settled = false;
                        continue;
                    }
                }
            }
            if let Some(parent) = target.parent() {
                if let Err(error) = fs::create_dir_all(parent) {
                    eprintln!(
                        "execution {}: cannot create the spill directory: {error}",
                        self.meta.id
                    );
                    settled = false;
                    continue;
                }
            }
            let source = self.dir.join(format!("{stream}.log"));
            if fs::hard_link(&source, &target).is_err() {
                if let Err(error) = copy_spill(&source, &target) {
                    if error.kind() != io::ErrorKind::NotFound {
                        eprintln!(
                            "execution {}: cannot write the {stream} spill: {error}",
                            self.meta.id
                        );
                        settled = false;
                    }
                    continue;
                }
            }
            match fs::metadata(&target) {
                Ok(metadata) if metadata.len() == facts.bytes => {
                    book.record(target, metadata.len());
                }
                Ok(metadata) => {
                    eprintln!(
                        "execution {}: the {stream} spill is {} bytes, expected {}",
                        self.meta.id,
                        metadata.len(),
                        facts.bytes
                    );
                    settled = false;
                }
                Err(error) => {
                    eprintln!(
                        "execution {}: cannot size the {stream} spill: {error}",
                        self.meta.id
                    );
                    settled = false;
                }
            }
        }
        if settled {
            self.spills_ready.store(true, Ordering::Relaxed);
        }
    }

    pub fn read_stream(
        &self,
        stream: &str,
        offset: u64,
        limit: usize,
        book: &SpillBook,
    ) -> io::Result<Value> {
        let status = self.state();
        let truncated = if stream == "stderr" {
            status.stderr.truncated
        } else {
            status.stdout.truncated
        };
        if status.state != "running" {
            self.ensure_spills(book);
        }
        // Only an existing spill is a path a reader can rely on.
        let spill = self.spill_path(stream).filter(|path| path.exists());
        capture::read_stream(
            &self.dir,
            spill.as_deref(),
            stream,
            offset,
            limit,
            truncated,
        )
    }

    pub fn detail(&self, book: &SpillBook) -> Value {
        let mut value = self.summary();
        value["argv"] = json!(self.meta.spec.argv);
        value["cwd"] = json!(self.meta.spec.cwd);
        for stream in ["stdout", "stderr"] {
            match self.read_stream(stream, 0, capture::READ_LIMIT, book) {
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

/// Copy a capture into the spill directory atomically.
///
/// The destination is written beside the target under a temporary name and
/// renamed into place, so a reader either sees no spill or the complete
/// capture — never a half-written file that reads as complete.
fn copy_spill(source: &Path, target: &Path) -> io::Result<()> {
    copy_spill_with(source, target, |from, to| fs::copy(from, to).map(|_| ()))
}

fn copy_spill_with(
    source: &Path,
    target: &Path,
    copy: impl FnOnce(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    let temporary = temporary_spill(target);
    let _ = fs::remove_file(&temporary);
    let result = copy(source, &temporary).and_then(|()| fs::rename(&temporary, target));
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// The sibling a copy writes before it is renamed onto `target`.
fn temporary_spill(target: &Path) -> PathBuf {
    let name = target
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    target.with_file_name(format!("{name}.tmp.{}", std::process::id()))
}

pub fn admit(meta: Meta, state: &Path, store: &Store) -> io::Result<Admission> {
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
        spills_ready: AtomicBool::new(false),
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
    // The executor owns the spawn outcome unless a shim claimed it first. The
    // claim must exist before the shim starts, so a shim that runs late cannot
    // race the decision the caller was already given.
    let arbitration = shim::claim_spawn(&execution.dir, "executor")?;
    let mut command = Command::new(shim_program()?);
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
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(error) => {
            let message = persist_failure(&execution, store, &error.to_string());
            let _ = fs::remove_dir_all(&execution.dir);
            return Ok(Admission {
                execution,
                outcome: SpawnOutcome::Refused {
                    message,
                    tracked: false,
                },
            });
        }
    };
    let outcome = execution.await_spawn(store, arbitration)?;
    // The shim outlives admission: reap it without holding the caller.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(Admission { execution, outcome })
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
    ///
    /// When the shim reports its identity the executor releases the spawn with
    /// the one decision it owns. Until that decision a timeout is a refusal;
    /// after `go` the command may already be starting, so the executor answers
    /// with the execution and lets settlement carry the real outcome.
    fn await_spawn(&self, store: &Store, arbitration: bool) -> io::Result<SpawnOutcome> {
        let path = self.dir.join("status.json");
        let grace = spawn_grace();
        let mut deadline = Instant::now() + grace;
        let mut authorized = false;
        loop {
            match shim::read_json::<Status>(&path) {
                Ok(status) => {
                    if status.spawn_error.is_some() {
                        // A published spawn error proves the command never
                        // started, so failing the reply cannot duplicate it.
                        let message = persist_failure(
                            self,
                            store,
                            status.spawn_error.as_deref().unwrap_or("spawn failed"),
                        );
                        let _ = fs::remove_dir_all(&self.dir);
                        return Ok(SpawnOutcome::Refused {
                            message,
                            tracked: false,
                        });
                    }
                    if status.pid.is_some() {
                        return Ok(SpawnOutcome::Admitted);
                    }
                    if arbitration && status.shim_pid != 0 && !authorized {
                        // The shim is alive and waiting for the one decision we
                        // own; the command may start. It gets its own window to
                        // report a pid or a spawn failure.
                        shim::decide_spawn(&self.dir, shim::VERDICT_GO)?;
                        authorized = true;
                        deadline = Instant::now() + grace;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("execution {}: status read failed: {error}", self.meta.id),
            }
            if Instant::now() >= deadline {
                if arbitration && !authorized {
                    // Nothing was authorized, so no command can start. The
                    // durable refusal also stops a shim that starts even later.
                    shim::decide_spawn(&self.dir, shim::VERDICT_FAIL)?;
                    let message =
                        persist_failure(self, store, "the shim did not report a spawn outcome");
                    // Keep the execution: the caller was told the spawn failed,
                    // and the failure must stay addressable.
                    return Ok(SpawnOutcome::Refused {
                        message,
                        tracked: true,
                    });
                }
                // `go` is published (or another party arbitrates this spawn),
                // so the command may run after this wait expires. Never answer
                // a refusal here: the host would retry a command that already
                // has side effects. The settlement carries the real outcome.
                return Ok(SpawnOutcome::Accepted);
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
        spills_ready: AtomicBool::new(false),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_interrupted_spill_copy_never_publishes_a_partial_file() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/spill-atomic");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("scratch");
        let source = root.join("stdout.log");
        let bytes: Vec<u8> = (0..4096).map(|index| b'a' + (index % 26) as u8).collect();
        fs::write(&source, &bytes).expect("source");
        let target = root.join("execution-stdout.log");

        let error = copy_spill_with(&source, &target, |from, to| {
            let half = fs::read(from)?;
            fs::write(to, &half[..half.len() / 2])?;
            Err(io::Error::other("copy interrupted"))
        })
        .expect_err("an interrupted copy is reported");
        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert!(
            !target.exists(),
            "a reader must never see an interrupted copy as the complete spill"
        );
        assert!(
            !temporary_spill(&target).exists(),
            "an interrupted copy leaves no temporary file"
        );

        copy_spill(&source, &target).expect("the retry publishes the capture");
        assert_eq!(fs::read(&target).expect("spill"), bytes);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn the_spill_budget_evicts_the_oldest_files_first() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/spill-budget");
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("scratch");
        let book = SpillBook::new(Duration::from_secs(600), 10);
        let older = root.join("older-stdout.log");
        let newer = root.join("newer-stdout.log");
        fs::write(&older, b"123456").expect("older spill");
        book.record(older.clone(), 6);
        std::thread::sleep(Duration::from_millis(5));
        fs::write(&newer, b"abcdef").expect("newer spill");
        book.record(newer.clone(), 6);

        book.prune();
        assert!(!older.exists(), "the oldest spill is evicted first");
        assert!(newer.exists(), "the newest spill stays inside the budget");
        fs::remove_dir_all(root).expect("cleanup");
    }
}
