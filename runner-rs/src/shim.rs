//! Per-execution supervisor. No executor-owned pipes, timers or child lifetime.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::protocol::SpawnSpec;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Meta {
    pub id: String,
    pub operation_id: String,
    pub digest: String,
    pub boot_id: String,
    #[serde(flatten)]
    pub spec: SpawnSpec,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct StreamStatus {
    pub bytes: u64,
    pub truncated: bool,
    pub lossy: bool,
}

/// Why an execution was terminated, in the vocabulary of the party that asked
/// for it: its own deadline, an explicit request, lease expiry, or a drain.
pub const TERMINATION_DEADLINE: &str = "deadline";
pub const TERMINATION_REQUEST: &str = "request";
pub const TERMINATION_LEASE: &str = "lease";
pub const TERMINATION_DRAIN: &str = "drain";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub state: String,
    pub pid: Option<i32>,
    pub pgid: Option<i32>,
    pub process_start_time: Option<u64>,
    pub shim_pid: i32,
    pub shim_start_time: u64,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub stdout: StreamStatus,
    pub stderr: StreamStatus,
    pub error: Option<String>,
    /// Set by the shim when it terminated the group on request (`kill.request`)
    /// and not because the command exited on its own.
    #[serde(default)]
    pub termination: Option<String>,
    /// Why the command could not be started; its presence is the spawn failure.
    #[serde(default)]
    pub spawn_error: Option<String>,
}

impl Status {
    /// A freshly admitted execution: the executor knows the id, no shim yet.
    pub fn admitted(started_at: u64) -> Self {
        Self {
            state: "running".into(),
            pid: None,
            pgid: None,
            process_start_time: None,
            shim_pid: 0,
            shim_start_time: 0,
            started_at,
            finished_at: None,
            exit_code: None,
            signal: None,
            stdout: StreamStatus::default(),
            stderr: StreamStatus::default(),
            error: None,
            termination: None,
            spawn_error: None,
        }
    }
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn atomic_json(path: &Path, value: &impl Serialize) -> io::Result<()> {
    let temp = path.with_extension("json.tmp");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(&temp)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(temp, path)
}

pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
    Ok(serde_json::from_reader(File::open(path)?)?)
}

/// `comm` can contain spaces and closing parentheses: split after its last `)`.
pub fn process_start_time(pid: i32) -> io::Result<u64> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let rest = stat
        .rsplit_once(')')
        .ok_or_else(|| io::Error::other("bad proc stat"))?
        .1;
    rest.split_whitespace()
        .nth(19)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::other("missing proc start time"))
}

pub fn matches_process(pid: i32, start: u64) -> bool {
    pid > 0 && process_start_time(pid).is_ok_and(|actual| actual == start)
}

/// A shim publishes its identity as its first act. Until it does, an execution
/// has no recorded supervisor to verify, so it is neither live nor lost.
pub const STARTUP_GRACE_MS: u64 = 10_000;

/// The supervisor owns the command's lifetime, so an execution is live exactly
/// while its shim is: the shim may already have reaped the direct child and
/// still be terminating the group and draining the capture pipes.
pub fn supervisor_alive(status: &Status) -> bool {
    matches_process(status.shim_pid, status.shim_start_time)
}

pub fn starting(status: &Status, now: u64) -> bool {
    status.state == "running"
        && status.shim_pid == 0
        && now < status.started_at.saturating_add(STARTUP_GRACE_MS)
}

/// Turn a `running` record into `lost` only when nothing can ever update it.
/// Returns whether the record changed. Both startup loading and the running
/// poll use this, so the two can never disagree about the same file.
pub fn reconcile(status: &mut Status, now: u64) -> bool {
    if status.state != "running" || starting(status, now) || supervisor_alive(status) {
        return false;
    }
    status.state = "lost".into();
    status.finished_at = Some(now);
    status.error = Some(
        if status.shim_pid == 0 {
            "the shim never published its status"
        } else {
            "the recorded shim process no longer exists"
        }
        .into(),
    );
    true
}

/// Read the published status, reconciling a stale `running` record.
///
/// A shim that exits right after writing its terminal status makes a stale read
/// look lost, so the read is repeated once before that conclusion stands.
pub fn read_status(dir: &Path) -> io::Result<Status> {
    let path = dir.join("status.json");
    let mut status: Status = read_json(&path)?;
    if status.state == "running" && !starting(&status, now_ms()) && !supervisor_alive(&status) {
        if let Ok(latest) = read_json::<Status>(&path) {
            status = latest;
        }
        reconcile(&mut status, now_ms());
    }
    Ok(status)
}

/// The reason recorded in `kill.request`, or `None` when nothing asked for a
/// termination. An empty request file (older executors, tests) is a plain request.
pub fn kill_request(dir: &Path) -> Option<String> {
    let text = fs::read_to_string(dir.join("kill.request")).ok()?;
    Some(
        match text.trim() {
            TERMINATION_DEADLINE => TERMINATION_DEADLINE,
            TERMINATION_LEASE => TERMINATION_LEASE,
            TERMINATION_DRAIN => TERMINATION_DRAIN,
            _ => TERMINATION_REQUEST,
        }
        .to_string(),
    )
}

fn signal_group(pgid: i32, signal: i32) {
    // SAFETY: only this shim's process group is addressed, and only while its
    // leader is still unreaped so the group id cannot have been recycled.
    unsafe {
        libc::kill(-pgid, signal);
    }
}

fn pump(
    mut input: impl Read + Send + 'static,
    mut file: File,
    cap: u64,
    facts: Arc<Mutex<StreamStatus>>,
) -> thread::JoinHandle<io::Result<()>> {
    thread::spawn(move || {
        let mut buffer = [0; 16 * 1024];
        let mut retained = 0u64;
        loop {
            let count = match input.read(&mut buffer) {
                Ok(0) => return Ok(()),
                Ok(count) => count,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            };
            let keep = (cap.saturating_sub(retained)).min(count as u64) as usize;
            let result = file.write_all(&buffer[..keep]);
            let mut status = facts
                .lock()
                .map_err(|_| io::Error::other("stream status poisoned"))?;
            status.bytes += count as u64;
            status.truncated |= keep < count || result.is_err();
            status.lossy |= status.truncated;
            result?;
            retained += keep as u64;
        }
    })
}

/// Facts about how the command ended, observed without reaping it.
struct ExitFacts {
    code: Option<i32>,
    signal: Option<i32>,
}

/// Observe the command's exit without reaping it.
///
/// The zombie keeps the leader's pid — and therefore the process group id —
/// reserved, so every later signal addresses this execution's group and can
/// never reach a recycled one.
fn try_wait_no_reap(pid: i32) -> io::Result<Option<ExitFacts>> {
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: `info` is a valid siginfo_t and the pid is this shim's child.
    let result = unsafe {
        libc::waitid(
            libc::P_PID,
            pid as libc::id_t,
            &mut info,
            libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
        )
    };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: waitid succeeded, so the siginfo fields are initialized.
    if unsafe { info.si_pid() } == 0 {
        return Ok(None);
    }
    Ok(Some(match info.si_code {
        libc::CLD_EXITED => ExitFacts {
            code: Some(unsafe { info.si_status() }),
            signal: None,
        },
        _ => ExitFacts {
            code: None,
            signal: Some(unsafe { info.si_status() }),
        },
    }))
}

pub fn run(state: &Path, id: &str) -> io::Result<()> {
    let dir = state.join(id);
    let mut meta: Meta = read_json(&dir.join("meta.json"))?;
    let pid = std::process::id() as i32;
    let mut status = Status {
        state: "running".into(),
        pid: None,
        pgid: None,
        process_start_time: None,
        shim_pid: pid,
        shim_start_time: process_start_time(pid)?,
        started_at: now_ms(),
        finished_at: None,
        exit_code: None,
        signal: None,
        stdout: StreamStatus::default(),
        stderr: StreamStatus::default(),
        error: None,
        termination: None,
        spawn_error: None,
    };
    // First act, before any child exists: the executor can tell a starting
    // shim from a dead one by this record alone.
    atomic_json(&dir.join("status.json"), &status)?;
    let result = supervise(&dir, &meta.spec, &mut status);
    if let Err(error) = &result {
        status.state = "failed".into();
        status.error = Some(error.to_string());
    }
    status.finished_at = Some(now_ms());
    // The command's environment is not terminal information and may hold secrets.
    meta.spec.env.clear();
    meta.spec.stdin = None;
    if let Err(error) = atomic_json(&dir.join("meta.json"), &meta) {
        eprintln!("shim {id}: meta scrub failed: {error}");
    }
    // Never exit before terminal facts are published, including temporary I/O failures.
    loop {
        match atomic_json(&dir.join("status.json"), &status) {
            Ok(()) => break,
            Err(error) => {
                eprintln!("shim {id}: terminal status write failed: {error}");
                thread::sleep(Duration::from_millis(250));
            }
        }
    }
    result
}

/// Rewrite the running record only when the published facts changed, so a quiet
/// execution does not replace `status.json` twice a second.
fn publish_running(dir: &Path, status: &Status, published: &mut Option<Status>) -> io::Result<()> {
    if published.as_ref() == Some(status) {
        return Ok(());
    }
    atomic_json(&dir.join("status.json"), status)?;
    *published = Some(status.clone());
    Ok(())
}

fn supervise(dir: &Path, spec: &SpawnSpec, status: &mut Status) -> io::Result<()> {
    let program = spec
        .argv
        .first()
        .ok_or_else(|| io::Error::other("empty argv"))?;
    let open = |stream: &str| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(dir.join(format!("{stream}.log")))
    };
    let stdout = open("stdout")?;
    let stderr = open("stderr")?;
    let mut command = Command::new(program);
    command
        .args(&spec.argv[1..])
        .current_dir(&spec.cwd)
        .env_clear()
        .envs(&spec.env)
        .stdin(if spec.stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: setsid is async-signal-safe; the command owns a new process group.
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
            status.spawn_error = Some(format!("{program}: {error}"));
            return Err(io::Error::other(format!("cannot start {program}: {error}")));
        }
    };
    let pgid = child.id() as i32;
    status.pid = Some(pgid);
    status.pgid = Some(pgid);
    let start = match process_start_time(pgid) {
        Ok(start) => start,
        Err(error) => {
            signal_group(pgid, libc::SIGKILL);
            let _ = child.wait();
            return Err(error);
        }
    };
    status.process_start_time = Some(start);
    if let Err(error) = atomic_json(&dir.join("status.json"), status) {
        signal_group(pgid, libc::SIGKILL);
        let _ = child.wait();
        return Err(error);
    }
    let out = Arc::new(Mutex::new(StreamStatus::default()));
    let err = Arc::new(Mutex::new(StreamStatus::default()));
    let cap = |budget: usize| {
        if spec.capture.spill_max_bytes > 0 {
            spec.capture.spill_max_bytes
        } else {
            budget as u64
        }
    };
    let out_thread = pump(
        child.stdout.take().expect("piped stdout"),
        stdout,
        cap(spec.capture.stdout_max_bytes),
        Arc::clone(&out),
    );
    let err_thread = pump(
        child.stderr.take().expect("piped stderr"),
        stderr,
        cap(spec.capture.stderr_max_bytes),
        Arc::clone(&err),
    );
    if let Some(input) = spec.stdin.clone() {
        let mut stdin = child.stdin.take().expect("piped stdin");
        thread::spawn(move || {
            let _ = stdin.write_all(input.as_bytes());
        });
    }
    let mut published = Some(status.clone());
    let outcome = wait_for_exit(dir, spec, status, &out, &err, &mut published);
    let outcome = match outcome {
        Ok(facts) => facts,
        Err(error) => {
            // The leader is still unreaped, so this addresses our group only.
            signal_group(pgid, libc::SIGKILL);
            let _ = child.wait();
            return Err(error);
        }
    };
    // The direct child's exit closes the execution scope; descendants must not
    // hold capture pipes or survive their supervisor's terminal publication.
    signal_group(pgid, libc::SIGTERM);
    let deadline = Instant::now() + Duration::from_millis(spec.grace_ms);
    while Instant::now() < deadline && !(out_thread.is_finished() && err_thread.is_finished()) {
        // A late termination request escalates immediately.
        if kill_request(dir).is_some() {
            signal_group(pgid, libc::SIGKILL);
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    signal_group(pgid, libc::SIGKILL);
    collect_streams(status, &out, &err);
    // A descendant that left the group keeps its copy of the pipe open forever.
    // Waiting for it would stall the terminal publication, so the join is bounded
    // and an unfinished stream is reported as lossy.
    let joined = out_thread.is_finished() && err_thread.is_finished();
    if joined {
        out_thread
            .join()
            .map_err(|_| io::Error::other("stdout reader panicked"))??;
        err_thread
            .join()
            .map_err(|_| io::Error::other("stderr reader panicked"))??;
        collect_streams(status, &out, &err);
    } else {
        status.stdout.lossy = true;
        status.stderr.lossy = true;
    }
    status.exit_code = outcome.code;
    status.signal = outcome.signal;
    status.state = if status.termination.is_some() {
        "killed"
    } else {
        "exited"
    }
    .into();
    // Reap last: nothing below signals the group id again.
    let _ = child.wait();
    Ok(())
}

fn collect_streams(
    status: &mut Status,
    out: &Arc<Mutex<StreamStatus>>,
    err: &Arc<Mutex<StreamStatus>>,
) {
    if let Ok(facts) = out.lock() {
        status.stdout = facts.clone();
    }
    if let Ok(facts) = err.lock() {
        status.stderr = facts.clone();
    }
}

fn wait_for_exit(
    dir: &Path,
    spec: &SpawnSpec,
    status: &mut Status,
    out: &Arc<Mutex<StreamStatus>>,
    err: &Arc<Mutex<StreamStatus>>,
    published: &mut Option<Status>,
) -> io::Result<ExitFacts> {
    let pid = status.pid.expect("spawned");
    let started = Instant::now();
    let mut terminating: Option<Instant> = None;
    let mut publication = Instant::now() - Duration::from_secs(1);
    loop {
        if let Some(facts) = try_wait_no_reap(pid)? {
            return Ok(facts);
        }
        if terminating.is_none() {
            let reason = kill_request(dir).or_else(|| {
                (spec.on_expiry == "kill"
                    && started.elapsed() >= Duration::from_millis(spec.timeout_ms))
                .then(|| TERMINATION_DEADLINE.to_string())
            });
            if let Some(reason) = reason {
                status.termination = Some(reason);
                terminating = Some(Instant::now());
                signal_group(pid, libc::SIGTERM);
            }
        }
        if terminating.is_some_and(|at| at.elapsed() >= Duration::from_millis(spec.grace_ms)) {
            signal_group(pid, libc::SIGKILL);
        }
        if publication.elapsed() >= Duration::from_millis(200) {
            collect_streams(status, out, err);
            if let Err(error) = publish_running(dir, status, published) {
                eprintln!(
                    "shim {}: running status write failed: {error}",
                    dir.display()
                );
            }
            publication = Instant::now();
        }
        thread::sleep(Duration::from_millis(20));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adoption_requires_matching_start_time() {
        let pid = std::process::id() as i32;
        let start = process_start_time(pid).expect("proc");
        assert!(matches_process(pid, start));
        assert!(!matches_process(pid, start + 1));
        assert!(!matches_process(i32::MAX, start));
        assert!(!matches_process(0, 0));
    }

    #[test]
    fn an_unpublished_shim_is_only_lost_after_the_grace() {
        let mut status = Status::admitted(1_000);
        assert!(starting(&status, 1_000 + STARTUP_GRACE_MS - 1));
        assert!(!starting(&status, 1_000 + STARTUP_GRACE_MS));
        assert!(!reconcile(&mut status, 1_000 + STARTUP_GRACE_MS - 1));
        assert!(reconcile(&mut status, 1_000 + STARTUP_GRACE_MS));
        assert_eq!(status.state, "lost");
    }

    #[test]
    fn a_live_shim_keeps_an_execution_running_after_its_child_was_reaped() {
        let pid = std::process::id() as i32;
        let start = process_start_time(pid).expect("proc");
        let mut status = Status::admitted(1_000);
        status.shim_pid = pid;
        status.shim_start_time = start;
        // The direct child is long gone; the supervisor still owns the execution.
        status.pid = Some(1);
        status.process_start_time = Some(1);
        assert!(!reconcile(&mut status, now_ms()));
        assert_eq!(status.state, "running");

        // The same record with a supervisor that no longer exists is lost.
        status.shim_start_time = start + 1;
        assert!(reconcile(&mut status, now_ms()));
        assert_eq!(status.state, "lost");
    }

    #[test]
    fn a_kill_request_names_the_party_that_asked() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/kill-request");
        fs::create_dir_all(&dir).expect("directory");
        assert_eq!(kill_request(&dir), None);
        fs::write(dir.join("kill.request"), b"").expect("empty request");
        assert_eq!(kill_request(&dir).as_deref(), Some(TERMINATION_REQUEST));
        for reason in [TERMINATION_LEASE, TERMINATION_DRAIN, TERMINATION_DEADLINE] {
            fs::write(dir.join("kill.request"), reason).expect("request");
            assert_eq!(kill_request(&dir).as_deref(), Some(reason));
        }
        fs::write(dir.join("kill.request"), "something else").expect("request");
        assert_eq!(kill_request(&dir).as_deref(), Some(TERMINATION_REQUEST));
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn shim_status_round_trip() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/status-roundtrip");
        fs::create_dir_all(&dir).expect("directory");
        let status = Status {
            state: "killed".into(),
            pid: Some(123),
            pgid: Some(123),
            process_start_time: Some(456),
            shim_pid: 789,
            shim_start_time: 42,
            started_at: 100,
            finished_at: Some(200),
            exit_code: None,
            signal: Some(9),
            stdout: StreamStatus {
                bytes: 99,
                truncated: true,
                lossy: true,
            },
            stderr: StreamStatus::default(),
            error: None,
            termination: Some(TERMINATION_LEASE.into()),
            spawn_error: None,
        };
        atomic_json(&dir.join("status.json"), &status).expect("write");
        let actual: Status = read_json(&dir.join("status.json")).expect("read");
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(status).unwrap()
        );
        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn a_status_without_the_new_facts_still_loads() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/status-legacy");
        fs::create_dir_all(&dir).expect("directory");
        fs::write(
            dir.join("status.json"),
            br#"{"state":"exited","pid":1,"pgid":1,"processStartTime":2,"shimPid":3,"shimStartTime":4,
                "startedAt":5,"finishedAt":6,"exitCode":7,"signal":null,
                "stdout":{"bytes":0,"truncated":false,"lossy":false},
                "stderr":{"bytes":0,"truncated":false,"lossy":false},"error":null}"#,
        )
        .expect("write");
        let status: Status = read_json(&dir.join("status.json")).expect("read");
        assert_eq!(status.exit_code, Some(7));
        assert_eq!(status.termination, None);
        assert_eq!(status.spawn_error, None);
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
