//! Per-execution supervisor. No executor-owned pipes, timers or child lifetime.
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::process::{CommandExt, ExitStatusExt};
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

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct StreamStatus {
    pub bytes: u64,
    pub truncated: bool,
    pub lossy: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
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
    process_start_time(pid).is_ok_and(|actual| actual == start)
}

pub fn is_live(status: &Status) -> bool {
    matches_process(status.shim_pid, status.shim_start_time)
        && matches!((status.pid, status.process_start_time), (Some(pid), Some(start)) if matches_process(pid, start))
}

fn signal_group(pgid: i32, signal: i32) {
    // SAFETY: only this shim's newly created process group is addressed.
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

pub fn run(state: &Path, id: &str) -> io::Result<()> {
    let dir = state.join(id);
    let meta: Meta = read_json(&dir.join("meta.json"))?;
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
    };
    let result = supervise(&dir, &meta.spec, &mut status);
    if let Err(error) = &result {
        status.state = "failed".into();
        status.error = Some(error.to_string());
    }
    status.finished_at = Some(now_ms());
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
    let mut child = command.spawn()?;
    let pgid = child.id() as i32;
    status.pid = Some(pgid);
    status.pgid = Some(pgid);
    status.process_start_time = Some(process_start_time(pgid)?);
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
    let started = Instant::now();
    let mut terminating: Option<Instant> = None;
    let mut publication = Instant::now() - Duration::from_secs(1);
    let outcome = loop {
        if let Some(outcome) = child.try_wait()? {
            break outcome;
        }
        if terminating.is_none()
            && (dir.join("kill.request").exists()
                || (spec.on_expiry == "kill"
                    && started.elapsed() >= Duration::from_millis(spec.timeout_ms)))
        {
            terminating = Some(Instant::now());
            signal_group(pgid, libc::SIGTERM);
        }
        if terminating.is_some_and(|at| at.elapsed() >= Duration::from_millis(spec.grace_ms)) {
            signal_group(pgid, libc::SIGKILL);
        }
        if publication.elapsed() >= Duration::from_millis(200) {
            status.stdout = out
                .lock()
                .map_err(|_| io::Error::other("stdout poisoned"))?
                .clone();
            status.stderr = err
                .lock()
                .map_err(|_| io::Error::other("stderr poisoned"))?
                .clone();
            if let Err(error) = atomic_json(&dir.join("status.json"), status) {
                eprintln!(
                    "shim {}: running status write failed: {error}",
                    dir.display()
                );
            }
            publication = Instant::now();
        }
        thread::sleep(Duration::from_millis(20));
    };
    // The direct child's exit closes the execution scope; descendants must not
    // hold capture pipes or survive their supervisor's terminal publication.
    signal_group(pgid, libc::SIGTERM);
    let deadline = Instant::now() + Duration::from_millis(spec.grace_ms);
    while !(out_thread.is_finished() && err_thread.is_finished()) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(10));
    }
    signal_group(pgid, libc::SIGKILL);
    let out_result = out_thread
        .join()
        .map_err(|_| io::Error::other("stdout reader panicked"))?;
    let err_result = err_thread
        .join()
        .map_err(|_| io::Error::other("stderr reader panicked"))?;
    status.stdout = out
        .lock()
        .map_err(|_| io::Error::other("stdout poisoned"))?
        .clone();
    status.stderr = err
        .lock()
        .map_err(|_| io::Error::other("stderr poisoned"))?
        .clone();
    status.exit_code = outcome.code();
    status.signal = outcome.signal();
    status.state = if terminating.is_some() {
        "killed"
    } else {
        "exited"
    }
    .into();
    out_result?;
    err_result
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
        };
        atomic_json(&dir.join("status.json"), &status).expect("write");
        let actual: Status = read_json(&dir.join("status.json")).expect("read");
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(status).unwrap()
        );
        fs::remove_dir_all(dir).expect("cleanup");
    }
}
