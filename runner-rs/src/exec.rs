//! Process execution: spawn in its own session, capture both streams, enforce
//! the deadline, and report terminal facts exactly once.

use std::io::{Read, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::json;
use tokio::sync::broadcast;

use crate::capture::CaptureState;
use crate::protocol::SpawnSpec;

/// Terminal facts of one execution.
#[derive(Clone, Debug)]
pub struct Terminal {
    /// Still running.
    pub running: bool,
    /// Ended because a termination was requested (kill, deadline, teardown).
    pub killed: bool,
    /// Exit code when the process exited normally.
    pub exit_code: Option<i32>,
    /// Terminating signal number, when signal-killed.
    pub signal: Option<i32>,
}

impl Terminal {
    fn running() -> Self {
        Terminal { running: true, killed: false, exit_code: None, signal: None }
    }
}

/// One live (or settled) execution.
pub struct Execution {
    /// Execution identity chosen by the host.
    pub id: String,
    /// Idempotency key of the spawn.
    pub operation_id: String,
    /// Digest of the spawn spec.
    pub digest: String,
    /// Host boot that owns this execution.
    pub boot_id: String,
    /// `argv[0]` for the dashboard.
    pub program: String,
    /// Full argv, for the dashboard detail view.
    pub argv: Vec<String>,
    /// Working directory the command ran in.
    pub cwd: String,
    /// When the process was created.
    pub started: Instant,
    /// When it settled.
    pub settled_at: Mutex<Option<Instant>>,
    /// Captured stdout.
    pub stdout: Arc<Mutex<CaptureState>>,
    /// Captured stderr.
    pub stderr: Arc<Mutex<CaptureState>>,
    /// Terminal facts.
    pub terminal: Arc<Mutex<Terminal>>,
    killed_by_request: Arc<AtomicBool>,
    pid: Arc<Mutex<Option<i32>>>,
    grace_ms: u64,
}

/// Totals the dashboard reports.
#[derive(Default)]
pub struct Counters {
    /// Spawns that reached the operating system.
    pub spawned: AtomicU64,
    /// Executions that exited on their own.
    pub settled: AtomicU64,
    /// Executions terminated by a request or the deadline.
    pub killed: AtomicU64,
    /// Executions that failed after admission.
    pub failed: AtomicU64,
}

impl Execution {
    /// Current terminal facts.
    pub fn state(&self) -> Terminal {
        self.terminal.lock().map(|guard| guard.clone()).unwrap_or(Terminal {
            running: true,
            killed: false,
            exit_code: None,
            signal: None,
        })
    }

    /// Request termination of the execution's process group.
    pub fn kill(&self) -> bool {
        if !self.state().running {
            return false;
        }
        self.killed_by_request.store(true, Ordering::SeqCst);
        self.signal_group(libc::SIGTERM);
        let pid = self.pid.lock().ok().and_then(|guard| *guard);
        let grace = self.grace_ms;
        let terminal = Arc::clone(&self.terminal);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(grace.max(1)));
            let running = terminal.lock().map(|guard| guard.running).unwrap_or(false);
            if running {
                if let Some(pid) = pid {
                    signal_group(pid, libc::SIGKILL);
                }
            }
        });
        true
    }

    /// One execution as the dashboard shows it, including captured output.
    pub fn detail(&self) -> serde_json::Value {
        let state = self.state();
        let read = |capture: &Arc<Mutex<CaptureState>>| {
            capture
                .lock()
                .map(|guard| guard.read_from(0, usize::MAX))
                .map(|read| (read.text, read.lossy, read.spill_path))
                .unwrap_or_default()
        };
        let (stdout, stdout_lossy, stdout_spill) = read(&self.stdout);
        let (stderr, stderr_lossy, stderr_spill) = read(&self.stderr);
        let finished = self.settled_at.lock().ok().and_then(|guard| *guard);
        json!({
            "id": self.id,
            "argv": self.argv,
            "command": self.program,
            "cwd": self.cwd,
            "state": if state.running { "running" } else if state.killed { "killed" } else { "exited" },
            "exitCode": state.exit_code,
            "signal": state.signal,
            "ageMs": self.started.elapsed().as_millis() as u64,
            "durationMs": finished.map(|at| at.saturating_duration_since(self.started).as_millis() as u64),
            "stdout": stdout,
            "stderr": stderr,
            "lossy": stdout_lossy || stderr_lossy,
            "stdoutSpillPath": stdout_spill,
            "stderrSpillPath": stderr_spill,
        })
    }

    fn signal_group(&self, signal: i32) {
        if let Ok(guard) = self.pid.lock() {
            if let Some(pid) = *guard {
                signal_group(pid, signal);
            }
        }
    }
}

/// Signal one process group.
fn signal_group(pid: i32, signal: i32) {
    // SAFETY: `kill` with a negative pid addresses the process group led by
    // `pid`; a failure means the group is already gone, which callers tolerate.
    unsafe {
        libc::kill(-pid, signal);
    }
}

/// Spawn one execution from a resolved spec.
///
/// The child becomes a session leader, so its process group id equals its pid
/// and a single `kill(-pid, …)` reaches every descendant.
pub fn spawn_execution(
    spec: SpawnSpec,
    execution_id: String,
    operation_id: String,
    digest: String,
    boot_id: String,
    events: broadcast::Sender<serde_json::Value>,
    counters: Arc<Counters>,
) -> std::io::Result<Arc<Execution>> {
    let mut command = Command::new(&spec.argv[0]);
    command
        .args(&spec.argv[1..])
        .current_dir(&spec.cwd)
        .env_clear()
        .envs(&spec.env)
        .stdin(if spec.stdin.is_some() { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: `setsid` is async-signal-safe and runs in the forked child before
    // exec; it makes the child a session leader with no controlling terminal.
    unsafe {
        use std::os::unix::process::CommandExt;
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = command.spawn()?;
    let pid = child.id() as i32;

    if let Some(input) = spec.stdin.clone() {
        if let Some(mut stdin) = child.stdin.take() {
            thread::spawn(move || {
                let _ = stdin.write_all(input.as_bytes());
            });
        }
    }

    let stdout = Arc::new(Mutex::new(CaptureState::new(
        spec.capture.stdout_max_bytes,
        spec.capture.spill_max_bytes,
        &spec.capture.spill_dir,
        &execution_id,
        "stdout",
    )));
    let stderr = Arc::new(Mutex::new(CaptureState::new(
        spec.capture.stderr_max_bytes,
        spec.capture.spill_max_bytes,
        &spec.capture.spill_dir,
        &execution_id,
        "stderr",
    )));

    let stdout_reader = pump(child.stdout.take(), Arc::clone(&stdout));
    let stderr_reader = pump(child.stderr.take(), Arc::clone(&stderr));

    counters.spawned.fetch_add(1, Ordering::Relaxed);
    let execution = Arc::new(Execution {
        id: execution_id.clone(),
        program: spec.argv.first().cloned().unwrap_or_default(),
        argv: spec.argv.clone(),
        cwd: spec.cwd.clone(),
        started: Instant::now(),
        settled_at: Mutex::new(None),
        operation_id,
        digest,
        boot_id: boot_id.clone(),
        stdout,
        stderr,
        terminal: Arc::new(Mutex::new(Terminal::running())),
        killed_by_request: Arc::new(AtomicBool::new(false)),
        pid: Arc::new(Mutex::new(Some(pid))),
        grace_ms: spec.grace_ms,
    });

    if spec.on_expiry == "kill" {
        let watchdog = Arc::clone(&execution);
        let timeout_ms = spec.timeout_ms;
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(timeout_ms.max(1)));
            watchdog.kill();
        });
    }

    let supervisor = Arc::clone(&execution);
    thread::spawn(move || {
        let outcome = wait_child(child);
        let _ = stdout_reader.join();
        let _ = stderr_reader.join();
        // Settlement waits for the *group* to be empty, not just the direct
        // child: a detached descendant keeps the managed range alive, and the
        // host's isolation story depends on that boundary being honest.
        await_group_empty(pid, Duration::from_millis(2_000));
        let killed = supervisor.killed_by_request.load(Ordering::SeqCst);
        let (exit_code, signal) = match outcome {
            Ok(status) => (
                status.code(),
                status.signal(),
            ),
            Err(_) => (None, None),
        };
        if let Ok(mut guard) = supervisor.terminal.lock() {
            guard.running = false;
            guard.killed = killed;
            guard.exit_code = exit_code;
            guard.signal = signal;
        }
        if let Ok(mut guard) = supervisor.pid.lock() {
            *guard = None;
        }
        if let Ok(mut guard) = supervisor.settled_at.lock() {
            *guard = Some(Instant::now());
        }
        let status = if killed { "killed" } else { "exited" };
        if killed {
            counters.killed.fetch_add(1, Ordering::Relaxed);
        } else if exit_code == Some(0) {
            counters.settled.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.failed.fetch_add(1, Ordering::Relaxed);
        }
        let _ = events.send(crate::protocol::event(
            "settled",
            json!({
                "executionId": supervisor.id,
                "status": status,
                "exitCode": exit_code,
                "signal": signal,
            }),
        ));
    });

    Ok(execution)
}

/// Drain one pipe into its capture in a thread.
fn pump<R: Read + Send + 'static>(
    stream: Option<R>,
    capture: Arc<Mutex<CaptureState>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let Some(mut stream) = stream else { return };
        let mut buffer = [0u8; 16 * 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    if let Ok(mut capture) = capture.lock() {
                        capture.push(&buffer[..read]);
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
    })
}

/// Wait for the child and return its exit status.
fn wait_child(mut child: Child) -> std::io::Result<std::process::ExitStatus> {
    child.wait()
}

/// Poll until the process group led by `pid` holds no live member.
///
/// `kill(-pgid, 0)` answers "does any non-zombie member remain"; the loop is
/// bounded so a group held open by an adopted child cannot block settlement.
fn await_group_empty(pid: i32, bound: Duration) {
    let deadline = std::time::Instant::now() + bound;
    loop {
        // SAFETY: signal 0 probes group membership without delivering anything.
        let alive = unsafe { libc::kill(-pid, 0) } == 0;
        if !alive || std::time::Instant::now() >= deadline {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_group_of_a_dead_group_is_tolerated() {
        signal_group(999_999_999, libc::SIGTERM);
    }

    #[test]
    fn group_probe_returns_for_a_missing_group() {
        let started = std::time::Instant::now();
        await_group_empty(999_999_999, Duration::from_millis(500));
        assert!(started.elapsed() < Duration::from_millis(100));
    }

    #[test]
    fn terminal_starts_running() {
        let terminal = Terminal::running();
        assert!(terminal.running);
        assert_eq!(terminal.exit_code, None);
    }
}
