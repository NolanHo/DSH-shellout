//! Spawn admission: exactly one party decides the outcome, the wait never
//! occupies the runtime, and later spawns survive a replaced binary.
mod support;

use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

/// Admission waits for the shim's first report. That wait must not occupy the
/// executor's runtime, so an unrelated request on another connection stays
/// responsive while a spawn is in flight.
#[test]
fn a_slow_admission_does_not_stall_other_requests() {
    let scratch = Scratch::new("responsive");
    let executor = Executor::start_full(
        &scratch,
        "executor",
        std::path::Path::new(support::BINARY),
        &[],
        &[
            ("TOKIO_WORKER_THREADS", "1"),
            ("DSH_SHELLOUT_SPAWN_GRACE_MS", "1500"),
            ("DSH_SHELLOUT_SHIM_START_DELAY_MS", "1200"),
        ],
    );
    let mut observer = executor.ready();
    let mut host = executor.ready();
    host.hello("boot-responsive");
    let admitted = std::thread::spawn(move || host.exec("slow", "exit 0", json!({})));
    std::thread::sleep(Duration::from_millis(200));

    let started = std::time::Instant::now();
    observer
        .request("observe", json!({}))
        .expect("an unrelated request is answered");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(500),
        "an unrelated request waited {elapsed:?} behind a spawn"
    );
    admitted
        .join()
        .expect("exec thread")
        .expect("the command is admitted");
}

/// Deployment replaces the binary file; the executor keeps running from the
/// image it already loaded. Spawning the shim from the old path would fail for
/// every later execution until a restart.
#[test]
fn deleting_the_running_binary_does_not_break_later_spawns() {
    let scratch = Scratch::new("replace");
    let binary = scratch.root.join("runner");
    std::fs::copy(support::BINARY, &binary).expect("copy the runner binary");
    let executor = Executor::start_full(
        &scratch,
        "executor",
        std::path::Path::new(&binary),
        &[],
        &[],
    );
    let mut host = executor.ready();
    host.hello("boot-replace");
    std::fs::remove_file(&binary).expect("the deployment removed the old file");
    host.exec("after-replace", "exit 0", json!({}))
        .expect("a later execution is still admitted");
    let state = until(
        || {
            host.state("after-replace")
                .filter(|state| state["state"] != "running")
        },
        Duration::from_secs(10),
        "the execution after the replacement settles",
    );
    assert_eq!(state["state"], "exited", "{state}");
}

/// Once the executor has published `go`, the command may already be starting:
/// the timeout must answer with the execution rather than a refusal the host
/// would retry, and settlement must carry the real exit facts.
#[test]
fn a_command_that_starts_after_the_go_verdict_is_accepted_and_settles_for_real() {
    let scratch = Scratch::new("late-go");
    let executor = Executor::start_full(
        &scratch,
        "executor",
        std::path::Path::new(support::BINARY),
        &[],
        &[
            ("DSH_SHELLOUT_SPAWN_GRACE_MS", "300"),
            ("DSH_SHELLOUT_SHIM_EXEC_DELAY_MS", "1200"),
        ],
    );
    let mut host = executor.ready();
    host.hello("boot-late-go");
    let marker = scratch.root.join("ran.marker");
    let command = format!("echo ran > {}; exit 7", marker.display());
    let reply = host
        .call(
            "exec",
            json!({
                "executionId": "late-go",
                "operationId": "op-late-go",
                "specDigest": "late-go",
                "spec": {
                    "argv": ["/bin/sh", "-c", command],
                    "cwd": scratch.root,
                    "env": {"PATH": "/usr/bin:/bin"},
                    "timeoutMs": 10_000,
                    "onExpiry": "kill",
                    "graceMs": 100,
                    "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                        "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
                },
            }),
        )
        .expect("the executor answered");
    assert_eq!(
        reply["kind"], "res",
        "a command authorized with `go` must not be answered as rejected: {reply}"
    );
    assert_eq!(reply["payload"]["executionId"], "late-go");

    // The command runs once, after the reply, and settles with its real code.
    let state = until(
        || {
            host.state("late-go")
                .filter(|state| state["state"] != "running")
        },
        Duration::from_secs(15),
        "the late command settles",
    );
    assert_eq!(state["state"], "exited", "{state}");
    assert_eq!(state["exitCode"], 7, "{state}");
    assert!(
        marker.exists(),
        "the accepted command really ran instead of being killed at the timeout"
    );

    let event = host
        .wait_event(
            |frame| frame["method"] == "settled" && frame["payload"]["executionId"] == "late-go",
            Duration::from_secs(5),
        )
        .expect("the settlement event is delivered");
    assert_eq!(event["payload"]["status"], "exited", "{event}");
    assert_eq!(event["payload"]["exitCode"], 7, "{event}");

    // History records the outcome that happened, not a fixed `failed`.
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let recorded = until(
        || {
            connection
                .query_row(
                    "SELECT state, exit_code FROM executions WHERE id = 'late-go'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
                )
                .ok()
                .filter(|(state, _)| state != "running")
        },
        Duration::from_secs(5),
        "the history row settles with the real outcome",
    );
    assert_eq!(recorded, ("exited".to_string(), Some(7)));
}

/// A shim that reports no spawn outcome in time is a failure the caller is
/// told about, and the command must never start afterwards: the executor owns
/// the decision and leaves a terminal, addressable execution behind.
#[test]
fn a_spawn_timeout_refuses_the_command_and_leaves_no_ghost() {
    let scratch = Scratch::new("timeout");
    let executor = Executor::start_full(
        &scratch,
        "executor",
        std::path::Path::new(support::BINARY),
        &[],
        &[
            ("DSH_SHELLOUT_SPAWN_GRACE_MS", "200"),
            ("DSH_SHELLOUT_SHIM_START_DELAY_MS", "600"),
        ],
    );
    let mut host = executor.ready();
    host.hello("boot-timeout");
    let marker = scratch.root.join("ran.marker");
    let command = format!("echo ran > {}", marker.display());
    let reply = host
        .call(
            "exec",
            json!({
                "executionId": "late",
                "operationId": "op-late",
                "specDigest": "late",
                "spec": {
                    "argv": ["/bin/sh", "-c", command],
                    "cwd": scratch.root,
                    "env": {"PATH": "/usr/bin:/bin"},
                    "timeoutMs": 10_000,
                    "onExpiry": "kill",
                    "graceMs": 100,
                    "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                        "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
                },
            }),
        )
        .expect("the executor answered");
    assert_eq!(
        reply["kind"], "err",
        "a spawn timeout is a failure: {reply}"
    );

    // The execution stays addressable and settles instead of vanishing.
    let state = until(
        || {
            host.state("late")
                .filter(|state| state["state"] != "running")
        },
        Duration::from_secs(10),
        "the timed-out execution settles",
    );
    assert_eq!(state["state"], "failed", "{state}");

    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let recorded: String = connection
        .query_row(
            "SELECT state FROM executions WHERE id = 'late'",
            [],
            |row| row.get(0),
        )
        .expect("history records the refused spawn");
    assert_eq!(recorded, "failed", "history never stays `running`");

    // The shim that starts after the deadline must honour the refusal.
    std::thread::sleep(Duration::from_millis(1_500));
    assert!(
        !marker.exists(),
        "the rejected command ran after the exec reply said it did not"
    );
}
