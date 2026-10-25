//! Executor-level scenarios: what a host observes across executor restarts,
//! which executor owns a state directory, and how admission reports a command
//! that could not be started.
mod support;

use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

/// Settlement must survive an executor restart: the host reconnects with the
/// same boot id and is told how the execution ended even though the event was
/// emitted while nobody was subscribed.
#[test]
fn a_settlement_emitted_while_no_host_was_connected_is_replayed() {
    let scratch = Scratch::new("replay");
    let mut executor = Executor::start(&scratch);
    let mut host = executor.ready();
    let hello = host.hello("boot-replay");
    assert_eq!(hello["executions"], json!([]));

    host.exec("sleepy", "sleep 1; exit 5", json!({}))
        .expect("execution admitted");
    // The executor dies while the command is still running.
    executor.kill();
    std::thread::sleep(Duration::from_millis(2_000));

    let executor = Executor::start_named(&scratch, "executor-2", &[]);
    let mut host = executor.ready();
    let hello = host.hello("boot-replay");
    let rows = hello["executions"].as_array().expect("execution facts");
    let row = rows
        .iter()
        .find(|row| row["executionId"] == "sleepy")
        .unwrap_or_else(|| panic!("adopted execution missing from hello: {hello}"));
    assert_eq!(row["state"], "exited", "hello carries the terminal state");
    assert_eq!(row["exitCode"], 5, "hello carries the exit code");

    let event = host
        .wait_event(
            |frame| frame["method"] == "settled" && frame["payload"]["executionId"] == "sleepy",
            Duration::from_secs(5),
        )
        .expect("the settlement is replayed to the reconnecting host");
    assert_eq!(event["payload"]["status"], "exited");
    assert_eq!(event["payload"]["exitCode"], 5);
}

/// Two executors on one state directory would each adopt the other's shims.
#[test]
fn a_second_executor_on_the_same_state_is_refused() {
    let scratch = Scratch::new("lock");
    let first = Executor::start(&scratch);
    let mut client = first.ready();
    client.hello("boot-lock");

    let mut second = Executor::start_named(&scratch, "second", &[]);
    let status = until(
        || second.child.try_wait().ok().flatten(),
        Duration::from_secs(15),
        "the second executor refuses to run",
    );
    assert!(!status.success(), "the second executor must not start");
    let stderr = second.diagnose();
    assert!(
        stderr.contains("already owned by another executor"),
        "the refusal names the lock: {stderr}"
    );

    // The running executor is untouched: it still admits and serves work.
    client
        .exec("alive", "echo ok", json!({}))
        .expect("executor still serves");
    let state = until(
        || {
            client
                .state("alive")
                .filter(|state| state["state"] != "running")
        },
        Duration::from_secs(10),
        "the first executor still settles executions",
    );
    assert_eq!(state["state"], "exited");
}

/// A shim that is still terminating its group has already lost its direct
/// child; loading the execution must not mistake that for a lost execution.
#[test]
fn an_execution_whose_child_was_reaped_but_whose_shim_lives_survives_a_restart() {
    let scratch = Scratch::new("reload");
    let mut executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-reload");
    let admitted = host
        .call(
            "exec",
            json!({
                "executionId": "survivor",
                "operationId": "op-survivor",
                "specDigest": "survivor",
                "spec": {
                    "argv": ["/bin/sh", "-c", "trap '' TERM; (sleep 30) & exit 3"],
                    "cwd": "/tmp",
                    "env": {"PATH": "/usr/bin:/bin"},
                    "timeoutMs": 10_000,
                    "onExpiry": "kill",
                    "graceMs": 6000,
                    "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                        "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
                },
            }),
        )
        .expect("exec answered");
    assert_eq!(admitted["kind"], "res", "{admitted}");
    std::thread::sleep(Duration::from_millis(1_000));
    executor.kill();

    let executor = Executor::start_named(&scratch, "executor-2", &[]);
    let mut host = executor.ready();
    host.hello("boot-reload");
    let state = until(
        || {
            host.state("survivor")
                .filter(|state| state["state"] != "running")
        },
        Duration::from_secs(15),
        "the adopted execution settles",
    );
    assert_eq!(state["state"], "exited", "{state}");
    assert_eq!(state["exitCode"], 3, "{state}");

    // History ends on the same facts, not on a `lost` guess. The row is written
    // by the poll pass that publishes the state, so the two are sampled apart.
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let recorded = until(
        || {
            connection
                .query_row(
                    "SELECT state, exit_code FROM executions WHERE id = 'survivor'",
                    [],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
                )
                .ok()
                .filter(|(state, exit_code)| state != "running" || exit_code.is_some())
        },
        Duration::from_secs(10),
        "the history row settles",
    );
    assert_eq!(recorded, ("exited".to_string(), Some(3)));
}

/// A command that cannot be started is a preparation failure: the exec reply
/// itself must say so, not a handle that fails later.
#[test]
fn a_spawn_failure_is_reported_by_the_exec_reply() {
    let scratch = Scratch::new("spawn");
    let executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-spawn");
    let reply = host
        .call(
            "exec",
            json!({
                "executionId": "no-cwd",
                "operationId": "op-no-cwd",
                "specDigest": "no-cwd",
                "spec": {
                    "argv": ["/bin/true"],
                    "cwd": scratch.root.join("missing-directory"),
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

    assert_eq!(reply["kind"], "err", "{reply}");
    let message = reply["payload"]["message"].as_str().unwrap_or_default();
    assert!(
        message.contains("/bin/true"),
        "the failure names the program: {message}"
    );

    // Nothing is left running, and history records the failure rather than a
    // row that stays `running` forever.
    assert!(host.state("no-cwd").is_none());
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let recorded: String = connection
        .query_row(
            "SELECT state FROM executions WHERE id = 'no-cwd'",
            [],
            |row| row.get(0),
        )
        .expect("a history row records the failed spawn");
    assert_eq!(recorded, "failed");
}

/// History is a projection of the state directory: a row whose directory is
/// gone has nothing left that could settle it.
#[test]
fn a_running_history_row_without_a_state_directory_becomes_lost() {
    let scratch = Scratch::new("projection");
    std::fs::create_dir_all(scratch.state()).expect("state directory");
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    connection
        .execute_batch(
            "CREATE TABLE executions (
                id TEXT PRIMARY KEY, argv TEXT NOT NULL, cwd TEXT NOT NULL,
                started_at INTEGER NOT NULL, finished_at INTEGER, state TEXT NOT NULL,
                exit_code INTEGER, signal INTEGER, stdout TEXT NOT NULL,
                stderr TEXT NOT NULL, lossy INTEGER NOT NULL
             );
             INSERT INTO executions VALUES
                ('orphan', '[\"/bin/sh\"]', '/tmp', 1, NULL, 'running', NULL, NULL, '', '', 0);",
        )
        .expect("seed history");
    drop(connection);

    let executor = Executor::start(&scratch);
    let mut observer = executor.ready();
    let snapshot = observer.request("observe", json!({})).expect("observation");
    assert_eq!(snapshot["live"], 0);

    let connection = rusqlite::Connection::open(scratch.history()).expect("history reopens");
    let (state, finished): (String, Option<i64>) = connection
        .query_row(
            "SELECT state, finished_at FROM executions WHERE id = 'orphan'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("the row survives");
    assert_eq!(state, "lost");
    assert!(finished.is_some(), "a lost row records when it was decided");
}

/// A terminal row is final: a later, contradictory state file cannot rewrite it.
#[test]
fn a_terminal_history_row_never_regresses() {
    let scratch = Scratch::new("oneway");
    let mut executor =
        Executor::start_named(&scratch, "executor", &["--state-retention-ms", "60000"]);
    let mut host = executor.ready();
    host.hello("boot-oneway");
    host.exec("once", "exit 3", json!({}))
        .expect("execution admitted");
    until(
        || {
            host.state("once")
                .filter(|state| state["state"] == "exited")
        },
        Duration::from_secs(10),
        "execution settles",
    );
    executor.kill();

    // A contradictory terminal record appears while the executor is away.
    let path = scratch.state().join("once").join("status.json");
    let mut status: serde_json::Value = support::read_json(&path);
    status["exitCode"] = json!(9);
    std::fs::write(&path, serde_json::to_vec(&status).unwrap()).expect("forged status");

    let executor =
        Executor::start_named(&scratch, "executor-2", &["--state-retention-ms", "60000"]);
    executor.ready();
    std::thread::sleep(Duration::from_millis(800));
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let (state, exit_code): (String, Option<i64>) = connection
        .query_row(
            "SELECT state, exit_code FROM executions WHERE id = 'once'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("the row survives");
    assert_eq!((state.as_str(), exit_code), ("exited", Some(3)));
}

/// Restarting the executor must not rewrite history it already agrees with.
#[test]
fn a_restart_does_not_rewrite_history_that_is_already_correct() {
    let scratch = Scratch::new("nowrite");
    let mut executor =
        Executor::start_named(&scratch, "executor", &["--state-retention-ms", "60000"]);
    let mut host = executor.ready();
    host.hello("boot-nowrite");
    host.exec("settled", "exit 4", json!({}))
        .expect("execution admitted");
    until(
        || {
            host.state("settled")
                .filter(|state| state["state"] == "exited")
        },
        Duration::from_secs(10),
        "execution settles",
    );
    executor.kill();

    // Any write from here on is a rewrite of a row that is already correct.
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    connection
        .execute_batch(
            "CREATE TRIGGER no_insert BEFORE INSERT ON executions
               BEGIN SELECT RAISE(FAIL, 'history was rewritten at startup'); END;
             CREATE TRIGGER no_update BEFORE UPDATE ON executions
               BEGIN SELECT RAISE(FAIL, 'history was rewritten at startup'); END;",
        )
        .expect("write guard");
    drop(connection);

    let executor =
        Executor::start_named(&scratch, "executor-2", &["--state-retention-ms", "60000"]);
    let mut observer = executor.ready();
    let snapshot = observer.request("observe", json!({})).expect("observation");
    assert_eq!(
        snapshot["historyWritable"], true,
        "the restart rewrote an unchanged row: {snapshot}"
    );
}

/// The state directory is a working copy: the durable row replaces it.
#[test]
fn a_settled_execution_directory_is_deleted_once_its_row_is_durable() {
    let scratch = Scratch::new("retain");
    let executor = Executor::start_named(&scratch, "executor", &["--state-retention-ms", "0"]);
    let mut host = executor.ready();
    host.hello("boot-retain");
    host.exec("gone", "echo kept; exit 0", json!({}))
        .expect("execution admitted");
    until(
        || {
            host.state("gone")
                .filter(|state| state["state"] == "exited")
        },
        Duration::from_secs(10),
        "execution settles",
    );
    let directory = scratch.state().join("gone");
    until(
        || (!directory.exists()).then_some(()),
        Duration::from_secs(10),
        "the state directory is deleted",
    );
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let (state, exit_code, stdout): (String, Option<i64>, String) = connection
        .query_row(
            "SELECT state, exit_code, stdout FROM executions WHERE id = 'gone'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .expect("the history row outlives the directory");
    assert_eq!(
        (state.as_str(), exit_code, stdout.as_str()),
        ("exited", Some(0), "kept\n")
    );
}
