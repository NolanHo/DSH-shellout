//! The operator view answers from state, so it can never match the observer.
//!
//! `pgrep -f dsh-shellout-runner` matches the shell that typed the pattern as
//! well as the executor. `--mode status` reads the identity file the executor
//! publishes plus the per-execution status documents instead.

mod support;

use std::process::Command;
use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

/// Run the status mode and return its exit code with stdout.
fn status(scratch: &Scratch) -> (i32, String) {
    let output = Command::new(support::BINARY)
        .args(["--mode", "status", "--state"])
        .arg(scratch.state())
        .output()
        .expect("status runs");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

/// Signal the executor by the pid it published, never by a command pattern.
fn signal_published(scratch: &Scratch, signal: i32) {
    let record = support::read_json(&scratch.state().join("executor.json"));
    let pid = record["pid"].as_i64().expect("published pid") as i32;
    // SAFETY: `kill` with a pid this executor published a moment ago; a stale
    // pid only fails with ESRCH, which the caller's wait then reports.
    unsafe {
        libc::kill(pid, signal);
    }
}

#[test]
fn reports_the_executor_that_published_its_identity() {
    let scratch = Scratch::new("inspect-alive");
    let executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-inspect");

    let (code, stdout) = status(&scratch);
    assert_eq!(code, 0, "a live executor answers with 0: {stdout}");
    assert!(stdout.contains("alive=yes"), "{stdout}");
    assert!(stdout.contains("version="), "{stdout}");
}

#[test]
fn lists_a_running_execution_with_its_pids() {
    let scratch = Scratch::new("inspect-rows");
    let executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-inspect-rows");
    host.exec("sleeping", "sleep 30", json!({}))
        .expect("admitted");

    let (_, stdout) = status(&scratch);
    assert!(stdout.contains("sleeping"), "{stdout}");
    let line = stdout
        .lines()
        .find(|line| line.contains("sleeping"))
        .expect("the row is printed");
    assert!(line.contains("running"), "{line}");
    assert!(line.contains("pid=") && !line.contains("pid=-"), "{line}");
    assert!(line.contains("shim="), "{line}");
}

#[test]
fn withdraws_its_identity_on_a_clean_stop_and_reports_a_dead_executor() {
    let scratch = Scratch::new("inspect-dead");
    let mut executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-inspect-dead");
    drop(host);

    signal_published(&scratch, libc::SIGTERM);
    let identity = scratch.state().join("executor.json");
    until(
        || (!identity.exists()).then_some(()),
        Duration::from_secs(10),
        "the clean stop withdraws the identity",
    );
    executor.kill();

    let (code, stdout) = status(&scratch);
    assert_eq!(code, 1, "no executor answers: {stdout}");
    assert!(
        stdout.contains("unknown") || stdout.contains("alive=no"),
        "{stdout}"
    );
}

#[test]
fn a_killed_executor_leaves_an_identity_the_view_reports_as_dead() {
    let scratch = Scratch::new("inspect-killed");
    let mut executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-inspect-killed");
    drop(host);
    executor.kill();

    let identity = scratch.state().join("executor.json");
    let record = support::read_json(&identity);
    assert!(record["pid"].as_i64().unwrap_or(0) > 0, "{record}");
    let (code, stdout) = status(&scratch);
    assert_eq!(code, 1, "{stdout}");
    assert!(stdout.contains("alive=no"), "{stdout}");
}
