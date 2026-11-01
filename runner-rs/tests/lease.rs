//! The lease outlives the connection: the shim enforces it, not the executor.
//!
//! rexec's lesson is that a remote worker must not depend on the client being
//! connected. The same rule applies here: a client that vanishes has to take its
//! commands with it even when no executor is left to notice.

mod support;

use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

fn long_command() -> &'static str {
    "sleep 30"
}

#[test]
fn a_vanished_client_loses_its_lease_even_with_no_executor_alive() {
    let scratch = Scratch::new("lease-orphan");
    let mut executor = Executor::start_named(&scratch, "executor", &["--lease-ttl-ms", "1500"]);
    let mut host = executor.ready();
    host.hello("boot-lease-orphan");
    host.exec("orphan", long_command(), json!({}))
        .expect("admitted");

    let status_path = scratch.state().join("orphan").join("status.json");
    until(
        || {
            let status: serde_json::Value = support::read_json(&status_path);
            (status["state"] == "running" && status["pid"].as_i64().is_some()).then_some(status)
        },
        Duration::from_secs(20),
        "the command starts",
    );

    // The executor disappears: no sweeper is left to enforce anything.
    executor.kill();

    let status = until(
        || {
            let status: serde_json::Value = support::read_json(&status_path);
            (status["state"] != "running").then_some(status)
        },
        Duration::from_secs(60),
        "the shim ends the command when the lease expires",
    );
    assert_eq!(
        status["termination"], "lease",
        "the shim attributes the ending to the lease it enforced: {status}"
    );
}

#[test]
fn a_client_that_keeps_pinging_keeps_its_command() {
    let scratch = Scratch::new("lease-kept");
    let executor = Executor::start_named(&scratch, "executor", &["--lease-ttl-ms", "1500"]);
    let mut host = executor.ready();
    host.hello("boot-lease-kept");
    host.exec("kept", long_command(), json!({}))
        .expect("admitted");

    let status_path = scratch.state().join("kept").join("status.json");
    until(
        || {
            let status: serde_json::Value = support::read_json(&status_path);
            (status["state"] == "running").then_some(())
        },
        Duration::from_secs(20),
        "the command starts",
    );

    // Two lease periods with heartbeats in between: the command must survive.
    for _ in 0..4 {
        std::thread::sleep(Duration::from_millis(400));
        assert!(
            host.ping("boot-lease-kept").is_some(),
            "the runner answers the heartbeat"
        );
    }
    let status: serde_json::Value = support::read_json(&status_path);
    assert_eq!(status["state"], "running", "{status}");
}

/// The plane's own bookkeeping must not look like an execution at startup.
#[test]
fn adoption_ignores_the_heartbeat_directory() {
    let scratch = Scratch::new("lease-adopt");
    let mut executor = Executor::start_named(&scratch, "executor", &["--lease-ttl-ms", "60000"]);
    let mut host = executor.ready();
    host.hello("boot-adopt");
    host.exec("real", "true", json!({})).expect("admitted");
    support::until(
        || {
            let status: serde_json::Value =
                support::read_json(&scratch.state().join("real").join("status.json"));
            (status["state"] != "running").then_some(())
        },
        Duration::from_secs(20),
        "the execution settles",
    );
    executor.kill();

    // The restarted executor adopts what it finds; `.boots/` must be skipped
    // rather than reported as a broken execution.
    let restarted = Executor::start_named(&scratch, "executor", &["--lease-ttl-ms", "60000"]);
    let stderr = restarted.diagnose();
    assert!(
        !stderr.contains(".boots: recovery failed"),
        "the heartbeat directory is not an execution: {stderr}"
    );
}

/// Heartbeats name the clients that are connected: they are owner-only.
#[test]
fn the_heartbeat_directory_is_private() {
    use std::os::unix::fs::PermissionsExt;

    let scratch = Scratch::new("lease-perms");
    let executor = Executor::start_named(&scratch, "executor", &["--lease-ttl-ms", "60000"]);
    let mut host = executor.ready();
    host.hello("boot-perms");

    let directory = scratch.state().join(".boots");
    support::until(
        || directory.join("boot-perms").exists().then_some(()),
        Duration::from_secs(10),
        "the heartbeat appears",
    );
    let mode = std::fs::metadata(&directory)
        .expect("metadata")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o700, "the heartbeat directory is owner-only");
}
