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
