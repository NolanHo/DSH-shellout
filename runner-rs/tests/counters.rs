//! Dashboard counters are a projection of history, not of this process.
mod support;

use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

/// Dashboard counters describe the execution plane, not the current process:
/// after a restart they still agree with history, and refused spawns count.
#[test]
fn observation_counters_are_derived_from_history() {
    let scratch = Scratch::new("counters");
    let mut executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-counters");
    host.exec("done", "exit 0", json!({}))
        .expect("execution admitted");
    host.exec(
        "terminated",
        "trap '' TERM; while :; do sleep 1; done",
        json!({"timeoutMs": 200, "graceMs": 100}),
    )
    .expect("execution admitted");
    let refused = host
        .call(
            "exec",
            json!({
                "executionId": "refused",
                "operationId": "op-refused",
                "specDigest": "refused",
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
    assert_eq!(refused["kind"], "err", "{refused}");
    until(
        || {
            host.state("done")
                .filter(|state| state["state"] == "exited")
        },
        Duration::from_secs(10),
        "the clean exit settles",
    );
    until(
        || {
            host.state("terminated")
                .filter(|state| state["state"] == "killed")
        },
        Duration::from_secs(10),
        "the deadline kill settles",
    );
    executor.kill();

    let executor = Executor::start_named(&scratch, "executor-2", &[]);
    let mut observer = executor.ready();
    let snapshot = observer
        .request("observe", json!({}))
        .expect("the observatory answers");
    assert_eq!(snapshot["spawned"], 3, "{snapshot}");
    assert_eq!(snapshot["settled"], 1, "{snapshot}");
    assert_eq!(snapshot["killed"], 1, "{snapshot}");
    assert_eq!(snapshot["failed"], 1, "{snapshot}");
}
