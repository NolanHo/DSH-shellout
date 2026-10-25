//! A terminal history row is final, including for a retried execution id.
mod support;

use serde_json::json;
use support::{Executor, Scratch};

/// An execution id whose row is already terminal cannot be admitted again: a
/// silent no-op on the settlement write would leave the retried run recorded as
/// the earlier failure.
#[test]
fn a_retried_execution_id_cannot_hide_a_failed_row() {
    let scratch = Scratch::new("reuse-id");
    let executor = Executor::start(&scratch);
    let mut host = executor.ready();
    host.hello("boot-reuse");
    let first = host
        .call(
            "exec",
            json!({
                "executionId": "reused",
                "operationId": "op-reused-1",
                "specDigest": "first",
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
    assert_eq!(first["kind"], "err", "{first}");

    let retried = host
        .call(
            "exec",
            json!({
                "executionId": "reused",
                "operationId": "op-reused-2",
                "specDigest": "second",
                "spec": {
                    "argv": ["/bin/sh", "-c", "exit 0"],
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
        retried["kind"], "err",
        "a terminal history row refuses the id: {retried}"
    );
    let connection = rusqlite::Connection::open(scratch.history()).expect("history opens");
    let (state, exit_code): (String, Option<i64>) = connection
        .query_row(
            "SELECT state, exit_code FROM executions WHERE id = 'reused'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("the row survives");
    assert_eq!((state.as_str(), exit_code), ("failed", None));
}
