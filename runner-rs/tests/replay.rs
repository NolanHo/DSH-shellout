//! Settlement replay is addressed to the connection that re-handshaked for it.
mod support;

use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

/// A settlement announced while nobody was subscribed is replayed to the
/// connection that re-handshakes for that boot, not broadcast to every host.
#[test]
fn a_replayed_settlement_reaches_only_the_reconnecting_connection() {
    let scratch = Scratch::new("replay-scope");
    let executor = Executor::start(&scratch);
    let mut owner = executor.ready();
    owner.hello("boot-owner");
    owner
        .exec("replayed", "exit 5", json!({}))
        .expect("execution admitted");
    until(
        || {
            owner
                .state("replayed")
                .filter(|state| state["state"] == "exited")
        },
        Duration::from_secs(10),
        "the execution settles",
    );

    // An unrelated boot is connected for the whole story and must never see the
    // owner's replays.
    let mut unrelated = executor.ready();
    unrelated.hello("boot-unrelated");
    for _ in 0..5 {
        let mut again = executor.ready();
        again.hello("boot-owner");
        assert!(
            again
                .wait_event(
                    |frame| frame["method"] == "settled"
                        && frame["payload"]["executionId"] == "replayed",
                    Duration::from_secs(2),
                )
                .is_some(),
            "the reconnecting host is told about its settlement"
        );
    }
    assert!(
        unrelated
            .wait_event(
                |frame| {
                    frame["method"] == "settled" && frame["payload"]["executionId"] == "replayed"
                },
                Duration::from_millis(500),
            )
            .is_none(),
        "a replay for another boot must not reach an unrelated connection"
    );
}
