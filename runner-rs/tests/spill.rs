//! The complete capture belongs to the host's spill directory and must outlive
//! the runner's ephemeral state directory.
mod support;

use std::time::Duration;

use serde_json::json;
use support::{until, Executor, Scratch};

/// The complete capture belongs to the host's spill directory: it must outlive
/// the ephemeral state directory, and a read after retention must not look like
/// a clean empty stream.
#[test]
fn a_spill_file_outlives_the_state_directory() {
    let scratch = Scratch::new("spill");
    let spill_dir = scratch.root.join("host-spill");
    std::fs::create_dir_all(&spill_dir).expect("spill directory");
    let executor = Executor::start_named(&scratch, "executor", &["--state-retention-ms", "0"]);
    let mut host = executor.ready();
    host.hello("boot-spill");
    host.exec(
        "spilled",
        "printf 'kept\\n'",
        json!({
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": spill_dir},
        }),
    )
    .expect("execution admitted");

    let read = until(
        || {
            let read = host.request(
                "readStream",
                json!({"executionId": "spilled", "stream": "stdout", "offset": 0, "limit": 100}),
            )?;
            read["spillPath"].as_str().map(|_| read.clone())
        },
        Duration::from_secs(10),
        "the runner reports a spill path",
    );
    let spill = std::path::PathBuf::from(read["spillPath"].as_str().expect("spill path"));
    assert!(
        spill.starts_with(&spill_dir),
        "the spill belongs to the host's spillDir, not the state directory: {}",
        spill.display()
    );
    let directory = scratch.state().join("spilled");
    until(
        || (!directory.exists()).then_some(()),
        Duration::from_secs(10),
        "the state directory is deleted",
    );
    assert!(spill.exists(), "the spill outlives the state directory");
    assert_eq!(
        std::fs::read_to_string(&spill).expect("spill readable"),
        "kept\n"
    );
    let late = host
        .request(
            "readStream",
            json!({"executionId": "spilled", "stream": "stdout", "offset": 0, "limit": 100}),
        )
        .expect("a late read is answered");
    assert_eq!(late["text"], "kept\n", "the spill still serves the capture");

    // With the capture genuinely gone, the read reports loss instead of a
    // clean empty stream.
    std::fs::remove_file(&spill).expect("spill removed");
    let lost = host
        .request(
            "readStream",
            json!({"executionId": "spilled", "stream": "stdout", "offset": 0, "limit": 100}),
        )
        .expect("a late read is answered");
    assert_eq!(lost["text"], "");
    assert_eq!(lost["lossy"], true, "{lost}");
}
