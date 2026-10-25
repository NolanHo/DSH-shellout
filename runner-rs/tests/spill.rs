//! Spills are host-owned copies of a complete stream, created only when the
//! host's in-memory cap cannot serve the output, and reclaimed by a bounded
//! retention policy instead of accumulating silently.
mod support;

use std::path::Path;
use std::time::Duration;

use serde_json::json;
use support::{until, write_meta, Executor, Scratch};

/// The capture policy used by these tests: a small in-memory cap so a modest
/// stream must be spilled, and a spill file larger than the stream.
fn capture(spill_dir: &Path) -> serde_json::Value {
    json!({"stdoutMaxBytes": 1024, "stderrMaxBytes": 1024,
        "spillMaxBytes": 1_048_576, "spillDir": spill_dir})
}

fn spill_files(spill_dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(spill_dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect()
        })
        .unwrap_or_default();
    names.sort();
    names
}

/// An empty or small stream is already held by the host's mirror; writing a
/// spill for it only costs a file that nothing ever deletes.
#[test]
fn no_output_and_small_streams_leave_no_spill_files() {
    let scratch = Scratch::new("no-spill");
    let spill_dir = scratch.root.join("host-spill");
    std::fs::create_dir_all(&spill_dir).expect("spill directory");
    let executor = Executor::start_named(&scratch, "executor", &["--state-retention-ms", "0"]);
    let mut host = executor.ready();
    host.hello("boot-no-spill");

    let mut ids = Vec::new();
    for index in 0..4 {
        let id = format!("empty-{index}");
        host.exec(&id, "true", json!({"capture": capture(&spill_dir)}))
            .expect("empty execution admitted");
        ids.push(id);
    }
    host.exec(
        "small",
        "printf 'ok\\n'",
        json!({"capture": capture(&spill_dir)}),
    )
    .expect("small execution admitted");
    ids.push("small".into());

    for id in &ids {
        until(
            || host.state(id).filter(|state| state["state"] != "running"),
            Duration::from_secs(10),
            "the execution settles",
        );
    }
    assert_eq!(
        spill_files(&spill_dir),
        Vec::<String>::new(),
        "an empty or small stream must not create a spill"
    );
}

/// A stream beyond the in-memory cap produces exactly one complete file per
/// stream, and that file serves a read after the state directory is gone.
#[test]
fn larger_streams_produce_one_complete_spill_per_stream() {
    let scratch = Scratch::new("large-spill");
    let spill_dir = scratch.root.join("host-spill");
    std::fs::create_dir_all(&spill_dir).expect("spill directory");
    let executor = Executor::start_named(&scratch, "executor", &["--state-retention-ms", "0"]);
    let mut host = executor.ready();
    host.hello("boot-large-spill");
    host.exec(
        "large",
        "head -c 4096 /dev/zero | tr '\\0' o; head -c 4096 /dev/zero | tr '\\0' e >&2",
        json!({"capture": capture(&spill_dir)}),
    )
    .expect("execution admitted");
    until(
        || {
            host.state("large")
                .filter(|state| state["state"] != "running")
        },
        Duration::from_secs(10),
        "the execution settles",
    );
    let names = until(
        || (spill_files(&spill_dir).len() == 2).then(|| spill_files(&spill_dir)),
        Duration::from_secs(10),
        "one spill per stream",
    );
    assert_eq!(
        names,
        vec![
            "large-stderr.log".to_string(),
            "large-stdout.log".to_string()
        ],
        "exactly one file per spilled stream"
    );
    for (stream, byte) in [("stdout", b'o'), ("stderr", b'e')] {
        let path = spill_dir.join(format!("large-{stream}.log"));
        let bytes = std::fs::read(&path).expect("spill readable");
        assert_eq!(bytes.len(), 4096, "{stream} spill is complete");
        assert!(
            bytes.iter().all(|value| *value == byte),
            "{stream} spill carries the stream"
        );
    }

    // The working copy is gone; the host-owned file still answers the read.
    until(
        || (!scratch.state().join("large").exists()).then_some(()),
        Duration::from_secs(10),
        "the state directory is deleted",
    );
    for stream in ["stdout", "stderr"] {
        let read = host
            .request(
                "readStream",
                json!({"executionId": "large", "stream": stream, "offset": 0, "limit": 100_000}),
            )
            .expect("the read is answered");
        assert_eq!(
            read["text"].as_str().unwrap_or_default().len(),
            4096,
            "{stream} reads back completely: {read}"
        );
        assert_eq!(read["lossy"], false, "{read}");
        assert_eq!(
            read["spillPath"],
            json!(spill_dir.join(format!("large-{stream}.log"))),
            "{read}"
        );
    }

    // With the spill genuinely gone, the read reports loss instead of a clean
    // empty stream.
    std::fs::remove_file(spill_dir.join("large-stdout.log")).expect("spill removed");
    let lost = host
        .request(
            "readStream",
            json!({"executionId": "large", "stream": "stdout", "offset": 0, "limit": 100}),
        )
        .expect("a late read is answered");
    assert_eq!(lost["text"], "");
    assert_eq!(lost["lossy"], true, "{lost}");
}

/// A spill outlives the state directory, but not the retention window: the
/// host directory stays bounded instead of accumulating every command's output.
#[test]
fn a_spill_is_pruned_after_its_retention_window() {
    let scratch = Scratch::new("retain");
    let spill_dir = scratch.root.join("host-spill");
    std::fs::create_dir_all(&spill_dir).expect("spill directory");
    let executor = Executor::start_named(
        &scratch,
        "executor",
        &["--state-retention-ms", "0", "--spill-retention-ms", "400"],
    );
    let mut host = executor.ready();
    host.hello("boot-spill-retention");
    host.exec(
        "aging",
        "head -c 4096 /dev/zero | tr '\\0' a",
        json!({"capture": capture(&spill_dir)}),
    )
    .expect("execution admitted");
    until(
        || (spill_files(&spill_dir).len() == 1).then_some(()),
        Duration::from_secs(10),
        "the spill is written",
    );
    until(
        || (spill_files(&spill_dir).is_empty()).then_some(()),
        Duration::from_secs(10),
        "the spill is pruned after its retention window",
    );
    // The poll pass must not recreate a spill it just reclaimed.
    std::thread::sleep(Duration::from_millis(1_000));
    assert_eq!(
        spill_files(&spill_dir),
        Vec::<String>::new(),
        "a pruned spill was recreated"
    );
}

/// A half-written spill left by an interrupted copy is not a complete capture:
/// the executor replaces it before a reader can open the published path.
#[test]
fn a_partial_spill_is_replaced_before_it_is_read_as_complete() {
    let scratch = Scratch::new("partial");
    let spill_dir = scratch.root.join("host-spill");
    std::fs::create_dir_all(&spill_dir).expect("spill directory");
    let state = scratch.state();
    write_meta(
        &state,
        "partial",
        json!({
            "argv": ["/bin/sh", "-c", "exit 0"],
            "cwd": scratch.root,
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 10_000,
            "onExpiry": "kill",
            "graceMs": 100,
            "capture": capture(&spill_dir),
        }),
    );
    let directory = state.join("partial");
    // What a previous executor left behind: a terminal execution and a copy
    // that was interrupted part-way through.
    std::fs::write(
        directory.join("status.json"),
        serde_json::to_vec(&json!({
            "state": "exited", "pid": 1, "pgid": 1, "processStartTime": 1,
            "shimPid": 0, "shimStartTime": 0, "startedAt": 1, "finishedAt": 2,
            "exitCode": 0, "signal": null,
            "stdout": {"bytes": 4096, "lossy": false, "truncated": false},
            "stderr": {"bytes": 0, "lossy": false, "truncated": false},
            "error": null,
        }))
        .expect("status json"),
    )
    .expect("status write");
    std::fs::write(directory.join("stdout.log"), vec![b'p'; 4096]).expect("capture");
    std::fs::write(spill_dir.join("partial-stdout.log"), vec![b'p'; 100])
        .expect("interrupted copy");

    let executor = Executor::start_named(&scratch, "executor", &["--state-retention-ms", "0"]);
    let mut host = executor.ready();
    host.hello("boot-partial-spill");
    let read = until(
        || {
            host.request(
                "readStream",
                json!({"executionId": "partial", "stream": "stdout", "offset": 0, "limit": 100_000}),
            )
            .filter(|read| read["text"].as_str().is_some_and(|text| text.len() == 4096))
        },
        Duration::from_secs(10),
        "the partial spill is replaced with the complete capture",
    );
    assert_eq!(read["lossy"], false, "{read}");
    assert_eq!(
        read["spillPath"],
        json!(spill_dir.join("partial-stdout.log")),
        "{read}"
    );
    assert_eq!(
        std::fs::read(spill_dir.join("partial-stdout.log"))
            .expect("spill readable")
            .len(),
        4096,
        "the published spill is the complete capture"
    );
}
