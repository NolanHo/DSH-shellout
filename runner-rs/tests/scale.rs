//! The cost of dashboard observation on a real history.
//!
//! `observe` answers the hub's dashboard: it merges the registry snapshot with
//! the counters it derives from history. History is never pruned, so whatever
//! that derivation costs is paid on every poll and, because it runs under the
//! store lock, also charged to the admission and settlement writes behind it.
//! This test measures the pair on a synthetic 50 000-row history and pins the
//! counters to the rows they claim to summarize.
mod support;

use std::path::Path;
use std::time::{Duration, Instant};

use rusqlite::{params, Connection};
use serde_json::json;
use support::{until, Client, Executor, Scratch};

/// A row of the shape the executor writes, without output.
fn insert_rows(database: &Path, rows: usize) {
    let connection = Connection::open(database).expect("history opens");
    let states = ["exited", "killed", "failed", "lost"];
    let transaction = connection.unchecked_transaction().expect("transaction");
    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO executions
                 (id, argv, cwd, started_at, finished_at, state, exit_code, signal,
                  stdout, stderr, lossy, stdout_lossy, stderr_lossy, stdout_truncated, stderr_truncated)
                 VALUES (?1, ?2, '/tmp', ?3, ?4, ?5, 0, NULL, '', '', 0, 0, 0, 0, 0)",
            )
            .expect("prepared");
        for index in 0..rows {
            let state = states[index % states.len()];
            statement
                .execute(params![
                    format!("history-{index}"),
                    "[\"bash\",\"-c\",\"true\"]",
                    index as i64,
                    index as i64 + 1,
                    state,
                ])
                .expect("row inserted");
        }
    }
    transaction.commit().expect("commit");
    // The seeded rows stand in for a history an older executor wrote: no counter
    // row exists yet, so the next open has to derive it from the rows.
    connection
        .execute_batch("DROP TABLE IF EXISTS counters")
        .expect("counter row dropped");
}

/// Median and worst round-trip of `samples` repetitions of `probe`.
fn measure(samples: usize, mut probe: impl FnMut() -> Duration) -> (Duration, Duration) {
    let mut timings: Vec<Duration> = (0..samples).map(|_| probe()).collect();
    timings.sort();
    (
        timings[timings.len() / 2],
        *timings.last().expect("a sample"),
    )
}

/// Build the executor's schema, then fill it with `rows` terminal executions.
fn seeded(scratch: &Scratch, rows: usize) {
    let mut executor = Executor::start(scratch);
    executor.ready();
    executor.kill();
    insert_rows(&scratch.history(), rows);
}

/// An observer connection: it never handshakes, so it can only observe.
fn observer(executor: &Executor) -> Client {
    executor.ready()
}

/// A host connection: `hello` owns the executions it starts.
fn host_client(executor: &Executor, boot: &str) -> Client {
    let mut client = executor.ready();
    client.hello(boot);
    client
}

#[test]
fn observation_and_admission_stay_cheap_on_a_large_history() {
    let scratch = Scratch::new("scale");
    seeded(&scratch, 50_000);
    let executor = Executor::start(&scratch);
    let mut client = observer(&executor);

    let (observe_median, observe_worst) = measure(20, || {
        let started = Instant::now();
        let snapshot = client
            .request("observe", json!({}))
            .expect("the observation answers");
        assert_eq!(snapshot["spawned"], 50_000, "{snapshot}");
        started.elapsed()
    });
    eprintln!("observe over 50 000 rows: median {observe_median:?}, worst {observe_worst:?}");

    // The point of the measurement: observation must not get slower because
    // history is long. An empty history is the floor it may not meaningfully
    // exceed, whatever this machine's absolute speed is.
    let floor = {
        let empty = Scratch::new("scale-empty");
        let executor = Executor::start(&empty);
        let mut client = observer(&executor);
        let (median, _) = measure(20, || {
            let started = Instant::now();
            client
                .request("observe", json!({}))
                .expect("the observation answers");
            started.elapsed()
        });
        median
    };
    eprintln!("observe over an empty history: median {floor:?}");
    assert!(
        observe_median <= floor + Duration::from_millis(3),
        "observation scans the history: {observe_median:?} at 50 000 rows against {floor:?} empty"
    );

    // An admission while the dashboard polls: its history write waits for the
    // same store lock the observation holds.
    let mut host = host_client(&executor, "boot-scale-host");
    let base = Instant::now();
    host.exec("baseline", "exit 0", json!({}))
        .expect("execution admitted");
    let admission_baseline = base.elapsed();

    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let polling = {
        let stop = std::sync::Arc::clone(&stop);
        let polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = std::sync::Arc::clone(&polls);
        let worker = std::thread::spawn(move || {
            let mut client = client;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                if client.request("observe", json!({})).is_none() {
                    break;
                }
                counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        });
        (worker, polls)
    };
    let loaded = Instant::now();
    host.exec("under-observation", "exit 0", json!({}))
        .expect("execution admitted while the dashboard polls");
    let admission_loaded = loaded.elapsed();
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let (worker, polls) = polling;
    worker.join().expect("the polling observer stops");
    eprintln!(
        "admission: {:?} idle, {:?} while {} observations ran",
        admission_baseline,
        admission_loaded,
        polls.load(std::sync::atomic::Ordering::Relaxed)
    );

    // Both executions settle with their real facts; the counters keep counting
    // the pre-seeded history plus them.
    for (id, code) in [("baseline", 0), ("under-observation", 0)] {
        let state = until(
            || host.state(id).filter(|state| state["state"] != "running"),
            Duration::from_secs(10),
            "the execution settles",
        );
        assert_eq!(state["exitCode"], code, "{state}");
    }
    let snapshot = observer(&executor)
        .request("observe", json!({}))
        .expect("observation");
    assert_eq!(snapshot["spawned"], 50_002, "{snapshot}");
    assert_eq!(snapshot["settled"], 12_502, "{snapshot}");
    assert_eq!(snapshot["killed"], 12_500, "{snapshot}");
    assert_eq!(snapshot["failed"], 25_000, "{snapshot}");
}
