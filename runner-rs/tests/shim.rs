//! The shim's own contract: it publishes terminal facts, bounds its cleanup,
//! names who terminated it, and cannot be stalled by a descendant.
mod support;

use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::json;
use support::{read_json, until, write_meta, Scratch, BINARY};

fn spawn_shim(state: &Path, id: &str) -> Child {
    Command::new(BINARY)
        .arg("--mode")
        .arg("shim")
        .arg("--state")
        .arg(state)
        .arg("--id")
        .arg(id)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("shim starts")
}

/// Read the published status, or `None` while it is missing or mid-replace.
fn status_of(state: &Path, id: &str) -> Option<serde_json::Value> {
    let text = std::fs::read_to_string(state.join(id).join("status.json")).ok()?;
    serde_json::from_str(&text).ok()
}

fn terminal(state: &Path, id: &str, timeout: Duration) -> serde_json::Value {
    until(
        || status_of(state, id).filter(|status| status["state"] != "running"),
        timeout,
        "terminal status",
    )
}

#[test]
fn a_descendant_outside_the_group_cannot_block_settlement() {
    let scratch = Scratch::new("detached");
    let state = scratch.state();
    write_meta(
        &state,
        "detached",
        json!({
            "argv": ["/bin/sh", "-c", "setsid sleep 5 & echo x"],
            "cwd": scratch.root,
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 10_000,
            "onExpiry": "kill",
            "graceMs": 200,
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
        }),
    );
    let started = Instant::now();
    let mut shim = spawn_shim(&state, "detached");
    let status = terminal(&state, "detached", Duration::from_secs(3));
    let settled = started.elapsed();
    shim.wait().expect("shim exits");
    assert!(
        settled < Duration::from_secs(3),
        "settlement took {settled:?}"
    );
    // The command itself exited 0; the stream that a detached descendant kept
    // open is reported as lossy instead of blocking the supervisor forever.
    assert_eq!(status["state"], "exited");
    assert_eq!(status["exitCode"], 0);
    assert_eq!(status["stdout"]["lossy"], true);
    assert!(status["finishedAt"].as_u64().is_some());
}

#[test]
fn an_unchanged_status_is_not_rewritten() {
    use std::os::unix::fs::MetadataExt;

    let scratch = Scratch::new("quiet");
    let state = scratch.state();
    write_meta(
        &state,
        "quiet",
        json!({
            "argv": ["/bin/sh", "-c", "sleep 1"],
            "cwd": scratch.root,
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 10_000,
            "onExpiry": "kill",
            "graceMs": 100,
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
        }),
    );
    let mut shim = spawn_shim(&state, "quiet");
    let path = state.join("quiet").join("status.json");
    let first = until(
        || {
            status_of(&state, "quiet")
                .filter(|status| status["pid"].as_i64().is_some())
                .and_then(|_| std::fs::metadata(&path).ok().map(|meta| meta.ino()))
        },
        Duration::from_secs(5),
        "spawned status",
    );
    std::thread::sleep(Duration::from_millis(600));
    let second = std::fs::metadata(&path).expect("status").ino();
    assert_eq!(first, second, "a quiet execution rewrote its status file");
    terminal(&state, "quiet", Duration::from_secs(5));
    shim.wait().expect("shim exits");
}

#[test]
fn a_spawn_failure_publishes_identity_and_the_error() {
    let scratch = Scratch::new("spawnerr");
    let state = scratch.state();
    write_meta(
        &state,
        "no-cwd",
        json!({
            "argv": ["/bin/true"],
            "cwd": scratch.root.join("missing-directory"),
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 10_000,
            "onExpiry": "kill",
            "graceMs": 100,
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
        }),
    );
    let mut shim = spawn_shim(&state, "no-cwd");
    assert!(shim.wait().expect("shim exits").code().is_some());
    let status = read_json(&state.join("no-cwd").join("status.json"));
    assert_eq!(status["state"], "failed");
    assert!(
        status["spawnError"]
            .as_str()
            .unwrap_or_default()
            .contains("/bin/true"),
        "spawn error named the program: {status}"
    );
    // The supervisor's identity is published even when no command exists.
    assert!(status["shimPid"].as_i64().unwrap_or_default() > 0);
    assert!(status["shimStartTime"].as_u64().unwrap_or_default() > 0);
    assert!(status["pid"].is_null());
}

#[test]
fn environment_values_are_cleared_once_the_execution_settled() {
    let scratch = Scratch::new("envscrub");
    let state = scratch.state();
    write_meta(
        &state,
        "scrub",
        json!({
            "argv": ["/bin/sh", "-c", "exit 0"],
            "cwd": scratch.root,
            "env": {"PATH": "/usr/bin:/bin", "SECRET_TOKEN": "hunter2"},
            "timeoutMs": 10_000,
            "onExpiry": "kill",
            "graceMs": 100,
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
        }),
    );
    let mut shim = spawn_shim(&state, "scrub");
    shim.wait().expect("shim exits");
    let meta = read_json(&state.join("scrub").join("meta.json"));
    assert_eq!(meta["env"], json!({}));
    assert_eq!(meta["argv"], json!(["/bin/sh", "-c", "exit 0"]));
    assert_eq!(meta["cwd"], json!(scratch.root));
}

/// The executor creates the spawn claim before it starts the shim and decides
/// the outcome; a shim that starts late must honour that decision instead of
/// running a command the caller was told was rejected.
#[test]
fn a_shim_does_not_run_a_command_the_executor_rejected() {
    let scratch = Scratch::new("rejected-spawn");
    let state = scratch.state();
    write_meta(
        &state,
        "rejected",
        json!({
            "argv": ["/bin/sh", "-c", "echo ran > ran.marker"],
            "cwd": scratch.root,
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 10_000,
            "onExpiry": "kill",
            "graceMs": 100,
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
        }),
    );
    let dir = state.join("rejected");
    std::fs::create_dir_all(&dir).expect("execution directory");
    // What the executor leaves behind when its admission wait expires.
    std::fs::write(dir.join("spawn.claim"), b"executor").expect("claim");
    std::fs::write(dir.join("spawn.verdict"), b"fail").expect("verdict");

    let mut shim = spawn_shim(&state, "rejected");
    assert!(
        !shim.wait().expect("shim exits").success(),
        "a refused spawn exits non-zero"
    );
    assert!(
        !scratch.root.join("ran.marker").exists(),
        "a rejected spawn must never run its command"
    );
    let status = read_json(&dir.join("status.json"));
    assert_eq!(status["state"], "failed");
    assert!(
        status["spawnError"]
            .as_str()
            .is_some_and(|error| error.contains("refused")),
        "the shim records why the command never started: {status}"
    );
}

#[test]
fn a_termination_request_records_the_party_that_asked() {
    let scratch = Scratch::new("term");
    let state = scratch.state();
    write_meta(
        &state,
        "lease",
        json!({
            "argv": ["/bin/sh", "-c", "trap '' TERM; sleep 30"],
            "cwd": scratch.root,
            "env": {"PATH": "/usr/bin:/bin"},
            "timeoutMs": 60_000,
            "onExpiry": "kill",
            "graceMs": 200,
            "capture": {"stdoutMaxBytes": 64_000, "stderrMaxBytes": 64_000,
                "spillMaxBytes": 1_048_576, "spillDir": "/tmp"},
        }),
    );
    let mut shim = spawn_shim(&state, "lease");
    until(
        || status_of(&state, "lease").filter(|status| status["pid"].as_i64().is_some()),
        Duration::from_secs(5),
        "command running",
    );
    std::fs::write(state.join("lease").join("kill.request"), "lease").expect("kill request");
    let status = terminal(&state, "lease", Duration::from_secs(5));
    shim.wait().expect("shim exits");
    assert_eq!(status["state"], "killed");
    assert_eq!(status["termination"], "lease");
    assert_eq!(status["signal"], 9);
}
