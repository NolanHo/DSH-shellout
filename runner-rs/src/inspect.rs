//! Operator view of the execution plane, resolved from its own state.
//!
//! Answering "which executor is running and what is it executing?" by scanning
//! processes requires a `pgrep -f` pattern, and any pattern narrow enough to
//! match the executor also matches the shell that typed it. The executor
//! therefore publishes its identity into the state directory, and this module
//! reads that file plus the per-execution status documents — no process
//! scanning, no pattern that can match the observer.

use std::path::{Path, PathBuf};

use serde_json::{json, Value};

use crate::shim::{self, process_start_time};

/// File the executor writes so operators and scripts can find it by identity.
pub const IDENTITY: &str = "executor.json";

/// One execution as the status view reports it.
struct Row {
    id: String,
    state: String,
    pid: Option<i32>,
    shim_pid: i32,
    age_seconds: u64,
    command: String,
}

/// Publish this executor's identity into the state directory.
pub fn publish(state: &Path, socket: &str) -> std::io::Result<()> {
    let pid = std::process::id();
    let document = json!({
        "pid": pid,
        "startTime": process_start_time(pid as i32).unwrap_or(0),
        "version": env!("CARGO_PKG_VERSION"),
        "socket": socket,
        "startedAt": epoch_millis(),
    });
    let path = state.join(IDENTITY);
    let temporary = state.join(format!(".{IDENTITY}.{pid}"));
    std::fs::write(&temporary, serde_json::to_vec(&document)?)?;
    std::fs::rename(&temporary, &path)
}

/// Remove the identity file on a clean shutdown.
pub fn withdraw(state: &Path) {
    let _ = std::fs::remove_file(state.join(IDENTITY));
}

/// Wall clock in epoch milliseconds.
fn epoch_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_millis() as u64)
        .unwrap_or(0)
}

/// Read the published identity and say whether that process still exists.
fn executor(state: &Path) -> Option<Value> {
    let text = std::fs::read_to_string(state.join(IDENTITY)).ok()?;
    let mut document: Value = serde_json::from_str(&text).ok()?;
    let pid = document.get("pid")?.as_i64()? as i32;
    let recorded = document
        .get("startTime")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    // A recycled pid must not be mistaken for the executor.
    let alive = process_start_time(pid)
        .map(|current| current == recorded)
        .unwrap_or(false);
    document["alive"] = Value::Bool(alive);
    Some(document)
}

/// Every execution the state directory still holds.
fn rows(state: &Path) -> Vec<Row> {
    let mut collected = Vec::new();
    let Ok(entries) = std::fs::read_dir(state) else {
        return collected;
    };
    for entry in entries.flatten() {
        if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            continue;
        }
        let directory = entry.path();
        let Ok(text) = std::fs::read_to_string(directory.join("status.json")) else {
            continue;
        };
        let Ok(status) = serde_json::from_str::<shim::Status>(&text) else {
            continue;
        };
        let command = std::fs::read_to_string(directory.join("meta.json"))
            .ok()
            .and_then(|meta| serde_json::from_str::<Value>(&meta).ok())
            .and_then(|meta| {
                meta.get("argv").and_then(Value::as_array).map(|argv| {
                    argv.iter()
                        .filter_map(Value::as_str)
                        .collect::<Vec<_>>()
                        .join(" ")
                })
            })
            .unwrap_or_default();
        collected.push(Row {
            id: entry.file_name().to_string_lossy().into_owned(),
            state: status.state,
            pid: status.pid,
            shim_pid: status.shim_pid,
            age_seconds: epoch_millis().saturating_sub(status.started_at) / 1000,
            command,
        });
    }
    collected.sort_by(|a, b| b.age_seconds.cmp(&a.age_seconds));
    collected
}

/// Print the operator view; the exit code is 1 when no executor answers.
pub fn status(state: &Path, as_json: bool) -> i32 {
    let executor = executor(state);
    let rows = rows(state);
    let alive = executor
        .as_ref()
        .and_then(|document| document.get("alive"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if as_json {
        let document = json!({
            "state": state.display().to_string(),
            "executor": executor,
            "executions": rows.iter().map(|row| json!({
                "id": row.id,
                "state": row.state,
                "pid": row.pid,
                "shimPid": row.shim_pid,
                "ageSeconds": row.age_seconds,
                "command": row.command,
            })).collect::<Vec<_>>(),
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&document).unwrap_or_default()
        );
    } else {
        match executor.as_ref() {
            Some(document) => println!(
                "executor  pid={} alive={} version={} socket={}",
                document.get("pid").and_then(Value::as_i64).unwrap_or(0),
                if alive { "yes" } else { "no" },
                document
                    .get("version")
                    .and_then(Value::as_str)
                    .unwrap_or("?"),
                document
                    .get("socket")
                    .and_then(Value::as_str)
                    .unwrap_or("?"),
            ),
            None => println!("executor  unknown (no {} in {})", IDENTITY, state.display()),
        }
        if rows.is_empty() {
            println!("executions none");
        } else {
            println!("executions {}", rows.len());
            for row in &rows {
                let pid = row
                    .pid
                    .map(|pid| pid.to_string())
                    .unwrap_or_else(|| "-".into());
                let command: String = row.command.chars().take(80).collect();
                println!(
                    "  {:<40} {:<8} pid={:<8} shim={:<8} {:>5}s  {}",
                    row.id, row.state, pid, row.shim_pid, row.age_seconds, command
                );
            }
        }
    }
    if alive {
        0
    } else {
        1
    }
}

/// State directory the operator view reads when none was given.
pub fn default_state(state: &Path) -> PathBuf {
    state.to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!("dsh-inspect-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        root
    }

    #[test]
    fn reports_an_executor_whose_recorded_start_time_matches() {
        let state = scratch("alive");
        publish(&state, "/tmp/control.sock").unwrap();
        let document = executor(&state).expect("identity");
        assert_eq!(document["alive"], true, "{document}");
        withdraw(&state);
        assert!(executor(&state).is_none());
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn rejects_a_recycled_pid_with_a_different_start_time() {
        let state = scratch("recycled");
        // This process exists, but its start time cannot equal the recorded one.
        let document = json!({ "pid": std::process::id(), "startTime": 1, "socket": "x" });
        std::fs::write(state.join(IDENTITY), document.to_string()).unwrap();
        let found = executor(&state).expect("identity");
        assert_eq!(found["alive"], false, "{found}");
        let _ = std::fs::remove_dir_all(&state);
    }

    #[test]
    fn lists_executions_from_their_status_documents() {
        let state = scratch("rows");
        let directory = state.join("exec-1");
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(
            directory.join("status.json"),
            json!({
                "state": "running", "pid": 42, "pgid": 42, "processStartTime": 1,
                "shimPid": 43, "shimStartTime": 2, "startedAt": epoch_millis() - 5_000,
                "finishedAt": null, "exitCode": null, "signal": null,
                "stdout": { "bytes": 0, "truncated": false, "lossy": false },
                "stderr": { "bytes": 0, "truncated": false, "lossy": false },
                "error": null
            })
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            directory.join("meta.json"),
            json!({ "argv": ["/bin/sh", "-c", "sleep 5"], "cwd": "/tmp" }).to_string(),
        )
        .unwrap();
        let rows = rows(&state);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "exec-1");
        assert_eq!(rows[0].state, "running");
        assert_eq!(rows[0].command, "/bin/sh -c sleep 5");
        let _ = std::fs::remove_dir_all(&state);
    }
}
