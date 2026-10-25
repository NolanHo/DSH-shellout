//! Protocol 1.0 compatibility.
//!
//! An older host sends a resolved *command* rather than a spawn spec: it lets
//! the runner resolve the shell argv, scrub the environment and pick capture
//! budgets. New hosts send the finished spec. Accepting both keeps a host and
//! a runner from two different releases working across a rolling restart.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::protocol::{CapturePolicy, SpawnSpec};

/// Environment names an old host expected the runner to set itself.
const ENV_OVERRIDES: [(&str, &str); 4] = [
    ("NO_COLOR", "1"),
    ("TERM", "dumb"),
    ("PAGER", "cat"),
    ("GIT_PAGER", "cat"),
];

/// A protocol 1.0 execution spec.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacySpec {
    /// Command line handed to `bash -c`.
    pub command: String,
    /// Working directory.
    pub workdir: String,
    /// Deadline in milliseconds.
    pub timeout_ms: u64,
    /// `kill` or `none`.
    pub on_expiry: String,
    /// stdout retention cap.
    pub stdout_max_bytes: usize,
    /// Bytes written to stdin.
    #[serde(default)]
    pub stdin: Option<String>,
    /// Caller environment entries.
    #[serde(default)]
    pub env: Option<BTreeMap<String, String>>,
    /// Trusted snapshot environment entries.
    #[serde(default)]
    pub dsh_env: Option<BTreeMap<String, String>>,
}

/// A protocol 1.0 `exec` request body.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyExecRequest {
    /// Idempotency key.
    pub operation_id: String,
    /// Execution identity.
    pub execution_id: String,
    /// The legacy spec.
    pub spec: LegacySpec,
    /// Stable digest.
    pub spec_digest: String,
}

/// True when a raw `exec` payload is a protocol 1.0 request.
pub fn is_legacy(payload: &serde_json::Value) -> bool {
    payload
        .get("spec")
        .map(|spec| spec.get("argv").is_none() && spec.get("command").is_some())
        .unwrap_or(false)
}

/// Convert a legacy spec into the resolved spawn spec the rest of the runner uses.
pub fn to_spawn_spec(spec: LegacySpec, grace_ms: u64, spill_max: u64, spill_dir: &str) -> SpawnSpec {
    let mut env: BTreeMap<String, String> = std::env::vars()
        .filter(|(key, _)| !is_sensitive(key))
        .collect();
    for (key, value) in ENV_OVERRIDES {
        env.insert(key.to_string(), value.to_string());
    }
    for source in [spec.env, spec.dsh_env].into_iter().flatten() {
        for (key, value) in source {
            env.insert(key, value);
        }
    }
    SpawnSpec {
        argv: vec!["bash".to_string(), "-c".to_string(), spec.command],
        cwd: spec.workdir,
        env,
        stdin: spec.stdin,
        timeout_ms: spec.timeout_ms,
        on_expiry: spec.on_expiry,
        grace_ms,
        capture: CapturePolicy {
            stdout_max_bytes: spec.stdout_max_bytes,
            stderr_max_bytes: 64_000,
            spill_max_bytes: spill_max,
            spill_dir: spill_dir.to_string(),
        },
    }
}

/// True for names an old host expected the runner to strip from the child.
fn is_sensitive(key: &str) -> bool {
    let upper = key.to_ascii_uppercase();
    upper.starts_with("DSH_")
        || ["KEY", "PASSWORD", "SECRET", "TOKEN"]
            .iter()
            .any(|needle| upper.contains(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_requests_are_detected_by_shape() {
        let legacy = serde_json::json!({ "spec": { "command": "ls", "workdir": "/tmp" } });
        let current = serde_json::json!({ "spec": { "argv": ["ls"], "cwd": "/tmp" } });
        assert!(is_legacy(&legacy));
        assert!(!is_legacy(&current));
    }

    #[test]
    fn legacy_spec_becomes_a_bash_spawn_spec() {
        let spec = LegacySpec {
            command: "echo hi".to_string(),
            workdir: "/tmp".to_string(),
            timeout_ms: 5_000,
            on_expiry: "kill".to_string(),
            stdout_max_bytes: 4_096,
            stdin: None,
            env: Some(BTreeMap::from([("A".to_string(), "1".to_string())])),
            dsh_env: None,
        };
        let spawn = to_spawn_spec(spec, 3_000, 1_048_576, "/tmp");
        assert_eq!(spawn.argv, vec!["bash", "-c", "echo hi"]);
        assert_eq!(spawn.env.get("A").map(String::as_str), Some("1"));
        assert_eq!(spawn.env.get("TERM").map(String::as_str), Some("dumb"));
        assert_eq!(spawn.capture.stderr_max_bytes, 64_000);
        assert!(spawn.env.keys().all(|key| !key.starts_with("DSH_")));
    }
}
