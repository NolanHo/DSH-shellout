//! Execution table: idempotent admission, per-boot leases, settlement events.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::exec::{spawn_execution, Execution};
use crate::protocol::ExecRequest;

/// Admission or lookup failure carrying a wire error code.
pub struct Failure {
    /// Wire error code.
    pub code: &'static str,
    /// Human-readable detail.
    pub message: String,
}

impl Failure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Failure { code, message: message.into() }
    }
}

/// The runner's ownership table.
pub struct Registry {
    executions: Mutex<HashMap<String, Arc<Execution>>>,
    operations: Mutex<HashMap<String, String>>,
    boots: Mutex<HashMap<String, Instant>>,
    events: broadcast::Sender<Value>,
    max_concurrent: usize,
    lease_ttl: Duration,
}

impl Registry {
    /// Create a registry; the returned receiver observes settlement events.
    pub fn new(max_concurrent: usize, lease_ttl: Duration) -> (Arc<Registry>, broadcast::Receiver<Value>) {
        let (events, receiver) = broadcast::channel(1024);
        let registry = Arc::new(Registry {
            executions: Mutex::new(HashMap::new()),
            operations: Mutex::new(HashMap::new()),
            boots: Mutex::new(HashMap::new()),
            events,
            max_concurrent,
            lease_ttl,
        });
        (registry, receiver)
    }

    /// Subscribe to settlement events.
    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    /// Start or refresh one boot's lease and report its live executions.
    pub fn attach(&self, boot_id: &str) -> Vec<Value> {
        if let Ok(mut boots) = self.boots.lock() {
            boots.insert(boot_id.to_string(), Instant::now() + self.lease_ttl);
        }
        let executions = self.executions.lock().ok();
        executions
            .map(|guard| {
                guard
                    .values()
                    .filter(|execution| execution.boot_id == boot_id)
                    .map(|execution| {
                        let state = execution.state();
                        json!({
                            "executionId": execution.id,
                            "state": if state.running { "running" } else if state.killed { "killed" } else { "exited" },
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Refresh a boot's lease.
    pub fn ping(&self, boot_id: &str) -> Result<(), Failure> {
        let mut boots = self.boots.lock().map_err(|_| Failure::new("internal", "registry poisoned"))?;
        if !boots.contains_key(boot_id) {
            return Err(Failure::new("protocol", format!("unknown boot {boot_id}; reconnect with hello")));
        }
        boots.insert(boot_id.to_string(), Instant::now() + self.lease_ttl);
        Ok(())
    }

    /// Admit one execution, or return the existing one for a retried operation.
    pub fn exec(&self, request: ExecRequest, boot_id: &str) -> Result<Value, Failure> {
        if let Ok(operations) = self.operations.lock() {
            if let Some(existing) = operations.get(&request.operation_id) {
                let executions = self.executions.lock().map_err(|_| Failure::new("internal", "registry poisoned"))?;
                let execution = executions
                    .get(existing)
                    .ok_or_else(|| Failure::new("internal", "operation lost its execution"))?;
                if execution.digest != request.spec_digest {
                    return Err(Failure::new("rejected", "operation id reused with a different spec"));
                }
                return Ok(json!({ "executionId": execution.id }));
            }
        }
        let live = self.live_count();
        if live >= self.max_concurrent {
            return Err(Failure::new("rejected", format!("runner is at its {}-execution limit", self.max_concurrent)));
        }
        let execution = spawn_execution(
            request.spec,
            request.execution_id.clone(),
            request.operation_id.clone(),
            request.spec_digest.clone(),
            boot_id.to_string(),
            self.events.clone(),
        )
        .map_err(|error| Failure::new("rejected", format!("spawn failed: {error}")))?;
        if let Ok(mut executions) = self.executions.lock() {
            executions.insert(request.execution_id.clone(), Arc::clone(&execution));
        }
        if let Ok(mut operations) = self.operations.lock() {
            operations.insert(request.operation_id.clone(), request.execution_id.clone());
        }
        Ok(json!({ "executionId": execution.id }))
    }

    /// Read one captured stream at an offset.
    pub fn read_stream(&self, execution_id: &str, stream: &str, offset: u64, limit: usize) -> Result<Value, Failure> {
        let execution = self.get(execution_id)?;
        let capture = if stream == "stderr" { &execution.stderr } else { &execution.stdout };
        let capture = capture.lock().map_err(|_| Failure::new("internal", "capture poisoned"))?;
        let read = capture.read_from(offset, limit);
        let mut value = json!({
            "text": read.text,
            "nextOffset": read.next_offset,
            "lossy": read.lossy,
        });
        if let Some(path) = read.spill_path {
            value["spillPath"] = Value::String(path);
        }
        Ok(value)
    }

    /// Consuming read over both streams, formatted like the harness executor.
    pub fn read_output(&self, execution_id: &str) -> Result<Value, Failure> {
        let execution = self.get(execution_id)?;
        let stdout = {
            let capture = execution.stdout.lock().map_err(|_| Failure::new("internal", "capture poisoned"))?;
            capture.read_from(0, usize::MAX)
        };
        let stderr = {
            let capture = execution.stderr.lock().map_err(|_| Failure::new("internal", "capture poisoned"))?;
            capture.read_from(0, usize::MAX)
        };
        let separator = if !stdout.text.is_empty() && !stdout.text.ends_with('\n') { "\n" } else { "" };
        let delta = if stderr.text.is_empty() {
            stdout.text
        } else {
            format!("{}{}[stderr]\n{}", stdout.text, separator, stderr.text)
        };
        Ok(json!({ "delta": delta, "lossy": stdout.lossy || stderr.lossy }))
    }

    /// Request termination.
    pub fn kill(&self, execution_id: &str) -> Result<Value, Failure> {
        let execution = self.get(execution_id)?;
        Ok(json!({ "requested": execution.kill() }))
    }

    /// Terminal facts at call time.
    pub fn state(&self, execution_id: &str) -> Result<Value, Failure> {
        let execution = self.get(execution_id)?;
        let state = execution.state();
        Ok(json!({
            "state": if state.running { "running" } else if state.killed { "killed" } else { "exited" },
            "exitCode": state.exit_code,
            "signal": state.signal,
        }))
    }

    /// Terminate live executions and wait for them to settle.
    pub fn drain(&self, timeout_ms: u64) -> Value {
        for execution in self.snapshot_live() {
            execution.kill();
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        while Instant::now() < deadline && self.live_count() > 0 {
            std::thread::sleep(Duration::from_millis(20));
        }
        json!({ "live": self.live_count() })
    }

    /// Terminate executions whose boot lease expired.
    pub fn sweep(&self) {
        let now = Instant::now();
        let expired: Vec<String> = self
            .boots
            .lock()
            .map(|boots| boots.iter().filter(|(_, deadline)| **deadline <= now).map(|(boot, _)| boot.clone()).collect())
            .unwrap_or_default();
        for boot in expired {
            for execution in self.snapshot_live().into_iter().filter(|execution| execution.boot_id == boot) {
                execution.kill();
                let _ = self.events.send(crate::protocol::event(
                    "settled",
                    json!({
                        "executionId": execution.id,
                        "status": "failed",
                        "exitCode": null,
                        "signal": null,
                        "detail": "lost: owning boot no longer holds a lease",
                    }),
                ));
            }
            if let Ok(mut boots) = self.boots.lock() {
                boots.remove(&boot);
            }
        }
    }

    fn get(&self, execution_id: &str) -> Result<Arc<Execution>, Failure> {
        self.executions
            .lock()
            .map_err(|_| Failure::new("internal", "registry poisoned"))?
            .get(execution_id)
            .cloned()
            .ok_or_else(|| Failure::new("unknown-execution", format!("no execution {execution_id}")))
    }

    fn live_count(&self) -> usize {
        self.snapshot_live().len()
    }

    fn snapshot_live(&self) -> Vec<Arc<Execution>> {
        self.executions
            .lock()
            .map(|executions| executions.values().filter(|execution| execution.state().running).cloned().collect())
            .unwrap_or_default()
    }
}
