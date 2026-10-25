//! Admission, adoption, leases and settlement reporting. Shims own execution.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::exec::{self, Counters, Execution};
use crate::protocol::ExecRequest;
use crate::shim::Meta;
use crate::store::Store;

pub struct Failure {
    pub code: &'static str,
    pub message: String,
}
impl Failure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

pub struct Registry {
    executions: Mutex<HashMap<String, Arc<Execution>>>,
    admission: Mutex<()>,
    boots: Mutex<HashMap<String, Instant>>,
    events: broadcast::Sender<Value>,
    max_concurrent: usize,
    lease_ttl: Duration,
    counters: Counters,
    started: Instant,
    store: Option<Arc<Store>>,
    state_dir: PathBuf,
    persistence_error: Mutex<Option<String>>,
}

impl Registry {
    #[cfg(test)]
    pub fn new(max_concurrent: usize, lease_ttl: Duration) -> Arc<Self> {
        let (events, _) = broadcast::channel(16);
        Arc::new(Self {
            executions: Mutex::new(HashMap::new()),
            admission: Mutex::new(()),
            boots: Mutex::new(HashMap::new()),
            events,
            max_concurrent,
            lease_ttl,
            counters: Counters::default(),
            started: Instant::now(),
            store: None,
            state_dir: PathBuf::new(),
            persistence_error: Mutex::new(None),
        })
    }
    pub fn with_store(
        max_concurrent: usize,
        lease_ttl: Duration,
        store: Option<Arc<Store>>,
        state_dir: PathBuf,
    ) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(&state_dir)?;
        let (events, _) = broadcast::channel(1024);
        let registry = Arc::new(Self {
            executions: Mutex::new(HashMap::new()),
            admission: Mutex::new(()),
            boots: Mutex::new(HashMap::new()),
            events,
            max_concurrent,
            lease_ttl,
            counters: Counters::default(),
            started: Instant::now(),
            store,
            state_dir,
            persistence_error: Mutex::new(None),
        });
        registry.adopt()?;
        Ok(registry)
    }

    fn adopt(&self) -> std::io::Result<()> {
        for entry in std::fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            match exec::load(entry.path()) {
                Ok(execution) => {
                    if execution.running() {
                        eprintln!(
                            "execution {}: adopted pid {:?}",
                            execution.meta.id,
                            execution.state().pid
                        );
                        self.boots
                            .lock()
                            .expect("boots poisoned")
                            .entry(execution.meta.boot_id.clone())
                            .or_insert(Instant::now() + self.lease_ttl);
                    }
                    self.executions
                        .lock()
                        .expect("executions poisoned")
                        .insert(execution.meta.id.clone(), execution);
                }
                Err(error) => eprintln!(
                    "execution {}: recovery failed: {error}",
                    entry.file_name().to_string_lossy()
                ),
            }
        }
        self.poll();
        Ok(())
    }

    pub fn history_health(&self) -> Value {
        let mut health = self
            .store
            .as_ref()
            .map(|store| store.health())
            .unwrap_or_else(
                || json!({"historyWritable":false,"historyError":"history database unavailable"}),
            );
        if let Some(error) = self
            .persistence_error
            .lock()
            .expect("persistence error poisoned")
            .as_ref()
        {
            health["historyWritable"] = json!(false);
            health["historyError"] = json!(error);
        }
        health
    }

    pub fn snapshot(&self) -> Value {
        let guard = self.executions.lock().expect("executions poisoned");
        let live = guard
            .values()
            .filter(|execution| execution.running())
            .count();
        let mut executions: Vec<_> = guard
            .values()
            .map(|execution| execution.summary())
            .collect();
        executions.sort_by_key(|row| std::cmp::Reverse(row["startedAtMs"].as_u64().unwrap_or(0)));
        executions.truncate(50);
        let mut result = json!({"version":format!("rust-{}",env!("CARGO_PKG_VERSION")),"protocols":["1.0","2.0"],
            "pid":std::process::id(),"uptimeMs":self.started.elapsed().as_millis() as u64,"live":live,
            "spawned":self.counters.spawned.load(Ordering::Relaxed),"settled":self.counters.settled.load(Ordering::Relaxed),
            "killed":self.counters.killed.load(Ordering::Relaxed),"failed":self.counters.failed.load(Ordering::Relaxed),
            "leases":self.boots.lock().expect("boots poisoned").len(),"maxConcurrent":self.max_concurrent,"executions":executions});
        for (key, value) in self.history_health().as_object().expect("health object") {
            result[key] = value.clone();
        }
        result
    }

    /// Observe never merges history: only the hub owns that policy.
    pub fn detail(&self, id: &str) -> Option<Value> {
        self.executions
            .lock()
            .ok()?
            .get(id)
            .filter(|execution| execution.running())
            .map(|execution| execution.detail())
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    /// Test hook: deliver one event to current subscribers.
    #[cfg(test)]
    pub fn publish(&self, event: Value) {
        let _ = self.events.send(event);
    }

    pub fn attach(&self, boot: &str) -> Vec<Value> {
        self.boots
            .lock()
            .expect("boots poisoned")
            .insert(boot.into(), Instant::now() + self.lease_ttl);
        self.executions.lock().expect("executions poisoned").values()
            .filter(|execution| execution.meta.boot_id == boot)
            .map(|execution| json!({"executionId":execution.meta.id,"state":execution.state().state})).collect()
    }

    pub fn ping(&self, boot: &str) -> Result<(), Failure> {
        let mut boots = self
            .boots
            .lock()
            .map_err(|_| Failure::new("internal", "boots poisoned"))?;
        let deadline = boots
            .get_mut(boot)
            .ok_or_else(|| Failure::new("protocol", "unknown boot; reconnect with hello"))?;
        *deadline = Instant::now() + self.lease_ttl;
        Ok(())
    }

    pub fn exec(&self, request: ExecRequest, boot: &str) -> Result<Value, Failure> {
        let _admission = self
            .admission
            .lock()
            .map_err(|_| Failure::new("internal", "admission poisoned"))?;
        let executions = self
            .executions
            .lock()
            .map_err(|_| Failure::new("internal", "executions poisoned"))?;
        if let Some(existing) = executions
            .values()
            .find(|execution| execution.meta.operation_id == request.operation_id)
        {
            if existing.meta.digest != request.spec_digest {
                return Err(Failure::new(
                    "rejected",
                    "operation id reused with a different spec",
                ));
            }
            return Ok(json!({"executionId":existing.meta.id}));
        }
        if executions.contains_key(&request.execution_id) {
            return Err(Failure::new("rejected", "execution id already exists"));
        }
        if executions
            .values()
            .filter(|execution| execution.running())
            .count()
            >= self.max_concurrent
        {
            return Err(Failure::new(
                "rejected",
                "executor concurrent execution limit reached",
            ));
        }
        drop(executions);
        if !valid_id(&request.execution_id)
            || request.spec.argv.is_empty()
            || request.spec.argv[0].is_empty()
        {
            return Err(Failure::new(
                "rejected",
                "invalid execution id or empty argv",
            ));
        }
        let store = self.store.as_ref().ok_or_else(|| {
            Failure::new("history", "history database unavailable; admission refused")
        })?;
        let meta = Meta {
            id: request.execution_id.clone(),
            operation_id: request.operation_id,
            digest: request.spec_digest,
            boot_id: boot.into(),
            spec: request.spec,
        };
        let execution = exec::admit(meta, &self.state_dir, store)
            .map_err(|error| Failure::new("rejected", error.to_string()))?;
        self.counters.spawned.fetch_add(1, Ordering::Relaxed);
        self.executions
            .lock()
            .expect("executions poisoned")
            .insert(request.execution_id.clone(), execution);
        Ok(json!({"executionId":request.execution_id}))
    }

    pub fn read_stream(
        &self,
        id: &str,
        stream: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Value, Failure> {
        self.get(id)?
            .read_stream(stream, offset, limit)
            .map_err(|error| Failure::new("capture", error.to_string()))
    }

    pub fn read_output(&self, id: &str) -> Result<Value, Failure> {
        let execution = self.get(id)?;
        let out = execution
            .read_stream("stdout", 0, crate::capture::READ_LIMIT)
            .map_err(|e| Failure::new("capture", e.to_string()))?;
        let err = execution
            .read_stream("stderr", 0, crate::capture::READ_LIMIT)
            .map_err(|e| Failure::new("capture", e.to_string()))?;
        let stdout = out["text"].as_str().unwrap_or("");
        let stderr = err["text"].as_str().unwrap_or("");
        let separator = if !stdout.is_empty() && !stdout.ends_with('\n') {
            "\n"
        } else {
            ""
        };
        Ok(
            json!({"delta": if stderr.is_empty() { stdout.to_string() } else { format!("{stdout}{separator}[stderr]\n{stderr}") },
            "lossy":out["lossy"] == true || err["lossy"] == true}),
        )
    }

    pub fn kill(&self, id: &str) -> Result<Value, Failure> {
        Ok(
            json!({"requested":self.get(id)?.kill().map_err(|error| Failure::new("capture",error.to_string()))?}),
        )
    }

    pub fn state(&self, id: &str) -> Result<Value, Failure> {
        let status = self.get(id)?.state();
        Ok(json!({"state":status.state,"exitCode":status.exit_code,"signal":status.signal}))
    }

    pub fn drain(&self, timeout_ms: u64) -> Value {
        for execution in self.all() {
            if let Err(error) = execution.kill() {
                eprintln!("execution {}: kill failed: {error}", execution.meta.id);
            }
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        while Instant::now() < deadline {
            self.poll();
            if self.all().iter().all(|execution| !execution.running()) {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        json!({"live":self.all().iter().filter(|execution| execution.running()).count()})
    }

    pub fn sweep(&self) {
        self.poll();
        let now = Instant::now();
        let expired: Vec<_> = self
            .boots
            .lock()
            .expect("boots poisoned")
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(boot, _)| boot.clone())
            .collect();
        for boot in expired {
            for execution in self
                .all()
                .into_iter()
                .filter(|execution| execution.meta.boot_id == boot && execution.running())
            {
                if let Err(error) = execution.kill() {
                    eprintln!(
                        "execution {}: lease termination failed: {error}",
                        execution.meta.id
                    );
                }
            }
            self.boots.lock().expect("boots poisoned").remove(&boot);
        }
    }

    fn poll(&self) {
        let mut persistence_error = None;
        for execution in self.all() {
            execution.refresh();
            if execution.dirty.load(Ordering::Relaxed) {
                let result = self
                    .store
                    .as_ref()
                    .ok_or_else(|| "history database unavailable".to_string())
                    .and_then(|store| execution.persist(store));
                if let Err(error) = result {
                    eprintln!(
                        "execution {}: history write failed (will retry): {error}",
                        execution.meta.id
                    );
                    persistence_error = Some(format!("execution {}: {error}", execution.meta.id));
                }
            }
            let status = execution.state();
            if status.state != "running" && !execution.announced.swap(true, Ordering::Relaxed) {
                match status.state.as_str() {
                    "killed" => &self.counters.killed,
                    "exited" => &self.counters.settled,
                    _ => &self.counters.failed,
                }
                .fetch_add(1, Ordering::Relaxed);
                let _ = self.events.send(crate::protocol::event(
                    "settled",
                    json!({"executionId":execution.meta.id,
                    "status":if status.state == "lost" { "failed" } else { &status.state },
                    "exitCode":status.exit_code,"signal":status.signal,"detail":status.error}),
                ));
            }
        }
        *self
            .persistence_error
            .lock()
            .expect("persistence error poisoned") = persistence_error;
    }

    fn all(&self) -> Vec<Arc<Execution>> {
        self.executions
            .lock()
            .expect("executions poisoned")
            .values()
            .cloned()
            .collect()
    }
    fn get(&self, id: &str) -> Result<Arc<Execution>, Failure> {
        self.executions
            .lock()
            .map_err(|_| Failure::new("internal", "executions poisoned"))?
            .get(id)
            .cloned()
            .ok_or_else(|| Failure::new("unknown-execution", format!("no execution {id}")))
    }
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && Path::new(id).components().count() == 1
        && id != "."
        && id != ".."
        && !id.contains('/')
        && !id.contains('\0')
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn admission_store_failure_is_returned_without_spawning() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/admission-failure");
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("history.sqlite");
        drop(Store::open(&db).unwrap());
        let store = Arc::new(Store::open_read_only(&db).unwrap());
        let registry =
            Registry::with_store(4, Duration::from_secs(60), Some(store), dir.join("state"))
                .unwrap();
        let request: ExecRequest = serde_json::from_value(json!({"executionId":"rejected","operationId":"op","specDigest":"digest",
            "spec":{"argv":["/bin/sh","-c","exit 0"],"cwd":dir,"env":{},"timeoutMs":500,"onExpiry":"kill","graceMs":10,
            "capture":{"stdoutMaxBytes":100,"stderrMaxBytes":100,"spillMaxBytes":0,"spillDir":dir}}})).unwrap();
        let error = registry
            .exec(request, "boot")
            .expect_err("admission must fail");
        assert!(error.message.contains("history admission write failed"));
        assert!(registry.attach("boot").is_empty());
        assert_eq!(registry.history_health()["historyWritable"], false);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_unpublished_shim_is_adopted_and_settles_from_its_status() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/unpublished-adoption");
        let state = dir.join("state");
        let execution_dir = state.join("starting");
        std::fs::create_dir_all(&execution_dir).unwrap();
        let meta: Meta = serde_json::from_value(json!({"id":"starting","operationId":"op","digest":"d","bootId":"boot",
            "argv":["/bin/sh","-c","exit 7"],"cwd":dir,"env":{},"timeoutMs":100,"onExpiry":"kill","graceMs":10,
            "capture":{"stdoutMaxBytes":10,"stderrMaxBytes":10,"spillMaxBytes":0,"spillDir":state}})).unwrap();
        crate::shim::atomic_json(&execution_dir.join("meta.json"), &meta).unwrap();
        let store = Arc::new(Store::open(&dir.join("history.sqlite")).unwrap());
        // Admitted, but the shim has not published its identity yet.
        let registry = Registry::with_store(
            4,
            Duration::from_secs(60),
            Some(Arc::clone(&store)),
            state.clone(),
        )
        .unwrap();
        assert_eq!(
            registry
                .state("starting")
                .unwrap_or_else(|_| panic!("an unpublished shim is adopted"))["state"],
            "running"
        );

        // This process stands in for both the shim and its command.
        let pid = std::process::id() as i32;
        let start = crate::shim::process_start_time(pid).unwrap();
        let published = |state: &str, finished: Value, exit: Value| {
            json!({"state":state,"pid":pid,"pgid":pid,"processStartTime":start,
                "shimPid":pid,"shimStartTime":start,"startedAt":1,"finishedAt":finished,
                "exitCode":exit,"signal":null,
                "stdout":{"bytes":0,"lossy":false,"truncated":false},
                "stderr":{"bytes":0,"lossy":false,"truncated":false},"error":null})
        };
        let status = execution_dir.join("status.json");
        crate::shim::atomic_json(&status, &published("running", Value::Null, Value::Null)).unwrap();
        registry.sweep();
        assert_eq!(
            registry
                .state("starting")
                .unwrap_or_else(|_| panic!("running"))["state"],
            "running"
        );

        crate::shim::atomic_json(&status, &published("exited", json!(2), json!(7))).unwrap();
        registry.sweep();
        assert_eq!(
            registry
                .state("starting")
                .unwrap_or_else(|_| panic!("settled"))["exitCode"],
            7
        );
        assert_eq!(store.detail("starting").unwrap().unwrap()["exitCode"], 7);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_unpublished_shim_older_than_the_grace_is_lost() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/unpublished-stale");
        let state = dir.join("state");
        let execution_dir = state.join("stale");
        std::fs::create_dir_all(&execution_dir).unwrap();
        let meta: Meta = serde_json::from_value(json!({"id":"stale","operationId":"op","digest":"d","bootId":"boot",
            "argv":["/bin/sh","-c","exit 0"],"cwd":dir,"env":{},"timeoutMs":100,"onExpiry":"kill","graceMs":10,
            "capture":{"stdoutMaxBytes":10,"stderrMaxBytes":10,"spillMaxBytes":0,"spillDir":state}})).unwrap();
        crate::shim::atomic_json(&execution_dir.join("meta.json"), &meta).unwrap();
        // Backdate the admission directory past the startup grace.
        let old =
            std::time::SystemTime::now() - Duration::from_millis(crate::shim::STARTUP_GRACE_MS * 2);
        std::fs::File::open(&execution_dir)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let store = Arc::new(Store::open(&dir.join("history.sqlite")).unwrap());
        let registry =
            Registry::with_store(4, Duration::from_secs(60), Some(Arc::clone(&store)), state)
                .unwrap();
        assert_eq!(
            registry.state("stale").unwrap_or_else(|_| panic!("loaded"))["state"],
            "lost"
        );
        assert_eq!(store.detail("stale").unwrap().unwrap()["state"], "lost");
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_settlement_is_retained_and_retried() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/settlement-retry");
        let state = dir.join("state");
        let execution_dir = state.join("retry");
        std::fs::create_dir_all(&execution_dir).unwrap();
        let db = dir.join("history.sqlite");
        let store = Arc::new(Store::open(&db).unwrap());
        let meta: Meta = serde_json::from_value(json!({"id":"retry","operationId":"op","digest":"d","bootId":"boot",
            "argv":["/bin/sh"],"cwd":dir,"env":{},"timeoutMs":100,"onExpiry":"kill","graceMs":10,
            "capture":{"stdoutMaxBytes":10,"stderrMaxBytes":10,"spillMaxBytes":0,"spillDir":state}})).unwrap();
        crate::shim::atomic_json(&execution_dir.join("meta.json"), &meta).unwrap();
        crate::shim::atomic_json(&execution_dir.join("status.json"), &json!({"state":"exited","pid":null,"pgid":null,
            "processStartTime":null,"shimPid":0,"shimStartTime":0,"startedAt":1,"finishedAt":2,"exitCode":7,"signal":null,
            "stdout":{"bytes":4,"lossy":true,"truncated":true},"stderr":{"bytes":0,"lossy":false,"truncated":false},"error":null})).unwrap();
        std::fs::write(execution_dir.join("stdout.log"), "kept").unwrap();
        let blocker = rusqlite::Connection::open(&db).unwrap();
        blocker.execute_batch("CREATE TRIGGER fail_save BEFORE INSERT ON executions BEGIN SELECT RAISE(FAIL, 'injected settlement failure'); END;").unwrap();
        let registry =
            Registry::with_store(4, Duration::from_secs(60), Some(Arc::clone(&store)), state)
                .unwrap();
        assert_eq!(registry.history_health()["historyWritable"], false);
        assert!(registry.history_health()["historyError"]
            .as_str()
            .unwrap()
            .contains("retry"));
        assert_eq!(store.count().unwrap(), 0);
        blocker.execute_batch("DROP TRIGGER fail_save").unwrap();
        registry.sweep();
        let saved = store.detail("retry").unwrap().unwrap();
        assert_eq!(saved["exitCode"], 7);
        assert_eq!(saved["stdout"], "kept");
        assert_eq!(saved["stdoutTruncated"], true);
        assert_eq!(registry.history_health()["historyWritable"], true);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
