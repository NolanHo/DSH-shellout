//! Admission, adoption, leases and settlement reporting. Shims own execution.
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::sync::broadcast;

use crate::exec::{self, Counters, Execution, SpawnOutcome};
use crate::protocol::ExecRequest;
use crate::shim::{self, Meta};
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

/// Make a directory readable only by its owner.
fn private(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700));
}

pub struct Registry {
    executions: Mutex<HashMap<String, Arc<Execution>>>,
    /// Serializes admission decisions without holding a lock across the spawn.
    admission: Mutex<()>,
    /// Executions currently being admitted, by id to operation id. They are
    /// not in `executions` yet, but they already hold a concurrency slot.
    reservations: Mutex<std::collections::HashMap<String, String>>,
    boots: Mutex<HashMap<String, Instant>>,
    events: broadcast::Sender<Value>,
    max_concurrent: usize,
    lease_ttl: Duration,
    counters: Counters,
    started: Instant,
    store: Option<Arc<Store>>,
    state_dir: PathBuf,
    state_retention: Duration,
    spills: exec::SpillBook,
    persistence_error: Mutex<Option<String>>,
}

/// How long a settled execution's files stay readable after its row is durable,
/// so a reader that follows the settlement event can still fetch its tail. It
/// must outlast the host's `disconnectGraceMs`: a host that reconnects inside
/// its grace still has to be able to read the execution it missed.
pub const DEFAULT_STATE_RETENTION: Duration = Duration::from_secs(15);

/// How long the executor keeps a host-owned spill file after creating it. A
/// spill only exists for output larger than the host's in-memory cap, and it is
/// the path a finished result points at; the window bounds that promise.
pub const DEFAULT_SPILL_RETENTION: Duration = Duration::from_secs(600);

/// Total size of the spills this executor keeps before it evicts the oldest.
pub const DEFAULT_SPILL_BUDGET_BYTES: u64 = 256 * 1024 * 1024;

impl Registry {
    #[cfg(test)]
    pub fn new(max_concurrent: usize, lease_ttl: Duration) -> Arc<Self> {
        let (events, _) = broadcast::channel(16);
        Arc::new(Self {
            executions: Mutex::new(HashMap::new()),
            admission: Mutex::new(()),
            reservations: Mutex::new(std::collections::HashMap::new()),
            boots: Mutex::new(HashMap::new()),
            events,
            max_concurrent,
            lease_ttl,
            counters: Counters::default(),
            started: Instant::now(),
            store: None,
            state_dir: PathBuf::new(),
            state_retention: DEFAULT_STATE_RETENTION,
            spills: exec::SpillBook::new(DEFAULT_SPILL_RETENTION, DEFAULT_SPILL_BUDGET_BYTES),
            persistence_error: Mutex::new(None),
        })
    }
    #[cfg(test)]
    pub fn with_store(
        max_concurrent: usize,
        lease_ttl: Duration,
        store: Option<Arc<Store>>,
        state_dir: PathBuf,
    ) -> std::io::Result<Arc<Self>> {
        Self::with_retention(
            max_concurrent,
            lease_ttl,
            store,
            state_dir,
            DEFAULT_STATE_RETENTION,
        )
    }
    #[cfg(test)]
    pub fn with_retention(
        max_concurrent: usize,
        lease_ttl: Duration,
        store: Option<Arc<Store>>,
        state_dir: PathBuf,
        state_retention: Duration,
    ) -> std::io::Result<Arc<Self>> {
        Self::with_spill_policy(
            max_concurrent,
            lease_ttl,
            store,
            state_dir,
            state_retention,
            DEFAULT_SPILL_RETENTION,
            DEFAULT_SPILL_BUDGET_BYTES,
        )
    }

    /// Build a registry with the spill retention and budget the deployment wants.
    pub fn with_spill_policy(
        max_concurrent: usize,
        lease_ttl: Duration,
        store: Option<Arc<Store>>,
        state_dir: PathBuf,
        state_retention: Duration,
        spill_retention: Duration,
        spill_budget: u64,
    ) -> std::io::Result<Arc<Self>> {
        std::fs::create_dir_all(&state_dir)?;
        private(&state_dir);
        let (events, _) = broadcast::channel(1024);
        let registry = Arc::new(Self {
            executions: Mutex::new(HashMap::new()),
            admission: Mutex::new(()),
            reservations: Mutex::new(std::collections::HashMap::new()),
            boots: Mutex::new(HashMap::new()),
            events,
            max_concurrent,
            lease_ttl,
            counters: Counters::default(),
            started: Instant::now(),
            store,
            state_dir,
            state_retention,
            spills: exec::SpillBook::new(spill_retention, spill_budget),
            persistence_error: Mutex::new(None),
        });
        registry.adopt()?;
        Ok(registry)
    }

    /// History is a projection of the state directory: at startup every row
    /// must match a directory that still exists.
    fn adopt(&self) -> std::io::Result<()> {
        let store = self.store.clone();
        for entry in std::fs::read_dir(&self.state_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            match exec::load(entry.path(), store.as_deref()) {
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
        self.reconcile_history();
        Ok(())
    }

    /// A `running` row with no execution behind it is a fact, not a guess: the
    /// state directory it projected is gone, so the row becomes `lost`.
    fn reconcile_history(&self) {
        let Some(store) = self.store.as_ref() else {
            return;
        };
        let running = match store.running_ids() {
            Ok(ids) => ids,
            Err(error) => {
                eprintln!("history: cannot list running rows: {error}");
                return;
            }
        };
        for id in running {
            let live = self
                .executions
                .lock()
                .expect("executions poisoned")
                .get(&id)
                .is_some_and(|execution| execution.running());
            if live {
                continue;
            }
            if let Err(error) = store.mark_lost(&id, shim::now_ms()) {
                eprintln!("execution {id}: cannot mark its history row lost: {error}");
            }
        }
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
        // Counters describe the execution plane, not this process: history is
        // the authority, so a restart cannot reset a dashboard to zero.
        let counts = self.store.as_ref().and_then(|store| match store.counts() {
            Ok(counts) => Some(counts),
            Err(error) => {
                eprintln!("history: cannot count executions: {error}");
                None
            }
        });
        let (spawned, settled, killed, failed) = counts.map_or_else(
            || {
                (
                    self.counters.spawned.load(Ordering::Relaxed),
                    self.counters.settled.load(Ordering::Relaxed),
                    self.counters.killed.load(Ordering::Relaxed),
                    self.counters.failed.load(Ordering::Relaxed),
                )
            },
            |counts| (counts.rows, counts.exited, counts.killed, counts.failed),
        );
        let mut result = json!({"version":format!("rust-{}",env!("CARGO_PKG_VERSION")),"protocols":["1.0","2.0"],
            "pid":std::process::id(),"uptimeMs":self.started.elapsed().as_millis() as u64,"live":live,
            "spawned":spawned,"settled":settled,"killed":killed,"failed":failed,
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
            .map(|execution| execution.detail(&self.spills))
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Value> {
        self.events.subscribe()
    }

    /// Test hook: deliver one event to current subscribers.
    #[cfg(test)]
    pub fn publish(&self, event: Value) {
        let _ = self.events.send(event);
    }

    /// Register a host boot and report every execution it still owns, with the
    /// terminal facts a host needs to settle one whose event it never saw.
    pub fn attach(&self, boot: &str) -> Vec<Value> {
        self.boots
            .lock()
            .expect("boots poisoned")
            .insert(boot.into(), Instant::now() + self.lease_ttl);
        self.executions
            .lock()
            .expect("executions poisoned")
            .values()
            .filter(|execution| execution.meta.boot_id == boot)
            .map(|execution| execution.terminal_facts())
            .collect()
    }

    /// The settlements of one boot whose files are still retained, for the
    /// connection that just re-handshaked for it.
    ///
    /// A settlement emitted while no connection was subscribed would otherwise
    /// be lost: the execution is announced once and never again. The caller
    /// writes these frames to its own connection only, so a replay stays
    /// proportional to the reconnecting host's recent work and never reaches
    /// another host's stream.
    ///
    /// @param boot - the boot that reconnected.
    /// @returns the settlement event frames to replay on that connection.
    pub fn replay(&self, boot: &str) -> Vec<Value> {
        self.executions
            .lock()
            .expect("executions poisoned")
            .values()
            .filter(|execution| {
                execution.meta.boot_id == boot
                    && execution.state().state != "running"
                    && execution.retained()
            })
            .map(|execution| settled_event(execution))
            .collect()
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

    /// Admit one execution. The decision is serialized, but the spawn wait is
    /// not: it holds no lock, so other requests keep flowing while a shim starts.
    pub fn exec(&self, request: ExecRequest, boot: &str) -> Result<Value, Failure> {
        let id = request.execution_id.clone();
        {
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
            if !valid_id(&request.execution_id)
                || request.spec.argv.is_empty()
                || request.spec.argv[0].is_empty()
            {
                return Err(Failure::new(
                    "rejected",
                    "invalid execution id or empty argv",
                ));
            }
            let mut reservations = self
                .reservations
                .lock()
                .map_err(|_| Failure::new("internal", "reservations poisoned"))?;
            // Admission in flight is a live execution for the limit: six
            // simultaneous requests must not all pass a limit of two before
            // any of them reaches `executions`.
            if executions
                .values()
                .filter(|execution| execution.running())
                .count()
                .saturating_add(reservations.len())
                >= self.max_concurrent
            {
                return Err(Failure::new(
                    "rejected",
                    "executor concurrent execution limit reached",
                ));
            }
            if reservations
                .values()
                .any(|operation| *operation == request.operation_id)
            {
                return Err(Failure::new(
                    "rejected",
                    "operation id is already being admitted",
                ));
            }
            if reservations.contains_key(&id) {
                return Err(Failure::new(
                    "rejected",
                    "execution id is already being admitted",
                ));
            }
            reservations.insert(id.clone(), request.operation_id.clone());
        }
        let outcome = self.admit(request, boot);
        match self.reservations.lock() {
            Ok(mut reservations) => {
                reservations.remove(&id);
            }
            Err(_) => eprintln!("execution {id}: admission reservation release failed"),
        }
        outcome
    }

    /// The part of admission that owns no lock: spawn, wait, and register.
    fn admit(&self, request: ExecRequest, boot: &str) -> Result<Value, Failure> {
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
        // A command that could not be started is a preparation failure, so the
        // reply is an error rather than a handle that fails later.
        let admission = exec::admit(meta, &self.state_dir, store)
            .map_err(|error| Failure::new("rejected", error.to_string()))?;
        match admission.outcome {
            SpawnOutcome::Admitted | SpawnOutcome::Accepted => {
                self.counters.spawned.fetch_add(1, Ordering::Relaxed);
                self.executions
                    .lock()
                    .expect("executions poisoned")
                    .insert(request.execution_id.clone(), admission.execution);
                Ok(json!({"executionId":request.execution_id}))
            }
            SpawnOutcome::Refused { message, tracked } => {
                // A pre-verdict failure stays addressable: its durable row is
                // already terminal, and a shim that starts later is refused.
                if tracked {
                    self.executions
                        .lock()
                        .expect("executions poisoned")
                        .insert(request.execution_id.clone(), admission.execution);
                }
                Err(Failure::new("rejected", message))
            }
        }
    }

    pub fn read_stream(
        &self,
        id: &str,
        stream: &str,
        offset: u64,
        limit: usize,
    ) -> Result<Value, Failure> {
        self.get(id)?
            .read_stream(stream, offset, limit, &self.spills)
            .map_err(|error| Failure::new("capture", error.to_string()))
    }

    pub fn read_output(&self, id: &str) -> Result<Value, Failure> {
        let execution = self.get(id)?;
        let out = execution
            .read_stream("stdout", 0, crate::capture::READ_LIMIT, &self.spills)
            .map_err(|e| Failure::new("capture", e.to_string()))?;
        let err = execution
            .read_stream("stderr", 0, crate::capture::READ_LIMIT, &self.spills)
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
        self.kill_for(id, shim::TERMINATION_REQUEST)
    }

    fn kill_for(&self, id: &str, reason: &str) -> Result<Value, Failure> {
        Ok(
            json!({"requested":self.get(id)?.kill(reason).map_err(|error| Failure::new("capture",error.to_string()))?}),
        )
    }

    /// Recent settlements, newest first.
    ///
    /// A controller attaching to a remote host needs its history, not just the
    /// settlements that happened while it was listening.
    pub fn history(&self, limit: usize) -> Result<Value, Failure> {
        let store = self
            .store
            .clone()
            .ok_or_else(|| Failure::new("history", "no history store"))?;
        let rows = store
            .recent(limit)
            .map_err(|error| Failure::new("history", error))?;
        Ok(json!({ "rows": rows }))
    }

    pub fn state(&self, id: &str) -> Result<Value, Failure> {
        // Reflect a settlement the poller has not reached yet.
        self.poll();
        let status = self.get(id)?.state();
        Ok(
            json!({"state":status.state,"exitCode":status.exit_code,"signal":crate::protocol::signal_name(status.signal)}),
        )
    }

    pub fn drain(&self, timeout_ms: u64) -> Value {
        for execution in self.all() {
            if let Err(error) = execution.kill(shim::TERMINATION_DRAIN) {
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
        // Spills are host-owned files that outlive the state directory, so
        // nothing but this pass ever reclaims them.
        self.spills.prune();
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
                if let Err(error) = execution.kill(shim::TERMINATION_LEASE) {
                    eprintln!(
                        "execution {}: lease termination failed: {error}",
                        execution.meta.id
                    );
                }
            }
            self.boots.lock().expect("boots poisoned").remove(&boot);
        }
    }

    pub fn poll(&self) {
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
                let _ = self.events.send(settled_event(&execution));
            }
            self.retire(&execution);
        }
        *self
            .persistence_error
            .lock()
            .expect("persistence error poisoned") = persistence_error;
    }

    /// Delete the files of a settled execution once its row is durable and the
    /// window for a reader following its settlement event has passed.
    fn retire(&self, execution: &Execution) {
        // The host-owned spill must exist before the working copy is deleted.
        execution.ensure_spills(&self.spills);
        if execution.running()
            || execution.dirty.load(Ordering::Relaxed)
            || !execution.announced.load(Ordering::Relaxed)
        {
            return;
        }
        execution.schedule_cleanup(self.state_retention);
        if !execution.cleanup_due() {
            return;
        }
        if let Err(error) = execution.remove_state() {
            eprintln!(
                "execution {}: state cleanup failed: {error}",
                execution.meta.id
            );
        }
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

/// The event a settled execution publishes, once.
fn settled_event(execution: &Execution) -> Value {
    let status = execution.state();
    crate::protocol::event(
        "settled",
        json!({"executionId":execution.meta.id,
            "status":if status.state == "lost" { "failed" } else { &status.state },
            "exitCode":status.exit_code,"signal":crate::protocol::signal_name(status.signal),
            "termination":status.termination,"detail":status.error}),
    )
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
    use crate::shim::{atomic_json, Status};
    use crate::store::Record;

    fn meta(id: &str, boot: &str, argv: Value) -> Meta {
        serde_json::from_value(
            json!({"id":id,"operationId":"op","digest":"d","bootId":boot,
            "argv":argv,"cwd":"/tmp","env":{},"timeoutMs":100,"onExpiry":"kill","graceMs":10,
            "capture":{"stdoutMaxBytes":10,"stderrMaxBytes":10,"spillMaxBytes":0,"spillDir":"."}}),
        )
        .unwrap()
    }

    fn terminal_status(exit_code: i32) -> Status {
        let mut status = Status::admitted(1);
        status.state = "exited".into();
        status.pid = Some(1);
        status.pgid = Some(1);
        status.process_start_time = Some(1);
        status.shim_pid = 1;
        status.shim_start_time = 1;
        status.finished_at = Some(2);
        status.exit_code = Some(exit_code);
        status
    }

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
        let meta = meta("starting", "boot", json!(["/bin/sh", "-c", "exit 7"]));
        atomic_json(&execution_dir.join("meta.json"), &meta).unwrap();
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
        let meta = meta("stale", "boot", json!(["/bin/sh", "-c", "exit 0"]));
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
        let meta = meta("retry", "boot", json!(["/bin/sh"]));
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

    #[test]
    fn attach_reports_the_terminal_facts_the_host_missed() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/settlement-replay");
        let state = dir.join("state");
        let execution_dir = state.join("settled-early");
        std::fs::create_dir_all(&execution_dir).unwrap();
        let meta = meta("settled-early", "boot", json!(["/bin/sh", "-c", "exit 5"]));
        crate::shim::atomic_json(&execution_dir.join("meta.json"), &meta).unwrap();
        crate::shim::atomic_json(&execution_dir.join("status.json"), &terminal_status(5)).unwrap();
        let store = Arc::new(Store::open(&dir.join("history.sqlite")).unwrap());
        // Startup announces the settlement with no connection subscribed.
        let registry = Registry::with_store(
            4,
            Duration::from_secs(60),
            Some(Arc::clone(&store)),
            state.clone(),
        )
        .unwrap();

        // The facts travel in the hello reply, so a host that never saw the
        // event can still settle the execution.
        let rows = registry.attach("boot");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["state"], "exited");
        assert_eq!(rows[0]["exitCode"], 5);
        assert_eq!(rows[0]["executionId"], "settled-early");

        // And the reconnecting connection receives the settlement frame itself.
        let events = registry.replay("boot");
        let event = events.first().expect("replayed settlement");
        assert_eq!(event["kind"], "evt");
        assert_eq!(event["method"], "settled");
        assert_eq!(event["payload"]["executionId"], "settled-early");
        assert_eq!(event["payload"]["exitCode"], 5);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_running_row_without_a_state_directory_becomes_lost() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/history-projection");
        let state = dir.join("state");
        std::fs::create_dir_all(&state).unwrap();
        let store = Arc::new(Store::open(&dir.join("history.sqlite")).unwrap());
        store
            .save(&Record {
                id: "gone".into(),
                argv: "[\"/bin/sh\"]".into(),
                cwd: "/tmp".into(),
                started_at: 10,
                finished_at: None,
                state: "running".into(),
                exit_code: None,
                signal: None,
                stdout: String::new(),
                stderr: String::new(),
                lossy: false,
                stdout_lossy: false,
                stderr_lossy: false,
                stdout_truncated: false,
                stderr_truncated: false,
            })
            .unwrap();
        let registry =
            Registry::with_store(4, Duration::from_secs(60), Some(Arc::clone(&store)), state)
                .unwrap();
        assert!(registry.state("gone").is_err());
        let facts = store.facts("gone").unwrap().unwrap();
        assert_eq!(facts.state, "lost");
        assert!(facts.finished_at.is_some());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_settled_execution_keeps_its_row_and_loses_its_directory() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-state/retention");
        let state = dir.join("state");
        let execution_dir = state.join("done");
        std::fs::create_dir_all(&execution_dir).unwrap();
        let meta = meta("done", "boot", json!(["/bin/sh", "-c", "exit 0"]));
        crate::shim::atomic_json(&execution_dir.join("meta.json"), &meta).unwrap();
        crate::shim::atomic_json(&execution_dir.join("status.json"), &terminal_status(0)).unwrap();
        let store = Arc::new(Store::open(&dir.join("history.sqlite")).unwrap());
        let registry = Registry::with_retention(
            4,
            Duration::from_secs(60),
            Some(Arc::clone(&store)),
            state.clone(),
            Duration::ZERO,
        )
        .unwrap();
        assert!(
            !execution_dir.exists(),
            "the terminal row replaced the directory"
        );
        assert_eq!(store.detail("done").unwrap().unwrap()["exitCode"], 0);
        // A second pass must not resurrect or rewrite anything.
        registry.sweep();
        assert_eq!(store.count().unwrap(), 1);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
