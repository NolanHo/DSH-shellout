//! Unix-socket server: framed requests in, replies and settlement events out.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;

use crate::legacy::{is_legacy, to_spawn_spec, LegacyExecRequest};
use crate::protocol::{
    error_reply, reply, write_frame, DrainRequest, Envelope, ExecRequest, ExecutionRef, Frames,
    HelloRequest, PingRequest, ReadStreamRequest,
};

/// Revisions this runner accepts: the current one and its predecessor.
const SUPPORTED_PROTOCOLS: [&str; 2] = ["2.0", "1.0"];
use crate::registry::Registry;

/// Server configuration.
#[derive(Clone)]
pub struct Config {
    /// Socket path to bind.
    pub socket_path: String,
    /// Default working directory reported to the host.
    pub cwd: String,
    /// Boot lease lifetime without a ping.
    pub lease_ttl_ms: u64,
    /// Maximum concurrently live executions.
    pub max_concurrent: usize,
    /// Largest frame this runner accepts.
    pub max_frame_bytes: usize,
    /// Timeout policy default caps reported to the host.
    pub timeout_ms: u64,
    /// Deadline cap reported to the host.
    pub max_timeout_ms: u64,
    /// stdout capture cap reported to the host.
    pub max_output_bytes: usize,
    /// Directory legacy requests spill into.
    pub spill_dir: String,
}

impl Config {
    /// Directory legacy (protocol 1.0) requests spill into.
    fn spill_dir(&self) -> String {
        self.spill_dir.clone()
    }
}

/// Run the accept loop until the process is told to stop.
pub async fn serve(config: Config, registry: Arc<Registry>) -> std::io::Result<()> {
    let path = Path::new(&config.socket_path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let _ = std::fs::remove_file(path);
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;

    let sweeper = Arc::clone(&registry);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(250));
        loop {
            ticker.tick().await;
            sweeper.sweep();
        }
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let registry = Arc::clone(&registry);
        let config = config.clone();
        tokio::spawn(async move {
            let _ = handle_connection(stream, registry, config).await;
        });
    }
}

async fn handle_connection(
    stream: UnixStream,
    registry: Arc<Registry>,
    config: Config,
) -> std::io::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let mut frames = Frames::new(&mut reader);
    let mut events: Option<broadcast::Receiver<Value>> = None;
    let mut observer = false;
    let mut max_frame_bytes = config.max_frame_bytes;
    let mut boot_id: Option<String> = None;

    loop {
        tokio::select! {
            frame = frames.next(max_frame_bytes) => {
                let envelope = match frame {
                    Ok(Some(envelope)) => envelope,
                    Ok(None) => break,
                    Err(_) => break,
                };
                // A hello makes this connection a host. Subscribe before the
                // reply is built, so a settlement decided during the handshake
                // is buffered for this connection instead of lost.
                let hello = envelope.kind == "req" && envelope.method == "hello" && events.is_none();
                if hello {
                    events = Some(registry.subscribe());
                }
                let reply_value = dispatch(envelope, &registry, &config, &mut boot_id, &mut observer, &mut max_frame_bytes).await;
                if hello {
                    match (&reply_value["kind"], boot_id.as_deref()) {
                        // A settlement decided before this handshake reached no
                        // subscriber; the reply carries its facts, and the
                        // recent ones are announced again.
                        (Value::String(kind), Some(boot)) if kind == "res" => registry.replay(boot),
                        _ => events = None,
                    }
                }
                write_frame(&mut writer, &reply_value, max_frame_bytes).await?;
            }
            event_value = async {
                match events.as_mut() {
                    Some(receiver) => receiver.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                // Only a connection that completed a handshake is a host.
                if let Ok(value) = event_value {
                    if boot_id.is_some() {
                        write_frame(&mut writer, &value, max_frame_bytes).await?;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn dispatch(
    envelope: Envelope,
    registry: &Arc<Registry>,
    config: &Config,
    boot_id: &mut Option<String>,
    observer: &mut bool,
    max_frame_bytes: &mut usize,
) -> Value {
    let id = envelope.id;
    if envelope.kind != "req" {
        return error_reply(id, "protocol", "expected request frame");
    }
    if envelope.method == "observe" {
        if boot_id.is_some() {
            return error_reply(id, "protocol", "host connections cannot observe");
        }
        *observer = true;
        if !envelope.payload.is_object() {
            return error_reply(id, "protocol", "bad observe payload");
        }
        let execution_id = match envelope.payload.get("executionId") {
            None => None,
            Some(Value::String(id)) => Some(id.as_str()),
            _ => return error_reply(id, "protocol", "bad observe executionId"),
        };
        let mut snapshot = registry.snapshot();
        if let Some(fields) = registry.history_health().as_object() {
            for (key, value) in fields {
                snapshot[key] = value.clone();
            }
        }
        if let Some(execution_id) = execution_id {
            snapshot["detail"] = registry.detail(execution_id).unwrap_or(Value::Null);
        }
        return bounded_snapshot(id, "observe", snapshot, *max_frame_bytes);
    }
    if *observer {
        return error_reply(id, "protocol", "observer connections cannot become hosts");
    }
    if envelope.method != "hello" && boot_id.is_none() {
        return error_reply(id, "protocol", "hello must precede this request");
    }
    match envelope.method.as_str() {
        "hello" => {
            let request: HelloRequest = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad hello: {error}")),
            };
            if !SUPPORTED_PROTOCOLS.contains(&request.protocol.as_str()) {
                return error_reply(
                    id,
                    "version",
                    &format!(
                        "protocol {} is not supported; this runner speaks {}",
                        request.protocol,
                        SUPPORTED_PROTOCOLS.join(", ")
                    ),
                );
            }
            if let Some(host_max) = request.max_frame_bytes {
                *max_frame_bytes = (*max_frame_bytes).min(host_max);
            }
            *boot_id = Some(request.boot_id.clone());
            let executions = registry.attach(&request.boot_id);
            reply(
                id,
                "hello",
                json!({
                    "protocol": request.protocol,
                    "runnerBuild": format!("rust-{}", env!("CARGO_PKG_VERSION")),
                    "runnerBootId": std::process::id().to_string(),
                    "descriptor": {
                        "cwd": config.cwd,
                        "timeoutMs": config.timeout_ms,
                        "maxTimeoutMs": config.max_timeout_ms,
                        "maxOutputBytes": config.max_output_bytes,
                    },
                    "maxFrameBytes": *max_frame_bytes,
                    "heartbeatMs": 5000,
                    "leaseTtlMs": config.lease_ttl_ms,
                    "executions": executions,
                }),
            )
        }
        "ping" => {
            let request: PingRequest = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad ping: {error}")),
            };
            match registry.ping(&request.boot_id) {
                Ok(()) => reply(id, "ping", json!({ "ok": true })),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "exec" => {
            let boot = match boot_id {
                Some(boot) => boot.clone(),
                None => return error_reply(id, "protocol", "hello must precede this request"),
            };
            let request = match if is_legacy(&envelope.payload) {
                serde_json::from_value::<LegacyExecRequest>(envelope.payload.clone())
                    .map(|legacy| ExecRequest {
                        operation_id: legacy.operation_id,
                        execution_id: legacy.execution_id,
                        spec_digest: legacy.spec_digest,
                        spec: to_spawn_spec(
                            legacy.spec,
                            3_000,
                            64 * 1024 * 1024,
                            &config.spill_dir(),
                        ),
                    })
                    .map_err(|error| format!("bad exec: {error}"))
            } else {
                serde_json::from_value::<ExecRequest>(envelope.payload.clone())
                    .map_err(|error| format!("bad exec: {error}"))
            } {
                Ok(request) => request,
                Err(message) => return error_reply(id, "protocol", &message),
            };
            // Admission waits for the shim's first report. That wait is file and
            // process I/O, so it runs on a blocking thread: the runtime keeps
            // serving other connections while a command starts.
            let registry = Arc::clone(registry);
            match tokio::task::spawn_blocking(move || registry.exec(request, &boot)).await {
                Ok(Ok(value)) => reply(id, "exec", value),
                Ok(Err(failure)) => error_reply(id, failure.code, &failure.message),
                Err(error) => {
                    error_reply(id, "internal", &format!("admission task failed: {error}"))
                }
            }
        }
        "readStream" => {
            let request: ReadStreamRequest = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => {
                    return error_reply(id, "protocol", &format!("bad readStream: {error}"))
                }
            };
            let limit = request.limit.min((*max_frame_bytes / 12).max(1));
            match registry.read_stream(
                &request.execution_id,
                &request.stream,
                request.offset,
                limit,
            ) {
                Ok(value) => reply(id, "readStream", value),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "readOutput" => {
            let request: ExecutionRef = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => {
                    return error_reply(id, "protocol", &format!("bad readOutput: {error}"))
                }
            };
            match registry.read_output(&request.execution_id) {
                Ok(value) => reply(id, "readOutput", value),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "kill" => {
            let request: ExecutionRef = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad kill: {error}")),
            };
            match registry.kill(&request.execution_id) {
                Ok(value) => reply(id, "kill", value),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "snapshot" => bounded_snapshot(id, "snapshot", registry.snapshot(), *max_frame_bytes),
        "state" => {
            let request: ExecutionRef = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad state: {error}")),
            };
            match registry.state(&request.execution_id) {
                Ok(value) => reply(id, "state", value),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "drain" => {
            let request: DrainRequest = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad drain: {error}")),
            };
            reply(id, "drain", registry.drain(request.timeout_ms))
        }
        "shutdown" => reply(id, "shutdown", json!({})),
        other => error_reply(id, "protocol", &format!("unknown method {other}")),
    }
}

/// Reserve the envelope bytes as well as payload bytes when limiting summaries.
fn bounded_snapshot(id: u64, method: &str, snapshot: Value, cap: usize) -> Value {
    let mut frame = reply(id, method, snapshot);
    if let Some(rows) = frame["payload"]["executions"].as_array_mut() {
        for row in rows {
            if let Some(object) = row.as_object_mut() {
                object.remove("stdout");
                object.remove("stderr");
            }
        }
    }
    while serde_json::to_vec(&frame).map_or(true, |bytes| bytes.len() > cap) {
        if frame["payload"]["executions"]
            .as_array_mut()
            .is_some_and(|rows| rows.pop().is_some())
        {
            continue;
        }
        return error_reply(id, "frameTooLarge", "observation exceeds frame budget");
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Frames;
    use tokio::io::AsyncWriteExt;

    fn config() -> Config {
        Config {
            socket_path: String::new(),
            cwd: ".".into(),
            lease_ttl_ms: 10000,
            max_concurrent: 8,
            max_frame_bytes: 8192,
            timeout_ms: 1000,
            max_timeout_ms: 10000,
            max_output_bytes: 4096,
            spill_dir: String::new(),
        }
    }

    fn request(kind: &str, method: &str, payload: Value) -> Envelope {
        serde_json::from_value(json!({"id": 9, "kind": kind, "method": method, "payload": payload}))
            .unwrap()
    }

    #[tokio::test]
    async fn observer_has_no_lease_and_cannot_become_host() {
        let registry = Registry::new(8, std::time::Duration::from_secs(10));
        let mut boot = None;
        let mut observer = false;
        let mut cap = 8192;
        let before = registry.snapshot();
        let response = dispatch(
            request("req", "observe", json!({})),
            &registry,
            &config(),
            &mut boot,
            &mut observer,
            &mut cap,
        )
        .await;
        assert_eq!(response["kind"], "res");
        assert_eq!(response["method"], "observe");
        assert!(observer);
        assert!(boot.is_none());
        assert_eq!(before["leases"], registry.snapshot()["leases"]);
        let response = dispatch(
            request(
                "req",
                "hello",
                json!({"protocol": "2.0", "clientBuild": "test", "bootId": "test"}),
            ),
            &registry,
            &config(),
            &mut boot,
            &mut observer,
            &mut cap,
        )
        .await;
        assert_eq!(response["kind"], "err");
        assert_eq!(response["method"], "error");
        assert!(boot.is_none());
    }

    #[tokio::test]
    async fn host_cannot_observe_and_nonrequests_cannot_dispatch() {
        let registry = Registry::new(8, std::time::Duration::from_secs(10));
        let mut boot = Some("host".to_string());
        let mut observer = false;
        let mut cap = 8192;
        for (kind, method) in [("req", "observe"), ("res", "observe"), ("evt", "kill")] {
            let response = dispatch(
                request(kind, method, json!({})),
                &registry,
                &config(),
                &mut boot,
                &mut observer,
                &mut cap,
            )
            .await;
            assert_eq!(response["kind"], "err");
            assert!(!observer);
        }
    }

    #[tokio::test]
    async fn a_request_split_around_an_event_still_replies() {
        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        let registry = Registry::new(8, std::time::Duration::from_secs(10));
        let task = tokio::spawn(handle_connection(server, Arc::clone(&registry), config()));
        let (mut reader, mut writer) = client.split();
        let mut replies = Frames::new(&mut reader);

        let hello = json!({"id": 1, "kind": "req", "method": "hello",
            "payload": {"protocol": "2.0", "clientBuild": "test", "bootId": "test"}});
        write_frame(&mut writer, &hello, 8192).await.unwrap();
        assert_eq!(replies.next(8192).await.unwrap().unwrap().id, 1);

        // Two writes with the handler parked on the second half; the settlement
        // event wins the select and must not consume the request bytes.
        let body = serde_json::to_vec(
            &json!({"id": 2, "kind": "req", "method": "state", "payload": {"executionId": "x"}}),
        )
        .unwrap();
        let mut frame = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        writer.write_all(&frame[..6]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        registry.publish(crate::protocol::event(
            "settled",
            json!({"executionId": "x"}),
        ));
        let event_frame = replies.next(8192).await.unwrap().unwrap();
        assert_eq!(event_frame.kind, "evt");
        writer.write_all(&frame[6..]).await.unwrap();

        let reply = replies.next(8192).await.unwrap().unwrap();
        assert_eq!(reply.id, 2);
        assert_eq!(reply.method, "error");
        task.abort();
    }

    #[test]
    fn snapshot_budget_includes_envelope_and_strips_output() {
        let snapshot = json!({"executions": (0..20).map(|id| json!({"id": id.to_string(), "argv": ["x".repeat(100)], "stdout": "not a summary", "stderr": "not a summary"})).collect::<Vec<_>>()});
        let frame = bounded_snapshot(1, "observe", snapshot, 512);
        assert!(serde_json::to_vec(&frame).unwrap().len() <= 512);
        assert_eq!(frame["kind"], "res");
        assert!(frame["payload"]["executions"].as_array().unwrap().len() < 20);
        for row in frame["payload"]["executions"].as_array().unwrap() {
            assert!(row.get("stdout").is_none());
            assert!(row.get("stderr").is_none());
        }
    }
}
