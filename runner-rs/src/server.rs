//! Unix-socket server: framed requests in, replies and settlement events out.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::broadcast;

use crate::legacy::{is_legacy, to_spawn_spec, LegacyExecRequest};
use crate::protocol::{
    error_reply, event, read_frame, reply, write_frame, DrainRequest, Envelope, ExecRequest, ExecutionRef,
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
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(500));
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

async fn handle_connection(stream: UnixStream, registry: Arc<Registry>, config: Config) -> std::io::Result<()> {
    let (mut reader, mut writer) = stream.into_split();
    let mut events: broadcast::Receiver<Value> = registry.subscribe();
    let mut max_frame_bytes = config.max_frame_bytes;
    let mut boot_id: Option<String> = None;

    loop {
        tokio::select! {
            frame = read_frame(&mut reader, max_frame_bytes) => {
                let envelope = match frame {
                    Ok(Some(envelope)) => envelope,
                    Ok(None) => break,
                    Err(_) => break,
                };
                let reply_value = dispatch(envelope, &registry, &config, &mut boot_id, &mut max_frame_bytes);
                write_frame(&mut writer, &reply_value, max_frame_bytes).await?;
            }
            event_value = events.recv() => {
                if let Ok(value) = event_value {
                    write_frame(&mut writer, &value, max_frame_bytes).await?;
                }
            }
        }
    }
    Ok(())
}

fn dispatch(
    envelope: Envelope,
    registry: &Arc<Registry>,
    config: &Config,
    boot_id: &mut Option<String>,
    max_frame_bytes: &mut usize,
) -> Value {
    let id = envelope.id;
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
            reply(id, "hello", json!({
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
            }))
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
                        spec: to_spawn_spec(legacy.spec, 3_000, 64 * 1024 * 1024, &config.spill_dir()),
                    })
                    .map_err(|error| format!("bad exec: {error}"))
            } else {
                serde_json::from_value::<ExecRequest>(envelope.payload.clone())
                    .map_err(|error| format!("bad exec: {error}"))
            } {
                Ok(request) => request,
                Err(message) => return error_reply(id, "protocol", &message),
            };
            match registry.exec(request, &boot) {
                Ok(value) => reply(id, "exec", value),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "readStream" => {
            let request: ReadStreamRequest = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad readStream: {error}")),
            };
            match registry.read_stream(&request.execution_id, &request.stream, request.offset, request.limit) {
                Ok(value) => reply(id, "readStream", value),
                Err(failure) => error_reply(id, failure.code, &failure.message),
            }
        }
        "readOutput" => {
            let request: ExecutionRef = match serde_json::from_value(envelope.payload) {
                Ok(request) => request,
                Err(error) => return error_reply(id, "protocol", &format!("bad readOutput: {error}")),
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
        "shutdown" => {
            reply(id, "shutdown", json!({}))
        }
        other => error_reply(id, "protocol", &format!("unknown method {other}")),
    }
}

/// Build a settlement event for tests and external callers.
pub fn settled_event(execution_id: &str, status: &str, exit_code: Option<i32>) -> Value {
    event("settled", json!({
        "executionId": execution_id,
        "status": status,
        "exitCode": exit_code,
        "signal": null,
    }))
}
