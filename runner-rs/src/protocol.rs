//! Wire protocol for the DSH-shellout runner.
//!
//! Frames are `[u32 big-endian length][UTF-8 JSON envelope]`. The envelope
//! vocabulary mirrors `dsh-shellout-protocol` (TypeScript); this module is the
//! Rust side of that same contract.

use std::io;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest frame accepted when the caller does not negotiate a smaller one.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;

/// One frame on the wire.
#[derive(Debug, Deserialize)]
pub struct Envelope {
    /// Request correlation id; 0 on events.
    pub id: u64,
    /// `req`, `res`, `err` or `evt`.
    pub kind: String,
    /// Method or event name.
    pub method: String,
    /// Method payload.
    #[serde(default)]
    pub payload: serde_json::Value,
}

/// `hello` request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloRequest {
    /// Protocol revision the host speaks.
    pub protocol: String,
    /// Identity of the host boot; executions belong to it.
    pub boot_id: String,
    /// Largest frame the host accepts.
    #[serde(default)]
    pub max_frame_bytes: Option<usize>,
}

/// How much of each stream is retained and spilled.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CapturePolicy {
    /// File retention cap for stdout when spill is disabled.
    pub stdout_max_bytes: usize,
    /// File retention cap for stderr when spill is disabled.
    pub stderr_max_bytes: usize,
    /// File cap for either stream; zero uses the per-stream retention cap.
    pub spill_max_bytes: u64,
    /// Legacy spill directory; durable capture always lives in the state directory.
    pub spill_dir: String,
}

/// One fully resolved execution: everything needed to exec, no harness policy.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SpawnSpec {
    /// Executable and arguments.
    pub argv: Vec<String>,
    /// Working directory.
    pub cwd: String,
    /// Complete child environment.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// Bytes written to stdin before it closes.
    #[serde(default)]
    pub stdin: Option<String>,
    /// Deadline in milliseconds, enforced here.
    pub timeout_ms: u64,
    /// `kill` kills at the deadline; `none` leaves the process running.
    pub on_expiry: String,
    /// Grace between the first signal and SIGKILL.
    pub grace_ms: u64,
    /// Capture budgets.
    pub capture: CapturePolicy,
}

/// `exec` request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecRequest {
    /// Idempotency key for a retried spawn.
    pub operation_id: String,
    /// Execution identity chosen by the host.
    pub execution_id: String,
    /// The resolved spawn spec.
    pub spec: SpawnSpec,
    /// Stable digest of `spec`.
    pub spec_digest: String,
}

/// `readStream` request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReadStreamRequest {
    /// Execution to read.
    pub execution_id: String,
    /// `stdout` or `stderr`.
    pub stream: String,
    /// Absolute byte offset to resume from.
    pub offset: u64,
    /// Byte budget for this reply.
    pub limit: usize,
}

/// `kill`, `state` and `readOutput` share this shape.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecutionRef {
    /// Execution to address.
    pub execution_id: String,
}

/// `ping` request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PingRequest {
    /// Boot whose lease is refreshed.
    pub boot_id: String,
}

/// `drain` request.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DrainRequest {
    /// Bound on waiting for live executions to settle.
    pub timeout_ms: u64,
}

/// Read one frame, or `None` at a clean end of stream.
pub async fn read_frame<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> io::Result<Option<Envelope>> {
    let mut header = [0u8; 4];
    match reader.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty frame"));
    }
    if length > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame of {length} bytes exceeds the {max_frame_bytes}-byte cap"),
        ));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).await?;
    let envelope: Envelope = serde_json::from_slice(&body).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bad frame body: {error}"),
        )
    })?;
    Ok(Some(envelope))
}

/// Write one frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    value: &serde_json::Value,
    max_frame_bytes: usize,
) -> io::Result<()> {
    let body = serde_json::to_vec(value)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    if body.len() > max_frame_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("outgoing frame of {} bytes exceeds the cap", body.len()),
        ));
    }
    writer.write_all(&(body.len() as u32).to_be_bytes()).await?;
    writer.write_all(&body).await?;
    writer.flush().await
}

/// Build a success reply frame.
pub fn reply(id: u64, method: &str, payload: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "id": id, "kind": "res", "method": method, "payload": payload })
}

/// Build an error reply frame.
pub fn error_reply(id: u64, code: &str, message: &str) -> serde_json::Value {
    serde_json::json!({
        "id": id,
        "kind": "err",
        "method": "error",
        "payload": { "code": code, "message": message },
    })
}

/// Build one event frame.
pub fn event(method: &str, payload: serde_json::Value) -> serde_json::Value {
    serde_json::json!({ "id": 0, "kind": "evt", "method": method, "payload": payload })
}
