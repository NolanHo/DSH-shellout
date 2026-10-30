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
    /// Host-owned directory the completed capture is linked into; it outlives
    /// the execution's state directory. Empty disables the spill file.
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

/// A framed reader over one connection.
///
/// Bytes read past the current frame stay buffered, and the only await is a
/// single cancel-safe `read`, so a frame that arrives in several writes
/// survives losing a `select!` race to an event or a deadline. One instance
/// must own a stream for its whole lifetime; reading one frame per instance
/// would drop whatever that read buffered beyond the frame.
pub struct Frames<'a, R> {
    reader: &'a mut R,
    buffer: Vec<u8>,
}

impl<'a, R: AsyncRead + Unpin> Frames<'a, R> {
    pub fn new(reader: &'a mut R) -> Self {
        Self {
            reader,
            buffer: Vec::new(),
        }
    }

    /// Read one frame, or `None` at a clean end of stream.
    pub async fn next(&mut self, max_frame_bytes: usize) -> io::Result<Option<Envelope>> {
        loop {
            if let Some(envelope) = self.take(max_frame_bytes)? {
                return Ok(Some(envelope));
            }
            let mut chunk = [0u8; 8 * 1024];
            let count = self.reader.read(&mut chunk).await?;
            if count == 0 {
                return match self.buffer.len() {
                    0 => Ok(None),
                    partial => Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("{partial} bytes of a frame remain at end of stream"),
                    )),
                };
            }
            self.buffer.extend_from_slice(&chunk[..count]);
        }
    }

    /// Split off one complete frame, leaving any later bytes buffered.
    fn take(&mut self, max_frame_bytes: usize) -> io::Result<Option<Envelope>> {
        if self.buffer.len() < 4 {
            return Ok(None);
        }
        let length = u32::from_be_bytes(self.buffer[..4].try_into().expect("four bytes")) as usize;
        if length == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "empty frame"));
        }
        if length > max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame of {length} bytes exceeds the {max_frame_bytes}-byte cap"),
            ));
        }
        if self.buffer.len() < 4 + length {
            return Ok(None);
        }
        let envelope = serde_json::from_slice(&self.buffer[4..4 + length]).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad frame body: {error}"),
            )
        })?;
        self.buffer.drain(..4 + length);
        Ok(Some(envelope))
    }
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

/// Name for a terminating signal, as the host reports it.
///
/// The wire carries `NodeJS.Signals | null`; a bare signal number is not a
/// string, so a killed process used to fail the tool's output schema.
pub fn signal_name(signal: Option<i32>) -> Option<&'static str> {
    Some(match signal? {
        1 => "SIGHUP",
        2 => "SIGINT",
        3 => "SIGQUIT",
        4 => "SIGILL",
        5 => "SIGTRAP",
        6 => "SIGABRT",
        7 => "SIGBUS",
        8 => "SIGFPE",
        9 => "SIGKILL",
        10 => "SIGUSR1",
        11 => "SIGSEGV",
        12 => "SIGUSR2",
        13 => "SIGPIPE",
        14 => "SIGALRM",
        15 => "SIGTERM",
        16 => "SIGSTKFLT",
        17 => "SIGCHLD",
        18 => "SIGCONT",
        19 => "SIGSTOP",
        20 => "SIGTSTP",
        21 => "SIGTTIN",
        22 => "SIGTTOU",
        23 => "SIGURG",
        24 => "SIGXCPU",
        25 => "SIGXFSZ",
        26 => "SIGVTALRM",
        27 => "SIGPROF",
        28 => "SIGWINCH",
        29 => "SIGIO",
        30 => "SIGPWR",
        31 => "SIGSYS",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(id: u64) -> Vec<u8> {
        let body =
            serde_json::to_vec(&serde_json::json!({"id": id, "kind": "req", "method": "state"}))
                .unwrap();
        let mut frame = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        frame.extend_from_slice(&body);
        frame
    }

    #[tokio::test]
    async fn a_frame_split_by_a_cancelled_read_is_still_delivered() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let frame = request(4);
        client.write_all(&frame[..7]).await.unwrap();
        let mut frames = Frames::new(&mut server);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), frames.next(8192))
                .await
                .is_err(),
            "an incomplete frame is still pending"
        );
        client.write_all(&frame[7..]).await.unwrap();
        let envelope = frames.next(8192).await.unwrap().unwrap();
        assert_eq!(envelope.id, 4);
        assert_eq!(envelope.method, "state");
    }

    #[tokio::test]
    async fn two_frames_in_one_read_are_delivered_in_order() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let (first, second) = (request(1), request(2));
        client.write_all(&[first, second].concat()).await.unwrap();
        let mut frames = Frames::new(&mut server);
        assert_eq!(frames.next(8192).await.unwrap().unwrap().id, 1);
        assert_eq!(frames.next(8192).await.unwrap().unwrap().id, 2);
    }
}
