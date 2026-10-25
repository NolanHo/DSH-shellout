//! Read-only HTTP surface. Live state is observed without creating a host lease.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};
use tokio::sync::Semaphore;

use crate::protocol::{write_frame, Frames, DEFAULT_MAX_FRAME_BYTES};
use crate::store::Store;

const OBSERVE_DEADLINE: Duration = Duration::from_millis(500);
const RECENT_LIMIT: usize = 50;

pub struct Hub {
    pub store: Option<Arc<Store>>,
    pub executor_socket: String,
    observers: Semaphore,
}

impl Hub {
    pub fn new(store: Option<Arc<Store>>, executor_socket: String) -> Self {
        Self {
            store,
            executor_socket,
            observers: Semaphore::new(8),
        }
    }

    /// The deadline includes queueing, connect, write, and every response read.
    async fn observe(&self, execution_id: Option<&str>) -> Option<Value> {
        tokio::time::timeout(OBSERVE_DEADLINE, async {
            let _permit = self.observers.acquire().await.ok()?;
            let mut stream = UnixStream::connect(&self.executor_socket).await.ok()?;
            let payload = execution_id.map_or_else(|| json!({}), |id| json!({"executionId": id}));
            write_frame(
                &mut stream,
                &json!({"id": 1, "kind": "req", "method": "observe", "payload": payload}),
                DEFAULT_MAX_FRAME_BYTES,
            )
            .await
            .ok()?;
            // One reader for the connection: a reply and an event may arrive
            // in the same read, and the event is skipped without losing bytes.
            let mut frames = Frames::new(&mut stream);
            loop {
                let frame = frames.next(DEFAULT_MAX_FRAME_BYTES).await.ok()??;
                if frame.kind == "evt" {
                    continue;
                }
                if frame.id != 1 {
                    return None;
                }
                if frame.kind == "err" && frame.method == "error" {
                    return None;
                }
                if frame.kind != "res" || frame.method != "observe" {
                    return None;
                }
                return valid_snapshot(&frame.payload, execution_id).then_some(frame.payload);
            }
        })
        .await
        .ok()
        .flatten()
    }

    async fn response(&self, path: &str, query: &str) -> (&'static str, &'static str, String) {
        // Static routes must not wait for or even connect to the executor.
        match path {
            "/" => {
                return (
                    "200 OK",
                    "text/html; charset=utf-8",
                    if self.store.is_some() {
                        crate::dashboard::page().to_string()
                    } else {
                        "<!doctype html><title>Shellout hub</title><h1>Shellout hub</h1><p>History unavailable.</p><p>Live status: <a href=\"/api/stats\">API</a></p>".to_string()
                    },
                )
            }
            "/health" => return ("200 OK", "text/plain", "ok".to_string()),
            "/api/stats" | "/api/executions" | "/api/execution" => {}
            _ => return ("404 Not Found", "text/plain", "not found".to_string()),
        }
        let id = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("id="))
            .map(percent_decode)
            .unwrap_or_default();
        let live = self
            .observe((path == "/api/execution").then_some(id.as_str()))
            .await;
        let mut availability =
            json!({"executor": if live.is_some() { "available" } else { "unreachable" }});
        match path {
            "/api/stats" => {
                let mut document = live.unwrap_or_else(|| json!({}));
                document.as_object_mut().unwrap().remove("detail");
                document.as_object_mut().unwrap().remove("executions");
                let history = self
                    .store
                    .as_ref()
                    .map(|store| Ok::<_, String>((store.count()?, store.recent(1)?)));
                match history {
                    Some(Ok((count, rows))) => {
                        document["history"] = json!("available");
                        document["historyRows"] = json!(count);
                        document["lastStartedAtMs"] = rows
                            .first()
                            .and_then(|row| row.get("startedAtMs"))
                            .cloned()
                            .unwrap_or(Value::Null);
                    }
                    _ => {
                        document["history"] = json!("unavailable");
                        document["historyRows"] = Value::Null;
                    }
                }
                document["executor"] = availability["executor"].take();
                ("200 OK", "application/json", document.to_string())
            }
            "/api/executions" => {
                let history = self.store.as_ref().map(|store| store.recent(RECENT_LIMIT));
                availability["history"] = json!(if matches!(&history, Some(Ok(_))) {
                    "available"
                } else {
                    "unavailable"
                });
                availability["executions"] = json!(merge(
                    live.as_ref(),
                    history.and_then(Result::ok).unwrap_or_default()
                ));
                // Include HTTP metadata in the same bounded document budget.
                trim_executions(&mut availability, DEFAULT_MAX_FRAME_BYTES);
                ("200 OK", "application/json", availability.to_string())
            }
            _ => {
                let live_detail = live
                    .as_ref()
                    .and_then(|value| value.get("detail"))
                    .filter(|value| value.is_object())
                    .cloned();
                let history = self.store.as_ref().map(|store| store.detail(&id));
                availability["history"] = json!(if matches!(&history, Some(Ok(_))) {
                    "available"
                } else {
                    "unavailable"
                });
                match live_detail.or_else(|| history.and_then(Result::ok).flatten()) {
                    Some(mut detail) => {
                        detail["executor"] = availability["executor"].take();
                        detail["history"] = availability["history"].take();
                        ("200 OK", "application/json", detail.to_string())
                    }
                    None => {
                        availability["error"] = json!("unknown execution");
                        (
                            "404 Not Found",
                            "application/json",
                            availability.to_string(),
                        )
                    }
                }
            }
        }
    }
}

fn valid_snapshot(value: &Value, id: Option<&str>) -> bool {
    if !value.is_object() {
        return false;
    }
    let Some(rows) = value.get("executions").and_then(Value::as_array) else {
        return false;
    };
    if rows.iter().any(|row| {
        !row.is_object()
            || row.get("id").and_then(Value::as_str).is_none()
            || row.get("stdout").is_some()
            || row.get("stderr").is_some()
    }) {
        return false;
    }
    for key in [
        "pid",
        "uptimeMs",
        "live",
        "spawned",
        "settled",
        "killed",
        "failed",
        "leases",
        "maxConcurrent",
    ] {
        if let Some(field) = value.get(key) {
            if !field.is_null() && field.as_u64().is_none() {
                return false;
            }
        }
    }
    if let Some(version) = value.get("version") {
        if !version.is_string() {
            return false;
        }
    }
    if let Some(protocols) = value.get("protocols") {
        if !protocols
            .as_array()
            .is_some_and(|items| items.iter().all(Value::is_string))
        {
            return false;
        }
    }
    if value
        .get("historyWritable")
        .is_some_and(|field| !field.is_boolean())
    {
        return false;
    }
    if value
        .get("historyError")
        .is_some_and(|field| !field.is_null() && !field.is_string())
    {
        return false;
    }
    match value.get("detail") {
        None | Some(Value::Null) => true,
        Some(detail) => {
            detail.is_object()
                && detail.get("id").and_then(Value::as_str) == id
                && ["stdout", "stderr", "cwd", "state"]
                    .iter()
                    .all(|key| detail.get(*key).is_none_or(Value::is_string))
        }
    }
}

fn merge(live: Option<&Value>, history: Vec<Value>) -> Vec<Value> {
    let mut rows = Vec::new();
    let mut seen = HashSet::new();
    let live_rows = live
        .and_then(|value| value.get("executions"))
        .and_then(Value::as_array);
    for mut row in live_rows.into_iter().flatten().cloned().chain(history) {
        let Some(id) = row.get("id").and_then(Value::as_str) else {
            continue;
        };
        if !seen.insert(id.to_string()) {
            continue;
        }
        if let Some(object) = row.as_object_mut() {
            object.remove("stdout");
            object.remove("stderr");
        }
        rows.push(row);
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row["startedAtMs"].as_u64().unwrap_or(0)));
    rows.truncate(RECENT_LIMIT);
    rows
}

/// Drop whole summaries, never serialize captured output into a list frame.
pub(crate) fn trim_executions(document: &mut Value, max_bytes: usize) {
    while serde_json::to_vec(document).map_or(true, |bytes| bytes.len() > max_bytes) {
        let Some(rows) = document.get_mut("executions").and_then(Value::as_array_mut) else {
            break;
        };
        if rows.pop().is_none() {
            break;
        }
    }
}

/// Decode bytes, not UTF-8 slices: malformed escapes and Unicode cannot panic.
fn percent_decode(value: &str) -> String {
    fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                decoded.push(high * 16 + low);
                index += 3;
                continue;
            }
        }
        decoded.push(if bytes[index] == b'+' {
            b' '
        } else {
            bytes[index]
        });
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

pub async fn serve(address: String, hub: Arc<Hub>) -> std::io::Result<()> {
    let listener = TcpListener::bind(&address).await?;
    eprintln!("hub listening on http://{address}");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            let mut buffer = [0u8; 4096];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let target = request.split_whitespace().nth(1).unwrap_or("/");
            let (path, query) = target.split_once('?').unwrap_or((target, ""));
            let (status, content_type, body) = hub.response(path, query).await;
            let response = format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}", body.len());
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

pub fn open_store(path: &Path) -> Option<Arc<Store>> {
    match Store::open_read_only(path) {
        Ok(store) => Some(Arc::new(store)),
        Err(error) => {
            eprintln!("hub: cannot open {}: {error}", path.display());
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tokio::net::UnixListener;

    static NEXT_SOCKET: AtomicU64 = AtomicU64::new(0);

    struct SocketPath(std::path::PathBuf);
    impl SocketPath {
        fn new() -> Self {
            let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join(".task-tmp");
            std::fs::create_dir_all(&directory).unwrap();
            Self(directory.join(format!(
                "observer-{}-{}.sock",
                std::process::id(),
                NEXT_SOCKET.fetch_add(1, Ordering::Relaxed)
            )))
        }
        fn hub(&self) -> Hub {
            Hub::new(None, self.0.to_string_lossy().into_owned())
        }
    }
    impl Drop for SocketPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    async fn responds(frames: Vec<Value>) -> Option<Value> {
        let path = SocketPath::new();
        let listener = UnixListener::bind(&path.0).unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = stream.split();
            let request = Frames::new(&mut reader)
                .next(DEFAULT_MAX_FRAME_BYTES)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(request.kind, "req");
            assert_eq!(request.method, "observe");
            for frame in frames {
                write_frame(&mut writer, &frame, DEFAULT_MAX_FRAME_BYTES)
                    .await
                    .unwrap();
            }
        });
        let result = path.hub().observe(None).await;
        task.await.unwrap();
        result
    }

    #[tokio::test]
    async fn observer_skips_events_and_accepts_correlated_response() {
        let result = responds(vec![
            json!({"id": 0, "kind": "evt", "method": "settled", "payload": {}}),
            json!({"id": 1, "kind": "res", "method": "observe", "payload": {"executions": [], "live": 2}}),
        ]).await.unwrap();
        assert_eq!(result["live"], 2);
    }

    #[tokio::test]
    async fn observer_rejects_errors_mismatch_and_invalid_payloads() {
        for frame in [
            json!({"id": 1, "kind": "err", "method": "error", "payload": {"code": "failed"}}),
            json!({"id": 2, "kind": "res", "method": "observe", "payload": {"executions": []}}),
            json!({"id": 1, "kind": "res", "method": "snapshot", "payload": {"executions": []}}),
            json!({"id": 1, "kind": "req", "method": "observe", "payload": {"executions": []}}),
            json!({"id": 1, "kind": "res", "method": "observe", "payload": {"executions": "bad"}}),
            json!({"id": 1, "kind": "res", "method": "observe", "payload": {"executions": [], "live": "bad"}}),
        ] {
            assert!(responds(vec![frame]).await.is_none());
        }
    }

    #[tokio::test]
    async fn observer_deadline_covers_stalled_frame_and_releases_permit() {
        let path = SocketPath::new();
        let listener = UnixListener::bind(&path.0).unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = stream.split();
            let _ = Frames::new(&mut reader).next(DEFAULT_MAX_FRAME_BYTES).await;
            writer.write_all(&100u32.to_be_bytes()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });
        let hub = path.hub();
        let start = tokio::time::Instant::now();
        assert!(hub.observe(None).await.is_none());
        assert!(start.elapsed() < Duration::from_millis(1500));
        assert_eq!(hub.observers.available_permits(), 8);
        task.abort();
    }

    #[tokio::test]
    async fn observer_deadline_includes_semaphore_queue() {
        let path = SocketPath::new();
        let listener = UnixListener::bind(&path.0).unwrap();
        let hub = path.hub();
        let _permits = hub.observers.acquire_many(8).await.unwrap();
        let start = tokio::time::Instant::now();
        assert!(hub.observe(None).await.is_none());
        assert!(start.elapsed() < Duration::from_millis(1500));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn observer_rejects_invalid_json_frame() {
        let path = SocketPath::new();
        let listener = UnixListener::bind(&path.0).unwrap();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let (mut reader, mut writer) = stream.split();
            let _ = Frames::new(&mut reader).next(DEFAULT_MAX_FRAME_BYTES).await;
            writer.write_all(&1u32.to_be_bytes()).await.unwrap();
            writer.write_all(b"{").await.unwrap();
        });
        assert!(path.hub().observe(None).await.is_none());
        task.await.unwrap();
    }

    #[tokio::test]
    async fn unavailable_executor_does_not_invent_zero_counters() {
        let path = SocketPath::new();
        let hub = path.hub();
        let (_, _, body) = hub.response("/api/stats", "").await;
        let document: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(document["executor"], "unreachable");
        assert_eq!(document["history"], "unavailable");
        assert!(document.get("live").is_none());
        assert!(document.get("spawned").is_none());
        assert!(document["historyRows"].is_null());
    }

    #[tokio::test]
    async fn static_routes_do_not_contact_executor() {
        let path = SocketPath::new();
        let listener = UnixListener::bind(&path.0).unwrap();
        let hub = path.hub();
        assert_eq!(hub.response("/", "").await.0, "200 OK");
        assert_eq!(hub.response("/health", "").await.2, "ok");
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }

    #[test]
    fn query_decode_handles_unicode_and_malformed_escapes() {
        assert_eq!(percent_decode("a%2Fb%20%E4%B8%AD+%"), "a/b 中 %");
        assert_eq!(percent_decode("%中%GG"), "%中%GG");
    }

    #[test]
    fn merge_prefers_live_and_never_returns_outputs() {
        let live = json!({"executions": [{"id": "same", "startedAtMs": 2, "state": "running"}]});
        let rows = merge(
            Some(&live),
            vec![
                json!({"id": "same", "state": "exited"}),
                json!({"id": "stored", "stdout": "secret", "stderr": "secret"}),
            ],
        );
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["state"], "running");
        assert!(rows[1].get("stdout").is_none());
        assert!(rows[1].get("stderr").is_none());
    }
}
