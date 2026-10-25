//! The service face: a stable process that serves the dashboard.
//!
//! It owns no child process. History comes from the SQLite database the
//! execution plane writes; live state comes from a read-only `snapshot` call
//! on the execution plane's socket. Restarting the execution plane therefore
//! costs the page nothing but the live rows, and restarting this process costs
//! the execution plane nothing at all.

use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixStream};

use crate::protocol::{read_frame, reply, write_frame, Envelope};
use crate::store::Store;

/// Everything the service face needs.
pub struct Hub {
    /// History database, written by the execution plane.
    pub store: Arc<Store>,
    /// Socket of the execution plane.
    pub executor_socket: String,
}

impl Hub {
    /// Ask the execution plane for its live snapshot; `None` when it is away.
    async fn snapshot(&self) -> Option<Value> {
        let stream = match UnixStream::connect(&self.executor_socket).await {
            Ok(stream) => stream,
            Err(_) => return None,
        };
        let (mut reader, mut writer) = stream.into_split();
        let hello = json!({
            "id": 1, "kind": "req", "method": "hello",
            "payload": { "protocol": "2.0", "clientBuild": "hub", "bootId": "hub", "maxFrameBytes": 8 * 1024 * 1024 },
        });
        write_frame(&mut writer, &hello, 8 * 1024 * 1024).await.ok()?;
        read_frame(&mut reader, 8 * 1024 * 1024).await.ok()??;
        let envelope: Envelope = match serde_json::from_value(json!({
            "id": 2, "kind": "req", "method": "snapshot", "payload": {},
        })) {
            Ok(envelope) => envelope,
            Err(_) => return None,
        };
        let frame = reply(envelope.id, "snapshot", json!({}));
        write_frame(&mut writer, &frame, 8 * 1024 * 1024).await.ok()?;
        let response = read_frame(&mut reader, 8 * 1024 * 1024).await.ok()??;
        Some(response.payload)
    }
}

/// Serve the dashboard until the process stops.
pub async fn serve(address: String, hub: Arc<Hub>) -> std::io::Result<()> {
    let listener = match TcpListener::bind(&address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("hub: cannot bind {address}: {error}");
            return Err(error);
        }
    };
    eprintln!("hub listening on http://{address}");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let hub = Arc::clone(&hub);
        tokio::spawn(async move {
            let mut buffer = [0u8; 4096];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let target = request.split_whitespace().nth(1).unwrap_or("/");
            let (path, query) = match target.split_once('?') {
                Some((path, query)) => (path, query),
                None => (target, ""),
            };
            let id = query.split('&').find_map(|pair| pair.strip_prefix("id=")).unwrap_or("");
            let live = hub.snapshot().await;
            let (status, content_type, body) = match path {
                "/api/stats" => ("200 OK", "application/json", stats_document(&hub, live.as_ref())),
                "/api/executions" => (
                    "200 OK",
                    "application/json",
                    json!({ "executions": merge(&hub, live.as_ref()) }).to_string(),
                ),
                "/api/execution" => match detail(&hub, live.as_ref(), id) {
                    Some(document) => ("200 OK", "application/json", document.to_string()),
                    None => ("404 Not Found", "application/json", "{\"error\":\"unknown execution\"}".to_string()),
                },
                "/health" => ("200 OK", "text/plain", "ok".to_string()),
                "/" => ("200 OK", "text/html; charset=utf-8", crate::dashboard_page().to_string()),
                _ => ("404 Not Found", "text/plain", "not found".to_string()),
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

/// Counters, from the live snapshot when the execution plane answers.
fn stats_document(hub: &Hub, live: Option<&Value>) -> String {
    let history = hub.store.recent(1);
    let base = live.cloned().unwrap_or_else(|| {
        json!({
            "version": "hub", "protocols": ["1.0", "2.0"], "pid": std::process::id(),
            "uptimeMs": 0, "live": 0, "spawned": 0, "settled": 0, "killed": 0,
            "failed": 0, "leases": 0, "maxConcurrent": 0, "executions": [],
        })
    });
    let mut document = base.clone();
    document["historyRows"] = json!(hub.store.count());
    document["lastStartedAtMs"] = json!(history_first_started(history));
    document.to_string()
}

/// First element's start time, if any.
fn history_first_started(rows: Vec<(String, String, String)>) -> Option<u64> {
    rows.first().and_then(|(_, _, document)| {
        serde_json::from_str::<Value>(document).ok()?.get("startedAtMs")?.as_u64()
    })
}

/// Live rows when the execution plane answers, otherwise stored history.
fn merge(hub: &Hub, live: Option<&Value>) -> Vec<Value> {
    let mut rows: Vec<Value> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    if let Some(executions) = live.and_then(|snapshot| snapshot.get("executions")).and_then(|value| value.as_array()) {
        for row in executions {
            if let Some(id) = row.get("id").and_then(|value| value.as_str()) {
                seen.insert(id.to_string());
            }
            rows.push(row.clone());
        }
    }
    for (id, _, document) in hub.store.recent(50) {
        if seen.contains(&id) {
            continue;
        }
        if let Ok(value) = serde_json::from_str::<Value>(&document) {
            rows.push(value);
        }
    }
    rows.sort_by_key(|row| std::cmp::Reverse(row["startedAtMs"].as_u64().unwrap_or(0)));
    rows.truncate(50);
    rows
}

/// One execution: live first, then history.
fn detail(hub: &Hub, live: Option<&Value>, id: &str) -> Option<Value> {
    if let Some(executions) = live.and_then(|snapshot| snapshot.get("executions")).and_then(|value| value.as_array()) {
        if let Some(row) = executions.iter().find(|row| row.get("id").and_then(|value| value.as_str()) == Some(id)) {
            return Some(row.clone());
        }
    }
    hub.store
        .recent(500)
        .into_iter()
        .find(|(stored, _, _)| stored == id)
        .and_then(|(_, _, document)| serde_json::from_str(&document).ok())
}

/// Open the history database the execution plane writes.
pub fn open_store(path: &Path) -> Option<Arc<Store>> {
    match Store::open_read_only(path) {
        Ok(store) => Some(Arc::new(store)),
        Err(error) => {
            eprintln!("hub: cannot open {}: {error}", path.display());
            None
        }
    }
}
