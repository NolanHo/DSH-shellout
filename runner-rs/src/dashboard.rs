//! Read-only dashboard: a JSON statistics endpoint plus one self-contained page.
//!
//! Bound to a private address and meant to sit behind the deployment's reverse
//! proxy; it exposes counts and live execution summaries, never captured output.

use std::sync::Arc;

use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::registry::Registry;

/// Serve the dashboard until the process stops.
pub async fn serve(address: String, registry: Arc<Registry>) -> std::io::Result<()> {
    let listener = match TcpListener::bind(&address).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("dashboard: cannot bind {address}: {error}");
            return Err(error);
        }
    };
    eprintln!("dashboard listening on http://{address}");
    loop {
        let (mut socket, _) = listener.accept().await?;
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let mut buffer = [0u8; 2048];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let path = request.split_whitespace().nth(1).unwrap_or("/");
            let (status, content_type, body) = match path {
                "/api/stats" => ("200 OK", "application/json", registry.stats().to_string()),
                "/health" => ("200 OK", "text/plain", "ok".to_string()),
                "/" => ("200 OK", "text/html; charset=utf-8", page().to_string()),
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

/// The single-page dashboard: fetch `/api/stats` and render it.
fn page() -> &'static str {
    r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>dsh shellout runner</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{color-scheme:dark}
body{margin:0;padding:24px;background:#0f1115;color:#e6e6ea;font:14px/1.5 ui-sans-serif,system-ui,-apple-system,sans-serif}
h1{margin:0 0 4px;font-size:18px;font-weight:600}
.sub{color:#8b8d98;font-size:12px;margin-bottom:20px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin-bottom:24px}
.card{background:#171a21;border:1px solid #242833;border-radius:10px;padding:12px 14px}
.card .k{color:#8b8d98;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:22px;font-weight:600;margin-top:4px}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid #22262f}
th{color:#8b8d98;font-weight:500}
code{color:#9ecbff;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.empty{color:#6d7078;padding:14px 0}
</style></head><body>
<h1>dsh shellout runner</h1>
<div class="sub" id="sub">loading…</div>
<div class="cards" id="cards"></div>
<table><thead><tr><th>execution</th><th>command</th><th>state</th><th>age</th></tr></thead>
<tbody id="rows"><tr><td colspan="4" class="empty">no live executions</td></tr></tbody></table>
<script>
const fmt = (ms) => ms < 1000 ? ms + ' ms' : (ms/1000).toFixed(1) + ' s'
async function tick() {
  try {
    const stats = await (await fetch('/api/stats', { cache: 'no-store' })).json()
    document.getElementById('sub').textContent =
      `${stats.version} · protocol ${stats.protocols.join('/')} · up ${fmt(stats.uptimeMs)} · pid ${stats.pid}`
    const cards = [
      ['live', stats.live],
      ['spawned', stats.spawned],
      ['settled', stats.settled],
      ['killed', stats.killed],
      ['failed', stats.failed],
      ['leases', stats.leases],
      ['limit', stats.maxConcurrent],
    ]
    document.getElementById('cards').innerHTML = cards
      .map(([k, v]) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join('')
    const rows = stats.executions || []
    document.getElementById('rows').innerHTML = rows.length === 0
      ? '<tr><td colspan="4" class="empty">no live executions</td></tr>'
      : rows.map(e => `<tr><td><code>${e.id}</code></td><td><code>${e.command}</code></td><td>${e.state}</td><td>${fmt(e.ageMs)}</td></tr>`).join('')
  } catch (error) {
    document.getElementById('sub').textContent = 'runner unreachable: ' + error
  }
}
tick(); setInterval(tick, 2000)
</script></body></html>"#
}
