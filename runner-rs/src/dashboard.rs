//! Read-only dashboard: statistics, a recent-execution list, one execution's
//! input and captured output, plus one self-contained page.
//!
//! Bound to a private address behind the deployment's reverse proxy. It never
//! exposes environment variables: a command's environment can carry credentials,
//! while its argv and output are what an operator needs to explain a run.

use std::sync::Arc;

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
            let mut buffer = [0u8; 4096];
            let read = socket.read(&mut buffer).await.unwrap_or(0);
            let request = String::from_utf8_lossy(&buffer[..read]);
            let target = request.split_whitespace().nth(1).unwrap_or("/");
            let (path, query) = match target.split_once('?') {
                Some((path, query)) => (path, query),
                None => (target, ""),
            };
            let id = query
                .split('&')
                .find_map(|pair| pair.strip_prefix("id="))
                .map(percent_decode)
                .unwrap_or_default();
            let (status, content_type, body) = match path {
                "/api/stats" => ("200 OK", "application/json", registry.stats().to_string()),
                "/api/executions" => (
                    "200 OK",
                    "application/json",
                    serde_json::json!({ "executions": registry.recent(50) }).to_string(),
                ),
                "/api/execution" => match registry.detail(&id) {
                    Some(detail) => ("200 OK", "application/json", detail.to_string()),
                    None => ("404 Not Found", "application/json", "{\"error\":\"unknown execution\"}".to_string()),
                },
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

/// Decode `%XX` escapes from a query value.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(&value[index + 1..index + 3], 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The single-page dashboard.
fn page() -> &'static str {
    r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>dsh shellout runner</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{color-scheme:dark}
body{margin:0;padding:24px;background:#0f1115;color:#e6e6ea;font:14px/1.5 ui-sans-serif,system-ui,-apple-system,sans-serif}
h1{margin:0 0 4px;font-size:18px;font-weight:600}
.sub{color:#8b8d98;font-size:12px;margin-bottom:20px}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:20px}
.card{background:#171a21;border:1px solid #242833;border-radius:10px;padding:12px 14px}
.card .k{color:#8b8d98;font-size:11px;text-transform:uppercase;letter-spacing:.04em}
.card .v{font-size:20px;font-weight:600;margin-top:4px}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:7px 10px;border-bottom:1px solid #22262f}
th{color:#8b8d98;font-weight:500}
tbody tr{cursor:pointer}
tbody tr:hover{background:#171a21}
tbody tr.sel{background:#1b2230}
code,pre{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:#9ecbff}
pre{margin:0;padding:12px;background:#0b0d11;border:1px solid #22262f;border-radius:8px;overflow:auto;max-height:420px;white-space:pre-wrap;word-break:break-word}
.empty{color:#6d7078;padding:14px 0}
#detail{margin-top:24px;display:none}
#detail.on{display:block}
.dhead{display:flex;flex-wrap:wrap;gap:14px;align-items:baseline;margin-bottom:10px}
.dtitle{font-size:13px;font-weight:600;color:#e6e6ea}
.dmeta{color:#8b8d98;font-size:12px}
.tabs{display:flex;gap:8px;margin:10px 0 8px}
.tab{background:#171a21;border:1px solid #242833;color:#b8b8c0;border-radius:8px;padding:4px 12px;font-size:12px;cursor:pointer}
.tab.on{border-color:#3d6fd8;color:#cfe0ff}
.flag{color:#e5a15a;font-size:11px;margin-left:8px}
.badge{font-size:11px;padding:1px 7px;border-radius:999px;border:1px solid #2b3140}
.badge.running{color:#7fd8a4;border-color:#2f6b4c}
.badge.exited{color:#9ecbff;border-color:#2f4d7a}
.badge.killed{color:#e5a15a;border-color:#6b4f2f}
</style></head><body>
<h1>dsh shellout runner</h1>
<div class="sub" id="sub">loading…</div>
<div class="cards" id="cards"></div>
<table><thead><tr><th>execution</th><th>command</th><th>state</th><th>exit</th><th>age / duration</th></tr></thead>
<tbody id="rows"><tr><td colspan="5" class="empty">no executions yet</td></tr></tbody></table>
<div id="detail">
  <div class="dhead"><span class="dtitle" id="dtitle"></span><span class="dmeta" id="dmeta"></span></div>
  <pre id="dcmd"></pre>
  <div class="tabs"><button class="tab on" data-tab="stdout">stdout</button><button class="tab" data-tab="stderr">stderr</button></div>
  <pre id="dout">—</pre>
</div>
<script>
const fmt = (ms) => ms == null ? '—' : ms < 1000 ? ms + ' ms' : (ms / 1000).toFixed(1) + ' s'
let selected = null
let tab = 'stdout'

function esc(text) {
  return String(text).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]))
}

async function loadStats() {
  const stats = await (await fetch('/api/stats', { cache: 'no-store' })).json()
  document.getElementById('sub').textContent =
    `${stats.version} \u00b7 protocol ${stats.protocols.join('/')} \u00b7 up ${fmt(stats.uptimeMs)} \u00b7 pid ${stats.pid}`
  const cards = [['live', stats.live], ['spawned', stats.spawned], ['settled', stats.settled],
    ['killed', stats.killed], ['failed', stats.failed], ['leases', stats.leases], ['limit', stats.maxConcurrent]]
  document.getElementById('cards').innerHTML = cards
    .map(([k, v]) => `<div class="card"><div class="k">${k}</div><div class="v">${v}</div></div>`).join('')
}

async function loadList() {
  const { executions } = await (await fetch('/api/executions', { cache: 'no-store' })).json()
  const rows = document.getElementById('rows')
  if (executions.length === 0) {
    rows.innerHTML = '<tr><td colspan="5" class="empty">no executions yet</td></tr>'
    return
  }
  rows.innerHTML = executions.map((e) => `
    <tr data-id="${esc(e.id)}" class="${e.id === selected ? 'sel' : ''}">
      <td><code>${esc(e.id.slice(0, 8))}</code></td>
      <td><code>${esc(e.command)}</code></td>
      <td><span class="badge ${e.state}">${e.state}</span></td>
      <td>${e.exitCode == null ? '\u2014' : e.exitCode}</td>
      <td>${fmt(e.durationMs != null ? e.durationMs : e.ageMs)}</td>
    </tr>`).join('')
  for (const row of rows.querySelectorAll('tr[data-id]')) {
    row.addEventListener('click', () => { selected = row.dataset.id; renderDetail() })
  }
}

function renderDetail() {
  if (!selected) return
  fetch('/api/execution?id=' + encodeURIComponent(selected), { cache: 'no-store' })
    .then((r) => r.ok ? r.json() : Promise.reject(r.status))
    .then((d) => {
      document.getElementById('detail').classList.add('on')
      document.getElementById('dtitle').textContent = d.id
      document.getElementById('dmeta').innerHTML =
        `<span class="badge ${d.state}">${d.state}</span> \u00b7 exit ${d.exitCode == null ? '\u2014' : d.exitCode}` +
        (d.signal == null ? '' : ' \u00b7 signal ' + d.signal) + ' \u00b7 ' + fmt(d.durationMs != null ? d.durationMs : d.ageMs) +
        ' \u00b7 cwd ' + esc(d.cwd) + (d.lossy ? '<span class="flag">output truncated (tail kept)</span>' : '')
      document.getElementById('dcmd').textContent = '$ ' + d.argv.join(' ')
      const text = tab === 'stdout' ? d.stdout : d.stderr
      document.getElementById('dout').textContent = text && text.length > 0 ? text : '\u2014'
    })
    .catch((error) => { document.getElementById('dout').textContent = 'cannot load: ' + error })
}

for (const button of document.querySelectorAll('.tab')) {
  button.addEventListener('click', () => {
    tab = button.dataset.tab
    for (const other of document.querySelectorAll('.tab')) other.classList.toggle('on', other === button)
    renderDetail()
  })
}

async function tick() {
  try { await loadStats(); await loadList(); if (selected) renderDetail() } catch (error) {
    document.getElementById('sub').textContent = 'runner unreachable: ' + error
  }
}
tick(); setInterval(tick, 2000)
</script></body></html>"#
}
