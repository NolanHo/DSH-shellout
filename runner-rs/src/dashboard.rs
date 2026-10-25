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
///
/// Selecting a row expands its detail directly underneath that row: the row is
/// already on screen, so nothing navigates and nothing scrolls to reveal it.
/// Only one row is open at a time.
fn page() -> &'static str {
    r#"<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>dsh shellout runner</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{color-scheme:dark;--bg:#0f1115;--panel:#141821;--line:#1d2129;--ink:#e6e6ea;--dim:#8b8d98;--accent:#3d6fd8;--code:#9ecbff}
*{box-sizing:border-box}
html,body{height:100%}
body{margin:0;background:var(--bg);color:var(--ink);font:13px/1.5 ui-sans-serif,system-ui,-apple-system,sans-serif;display:flex;flex-direction:column;overflow:hidden}
header{flex:none;display:flex;flex-wrap:wrap;gap:14px;align-items:center;padding:12px 18px;border-bottom:1px solid var(--line);background:var(--panel)}
.title{font-size:14px;font-weight:600}
.meta{color:var(--dim);font-size:12px}
.pills{display:flex;gap:8px;margin-left:auto;flex-wrap:wrap}
.pill{background:#171a21;border:1px solid #242833;border-radius:8px;padding:3px 10px;font-size:11px;color:var(--dim)}
.pill b{color:var(--ink);font-size:12px;margin-left:5px}
.tools{flex:none;display:flex;gap:8px;align-items:center;padding:8px 12px;border-bottom:1px solid var(--line)}
.tools input{flex:1;background:#0b0d11;border:1px solid #242833;border-radius:8px;color:var(--ink);padding:5px 9px;font:12px/1.4 ui-monospace,Menlo,monospace}
.tools input:focus{outline:none;border-color:var(--accent)}
.filters{flex:none;display:flex;gap:6px;padding:8px 12px 0}
.fbtn{background:#171a21;border:1px solid #242833;border-radius:8px 8px 0 0;color:var(--dim);padding:5px 14px;font-size:12px;cursor:pointer;border-bottom:none}
.fbtn.on{background:var(--bg);border-color:var(--accent);color:#cfe0ff}
.toggle{background:#171a21;border:1px solid #242833;border-radius:8px;color:var(--dim);padding:4px 10px;font-size:11px;cursor:pointer}
.toggle.on{border-color:var(--accent);color:#cfe0ff}
#listScroll{flex:1;min-height:0;overflow:auto}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:6px 12px;border-bottom:1px solid #191d25}
th{position:sticky;top:0;background:var(--panel);color:var(--dim);font-weight:500;z-index:2}
tr.row{cursor:pointer}
tr.row:hover{background:#171a21}
tr.row.sel{background:#1a2130;box-shadow:inset 3px 0 0 var(--accent)}
td.cmd{color:#c9d4e6;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:1px;font-family:ui-monospace,Menlo,monospace;font-size:11.5px}
tr.detail>td{background:#0c0f14;padding:0;border-bottom:1px solid #22262f}
.dwrap{padding:14px 16px 16px}
.dhead{display:flex;flex-wrap:wrap;gap:10px;align-items:baseline;margin-bottom:10px;color:var(--dim);font-size:12px}
.dhead b{color:var(--ink);font-weight:600}
pre{margin:0 0 12px;padding:11px 13px;background:#0b0d11;border:1px solid #22262f;border-radius:8px;white-space:pre-wrap;word-break:break-word;font:12px/1.55 ui-monospace,Menlo,monospace;color:var(--code);max-height:46vh;overflow:auto}
.tabs{display:flex;gap:8px;margin:0 0 8px}
.tab{background:#171a21;border:1px solid #242833;color:#b8b8c0;border-radius:8px;padding:3px 12px;font-size:11px;cursor:pointer}
.tab.on{border-color:var(--accent);color:#cfe0ff}
.badge{font-size:11px;padding:1px 8px;border-radius:999px;border:1px solid #2b3140}
.badge.running{color:#7fd8a4;border-color:#2f6b4c}
.badge.exited{color:#9ecbff;border-color:#2f4d7a}
.badge.killed{color:#e5a15a;border-color:#6b4f2f}
.empty{color:#6d7078;padding:18px}
.flag{color:#e5a15a}
</style></head><body>
<header>
  <span class="title">dsh shellout runner</span>
  <span class="meta" id="sub">connecting…</span>
  <div class="pills" id="pills"></div>
</header>
<div class="filters" id="filters">
  <button class="fbtn on" data-filter="all">全部 <b data-count="all">0</b></button>
  <button class="fbtn" data-filter="running">运行中 <b data-count="running">0</b></button>
  <button class="fbtn" data-filter="finished">已结束 <b data-count="finished">0</b></button>
</div>
<div class="tools">
  <input id="filter" placeholder="filter by command…" autocomplete="off">
  <button class="toggle on" id="follow" title="pause the two-second refresh">follow</button>
</div>
<div id="listScroll">
  <table><thead><tr><th>exec</th><th>command</th><th>state</th><th>exit</th><th>started</th><th>duration</th></tr></thead>
  <tbody id="rows"><tr><td colspan="6" class="empty">no executions yet</td></tr></tbody></table>
</div>
<script>
const fmt = (ms) => ms == null ? '\u2014' : ms < 1000 ? ms + ' ms' : (ms / 1000).toFixed(1) + ' s'
const esc = (text) => String(text).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]))
let selected = null
let tab = 'stdout'
let filter = ''
let kind = 'all'
let follow = true
let rowsCache = []
let detailCache = null

const clock = (ms) => {
  if (ms == null) return '\u2014'
  const d = new Date(ms)
  return d.toLocaleTimeString([], { hour12: false })
}

const commandLine = (execution) => {
  const argv = execution.argv || []
  const raw = (argv.length > 2 ? argv.slice(2).join(' ') : argv.join(' ')) || execution.command || ''
  const text = raw.replace(/\s+/g, ' ').trim()
  const stripped = text.replace(/^cd\s+[^&|;]+?(?:&&|;)\s*/, '')
  return stripped.length > 0 ? stripped : text
}

async function loadStats() {
  const stats = await (await fetch('/api/stats', { cache: 'no-store' })).json()
  document.getElementById('sub').textContent =
    `${stats.version} \u00b7 protocol ${stats.protocols.join('/')} \u00b7 up ${fmt(stats.uptimeMs)} \u00b7 pid ${stats.pid}`
  const pills = [['live', stats.live], ['spawned', stats.spawned], ['settled', stats.settled],
    ['killed', stats.killed], ['failed', stats.failed], ['leases', stats.leases], ['limit', stats.maxConcurrent]]
  document.getElementById('pills').innerHTML = pills
    .map(([k, v]) => `<span class="pill">${k}<b>${v}</b></span>`).join('')
}

function detailMarkup(d) {
  if (!d) return '<div class="empty">This execution is no longer retained.</div>'
  const text = tab === 'stdout' ? d.stdout : d.stderr
  return `
    <div class="dwrap">
      <div class="dhead">
        <span class="badge ${d.state}">${d.state}</span>
        <span>exit <b>${d.exitCode == null ? '\u2014' : d.exitCode}</b></span>
        ${d.signal == null ? '' : `<span>signal <b>${d.signal}</b></span>`}
        <span>${fmt(d.durationMs != null ? d.durationMs : d.ageMs)}</span>
        <span>CWD ${esc(d.cwd)}</span>
        ${d.lossy ? '<span class="flag">output truncated (tail kept)</span>' : ''}
        <span>${esc(d.id)}</span>
      </div>
      <pre>$ ${esc((d.argv || []).join(' '))}</pre>
      <div class="tabs">
        <button class="tab ${tab === 'stdout' ? 'on' : ''}" data-tab="stdout">stdout</button>
        <button class="tab ${tab === 'stderr' ? 'on' : ''}" data-tab="stderr">stderr</button>
      </div>
      <pre data-output>${text && text.length > 0 ? esc(text) : '\u2014'}</pre>
    </div>`
}

function render() {
  const needle = filter.trim().toLowerCase()
  const byKind = kind === 'running' ? rowsCache.filter(e => e.state === 'running')
    : kind === 'finished' ? rowsCache.filter(e => e.state !== 'running')
    : rowsCache
  const visible = needle === '' ? byKind : byKind.filter(e => commandLine(e).toLowerCase().includes(needle))
  const counts = {
    all: rowsCache.length,
    running: rowsCache.filter(e => e.state === 'running').length,
    finished: rowsCache.filter(e => e.state !== 'running').length,
  }
  for (const [key, value] of Object.entries(counts)) {
    const node = document.querySelector(`[data-count="${key}"]`)
    if (node) node.textContent = String(value)
  }
  const scroll = document.getElementById('listScroll')
  const keepTop = scroll.scrollTop
  const rows = document.getElementById('rows')
  if (visible.length === 0) {
    rows.innerHTML = `<tr><td colspan="6" class="empty">${rowsCache.length === 0 ? 'no executions yet' : 'nothing matches the filter'}</td></tr>`
    return
  }
  let html = ''
  for (const e of visible) {
    html += `<tr class="row ${e.id === selected ? 'sel' : ''}" data-id="${esc(e.id)}">
      <td><code>${esc(e.id.slice(0, 8))}</code></td>
      <td class="cmd" title="${esc(commandLine(e))}">${esc(commandLine(e))}</td>
      <td><span class="badge ${e.state}">${e.state}</span></td>
      <td>${e.exitCode == null ? '\u2014' : e.exitCode}</td>
      <td>${clock(e.startedAtMs)}</td>
      <td>${fmt(e.durationMs != null ? e.durationMs : e.ageMs)}</td>
    </tr>`
    if (e.id === selected) {
      html += `<tr class="detail" data-detail><td colspan="6">${detailMarkup(detailCache)}</td></tr>`
    }
  }
  rows.innerHTML = html
  for (const row of rows.querySelectorAll('tr.row')) {
    row.addEventListener('click', () => toggle(row.dataset.id))
  }
  for (const button of rows.querySelectorAll('.tab')) {
    button.addEventListener('click', (event) => {
      event.stopPropagation()
      tab = button.dataset.tab
      render()
    })
  }
  scroll.scrollTop = keepTop
}

async function loadList() {
  const { executions } = await (await fetch('/api/executions', { cache: 'no-store' })).json()
  rowsCache = executions
  if (selected !== null && !executions.some(e => e.id === selected)) {
    detailCache = null
  }
  render()
}

async function loadDetail() {
  if (selected === null) { detailCache = null; return }
  const response = await fetch('/api/execution?id=' + encodeURIComponent(selected), { cache: 'no-store' })
  detailCache = response.ok ? await response.json() : null
}

function toggle(id) {
  if (selected === id) { selected = null; detailCache = null; render(); return }
  selected = id
  detailCache = null
  render()
  loadDetail().then(() => {
    // Replace only the open row's content so the list does not flicker.
    const cell = document.querySelector('tr.detail td')
    if (cell && selected === id) {
      cell.innerHTML = detailMarkup(detailCache)
      for (const button of cell.querySelectorAll('.tab')) {
        button.addEventListener('click', (event) => { event.stopPropagation(); tab = button.dataset.tab; render() })
      }
    }
  })
}

for (const button of document.querySelectorAll('.fbtn')) {
  button.addEventListener('click', () => {
    kind = button.dataset.filter
    for (const other of document.querySelectorAll('.fbtn')) other.classList.toggle('on', other === button)
    render()
  })
}
document.getElementById('follow').addEventListener('click', (event) => {
  follow = !follow
  event.target.classList.toggle('on', follow)
})
document.getElementById('filter').addEventListener('input', (event) => {
  filter = event.target.value
  render()
})
document.addEventListener('keydown', (event) => {
  if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return
  const rows = [...document.querySelectorAll('tr.row')]
  if (rows.length === 0) return
  const index = rows.findIndex(row => row.dataset.id === selected)
  const next = event.key === 'ArrowDown' ? Math.min(rows.length - 1, index + 1) : Math.max(0, index - 1)
  const target = rows[index === -1 ? 0 : next]
  if (!target) return
  event.preventDefault()
  toggle(target.dataset.id)
  target.scrollIntoView({ block: 'nearest' })
})

async function tick() {
  try {
    await loadStats()
    if (!follow && rowsCache.length > 0) return
    const selectedRow = rowsCache.find(e => e.id === selected)
    await loadList()
    // A live execution keeps producing output; refresh just its pane.
    if (selected !== null && (selectedRow === undefined || selectedRow.state === 'running')) {
      await loadDetail()
      const cell = document.querySelector('tr.detail td')
      if (cell && detailCache !== null) {
        const output = cell.querySelector('[data-output]')
        const keep = output ? output.scrollTop : 0
        cell.innerHTML = detailMarkup(detailCache)
        for (const button of cell.querySelectorAll('.tab')) {
          button.addEventListener('click', (event) => { event.stopPropagation(); tab = button.dataset.tab; render() })
        }
        const next = cell.querySelector('[data-output]')
        if (next) next.scrollTop = keep
      }
    }
  } catch (error) {
    document.getElementById('sub').textContent = 'runner unreachable: ' + error
  }
}
tick(); setInterval(tick, 2000)
</script></body></html>"#
}
