/** Isolated Rust execution-plane scenarios; no harness or service dependency. */
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import net from 'node:net'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const index = process.argv.indexOf('--binary')
const binary = resolve(repo, index < 0 ? 'runner-rs/target/debug/dsh-shellout-runner' : process.argv[index + 1])
const temp = join(repo, '.task-tmp')
mkdirSync(temp, { recursive: true })
const root = mkdtempSync(join(temp, 'e2e-'))
const state = join(root, 'state')
const socket = join(root, 'control.sock')
const history = join(root, 'history.sqlite')
const children = new Set()
const clients = new Set()
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
let serial = 0
let stderr = ''

async function until(fn, label, timeout = 12_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    const value = await fn()
    if (value) return value
    await sleep(50)
  }
  throw new Error(`timeout: ${label}\n${stderr}`)
}
function startExecutor() {
  const child = spawn(binary, ['--state', state, '--history', history, '--socket', socket, '--lease-ttl-ms', '60000'], {
    cwd: root, env: { ...process.env, DSH_HOME: join(root, 'home') }, stdio: ['ignore', 'ignore', 'pipe'],
  })
  children.add(child)
  child.once('exit', () => children.delete(child))
  child.stderr.on('data', chunk => { stderr += chunk })
  return child
}
async function stop(child, signal) {
  if (child.exitCode !== null || child.signalCode !== null) return
  const exited = once(child, 'exit')
  child.kill(signal)
  await exited
}
async function connect() {
  const stream = net.createConnection(socket)
  await new Promise((resolve, reject) => { stream.once('connect', resolve); stream.once('error', reject) })
  stream.removeAllListeners('error')
  clients.add(stream)
  let buffer = Buffer.alloc(0)
  let next = 0
  const waiting = new Map()
  const events = []
  stream.on('data', chunk => {
    buffer = Buffer.concat([buffer, chunk])
    while (buffer.length >= 4 && buffer.length >= 4 + buffer.readUInt32BE(0)) {
      const length = buffer.readUInt32BE(0)
      const frame = JSON.parse(buffer.subarray(4, 4 + length).toString())
      buffer = buffer.subarray(4 + length)
      if (frame.kind === 'evt') { events.push(frame); continue }
      const pending = waiting.get(frame.id)
      if (!pending) continue
      waiting.delete(frame.id)
      clearTimeout(pending.timer)
      if (frame.kind === 'err') pending.reject(new Error(JSON.stringify(frame.payload)))
      else if (frame.kind !== 'res' || frame.method !== pending.method) pending.reject(new Error('bad reply correlation'))
      else pending.resolve(frame.payload)
    }
  })
  const closed = () => {
    clients.delete(stream)
    for (const pending of waiting.values()) { clearTimeout(pending.timer); pending.reject(new Error('executor disconnected')) }
    waiting.clear()
  }
  stream.on('error', closed)
  stream.on('close', closed)
  return {
    events,
    close: () => stream.destroy(),
    request(method, payload = {}) {
      return new Promise((resolve, reject) => {
        const id = ++next
        const body = Buffer.from(JSON.stringify({ id, kind: 'req', method, payload }))
        const header = Buffer.alloc(4)
        header.writeUInt32BE(body.length)
        const timer = setTimeout(() => { waiting.delete(id); reject(new Error(`RPC timeout: ${method}`)) }, 5000)
        waiting.set(id, { resolve, reject, timer, method })
        stream.write(Buffer.concat([header, body]))
      })
    },
  }
}
async function ready() {
  return until(async () => { try { const client = await connect(); await client.request('observe'); return client } catch { return null } }, 'executor ready')
}
async function host() {
  const client = await connect()
  const hello = await client.request('hello', { protocol: '2.0', clientBuild: 'e2e', bootId: 'survival-host', maxFrameBytes: 8 * 1024 * 1024 })
  return { client, hello }
}
async function exec(client, command, overrides = {}) {
  const id = `e2e-${++serial}`
  await client.request('exec', { executionId: id, operationId: `op-${id}`, specDigest: command,
    spec: { argv: ['/bin/sh', '-c', command], cwd: root, env: { PATH: '/usr/bin:/bin' }, timeoutMs: 10000,
      onExpiry: 'kill', graceMs: 100, capture: { stdoutMaxBytes: 64000, stderrMaxBytes: 64000, spillMaxBytes: 1048576, spillDir: state }, ...overrides } })
  return id
}
async function settled(client, id) {
  return until(async () => { const value = await client.request('state', { executionId: id }); return value.state !== 'running' && value }, `settlement ${id}`)
}
const status = id => JSON.parse(readFileSync(join(state, id, 'status.json'), 'utf8'))
function ok(label) { console.log(`PASS ${label}`) }

async function startHub(database = history) {
  const reservation = net.createServer()
  reservation.listen(0, '127.0.0.1')
  await once(reservation, 'listening')
  const port = reservation.address().port
  await new Promise(resolve => reservation.close(resolve))
  const child = spawn(binary, ['--mode', 'hub', '--listen', `127.0.0.1:${port}`, '--executor-socket', socket, '--history', database], {
    cwd: root, env: { ...process.env, DSH_HOME: join(root, 'home') }, stdio: ['ignore', 'ignore', 'pipe'],
  })
  children.add(child)
  child.once('exit', () => children.delete(child))
  child.stderr.on('data', chunk => { stderr += chunk })
  const url = `http://127.0.0.1:${port}`
  await until(async () => { try { return (await fetch(`${url}/health`)).ok } catch { return false } }, 'hub ready')
  return { child, url }
}

async function main() {
  let runner = startExecutor()
  const observer = await ready()
  assert.equal((await observer.request('observe')).leases, 0)
  observer.close()
  let { client } = await host()
  const basic = await exec(client, 'printf "hello\\n"; printf "oops\\n" >&2; exit 3')
  assert.equal((await settled(client, basic)).exitCode, 3)
  assert.equal((await client.request('readStream', { executionId: basic, stream: 'stdout', offset: 0, limit: 2 })).text, 'he')
  assert.equal((await client.request('readStream', { executionId: basic, stream: 'stdout', offset: 2, limit: 100 })).text, 'llo\n')
  assert.equal((await client.request('readStream', { executionId: basic, stream: 'stderr', offset: 0, limit: 100 })).text, 'oops\n')
  ok('stdout/stderr offsets and real exit code')

  const capped = await exec(client, 'printf 123456789; printf abcdef >&2', {
    capture: { stdoutMaxBytes: 4, stderrMaxBytes: 3, spillMaxBytes: 0, spillDir: state },
  })
  await settled(client, capped)
  const cappedOut = await client.request('readStream', { executionId: capped, stream: 'stdout', offset: 0, limit: 100 })
  assert.equal(cappedOut.text, '1234')
  assert.equal(cappedOut.lossy, true)
  assert.equal(status(capped).stdout.bytes, 9)
  assert.equal(status(capped).stderr.truncated, true)
  ok('file byte cap and per-stream truncation facts')

  const timed = await exec(client, "trap '' TERM; while :; do sleep 1; done", { timeoutMs: 200, graceMs: 100 })
  const deadlineResult = await settled(client, timed)
  assert.equal(deadlineResult.state, 'killed')
  assert.equal(deadlineResult.signal, 9)
  ok('deadline TERM-to-KILL escalation')

  const background = await exec(client, 'sleep 600 & echo started')
  assert.equal((await settled(client, background)).exitCode, 0)
  ok('background descendant cannot hold settlement open')

  // A settlement announced while no host connection existed must still reach
  // the reconnecting host: the event is sent once and never repeated.
  const replay = await exec(client, 'sleep 1; exit 5');
  await until(() => existsSync(join(state, replay, 'status.json')), 'replay execution starts');
  await stop(runner, 'SIGKILL');
  await sleep(2000);
  runner = startExecutor();
  ;(await ready()).close();
  const revived = await host();
  client = revived.client;
  const replayRow = revived.hello.executions.find(row => row.executionId === replay);
  assert(replayRow, 'the adopted execution is reported at handshake');
  assert.equal(replayRow.state, 'exited');
  assert.equal(replayRow.exitCode, 5);
  await until(() => client.events.some(frame => frame.method === 'settled' && frame.payload.executionId === replay && frame.payload.exitCode === 5), 'replayed settlement');
  ok('a settlement nobody was subscribed for is replayed to the reconnecting host');

  const survivor = await exec(client, 'i=0; while [ "$i" -lt 5 ]; do echo marker-$i; i=$((i+1)); sleep 1; done; exit 7')
  const logPath = join(state, survivor, 'stdout.log')
  await until(() => existsSync(logPath) && readFileSync(logPath, 'utf8').includes('marker-0'), 'first marker')
  const before = readFileSync(logPath, 'utf8').length
  await stop(runner, 'SIGKILL')
  await until(() => readFileSync(logPath, 'utf8').length > before, 'shim writes after executor SIGKILL')
  runner = startExecutor()
  ;(await ready()).close()
  const attached = await host()
  client = attached.client
  assert(attached.hello.executions.some(row => row.executionId === survivor && row.state === 'running'))
  assert.equal((await settled(client, survivor)).exitCode, 7)
  assert.equal(readFileSync(logPath, 'utf8'), 'marker-0\nmarker-1\nmarker-2\nmarker-3\nmarker-4\n')
  // The event is published by the next 250 ms poll after settlement is
  // observed, so it can trail the state reply on the same connection.
  await until(() => client.events.some(frame => frame.method === 'settled' && frame.payload.executionId === survivor && frame.payload.exitCode === 7), 'adopted settlement event')
  ok('SIGKILL survival, matching-pid adoption, continued capture and exit 7')

  const termSurvivor = await exec(client, 'echo ready; sleep 2; echo survived; exit 4')
  await until(() => existsSync(join(state, termSurvivor, 'stdout.log')), 'TERM survivor starts')
  await stop(runner, 'SIGTERM')
  runner = startExecutor()
  ;(await ready()).close()
  ;({ client } = await host())
  assert.equal((await settled(client, termSurvivor)).exitCode, 4)
  ok('executor SIGTERM leaves shim and command running')

  // A forged supervisor identity must not adopt or signal any process.
  const forged = await exec(client, 'sleep 60');
  await until(() => existsSync(join(state, forged, 'status.json')), 'forged execution starts');
  await sleep(300);
  await stop(runner, 'SIGTERM');
  const mismatch = status(forged);
  mismatch.shimStartTime += 1;
  writeFileSync(join(state, forged, 'status.json'), JSON.stringify(mismatch));
  runner = startExecutor();
  ;(await ready()).close()
  ;({ client } = await host());
  assert.equal((await client.request('state', { executionId: forged })).state, 'lost');
  ok('mismatched supervisor start time becomes lost')
  // Two executors on one state directory would each adopt the other's shims.
  const rival = spawn(binary, ['--state', state, '--history', history, '--socket', join(root, 'rival.sock'), '--lease-ttl-ms', '60000'], {
    cwd: root, env: { ...process.env, DSH_HOME: join(root, 'home') }, stdio: ['ignore', 'ignore', 'pipe'],
  });
  let rivalError = '';
  rival.stderr.on('data', chunk => { rivalError += chunk });
  const [rivalCode] = await once(rival, 'exit');
  assert.notEqual(rivalCode, 0, 'the second executor must refuse to start');
  assert(rivalError.includes('already owned by another executor'), rivalError);
  const stillAlive = await exec(client, 'echo owned');
  assert.equal((await settled(client, stillAlive)).exitCode, 0);
  ok('a second executor on the same state directory refuses to start');

  // A descendant that leaves the group and keeps a pipe open cannot stall the
  // shim: the stream is lossy and the terminal facts are published anyway.
  const detachedStarted = Date.now();
  const detached = await exec(client, 'setsid sleep 5 & echo x', { graceMs: 200 });
  const detachedState = await settled(client, detached);
  assert(Date.now() - detachedStarted < 3000, `settlement took ${Date.now() - detachedStarted} ms`);
  assert.equal(detachedState.state, 'exited');
  assert.equal(status(detached).stdout.lossy, true);
  ok('a detached descendant cannot hold settlement open');

  // A shim still terminating its group has already reaped its direct child;
  // adoption must judge the supervisor, not the child.
  const reaped = await exec(client, "trap '' TERM; (sleep 30) & exit 3", { graceMs: 6000 });
  await sleep(1000);
  await stop(runner, 'SIGKILL');
  runner = startExecutor();
  ;(await ready()).close();
  ;({ client } = await host());
  const reapedState = await settled(client, reaped);
  assert.equal(reapedState.state, 'exited');
  assert.equal(reapedState.exitCode, 3);
  ok('a reaped child with a live supervisor survives a restart');

  // A command that cannot be started fails the exec reply itself.
  const spawnFailure = await exec(client, '/bin/true', { cwd: join(root, 'no-such-directory') }).then(() => null, error => error);
  assert(spawnFailure, 'the exec reply must be an error');
  assert(String(spawnFailure.message).includes('cannot start'), String(spawnFailure.message));
  ok('a spawn failure surfaces synchronously from exec');

  const hub = await startHub()
  const liveStats = await (await fetch(`${hub.url}/api/stats`)).json()
  assert.equal(liveStats.executor, 'available')
  assert.equal(liveStats.historyWritable, true)
  const list = await (await fetch(`${hub.url}/api/executions`)).json()
  assert(list.executions.every(row => row.stdout === undefined && row.stderr === undefined))
  const encodedId = [...capped].map(char => `%${char.charCodeAt(0).toString(16)}`).join('')
  const cappedDetail = await (await fetch(`${hub.url}/api/execution?id=${encodedId}`)).json()
  assert.equal(cappedDetail.stdout, '1234')
  assert.equal(cappedDetail.stdoutLossy, true)
  assert.equal(cappedDetail.stderrTruncated, true)
  ok('hub observer, output-free summaries, percent-decoded detail and durable stream flags')
  await stop(runner, 'SIGTERM')
  assert((await fetch(`${hub.url}/`)).ok)
  assert.equal(await (await fetch(`${hub.url}/health`)).text(), 'ok')
  const degraded = await (await fetch(`${hub.url}/api/stats`)).json()
  assert.equal(degraded.executor, 'unreachable')
  assert.equal(degraded.history, 'available')
  assert.equal(degraded.live, undefined)
  assert.equal((await (await fetch(`${hub.url}/api/execution?id=${basic}`)).json()).exitCode, 3)
  const reapedDetail = await (await fetch(`${hub.url}/api/execution?id=${reaped}`)).json();
  assert.equal(reapedDetail.state, 'exited');
  assert.equal(reapedDetail.exitCode, 3);
  await stop(hub.child, 'SIGTERM')
  const emptyHub = await startHub(join(root, 'missing.sqlite'))
  assert((await (await fetch(`${emptyHub.url}/`)).text()).includes('History unavailable'))
  assert.equal((await (await fetch(`${emptyHub.url}/api/stats`)).json()).history, 'unavailable')
  await stop(emptyHub.child, 'SIGTERM')
  ok('hub history-only degradation and missing-history page')
  console.log('Rust execution-plane e2e: all scenarios passed')
}

try { await main() } finally {
  for (const stream of clients) stream.destroy()
  for (const child of children) await stop(child, 'SIGTERM')
  // Only isolated executions created here; request cleanup if an assertion failed.
  if (existsSync(state)) {
    const { readdirSync } = await import('node:fs')
    for (const id of readdirSync(state)) {
      try { if (status(id).state === 'running') writeFileSync(join(state, id, 'kill.request'), '') } catch { /* never admitted */ }
    }
    await sleep(400)
  }
  rmSync(root, { recursive: true, force: true })
}
