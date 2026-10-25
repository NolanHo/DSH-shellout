/**
 * End-to-end check of the real host plugin against the Rust executor.
 *
 * `scripts/e2e-rust.mjs` speaks the protocol itself and `packages/host` runs
 * against the TypeScript runner, so neither covers the pair that ships: the
 * cordis host plugin `dsh-shellout`, mounted the way
 * `examples/host-profile.cordis.patch.yml` mounts it, talking to `runner-rs`.
 * This script boots the Rust binary on a temporary state directory and socket,
 * mounts the plugin from this worktree into a fresh cordis context, and drives
 * it through the harness shell seam.
 *
 * Checks:
 *   1. basic   — output and exit code arrive through the host plugin
 *   2. restart — SIGKILL the executor mid-command; the successor adopts the
 *                shim and the reconnecting host settles with the real exit code
 *   3. missed  — the command finishes while no executor runs; the reconnecting
 *                host settles it from the handshake facts
 *
 * Usage: node --import tsx/esm scripts/e2e-host-rust.mjs [--binary <path>]
 */

import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import net from 'node:net'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const index = process.argv.indexOf('--binary')
const binary = resolve(repo, index < 0 ? 'runner-rs/target/debug/dsh-shellout-runner' : process.argv[index + 1])
const temp = join(repo, '.task-tmp')
mkdirSync(temp, { recursive: true })
const root = mkdtempSync(join(temp, 'e2e-host-'))
const state = join(root, 'state')
const socket = join(root, 'control.sock')
const history = join(root, 'history.sqlite')
const children = new Set()
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms))
let stderr = ''

/** Resolve the harness packages exactly as the plugin's package does. */
const hostRequire = createRequire(join(repo, 'packages/host/package.json'))

async function until(fn, label, timeout = 20_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    const value = await fn()
    if (value) return value
    await sleep(50)
  }
  throw new Error(`timeout: ${label}\n${stderr}`)
}

async function within(promise, label, timeout = 40_000) {
  let timer
  const expiry = new Promise((_, reject) => {
    timer = setTimeout(() => { reject(new Error(`timeout: ${label}\n${stderr}`)) }, timeout)
  })
  try {
    return await Promise.race([promise, expiry])
  } finally {
    clearTimeout(timer)
  }
}

function startExecutor() {
  const child = spawn(binary, ['--state', state, '--history', history, '--socket', socket, '--lease-ttl-ms', '60000'], {
    cwd: root, env: { ...process.env, DSH_HOME: join(root, 'home') }, stdio: ['ignore', 'ignore', 'pipe'],
  })
  children.add(child)
  child.once('exit', () => { children.delete(child) })
  child.stderr.on('data', (chunk) => { stderr += chunk })
  return child
}

async function stop(child, signal) {
  if (child.exitCode !== null || child.signalCode !== null) return
  const exited = once(child, 'exit')
  child.kill(signal)
  await exited
}

/** Prove the socket accepts a connection before asserting on the host. */
async function serving() {
  const stream = net.createConnection(socket)
  try {
    await new Promise((resolve, reject) => {
      stream.once('connect', resolve)
      stream.once('error', reject)
    })
    return true
  } catch {
    return false
  } finally {
    stream.destroy()
  }
}

function stateDirectories() {
  if (!existsSync(state)) return []
  return readdirSync(state).filter(name => !name.startsWith('.'))
}

function status(id) {
  return JSON.parse(readFileSync(join(state, id, 'status.json'), 'utf8'))
}

function captured(id) {
  try {
    return readFileSync(join(state, id, 'stdout.log'), 'utf8')
  } catch {
    return ''
  }
}

/** The execution directory the host just created for a command printing `marker`. */
async function executionShowing(marker) {
  return await until(() => stateDirectories().find(id => captured(id).includes(marker)), `an execution printing ${marker}`)
}

function ok(label) {
  console.log(`PASS ${label}`)
}

async function bootHost() {
  const { Context } = await import(pathToFileURL(hostRequire.resolve('@deepseek-ai/cordis')).href)
  const plugin = await import(pathToFileURL(join(repo, 'packages/host/src/index.ts')).href)
  const ctx = new Context()
  // The deployment mounts the plugin with the profile's config; for an isolated
  // home that resolves to this socket path, and no fallback keeps the seam
  // honest about which executor answered.
  await plugin.apply(ctx, { socketPath: socket, fallback: 'none' })
  const shell = ctx.get('shell')
  assert(shell instanceof plugin.ShelloutExecutor, 'the plugin registered its own shell executor')
  return { ctx, shell }
}

async function main() {
  let executor = startExecutor()
  await until(serving, 'executor ready')
  const host = await bootHost()
  const { shell } = host

  // 1. A normal command: the seam resolves the request, the runner reports its
  //    descriptor defaults, and the captured streams and exit code come back.
  const basic = await shell.execute(shell.resolve({
    command: 'printf "hello\\n"; printf "oops\\n" >&2; exit 3',
    timeoutMs: 20_000,
    workdir: root,
  }))
  const first = await within(basic.result(), 'the first execution settles')
  assert.equal(first.exitCode, 3)
  assert.equal(first.signal, null)
  assert.equal(first.timedOut, false)
  assert.equal(first.stdout.text, 'hello\n')
  assert.equal(first.stderr.text, 'oops\n')
  assert.equal(basic.status, 'completed')
  assert.equal(basic.exitCode, 3)
  // The command ran in the Rust executor's state directory, not in this process.
  const ran = await executionShowing('hello')
  assert.equal(status(ran).state, 'exited')
  assert.equal(status(ran).exitCode, 3)
  ok('output and exit code through the host plugin, executed by the Rust executor')

  // 2. SIGKILL the executor while the command is running. The shim survives and
  //    keeps capturing; the successor adopts it; the reconnecting host learns
  //    the settlement of an execution the handshake still reported as running,
  //    so the real exit code arrives as an event rather than a lost runner.
  const survivor = await shell.execute(shell.resolve({
    command: 'i=0; while [ "$i" -lt 5 ]; do echo marker-$i; i=$((i+1)); sleep 1; done; exit 7',
    timeoutMs: 60_000,
    workdir: root,
  }))
  const survivorId = await executionShowing('marker-0')
  const before = captured(survivorId).length
  await stop(executor, 'SIGKILL')
  await until(() => captured(survivorId).length > before, 'the shim writes while no executor runs')
  executor = startExecutor()
  await until(serving, 'the successor executor serves')
  const second = await within(survivor.result(), 'the surviving execution settles')
  assert.equal(second.exitCode, 7)
  assert.equal(second.signal, null)
  assert.equal(second.timedOut, false)
  assert.equal(survivor.status, 'completed')
  assert.equal(second.stderr.text, '', 'a signal-less recovery must not report a lost runner')
  assert.equal(captured(survivorId), 'marker-0\nmarker-1\nmarker-2\nmarker-3\nmarker-4\n')
  ok('SIGKILL mid-command: the reconnecting host settles with exit 7')

  // 3. A command that finishes while no executor is running. Its settlement was
  //    announced with no connection subscribed, so the facts travel in the
  //    handshake reply the reconnecting host receives.
  const missed = await shell.execute(shell.resolve({
    command: 'echo started; sleep 2; exit 5',
    timeoutMs: 60_000,
    workdir: root,
  }))
  const missedId = await executionShowing('started')
  await stop(executor, 'SIGKILL')
  const finished = await until(() => {
    try {
      const facts = status(missedId)
      return facts.state !== 'running' ? facts : null
    } catch {
      return null
    }
  }, 'the command finishes with no executor running')
  assert.equal(finished.state, 'exited')
  assert.equal(finished.exitCode, 5)
  assert.equal(await serving(), false, 'no executor runs while the command finishes')
  executor = startExecutor()
  await until(serving, 'the recording executor serves')
  const third = await within(missed.result(), 'the missed settlement is recovered')
  assert.equal(third.exitCode, 5)
  assert.equal(third.signal, null)
  assert.equal(third.timedOut, false)
  assert.equal(missed.status, 'completed')
  assert.equal(third.stderr.text, '')
  assert.equal(third.stdout.text, 'started\n')
  ok('an execution that finished with no executor running settles from the handshake facts')

  // Disposing the root fiber runs the plugin's effects, which closes the
  // runner connection; without it the socket keeps this process alive.
  await host.ctx.fiber.dispose()
  await stop(executor, 'SIGTERM')
  console.log('Host plugin against the Rust executor: all scenarios passed')
}

try {
  await main()
} finally {
  // Ask every still-running shim to terminate its group before the executor is
  // stopped; the shim consumes the request from its state directory itself.
  if (existsSync(state)) {
    for (const id of stateDirectories()) {
      try {
        if (status(id).state === 'running') writeFileSync(join(state, id, 'kill.request'), '')
      } catch { /* never admitted */ }
    }
    await sleep(500)
  }
  for (const child of children) await stop(child, 'SIGTERM')
  rmSync(root, { recursive: true, force: true })
}
