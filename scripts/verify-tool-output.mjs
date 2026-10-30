/**
 * Verify the tool-facing output of the host plugin against the bash tool's
 * declared schema, without restarting any service.
 *
 * The failure this guards against is invisible to the shell seam: the seam
 * accepts `{ text, truncated, spillPath? }`, but the bash tool's result schema
 * declares `additionalProperties: false` and `spillPath` as a string, so a
 * `null` there makes every call fail with "no oneOf branch matched". This
 * script boots the real executor binary in a temp state directory, mounts the
 * host plugin from this worktree, runs commands through the seam, and validates
 * the projected tool value with the same rules the harness enforces.
 *
 * Usage: node --import tsx/esm scripts/verify-tool-output.mjs [--binary <path>]
 */

import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs'
import { createRequire } from 'node:module'
import net from 'node:net'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '..')
const index = process.argv.indexOf('--binary')
const binary = resolve(repo, index < 0 ? 'runner-rs/target/release/dsh-shellout-runner' : process.argv[index + 1])
const temp = join(repo, '.task-tmp')
mkdirSync(temp, { recursive: true })
const root = mkdtempSync(join(temp, 'verify-tool-'))
const state = join(root, 'state')
const socket = join(root, 'control.sock')
const history = join(root, 'history.sqlite')
const spill = join(root, 'spill')
mkdirSync(spill, { recursive: true })
const hostRequire = createRequire(join(repo, 'packages/host/package.json'))
const sleep = ms => new Promise(r => setTimeout(r, ms))

let executor
let failures = 0

const FOREGROUND_KEYS = new Set([
  'kind', 'cwd', 'exitCode', 'signal', 'timedOut', 'aborted', 'stopped', 'timeoutMs',
  'expect', 'stdout', 'stderr', 'sandbox',
])
const STREAM_KEYS = new Set(['text', 'truncated', 'spillPath'])

/** Report one schema violation, or nothing when the value conforms. */
function violations(value, label) {
  const found = []
  const isObject = candidate => typeof candidate === 'object' && candidate !== null && !Array.isArray(candidate)
  if (!isObject(value)) return [`${label}: not an object`]
  for (const key of Object.keys(value)) {
    if (!FOREGROUND_KEYS.has(key)) found.push(`${label}.${key}: unexpected property`)
  }
  if (value.kind !== 'foreground') found.push(`${label}.kind: expected "foreground"`)
  if (typeof value.cwd !== 'string') found.push(`${label}.cwd: expected string`)
  if (!(Number.isInteger(value.exitCode) || value.exitCode === null)) found.push(`${label}.exitCode: expected integer or null`)
  if (!(typeof value.signal === 'string' || value.signal === null)) found.push(`${label}.signal: expected string or null`)
  if (typeof value.timedOut !== 'boolean') found.push(`${label}.timedOut: expected boolean`)
  if (typeof value.aborted !== 'boolean') found.push(`${label}.aborted: expected boolean`)
  if (typeof value.timeoutMs !== 'number') found.push(`${label}.timeoutMs: expected number`)
  for (const stream of ['stdout', 'stderr']) {
    const candidate = value[stream]
    if (!isObject(candidate)) { found.push(`${label}.${stream}: not an object`); continue }
    for (const key of Object.keys(candidate)) {
      if (!STREAM_KEYS.has(key)) found.push(`${label}.${stream}.${key}: unexpected property`)
    }
    if (typeof candidate.text !== 'string') found.push(`${label}.${stream}.text: expected string, got ${JSON.stringify(candidate.text)}`)
    if (typeof candidate.truncated !== 'boolean') found.push(`${label}.${stream}.truncated: expected boolean, got ${JSON.stringify(candidate.truncated)}`)
    if (candidate.spillPath !== undefined && typeof candidate.spillPath !== 'string') {
      found.push(`${label}.${stream}.spillPath: expected string, got ${JSON.stringify(candidate.spillPath)}`)
    }
  }
  return found
}

/** The value tool-bash builds from one ShellRunResult. */
function projectToolOutput(result, workdir) {
  const stream = value => ({
    text: value.text,
    truncated: value.truncated,
    ...value.spillPath === undefined ? {} : { spillPath: value.spillPath },
  })
  return {
    kind: 'foreground',
    cwd: workdir,
    exitCode: result.exitCode,
    signal: result.signal,
    timedOut: result.timedOut,
    aborted: result.aborted,
    timeoutMs: result.timeoutMs,
    stdout: stream(result.stdout),
    stderr: stream(result.stderr),
  }
}

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

async function until(fn, label, timeout = 20_000) {
  const end = Date.now() + timeout
  while (Date.now() < end) {
    const value = await fn()
    if (value) return value
    await sleep(50)
  }
  throw new Error(`timeout: ${label}`)
}

function startExecutor() {
  executor = spawn(binary, ['--mode', 'executor', '--state', state, '--history', history, '--socket', socket], {
    cwd: root,
    env: { ...process.env, DSH_HOME: join(root, 'home') },
    stdio: ['ignore', 'ignore', 'pipe'],
  })
  executor.stderr.on('data', () => {})
  return executor
}

async function check(label, command, options = {}) {
  const { shell } = await bootHost()
  const spec = shell.resolve({
    command,
    workdir: root,
    timeoutMs: 30_000,
    ...options.resolve,
  })
  const execution = await shell.execute(spec)
  const result = await execution.result()
  const value = projectToolOutput(result, spec.workdir)
  const found = violations(value, label)
  if (found.length === 0) {
    console.log(`PASS ${label}: exit=${value.exitCode} stdout=${JSON.stringify(value.stdout.text.slice(0, 24))} spill=${JSON.stringify(value.stdout.spillPath ?? null)}`)
  } else {
    failures += 1
    console.log(`FAIL ${label}: ${found.join('; ')}`)
  }
  return value
}

let cached
async function bootHost() {
  if (cached !== undefined) return cached
  const { Context } = await import(pathToFileURL(hostRequire.resolve('@deepseek-ai/cordis')).href)
  const plugin = await import(pathToFileURL(join(repo, 'packages/host/src/index.ts')).href)
  const ctx = new Context()
  await plugin.apply(ctx, { socketPath: socket, fallback: 'none' })
  cached = { ctx, shell: ctx.get('shell') }
  return cached
}

async function main() {
  if (!existsSync(binary)) throw new Error(`missing binary: ${binary}`)
  startExecutor()
  await until(serving, 'executor ready')

  await check('plain output', 'printf "hello\\n"')
  await check('stderr and non-zero exit', 'printf "oops\\n" >&2; exit 3')
  await check('empty output', 'true')
  // A stream larger than the in-memory cap is the case that produces a spill,
  // and the one where a null spill path used to break the tool schema.
  await check('large output with a spill', 'head -c 200000 /dev/zero | tr "\\0" "x"', {
    resolve: { stdoutMaxBytes: 4096 },
  })
  await check('kill escalation', 'trap "" TERM; sleep 5', { resolve: { timeoutMs: 1_000 } })

  console.log(failures === 0 ? 'tool-facing output conforms for every case' : `${failures} case(s) violate the tool schema`)
}

try {
  await main()
} finally {
  // The host plugin's client keeps heartbeat and reconnect timers alive, so the
  // context must be disposed before the process can end on its own.
  if (cached?.ctx?.dispose !== undefined) await cached.ctx.dispose()
  if (executor !== undefined && executor.exitCode === null) executor.kill('SIGTERM')
  await sleep(200)
  rmSync(root, { recursive: true, force: true })
}
process.exit(failures === 0 ? 0 : 1)
