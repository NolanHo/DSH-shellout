/**
 * End-to-end check for the Rust runner on an isolated harness instance.
 *
 * Same host scenarios as `scripts/e2e.mjs`, but the execution plane is the
 * Rust binary instead of the Node runner. Creates its own DSH_HOME and socket
 * under the system temp directory; nothing touches a running harness.
 *
 * Usage: node scripts/e2e-rust.mjs [--binary target/debug/dsh-shellout-runner] [--keep]
 */

import { execFileSync, spawn } from 'node:child_process'
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const HERE = dirname(fileURLToPath(import.meta.url))
const REPO = resolve(HERE, '..')
const HARNESS = process.env.DSH_HARNESS_CLONE ?? '/root/dsh-web/app'
const SHARED_PROFILE = process.env.DSH_SHARED_PROFILE ?? '/root/.dsh/profiles/web'
const keep = process.argv.includes('--keep')
const binaryIndex = process.argv.indexOf('--binary')
const binary = resolve(REPO, binaryIndex >= 0 ? process.argv[binaryIndex + 1] : 'runner-rs/target/debug/dsh-shellout-runner')

const root = mkdtempSync(join(tmpdir(), 'dsh-so-rust-'))
const home = join(root, 'home')
const socket = join(root, 'control.sock')
const RESULT = 'SHELLOUT-RESULT '

function log(step, detail) {
  process.stdout.write(`${step}: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}\n`)
}

function fail(step, detail) {
  log(`${step} FAILED`, detail)
  cleanup(1)
}

function assert(condition, step, detail) {
  if (!condition) fail(step, detail)
  log(`${step} ok`, detail)
}

function cleanup(code) {
  if (!keep) rmSync(root, { recursive: true, force: true })
  else process.stdout.write(`kept: ${root}\n`)
  process.exit(code)
}

function makeProfile(name, entries) {
  const dir = join(home, 'profiles', name)
  mkdirSync(join(dir, 'node_modules'), { recursive: true })
  for (const [local, target] of [
    ['dsh-shellout', join(REPO, 'packages/host')],
    ['dsh-shellout-protocol', join(REPO, 'packages/protocol')],
    ['dsh-shellout-testapp', join(REPO, 'packages/testapp')],
  ]) {
    try { symlinkSync(target, join(dir, 'node_modules', local)) } catch { /* already linked */ }
  }
  writeFileSync(join(dir, 'package.json'), JSON.stringify({
    name: `dsh-profile-${name}`,
    private: true,
    type: 'module',
    dsh: { profile: { bundles: ['@deepseek-ai/dsh-base'] } },
  }, null, 2))
  writeFileSync(join(dir, 'cordis.yml'), '[]\n')
  writeFileSync(join(dir, 'cordis.patch.yml'), `${entries.join('\n')}\n`)
}

function spawnProfile(profile, env) {
  const child = spawn(process.execPath, ['--import', 'tsx/esm', 'apps/cli/src/bin.ts', '--profile', profile], {
    cwd: HARNESS,
    env: { ...process.env, DSH_HOME: home, DSH_TELEMETRY_DISABLED: '1', ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let output = ''
  child.stdout.on('data', (chunk) => { output += String(chunk) })
  child.stderr.on('data', (chunk) => { output += String(chunk) })
  return {
    child,
    output: () => output,
    results: () => output.split('\n')
      .filter(entry => entry.includes(RESULT))
      .map((entry) => JSON.parse(entry.slice(entry.indexOf(RESULT) + RESULT.length))),
  }
}

function startHost(scenario, extraEnv = {}, options = {}) {
  const hostSocket = options.socketPath ?? socket
  makeProfile('shellout-host', [
    '- id: bash-sandbox',
    '  disabled: true',
    '- insert:',
    '    - id: shellout',
    "      name: 'dsh-shellout'",
    '      config:',
    `        socketPath: ${hostSocket}`,
    '        mirrorPollMs: 50',
    '        connectRetryMs: 3000',
    '        disconnectGraceMs: 2500',
    '    - id: shellout-testapp',
    "      name: 'dsh-shellout-testapp'",
  ])
  return spawnProfile('shellout-host', { DSH_SHELLOUT_SCENARIO: scenario, ...extraEnv })
}

async function waitForResult(handle, predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const match = handle.results().find(predicate)
    if (match !== undefined) return match
    if (handle.child.exitCode !== null) break
    await new Promise(r => setTimeout(r, 50))
  }
  fail(label, `no matching result\n${handle.output()}`)
}

async function runScenario(scenario, timeoutMs = 30_000, extraEnv = {}) {
  const handle = startHost(scenario, extraEnv)
  const value = await waitForResult(handle, entry => entry.exitCode !== undefined
    || entry.maxLagMs !== undefined
    || entry.stdout !== undefined, timeoutMs, `scenario ${scenario}`)
  handle.child.kill('SIGKILL')
  return value
}

function alive(pid) {
  try { process.kill(pid, 0); return true } catch { return false }
}

async function main() {
  mkdirSync(join(home, 'profiles'), { recursive: true })
  log('root', root)
  assert(existsSync(binary), 'rust binary present', binary)

  const runner = spawn(binary, ['--socket', socket, '--cwd', root, '--lease-ttl-ms', '4000'], { stdio: ['ignore', 'pipe', 'pipe'] })
  let runnerOutput = ''
  runner.stdout.on('data', (chunk) => { runnerOutput += String(chunk) })
  runner.stderr.on('data', (chunk) => { runnerOutput += String(chunk) })
  const deadline = Date.now() + 30_000
  while (!existsSync(socket) && Date.now() < deadline) await new Promise(r => setTimeout(r, 100))
  if (!existsSync(socket)) fail('rust runner boot', runnerOutput)
  log('rust runner boot', { pid: runner.pid, socket })

  const basic = await runScenario('basic')
  assert(basic.exitCode === 3, 'basic exit code (rust)', basic.exitCode)
  assert(basic.stdout === 'hello\n', 'basic stdout (rust)', JSON.stringify(basic.stdout))
  assert(basic.stderr === 'oops\n', 'basic stderr (rust)', JSON.stringify(basic.stderr))

  const lag = await runScenario('lag', 40_000)
  assert(lag.maxLagMs < 250, 'host event loop under load (rust)', `${String(lag.maxLagMs)} ms max lag`)

  const background = await runScenario('background')
  assert(background.stdout.includes('started'), 'background descendant command (rust)', JSON.stringify(background.stdout))

  const pidFile = join(root, 'hold.pid')
  const hold = startHost('hold-then-die', { DSH_SHELLOUT_PIDFILE: pidFile })
  const holdInfo = await waitForResult(hold, entry => entry.started === true, 30_000, 'hold-then-die (rust)')
  await new Promise(r => setTimeout(r, 800))
  const children = execFileSync('bash', ['-c', `pgrep -P ${String(holdInfo.hostPid)} || true`], { encoding: 'utf8' }).trim()
  assert(children === '', 'host has no children (rust)', children === '' ? 'none' : children)
  const commandPid = Number(readFileSync(pidFile, 'utf8').trim())
  assert(alive(commandPid), 'descendant alive before lease expiry (rust)', `pid ${String(commandPid)}`)
  await new Promise(r => setTimeout(r, 7_000))
  assert(!alive(commandPid), 'lease expiry terminated the group (rust)', `pid ${String(commandPid)} alive=${String(alive(commandPid))}`)

  runner.kill('SIGTERM')
  cleanup(0)
}

main().catch(error => { log('error', error instanceof Error ? error.message : String(error)); cleanup(1) })
