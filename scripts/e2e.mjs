/**
 * End-to-end check for DSH-shellout on an isolated harness instance.
 *
 * Creates its own DSH_HOME under the system temp directory, its own runner
 * socket, and profiles that mount this repository's built packages. Nothing
 * here touches a production harness: the home, socket, and profiles are new
 * paths created by this run.
 *
 * Checks:
 *   1. boot         — the runner profile boots and binds its socket
 *   2. basic        — output, stderr and exit code match a local run
 *   3. lag          — a long command does not delay the host event loop
 *   4. background   — a command that leaves a descendant still reports its exit
 *   5. isolation    — the host process has no child in the command's group
 *   6. lease        — after the host dies, the runner terminates the group
 *   7. runner crash — a live execution settles as killed with a provider note
 *
 * Usage: node scripts/e2e.mjs [--keep]
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

const root = mkdtempSync(join(tmpdir(), 'dsh-so-e2e-'))
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

/** One profile directory that resolves harness packages from the shared profile. */
function makeProfile(name, entries) {
  const dir = join(home, 'profiles', name)
  mkdirSync(join(dir, 'node_modules', '@deepseek-ai'), { recursive: true })
  const sharedScope = join(SHARED_PROFILE, 'node_modules', '@deepseek-ai')
  for (const entry of execFileSync('ls', [sharedScope], { encoding: 'utf8' }).trim().split('\n')) {
    try { symlinkSync(join(sharedScope, entry), join(dir, 'node_modules', '@deepseek-ai', entry)) } catch { /* already linked */ }
  }
  for (const [local, target] of [
    ['dsh-shellout', join(REPO, 'packages/host')],
    ['dsh-shellout-runner', join(REPO, 'packages/runner')],
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
  return dir
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

function startRunner() {
  makeProfile('shellout-runner', [
    '- insert:',
    '    - id: shellout-runner',
    "      name: 'dsh-shellout-runner'",
    '      config:',
    `        socketPath: ${socket}`,
    `        cwd: ${root}`,
    '        leaseTtlMs: 4000',
    '        sweepIntervalMs: 250',
  ])
  return spawnProfile('shellout-runner', {})
}

/** The host profile replaces the local shell executor — the production switch in one row. */
function startHost(scenario, extraEnv = {}, options = {}) {
  const hostSocket = options.socketPath ?? socket
  const fallback = options.fallback ?? 'local'
  const retryMs = options.connectRetryMs ?? 10_000
  makeProfile('shellout-host', [
    '- id: bash-sandbox',
    '  disabled: true',
    '- insert:',
    '    - id: shellout',
    "      name: 'dsh-shellout'",
    '      config:',
    `        socketPath: ${hostSocket}`,
    '        mirrorPollMs: 50',
    `        fallback: ${fallback}`,
    `        connectRetryMs: ${String(retryMs)}`,
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

/** Run one scenario to completion. */
async function runScenario(scenario, timeoutMs = 30_000, extraEnv = {}) {
  return await runScenarioWithOptions(scenario, timeoutMs, extraEnv, {})
}

async function runScenarioWithOptions(scenario, timeoutMs, extraEnv, options) {
  const handle = startHost(scenario, extraEnv, options)
  const value = await waitForResult(handle, entry => entry.exitCode !== undefined
    || entry.maxLagMs !== undefined
    || entry.stdout !== undefined
    || entry.satisfied === true, timeoutMs, `scenario ${scenario}`)
  handle.child.kill('SIGKILL')
  return value
}

function alive(pid) {
  try { process.kill(pid, 0); return true } catch { return false }
}

async function main() {
  mkdirSync(join(home, 'profiles'), { recursive: true })
  log('root', root)
  assert(existsSync(join(REPO, 'packages/host/lib/index.js')), 'build', 'built artifacts present')

  const runner = startRunner()
  const deadline = Date.now() + 90_000
  while (!existsSync(socket) && Date.now() < deadline) await new Promise(r => setTimeout(r, 200))
  if (!existsSync(socket)) fail('runner boot', runner.output())
  log('runner boot', { pid: runner.child.pid, socket })

  const basic = await runScenario('basic')
  assert(basic.exitCode === 3, 'basic exit code', basic.exitCode)
  assert(basic.stdout === 'hello\n', 'basic stdout', JSON.stringify(basic.stdout))
  assert(basic.stderr === 'oops\n', 'basic stderr', JSON.stringify(basic.stderr))

  const lag = await runScenario('lag', 40_000)
  assert(lag.maxLagMs < 250, 'host event loop under load', `${String(lag.maxLagMs)} ms max lag, ${String(lag.samples)} samples`)

  const background = await runScenario('background')
  assert(background.stdout.includes('started'), 'background descendant command', JSON.stringify(background.stdout))

  const pidFile = join(root, 'hold.pid')
  const hold = startHost('hold-then-die', { DSH_SHELLOUT_PIDFILE: pidFile })
  const holdInfo = await waitForResult(hold, entry => entry.started === true, 30_000, 'hold-then-die')
  await new Promise(r => setTimeout(r, 800))
  const children = execFileSync('bash', ['-c', `pgrep -P ${String(holdInfo.hostPid)} || true`], { encoding: 'utf8' }).trim()
  assert(children === '', 'host has no children', children === '' ? 'none' : children)
  const commandPid = Number(readFileSync(pidFile, 'utf8').trim())
  assert(alive(commandPid), 'surviving descendant alive before the lease expires', `pid ${String(commandPid)}`)
  await new Promise(r => setTimeout(r, 7_000))
  assert(!alive(commandPid), 'lease expiry terminated the group', `pid ${String(commandPid)} alive=${String(alive(commandPid))}`)

  // Fallback: with no runner listening, the host must still run commands.
  const fallback = await runScenarioWithOptions('basic', 60_000, {}, {
    socketPath: join(root, 'missing.sock'), fallback: 'local', connectRetryMs: 3_000,
  })
  assert(fallback.exitCode === 3 && fallback.stdout === 'hello\n', 'fallback runs commands locally when the runner is absent',
    `exit=${String(fallback.exitCode)} stdout=${JSON.stringify(fallback.stdout)}`)

  const crash = startHost('runner-crash')
  await waitForResult(crash, entry => entry.started === true, 30_000, 'runner-crash start')
  await new Promise(r => setTimeout(r, 500))
  runner.child.kill('SIGKILL')
  const settled = await waitForResult(crash, entry => entry.satisfied === true, 30_000, 'runner-crash settle')
  assert(settled.status === 'killed', 'runner loss settles as killed', settled.status)
  assert(String(settled.stderr ?? '').includes('runner lost'), 'runner loss carries the provider note', settled.stderr)
  crash.child.kill('SIGKILL')

  cleanup(0)
}

main().catch(error => { log('error', error instanceof Error ? error.message : String(error)); cleanup(1) })
