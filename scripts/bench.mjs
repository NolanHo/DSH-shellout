/**
 * Measure what the socket layer costs relative to the work itself.
 *
 * Boots a runner in a temporary DSH_HOME, then measures, for the same trivial
 * command:
 *   - sequential round trips through the runner (exec → settle)
 *   - a bare local `bash -c true` spawn for reference
 *
 * The point is the ratio: if the protocol were the bottleneck, a faster runner
 * implementation would matter. If process creation dominates, it would not.
 *
 * Usage: node scripts/bench.mjs [--count 50]
 */

import { execFileSync, spawn } from 'node:child_process'
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { performance } from 'node:perf_hooks'
import { ShelloutClient, RemoteExecution } from '../packages/host/lib/core/index.js'
import { specDigest } from '../packages/protocol/lib/index.js'

const HERE = dirname(fileURLToPath(import.meta.url))
const REPO = resolve(HERE, '..')
const HARNESS = process.env.DSH_HARNESS_CLONE ?? '/root/dsh-web/app'
const SHARED_PROFILE = process.env.DSH_SHARED_PROFILE ?? '/root/.dsh/profiles/web'
const countIndex = process.argv.indexOf('--count')
const count = countIndex >= 0 ? Number(process.argv[countIndex + 1]) : 50

const root = mkdtempSync(join(tmpdir(), 'dsh-so-bench-'))
const home = join(root, 'home')
const socketPath = join(root, 'control.sock')

function makeRunner() {
  const dir = join(home, 'profiles', 'shellout-runner')
  mkdirSync(join(dir, 'node_modules'), { recursive: true })
  for (const [name, target] of [
    ['dsh-shellout-runner', join(REPO, 'packages/runner')],
    ['dsh-shellout-protocol', join(REPO, 'packages/protocol')],
  ]) symlinkSync(target, join(dir, 'node_modules', name))
  writeFileSync(join(dir, 'package.json'), JSON.stringify({
    name: 'dsh-profile-shellout-runner', private: true, type: 'module',
    dsh: { profile: { bundles: [] } },
  }, null, 2))
  writeFileSync(join(dir, 'cordis.yml'), '[]\n')
  writeFileSync(join(dir, 'cordis.patch.yml'), [
    '- insert:',
    '    - id: subprocess',
    "      name: '@deepseek-ai/dsh-subprocess-local'",
    '    - id: bash-sandbox',
    "      name: '@deepseek-ai/dsh-bash-local'",
    '    - id: shellout-runner',
    "      name: 'dsh-shellout-runner'",
    '      config:',
    `        socketPath: ${socketPath}`,
    `        cwd: ${root}`,
  ].join('\n') + '\n')
}

const percentile = (values, p) => {
  const sorted = [...values].sort((a, b) => a - b)
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))] ?? 0
}

const runner = { child: undefined }
makeRunner()
runner.child = spawn(process.execPath, ['--import', 'tsx/esm', 'apps/cli/src/bin.ts', '--profile', 'shellout-runner'], {
  cwd: HARNESS,
  env: { ...process.env, DSH_HOME: home, DSH_TELEMETRY_DISABLED: '1' },
  stdio: ['ignore', 'ignore', 'ignore'],
})

const deadline = Date.now() + 90_000
while (!execFileSync('bash', ['-c', `test -S ${socketPath} && echo yes || echo no`], { encoding: 'utf8' }).includes('yes')) {
  if (Date.now() > deadline) { console.error('runner did not start'); process.exit(1) }
  await new Promise(r => setTimeout(r, 200))
}

const client = await ShelloutClient.connect({ socketPath, bootId: 'bench' })
const spec = {
  command: 'true', workdir: root, timeoutMs: 60_000, onExpiry: 'kill', stdoutMaxBytes: 1_048_576,
}
const digest = specDigest(spec)

const throughRunner = []
for (let index = 0; index < count; index += 1) {
  const executionId = `bench-${String(index)}`
  const execution = new RemoteExecution(client, executionId, { pollMs: 10 })
  client.onSettled(event => { if (event.executionId === executionId) execution.receive(event) })
  const started = performance.now()
  await client.request('exec', { operationId: `op-${String(index)}`, executionId, spec, specDigest: digest })
  execution.start()
  await execution.done
  throughRunner.push(performance.now() - started)
}

const local = []
for (let index = 0; index < count; index += 1) {
  const started = performance.now()
  await new Promise((resolveSpawn) => {
    const child = spawn('bash', ['-c', 'true'], { stdio: 'ignore' })
    child.on('exit', () => { resolveSpawn() })
  })
  local.push(performance.now() - started)
}

const summarize = (values) => ({ p50: +percentile(values, 50).toFixed(2), p95: +percentile(values, 95).toFixed(2), max: +Math.max(...values).toFixed(2) })
console.log(JSON.stringify({
  count,
  throughRunnerMs: summarize(throughRunner),
  localSpawnMs: summarize(local),
  ratioP50: +(percentile(throughRunner, 50) / percentile(local, 50)).toFixed(2),
}, null, 2))

client.close()
runner.child.kill('SIGTERM')
rmSync(root, { recursive: true, force: true })
