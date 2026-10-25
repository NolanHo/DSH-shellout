/**
 * Start an isolated web instance for agent-level checks.
 *
 * Copies the metadata of a source profile into a temporary DSH_HOME (the
 * production profile's `node_modules` is shared read-only), rewrites every
 * absolute storage path into that home, mounts this repository's shell client
 * in place of the local executor, and binds a private port.
 *
 * Nothing here writes to the source profile or its home: the copy is a new
 * directory, and the only shared files are read.
 *
 * Usage:
 *   node scripts/isolated-web.mjs [--port 3107] [--source /root/.dsh/profiles/web]
 *                                 [--harness /root/dsh-web/app] [--credentials <file>]
 */

import { spawn } from 'node:child_process'
import { chmodSync, cpSync, existsSync, mkdirSync, mkdtempSync, openSync, readFileSync, symlinkSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { request } from 'node:http'

const HERE = dirname(fileURLToPath(import.meta.url))
const REPO = resolve(HERE, '..')
const args = process.argv.slice(2)
const arg = (name, fallback) => {
  const index = args.indexOf(name)
  return index >= 0 && args[index + 1] !== undefined ? args[index + 1] : fallback
}

const port = Number(arg('--port', '3107'))
const source = arg('--source', '/root/.dsh/profiles/web')
const harness = arg('--harness', '/root/dsh-web/app')
const credentials = arg('--credentials', '/root/.dsh/.credentials.yaml')
const profileName = arg('--profile', 'shellout-web')
const runnerProfile = arg('--runner-profile', 'shellout-runner')
const reusePatch = args.includes('--reuse-patch')
const socketOverride = arg('--socket', '')

const root = mkdtempSync(join(tmpdir(), 'dsh-so-web-'))
const home = join(root, 'home')
const profile = join(home, 'profiles', profileName)

// 1. Profile metadata only; node_modules is shared read-only with the source profile.
mkdirSync(profile, { recursive: true })
for (const file of ['package.json', 'cordis.yml', 'cordis.patch.yml']) {
  cpSync(join(source, file), join(profile, file))
}
symlinkSync(join(source, 'node_modules'), join(profile, 'node_modules'))

// 2. Local packages resolve from the isolated home's own node_modules.
mkdirSync(join(home, 'node_modules'), { recursive: true })
for (const [name, target] of [
  ['dsh-shellout', join(REPO, 'packages/host')],
  ['dsh-shellout-runner', join(REPO, 'packages/runner')],
  ['dsh-shellout-protocol', join(REPO, 'packages/protocol')],
]) {
  symlinkSync(target, join(home, 'node_modules', name))
}

// 3. Rewrite absolute storage paths so the copy can never open the source home.
const patchPath = join(profile, 'cordis.patch.yml')
let patch = readFileSync(patchPath, 'utf8')
patch = patch
  .replace(/\/root\/\.dsh\/sessions-jsonl/g, join(home, 'sessions-jsonl'))
  .replace(/\/root\/\.dsh\/metadata-index\.sqlite/g, join(home, 'metadata-index.sqlite'))
  .replace(/\/root\/\.dsh\/session-query\.sqlite/g, join(home, 'session-query.sqlite'))
  .replace(/\/root\/\.dsh\/sessions\.sqlite/g, join(home, 'sessions.sqlite'))
// 4. Optionally reuse the copied patch as-is (dry run of an already-configured
//    profile), else replace the local executor with the runner client.
if (reusePatch) {
  writeFileSync(patchPath, patch)
  mkdirSync(join(home, 'profiles'), { recursive: true })
  if (existsSync(credentials)) {
    cpSync(credentials, join(home, '.credentials.yaml'))
    chmodSync(join(home, '.credentials.yaml'), 0o600)
  }
}
if (!reusePatch) patch += [
  '',
  '- id: bash-sandbox',
  '  disabled: true',
  '',
  '- insert:',
  '    - id: shellout',
  "      name: 'dsh-shellout'",
  '      config:',
  `        socketPath: ${socketOverride === '' ? join(root, 'control.sock') : socketOverride}`,
  '        mirrorPollMs: 50',
  '',
].join('\n')
if (!reusePatch) writeFileSync(patchPath, patch)

// 5. Credentials: a private copy inside the isolated home.
if (existsSync(credentials)) {
  cpSync(credentials, join(home, '.credentials.yaml'))
  chmodSync(join(home, '.credentials.yaml'), 0o600)
}

// 6. Bind overlay: a private port, loopback only.
const bindPath = join(root, 'bind.yml')
writeFileSync(bindPath, [
  '- id: webserver',
  '  config:',
  "    host: '127.0.0.1'",
  `    port: ${String(port)}`,
  '    compression: gzip',
  '    compressionLevel: 3',
  '',
].join('\n'))

// 7. Launch, mirroring the deployment's launch environment where present.
const env = { ...process.env, DSH_HOME: home, DSH_TELEMETRY_DISABLED: '1' }
const proxyEnv = '/root/dsh-web/llm-proxy.env'
if (existsSync(proxyEnv)) {
  for (const line of readFileSync(proxyEnv, 'utf8').split('\n')) {
    const match = /^export\s+([A-Z0-9_]+)=(.*)$/.exec(line.trim())
    if (match?.[1] !== undefined && match[2] !== undefined) {
      env[match[1]] = match[2].trim().replace(/^['"]|['"]$/g, '')
    }
  }
  if (env.DSH_LLM_PROXY !== undefined && existsSync('/root/dsh-web/proxy-fetch.mjs')) {
    env.NODE_OPTIONS = `${env.NODE_OPTIONS ?? ''} --import /root/dsh-web/proxy-fetch.mjs`
  }
}
// Runner profile in the same home: a local executor plus this repo's service.
const runnerDir = join(home, 'profiles', runnerProfile)
mkdirSync(runnerDir, { recursive: true })
symlinkSync(join(source, 'node_modules'), join(runnerDir, 'node_modules'))
writeFileSync(join(runnerDir, 'package.json'), JSON.stringify({
  name: `dsh-profile-${runnerProfile}`,
  private: true,
  type: 'module',
  dsh: { profile: { bundles: ['@deepseek-ai/dsh-base'] } },
}, null, 2))
writeFileSync(join(runnerDir, 'cordis.yml'), '[]\n')
writeFileSync(join(runnerDir, 'cordis.patch.yml'), [
  '- insert:',
  '    - id: subprocess',
  "      name: '@deepseek-ai/dsh-subprocess-local'",
  '    - id: shellout-runner',
  "      name: 'dsh-shellout-runner'",
  '      config:',
  `        socketPath: ${join(root, 'control.sock')}`,
  `        cwd: ${root}`,
  '        leaseTtlMs: 60000',
  '',
].join('\n'))

const runner = spawn(process.execPath, ['--import', 'tsx/esm', 'apps/cli/src/bin.ts', '--profile', runnerProfile], {
  cwd: harness,
  env: { ...process.env, DSH_HOME: home, DSH_TELEMETRY_DISABLED: '1' },
  stdio: ['ignore', 'pipe', 'pipe'],
})
runner.stderr.on('data', (chunk) => { process.stdout.write(`runner: ${String(chunk)}`) })
const socketDeadline = Date.now() + 90_000
while (!existsSync(join(root, 'control.sock')) && Date.now() < socketDeadline) await new Promise(r => setTimeout(r, 200))
if (!existsSync(join(root, 'control.sock'))) {
  process.stdout.write('runner did not bind its socket\n')
  runner.kill('SIGKILL')
  process.exit(1)
}
process.stdout.write(`runner ready pid ${String(runner.pid)}\n`)

const child = spawn(process.execPath, ['--import', 'tsx/esm', 'apps/cli/src/bin.ts', '--profile', profileName, '--patch', bindPath, '--no-open'], {
  cwd: harness,
  env,
  stdio: [openSync('/dev/zero', 'r'), 'pipe', 'pipe'],
})
let output = ''
child.stdout.on('data', (chunk) => { output += String(chunk) })
child.stderr.on('data', (chunk) => { output += String(chunk) })

writeFileSync(join(root, 'instance.json'), JSON.stringify({ root, home, port, pid: child.pid, runnerPid: runner.pid, harness }, null, 2))
process.stdout.write(`home: ${home}\nport: ${String(port)}\npid: ${String(child.pid)}\nsocket: ${join(root, 'control.sock')}\n`)

const deadline = Date.now() + 180_000
while (Date.now() < deadline) {
  if (child.exitCode !== null) {
    process.stdout.write(`instance exited early:\n${output}\n`)
    process.exit(1)
  }
  if (await probe(port)) {
    process.stdout.write(`READY http://127.0.0.1:${String(port)}/\n`)
    break
  }
  await new Promise(r => setTimeout(r, 500))
}
process.on('SIGTERM', () => { child.kill('SIGTERM'); runner.kill('SIGTERM') })
child.on('exit', (code, signal) => {
  process.stdout.write(`instance exit ${String(code)} ${String(signal)}\n${output}\n`)
  runner.kill('SIGTERM')
  process.exit(0)
})

function probe(target) {
  return new Promise((resolveProbe) => {
    const req = request({ host: '127.0.0.1', port: target, path: '/', timeout: 1_000 }, (res) => {
      res.resume()
      resolveProbe(true)
    })
    req.on('error', () => { resolveProbe(false) })
    req.on('timeout', () => { req.destroy(); resolveProbe(false) })
    req.end()
  })
}
