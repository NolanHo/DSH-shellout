/**
 * Scenario 4: a host reboot loses the processes, not the truth.
 *
 * A command runs on the host; the host is restarted before it finishes, so its
 * shim and command are gone. A later session must reconcile the row to `lost`
 * rather than leaving it `running` forever — the same rule the local plane
 * applies after a reboot.
 *
 * Run with the same environment as `scripts/e2e-remote-ssh.mjs`, plus
 * `DSH_CONTAINER` naming the throwaway container to restart:
 *
 *   DSH_SSH_HOST=root@127.0.0.1 DSH_SSH_PORT=2327 DSH_SSH_KEY=/tmp/key/id \
 *   DSH_CONTAINER=dsh-ssh-iso node --import tsx/esm scripts/e2e-remote-reboot.mjs
 */
import assert from 'node:assert/strict'
import { execFileSync, spawn } from 'node:child_process'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const repo = process.env.DSH_REPO ?? '/root/code/DSH-shellout/.worktrees/v2'
const binary = resolve(repo, 'runner-rs/target/x86_64-unknown-linux-musl/release/dsh-shellout-runner')
const host = process.env.DSH_SSH_HOST ?? 'root@127.0.0.1'
const port = process.env.DSH_SSH_PORT ?? '2327'
const key = process.env.DSH_SSH_KEY
const container = process.env.DSH_CONTAINER
assert(key, 'set DSH_SSH_KEY')
assert(container, 'set DSH_CONTAINER to the throwaway container name')

const sshCli = ['-i', key, '-p', port, '-o', 'BatchMode=yes', '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null', host]
const sshArgs = ['--host', host, '--ssh', 'ssh', '--ssh', '-i', '--ssh', key, '--ssh', '-p', '--ssh', port,
  '--ssh', '-o', '--ssh', 'BatchMode=yes', '--ssh', '-o', '--ssh', 'StrictHostKeyChecking=no',
  '--ssh', '-o', '--ssh', 'UserKnownHostsFile=/dev/null']

function attach() {
  const child = spawn(binary, ['--mode', 'attach', ...sshArgs, '--remote-root', '/root/.dsh-shellout'], { stdio: ['pipe', 'pipe', 'inherit'] })
  let buffer = Buffer.alloc(0)
  const waiters = []
  const queued = []
  child.stdout.on('data', (chunk) => {
    buffer = Buffer.concat([buffer, chunk])
    while (buffer.length >= 4) {
      const length = buffer.readUInt32BE(0)
      if (buffer.length < 4 + length) return
      const frame = JSON.parse(buffer.subarray(4, 4 + length).toString())
      buffer = buffer.subarray(4 + length)
      const waiter = waiters.shift()
      if (waiter === undefined) queued.push(frame)
      else waiter(frame)
    }
  })
  let nextId = 1
  const read = () => {
    const ready = queued.shift()
    if (ready !== undefined) return Promise.resolve(ready)
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('no frame')), 120_000)
      waiters.push((frame) => { clearTimeout(timer); resolve(frame) })
    })
  }
  const session = {
    child,
    send(frame) {
      const body = Buffer.from(JSON.stringify(frame))
      const header = Buffer.alloc(4)
      header.writeUInt32BE(body.length)
      child.stdin.write(Buffer.concat([header, body]))
    },
    async request(method, payload) {
      const id = nextId++
      session.send({ id, kind: 'req', method, payload })
      for (;;) {
        const frame = await read()
        if (frame.kind === 'evt') continue
        assert.equal(frame.kind, 'res', `${method}: ${JSON.stringify(frame)}`)
        return frame.payload
      }
    },
    close() { child.kill('SIGTERM') },
  }
  return session
}

const run = Date.now()
const executionId = `e2e-reboot-${run}`
const spec = {
  argv: ['/bin/sh', '-c', 'sleep 600'],
  cwd: '/root',
  env: { PATH: '/usr/local/bin:/usr/bin:/bin' },
  timeoutMs: 900_000,
  onExpiry: 'kill',
  graceMs: 500,
  capture: { stdoutMaxBytes: 4096, stderrMaxBytes: 4096, spillMaxBytes: 0, spillDir: '/tmp' },
}

const first = attach()
await first.request('hello', { protocol: '2.0', clientBuild: 'e2e-reboot', bootId: `boot-reboot-${run}` })
await first.request('exec', { operationId: `op-${executionId}`, executionId, specDigest: 'digest', spec })
await new Promise((resolve) => setTimeout(resolve, 1000))
first.close()
await new Promise((resolve) => setTimeout(resolve, 500))

const pid = execFileSync('ssh', [...sshCli, `cat /root/.dsh-shellout/state/${executionId}/status.json`], { encoding: 'utf8' })
assert.match(pid, /"state":"running"/, `the command is running before the reboot: ${pid}`)
console.log('PASS scenario 4a: the command is running on the host before the reboot')

execFileSync('docker', ['restart', container], { encoding: 'utf8' })
for (let i = 0; i < 60; i += 1) {
  try {
    execFileSync('ssh', [...sshCli, 'true'], { stdio: 'ignore' })
    break
  } catch {
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
}
console.log('PASS scenario 4b: the host restarted, taking every process with it')

const observer = attach()
await observer.request('hello', { protocol: '2.0', clientBuild: 'e2e-reboot-after', bootId: `boot-reboot-after-${run}` })
let row
for (let i = 0; i < 40; i += 1) {
  const history = await observer.request('history', { limit: 200 })
  row = history.rows.find((candidate) => candidate.id === executionId)
  if (row?.state === 'lost') break
  await new Promise((resolve) => setTimeout(resolve, 500))
}
observer.close()
assert.equal(row?.state, 'lost', `the rebooted row reconciles to lost: ${JSON.stringify(row)}`)
console.log('PASS scenario 4c: the rebooted row reconciles to lost')
