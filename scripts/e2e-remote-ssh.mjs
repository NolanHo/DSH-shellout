/**
 * End-to-end over a real ssh hop.
 *
 * Everything else in this repository drives the attach path through
 * `scripts/fake-remote.sh`, which shares the machine with the test. This script
 * speaks to a real sshd so the transport itself — option handling, key
 * authentication, the remote shell that parses the command line — is exercised.
 *
 * It runs against whatever host the environment names, so it stays usable for a
 * throwaway container as well as a real machine. A container with a musl user
 * space is the interesting case: it is exactly where a glibc controller binary
 * fails, and where the static artifact must simply work.
 *
 *   DSH_SSH_HOST=root@127.0.0.1 DSH_SSH_PORT=2322 DSH_SSH_KEY=/tmp/key/id \
 *     node --import tsx/esm scripts/e2e-remote-ssh.mjs
 *
 * Example host (throwaway): run `alpine:latest` with openssh installed, the
 * public key in /root/.ssh/authorized_keys, and port 22 published to 2322.
 */
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const repo = resolve(here, '..')
const binary = resolve(repo, 'runner-rs/target/x86_64-unknown-linux-musl/release/dsh-shellout-runner')
const host = process.env.DSH_SSH_HOST ?? 'root@127.0.0.1'
const port = process.env.DSH_SSH_PORT ?? '2322'
const key = process.env.DSH_SSH_KEY
assert(key, 'set DSH_SSH_KEY to the private key for the test host')

const sshArgs = [
  '--host', host,
  '--ssh', 'ssh',
  '--ssh', '-i', '--ssh', key,
  '--ssh', '-p', '--ssh', port,
  '--ssh', '-o', '--ssh', 'BatchMode=yes',
  '--ssh', '-o', '--ssh', 'StrictHostKeyChecking=no',
  '--ssh', '-o', '--ssh', 'UserKnownHostsFile=/dev/null',
  '--ssh', '-o', '--ssh', 'ConnectTimeout=8',
]

const child = spawn(binary, ['--mode', 'attach', ...sshArgs, '--remote-root', '/root/.dsh-shellout'], {
  stdio: ['pipe', 'pipe', 'inherit'],
})
let nextId = 1
const pending = []
const received = []

function write(value) {
  const body = Buffer.from(JSON.stringify(value))
  const header = Buffer.alloc(4)
  header.writeUInt32BE(body.length)
  child.stdin.write(Buffer.concat([header, body]))
}

let buffer = Buffer.alloc(0)
child.stdout.on('data', (chunk) => {
  buffer = Buffer.concat([buffer, chunk])
  while (buffer.length >= 4) {
    const length = buffer.readUInt32BE(0)
    if (buffer.length < 4 + length) return
    const frame = JSON.parse(buffer.subarray(4, 4 + length).toString())
    buffer = buffer.subarray(4 + length)
    const waiter = pending.shift()
    if (waiter === undefined) received.push(frame)
    else waiter(frame)
  }
})

function read() {
  const ready = received.shift()
  if (ready !== undefined) return Promise.resolve(ready)
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { reject(new Error('no frame within 120s')) }, 120_000)
    pending.push((frame) => { clearTimeout(timer); resolve(frame) })
  })
}

async function request(method, payload) {
  const id = nextId++
  write({ id, kind: 'req', method, payload })
  for (;;) {
    const frame = await read()
    if (frame.kind === 'evt') continue
    assert.equal(frame.kind, 'res', `${method}: ${JSON.stringify(frame)}`)
    return frame.payload
  }
}

// The host keeps history, so an id from a previous run is already taken.
const executionId = `e2e-ssh-${Date.now()}`

const spec = {
  argv: ['/bin/sh', '-c', 'uname -sm; cat /etc/alpine-release 2>/dev/null || echo non-alpine; hostname'],
  cwd: '/root',
  env: { PATH: '/usr/local/bin:/usr/bin:/bin' },
  timeoutMs: 30_000,
  onExpiry: 'kill',
  graceMs: 500,
  capture: { stdoutMaxBytes: 4096, stderrMaxBytes: 4096, spillMaxBytes: 0, spillDir: '/tmp' },
}

try {
  const hello = await request('hello', { protocol: '2.0', clientBuild: 'e2e-remote-ssh', bootId: 'boot-e2e-ssh' })
  assert.equal(hello.protocol, '2.0')
  console.log(`PASS attach over ssh to ${host}:${port}`)

  await request('exec', { operationId: 'op-e2e-ssh', executionId, specDigest: 'digest', spec })
  let settled
  while (settled === undefined) {
    const frame = await read()
    if (frame.kind === 'evt' && frame.payload?.executionId === executionId) settled = frame.payload
  }
  assert.equal(settled.status, 'exited', JSON.stringify(settled))
  assert.equal(settled.exitCode, 0, JSON.stringify(settled))
  console.log('PASS the command settled on the host with exit 0')

  const captured = await request('readStream', { executionId, stream: 'stdout', offset: 0, limit: 4096 })
  assert.match(captured.text, /Linux/, JSON.stringify(captured.text))
  console.log(`PASS the output came from the host: ${JSON.stringify(captured.text.trim())}`)

  const history = await request('history', { limit: 5 })
  const row = history.rows.find(candidate => candidate.id === executionId)
  assert.ok(row, JSON.stringify(history.rows))
  assert.equal(row.state, 'exited')
  console.log('PASS history answered from the host')
} finally {
  child.kill('SIGTERM')
  await once(child, 'exit').catch(() => {})
}
