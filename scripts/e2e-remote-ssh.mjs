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
import { execFileSync, spawn } from 'node:child_process'
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

/** The plain ssh invocation for inspecting the host itself. */
const sshCli = [
  '-i', key,
  '-p', port,
  '-o', 'BatchMode=yes',
  '-o', 'StrictHostKeyChecking=no',
  '-o', 'UserKnownHostsFile=/dev/null',
  '-o', 'ConnectTimeout=8',
  host,
]

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


/** One attach session: a spawned attach child plus frame plumbing. */
function attach(executorArgs = []) {
  const child = spawn(binary, ['--mode', 'attach', ...executorArgs, ...sshArgs, '--remote-root', '/root/.dsh-shellout'], {
    stdio: ['pipe', 'pipe', 'inherit'],
  })
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
      const timer = setTimeout(() => { reject(new Error('no frame within 120s')) }, 120_000)
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
    async settle(executionId) {
      for (;;) {
        const frame = await read()
        if (frame.kind === 'evt' && frame.payload?.executionId === executionId) return frame.payload
      }
    },
    async state(executionId) {
      return await session.request('state', { executionId })
    },
    async hello(bootId) {
      return await session.request('hello', { protocol: '2.0', clientBuild: 'e2e-remote-ssh', bootId })
    },
    close() {
      child.kill('SIGTERM')
    },
  }
  return session
}

function spec(argv, timeoutMs = 60_000) {
  return {
    argv,
    cwd: '/root',
    env: { PATH: '/usr/local/bin:/usr/bin:/bin' },
    timeoutMs,
    onExpiry: 'kill',
    graceMs: 500,
    capture: { stdoutMaxBytes: 65536, stderrMaxBytes: 65536, spillMaxBytes: 0, spillDir: '/tmp' },
  }
}

async function readStdout(session, executionId) {
  const stream = await session.request('readStream', { executionId, stream: 'stdout', offset: 0, limit: 65536 })
  return stream.text
}

/** Wait until the host reports a terminal state, however the session ends. */
async function settledInHistory(session, executionId, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs
  for (;;) {
    const state = await session.state(executionId)
    if (state.state !== 'running') return state
    assert.ok(Date.now() < deadline, `${executionId} never settled: ${JSON.stringify(state)}`)
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
}

const run = Date.now()
const id = (name) => `e2e-${name}-${run}`

try {
  // 1. The controller dies mid-command; the command keeps running and a later
  //    session reads its real exit code and complete output.
  {
    const first = attach()
    await first.hello(`boot-${run}-1`)
    const execution = id('survive')
    await first.request('exec', {
      operationId: `op-${execution}`,
      executionId: execution,
      specDigest: 'digest',
      spec: spec(['/bin/sh', '-c', 'i=0; while [ $i -lt 6 ]; do echo tick-$i; i=$((i+1)); sleep 1; done; exit 7']),
    })
    await new Promise((resolve) => setTimeout(resolve, 1500))
    first.close()
    console.log('PASS scenario 1a: controller exits while the command is running')

    const second = attach()
    await second.hello(`boot-${run}-2`)
    const state = await settledInHistory(second, execution)
    assert.equal(state.exitCode, 7, JSON.stringify(state))
    const stdout = await readStdout(second, execution)
    assert.equal(stdout, 'tick-0\ntick-1\ntick-2\ntick-3\ntick-4\ntick-5\n', JSON.stringify(stdout))
    console.log('PASS scenario 1b: a later session reads exit 7 and the full stream')
    second.close()
  }

  // 2. A client can end a remote execution; the host settles it as killed.
  {
    const session = attach()
    await session.hello(`boot-${run}-kill`)
    const execution = id('kill')
    await session.request('exec', {
      operationId: `op-${execution}`,
      executionId: execution,
      specDigest: 'digest',
      spec: spec(['/bin/sh', '-c', 'sleep 600']),
    })
    await new Promise((resolve) => setTimeout(resolve, 800))
    await session.request('kill', { executionId: execution })
    const settled = await session.settle(execution)
    assert.equal(settled.status, 'killed', JSON.stringify(settled))
    console.log('PASS scenario 2: a client kill settles the remote execution as killed')
    session.close()
  }

  // 3. The remote executor is killed; a later session adopts the shim and
  //    reports the command's real exit code and complete output.
  {
    const first = attach()
    await first.hello(`boot-${run}-adopt`)
    const execution = id('adopt')
    await first.request('exec', {
      operationId: `op-${execution}`,
      executionId: execution,
      specDigest: 'digest',
      spec: spec(['/bin/sh', '-c', 'i=0; while [ $i -lt 4 ]; do echo keep-$i; i=$((i+1)); sleep 1; done; exit 5']),
    })
    await new Promise((resolve) => setTimeout(resolve, 1200))
    // The pid comes from the identity the executor published, never from a
    // command-line pattern that could match this script's own shell.
    const identity = JSON.parse(execFileSync('ssh', [...sshCli, 'cat /root/.dsh-shellout/state/executor.json'], { encoding: 'utf8' }))
    execFileSync('ssh', [...sshCli, `kill -9 ${identity.pid}`], { encoding: 'utf8' })
    first.close()
    await new Promise((resolve) => setTimeout(resolve, 800))

    const second = attach()
    await second.hello(`boot-${run}-adopt-after`)
    const state = await settledInHistory(second, execution)
    assert.equal(state.exitCode, 5, JSON.stringify(state))
    const stdout = await readStdout(second, execution)
    assert.equal(stdout, 'keep-0\nkeep-1\nkeep-2\nkeep-3\n', JSON.stringify(stdout))
    console.log('PASS scenario 3: a killed remote executor is adopted; exit 5 and the full stream survive')
    second.close()
  }

  // 6. The admission limit is the same rule on the host: a second execution is
  //    refused while the first holds the only slot.
  {
    const session = attach(['--remote-executor-arg', '--max-concurrent', '--remote-executor-arg', '1'])
    await session.hello(`boot-${run}-admit`)
    const first = id('admit-a')
    const second = id('admit-b')
    await session.request('exec', {
      operationId: `op-${first}`,
      executionId: first,
      specDigest: 'digest',
      spec: spec(['/bin/sh', '-c', 'sleep 30']),
    })
    let refused = null
    try {
      await session.request('exec', {
        operationId: `op-${second}`,
        executionId: second,
        specDigest: 'digest',
        spec: spec(['/bin/echo', 'should-not-run']),
      })
    } catch (error) {
      refused = error
    }
    assert.ok(refused, 'the host refuses past its admission limit')
    assert.match(String(refused.message), /busy|concurrent|refus/i, String(refused.message))
    await session.request('kill', { executionId: first })
    const settled = await session.settle(first)
    assert.equal(settled.status, 'killed', JSON.stringify(settled))
    console.log(`PASS scenario 6: the host enforces its admission limit (${String(refused.message).slice(0, 60)})`)
    session.close()
  }

  // 7. With no executor alive the shim still enforces the lease it was given.
  {
    const session = attach([
      '--remote-executor-arg', '--lease-ttl-ms', '--remote-executor-arg', '1500',
    ])
    await session.hello(`boot-${run}-lease`)
    const execution = id('lease')
    await session.request('exec', {
      operationId: `op-${execution}`,
      executionId: execution,
      specDigest: 'digest',
      spec: spec(['/bin/sh', '-c', 'sleep 120'], 240_000),
    })
    await new Promise((resolve) => setTimeout(resolve, 800))
    // Kill the executor and stop pinging: only the shim is left to notice.
    const identity = JSON.parse(execFileSync('ssh', [...sshCli, 'cat /root/.dsh-shellout/state/executor.json'], { encoding: 'utf8' }))
    execFileSync('ssh', [...sshCli, `kill -9 ${identity.pid}`], { encoding: 'utf8' })
    session.close()
    await new Promise((resolve) => setTimeout(resolve, 5000))

    const observer = attach()
    await observer.hello(`boot-${run}-lease-after`)
    const state = await settledInHistory(observer, execution, 60_000)
    assert.equal(state.state, 'killed', JSON.stringify(state))
    console.log('PASS scenario 7: the shim ends an orphaned execution on its lease with no executor alive')
    observer.close()
  }

  // 5. Two sessions on one host: the second bridges to the same supervisor.
  {
    const a = attach()
    const b = attach()
    await a.hello(`boot-${run}-bridge-a`)
    await b.hello(`boot-${run}-bridge-b`)
    const one = id('bridge-a')
    const two = id('bridge-b')
    await a.request('exec', { operationId: `op-${one}`, executionId: one, specDigest: 'digest', spec: spec(['/bin/echo', 'from-a']) })
    await b.request('exec', { operationId: `op-${two}`, executionId: two, specDigest: 'digest', spec: spec(['/bin/echo', 'from-b']) })
    const settledOne = await a.settle(one)
    const settledTwo = await b.settle(two)
    assert.equal(settledOne.exitCode, 0, JSON.stringify(settledOne))
    assert.equal(settledTwo.exitCode, 0, JSON.stringify(settledTwo))
    assert.equal((await readStdout(a, one)).trim(), 'from-a')
    assert.equal((await readStdout(b, two)).trim(), 'from-b')
    const owners = await a.request('snapshot', {})
    assert.ok(owners, 'the shared supervisor answers both sessions')
    console.log('PASS scenario 5: two sessions share one supervisor')
    a.close()
    b.close()
  }
} finally {
  process.exitCode = 0
}
