/**
 * Print the raw protocol replies for one trivial execution.
 *
 * Used to separate an executor-side shape change from a host-side mapping bug:
 * it talks to the runner socket directly, with no tool layer in between.
 *
 * Usage: node scripts/probe-exec.mjs [socketPath]
 */

const base = new URL('../packages/', import.meta.url).pathname
const { ShelloutClient } = await import(base + 'host/lib/core/client.js')
const { specDigest } = await import(base + 'protocol/lib/index.js')

const socketPath = process.argv[2] ?? '/root/.dsh/shellout/control.sock'
const client = await ShelloutClient.connect({ socketPath, bootId: `probe-${Date.now()}` })
console.log('handshake: ok')

const id = `probe-${Date.now()}`
const spec = {
  argv: ['/bin/sh', '-c', 'echo probe-ok'],
  cwd: '/tmp',
  env: { PATH: '/usr/local/bin:/usr/bin:/bin' },
  timeoutMs: 30_000,
  onExpiry: 'kill',
  graceMs: 1_000,
  capture: { stdoutMaxBytes: 4_096, stderrMaxBytes: 4_096, spillMaxBytes: 0, spillDir: '/tmp' },
}
const execReply = await client.request('exec', {
  operationId: `op-${id}`,
  executionId: id,
  spec,
  specDigest: specDigest(spec),
})
console.log('exec reply:', JSON.stringify(execReply))

let last = null
for (let attempt = 0; attempt < 60; attempt += 1) {
  last = await client.request('state', { executionId: id }).catch((error) => ({ error: String(error) }))
  if (last && last.state && last.state !== 'running') break
  await new Promise((resolve) => setTimeout(resolve, 50))
}
console.log('state reply:', JSON.stringify(last))

const output = await client.request('readStream', { executionId: id, stream: 'stdout', offset: 0, limit: 4096 })
  .catch((error) => ({ error: String(error) }))
console.log('readStream reply:', JSON.stringify(output))
client.close()
