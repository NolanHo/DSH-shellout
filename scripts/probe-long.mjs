const base = new URL('../packages/', import.meta.url).pathname
const { ShelloutClient } = await import(base + 'host/lib/core/client.js')
const { specDigest } = await import(base + 'protocol/lib/index.js')
const seconds = Number(process.argv[2] ?? 60)
const client = await ShelloutClient.connect({ socketPath: '/root/.dsh/shellout/control.sock', bootId: `probe-long-${Date.now()}` })
const id = `survive-${Date.now()}`
const spec = {
  argv: ['/bin/sh', '-c', `i=0; while [ $i -lt ${seconds} ]; do echo "tick-$i"; i=$((i+1)); sleep 1; done; exit 0`],
  cwd: '/tmp', env: { PATH: '/usr/local/bin:/usr/bin:/bin' },
  timeoutMs: 300000, onExpiry: 'kill', graceMs: 2000,
  capture: { stdoutMaxBytes: 65536, stderrMaxBytes: 65536, spillMaxBytes: 0, spillDir: '/tmp' },
}
await client.request('exec', { operationId: `op-${id}`, executionId: id, spec, specDigest: specDigest(spec) })
console.log(id)
client.close()
process.exit(0)
