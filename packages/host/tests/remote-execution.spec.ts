import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import { specDigest, type SpawnSpec } from 'dsh-shellout-protocol'
import { ExecutionRegistry, RunnerServer } from 'dsh-shellout-runner/core'
import { ShelloutClient, ShelloutRpcError } from '../src/core/client.ts'
import { RemoteExecution } from '../src/core/execution.ts'
import { FakeBackend } from '../../runner/tests/support/fake-backend.ts'

const SPEC: SpawnSpec = {
  argv: ['bash', '-c', 'echo hi'],
  cwd: '/tmp',
  env: { PATH: '/usr/bin:/bin' },
  timeoutMs: 5_000,
  onExpiry: 'kill',
  graceMs: 3_000,
  capture: { stdoutMaxBytes: 1_048_576, stderrMaxBytes: 65_536, spillMaxBytes: 67_108_864, spillDir: '/tmp' },
}

const cleanups: (() => Promise<void> | void)[] = []
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.()
})

interface Harness {
  readonly backend: FakeBackend
  readonly client: ShelloutClient
  readonly registry: ExecutionRegistry
  exec(executionId?: string): Promise<RemoteExecution>
}

async function harness(options: { leaseTtlMs?: number; sweepIntervalMs?: number } = {}): Promise<Harness> {
  const dir = await mkdtemp(join(tmpdir(), 'shellout-host-'))
  const socketPath = join(dir, 'control.sock')
  const backend = new FakeBackend()
  const registry = new ExecutionRegistry(backend, {
    sweepIntervalMs: options.sweepIntervalMs ?? 20,
    ...options.leaseTtlMs === undefined ? {} : { leaseTtlMs: options.leaseTtlMs },
  })
  const server = await RunnerServer.listen({
    socketPath,
    registry,
    descriptor: backend.descriptor,
    runnerBuild: 'test',
    runnerBootId: 'runner-boot',
  })
  const client = await ShelloutClient.connect({ socketPath, bootId: 'boot-host', clientBuild: 'test' })
  cleanups.push(async () => { client.close(); await server.close(); registry.dispose(); await rm(dir, { recursive: true, force: true }) })
  return {
    backend,
    client,
    registry,
    async exec(executionId = 'e1') {
      const execution = new RemoteExecution(client, executionId, { pollMs: 20 })
      client.onSettled(event => { if (event.executionId === executionId) execution.receive(event) })
      await client.request('exec', {
        operationId: `op-${executionId}`,
        executionId,
        spec: SPEC,
        specDigest: specDigest(SPEC),
      })
      execution.start()
      return execution
    },
  }
}

describe('ShelloutClient + RemoteExecution', () => {
  it('handshakes and reports the runner descriptor', async () => {
    const { client } = await harness()
    expect(client.connected).toBe(true)
    expect(client.descriptor).toMatchObject({ cwd: '/tmp', timeoutMs: 60_000 })
  })

  it('mirrors output and settles with exit facts', async () => {
    const { backend, exec } = await harness()
    const execution = await exec()
    backend.executions[0]?.append('stdout', 'hello\n')
    backend.executions[0]?.append('stderr', 'careful')
    await new Promise(resolve => { setTimeout(resolve, 120) })
    backend.executions[0]?.complete(0)
    await execution.done
    expect(execution.state).toBe('exited')
    expect(execution.exitCode).toBe(0)
    expect(execution.readOutput()).toMatchObject({ delta: 'hello\n[stderr]\ncareful', lossy: false })
  })

  it('joins stdout and stderr with one newline when stdout lacks it', async () => {
    const { backend, exec } = await harness()
    const execution = await exec()
    backend.executions[0]?.append('stdout', 'no-newline')
    backend.executions[0]?.append('stderr', 'err')
    await new Promise(resolve => { setTimeout(resolve, 120) })
    backend.executions[0]?.complete(1)
    await execution.done
    expect(execution.readOutput().delta).toBe('no-newline\n[stderr]\nerr')
    expect(execution.exitCode).toBe(1)
  })

  it('serves non-consuming offset reads', async () => {
    const { backend, exec } = await harness()
    const execution = await exec()
    backend.executions[0]?.append('stdout', 'abcdef')
    await new Promise(resolve => { setTimeout(resolve, 120) })
    expect(execution.readStream('stdout', 0, 3).text).toBe('abc')
    expect(execution.readStream('stdout', 0, 3).text).toBe('abc')
    backend.executions[0]?.complete(0)
    await execution.done
  })

  it('reconstructs non-ASCII output byte for byte', async () => {
    const { backend, exec } = await harness()
    const execution = await exec()
    backend.executions[0]?.append('stdout', 'é中\nok\n')
    // Two poll passes: a second read must resume where the runner's byte
    // offset says, not where the decoded text happens to end.
    await new Promise(resolve => { setTimeout(resolve, 140) })
    await new Promise(resolve => { setTimeout(resolve, 140) })
    backend.executions[0]?.complete(0)
    await execution.done
    expect(execution.readOutput().delta).toBe('é中\nok\n')
    expect(execution.readStream('stdout', 0, 1024)).toMatchObject({ text: 'é中\nok\n', nextOffset: 9 })
    const first = execution.readStream('stdout', 0, 2)
    expect(first).toMatchObject({ text: 'é', nextOffset: 2 })
    expect(execution.readStream('stdout', first.nextOffset, 1024).text).toBe('中\nok\n')
  })

  it('reports loss below the retained window', async () => {
    const { backend, client } = await harness()
    const execution = new RemoteExecution(client, 'e1', { pollMs: 20, retainBytes: 4 })
    client.onSettled(event => { execution.receive(event) })
    await client.request('exec', { operationId: 'op1', executionId: 'e1', spec: SPEC, specDigest: specDigest(SPEC) })
    execution.start()
    backend.executions[0]?.append('stdout', '0123456789')
    await new Promise(resolve => { setTimeout(resolve, 150) })
    const read = execution.readStream('stdout', 0, 2)
    expect(read.lossy).toBe(true)
    backend.executions[0]?.complete(0)
    await execution.done
  })

  it('surfaces a kill as a killed settlement', async () => {
    const { exec } = await harness()
    const execution = await exec()
    await new Promise(resolve => { setTimeout(resolve, 60) })
    expect(await execution.kill()).toBe(true)
    await execution.done
    expect(execution.state).toBe('killed')
  })

  it('settles as lost when the owning boot lease expires', async () => {
    const { exec } = await harness({ leaseTtlMs: 60 })
    const execution = await exec()
    await execution.done
    expect(execution.state).toBe('lost')
  })

  it('refuses a handshake against a runner speaking another protocol', async () => {
    await expect(
      ShelloutClient.connect({ socketPath: join(tmpdir(), 'missing-shellout.sock'), bootId: 'b' }),
    ).rejects.toBeInstanceOf(ShelloutRpcError)
  })
})
