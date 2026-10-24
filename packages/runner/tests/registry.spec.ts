import { describe, expect, it } from 'vitest'
import { specDigest, type ExecSpec, type ExecRequest, type SettledEvent } from 'dsh-shellout-protocol'
import { ExecutionRegistry, RunnerError } from '../src/core/registry.ts'
import { FakeBackend } from './support/fake-backend.ts'

const SPEC: ExecSpec = {
  command: 'echo hi',
  workdir: '/tmp',
  timeoutMs: 1_000,
  onExpiry: 'kill',
  stdoutMaxBytes: 4_096,
}

const BOOT = 'boot-a'

function request(id: string, spec: ExecSpec = SPEC, operationId = `op-${id}`): ExecRequest {
  return { operationId, executionId: id, spec, specDigest: specDigest(spec) }
}

function registry(options: Parameters<typeof buildRegistry>[0] = {}): ReturnType<typeof buildRegistry> {
  return buildRegistry(options)
}

function buildRegistry(options: { leaseTtlMs?: number; maxConcurrent?: number; settleRetentionMs?: number; sweepIntervalMs?: number } = {}) {
  const backend = new FakeBackend()
  const instance = new ExecutionRegistry(backend, { sweepIntervalMs: 15, ...options })
  const events: SettledEvent[] = []
  instance.onSettled(event => { events.push(event) })
  return { backend, instance, events }
}

describe('ExecutionRegistry', () => {
  it('registers one execution and reports it on attach', async () => {
    const { backend, instance } = registry()
    instance.attach(BOOT)
    const started = await instance.exec(request('e1'), BOOT)
    expect(started.executionId).toBe('e1')
    expect(backend.specs).toHaveLength(1)
    expect(instance.attach(BOOT)).toEqual([{ executionId: 'e1', state: 'running' }])
    instance.dispose()
  })

  it('makes a retried operation idempotent', async () => {
    const { backend, instance } = registry()
    instance.attach(BOOT)
    const first = await instance.exec(request('e1'), BOOT)
    const second = await instance.exec(request('e1'), BOOT)
    expect(second.executionId).toBe(first.executionId)
    expect(backend.specs).toHaveLength(1)
    instance.dispose()
  })

  it('rejects an operation id reused with a different spec', async () => {
    const { instance } = registry()
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    await expect(instance.exec(request('e2', { ...SPEC, command: 'echo bye' }, 'op-e1'), BOOT))
      .rejects.toBeInstanceOf(RunnerError)
    instance.dispose()
  })

  it('rejects when the concurrency cap is reached', async () => {
    const { instance } = registry({ maxConcurrent: 1 })
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    await expect(instance.exec(request('e2'), BOOT)).rejects.toThrowError(/limit/)
    instance.dispose()
  })

  it('surfaces a backend preparation failure as a rejection', async () => {
    const { backend, instance } = registry()
    instance.attach(BOOT)
    backend.failNext = new Error('spawn failed: ENOENT')
    await expect(instance.exec(request('e1'), BOOT)).rejects.toThrowError(/ENOENT/)
    instance.dispose()
  })

  it('announces settlement once and keeps output readable afterwards', async () => {
    const { backend, instance, events } = registry()
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    backend.executions[0]?.append('stdout', 'hello')
    backend.executions[0]?.complete(0)
    await new Promise(resolve => { setTimeout(resolve, 10) })
    expect(events).toHaveLength(1)
    expect(events[0]).toMatchObject({ executionId: 'e1', status: 'exited', exitCode: 0 })
    expect(instance.readStream('e1', 'stdout', 0, 16).text).toBe('hello')
    instance.dispose()
  })

  it('answers kill only for a live execution', async () => {
    const { backend, instance } = registry()
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    expect(instance.kill('e1')).toEqual({ requested: true })
    await new Promise(resolve => { setTimeout(resolve, 10) })
    expect(backend.executions[0]?.state().state).toBe('killed')
    expect(instance.kill('e1')).toEqual({ requested: false })
    instance.dispose()
  })

  it('terminates a boot\'s executions when its lease expires', async () => {
    const { backend, instance, events } = registry({ leaseTtlMs: 40, sweepIntervalMs: 10 })
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    await new Promise(resolve => { setTimeout(resolve, 120) })
    expect(backend.executions[0]?.state().state).toBe('killed')
    expect(events[0]).toMatchObject({ status: 'failed' })
    expect(events[0]?.detail).toMatch(/lease/)
    instance.dispose()
  })

  it('keeps a boot alive while pings arrive', async () => {
    const { backend, instance } = registry({ leaseTtlMs: 60, sweepIntervalMs: 10 })
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    for (let index = 0; index < 5; index += 1) {
      await new Promise(resolve => { setTimeout(resolve, 30) })
      instance.ping(BOOT)
    }
    expect(backend.executions[0]?.state().state).toBe('running')
    instance.dispose()
  })

  it('rejects a ping for an unknown boot', () => {
    const { instance } = registry()
    expect(() => instance.ping('nope')).toThrowError(/unknown boot/)
    instance.dispose()
  })

  it('drains live executions', async () => {
    const { backend, instance } = registry()
    instance.attach(BOOT)
    await instance.exec(request('e1'), BOOT)
    const live = backend.executions[0]
    if (live === undefined) throw new Error('missing execution')
    live.complete(0)
    const drained = await instance.drain(1_000)
    expect(drained.live).toBe(0)
    instance.dispose()
  })
})
