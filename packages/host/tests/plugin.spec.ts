import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Context } from '@deepseek-ai/cordis'
import type { ShellExecutor } from '@deepseek-ai/dsh-shell'
import { afterEach, describe, expect, it } from 'vitest'
import { ExecutionRegistry, RunnerServer } from 'dsh-shellout-runner/core'
import { apply, ShelloutExecutor } from '../src/index.ts'
import { FakeBackend } from '../../runner/tests/support/fake-backend.ts'

const cleanups: (() => Promise<void> | void)[] = []
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.()
})

async function boot(): Promise<{ ctx: Context; shell: ShellExecutor; backend: FakeBackend }> {
  const dir = await mkdtemp(join(tmpdir(), 'shellout-plugin-'))
  const socketPath = join(dir, 'control.sock')
  const backend = new FakeBackend()
  const registry = new ExecutionRegistry(backend, { sweepIntervalMs: 20 })
  const server = await RunnerServer.listen({
    socketPath,
    registry,
    descriptor: backend.descriptor,
    runnerBuild: 'test',
    runnerBootId: 'runner-boot',
  })
  const ctx = new Context()
  cleanups.push(async () => { await (ctx as unknown as { dispose?: () => Promise<void> }).dispose?.(); await server.close(); registry.dispose(); await rm(dir, { recursive: true, force: true }) })
  await apply(ctx, { socketPath, mirrorPollMs: 20 })
  const shell = ctx.get('shell') as ShellExecutor | undefined
  if (shell === undefined) throw new Error('shell service was not registered')
  return { ctx, shell, backend }
}

describe('host plugin', () => {
  it('registers a shell executor over the runner connection', async () => {
    const { shell } = await boot()
    expect(shell).toBeInstanceOf(ShelloutExecutor)
    expect(shell.sandboxMode).toBeUndefined()
  })

  it('resolves a request with the runner descriptor defaults', async () => {
    const { shell } = await boot()
    const spec = shell.resolve({ command: 'echo hi' })
    expect(spec.workdir).toBe('/tmp')
    expect(spec.timeoutMs).toBe(60_000)
    expect(spec.onExpiry).toBe('kill')
    expect(spec.stdoutMaxBytes).toBe(1_048_576)
  })

  it('executes through the runner and reports the result', async () => {
    const { shell, backend } = await boot()
    const spec = shell.resolve({ command: 'echo hi', timeoutMs: 5_000 })
    const execution = await shell.execute(spec)
    backend.executions[0]?.append('stdout', 'hi\n')
    await new Promise(resolve => { setTimeout(resolve, 100) })
    backend.executions[0]?.complete(0)
    const result = await execution.result()
    expect(result.exitCode).toBe(0)
    expect(result.timedOut).toBe(false)
    expect(result.aborted).toBe(false)
    expect(result.stdout.text).toBe('hi\n')
    await execution.done
  })

  it('keeps a returned handle readable for background producers', async () => {
    const { shell, backend } = await boot()
    const spec = shell.resolve({ command: 'long-running', timeoutMs: 5_000 })
    const execution = await shell.execute(spec)
    backend.executions[0]?.append('stdout', 'part one\n')
    await new Promise(resolve => { setTimeout(resolve, 100) })
    expect(execution.readOutput().delta).toContain('part one')
    backend.executions[0]?.complete(3)
    await execution.done
    expect(execution.status).toBe('completed')
    expect(execution.exitCode).toBe(3)
  })

  it('turns a caller abort into an aborted result', async () => {
    const { shell, backend } = await boot()
    const controller = new AbortController()
    const spec = shell.resolve({ command: 'sleep', timeoutMs: 60_000, signal: controller.signal })
    const execution = await shell.execute(spec)
    await new Promise(resolve => { setTimeout(resolve, 60) })
    controller.abort()
    await execution.done
    expect(backend.executions[0]?.state().state).toBe('killed')
    expect(execution.status).toBe('killed')
  })
})
