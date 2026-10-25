import { mkdtemp, rm } from 'node:fs/promises'
import net from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { once } from 'node:events'
import { Context } from '@deepseek-ai/cordis'
import type { ShellExecutor } from '@deepseek-ai/dsh-shell'
import { afterEach, describe, expect, it } from 'vitest'
import { ExecutionRegistry, RunnerServer } from 'dsh-shellout-runner/core'
import { ShelloutClient } from '../src/core/client.ts'
import { ShelloutExecutor, resolveConfig } from '../src/dsh/provider.ts'
import { FakeBackend } from '../../runner/tests/support/fake-backend.ts'

const cleanups: (() => Promise<void> | void)[] = []
afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.()
})

interface Wiring {
  readonly backend: FakeBackend
  readonly executor: ShelloutExecutor
  /** Drop every proxied connection, as an executor crash would. */
  disconnect(): void
  /** Replace the runner with a fresh one that holds no executions. */
  restartRunner(): Promise<void>
}

/**
 * A proxy in front of the runner socket, so a test can destroy the connection
 * and let the client reconnect on its own — with or without the runner keeping
 * the execution.
 */
async function boot(): Promise<Wiring> {
  const dir = await mkdtemp(join(tmpdir(), 'shellout-reconnect-'))
  const runnerPath = join(dir, 'runner.sock')
  const proxyPath = join(dir, 'proxy.sock')
  const backend = new FakeBackend()
  let registry = new ExecutionRegistry(backend, { sweepIntervalMs: 20 })
  let server = await RunnerServer.listen({
    socketPath: runnerPath,
    registry,
    descriptor: backend.descriptor,
    runnerBuild: 'test',
    runnerBootId: 'runner-boot',
  })

  const proxied = new Set<net.Socket>()
  const proxy = net.createServer((downstream) => {
    const upstream = net.connect(runnerPath)
    proxied.add(downstream)
    downstream.on('close', () => { proxied.delete(downstream); upstream.destroy() })
    downstream.on('error', () => { /* Reported as a close. */ })
    upstream.on('error', () => { downstream.destroy() })
    downstream.pipe(upstream).pipe(downstream)
  })
  proxy.listen(proxyPath)
  await once(proxy, 'listening')

  const client = await ShelloutClient.connect({
    socketPath: proxyPath,
    bootId: 'boot-reconnect',
    clientBuild: 'test',
    reconnectDelayMs: 20,
  })
  const ctx = new Context()
  const executor = new ShelloutExecutor(ctx, resolveConfig({ mirrorPollMs: 20, reconnectDelayMs: 20 }), client)
  const disconnect = (): void => { for (const socket of proxied) socket.destroy() }
  const restartRunner = async (): Promise<void> => {
    disconnect()
    await server.close()
    registry.dispose()
    registry = new ExecutionRegistry(backend, { sweepIntervalMs: 20 })
    server = await RunnerServer.listen({
      socketPath: runnerPath,
      registry,
      descriptor: backend.descriptor,
      runnerBuild: 'test-2',
      runnerBootId: 'runner-boot-2',
    })
  }
  cleanups.push(async () => {
    client.close()
    disconnect()
    await new Promise<void>((resolve) => { proxy.close(() => { resolve() }) })
    await server.close()
    registry.dispose()
    await rm(dir, { recursive: true, force: true })
  })
  return { backend, executor, disconnect, restartRunner }
}

const asShell = (executor: ShelloutExecutor): ShellExecutor => executor as unknown as ShellExecutor

describe('host recovery after an executor restart', () => {
  it('settles an execution that ended while the host was disconnected', async () => {
    const { backend, executor, disconnect } = await boot()
    const shell = asShell(executor)
    const execution = await shell.execute(shell.resolve({ command: 'exit 5', timeoutMs: 5_000 }))
    backend.executions[0]?.append('stdout', 'tail\n')
    await new Promise(resolve => { setTimeout(resolve, 80) })

    disconnect()
    // The runner keeps the record; the settlement event reaches no connection.
    backend.executions[0]?.complete(5)
    await new Promise(resolve => { setTimeout(resolve, 80) })

    const result = await execution.result()
    expect(result.exitCode).toBe(5)
    expect(result.signal).toBeNull()
    expect(result.stdout.text).toBe('tail\n')
    expect(execution.status).toBe('completed')
  })

  it('reports an execution a restarted runner no longer holds as killed', async () => {
    const { executor, restartRunner } = await boot()
    const shell = asShell(executor)
    const execution = await shell.execute(shell.resolve({ command: 'hold', timeoutMs: 5_000 }))
    await new Promise(resolve => { setTimeout(resolve, 60) })
    await restartRunner()
    const result = await execution.result()
    expect(result.exitCode).toBeNull()
    expect(execution.status).toBe('killed')
    expect(result.stderr.text).toContain('shellout: runner lost')
  })
})
