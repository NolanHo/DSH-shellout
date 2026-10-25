/**
 * Cordis entry for the runner service.
 *
 * The runner mounts a stock local shell executor plus this plugin; the plugin
 * owns the socket, the execution table and the boot lease. Nothing here needs
 * an agent, a session store, or the web surface — the process stays alive on
 * its listening socket.
 *
 * @module dsh-shellout-runner
 */

import type { Context } from '@deepseek-ai/cordis'
import type { SubprocessRuntime } from '@deepseek-ai/dsh-subprocess'
import type { RunnerDescriptor } from 'dsh-shellout-protocol'
import { ExecutionRegistry } from './core/registry.js'
import { RunnerServer } from './core/server.js'
import { LocalProcessBackend } from './dsh/backend.js'

export { ExecutionRegistry, RunnerError } from './core/registry.js'
export { RunnerServer } from './core/server.js'
export { LocalProcessBackend } from './dsh/backend.js'
export type { BackendExecution, ShellBackend } from './core/types.js'

/** Runner configuration; every field is optional. */
export interface RunnerConfig {
  /** Socket path the host connects to. */
  socketPath?: string
  /** Exported as the descriptor's default working directory. */
  cwd?: string
  /** Descriptor default deadline, in milliseconds. */
  timeoutMs?: number
  /** Descriptor deadline cap, in milliseconds. */
  maxTimeoutMs?: number
  /** Descriptor default stdout capture budget, in bytes. */
  maxOutputBytes?: number
  /** Boot lease lifetime without a ping, in milliseconds. */
  leaseTtlMs?: number
  /** Maximum concurrently live executions. */
  maxConcurrent?: number
  /** How long a settled execution stays readable, in milliseconds. */
  settleRetentionMs?: number
  /** Lease sweep cadence, in milliseconds. */
  sweepIntervalMs?: number
}

/** Plugin name used by the loader. */
export const name = 'shellout-runner'

/** The runner creates processes through the subprocess service. */
export const inject = ['subprocess']

/**
 * Start the runner server for the mounted executor.
 * @param ctx - owning context.
 * @param config - plugin configuration.
 * @throws when the socket cannot be bound.
 */
export async function apply(ctx: Context, config: RunnerConfig = {}): Promise<void> {
  const subprocess = ctx.get('subprocess') as SubprocessRuntime | undefined
  if (subprocess === undefined) throw new Error('shellout-runner requires a mounted subprocess service')
  const socketPath = config.socketPath ?? `${process.env.DSH_HOME ?? `${process.env.HOME ?? '/root'}/.dsh`}/shellout/control.sock`
  const descriptor: RunnerDescriptor = {
    cwd: config.cwd ?? process.cwd(),
    timeoutMs: config.timeoutMs ?? 60_000,
    maxTimeoutMs: config.maxTimeoutMs ?? 600_000,
    maxOutputBytes: config.maxOutputBytes ?? 1_048_576,
  }
  const registry = new ExecutionRegistry(new LocalProcessBackend(subprocess, descriptor), {
    ...config.leaseTtlMs === undefined ? {} : { leaseTtlMs: config.leaseTtlMs },
    ...config.maxConcurrent === undefined ? {} : { maxConcurrent: config.maxConcurrent },
    ...config.settleRetentionMs === undefined ? {} : { settleRetentionMs: config.settleRetentionMs },
    ...config.sweepIntervalMs === undefined ? {} : { sweepIntervalMs: config.sweepIntervalMs },
  })
  const server = await RunnerServer.listen({
    socketPath,
    registry,
    descriptor,
    runnerBuild: '0.1.0',
    runnerBootId: `${String(process.pid)}-${String(Date.now())}`,
    leaseTtlMs: config.leaseTtlMs ?? 60_000,
  })
  ctx.effect(() => async () => {
    await server.close()
    registry.dispose()
  }, 'shellout-runner.server')
  ctx.logger?.info?.(`shellout-runner listening on ${socketPath}`)
}
