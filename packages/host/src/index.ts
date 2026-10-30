/**
 * Cordis entry for the host plugin.
 *
 * Preferred path: connect to the runner and register an executor that runs
 * commands out of process. Resilience path: if the runner is unreachable after
 * a bounded wait, fall back to the stock in-process executor — an agent never
 * loses its shell because the execution plane is down.
 *
 * The fallback is explicit, configured, and logged as one warning line naming
 * the socket that did not answer; it is never silent.
 *
 * @module dsh-shellout
 */

import { randomUUID } from 'node:crypto'
import type { Context } from '@deepseek-ai/cordis'
import { ShelloutClient } from './core/client.js'
import { ShelloutExecutor, resolveConfig, type ShelloutConfig } from './dsh/provider.js'

export { ShelloutExecutor, resolveConfig, type ShelloutConfig } from './dsh/provider.js'
export { ShelloutClient, ShelloutRpcError } from './core/client.js'
export { RemoteExecution } from './core/execution.js'

/** Plugin name used by the loader. */
export const name = 'shellout'

/**
 * Connect to the runner and register the shell executor, or fall back to the
 * local executor when the runner does not answer in time.
 * @param ctx - owning context.
 * @param config - plugin configuration.
 * @throws when the runner is unreachable and `fallback` is `'none'`.
 */
export async function apply(ctx: Context, config: ShelloutConfig = {}): Promise<void> {
  const resolved = resolveConfig(config)
  const client = await connectWithRetry(resolved)
  if (client !== undefined) {
    const connected: ShelloutClient = client
    ctx.effect(() => () => { connected.close() }, 'shellout.connection')
    await ctx.plugin(class extends ShelloutExecutor {
      constructor(inner: Context) {
        super(inner, resolved, connected)
      }
    })
    return
  }
  if (resolved.fallback === 'none') {
    throw new Error(`shellout: ${described(resolved)} did not answer within ${String(resolved.connectRetryMs)} ms and fallback is disabled`)
  }
  ctx.logger?.warn?.(`shellout: ${described(resolved)} did not answer within ${String(resolved.connectRetryMs)} ms; falling back to in-process execution`)
  const { default: LocalBashExecutor } = await import('@deepseek-ai/dsh-bash-local')
  await ctx.plugin(LocalBashExecutor, {
    ...resolved.local.cwd === undefined ? {} : { cwd: resolved.local.cwd },
    timeoutMs: resolved.local.timeoutMs,
    maxTimeoutMs: resolved.local.maxTimeoutMs,
    maxOutputBytes: resolved.local.maxOutputBytes,
    maxSpillBytes: resolved.local.maxSpillBytes,
    graceMs: resolved.local.graceMs,
  })
}

/** Name the target a failure refers to. */
function described(config: { transport: { kind: string; path?: string; argv?: readonly string[] } }): string {
  if (config.transport.kind === 'socket') return `runner socket ${String(config.transport.path)}`
  return `remote attach command ${(config.transport.argv ?? []).join(' ')}`
}

/**
 * Try to connect until the configured deadline.
 * @param config - resolved plugin configuration.
 * @returns the connected client, or undefined when the deadline passed.
 */
async function connectWithRetry(config: ReturnType<typeof resolveConfig>): Promise<ShelloutClient | undefined> {
  const deadline = Date.now() + config.connectRetryMs
  for (;;) {
    try {
      return await ShelloutClient.connect({
        transport: config.transport,
        socketPath: config.socketPath,
        bootId: randomUUID(),
        clientBuild: 'dsh-shellout',
        connectTimeoutMs: config.connectTimeoutMs,
        heartbeatMs: config.heartbeatMs,
        ...config.reconnectDelayMs === undefined ? {} : { reconnectDelayMs: config.reconnectDelayMs },
      })
    } catch {
      if (Date.now() >= deadline) return undefined
      await new Promise<void>((resolve) => { setTimeout(resolve, 250) })
    }
  }
}
