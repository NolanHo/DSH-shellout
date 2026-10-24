/**
 * Cordis entry for the host plugin.
 *
 * Loading is fail-loud: the runner handshake completes before the executor is
 * registered, so a missing or incompatible runner stops the host instead of
 * leaving a shell service that cannot run commands.
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
 * Connect to the runner and register the shell executor.
 * @param ctx - owning context.
 * @param config - plugin configuration.
 * @throws when the runner is unreachable or speaks another protocol version.
 */
export async function apply(ctx: Context, config: ShelloutConfig = {}): Promise<void> {
  const resolved = resolveConfig(config)
  const client = await ShelloutClient.connect({
    socketPath: resolved.socketPath,
    bootId: randomUUID(),
    clientBuild: 'dsh-shellout',
    connectTimeoutMs: resolved.connectTimeoutMs,
    heartbeatMs: resolved.heartbeatMs,
  })
  ctx.effect(() => () => { client.close() }, 'shellout.connection')
  await ctx.plugin(class extends ShelloutExecutor {
    constructor(inner: Context) {
      super(inner, resolved, client)
    }
  })
}
