/**
 * The `shell` tool: run one command on this machine or on a configured host.
 *
 * The result is the same either way, so a command that ran remotely reads like
 * one that ran locally. A host the deployment did not configure is refused with
 * its closest names rather than attempted.
 */
import type { Context } from '@deepseek-ai/cordis'
import { defineTool } from '@deepseek-ai/dsh-tools'
import type { ShellRunResult } from '@deepseek-ai/dsh-shell'

import type { ShelloutClient } from '../core/client.js'
import { startOn, type ShelloutExecutor } from './provider.js'
import { attachArgv, resolveHost, type Hosts } from './hosts.js'

/** What the tool needs from the plugin to reach a host. */
export interface ShellToolOptions {
  /** Handler bound to the local runner. */
  readonly local: ShelloutExecutor
  /** Configured hosts by alias. */
  readonly hosts: Hosts
  /** Plugin configuration, for per-host defaults. */
  readonly connect: (argv: readonly string[]) => Promise<ShelloutClient>
}

/** One connection per host, kept for the life of the plugin. */
interface HostBinding {
  readonly client: ShelloutClient
}

/** Render one finished command the way the model reads it. */
function render(host: string, result: ShellRunResult): string {
  const header = [
    `host: ${host}`,
    `exit: ${result.exitCode ?? 'null'}${result.signal === null ? '' : ` (signal ${result.signal})`}`,
    ...result.timedOut ? ['timedOut: true'] : [],
  ].join('  ')
  const parts = [header]
  if (result.stdout.text.length > 0) parts.push(`stdout:\n${result.stdout.text}`)
  if (result.stderr.text.length > 0) parts.push(`stderr:\n${result.stderr.text}`)
  if (result.stdout.truncated) parts.push('stdout was truncated by the runner')
  return parts.join('\n')
}

/**
 * Register the tool once the plugin is mounted.
 * @param ctx - owning context.
 * @param options - local handler, host registry, and the connector.
 */
export async function registerShellTool(ctx: Context, options: ShellToolOptions): Promise<void> {
  const bindings = new Map<string, HostBinding>()

  const executorFor = async (alias: string): Promise<HostBinding> => {
    const existing = bindings.get(alias)
    if (existing !== undefined) return existing
    const client = await options.connect(attachArgv(resolveHost(options.hosts, alias)))
    const binding = { client }
    bindings.set(alias, binding)
    ctx.effect(() => () => {
      client.close()
      bindings.delete(alias)
    }, `shellout.host.${alias}`)
    return binding
  }

  ctx.effect(() => () => {
    for (const binding of bindings.values()) binding.client.close()
    bindings.clear()
  }, 'shellout.hosts')

  ctx.inject(['tools'], (scope) => {
  scope.tools.register(defineTool({
    name: 'shell',
    description: 'Run one shell command and return its exit code with the full captured output. Without remote_host it runs on this machine through the same supervised runner as the bash tool. With remote_host it runs on that configured host: the command keeps running there even if this connection drops, and a later call reads the same result. remote_host must be one of the configured aliases; an unknown name is answered with the closest configured ones.',
    parameters: {
      command: { type: 'string', required: true, description: 'Shell command line to run.' },
      remote_host: { type: 'string', description: 'Configured host alias; omit to run locally.' },
      cwd: { type: 'string', description: 'Working directory; defaults to the local workspace or the host default.' },
      timeout_ms: { type: 'number', description: 'Deadline in milliseconds; the runner caps it.' },
    },
    output: {
      schema: { type: 'string' },
      render: (_args, value) => [{ type: 'text', text: value }],
    },
    async execute(args, exec) {
      exec.signal.throwIfAborted()
      const request = {
        command: args.command,
        ...args.cwd === undefined ? {} : { workdir: args.cwd },
        ...args.timeout_ms === undefined ? {} : { timeoutMs: args.timeout_ms },
        ...exec.signal === undefined ? {} : { signal: exec.signal },
      } as const
      if (args.remote_host === undefined) {
        const execution = await options.local.execute(options.local.resolve(request))
        return render('local', await execution.result())
      }
      const binding = await executorFor(args.remote_host)
      const entry = resolveHost(options.hosts, args.remote_host)
      const host = args.cwd ?? entry.cwd ?? process.cwd()
      const started = await startOn(binding.client, options.local.configuration, {
        command: args.command,
        workdir: host,
        timeoutMs: Math.min(args.timeout_ms ?? options.local.configuration.local.timeoutMs, binding.client.descriptor.maxTimeoutMs),
        onExpiry: 'kill',
        sandboxPolicy: undefined,
        stdoutMaxBytes: binding.client.descriptor.maxOutputBytes,
        ...exec.signal === undefined ? {} : { signal: exec.signal },
      }, { remote: true })
      return render(args.remote_host, await started.execution.result())
    },
  }))
  })
}
