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
import { probePlatform, runDirect } from './direct.js'
import { attachArgv, resolveHost, type Hosts } from './hosts.js'
import { syncToHost, type SyncReport, type SyncRequest } from './sync.js'
import { runInTty } from './tty.js'

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

/** One line that says what the copy did before the command ran. */
function describeSync(report: SyncReport): string {
  const deleted = report.deleted ? 'host files absent from the source were removed' : 'nothing was deleted on the host'
  return `synced ${report.from} -> ${report.to} in ${String(report.ms)} ms (${report.mode}; ${deleted})`
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
  /** Chosen path per alias, decided once from the host itself. */
  const paths = new Map<string, 'runner' | 'ssh'>()

  /** A host runs the runner only when it is Linux on an architecture this plane ships. */
  const pathFor = async (alias: string): Promise<'runner' | 'ssh'> => {
    const declared = resolveHost(options.hosts, alias).mode
    if (declared !== undefined) return declared
    const known = paths.get(alias)
    if (known !== undefined) return known
    const platform = await probePlatform(resolveHost(options.hosts, alias))
    const chosen = platform === undefined
      ? 'runner'
      : platform.os === 'Linux' && (platform.arch === 'x86_64' || platform.arch === 'aarch64')
        ? 'runner'
        : 'ssh'
    paths.set(alias, chosen)
    return chosen
  }

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
      tty: {
        type: 'boolean',
        description: 'Attach a terminal, for programs that need one (interactive prompts, TUIs). Output arrives as one merged stream with control sequences, and a terminal run is bound to this call: it is not indexed, cannot be adopted after the connection drops, and ignores remote_host sync. Defaults to false, which keeps stdout and stderr separate.',
      },
      sync: {
        type: 'object',
        additionalProperties: false,
        description: 'Copy a local path to the host before the command runs. Needs remote_host. Nothing is deleted on the host unless delete is true.',
        properties: {
          from: { type: 'string', required: true, description: 'Local file or directory to copy.' },
          to: { type: 'string', required: true, description: 'Destination path on the host.' },
          delete: { type: 'boolean', description: 'Remove host files the source does not have; defaults to false.' },
        },
      },
    },
    output: {
      schema: { type: 'string' },
      render: (_args, value) => [{ type: 'text', text: value }],
    },
    async execute(args, exec) {
      exec.signal.throwIfAborted()
      if (args.tty === true) {
        if (args.sync !== undefined) throw new Error('shell: sync needs a runner execution; a terminal run is connection-bound and cannot be indexed')
        const entry = args.remote_host === undefined ? undefined : resolveHost(options.hosts, args.remote_host)
        const result = await runInTty({
          command: args.command,
          ...args.cwd === undefined ? {} : { cwd: args.cwd },
          ...args.timeout_ms === undefined ? {} : { timeoutMs: args.timeout_ms },
          ...exec.signal === undefined ? {} : { signal: exec.signal },
        }, entry)
        const header = [
          `host: ${args.remote_host ?? 'local'}  tty: true  merged: true`,
          `exit: ${result.exitCode ?? 'null'}${result.signal === null ? '' : ` (signal ${result.signal})`}`,
          ...result.timedOut ? ['timedOut: true'] : [],
        ].join('  ')
        return `${header}\n${result.output}`
      }
      const request = {
        command: args.command,
        ...args.cwd === undefined ? {} : { workdir: args.cwd },
        ...args.timeout_ms === undefined ? {} : { timeoutMs: args.timeout_ms },
        ...exec.signal === undefined ? {} : { signal: exec.signal },
      } as const
      if (args.remote_host === undefined) {
        if (args.sync !== undefined) throw new Error('shell: sync needs remote_host; there is nothing to copy to for a local command')
        const execution = await options.local.execute(options.local.resolve(request))
        return render('local', await execution.result())
      }
      const entry = resolveHost(options.hosts, args.remote_host)
      const requested = args.sync as SyncRequest | undefined
      const report = requested === undefined ? undefined : await syncToHost(entry, requested)
      if (await pathFor(args.remote_host) === 'ssh') {
        const direct = await runDirect(entry, {
          command: args.command,
          ...(args.cwd ?? entry.cwd) === undefined ? {} : { cwd: args.cwd ?? entry.cwd },
          ...entry.shell === undefined ? {} : { shell: entry.shell },
          ...args.timeout_ms === undefined ? {} : { timeoutMs: args.timeout_ms },
          ...exec.signal === undefined ? {} : { signal: exec.signal },
        })
        const header = [
          `host: ${args.remote_host}  path: ssh (no runner there)`,
          `exit: ${direct.exitCode ?? 'null'}${direct.signal === null ? '' : ` (signal ${direct.signal})`}`,
          ...direct.timedOut ? ['timedOut: true'] : [],
        ].join('  ')
        const parts = [header]
        if (direct.stdout.length > 0) parts.push(`stdout:\n${direct.stdout}`)
        if (direct.stderr.length > 0) parts.push(`stderr:\n${direct.stderr}`)
        const body = parts.join('\n')
        return report === undefined ? body : `${describeSync(report)}\n${body}`
      }
      const binding = await executorFor(args.remote_host)
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
      const rendered = render(args.remote_host, await started.execution.result())
      return report === undefined ? rendered : `${describeSync(report)}\n${rendered}`
    },
  }))
  })
}
