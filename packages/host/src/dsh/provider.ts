/**
 * The host plugin: an ordinary shell executor whose commands run in the
 * runner process.
 *
 * It subclasses the harness `ShellExecutor` seam, so tools, jobs and every
 * other consumer keep calling the same service. Everything the seam reads
 * synchronously (`resolve`, `readOutput`, `observed`, `status`) is served from
 * local state; only spawning, termination and offset reads cross the socket.
 *
 * @module dsh-shellout/provider
 */

import { randomUUID } from 'node:crypto'
import { join } from 'node:path'
import type { Context } from '@deepseek-ai/cordis'
import type { SandboxMode } from '@deepseek-ai/dsh-sandbox'
import { ShellExecutor } from '@deepseek-ai/dsh-shell'
import type {
  CollectedOutput,
  ShellExecRequest,
  ShellExecSpec,
  ShellExecution,
  ShellProcessRead,
  ShellRunResult,
  ShellSandboxInfo,
} from '@deepseek-ai/dsh-shell'
import { specDigest, type ExecSpec } from 'dsh-shellout-protocol'
import { ShelloutClient } from '../core/client.js'
import { RemoteExecution } from '../core/execution.js'

/** Plugin configuration; every field is optional. */
export interface ShelloutConfig {
  /** Runner socket path; defaults to `$DSH_HOME/shellout/control.sock`. */
  socketPath?: string
  /** Bound on the startup handshake. */
  connectTimeoutMs?: number
  /** Output poll cadence, in milliseconds. */
  mirrorPollMs?: number
  /** Bytes retained per stream for synchronous reads. */
  mirrorRetainBytes?: number
  /** Boot-lease ping cadence. */
  heartbeatMs?: number
}

/** Configuration after defaulting. */
export interface ResolvedShelloutConfig extends Required<ShelloutConfig> {}

/**
 * Apply defaults to a plugin configuration.
 * @param config - caller configuration, possibly partial.
 * @param env - environment to read the harness home from.
 * @returns the fully specified configuration.
 */
export function resolveConfig(config: ShelloutConfig = {}, env: NodeJS.ProcessEnv = process.env): ResolvedShelloutConfig {
  const home = env.DSH_HOME ?? join(env.HOME ?? '/root', '.dsh')
  return {
    socketPath: config.socketPath ?? join(home, 'shellout', 'control.sock'),
    connectTimeoutMs: config.connectTimeoutMs ?? 5_000,
    mirrorPollMs: config.mirrorPollMs ?? 150,
    mirrorRetainBytes: config.mirrorRetainBytes ?? 1_048_576,
    heartbeatMs: config.heartbeatMs ?? 5_000,
  }
}

/** The host-side executor backed by a runner process. */
export class ShelloutExecutor extends ShellExecutor {
  private readonly bootId = randomUUID()
  private readonly live = new Set<RemoteExecution>()
  private readonly detachClose: () => void

  /**
   * @param ctx - owning context.
   * @param config - resolved plugin configuration.
   * @param client - connected runner client.
   */
  constructor(ctx: Context, private readonly config: ResolvedShelloutConfig, private readonly client: ShelloutClient) {
    super(ctx)
    this.detachClose = client.onClose(() => {
      for (const execution of this.live) execution.abortLost('shellout: runner lost')
      this.live.clear()
    })
  }

  /** The runner's sandbox mode, when its executor confines. */
  override get sandboxMode(): SandboxMode | undefined {
    return toSandboxMode(this.client.descriptor.sandboxMode)
  }

  /**
   * Apply this executor's defaults and caps without performing I/O.
   * @param request - the caller's request.
   * @returns the fully specified spec.
   */
  resolve(request: ShellExecRequest): ShellExecSpec {
    const descriptor = this.client.descriptor
    const timeoutMs = request.timeoutMs ?? descriptor.timeoutMs
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) throw new Error(`shellout: invalid timeoutMs ${String(request.timeoutMs)}`)
    const stdoutMaxBytes = request.stdoutMaxBytes ?? descriptor.maxOutputBytes
    if (!Number.isFinite(stdoutMaxBytes) || stdoutMaxBytes <= 0) {
      throw new Error(`shellout: invalid stdoutMaxBytes ${String(request.stdoutMaxBytes)}`)
    }
    return {
      command: request.command,
      workdir: request.workdir ?? descriptor.cwd,
      timeoutMs: Math.min(timeoutMs, descriptor.maxTimeoutMs),
      onExpiry: request.onExpiry ?? 'kill',
      stdoutMaxBytes,
      ...request.signal === undefined ? {} : { signal: request.signal },
      ...request.stdin === undefined ? {} : { stdin: request.stdin },
      ...request.env === undefined ? {} : { env: request.env },
      ...request.dshEnv === undefined ? {} : { dshEnv: request.dshEnv },
      sandboxPolicy: request.sandboxPolicy,
    }
  }

  /**
   * Start one command in the runner.
   * @param spec - a resolved spec.
   * @returns the live execution handle.
   * @throws when the command could not be prepared or the caller aborted first.
   */
  async execute(spec: ShellExecSpec): Promise<ShellExecution> {
    if (spec.signal?.aborted === true) throw new Error('shellout: command aborted before start')
    const executionId = randomUUID()
    const operationId = randomUUID()
    const wireSpec = toWireSpec(spec)
    const remote = new RemoteExecution(this.client, executionId, {
      pollMs: this.config.mirrorPollMs,
      retainBytes: this.config.mirrorRetainBytes,
    })
    const detach = this.client.onSettled((event) => { if (event.executionId === executionId) remote.receive(event) })
    try {
      await this.client.request('exec', {
        operationId,
        executionId,
        spec: wireSpec,
        specDigest: specDigest(wireSpec),
      })
    } catch (error: unknown) {
      detach()
      throw error
    }
    remote.start()
    this.live.add(remote)
    const release = (): void => { this.live.delete(remote); detach() }
    void remote.done.then(release)
    return new RunnerShellExecution(remote, spec, release)
  }

  /** Close the connection and settle every live execution. */
  async [Symbol.asyncDispose](): Promise<void> {
    this.detachClose()
    this.client.close()
  }
}

/** One execution handle returned to the harness. */
class RunnerShellExecution implements ShellExecution {
  private timedOut = false
  private aborted = false
  private readonly resultPromise: Promise<ShellRunResult>
  private readonly donePromise: Promise<void>
  private readonly timer: NodeJS.Timeout | undefined
  private readonly onAbort: (() => void) | undefined

  /**
   * @param remote - the mirrored execution.
   * @param spec - the resolved spec this execution runs under.
   * @param detach - removes the settlement subscription once the execution ends.
   */
  constructor(
    private readonly remote: RemoteExecution,
    private readonly spec: ShellExecSpec,
    private readonly detach: () => void,
  ) {
    this.donePromise = remote.done.then(() => { this.cleanup() })
    this.resultPromise = this.donePromise.then(() => this.buildResult())
    if (spec.onExpiry === 'kill') {
      this.timer = setTimeout(() => {
        if (this.remote.state !== 'running') return
        this.timedOut = true
        void this.remote.kill()
      }, spec.timeoutMs)
      this.timer.unref()
    }
    if (spec.signal !== undefined) {
      this.onAbort = (): void => {
        if (this.remote.state !== 'running') return
        if (!this.timedOut) this.aborted = true
        void this.remote.kill()
      }
      spec.signal.addEventListener('abort', this.onAbort, { once: true })
    }
  }

  get status(): 'running' | 'completed' | 'killed' {
    return this.remote.processStatus
  }

  get exitCode(): number | null {
    return this.remote.exitCode
  }

  get signal(): NodeJS.Signals | null {
    return this.remote.signal as NodeJS.Signals | null
  }

  get sandbox(): ShellSandboxInfo | undefined {
    return this.remote.sandbox as ShellSandboxInfo | undefined
  }

  get done(): Promise<void> {
    return this.donePromise
  }

  readOutput(): ShellProcessRead {
    return this.remote.readOutput()
  }

  get observed(): { stdout: { readFrom(offset: number): { text: string; nextOffset: number; lossy: boolean; spillPath?: string } }; stderr: { readFrom(offset: number): { text: string; nextOffset: number; lossy: boolean; spillPath?: string } } } {
    const reader = (stream: 'stdout' | 'stderr') => ({
      readFrom: (offset: number) => this.remote.readStream(stream, offset, 256 * 1024),
    })
    return { stdout: reader('stdout'), stderr: reader('stderr') }
  }

  kill(): boolean {
    if (this.remote.state !== 'running') return false
    void this.remote.kill()
    return true
  }

  async result(): Promise<ShellRunResult> {
    return await this.resultPromise
  }

  private cleanup(): void {
    if (this.timer !== undefined) clearTimeout(this.timer)
    if (this.onAbort !== undefined && this.spec.signal !== undefined) {
      this.spec.signal.removeEventListener('abort', this.onAbort)
    }
    this.detach()
  }

  private buildResult(): ShellRunResult {
    const collected = (stream: 'stdout' | 'stderr'): CollectedOutput => {
      const read = this.remote.readStream(stream, 0, Number.MAX_SAFE_INTEGER)
      return {
        text: read.text,
        truncated: read.lossy,
        ...read.spillPath === undefined ? {} : { spillPath: read.spillPath },
      }
    }
    return {
      exitCode: this.remote.exitCode,
      signal: this.signal,
      timedOut: this.timedOut,
      aborted: this.aborted,
      timeoutMs: this.spec.timeoutMs,
      stdout: collected('stdout'),
      stderr: collected('stderr'),
      ...this.sandbox === undefined ? {} : { sandbox: this.sandbox },
    }
  }
}

/** The sandbox modes the harness defines; anything else on the wire is rejected. */
const SANDBOX_MODES: readonly string[] = ['read-only', 'workspace-write', 'danger-full-access']

/**
 * Narrow a wire value to a harness sandbox mode.
 * @param value - value reported by the runner.
 * @returns the mode, or undefined when the runner confines nothing (or reports an unknown mode).
 */
function toSandboxMode(value: string | undefined): SandboxMode | undefined {
  if (value === undefined) return undefined
  return (SANDBOX_MODES as readonly string[]).includes(value) ? (value as SandboxMode) : undefined
}

/**
 * Project a harness spec onto the wire.
 * @param spec - the resolved spec.
 * @returns the JSON-safe wire spec.
 */
function toWireSpec(spec: ShellExecSpec): ExecSpec {
  return {
    command: spec.command,
    workdir: spec.workdir,
    timeoutMs: spec.timeoutMs,
    onExpiry: spec.onExpiry,
    stdoutMaxBytes: spec.stdoutMaxBytes,
    ...spec.stdin === undefined ? {} : { stdin: spec.stdin },
    ...spec.env === undefined ? {} : { env: spec.env },
    ...spec.dshEnv === undefined ? {} : { dshEnv: spec.dshEnv as Record<string, string> },
    ...spec.sandboxPolicy === undefined ? {} : { sandboxPolicy: spec.sandboxPolicy as unknown as Record<string, unknown> },
  }
}
