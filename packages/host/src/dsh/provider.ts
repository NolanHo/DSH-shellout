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
import { tmpdir } from 'node:os'
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
import { ENV_OVERRIDES } from '@deepseek-ai/dsh-bash-local'
import { scrubbedParentEnv } from '@deepseek-ai/dsh-subprocess'
import { specDigest, type SpawnSpec } from 'dsh-shellout-protocol'
import { ShelloutClient } from '../core/client.js'
import type { TransportOptions } from '../core/transport.js'
import { RemoteExecution } from '../core/execution.js'

/** Local executor settings used by the fallback path. */
export interface LocalExecutorConfig {
  /** Default working directory for commands. */
  cwd?: string
  /** Default foreground timeout in milliseconds. */
  timeoutMs?: number
  /** Upper bound on a per-call timeout. */
  maxTimeoutMs?: number
  /** Per-stream in-memory output cap. */
  maxOutputBytes?: number
  /** Per-stream spill-file cap. */
  maxSpillBytes?: number
  /** Termination grace period. */
  graceMs?: number
}

/** A host reached over ssh instead of the local socket. */
export interface RemoteTarget {
  /** ssh host alias or `user@host`. */
  readonly host: string
  /** Runner binary used to attach; defaults to `dsh-shellout-runner` on `PATH`. */
  readonly runner?: string
  /** ssh argv, with `{host}` substituted where present; defaults to `ssh -o BatchMode=yes`. */
  readonly ssh?: readonly string[]
  /** Remote state root; defaults to `$HOME/.dsh-shellout` on the host. */
  readonly remoteRoot?: string
}

/** Plugin configuration; every field is optional. */
export interface ShelloutConfig {
  /** Runner socket path; defaults to `$DSH_HOME/shellout/control.sock`. */
  socketPath?: string
  /**
   * Attach to a remote host instead of the local socket. The runner bootstraps
   * the host and speaks the same protocol over the ssh command's stdio.
   */
  remote?: RemoteTarget
  /** Bound on one startup handshake attempt. */
  connectTimeoutMs?: number
  /** How long to keep retrying the runner at startup before deciding. */
  connectRetryMs?: number
  /** Delay before reconnecting after a mid-run connection loss; `0` disables reconnect. */
  reconnectDelayMs?: number
  /**
   * How long a lost runner connection may stay down before its live executions
   * are settled as lost. A reconnect inside the window keeps them running.
   */
  disconnectGraceMs?: number
  /** Output poll cadence, in milliseconds. */
  mirrorPollMs?: number
  /** Bytes retained per stream for synchronous reads. */
  mirrorRetainBytes?: number
  /** Boot-lease ping cadence. */
  heartbeatMs?: number
  /**
   * What to do when the runner never answers at startup: `local` keeps the
   * agent's shell by running commands in this process, `none` refuses to load.
   */
  fallback?: 'local' | 'none'
  /** Settings for the fallback executor. */
  local?: LocalExecutorConfig
}

/** Configuration after defaulting. */
/** Signal numbers a runner may report, mapped to the names the seam declares. */
const SIGNAL_NAMES: Record<number, NodeJS.Signals> = {
  1: 'SIGHUP', 2: 'SIGINT', 3: 'SIGQUIT', 4: 'SIGILL', 5: 'SIGTRAP', 6: 'SIGABRT',
  7: 'SIGBUS', 8: 'SIGFPE', 9: 'SIGKILL', 10: 'SIGUSR1', 11: 'SIGSEGV', 12: 'SIGUSR2',
  13: 'SIGPIPE', 14: 'SIGALRM', 15: 'SIGTERM', 16: 'SIGSTKFLT', 17: 'SIGCHLD',
  18: 'SIGCONT', 19: 'SIGSTOP', 20: 'SIGTSTP', 21: 'SIGTTIN', 22: 'SIGTTOU',
  23: 'SIGURG', 24: 'SIGXCPU', 25: 'SIGXFSZ', 26: 'SIGVTALRM', 27: 'SIGPROF',
  28: 'SIGWINCH', 29: 'SIGIO', 30: 'SIGPWR', 31: 'SIGSYS',
}

/**
 * Normalize a reported terminating signal to the seam's name form.
 * @param value - name, number, or null as reported by the runner.
 * @returns the signal name, or null when none was reported.
 */
function signalName(value: string | number | null | undefined): NodeJS.Signals | null {
  if (value === null || value === undefined) return null
  if (typeof value === 'string') return value as NodeJS.Signals
  return SIGNAL_NAMES[value] ?? null
}

export interface ResolvedShelloutConfig {
  readonly socketPath: string
  /** How the client reaches the runner. */
  readonly transport: TransportOptions
  readonly connectTimeoutMs: number
  readonly connectRetryMs: number
  readonly reconnectDelayMs: number
  readonly disconnectGraceMs: number
  readonly mirrorPollMs: number
  readonly mirrorRetainBytes: number
  readonly heartbeatMs: number
  readonly fallback: 'local' | 'none'
  readonly local: Omit<Required<LocalExecutorConfig>, 'cwd'> & { readonly cwd?: string }
}

/**
 * Choose the transport a configuration asks for.
 * @param config - caller configuration.
 * @param socketPath - the local socket path to use when no remote is named.
 * @returns the transport the client opens.
 */
function transportFor(config: ShelloutConfig, socketPath: string): TransportOptions {
  const remote = config.remote
  if (remote === undefined) return { kind: 'socket', path: socketPath }
  const argv = [remote.runner ?? 'dsh-shellout-runner', '--mode', 'attach', '--host', remote.host]
  for (const word of remote.ssh ?? []) argv.push('--ssh', word)
  if (remote.remoteRoot !== undefined) argv.push('--remote-root', remote.remoteRoot)
  return { kind: 'command', argv }
}

/**
 * Apply defaults to a plugin configuration.
 * @param config - caller configuration, possibly partial.
 * @param env - environment to read the harness home from.
 * @returns the fully specified configuration.
 */
export function resolveConfig(config: ShelloutConfig = {}, env: NodeJS.ProcessEnv = process.env): ResolvedShelloutConfig {
  const home = env.DSH_HOME ?? join(env.HOME ?? '/root', '.dsh')
  const socketPath = config.socketPath ?? join(home, 'shellout', 'control.sock')
  return {
    socketPath,
    transport: transportFor(config, socketPath),
    connectTimeoutMs: config.connectTimeoutMs ?? 5_000,
    connectRetryMs: config.connectRetryMs ?? 10_000,
    reconnectDelayMs: config.reconnectDelayMs ?? 2_000,
    disconnectGraceMs: config.disconnectGraceMs ?? 10_000,
    mirrorPollMs: config.mirrorPollMs ?? 150,
    mirrorRetainBytes: config.mirrorRetainBytes ?? 1_048_576,
    heartbeatMs: config.heartbeatMs ?? 5_000,
    fallback: config.fallback ?? 'local',
    local: {
      ...config.local?.cwd === undefined ? {} : { cwd: config.local.cwd },
      timeoutMs: config.local?.timeoutMs ?? 120_000,
      maxTimeoutMs: config.local?.maxTimeoutMs ?? 600_000,
      maxOutputBytes: config.local?.maxOutputBytes ?? 64_000,
      maxSpillBytes: config.local?.maxSpillBytes ?? 67_108_864,
      graceMs: config.local?.graceMs ?? 3_000,
    },
  }
}

/** The host-side executor backed by a runner process. */
export class ShelloutExecutor extends ShellExecutor {
  private readonly bootId = randomUUID()
  private readonly live = new Set<RemoteExecution>()
  private readonly detachClose: () => void
  private readonly detachDisconnect: () => void
  private readonly detachReconnect: () => void
  private lossTimer: NodeJS.Timeout | undefined

  /**
   * @param ctx - owning context.
   * @param config - resolved plugin configuration.
   * @param client - connected runner client.
   */
  constructor(ctx: Context, private readonly config: ResolvedShelloutConfig, private readonly client: ShelloutClient) {
    super(ctx)
    this.detachClose = client.onClose(() => { this.settleAllLost() })
    this.detachDisconnect = client.onDisconnect(() => { this.beginLossGrace() })
    this.detachReconnect = client.onReconnect(() => { this.recover() })
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
    const wireSpec = toSpawnSpec(spec, this.config)
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
    this.detachDisconnect()
    this.detachReconnect()
    if (this.lossTimer !== undefined) clearTimeout(this.lossTimer)
    this.client.close()
  }

  /** Start the window in which a reconnect still saves the live executions. */
  private beginLossGrace(): void {
    if (this.lossTimer !== undefined) return
    this.lossTimer = setTimeout(() => {
      this.lossTimer = undefined
      this.settleAllLost()
    }, this.config.disconnectGraceMs)
    this.lossTimer.unref()
  }

  /** A reconnect inside the grace window: keep what the runner still holds. */
  private recover(): void {
    if (this.lossTimer !== undefined) {
      clearTimeout(this.lossTimer)
      this.lossTimer = undefined
    }
    const held = new Map(this.client.executions.map(entry => [entry.executionId, entry]))
    for (const execution of [...this.live]) {
      const facts = held.get(execution.executionId)
      if (facts === undefined) {
        execution.abortLost('shellout: runner lost')
        this.live.delete(execution)
        continue
      }
      // A settlement announced while this host was away is never repeated:
      // the handshake reports how the execution ended instead.
      if (facts.state !== 'running') execution.settleFromFacts(facts)
    }
  }

  /** Settle every live execution because the runner is gone for good. */
  private settleAllLost(): void {
    if (this.lossTimer !== undefined) {
      clearTimeout(this.lossTimer)
      this.lossTimer = undefined
    }
    for (const execution of this.live) execution.abortLost('shellout: runner lost')
    this.live.clear()
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
    // The seam declares a signal name, but an executor may report the raw
    // number; a number reaching the tool schema fails its string branch.
    return signalName(this.remote.signal)
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
        // A runner from an older or newer release may send null here; the
        // contract is an optional string, so treat null as absent.
        ...read.spillPath == null ? {} : { spillPath: read.spillPath },
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
 * Resolve a harness spec into the runner's spawn vocabulary.
 *
 * Everything policy-shaped is decided here — argv, the fully merged child
 * environment, capture budgets — so the runner only creates, supervises and
 * captures a process. The command runs through `bash -c`, exactly as the local
 * executor would run it.
 *
 * @param spec - the resolved harness spec.
 * @param config - resolved plugin configuration (local budgets and grace).
 * @returns the wire spawn spec.
 */
function toSpawnSpec(spec: ShellExecSpec, config: ResolvedShelloutConfig): SpawnSpec {
  const env: Record<string, string> = {}
  for (const [key, value] of Object.entries({
    ...scrubbedParentEnv(),
    ...ENV_OVERRIDES,
    ...spec.env,
    ...spec.dshEnv as Record<string, string> | undefined,
  })) {
    if (value !== undefined) env[key] = value
  }
  return {
    argv: ['bash', '-c', spec.command],
    cwd: spec.workdir,
    env,
    ...spec.stdin === undefined ? {} : { stdin: spec.stdin },
    timeoutMs: spec.timeoutMs,
    onExpiry: spec.onExpiry,
    graceMs: config.local.graceMs,
    capture: {
      stdoutMaxBytes: spec.stdoutMaxBytes,
      stderrMaxBytes: config.local.maxOutputBytes,
      spillMaxBytes: config.local.maxSpillBytes,
      spillDir: tmpdir(),
    },
  }
}
