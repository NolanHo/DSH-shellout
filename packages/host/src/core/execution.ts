/**
 * Host-side mirror of one runner execution.
 *
 * The harness shell seam reads output synchronously, so the host keeps a
 * bounded copy of each captured stream that a poller refreshes from the
 * runner. Consuming reads (`readOutput`) and offset reads (`readStream`) are
 * both served from that copy; a request below the retained window reports
 * `lossy`, which the seam already permits, rather than blocking on I/O.
 *
 * @module dsh-shellout/core/execution
 */

import type { ExecutionState, OutputRead, SettledEvent, StreamName, StreamRead } from 'dsh-shellout-protocol'
import type { ShelloutClient } from './client.js'

/** Mirror bounds. */
export interface RemoteExecutionOptions {
  /** Poll cadence for captured output. */
  readonly pollMs?: number
  /** Bytes retained per stream. */
  readonly retainBytes?: number
  /** Bytes requested per `readStream` reply. */
  readonly chunkBytes?: number
}

interface StreamBuffer {
  text: string
  /** Absolute offset of `text[0]`. */
  base: number
  /** Whether the runner reported loss for this stream at least once. */
  lossy: boolean
  /** Complete-stream file the runner keeps, when it keeps one. */
  spillPath?: string
}

const STDOUT_OR_DEFAULT = 0

/** One execution as the host sees it. */
export class RemoteExecution {
  /** Resolves once the execution settled and its final output was drained. */
  readonly done: Promise<void>
  private readonly buffers: Record<StreamName, StreamBuffer>
  private readonly consumed: Record<StreamName, number> = { stdout: STDOUT_OR_DEFAULT, stderr: STDOUT_OR_DEFAULT }
  private readonly options: Required<RemoteExecutionOptions>
  private resolveDone!: () => void
  private timer: NodeJS.Timeout | undefined
  private pollInFlight = false
  private stopped = false
  private settledEvent: SettledEvent | undefined
  private stateValue: ExecutionState = 'running'
  private exitCodeValue: number | null = null
  private signalValue: string | null = null
  private sandboxValue: Record<string, unknown> | undefined

  /** @param client - connected runner client. @param executionId - execution to mirror. @param options - bounds. */
  constructor(
    private readonly client: ShelloutClient,
    readonly executionId: string,
    options: RemoteExecutionOptions = {},
  ) {
    this.options = {
      pollMs: options.pollMs ?? 150,
      retainBytes: options.retainBytes ?? 1_048_576,
      chunkBytes: options.chunkBytes ?? 256 * 1024,
    }
    this.buffers = {
      stdout: { text: '', base: 0, lossy: false },
      stderr: { text: '', base: 0, lossy: false },
    }
    this.done = new Promise<void>((resolve) => { this.resolveDone = resolve })
  }

  /** Current lifecycle state. */
  get state(): ExecutionState {
    return this.stateValue
  }

  /** Exit code once settled, else null. */
  get exitCode(): number | null {
    return this.exitCodeValue
  }

  /** Terminating signal once settled, else null. */
  get signal(): string | null {
    return this.signalValue
  }

  /** Sandbox facts reported at settlement, when the runner confines. */
  get sandbox(): Record<string, unknown> | undefined {
    return this.sandboxValue
  }

  /** Terminal facts once settled. */
  get settled(): SettledEvent | undefined {
    return this.settledEvent
  }

  /** Begin polling. */
  start(): void {
    this.schedule(0)
  }

  /** Stop polling without settling. */
  stop(): void {
    this.stopped = true
    if (this.timer !== undefined) clearTimeout(this.timer)
    this.timer = undefined
  }

  /**
   * Consuming read over the mirrored streams, formatted like the local executor.
   * @returns the delta since the previous consuming read.
   */
  readOutput(): OutputRead {
    const out = this.slice('stdout')
    const err = this.slice('stderr')
    const separator = out.text.length > 0 && !out.text.endsWith('\n') ? '\n' : ''
    const delta = out.text + (err.text.length > 0 ? `${separator}[stderr]\n${err.text}` : '')
    return {
      delta,
      lossy: out.lossy || err.lossy,
      ...out.spillPath === undefined ? {} : { stdoutSpillPath: out.spillPath },
      ...err.spillPath === undefined ? {} : { stderrSpillPath: err.spillPath },
    }
  }

  /**
   * Non-consuming offset read over one mirrored stream.
   * @param stream - stream to read.
   * @param offset - absolute offset to resume from.
   * @param limit - byte budget.
   * @returns the delta and resume facts; `lossy` when the offset left the window.
   */
  readStream(stream: StreamName, offset: number, limit: number): StreamRead {
    const buffer = this.buffers[stream]
    if (offset < buffer.base) {
      return {
        text: buffer.text,
        nextOffset: buffer.base + buffer.text.length,
        lossy: true,
        ...buffer.spillPath === undefined ? {} : { spillPath: buffer.spillPath },
      }
    }
    const start = offset - buffer.base
    const text = buffer.text.slice(start, start + limit)
    return {
      text,
      nextOffset: offset + text.length,
      lossy: buffer.lossy,
      ...buffer.spillPath === undefined ? {} : { spillPath: buffer.spillPath },
    }
  }

  /**
   * Request termination.
   * @returns whether the runner accepted the request.
   */
  async kill(): Promise<boolean> {
    if (this.stateValue !== 'running') return false
    try {
      const result = await this.client.request<{ requested: boolean }>('kill', { executionId: this.executionId })
      return result.requested
    } catch {
      return false
    }
  }

  /**
   * Settle this execution because the runner is gone and will never report it.
   * @param note - one line appended to stderr, matching the seam's provider-failure shape.
   */
  abortLost(note: string): void {
    if (this.stateValue !== 'running') return
    this.append('stderr', this.buffers.stderr.text.endsWith('\n') || this.buffers.stderr.text === '' ? `${note}\n` : `\n${note}\n`)
    this.stateValue = 'killed'
    this.exitCodeValue = null
    this.signalValue = null
    this.stopped = true
    if (this.timer !== undefined) clearTimeout(this.timer)
    this.timer = undefined
    this.resolveDone()
  }

  /**
   * Record a settlement event pushed by the runner.
   * @param event - the event payload.
   */
  receive(event: SettledEvent): void {
    if (this.settledEvent !== undefined) return
    this.settledEvent = event
  }

  /**
   * Map a settlement status onto the seam's process status.
   * @returns `completed` for an ordinary exit, `killed` otherwise.
   */
  get processStatus(): 'running' | 'completed' | 'killed' {
    if (this.stateValue === 'running') return 'running'
    return this.stateValue === 'exited' ? 'completed' : 'killed'
  }

  private slice(stream: StreamName): { text: string; lossy: boolean; spillPath?: string } {
    const buffer = this.buffers[stream]
    const start = this.consumed[stream] - buffer.base
    const text = start <= 0 ? buffer.text : buffer.text.slice(start)
    this.consumed[stream] = buffer.base + buffer.text.length
    return {
      text,
      lossy: buffer.lossy,
      ...buffer.spillPath === undefined ? {} : { spillPath: buffer.spillPath },
    }
  }

  private schedule(delayMs: number): void {
    if (this.stopped) return
    this.timer = setTimeout(() => { void this.poll() }, delayMs)
    this.timer.unref()
  }

  private async poll(): Promise<void> {
    if (this.stopped || this.pollInFlight) return
    this.pollInFlight = true
    try {
      await this.drainStreams()
      if (this.settledEvent !== undefined) {
        // The runner announces settlement only after the final bytes are readable.
        await this.drainStreams()
        const event = this.settledEvent
        this.stateValue = event.status === 'exited' ? 'exited' : event.status === 'failed' ? 'lost' : 'killed'
        this.exitCodeValue = event.exitCode
        this.signalValue = event.signal
        if (event.sandbox !== undefined) this.sandboxValue = event.sandbox
        this.stopped = true
        this.resolveDone()
        return
      }
      this.schedule(this.options.pollMs)
    } catch {
      // A transport failure resolves through the client's close path; keep polling.
      this.schedule(this.options.pollMs)
    } finally {
      this.pollInFlight = false
    }
  }

  private async drainStreams(): Promise<void> {
    for (const stream of ['stdout', 'stderr'] as const) {
      for (;;) {
        const buffer = this.buffers[stream]
        const highWater = buffer.base + buffer.text.length
        const read = await this.client.request<StreamRead>('readStream', {
          executionId: this.executionId,
          stream,
          offset: highWater,
          limit: this.options.chunkBytes,
        })
        if (read.spillPath !== undefined) buffer.spillPath = read.spillPath
        if (read.lossy) buffer.lossy = true
        if (read.text.length === 0) break
        this.append(stream, read.text)
        if (read.text.length < this.options.chunkBytes) break
      }
    }
  }

  private append(stream: StreamName, text: string): void {
    const buffer = this.buffers[stream]
    buffer.text += text
    const excess = buffer.text.length - this.options.retainBytes
    if (excess > 0) {
      const keepFrom = Math.max(this.consumed[stream] - buffer.base, 0)
      const cut = Math.max(excess, keepFrom)
      buffer.text = buffer.text.slice(cut)
      buffer.base += cut
    }
  }
}
