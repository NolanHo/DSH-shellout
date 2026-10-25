/**
 * Host-side mirror of one runner execution.
 *
 * The harness shell seam reads output synchronously, so the host keeps a
 * bounded copy of each captured stream that a poller refreshes from the
 * runner. Consuming reads (`readOutput`) and offset reads (`readStream`) are
 * both served from that copy; a request below the retained window reports
 * `lossy`, which the seam already permits, rather than blocking on I/O.
 *
 * Offsets are UTF-8 **bytes**, the unit the runner seeks its capture files in.
 * The retained window keeps byte offsets and only decodes at the edges, so
 * non-ASCII output reconstructs exactly as captured.
 *
 * @module dsh-shellout/core/execution
 */

import type {
  ExecutionState,
  HeldExecution,
  OutputRead,
  SettledEvent,
  StreamName,
  StreamRead,
} from 'dsh-shellout-protocol'
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
  /** Byte offset of `text[0]` in the whole stream. */
  base: number
  /** Byte offset the runner resumes from: its last reported `nextOffset`. */
  end: number
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
      stdout: { text: '', base: 0, end: 0, lossy: false },
      stderr: { text: '', base: 0, end: 0, lossy: false },
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
   * @param offset - absolute byte offset to resume from.
   * @param limit - byte budget.
   * @returns the delta and resume facts; `lossy` when the offset left the window.
   */
  readStream(stream: StreamName, offset: number, limit: number): StreamRead {
    const buffer = this.buffers[stream]
    if (offset < buffer.base) {
      const window = this.decode(buffer, 0, Number.MAX_SAFE_INTEGER)
      return {
        text: window.text,
        nextOffset: buffer.base + window.bytes,
        lossy: true,
        ...buffer.spillPath === undefined ? {} : { spillPath: buffer.spillPath },
      }
    }
    const window = this.decode(buffer, offset - buffer.base, limit)
    return {
      text: window.text,
      nextOffset: offset + window.bytes,
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
   * Settle from the terminal facts a handshake reported.
   *
   * A settlement announced while this host was disconnected is never repeated,
   * so a reconnecting host takes the outcome from the runner's reply and lets
   * the poll finish the final drain before `done` resolves.
   * @param facts - one execution row from the handshake reply.
   */
  settleFromFacts(facts: HeldExecution): void {
    this.receive({
      executionId: facts.executionId,
      status: facts.state === 'exited' ? 'exited' : facts.state === 'lost' ? 'failed' : 'killed',
      exitCode: facts.exitCode ?? null,
      signal: facts.signal === undefined ? null : facts.signal as SettledEvent['signal'],
      ...facts.detail === undefined || facts.detail === null ? {} : { detail: facts.detail },
    })
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
    const start = Math.max(this.consumed[stream] - buffer.base, 0)
    const window = this.decode(buffer, start, Number.MAX_SAFE_INTEGER)
    this.consumed[stream] = buffer.base + start + window.bytes
    return {
      text: window.text,
      lossy: buffer.lossy,
      ...buffer.spillPath === undefined ? {} : { spillPath: buffer.spillPath },
    }
  }

  /**
   * Decode a byte range of the retained window, never splitting a character.
   * @param buffer - retained window of one stream.
   * @param start - bytes into the window.
   * @param limit - maximum bytes to return.
   * @returns the decoded text and how many bytes it consumed.
   */
  private decode(buffer: StreamBuffer, start: number, limit: number): { text: string; bytes: number } {
    const encoded = Buffer.from(buffer.text, 'utf8')
    const from = Math.max(0, Math.min(start, encoded.byteLength))
    const to = snapUtf8End(encoded, from, Math.max(from, Math.min(from + limit, encoded.byteLength)))
    return { text: encoded.subarray(from, to).toString('utf8'), bytes: to - from }
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
        const offset = buffer.end
        const read = await this.client.request<StreamRead>('readStream', {
          executionId: this.executionId,
          stream,
          offset,
          limit: this.options.chunkBytes,
        })
        if (read.spillPath !== undefined) buffer.spillPath = read.spillPath
        if (read.lossy) buffer.lossy = true
        // Only an empty read proves the stream is exhausted: `chunkBytes` is a
        // byte budget, while `text.length` counts UTF-16 code units, so a full
        // byte chunk of non-ASCII text is shorter decoded. A read that consumed
        // no bytes cannot make progress either.
        if (read.text.length === 0 || read.nextOffset <= offset) break
        this.append(stream, read.text, read.nextOffset)
      }
    }
  }

  /**
   * Append captured text to the retained window.
   * @param stream - stream the text belongs to.
   * @param text - decoded capture, a whole number of characters.
   * @param nextOffset - the runner's offset after this text, when it reported one.
   */
  private append(stream: StreamName, text: string, nextOffset?: number): void {
    const buffer = this.buffers[stream]
    buffer.text += text
    buffer.end = nextOffset ?? buffer.end + Buffer.byteLength(text, 'utf8')
    const excess = buffer.end - buffer.base - this.options.retainBytes
    if (excess <= 0) return
    const keepFrom = Math.max(this.consumed[stream] - buffer.base, 0)
    const cut = Math.max(excess, keepFrom)
    if (cut <= 0) return
    const encoded = Buffer.from(buffer.text, 'utf8')
    const dropped = snapUtf8End(encoded, 0, Math.min(cut, encoded.byteLength))
    buffer.text = encoded.subarray(dropped).toString('utf8')
    buffer.base += dropped
  }
}

/**
 * Largest character boundary at or below `end`.
 * @param bytes - UTF-8 bytes.
 * @param start - do not move below this offset.
 * @param end - requested exclusive end.
 * @returns an offset that does not split a UTF-8 sequence.
 */
function snapUtf8End(bytes: Buffer, start: number, end: number): number {
  let cut = end
  while (cut > start && cut < bytes.byteLength && (bytes[cut]! & 0xc0) === 0x80) cut -= 1
  return cut
}
