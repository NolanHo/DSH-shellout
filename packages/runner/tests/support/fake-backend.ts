/**
 * A scripted shell backend for tests: every execution is a controllable buffer
 * pair plus a settlement trigger, so registry and server behaviour can be
 * asserted without spawning processes.
 */

import type {
  ExecSpec,
  ExecutionState,
  OutputRead,
  RunnerDescriptor,
  StreamName,
  StreamRead,
} from 'dsh-shellout-protocol'
import type { BackendExecution, ShellBackend } from '../../src/core/types.ts'

/** One scripted execution. */
export class FakeExecution implements BackendExecution {
  readonly done: Promise<void>
  private finish!: () => void
  private readonly buffers: Record<StreamName, string> = { stdout: '', stderr: '' }
  private consumed = 0
  private finished = false
  private killed = false

  /** @param onKill - invoked when the runner asks the execution to stop. */
  constructor(private readonly onKill?: () => void) {
    this.done = new Promise<void>((resolve) => { this.finish = resolve })
  }

  /** Append captured output. @param stream - target stream. @param text - bytes to append. */
  append(stream: StreamName, text: string): void {
    this.buffers[stream] += text
  }

  /** Complete the execution. @param exitCode - exit status. */
  complete(exitCode: number | null = 0): void {
    if (this.finished) return
    this.finished = true
    this.exitCodeValue = exitCode
    this.finish()
  }

  /** Abort through a signal. */
  abort(): void {
    if (this.finished) return
    this.finished = true
    this.killed = true
    this.finish()
  }

  readOutput(): OutputRead {
    const delta = this.buffers.stdout.slice(this.consumed)
    this.consumed = this.buffers.stdout.length
    return { delta, lossy: false }
  }

  readStream(stream: StreamName, offset: number, limit: number): StreamRead {
    const buffer = this.buffers[stream]
    const text = buffer.slice(offset, offset + limit)
    return { text, nextOffset: offset + text.length, lossy: offset > buffer.length }
  }

  kill(): boolean {
    if (this.finished) return false
    this.onKill?.()
    this.abort()
    return true
  }

  state(): { state: ExecutionState; exitCode: number | null; signal: string | null } {
    if (!this.finished) return { state: 'running', exitCode: null, signal: null }
    return {
      state: this.killed ? 'killed' : 'exited',
      exitCode: this.killed ? null : this.exitCodeValue,
      signal: this.killed ? 'SIGTERM' : null,
    }
  }

  private exitCodeValue: number | null = 0
}

/** A backend that records specs and hands out scripted executions. */
export class FakeBackend implements ShellBackend {
  readonly executions: FakeExecution[] = []
  readonly specs: ExecSpec[] = []
  descriptor: RunnerDescriptor = {
    cwd: '/tmp',
    timeoutMs: 60_000,
    maxTimeoutMs: 600_000,
    maxOutputBytes: 1_048_576,
  }

  /** Fail the next `execute` call with this error. */
  failNext: Error | undefined

  async execute(spec: ExecSpec): Promise<BackendExecution> {
    if (this.failNext !== undefined) {
      const failure = this.failNext
      this.failNext = undefined
      throw failure
    }
    this.specs.push(spec)
    const execution = new FakeExecution()
    this.executions.push(execution)
    return execution
  }
}
