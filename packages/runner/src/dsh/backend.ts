/**
 * The runner's execution backend: a thin adapter over the harness subprocess
 * service.
 *
 * A v2 spawn spec is already fully resolved — argv, environment, deadline
 * policy, capture budgets — so this file only creates the process, exposes its
 * captured streams, and reports its terminal facts. No shell semantics live
 * here, which is what lets the same contract be implemented by a Rust binary.
 *
 * @module dsh-shellout-runner/dsh/backend
 */

import type { SubprocessHandle, SubprocessRuntime } from '@deepseek-ai/dsh-subprocess'
import type { ExecutionState, OutputRead, RunnerDescriptor, SpawnSpec, StreamName, StreamRead } from 'dsh-shellout-protocol'
import type { BackendExecution, ShellBackend } from '../core/types.js'

/** Runs spawn specs through the harness subprocess service. */
export class LocalProcessBackend implements ShellBackend {
  /**
   * @param subprocess - the subprocess service mounted in this runner.
   * @param descriptor - defaults the host mirrors in its synchronous `resolve`.
   */
  constructor(
    private readonly subprocess: SubprocessRuntime,
    readonly descriptor: RunnerDescriptor,
  ) {}

  /**
   * Create one process from a resolved spawn spec.
   * @param spec - fully resolved argv, environment and budgets.
   * @param signal - cancellation owned by the caller.
   * @returns the live execution handle.
   * @throws when the subprocess service rejects the spawn synchronously.
   */
  async execute(spec: SpawnSpec, signal: AbortSignal): Promise<BackendExecution> {
    const deadline = spec.onExpiry === 'kill' ? AbortSignal.timeout(spec.timeoutMs) : undefined
    const cancel = deadline === undefined ? signal : AbortSignal.any([signal, deadline])
    const handle = this.subprocess.spawn({
      argv: spec.argv,
      cwd: spec.cwd,
      env: spec.env,
      stdio: {
        stdin: spec.stdin !== undefined ? { data: spec.stdin } : 'ignore',
        stdout: { maxBytes: spec.capture.stdoutMaxBytes, spill: { maxBytes: spec.capture.spillMaxBytes } },
        stderr: { maxBytes: spec.capture.stderrMaxBytes, spill: { maxBytes: spec.capture.spillMaxBytes } },
      },
      graceMs: spec.graceMs,
      signal: cancel,
    })
    return new ProcessExecution(handle, spec)
  }
}

/** One live process. */
class ProcessExecution implements BackendExecution {
  readonly done: Promise<void>
  private finished = false
  private killed = false
  private exitCodeValue: number | null = null
  private signalValue: string | null = null
  private readonly stdoutOffset = { value: 0 }
  private readonly stderrOffset = { value: 0 }

  /**
   * @param handle - the spawned process.
   * @param spec - the spec it runs under, for capture facts.
   */
  constructor(
    private readonly handle: SubprocessHandle,
    private readonly spec: SpawnSpec,
  ) {
    this.done = handle.done.then(
      (outcome) => {
        this.finished = true
        this.exitCodeValue = outcome.exitCode
        this.signalValue = outcome.signal
      },
      () => {
        this.finished = true
        this.killed = true
      },
    )
  }

  readOutput(): OutputRead {
    const stdout = this.handle.collected.stdout
    const stderr = this.handle.collected.stderr
    const out = stdout === undefined ? { text: '', lossy: false } : stdout.readFrom(this.stdoutOffset.value)
    const err = stderr === undefined ? { text: '', lossy: false } : stderr.readFrom(this.stderrOffset.value)
    this.stdoutOffset.value = 'nextOffset' in out ? out.nextOffset : 0
    this.stderrOffset.value = 'nextOffset' in err ? err.nextOffset : 0
    const outText = out.text
    const errText = err.text
    const separator = outText.length > 0 && !outText.endsWith('\n') ? '\n' : ''
    return {
      delta: outText + (errText.length > 0 ? `${separator}[stderr]\n${errText}` : ''),
      lossy: out.lossy || err.lossy,
      ...'spillPath' in out && out.spillPath !== undefined ? { stdoutSpillPath: out.spillPath } : {},
      ...'spillPath' in err && err.spillPath !== undefined ? { stderrSpillPath: err.spillPath } : {},
    }
  }

  readStream(stream: StreamName, offset: number, limit: number): StreamRead {
    const reader = stream === 'stdout' ? this.handle.collected.stdout : this.handle.collected.stderr
    if (reader === undefined) return { text: '', nextOffset: offset, lossy: false }
    const read = reader.readFrom(offset)
    const text = read.text.slice(0, limit)
    return {
      text,
      nextOffset: offset + text.length,
      lossy: read.lossy,
      ...read.spillPath === undefined ? {} : { spillPath: read.spillPath },
    }
  }

  kill(): boolean {
    if (this.finished) return false
    this.killed = true
    this.handle.terminate()
    return true
  }

  state(): {
    readonly state: ExecutionState
    readonly exitCode: number | null
    readonly signal: string | null
  } {
    return {
      state: this.finished ? (this.killed ? 'killed' : 'exited') : 'running',
      exitCode: this.exitCodeValue,
      signal: this.signalValue,
    }
  }

  /** The spec this execution runs under; kept for diagnostics. */
  get spawnSpec(): SpawnSpec {
    return this.spec
  }
}
