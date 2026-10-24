/**
 * Adapter that turns the runner process's own shell executor into the backend
 * the registry drives.
 *
 * The runner mounts the stock local executor, so spawning, environment
 * scrubbing, sandbox policy and spill files behave exactly as they do in any
 * harness process — this file only translates one interface into the other.
 *
 * @module dsh-shellout-runner/dsh/backend
 */

import type { ShellExecutor, ShellExecRequest, ShellExecSpec } from '@deepseek-ai/dsh-shell'
import type { ExecSpec, ExecutionState, OutputRead, RunnerDescriptor, StreamName, StreamRead } from 'dsh-shellout-protocol'
import type { BackendExecution, ShellBackend } from '../core/types.js'

/** What the runner's executor reports about its own defaults. */
export interface BackendDescriptorInput extends RunnerDescriptor {}

/** Adapts the harness shell service to the runner's backend seam. */
export class HarnessShellBackend implements ShellBackend {
  /**
   * @param shell - the executor mounted in this runner process.
   * @param descriptor - defaults the host mirrors in its synchronous `resolve`.
   */
  constructor(
    private readonly shell: ShellExecutor,
    readonly descriptor: RunnerDescriptor,
  ) {}

  /**
   * Spawn one command through the local executor.
   * @param spec - fully resolved wire spec.
   * @param signal - cancellation owned by the caller.
   * @returns the live execution handle.
   * @throws when the local executor rejects preparation.
   */
  async execute(spec: ExecSpec, signal: AbortSignal): Promise<BackendExecution> {
    const request: ShellExecRequest = {
      command: spec.command,
      workdir: spec.workdir,
      timeoutMs: spec.timeoutMs,
      onExpiry: spec.onExpiry,
      stdoutMaxBytes: spec.stdoutMaxBytes,
      signal,
      ...spec.stdin === undefined ? {} : { stdin: spec.stdin },
      ...spec.env === undefined ? {} : { env: spec.env },
      ...spec.dshEnv === undefined ? {} : { dshEnv: spec.dshEnv as ShellExecSpec['dshEnv'] },
      ...spec.sandboxPolicy === undefined ? {} : { sandboxPolicy: spec.sandboxPolicy as unknown as ShellExecSpec['sandboxPolicy'] },
    }
    const resolved = this.shell.resolve(request)
    const execution = await this.shell.execute(resolved)
    return new HarnessBackendExecution(execution)
  }
}

/** One local execution exposed to the registry. */
class HarnessBackendExecution implements BackendExecution {
  /** @param execution - the handle returned by the local executor. */
  constructor(private readonly execution: Awaited<ReturnType<ShellExecutor['execute']>>) {}

  get done(): Promise<void> {
    return this.execution.done
  }

  readOutput(): OutputRead {
    return this.execution.readOutput()
  }

  readStream(stream: StreamName, offset: number, limit: number): StreamRead {
    const read = this.execution.observed[stream].readFrom(offset)
    const text = read.text.slice(0, limit)
    return {
      text,
      nextOffset: offset + text.length,
      lossy: read.lossy,
      ...read.spillPath === undefined ? {} : { spillPath: read.spillPath },
    }
  }

  kill(): boolean {
    return this.execution.kill()
  }

  state(): {
    readonly state: ExecutionState
    readonly exitCode: number | null
    readonly signal: string | null
    readonly sandbox?: Record<string, unknown>
  } {
    const sandbox = this.execution.sandbox
    return {
      state: toState(this.execution.status),
      exitCode: this.execution.exitCode,
      signal: this.execution.signal,
      ...sandbox === undefined ? {} : { sandbox: { ...sandbox } as unknown as Record<string, unknown> },
    }
  }
}

/**
 * Map the seam's process status onto the wire state.
 * @param status - the local executor's status.
 * @returns the wire state.
 */
function toState(status: 'running' | 'completed' | 'killed'): ExecutionState {
  return status === 'running' ? 'running' : status === 'completed' ? 'exited' : 'killed'
}
