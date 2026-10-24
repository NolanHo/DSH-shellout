/**
 * The runner's view of a shell executor: everything the wire protocol serves,
 * with no dependency on the harness packages.
 *
 * The production adapter wraps the harness `ctx.shell` service; tests supply a
 * fake. Keeping this seam free of harness imports is what lets the registry and
 * server be tested without booting a harness.
 *
 * @module dsh-shellout-runner/core/types
 */

import type {
  ExecSpec,
  ExecutionState,
  OutputRead,
  RunnerDescriptor,
  StreamName,
  StreamRead,
} from 'dsh-shellout-protocol'

/** One execution as the backend exposes it. */
export interface BackendExecution {
  /** Resolves once the command settled; never rejects. */
  readonly done: Promise<void>
  /** Consuming delta over the captured streams. */
  readOutput(): OutputRead
  /**
   * Non-consuming offset read.
   * @param stream - captured stream to read.
   * @param offset - whole-stream byte offset to resume from.
   * @param limit - maximum bytes to return.
   * @returns the delta, the next offset, and the lossy/spill facts.
   */
  readStream(stream: StreamName, offset: number, limit: number): StreamRead
  /** Begin termination of the managed range; false when it already finished. */
  kill(): boolean
  /** Terminal facts at call time. */
  state(): {
    readonly state: ExecutionState
    readonly exitCode: number | null
    readonly signal: string | null
    readonly sandbox?: Record<string, unknown>
  }
}

/** The executor a runner server drives. */
export interface ShellBackend {
  /** Defaults and caps the host mirrors in its synchronous `resolve`. */
  readonly descriptor: RunnerDescriptor
  /**
   * Spawn one command.
   * @param spec - a fully resolved execution spec.
   * @param signal - cancellation owned by the caller.
   * @returns the live execution handle.
   * @throws when preparation fails.
   */
  execute(spec: ExecSpec, signal: AbortSignal): Promise<BackendExecution>
}
