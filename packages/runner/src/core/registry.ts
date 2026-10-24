/**
 * Execution registry: the runner's ownership table.
 *
 * Holds one record per execution, enforces the concurrency cap, makes a
 * retried `exec` idempotent, tracks the boot lease that keeps a host's
 * executions alive, and reports settlement exactly once.
 *
 * @module dsh-shellout-runner/core/registry
 */

import type {
  ExecRequest,
  ExecutionState,
  OutputRead,
  SettledEvent,
  StateResponse,
  StreamName,
  StreamRead,
  WireErrorCode,
} from 'dsh-shellout-protocol'
import type { BackendExecution, ShellBackend } from './types.js'

/** A failure the server maps onto an `err` frame. */
export class RunnerError extends Error {
  /** @param code - wire error code. @param message - actionable detail. */
  constructor(readonly code: WireErrorCode, message: string) {
    super(message)
    this.name = 'RunnerError'
  }
}

/** Registry bounds; every value has a production default. */
export interface RegistryOptions {
  /** How long a boot survives without a ping before its executions are terminated. */
  readonly leaseTtlMs?: number
  /** Maximum concurrently live executions. */
  readonly maxConcurrent?: number
  /** How long a settled record stays readable. */
  readonly settleRetentionMs?: number
  /** Lease sweep cadence. */
  readonly sweepIntervalMs?: number
}

interface Record {
  readonly executionId: string
  readonly operationId: string
  readonly digest: string
  readonly bootId: string
  readonly execution: BackendExecution
  readonly abort: AbortController
  settled: boolean
  terminal?: SettledEvent
}

const DEFAULTS = {
  leaseTtlMs: 60_000,
  maxConcurrent: 64,
  settleRetentionMs: 5 * 60_000,
  sweepIntervalMs: 1_000,
} as const

/** One boot's lease state. */
interface Boot {
  deadline: number
}

/** The runner's execution table. */
export class ExecutionRegistry {
  private readonly records = new Map<string, Record>()
  private readonly byOperation = new Map<string, string>()
  private readonly boots = new Map<string, Boot>()
  private readonly listeners = new Set<(event: SettledEvent) => void>()
  private readonly leaseTtlMs: number
  private readonly maxConcurrent: number
  private readonly settleRetentionMs: number
  private readonly sweepTimer: NodeJS.Timeout
  private disposed = false

  /** @param backend - the executor this runner serves. @param options - bounds. */
  constructor(private readonly backend: ShellBackend, options: RegistryOptions = {}) {
    this.leaseTtlMs = options.leaseTtlMs ?? DEFAULTS.leaseTtlMs
    this.maxConcurrent = options.maxConcurrent ?? DEFAULTS.maxConcurrent
    this.settleRetentionMs = options.settleRetentionMs ?? DEFAULTS.settleRetentionMs
    const interval = options.sweepIntervalMs ?? DEFAULTS.sweepIntervalMs
    this.sweepTimer = setInterval(() => { this.sweep() }, interval)
    this.sweepTimer.unref()
  }

  /**
   * Subscribe to settlement events.
   * @param listener - called once per execution, after its final output is readable.
   * @returns a disposer removing the listener.
   */
  onSettled(listener: (event: SettledEvent) => void): () => void {
    this.listeners.add(listener)
    return () => { this.listeners.delete(listener) }
  }

  /**
   * Start or refresh one boot's lease and report its live executions.
   * @param bootId - the host's boot identity.
   * @returns every execution this boot still owns.
   */
  attach(bootId: string): { readonly executionId: string; readonly state: ExecutionState }[] {
    this.boots.set(bootId, { deadline: Date.now() + this.leaseTtlMs })
    const held: { executionId: string; state: ExecutionState }[] = []
    for (const record of this.records.values()) {
      if (record.bootId !== bootId) continue
      held.push({ executionId: record.executionId, state: record.execution.state().state })
    }
    return held
  }

  /**
   * Refresh a boot's lease.
   * @param bootId - the host's boot identity.
   * @throws RunnerError when the boot is unknown (the host must re-handshake).
   */
  ping(bootId: string): void {
    const boot = this.boots.get(bootId)
    if (boot === undefined) throw new RunnerError('protocol', `unknown boot ${bootId}; reconnect with hello`)
    boot.deadline = Date.now() + this.leaseTtlMs
  }

  /**
   * Start one execution, or return the existing one for a retried operation.
   * @param request - execution identity, spec, and digest.
   * @param bootId - the boot that owns the execution.
   * @returns the execution id.
   * @throws RunnerError on a digest conflict, concurrency overflow, or backend failure.
   */
  async exec(request: ExecRequest, bootId: string): Promise<{ executionId: string }> {
    const existingId = this.byOperation.get(request.operationId)
    if (existingId !== undefined) {
      const existing = this.records.get(existingId)
      if (existing === undefined) throw new RunnerError('internal', `operation ${request.operationId} lost its record`)
      if (existing.digest !== request.specDigest) {
        throw new RunnerError('rejected', 'operation id reused with a different spec')
      }
      return { executionId: existing.executionId }
    }
    if (this.liveCount() >= this.maxConcurrent) {
      throw new RunnerError('busy', `runner is at its ${String(this.maxConcurrent)}-execution limit`)
    }
    const abort = new AbortController()
    let execution: BackendExecution
    try {
      execution = await this.backend.execute(request.spec, abort.signal)
    } catch (error: unknown) {
      throw new RunnerError('rejected', errorMessage(error))
    }
    const record: Record = {
      executionId: request.executionId,
      operationId: request.operationId,
      digest: request.specDigest,
      bootId,
      execution,
      abort,
      settled: false,
    }
    this.records.set(record.executionId, record)
    this.byOperation.set(record.operationId, record.executionId)
    void execution.done.then(() => { this.settle(record, false) }, () => { this.settle(record, false) })
    return { executionId: record.executionId }
  }

  /**
   * Consuming read of one execution's captured output.
   * @param executionId - execution to read.
   * @returns the delta since the previous consuming read.
   */
  readOutput(executionId: string): OutputRead {
    return this.require(executionId).execution.readOutput()
  }

  /**
   * Non-consuming offset read of one captured stream.
   * @param executionId - execution to read.
   * @param stream - captured stream name.
   * @param offset - resume offset.
   * @param limit - byte budget.
   * @returns the delta and resume facts.
   */
  readStream(executionId: string, stream: StreamName, offset: number, limit: number): StreamRead {
    return this.require(executionId).execution.readStream(stream, offset, limit)
  }

  /**
   * Request termination.
   * @param executionId - execution to terminate.
   * @returns whether a live execution accepted the request.
   */
  kill(executionId: string): { requested: boolean } {
    const record = this.require(executionId)
    if (record.settled) return { requested: false }
    return { requested: record.execution.kill() }
  }

  /**
   * Terminal facts at call time.
   * @param executionId - execution to inspect.
   * @returns the current state and exit facts.
   */
  state(executionId: string): StateResponse {
    const record = this.require(executionId)
    const current = record.execution.state()
    return {
      state: current.state,
      exitCode: current.exitCode,
      signal: current.signal,
      ...current.sandbox === undefined ? {} : { sandbox: current.sandbox },
    }
  }

  /**
   * Stop admitting executions, terminate live ones, and wait for settlement.
   * @param timeoutMs - bound on waiting for settlements.
   * @returns how many executions were still live when the wait ended.
   */
  async drain(timeoutMs: number): Promise<{ live: number }> {
    this.draining = true
    const live = () => [...this.records.values()].filter(record => !record.settled)
    for (const record of live()) record.execution.kill()
    const deadline = Date.now() + timeoutMs
    while (live().length > 0 && Date.now() < deadline) {
      await new Promise<void>(resolve => { setTimeout(resolve, 25) })
    }
    return { live: live().length }
  }

  /** Terminate every execution and stop the sweep timer. */
  dispose(): void {
    if (this.disposed) return
    this.disposed = true
    clearInterval(this.sweepTimer)
    for (const record of this.records.values()) {
      if (!record.settled) record.execution.kill()
    }
    this.listeners.clear()
  }

  private draining = false

  private liveCount(): number {
    let count = 0
    for (const record of this.records.values()) if (!record.settled) count += 1
    return count
  }

  private require(executionId: string): Record {
    const record = this.records.get(executionId)
    if (record === undefined) throw new RunnerError('unknown-execution', `no execution ${executionId}`)
    return record
  }

  /**
   * Record terminal facts once and announce them.
   * @param record - the settled record.
   * @param lost - true when the runner is ending the execution itself.
   */
  private settle(record: Record, lost: boolean): void {
    if (record.settled) return
    record.settled = true
    const current = record.execution.state()
    const event: SettledEvent = {
      executionId: record.executionId,
      status: lost ? 'failed' : current.state === 'exited' ? 'exited' : 'killed',
      exitCode: current.exitCode,
      signal: current.signal,
      ...current.sandbox === undefined ? {} : { sandbox: current.sandbox },
      ...lost ? { detail: 'lost: owning boot no longer holds a lease' } : {},
    }
    record.terminal = event
    for (const listener of this.listeners) listener(event)
    setTimeout(() => { this.forget(record.executionId) }, this.settleRetentionMs).unref()
  }

  /** Drop a settled record once its retention window closed. */
  private forget(executionId: string): void {
    const record = this.records.get(executionId)
    if (record === undefined || !record.settled) return
    this.records.delete(executionId)
    this.byOperation.delete(record.operationId)
  }

  /** Terminate executions whose owning boot let its lease expire. */
  private sweep(): void {
    const now = Date.now()
    for (const [bootId, boot] of this.boots) {
      if (boot.deadline > now) continue
      for (const record of this.records.values()) {
        if (record.bootId !== bootId || record.settled) continue
        record.execution.kill()
        this.settle(record, true)
      }
      this.boots.delete(bootId)
    }
  }
}

/**
 * Describe an unknown thrown value in one line.
 * @param error - caught value.
 * @returns a human-readable message.
 */
function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error)
}
