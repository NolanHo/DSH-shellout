/**
 * Wire protocol shared by the host plugin and the runner service.
 *
 * Transport: a Unix domain socket carrying length-prefixed JSON frames —
 * `[u32 big-endian length][UTF-8 JSON envelope]`. Every value crossing the
 * wire is text, so one JSON encoding is enough; there is no binary channel.
 *
 * @module dsh-shellout-protocol
 */

import { createHash } from 'node:crypto'

/** Exact protocol revision this build speaks. A mismatch is fatal, never negotiated down. */
export const PROTOCOL_VERSION = '1.0'

/** Largest frame either side accepts unless the handshake says otherwise. */
export const DEFAULT_MAX_FRAME_BYTES = 8 * 1024 * 1024

/** Frame kinds: a request, its success or failure reply, and an unsolicited event. */
export type FrameKind = 'req' | 'res' | 'err' | 'evt'

/** Requests the host sends to the runner. */
export type RequestMethod =
  | 'hello'
  | 'resolve'
  | 'exec'
  | 'readOutput'
  | 'readStream'
  | 'kill'
  | 'state'
  | 'ping'
  | 'drain'
  | 'shutdown'

/** Events the runner pushes to the host. */
export type EventMethod = 'settled'

/** One length-prefixed JSON envelope. `id` is 0 on events, which carry no request. */
export interface Envelope<T = unknown> {
  readonly id: number
  readonly kind: FrameKind
  readonly method: string
  readonly payload: T
}

/** A structured protocol or execution failure crossing the wire. */
export interface WireError {
  /** Stable machine code; see {@link WireErrorCode}. */
  readonly code: WireErrorCode
  /** One actionable line for the operator. */
  readonly message: string
  /** Optional structured facts (never a stack trace from the other process). */
  readonly detail?: Record<string, unknown>
}

/** Failure codes a runner reply may carry. */
export type WireErrorCode =
  | 'protocol'
  | 'version'
  | 'unknown-execution'
  | 'busy'
  | 'rejected'
  | 'internal'

/** Raised locally for a malformed, oversized, or unversioned frame stream. */
export class ProtocolError extends Error {
  /** @param code - machine code for the local failure. @param message - actionable detail. */
  constructor(readonly code: WireErrorCode, message: string) {
    super(message)
    this.name = 'ProtocolError'
  }
}

/** Runner capabilities reported by the handshake. */
export interface RunnerDescriptor {
  /** Default working directory applied when a request carries none. */
  readonly cwd: string
  /** Default command deadline in milliseconds. */
  readonly timeoutMs: number
  /** Upper bound applied to a requested deadline. */
  readonly maxTimeoutMs: number
  /** Default stdout capture budget in bytes. */
  readonly maxOutputBytes: number
  /** Sandbox mode the runner's executor applies, when it confines. */
  readonly sandboxMode?: string
}

/** Terminal facts for one execution. */
export type ExecutionState = 'running' | 'exited' | 'killed' | 'lost'

/** One resolved execution spec crossing the wire (the harness shell spec minus its AbortSignal). */
export interface ExecSpec {
  readonly command: string
  readonly workdir: string
  readonly timeoutMs: number
  readonly onExpiry: 'kill' | 'none'
  readonly stdoutMaxBytes: number
  readonly stdin?: string
  readonly env?: Record<string, string>
  readonly dshEnv?: Record<string, string>
  readonly sandboxPolicy?: Record<string, unknown>
}

/** One captured stream an execution owns. */
export type StreamName = 'stdout' | 'stderr'

/** Offset read result, matching the harness subprocess offset-read vocabulary. */
export interface StreamRead {
  /** Text captured from the requested offset. */
  readonly text: string
  /** Whole-stream offset to resume from. */
  readonly nextOffset: number
  /** True when the requested offset slid out of the retained window. */
  readonly lossy: boolean
  /** Complete-stream file the runner currently keeps, when it keeps one. */
  readonly spillPath?: string
}

/** Consuming delta read, matching the harness shell process read vocabulary. */
export interface OutputRead {
  /** Output produced since the previous consuming read. */
  readonly delta: string
  /** True when truncation dropped unread bytes the delta cannot include. */
  readonly lossy: boolean
  /** Complete stdout file, when truncation occurred and a safe path exists. */
  readonly stdoutSpillPath?: string
  /** Complete stderr file, when truncation occurred and a safe path exists. */
  readonly stderrSpillPath?: string
}

/** `hello` request payload. */
export interface HelloRequest {
  readonly protocol: string
  readonly clientBuild: string
  readonly bootId: string
  readonly maxFrameBytes: number
}

/** `hello` reply payload. */
export interface HelloResponse {
  readonly protocol: string
  readonly runnerBuild: string
  readonly runnerBootId: string
  readonly descriptor: RunnerDescriptor
  readonly maxFrameBytes: number
  readonly heartbeatMs: number
  readonly leaseTtlMs: number
  /** Executions of this boot the runner still holds, for reconnect reconciliation. */
  readonly executions: readonly { readonly executionId: string; readonly state: ExecutionState }[]
}

/** `exec` request payload. `operationId` makes a retried spawn idempotent. */
export interface ExecRequest {
  readonly operationId: string
  readonly executionId: string
  readonly spec: ExecSpec
  readonly specDigest: string
}

/** `exec` reply payload. */
export interface ExecResponse {
  readonly executionId: string
}

/** `readStream` request payload. */
export interface ReadStreamRequest {
  readonly executionId: string
  readonly stream: StreamName
  readonly offset: number
  /** Byte budget for this reply; the runner clamps it to its own frame cap. */
  readonly limit: number
}

/** `kill` reply payload. */
export interface KillResponse {
  /** True when a live execution received the termination request. */
  readonly requested: boolean
}

/** `state` reply payload. */
export interface StateResponse {
  readonly state: ExecutionState
  readonly exitCode: number | null
  readonly signal: string | null
  readonly sandbox?: Record<string, unknown>
}

/** `ping` request payload: refreshes the boot lease. */
export interface PingRequest {
  readonly bootId: string
}

/** `drain` request payload. */
export interface DrainRequest {
  readonly timeoutMs: number
}

/** `drain` reply payload. */
export interface DrainResponse {
  /** Executions still live when the drain returned. */
  readonly live: number
}

/** `settled` event payload. */
export interface SettledEvent {
  readonly executionId: string
  readonly status: 'exited' | 'killed' | 'failed'
  readonly exitCode: number | null
  readonly signal: string | null
  readonly sandbox?: Record<string, unknown>
  /** Human-readable terminal note, including the cause when the runner lost the execution. */
  readonly detail?: string
}

/**
 * Encode one envelope as a length-prefixed frame.
 * @param envelope - the value to send.
 * @param maxFrameBytes - bound applied to the encoded body.
 * @returns the complete frame, header included.
 * @throws ProtocolError when the encoded body exceeds the bound.
 */
export function encodeFrame(
  envelope: Envelope,
  maxFrameBytes: number = DEFAULT_MAX_FRAME_BYTES,
): Buffer {
  const body = Buffer.from(JSON.stringify(envelope), 'utf8')
  if (body.byteLength > maxFrameBytes) {
    throw new ProtocolError(
      'protocol',
      `frame of ${String(body.byteLength)} bytes exceeds the ${String(maxFrameBytes)}-byte cap`,
    )
  }
  const header = Buffer.allocUnsafe(4)
  header.writeUInt32BE(body.byteLength, 0)
  return Buffer.concat([header, body])
}

/**
 * Incremental decoder for a byte stream of frames.
 *
 * Buffers partial frames across chunks and rejects an oversized or malformed
 * frame before it is dispatched.
 */
export class FrameDecoder {
  private buffered: Buffer = Buffer.alloc(0)

  /** @param maxFrameBytes - largest body this decoder accepts. */
  constructor(private readonly maxFrameBytes: number = DEFAULT_MAX_FRAME_BYTES) {}

  /** Bytes held for a frame that has not fully arrived. */
  get pendingBytes(): number {
    return this.buffered.byteLength
  }

  /**
   * Feed one socket chunk.
   * @param chunk - newly received bytes.
   * @returns every complete envelope the chunk completed, in arrival order.
   * @throws ProtocolError on an oversized, empty, or non-JSON frame.
   */
  push(chunk: Buffer): Envelope[] {
    this.buffered = this.buffered.byteLength === 0 ? chunk : Buffer.concat([this.buffered, chunk])
    const frames: Envelope[] = []
    while (this.buffered.byteLength >= 4) {
      const length = this.buffered.readUInt32BE(0)
      if (length === 0) throw new ProtocolError('protocol', 'empty frame')
      if (length > this.maxFrameBytes) {
        throw new ProtocolError(
          'protocol',
          `incoming frame of ${String(length)} bytes exceeds the ${String(this.maxFrameBytes)}-byte cap`,
        )
      }
      if (this.buffered.byteLength < 4 + length) break
      const body = this.buffered.subarray(4, 4 + length)
      this.buffered = this.buffered.subarray(4 + length)
      frames.push(decodeBody(body))
    }
    return frames
  }
}

/**
 * Parse one frame body.
 * @param body - UTF-8 JSON bytes.
 * @returns the validated envelope.
 * @throws ProtocolError when the body is not a well-formed envelope.
 */
function decodeBody(body: Buffer): Envelope {
  let parsed: unknown
  try {
    parsed = JSON.parse(body.toString('utf8'))
  } catch {
    throw new ProtocolError('protocol', 'frame body is not valid JSON')
  }
  if (parsed === null || typeof parsed !== 'object') {
    throw new ProtocolError('protocol', 'frame body is not an object')
  }
  const envelope = parsed as Partial<Envelope>
  if (typeof envelope.id !== 'number' || !Number.isInteger(envelope.id) || envelope.id < 0) {
    throw new ProtocolError('protocol', 'frame id must be a non-negative integer')
  }
  if (envelope.kind !== 'req' && envelope.kind !== 'res' && envelope.kind !== 'err' && envelope.kind !== 'evt') {
    throw new ProtocolError('protocol', `unknown frame kind ${String(envelope.kind)}`)
  }
  if (typeof envelope.method !== 'string' || envelope.method.length === 0) {
    throw new ProtocolError('protocol', 'frame method must be a non-empty string')
  }
  return {
    id: envelope.id,
    kind: envelope.kind,
    method: envelope.method,
    payload: envelope.payload,
  }
}

/**
 * Stable digest of an execution spec, used to make a retried `exec` idempotent
 * without re-sending the payload for comparison.
 * @param spec - the resolved execution spec.
 * @returns a stable hex digest that changes whenever the spec changes.
 */
export function specDigest(spec: ExecSpec): string {
  return createHash('sha256').update(canonicalJson(spec)).digest('hex')
}

/**
 * Serialize a value with object keys sorted, so equal specs digest equally.
 * @param value - JSON-compatible value.
 * @returns the canonical JSON text.
 */
function canonicalJson(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null'
  if (Array.isArray(value)) return `[${value.map(entry => canonicalJson(entry)).join(',')}]`
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, entry]) => entry !== undefined)
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
  return `{${entries.map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`).join(',')}}`
}
