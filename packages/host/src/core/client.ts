/**
 * Host-side socket client: one connection to the runner, framed requests,
 * replies, and settlement events.
 *
 * The client owns the boot id. A dropped connection is retried; a reconnect
 * re-handshakes with the same boot id, which the runner answers with the
 * executions that boot still holds, so live work continues rather than being
 * re-created.
 *
 * @module dsh-shellout/core/client
 */

import { Duplex } from 'node:stream'

import { openTransport, type TransportOptions } from './transport.js'
import {
  DEFAULT_MAX_FRAME_BYTES,
  FrameDecoder,
  PROTOCOL_VERSION,
  ProtocolError,
  encodeFrame,
  type Envelope,
  type HelloResponse,
  type RunnerDescriptor,
  type SettledEvent,
  type WireError,
  type WireErrorCode,
} from 'dsh-shellout-protocol'

/** A failure reported by the runner, or a local transport failure. */
export class ShelloutRpcError extends Error {
  /** @param code - wire or local error code. @param message - actionable detail. */
  constructor(readonly code: WireErrorCode, message: string) {
    super(message)
    this.name = 'ShelloutRpcError'
  }
}

/** Connection options. */
export interface ClientOptions {
  /** How to reach the runner; `socketPath` is a shorthand for the socket form. */
  readonly transport?: TransportOptions
  /** Runner socket path; ignored when `transport` is given. */
  readonly socketPath?: string
  /** Identity for this host boot; executions are owned by it. */
  readonly bootId: string
  /** Build label sent at handshake. */
  readonly clientBuild?: string
  /** Bound on the first handshake before the connection is declared failed. */
  readonly connectTimeoutMs?: number
  /** Boot-lease ping cadence. */
  readonly heartbeatMs?: number
  /** Retry delay after a dropped connection; `undefined` disables reconnect. */
  readonly reconnectDelayMs?: number
}

/**
 * Bound a handshake.
 *
 * `connectTimeoutMs` used to cover only the spawn event, so a blackholed host —
 * or a remote bootstrap that pushes three megabytes first — left `connect`
 * pending forever and the caller's fallback unreachable.
 *
 * @param handshake - the in-flight handshake.
 * @param timeoutMs - how long the whole exchange may take.
 * @param socket - the transport to destroy when the bound is hit.
 * @returns the handshake result.
 */
async function withDeadline<T>(handshake: Promise<T>, timeoutMs: number, socket: Duplex): Promise<T> {
  let timer: NodeJS.Timeout | undefined
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      socket.destroy()
      reject(new Error(`the runner did not complete its handshake within ${timeoutMs}ms`))
    }, timeoutMs)
    timer.unref()
  })
  try {
    return await Promise.race([handshake, deadline])
  } finally {
    clearTimeout(timer)
  }
}

/** The transport a client uses, derived from its options. */
function transportOf(options: ClientOptions): TransportOptions {
  if (options.transport !== undefined) return options.transport
  if (options.socketPath === undefined) throw new Error('shellout: client needs a transport or a socket path')
  return { kind: 'socket', path: options.socketPath }
}

/** A connected runner client. */
export class ShelloutClient {
  private nextId = 1
  private readonly pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>()
  private readonly eventListeners = new Set<(event: SettledEvent) => void>()
  private readonly closeListeners = new Set<() => void>()
  private readonly disconnectListeners = new Set<() => void>()
  private readonly reconnectListeners = new Set<() => void>()
  private latest: HelloResponse
  private heartbeat: NodeJS.Timeout | undefined
  private closed = false
  private current: Duplex

  private constructor(
    socket: Duplex,
    private readonly options: ClientOptions,
    /** Descriptor answered by the handshake. */
    readonly descriptor: RunnerDescriptor,
    private readonly handshake: HelloResponse,
  ) {
    this.current = socket
    this.latest = handshake
    this.attach(socket)
    this.heartbeat = setInterval(() => { void this.ping() }, options.heartbeatMs ?? handshake.heartbeatMs)
    this.heartbeat.unref()
  }

  /**
   * Connect and handshake.
   * @param options - socket path and boot identity.
   * @returns the connected client.
   * @throws ShelloutRpcError when the socket is unreachable or the protocol mismatches.
   */
  static async connect(options: ClientOptions): Promise<ShelloutClient> {
    const socket = await openTransport(transportOf(options), options.connectTimeoutMs ?? 5_000).catch(
      (error: Error) => { throw new ShelloutRpcError('protocol', error.message) },
    )
    // A transport that already started (an attach child, a socket) must not
    // outlive a failed handshake: the caller retries, and every retry would
    // otherwise leave another session holding the host's state lock.
    const { hello, trailing } = await withDeadline(
      handshakeOn(socket, options),
      options.connectTimeoutMs ?? 5_000,
      socket,
    ).catch((error: Error) => {
      socket.destroy()
      throw error
    })
    const client = new ShelloutClient(socket, options, hello.descriptor, hello)
    for (const frame of trailing) client.dispatch(frame)
    socket.resume()
    return client
  }

  /** Executions the runner held for this boot at the most recent handshake. */
  get executions(): HelloResponse['executions'] {
    return this.latest.executions
  }

  /** Heartbeat and lease facts answered by the runner. */
  get lease(): { heartbeatMs: number; leaseTtlMs: number } {
    return { heartbeatMs: this.handshake.heartbeatMs, leaseTtlMs: this.handshake.leaseTtlMs }
  }

  /** True while the socket is usable. */
  get connected(): boolean {
    return !this.closed && !this.current.destroyed
  }

  /**
   * Send one request.
   * @param method - protocol method name.
   * @param payload - method payload.
   * @returns the reply payload.
   * @throws ShelloutRpcError carrying the runner's code or a transport failure.
   */
  async request<T>(method: string, payload: unknown): Promise<T> {
    if (!this.connected) throw new ShelloutRpcError('protocol', 'runner connection is closed')
    const id = this.nextId++
    const envelope: Envelope = { id, kind: 'req', method, payload }
    return await new Promise<T>((resolve, reject) => {
      this.pending.set(id, { resolve: value => { resolve(value as T) }, reject })
      try {
        this.current.write(encodeFrame(envelope, this.handshake.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES))
      } catch (error: unknown) {
        this.pending.delete(id)
        reject(toRpcError(error))
      }
    })
  }

  /**
   * Subscribe to settlement events.
   * @param listener - called once per settled execution.
   * @returns a disposer.
   */
  onSettled(listener: (event: SettledEvent) => void): () => void {
    this.eventListeners.add(listener)
    return () => { this.eventListeners.delete(listener) }
  }

  /**
   * Subscribe to a permanent connection loss.
   * @param listener - called once when no reconnect attempt will follow.
   * @returns a disposer.
   */
  onClose(listener: () => void): () => void {
    this.closeListeners.add(listener)
    return () => { this.closeListeners.delete(listener) }
  }

  /**
   * Subscribe to a lost connection that may still recover.
   * @param listener - called once per loss.
   * @returns a disposer.
   */
  onDisconnect(listener: () => void): () => void {
    this.disconnectListeners.add(listener)
    return () => { this.disconnectListeners.delete(listener) }
  }

  /**
   * Subscribe to a successful reconnect.
   * @param listener - called once per recovered connection.
   * @returns a disposer.
   */
  onReconnect(listener: () => void): () => void {
    this.reconnectListeners.add(listener)
    return () => { this.reconnectListeners.delete(listener) }
  }

  /** Stop the heartbeat and close the socket. */
  close(): void {
    if (this.closed) return
    this.closed = true
    if (this.heartbeat !== undefined) clearInterval(this.heartbeat)
    this.heartbeat = undefined
    this.current.destroy()
    for (const listener of this.closeListeners) listener()
  }

  private async ping(): Promise<void> {
    if (!this.connected) return
    try {
      await this.request('ping', { bootId: this.options.bootId })
    } catch {
      // The next socket event decides whether this is a loss; a failed ping is not fatal.
    }
  }

  private attach(socket: Duplex): void {
    const decoder = new FrameDecoder(this.handshake.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES)
    socket.on('data', (chunk: Buffer) => {
      let frames: Envelope[]
      try {
        frames = decoder.push(chunk)
      } catch {
        socket.destroy()
        return
      }
      for (const frame of frames) this.dispatch(frame)
    })
    socket.on('error', () => { /* Handled by the close path. */ })
    socket.on('close', () => { this.onSocketClosed() })
  }

  private dispatch(frame: Envelope): void {
    if (frame.kind === 'evt') {
      if (frame.method === 'settled') {
        for (const listener of this.eventListeners) listener(frame.payload as SettledEvent)
      }
      return
    }
    const waiter = this.pending.get(frame.id)
    if (waiter === undefined) return
    this.pending.delete(frame.id)
    if (frame.kind === 'err') {
      const error = frame.payload as WireError
      waiter.reject(new ShelloutRpcError(error.code, error.message))
      return
    }
    waiter.resolve(frame.payload)
  }

  private onSocketClosed(): void {
    if (this.closed) return
    const reconnect = this.options.reconnectDelayMs
    if (reconnect === undefined || reconnect <= 0) {
      this.failPending('runner connection closed')
      this.notifyClosed()
      return
    }
    this.failPending('runner connection closed')
    for (const listener of this.disconnectListeners) listener()
    setTimeout(() => { void this.reconnect() }, reconnect)
  }

  private async reconnect(): Promise<void> {
    if (this.closed) return
    try {
      const socket = await openTransport(transportOf(this.options), this.options.connectTimeoutMs ?? 5_000).catch(
        (error: Error) => { throw new ShelloutRpcError('protocol', error.message) },
      )
      // Same rule as the first connect: a transport that already started must
      // not survive a failed handshake, or each retry leaks another session.
      const { hello, trailing } = await withDeadline(
        handshakeOn(socket, this.options),
        this.options.connectTimeoutMs ?? 5_000,
        socket,
      ).catch((error: Error) => {
        socket.destroy()
        throw error
      })
      this.latest = hello
      this.current = socket
      this.attach(socket)
      // The runner replays this boot's retained settlements on this connection,
      // behind the handshake reply; they are dispatched before the socket is
      // read again, so a settlement announced while this host was away arrives.
      for (const frame of trailing) this.dispatch(frame)
      socket.resume()
      for (const listener of this.reconnectListeners) listener()
    } catch {
      if (this.closed) return
      setTimeout(() => { void this.reconnect() }, this.options.reconnectDelayMs ?? 1_000)
    }
  }

  /** Mark the client permanently closed and notify subscribers once. */
  private notifyClosed(): void {
    if (this.closed) return
    this.closed = true
    if (this.heartbeat !== undefined) clearInterval(this.heartbeat)
    this.heartbeat = undefined
    for (const listener of this.closeListeners) listener()
  }

  private failPending(message: string): void {
    for (const [, waiter] of this.pending) waiter.reject(new ShelloutRpcError('internal', message))
    this.pending.clear()
  }
}

/**
 * Open one socket and wait for it to connect.
 * @param path - unix socket path.
 * @param timeoutMs - connection bound.
 * @returns the connected socket.
 */

/** A completed handshake: the reply, and whatever arrived behind it. */
interface Handshake {
  /** The runner's hello reply. */
  readonly hello: HelloResponse
  /** Frames decoded after the reply; dispatch them once the connection is attached. */
  readonly trailing: Envelope[]
}

/**
 * Perform the handshake on a fresh socket.
 *
 * The runner writes this boot's retained settlement replays right behind the
 * reply. The socket is left paused after the reply so those bytes stay buffered
 * until the caller has attached its handler and dispatched `trailing`: handshake
 * frames are never dropped in the gap between the two.
 * @param socket - connected socket.
 * @param options - boot identity and build label.
 * @returns the runner's hello reply and the frames behind it.
 * @throws ShelloutRpcError on a protocol mismatch or a malformed reply.
 */
async function handshakeOn(socket: Duplex, options: ClientOptions): Promise<Handshake> {
  const decoder = new FrameDecoder()
  const handshake = await new Promise<{ reply: Envelope; trailing: Envelope[] }>((resolve, reject) => {
    const onData = (chunk: Buffer): void => {
      try {
        const frames = decoder.push(chunk)
        const index = frames.findIndex(frame => frame.id === 1)
        if (index < 0) return
        socket.off('data', onData)
        socket.pause()
        resolve({ reply: frames[index] as Envelope, trailing: frames.slice(index + 1) })
      } catch (error: unknown) {
        reject(toRpcError(error))
      }
    }
    socket.on('data', onData)
    socket.once('error', (error: Error) => { reject(toRpcError(error)) })
    socket.write(encodeFrame({
      id: 1,
      kind: 'req',
      method: 'hello',
      payload: {
        protocol: PROTOCOL_VERSION,
        clientBuild: options.clientBuild ?? 'dev',
        bootId: options.bootId,
        maxFrameBytes: DEFAULT_MAX_FRAME_BYTES,
      },
    }))
  })
  socket.removeAllListeners('data')
  if (handshake.reply.kind === 'err') {
    const error = handshake.reply.payload as WireError
    throw new ShelloutRpcError(error.code, error.message)
  }
  const hello = handshake.reply.payload as HelloResponse
  if (hello.protocol !== PROTOCOL_VERSION) {
    throw new ShelloutRpcError('version', `runner speaks protocol ${String(hello.protocol)}, this host speaks ${PROTOCOL_VERSION}`)
  }
  return { hello, trailing: handshake.trailing }
}

/**
 * Normalize a caught value into a transport error.
 * @param error - caught value.
 * @returns the RPC error to surface.
 */
function toRpcError(error: unknown): ShelloutRpcError {
  if (error instanceof ShelloutRpcError) return error
  if (error instanceof ProtocolError) return new ShelloutRpcError(error.code, error.message)
  return new ShelloutRpcError('internal', error instanceof Error ? error.message : String(error))
}
