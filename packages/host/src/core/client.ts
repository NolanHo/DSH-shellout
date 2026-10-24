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

import { connect, type Socket } from 'node:net'
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
  /** Runner socket path. */
  readonly socketPath: string
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

/** A connected runner client. */
export class ShelloutClient {
  private nextId = 1
  private readonly pending = new Map<number, { resolve: (value: unknown) => void; reject: (error: Error) => void }>()
  private readonly eventListeners = new Set<(event: SettledEvent) => void>()
  private readonly closeListeners = new Set<() => void>()
  private heartbeat: NodeJS.Timeout | undefined
  private closed = false
  private current: Socket

  private constructor(
    socket: Socket,
    private readonly options: ClientOptions,
    /** Descriptor answered by the handshake. */
    readonly descriptor: RunnerDescriptor,
    private readonly handshake: HelloResponse,
  ) {
    this.current = socket
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
    const socket = await openSocket(options.socketPath, options.connectTimeoutMs ?? 5_000)
    const probe = { id: 0 }
    void probe
    const handshake = await handshakeOn(socket, options)
    return new ShelloutClient(socket, options, handshake.descriptor, handshake)
  }

  /** Executions the runner held for this boot at handshake time. */
  get executions(): HelloResponse['executions'] {
    return this.handshake.executions
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

  private attach(socket: Socket): void {
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
    if (reconnect === undefined) {
      this.failPending('runner connection closed')
      this.notifyClosed()
      return
    }
    setTimeout(() => { void this.reconnect() }, reconnect)
  }

  private async reconnect(): Promise<void> {
    if (this.closed) return
    try {
      const socket = await openSocket(this.options.socketPath, this.options.connectTimeoutMs ?? 5_000)
      const handshake = await handshakeOn(socket, this.options)
      this.current = socket
      this.attach(socket)
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
async function openSocket(path: string, timeoutMs: number): Promise<Socket> {
  const socket = connect(path)
  socket.setNoDelay(true)
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.destroy()
      reject(new ShelloutRpcError('internal', `runner socket ${path} did not accept a connection within ${String(timeoutMs)} ms`))
    }, timeoutMs)
    socket.once('connect', () => { clearTimeout(timer); resolve() })
    socket.once('error', (error: Error) => { clearTimeout(timer); reject(toRpcError(error)) })
  })
  return socket
}

/**
 * Perform the handshake on a fresh socket.
 * @param socket - connected socket.
 * @param options - boot identity and build label.
 * @returns the runner's hello reply.
 * @throws ShelloutRpcError on a protocol mismatch or a malformed reply.
 */
async function handshakeOn(socket: Socket, options: ClientOptions): Promise<HelloResponse> {
  const decoder = new FrameDecoder()
  const reply = await new Promise<Envelope>((resolve, reject) => {
    const onData = (chunk: Buffer): void => {
      try {
        for (const frame of decoder.push(chunk)) {
          if (frame.id === 1) {
            socket.off('data', onData)
            resolve(frame)
            return
          }
        }
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
  if (reply.kind === 'err') {
    const error = reply.payload as WireError
    throw new ShelloutRpcError(error.code, error.message)
  }
  const hello = reply.payload as HelloResponse
  if (hello.protocol !== PROTOCOL_VERSION) {
    throw new ShelloutRpcError('version', `runner speaks protocol ${String(hello.protocol)}, this host speaks ${PROTOCOL_VERSION}`)
  }
  return hello
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
