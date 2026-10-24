/**
 * Runner server: the socket endpoint the host plugin talks to.
 *
 * One connection carries framed requests and replies plus unsolicited
 * `settled` events. A connection binds to one boot id at handshake; only the
 * connection holding that boot receives its events, and a later reconnect with
 * the same boot id takes over.
 *
 * @module dsh-shellout-runner/core/server
 */

import { chmod, unlink } from 'node:fs/promises'
import { createServer, type Server, type Socket } from 'node:net'
import {
  DEFAULT_MAX_FRAME_BYTES,
  FrameDecoder,
  PROTOCOL_VERSION,
  encodeFrame,
  type DrainRequest,
  type Envelope,
  type ExecRequest,
  type HelloRequest,
  type ReadStreamRequest,
  type RunnerDescriptor,
  type SettledEvent,
  type WireErrorCode,
} from 'dsh-shellout-protocol'
import { ExecutionRegistry, RunnerError } from './registry.js'

/** Server bounds and identity. */
export interface RunnerServerOptions {
  /** Socket path to bind; created `0600`. */
  readonly socketPath: string
  /** Execution table this server exposes. */
  readonly registry: ExecutionRegistry
  /** Defaults and caps the host mirrors. */
  readonly descriptor: RunnerDescriptor
  /** Build label reported at handshake. */
  readonly runnerBuild?: string
  /** Identity of this runner process incarnation. */
  readonly runnerBootId?: string
  /** Frame cap this server accepts. */
  readonly maxFrameBytes?: number
  /** Heartbeat cadence advertised to the host. */
  readonly heartbeatMs?: number
  /** Boot lease the registry enforces. */
  readonly leaseTtlMs?: number
  /** Called after a `shutdown` request has been answered. */
  readonly onShutdown?: () => void
}

interface Connection {
  readonly socket: Socket
  readonly decoder: FrameDecoder
  bootId?: string
}

/** The runner's socket endpoint. */
export class RunnerServer {
  private readonly connections = new Set<Connection>()
  private readonly server: Server
  private readonly detach: () => void
  private closing = false

  private constructor(private readonly options: RunnerServerOptions, server: Server) {
    this.server = server
    this.detach = options.registry.onSettled(event => { this.broadcast(event) })
  }

  /** Socket path this server bound. */
  get path(): string {
    return this.options.socketPath
  }

  /**
   * Bind the socket and start accepting connections.
   * @param options - bounds, identity, and the registry to serve.
   * @returns the running server.
   * @throws when the path is already served by another process.
   */
  static async listen(options: RunnerServerOptions): Promise<RunnerServer> {
    const maxFrameBytes = options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES
    await removeStaleSocket(options.socketPath)
    const server = createServer()
    const instance = new RunnerServer(options, server)
    await new Promise<void>((resolve, reject) => {
      const onError = (error: Error): void => { reject(error) }
      server.once('error', onError)
      server.listen(options.socketPath, () => {
        server.off('error', onError)
        resolve()
      })
    })
    await chmod(options.socketPath, 0o600)
    server.on('connection', (socket) => { instance.accept(socket, maxFrameBytes) })
    server.on('error', () => { /* A post-listen server error surfaces through close(); connections see EOF. */ })
    return instance
  }

  /**
   * Stop accepting connections and close the socket.
   * @returns fulfillment once the listener and every connection closed.
   */
  async close(): Promise<void> {
    if (this.closing) return
    this.closing = true
    this.detach()
    for (const connection of this.connections) connection.socket.destroy()
    this.connections.clear()
    await new Promise<void>((resolve) => { this.server.close(() => { resolve() }) })
    await unlink(this.options.socketPath).catch(() => { /* Already gone: nothing to clean. */ })
  }

  private accept(socket: Socket, maxFrameBytes: number): void {
    const connection: Connection = { socket, decoder: new FrameDecoder(maxFrameBytes) }
    this.connections.add(connection)
    socket.on('data', (chunk: Buffer) => { void this.onData(connection, chunk) })
    socket.on('error', () => { this.drop(connection) })
    socket.on('close', () => { this.drop(connection) })
  }

  private drop(connection: Connection): void {
    this.connections.delete(connection)
  }

  private async onData(connection: Connection, chunk: Buffer): Promise<void> {
    let frames: Envelope[]
    try {
      frames = connection.decoder.push(chunk)
    } catch (error: unknown) {
      this.fail(connection, 0, 'protocol', error instanceof Error ? error.message : String(error), true)
      return
    }
    for (const frame of frames) {
      if (frame.kind !== 'req') {
        this.fail(connection, frame.id, 'protocol', `unexpected ${frame.kind} frame`, false)
        continue
      }
      await this.dispatch(connection, frame)
    }
  }

  private async dispatch(connection: Connection, frame: Envelope): Promise<void> {
    try {
      switch (frame.method) {
        case 'hello': {
          const request = frame.payload as HelloRequest
          if (request.protocol !== PROTOCOL_VERSION) {
            this.fail(connection, frame.id, 'version',
              `protocol ${String(request.protocol)} is not supported; this runner speaks ${PROTOCOL_VERSION}`, true)
            return
          }
          connection.bootId = request.bootId
          const executions = this.options.registry.attach(request.bootId)
          this.reply(connection, frame.id, frame.method, {
            protocol: PROTOCOL_VERSION,
            runnerBuild: this.options.runnerBuild ?? 'dev',
            runnerBootId: this.options.runnerBootId ?? 'unknown',
            descriptor: this.options.descriptor,
            maxFrameBytes: this.options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES,
            heartbeatMs: this.options.heartbeatMs ?? 5_000,
            leaseTtlMs: this.options.leaseTtlMs ?? 60_000,
            executions,
          })
          return
        }
        case 'exec': {
          const bootId = this.requireBoot(connection)
          const request = frame.payload as ExecRequest
          const result = await this.options.registry.exec(request, bootId)
          this.reply(connection, frame.id, frame.method, result)
          return
        }
        case 'readOutput': {
          const request = frame.payload as { executionId: string }
          this.reply(connection, frame.id, frame.method, this.options.registry.readOutput(request.executionId))
          return
        }
        case 'readStream': {
          const request = frame.payload as ReadStreamRequest
          this.reply(connection, frame.id, frame.method, this.options.registry.readStream(
            request.executionId, request.stream, request.offset, request.limit,
          ))
          return
        }
        case 'kill': {
          const request = frame.payload as { executionId: string }
          this.reply(connection, frame.id, frame.method, this.options.registry.kill(request.executionId))
          return
        }
        case 'state': {
          const request = frame.payload as { executionId: string }
          this.reply(connection, frame.id, frame.method, this.options.registry.state(request.executionId))
          return
        }
        case 'ping': {
          const request = frame.payload as { bootId: string }
          this.options.registry.ping(request.bootId)
          this.reply(connection, frame.id, frame.method, { ok: true })
          return
        }
        case 'drain': {
          const request = frame.payload as DrainRequest
          const result = await this.options.registry.drain(request.timeoutMs)
          this.reply(connection, frame.id, frame.method, result)
          return
        }
        case 'shutdown': {
          this.reply(connection, frame.id, frame.method, {})
          this.options.onShutdown?.()
          await this.close()
          return
        }
        default: {
          this.fail(connection, frame.id, 'protocol', `unknown method ${frame.method}`, false)
        }
      }
    } catch (error: unknown) {
      const code: WireErrorCode = error instanceof RunnerError ? error.code : 'internal'
      this.fail(connection, frame.id, code, error instanceof Error ? error.message : String(error), false)
    }
  }

  private requireBoot(connection: Connection): string {
    if (connection.bootId === undefined) {
      throw new RunnerError('protocol', 'hello must precede this request')
    }
    return connection.bootId
  }

  private reply(connection: Connection, id: number, method: string, payload: unknown): void {
    this.write(connection, { id, kind: 'res', method, payload })
  }

  private fail(connection: Connection, id: number, code: WireErrorCode, message: string, fatal: boolean): void {
    this.write(connection, { id, kind: 'err', method: 'error', payload: { code, message } })
    if (fatal) {
      this.connections.delete(connection)
      connection.socket.destroy()
    }
  }

  private write(connection: Connection, envelope: Envelope): void {
    if (connection.socket.destroyed) return
    try {
      connection.socket.write(encodeFrame(envelope, this.options.maxFrameBytes ?? DEFAULT_MAX_FRAME_BYTES))
    } catch {
      this.connections.delete(connection)
      connection.socket.destroy()
    }
  }

  private broadcast(event: SettledEvent): void {
    for (const connection of this.connections) {
      if (connection.bootId === undefined) continue
      this.write(connection, { id: 0, kind: 'evt', method: 'settled', payload: event })
    }
  }
}

/**
 * Remove a socket file left behind by an earlier runner.
 *
 * A live server keeps the path bound, so a successful connect proves the file
 * is in use; only an unreachable path is removed.
 *
 * @param path - socket path to clear.
 * @returns fulfillment once the path is free.
 */
async function removeStaleSocket(path: string): Promise<void> {
  const { connect } = await import('node:net')
  const inUse = await new Promise<boolean>((resolve) => {
    const probe = connect(path)
    probe.once('connect', () => { probe.destroy(); resolve(true) })
    probe.once('error', () => { probe.destroy(); resolve(false) })
  })
  if (inUse) throw new Error(`socket ${path} is already served by another runner`)
  await unlink(path).catch(() => { /* No stale file: nothing to remove. */ })
}
