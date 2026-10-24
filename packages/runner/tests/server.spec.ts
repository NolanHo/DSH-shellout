import { mkdtemp, rm } from 'node:fs/promises'
import { connect, type Socket } from 'node:net'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, describe, expect, it } from 'vitest'
import {
  FrameDecoder,
  PROTOCOL_VERSION,
  encodeFrame,
  specDigest,
  type Envelope,
  type ExecSpec,
} from 'dsh-shellout-protocol'
import { ExecutionRegistry } from '../src/core/registry.ts'
import { RunnerServer } from '../src/core/server.ts'
import { FakeBackend } from './support/fake-backend.ts'

const SPEC: ExecSpec = {
  command: 'echo hi',
  workdir: '/tmp',
  timeoutMs: 1_000,
  onExpiry: 'kill',
  stdoutMaxBytes: 4_096,
}

const cleanups: (() => Promise<void> | void)[] = []

afterEach(async () => {
  while (cleanups.length > 0) await cleanups.pop()?.()
})

/** Minimal framed client used to exercise the server over a real socket. */
class TestClient {
  private readonly decoder = new FrameDecoder()
  private nextId = 1
  private readonly pending = new Map<number, (frame: Envelope) => void>()
  readonly events: Envelope[] = []
  readonly closed: Promise<void>

  private constructor(private readonly socket: Socket) {
    this.closed = new Promise<void>((resolve) => { socket.once('close', () => { resolve() }) })
    socket.on('data', (chunk: Buffer) => {
      for (const frame of this.decoder.push(chunk)) {
        if (frame.kind === 'evt') { this.events.push(frame); continue }
        const settle = this.pending.get(frame.id)
        if (settle === undefined) continue
        this.pending.delete(frame.id)
        settle(frame)
      }
    })
  }

  static async connect(path: string): Promise<TestClient> {
    const socket = connect(path)
    await new Promise<void>((resolve, reject) => {
      socket.once('connect', () => { resolve() })
      socket.once('error', reject)
    })
    return new TestClient(socket)
  }

  request(method: string, payload: unknown): Promise<Envelope> {
    const id = this.nextId++
    const frame: Envelope = { id, kind: 'req', method, payload }
    return new Promise<Envelope>((resolve) => {
      this.pending.set(id, resolve)
      this.socket.write(encodeFrame(frame))
    })
  }

  raw(bytes: Buffer): void {
    this.socket.write(bytes)
  }

  close(): void {
    this.socket.destroy()
  }
}

async function startServer(options: { heartbeatMs?: number } = {}): Promise<{
  server: RunnerServer
  registry: ExecutionRegistry
  backend: FakeBackend
  path: string
  client: () => Promise<TestClient>
}> {
  const dir = await mkdtemp(join(tmpdir(), 'shellout-test-'))
  const path = join(dir, 'control.sock')
  const backend = new FakeBackend()
  const registry = new ExecutionRegistry(backend, { sweepIntervalMs: 20 })
  const server = await RunnerServer.listen({
    socketPath: path,
    registry,
    descriptor: backend.descriptor,
    runnerBuild: 'test',
    runnerBootId: 'runner-boot',
    ...options,
  })
  cleanups.push(async () => { await server.close(); registry.dispose(); await rm(dir, { recursive: true, force: true }) })
  return { server, registry, backend, path, client: () => TestClient.connect(path) }
}

const hello = (bootId = 'boot-a'): { protocol: string; clientBuild: string; bootId: string; maxFrameBytes: number } => ({
  protocol: PROTOCOL_VERSION,
  clientBuild: 'test',
  bootId,
  maxFrameBytes: 8 * 1024 * 1024,
})

describe('RunnerServer', () => {
  it('answers hello with the descriptor and held executions', async () => {
    const { client } = await startServer()
    const channel = await client()
    cleanups.push(() => { channel.close() })
    const reply = await channel.request('hello', hello())
    expect(reply.kind).toBe('res')
    expect(reply.payload).toMatchObject({
      protocol: PROTOCOL_VERSION,
      runnerBuild: 'test',
      descriptor: { cwd: '/tmp' },
      executions: [],
    })
  })

  it('refuses an unsupported protocol version', async () => {
    const { client } = await startServer()
    const channel = await client()
    const reply = await channel.request('hello', { ...hello(), protocol: '9.9' })
    expect(reply.kind).toBe('err')
    expect((reply.payload as { code: string }).code).toBe('version')
    await channel.closed
  })

  it('requires the handshake before execution', async () => {
    const { client } = await startServer()
    const channel = await client()
    cleanups.push(() => { channel.close() })
    const reply = await channel.request('exec', {
      operationId: 'op1', executionId: 'e1', spec: SPEC, specDigest: specDigest(SPEC),
    })
    expect(reply.kind).toBe('err')
    expect((reply.payload as { code: string }).code).toBe('protocol')
  })

  it('executes and pushes one settled event', async () => {
    const { client, backend } = await startServer()
    const channel = await client()
    cleanups.push(() => { channel.close() })
    await channel.request('hello', hello())
    const reply = await channel.request('exec', {
      operationId: 'op1', executionId: 'e1', spec: SPEC, specDigest: specDigest(SPEC),
    })
    expect(reply.payload).toEqual({ executionId: 'e1' })
    backend.executions[0]?.append('stdout', 'hi\n')
    backend.executions[0]?.complete(0)
    await new Promise(resolve => { setTimeout(resolve, 50) })
    expect(channel.events).toHaveLength(1)
    expect(channel.events[0]?.method).toBe('settled')
    expect(channel.events[0]?.payload).toMatchObject({ executionId: 'e1', status: 'exited', exitCode: 0 })
  })

  it('serves offset reads without consuming them', async () => {
    const { client, backend } = await startServer()
    const channel = await client()
    cleanups.push(() => { channel.close() })
    await channel.request('hello', hello())
    await channel.request('exec', {
      operationId: 'op1', executionId: 'e1', spec: SPEC, specDigest: specDigest(SPEC),
    })
    backend.executions[0]?.append('stdout', 'abcdef')
    const first = await channel.request('readStream', { executionId: 'e1', stream: 'stdout', offset: 0, limit: 4 })
    expect(first.payload).toEqual({ text: 'abcd', nextOffset: 4, lossy: false })
    const second = await channel.request('readStream', { executionId: 'e1', stream: 'stdout', offset: 0, limit: 40 })
    expect((second.payload as { text: string }).text).toBe('abcdef')
  })

  it('reports an unknown execution', async () => {
    const { client } = await startServer()
    const channel = await client()
    cleanups.push(() => { channel.close() })
    await channel.request('hello', hello())
    const reply = await channel.request('state', { executionId: 'ghost' })
    expect(reply.kind).toBe('err')
    expect((reply.payload as { code: string }).code).toBe('unknown-execution')
  })

  it('closes the connection on an oversized frame', async () => {
    const { client } = await startServer()
    const channel = await client()
    const header = Buffer.alloc(4)
    header.writeUInt32BE(64 * 1024 * 1024, 0)
    channel.raw(header)
    await channel.closed
  })
})
