import { describe, expect, it } from 'vitest'
import {
  DEFAULT_MAX_FRAME_BYTES,
  FrameDecoder,
  ProtocolError,
  encodeFrame,
  specDigest,
  type Envelope,
  type SpawnSpec,
} from '../src/index.ts'

const request: Envelope = {
  id: 7,
  kind: 'req',
  method: 'exec',
  payload: { executionId: 'x' },
}

describe('framing', () => {
  it('round-trips one envelope', () => {
    const decoder = new FrameDecoder()
    const frames = decoder.push(encodeFrame(request))
    expect(frames).toEqual([request])
    expect(decoder.pendingBytes).toBe(0)
  })

  it('buffers a frame split across chunks', () => {
    const frame = encodeFrame(request)
    const decoder = new FrameDecoder()
    expect(decoder.push(frame.subarray(0, 3))).toEqual([])
    expect(decoder.push(frame.subarray(3, 9))).toEqual([])
    expect(decoder.push(frame.subarray(9))).toEqual([request])
  })

  it('decodes several frames from one chunk in order', () => {
    const decoder = new FrameDecoder()
    const batch = Buffer.concat([
      encodeFrame({ ...request, id: 1 }),
      encodeFrame({ ...request, id: 2 }),
      encodeFrame({ ...request, id: 3 }),
    ])
    expect(decoder.push(batch).map(frame => frame.id)).toEqual([1, 2, 3])
  })

  it('rejects an oversized outgoing frame', () => {
    const big: Envelope = { id: 1, kind: 'req', method: 'exec', payload: { text: 'x'.repeat(64) } }
    expect(() => encodeFrame(big, 32)).toThrowError(ProtocolError)
  })

  it('rejects an oversized incoming frame before dispatching it', () => {
    const header = Buffer.alloc(4)
    header.writeUInt32BE(DEFAULT_MAX_FRAME_BYTES + 1, 0)
    const decoder = new FrameDecoder()
    expect(() => decoder.push(header)).toThrowError(/exceeds/)
  })

  it('rejects a frame body that is not JSON', () => {
    const body = Buffer.from('not json', 'utf8')
    const header = Buffer.alloc(4)
    header.writeUInt32BE(body.byteLength, 0)
    const decoder = new FrameDecoder()
    expect(() => decoder.push(Buffer.concat([header, body]))).toThrowError(/valid JSON/)
  })

  it('rejects an unknown frame kind', () => {
    const body = Buffer.from(JSON.stringify({ id: 1, kind: 'nope', method: 'x', payload: {} }), 'utf8')
    const header = Buffer.alloc(4)
    header.writeUInt32BE(body.byteLength, 0)
    const decoder = new FrameDecoder()
    expect(() => decoder.push(Buffer.concat([header, body]))).toThrowError(/unknown frame kind/)
  })
})

describe('specDigest', () => {
  const base: SpawnSpec = {
  argv: ['bash', '-c', 'echo hi'],
  cwd: '/tmp',
  env: { PATH: '/usr/bin:/bin' },
  timeoutMs: 1_000,
  onExpiry: 'kill',
  graceMs: 3_000,
  capture: { stdoutMaxBytes: 4_096, stderrMaxBytes: 4_096, spillMaxBytes: 1_048_576, spillDir: '/tmp' },
}

  it('is stable across key order', () => {
    const reordered: SpawnSpec = {
      capture: { spillDir: '/tmp', spillMaxBytes: 1_048_576, stderrMaxBytes: 4_096, stdoutMaxBytes: 4_096 },
      graceMs: 3_000,
      onExpiry: 'kill',
      timeoutMs: 1_000,
      env: { PATH: '/usr/bin:/bin' },
      cwd: '/tmp',
      argv: ['bash', '-c', 'echo hi'],
    }
    expect(specDigest(reordered)).toBe(specDigest(base))
  })

  it('changes when the spec changes', () => {
    expect(specDigest({ ...base, argv: ['bash', '-c', 'echo bye'] })).not.toBe(specDigest(base))
  })

  it('ignores undefined optional fields', () => {
    expect(specDigest({ ...base, stdin: undefined })).toBe(specDigest(base))
  })
})
