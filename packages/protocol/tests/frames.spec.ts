import { describe, expect, it } from 'vitest'
import {
  DEFAULT_MAX_FRAME_BYTES,
  FrameDecoder,
  ProtocolError,
  encodeFrame,
  specDigest,
  type Envelope,
  type ExecSpec,
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
  const base: ExecSpec = {
    command: 'echo hi',
    workdir: '/tmp',
    timeoutMs: 1000,
    onExpiry: 'kill',
    stdoutMaxBytes: 4096,
  }

  it('is stable across key order', () => {
    const reordered: ExecSpec = {
      stdoutMaxBytes: 4096,
      onExpiry: 'kill',
      timeoutMs: 1000,
      workdir: '/tmp',
      command: 'echo hi',
    }
    expect(specDigest(reordered)).toBe(specDigest(base))
  })

  it('changes when the spec changes', () => {
    expect(specDigest({ ...base, command: 'echo bye' })).not.toBe(specDigest(base))
  })

  it('ignores undefined optional fields', () => {
    expect(specDigest({ ...base, stdin: undefined })).toBe(specDigest(base))
  })
})
