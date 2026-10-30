/**
 * Reaching a runner by command instead of socket.
 *
 * The attach command is the whole remote story: the runner bootstraps the host
 * and speaks the same frames on its stdio, so the client only has to pick a
 * transport. These tests pin that selection and, when the Rust binary is built,
 * drive one attach end to end through the local stand-in for an ssh hop.
 */
import { existsSync } from 'node:fs'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

import { describe, expect, it } from 'vitest'

import { ShelloutClient } from '../src/core/client.ts'
import { resolveConfig } from '../src/dsh/provider.ts'

const repo = resolve(dirname(fileURLToPath(import.meta.url)), '../../..')
const runner = resolve(repo, 'runner-rs/target/release/dsh-shellout-runner')
const fakeRemote = resolve(repo, 'scripts/fake-remote.sh')

describe('remote transport selection', () => {
  it('uses the socket when no remote is named', () => {
    const config = resolveConfig({ socketPath: '/tmp/control.sock' }, { HOME: '/root' })
    expect(config.transport).toEqual({ kind: 'socket', path: '/tmp/control.sock' })
  })

  it('builds an attach command for a named host', () => {
    const config = resolveConfig(
      { remote: { host: 'worker-1', ssh: ['ssh', '-J', 'jump'], remoteRoot: '/tmp/state' } },
      { HOME: '/root' },
    )
    expect(config.transport).toEqual({
      kind: 'command',
      argv: [
        'dsh-shellout-runner', '--mode', 'attach', '--host', 'worker-1',
        '--ssh', 'ssh -J jump',
        '--remote-root', '/tmp/state',
      ],
    })
  })

  it('omits the optional flags a deployment did not set', () => {
    const config = resolveConfig({ remote: { host: 'worker-1', runner: '/opt/runner' } }, { HOME: '/root' })
    expect(config.transport).toEqual({
      kind: 'command',
      argv: ['/opt/runner', '--mode', 'attach', '--host', 'worker-1'],
    })
  })
})

const ready = existsSync(runner) && existsSync(fakeRemote)

describe.skipIf(!ready)('attach through a command transport', () => {
  it('connects, runs a command and reports its settlement', async () => {
    const root = `/tmp/dsh-ts-attach-${String(process.pid)}`
    const client = await ShelloutClient.connect({
      transport: {
        kind: 'command',
        argv: [runner, '--mode', 'attach', '--host', 'fake', '--ssh', fakeRemote, '--remote-root', root],
      },
      bootId: 'boot-ts-attach',
      connectTimeoutMs: 20_000,
      reconnectDelayMs: 0,
    })
    try {
      const executionId = 'ts-attach-1'
      await client.request('exec', {
        operationId: `op-${executionId}`,
        executionId,
        specDigest: 'digest',
        spec: {
          argv: ['/bin/sh', '-c', "printf 'ts-remote\\n'"],
          cwd: '/tmp',
          env: { PATH: '/usr/local/bin:/usr/bin:/bin' },
          timeoutMs: 20_000,
          onExpiry: 'kill',
          graceMs: 500,
          capture: { stdoutMaxBytes: 4096, stderrMaxBytes: 4096, spillMaxBytes: 0, spillDir: '/tmp' },
        },
      })
      const settled = await new Promise<{ status?: string; exitCode?: number }>((settle, fail) => {
        const timer = setTimeout(() => { fail(new Error('no settlement')) }, 20_000)
        client.onSettled((event) => {
          if (event.executionId !== executionId) return
          clearTimeout(timer)
          settle(event as { status?: string; exitCode?: number })
        })
      })
      expect(settled.status).toBe('exited')
      expect(settled.exitCode).toBe(0)
      const read = await client.request<{ text: string }>('readStream', {
        executionId, stream: 'stdout', offset: 0, limit: 4096,
      })
      expect(read.text).toBe('ts-remote\n')
    } finally {
      client.close()
    }
  }, 40_000)
})
