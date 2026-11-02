/**
 * The direct path exists for hosts that cannot run the plane's runner. It keeps
 * the caller-visible contract — exit status, separated streams, deadline — and
 * gives up only what needs state on the host.
 */
import { describe, expect, it } from 'vitest'

import { directArgv, directScript, probePlatform, runDirect } from '../src/dsh/direct.ts'
import type { HostEntry } from '../src/dsh/hosts.ts'

const macHost = process.env.DSH_MAC_HOST

describe('direct runs', () => {
  it('hands the host one script, with the directory applied there', () => {
    const script = directScript({ command: 'echo hi', cwd: '/tmp/it\'s here', shell: 'bash' })
    expect(script).toContain(`cd '/tmp/it'\\''s here'`)
    expect(script).toContain(`exec bash -c 'echo hi'`)
  })

  it('passes the account ssh words through, so a ProxyCommand applies', () => {
    const entry: HostEntry = { target: 'nolan-mac', ssh: ['ssh', '-o', 'BatchMode=yes'] }
    const { program, args } = directArgv(entry, { command: 'true' })
    expect(program).toBe('ssh')
    expect(args.slice(0, 2)).toEqual(['-o', 'BatchMode=yes'])
    expect(args).toContain('nolan-mac')
  })
})

describe.skipIf(macHost === undefined)('direct runs on a host without a runner', () => {
  it('reports the remote platform', async () => {
    const platform = await probePlatform({ target: macHost! })
    expect(platform?.arch.length).toBeGreaterThan(0)
  }, 60_000)

  it('keeps exit status and the two streams apart', async () => {
    const entry: HostEntry = { target: macHost! }
    const failed = await runDirect(entry, { command: 'echo out; echo err >&2; exit 42', cwd: '/tmp' })
    expect(failed.exitCode).toBe(42)
    expect(failed.stdout.trim()).toBe('out')
    expect(failed.stderr.trim()).toBe('err')
  }, 60_000)
})
