/**
 * A terminal is a separate contract from a pipe, and the only proof that one was
 * allocated is the program seeing it: `[ -t 1 ]` succeeds inside a terminal and
 * fails on a pipe.
 */
import { describe, expect, it } from 'vitest'

import type { HostEntry } from '../src/dsh/hosts.ts'
import { runInTty, ttyArgv } from '../src/dsh/tty.ts'

/** A host that answers ssh, named by the caller; absent means the live case is skipped. */
const liveHost = process.env.DSH_TTY_HOST

describe('terminal runs', () => {
  it('builds a local terminal through script', () => {
    const { program, args } = ttyArgv({ command: 'echo hi', cwd: '/tmp', shell: '/bin/bash' })
    expect(program).toBe('script')
    expect(args[0]).toBe('-qec')
    expect(args[1]).toContain("cd '/tmp' && exec /bin/bash -c 'echo hi'")
  })

  it('builds a host terminal through ssh -tt, keeping a path with a space intact', () => {
    const host: HostEntry = { target: 'root@gpu-a.example', ssh: ['ssh', '-p', '2222'] }
    const { program, args } = ttyArgv({ command: 'cat my file', cwd: "/srv/it's here" }, host)
    expect(program).toBe('ssh')
    expect(args).toContain('-tt')
    expect(args).toContain('root@gpu-a.example')
    const script = args.at(-1)!
    expect(script).toContain(`cd '/srv/it'\\''s here'`)
    expect(script).toContain(`exec /bin/bash -c 'cat my file'`)
  })

  it('gives the program a real terminal locally', async () => {
    const result = await runInTty({ command: 'test -t 1 && echo is-tty || echo not-tty' })
    expect(result.output).toContain('is-tty')
    expect(result.merged).toBe(true)
    expect(result.exitCode).toBe(0)
  })

  it('reports the exit status of the command, not of the terminal wrapper', async () => {
    const result = await runInTty({ command: 'exit 9' })
    expect(result.exitCode).toBe(9)
  })

  it('ends the run when the deadline passes', async () => {
    const started = Date.now()
    const result = await runInTty({ command: 'sleep 30', timeoutMs: 500 })
    expect(result.timedOut).toBe(true)
    expect(Date.now() - started).toBeLessThan(20_000)
  }, 30_000)
})

describe.skipIf(liveHost === undefined)('terminal runs on a host', () => {
  it('gives the remote program a real terminal and reports its exit status', async () => {
    const result = await runInTty({ command: 'test -t 1 && echo is-tty; exit 3' }, { target: liveHost! })
    expect(result.output).toContain('is-tty')
    expect(result.exitCode).toBe(3)
    expect(result.merged).toBe(true)
  }, 60_000)
})
