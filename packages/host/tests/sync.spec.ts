/**
 * Copying to a host is destructive only when asked: a mirror deletes host files,
 * so the request must say so and the command must carry it.
 */
import { describe, expect, it } from 'vitest'

import { execFileSync } from 'node:child_process'

import { rsyncArgv, shellQuote, streamCommands, type SyncRequest } from '../src/dsh/sync.ts'
import type { HostEntry } from '../src/dsh/hosts.ts'

const host: HostEntry = { target: 'root@gpu-a.example', ssh: ['ssh', '-i', '/root/.ssh/key', '-p', '2222'] }
const request: SyncRequest = { from: '/work/project', to: '/srv/project' }

describe('sync to a host', () => {
  it('copies directory contents and never deletes unless asked', () => {
    expect(rsyncArgv(host, request)).toEqual([
      'rsync', '-az',
      '-e', 'ssh -i /root/.ssh/key -p 2222',
      '/work/project/', 'root@gpu-a.example:/srv/project/',
    ])
    expect(rsyncArgv(host, { ...request, delete: true })).toContain('--delete')
  })

  it('keeps a trailing slash from doubling', () => {
    expect(rsyncArgv(host, { from: '/a/', to: '/b/' })).toContain('/a/')
    expect(rsyncArgv(host, { from: '/a/', to: '/b/' })).toContain('root@gpu-a.example:/b/')
  })

  it('builds a stream for a host without rsync', () => {
    const { local, host: command } = streamCommands(host, { from: '/work/a b', to: '/srv/project' })
    expect(local).toEqual(['tar', '-C', '/work/a b', '-cf', '-', '.'])
    expect(command).toContain("'ssh'")
    expect(command).toContain('root@gpu-a.example')
    expect(command).toContain('tar -C')
    expect(command).toContain('mkdir -p')
  })

  it('quotes a word so a shell reproduces it exactly', () => {
    // A path with a space and a quote is the case that silently splits a remote
    // command when the quoting is wrong.
    const word = "/srv/it's a path"
    const echoed = execFileSync('sh', ['-c', `printf %s ${shellQuote(word)}`], { encoding: 'utf8' })
    expect(echoed).toBe(word)
  })
})
