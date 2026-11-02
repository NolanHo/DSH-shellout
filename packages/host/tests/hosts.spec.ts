/**
 * Host aliases are the only thing the model names, so a wrong one has to be
 * answered with the configured names rather than with a shell error from
 * whatever machine the typo happened to reach.
 */
import { describe, expect, it } from 'vitest'

import { attachArgv, resolveHost, splitTarget, sshWordsFor, suggestAliases, type Hosts } from '../src/dsh/hosts.ts'

const hosts: Hosts = {
  'gpu-a': { target: 'root@gpu-a.example', root: '/root/.dsh-shellout', cwd: '/workspace', shell: '/bin/bash' },
  'gpu-b': { target: 'root@gpu-b.example' },
  lab: { target: 'nolan-lab' },
}

describe('host registry', () => {
  it('resolves a configured alias', () => {
    expect(resolveHost(hosts, 'lab').target).toBe('nolan-lab')
  })

  it('answers an unknown alias with the closest configured ones', () => {
    const suggestions = suggestAliases(hosts, 'gpu-c')
    expect(suggestions).toContain('gpu-a')
    expect(suggestions).toContain('gpu-b')
    expect(() => resolveHost(hosts, 'gpu-c')).toThrow(/Did you mean gpu-a, gpu-b/)
  })

  it('accepts a name the ssh config defines, and a typed address', () => {
    const aliases = ['gpu-a', 'workstation']
    expect(resolveHost(hosts, 'workstation', { aliases })).toEqual({ target: 'workstation' })
    expect(resolveHost({}, 'root@10.0.0.5:2222', { aliases })).toEqual({ target: 'root@10.0.0.5:2222' })
    // Without the ssh list the same name is refused, so the fallback is what
    // makes it work rather than a loosened check.
    expect(() => resolveHost(hosts, 'workstation')).toThrow(/unknown remote_host/)
  })

  it('says so when nothing is close and lists what exists', () => {
    expect(() => resolveHost(hosts, 'zzzzzzzzzz')).toThrow(/Known hosts: gpu-a, gpu-b, lab/)
    expect(() => resolveHost({}, 'anything')).toThrow(/No hosts are configured/)
  })

  it('carries a port written into the target', () => {
    expect(splitTarget('root@10.0.0.5:2222')).toEqual({ destination: 'root@10.0.0.5', port: '2222' })
    expect(splitTarget('nolan-mac')).toEqual({ destination: 'nolan-mac' })
    // A host that already names its port keeps it, and is not given a second one.
    const entry = { target: 'root@10.0.0.5:2222', ssh: ['ssh', '-p', '2200'] }
    expect(sshWordsFor(entry)).toEqual(['ssh', '-p', '2200'])
    expect(sshWordsFor({ target: 'root@10.0.0.5:2222' })).toEqual(['ssh', '-o', 'BatchMode=yes', '-p', '2222'])
    expect(attachArgv(entry)).toContain('root@10.0.0.5')
  })

  it('builds an attach command from the entry', () => {
    expect(attachArgv(hosts['gpu-a']!)).toEqual([
      'dsh-shellout-runner', '--mode', 'attach', '--host', 'root@gpu-a.example',
      '--ssh', 'ssh', '--ssh', '-o', '--ssh', 'BatchMode=yes',
      '--remote-root', '/root/.dsh-shellout',
    ])
    expect(attachArgv({ target: 'h', ssh: ['ssh', '-p', '2222'] })).toEqual([
      'dsh-shellout-runner', '--mode', 'attach', '--host', 'h',
      '--ssh', 'ssh', '--ssh', '-p', '--ssh', '2222',
    ])
  })
})
