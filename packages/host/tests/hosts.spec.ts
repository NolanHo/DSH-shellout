/**
 * Host aliases are the only thing the model names, so a wrong one has to be
 * answered with the configured names rather than with a shell error from
 * whatever machine the typo happened to reach.
 */
import { describe, expect, it } from 'vitest'

import { attachArgv, resolveHost, suggestAliases, type Hosts } from '../src/dsh/hosts.ts'

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

  it('says so when nothing is close and lists what exists', () => {
    expect(() => resolveHost(hosts, 'zzzzzzzzzz')).toThrow(/Configured hosts: gpu-a, gpu-b, lab/)
    expect(() => resolveHost({}, 'anything')).toThrow(/No hosts are configured/)
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
