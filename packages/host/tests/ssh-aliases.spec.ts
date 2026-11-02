/**
 * The parser behind automatic aliases: only literal `Host` names count, the
 * `Include` files count too, and wildcards never make a name valid.
 */
import { mkdtempSync, mkdirSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import { aliasesFromSshConfig } from '../src/dsh/ssh-aliases.ts'

/** Write one config tree and return its root. */
function configTree(main: string, includes: Record<string, string> = {}): string {
  const root = mkdtempSync(join(tmpdir(), 'dsh-ssh-config-'))
  writeFileSync(join(root, 'config'), main)
  for (const [name, text] of Object.entries(includes)) {
    mkdirSync(join(root, name, '..'), { recursive: true })
    writeFileSync(join(root, name), text)
  }
  return join(root, 'config')
}

describe('ssh config aliases', () => {
  it('collects literal host names and follows Include', () => {
    const path = configTree(
      `Include config.d/*.conf
Host nolan-mac
  HostName nolanho-mac.local
Host *.example.com
  User ops
Host * !excluded
  IdentityFile ~/.ssh/default
`,
      { 'config.d/work.conf': 'Host workstation\n  HostName 10.0.0.9\n' },
    )
    const aliases = aliasesFromSshConfig(path)
    expect(aliases).toEqual(['nolan-mac', 'workstation'])
  })

  it('does not treat a wildcard block as a host name', () => {
    const path = configTree('Host *\n  User root\nHost 10.0.0.*\n  User ops\n')
    expect(aliasesFromSshConfig(path)).toEqual([])
  })

  it('returns nothing for a missing config', () => {
    expect(aliasesFromSshConfig('/nonexistent/config')).toEqual([])
  })
})
