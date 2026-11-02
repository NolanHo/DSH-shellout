/**
 * A real copy to a real host, when one is named.
 *
 * The unit tests prove the command lines; only an actual transfer proves the
 * file arrives, the directory contents land where asked, and a delete request is
 * what removes anything.
 */
import { execFileSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

import { describe, expect, it } from 'vitest'

import type { HostEntry } from '../src/dsh/hosts.ts'
import { syncToHost } from '../src/dsh/sync.ts'

const target = process.env.DSH_SYNC_HOST

/** Run one command on the host under test. */
function host(command: string): string {
  return execFileSync('ssh', ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', target!, command], { encoding: 'utf8' })
}

describe.skipIf(target === undefined)('sync to a real host', () => {
  it('copies a directory, mirrors only when asked, and reports what it did', async () => {
    const entry: HostEntry = { target: target! }
    const local = mkdtempSync(join(tmpdir(), 'dsh-sync-'))
    mkdirSync(join(local, 'nested'))
    writeFileSync(join(local, 'a.txt'), 'alpha\n')
    writeFileSync(join(local, 'nested', 'b.txt'), 'beta\n')
    const remote = `/tmp/dsh-sync-${process.pid}`
    host(`mkdir -p ${remote}`)

    try {
      const copied = await syncToHost(entry, { from: local, to: remote })
      expect(copied.mode).toBe('rsync')
      expect(copied.deleted).toBe(false)
      expect(host(`cat ${remote}/a.txt; cat ${remote}/nested/b.txt`)).toBe('alpha\nbeta\n')

      // A host file absent from the source survives a copy that did not ask to delete.
      host(`printf extra > ${remote}/extra.txt`)
      await syncToHost(entry, { from: local, to: remote })
      expect(host(`test -f ${remote}/extra.txt && echo kept`)).toBe('kept\n')

      const mirrored = await syncToHost(entry, { from: local, to: remote, delete: true })
      expect(mirrored.deleted).toBe(true)
      expect(host(`test -f ${remote}/extra.txt || echo gone`)).toBe('gone\n')
    } finally {
      host(`find ${remote} -mindepth 1 -delete 2>/dev/null; rmdir ${remote} 2>/dev/null; true`)
    }
  }, 120_000)
})
