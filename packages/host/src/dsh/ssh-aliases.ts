/**
 * Host names the account's ssh already defines.
 *
 * Naming every machine in the plugin's `hosts` map duplicates what
 * `~/.ssh/config` already answers. A name that config defines is accepted as a
 * host on its own; only names nobody defines are refused, so a typo still cannot
 * reach an unintended machine.
 *
 * Only literal `Host` patterns count. A wildcard block (`Host *`, `Host 10.0.0.*`)
 * describes defaults, not a host, and treating it as a name would accept every
 * typo anyone types.
 */
import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs'
import { homedir } from 'node:os'
import { dirname, isAbsolute, join } from 'node:path'

/** Depth limit for `Include` expansion, which can nest. */
const MAX_INCLUDE_DEPTH = 8

/**
 * Expand one path as ssh does: `~`, relative to the including file, or absolute.
 * @param value - path from the config.
 * @param base - directory of the file naming it.
 * @returns the resolved path.
 */
function resolvePath(value: string, base: string): string {
  if (value === '~') return homedir()
  if (value.startsWith('~/')) return join(homedir(), value.slice(2))
  return isAbsolute(value) ? value : join(base, value)
}

/**
 * Read a config file and every file it includes, in order.
 * @param path - config file to read.
 * @param depth - current include depth.
 * @returns the text of each file, outermost first.
 */
function readConfig(path: string, depth = 0): string[] {
  if (depth > MAX_INCLUDE_DEPTH || !existsSync(path)) return []
  const text = readFileSync(path, 'utf8')
  const parts = [text]
  for (const line of text.split('\n')) {
    const match = /^\s*Include\s+(.+)$/i.exec(line)
    if (match === null) continue
    for (const word of match[1]!.trim().split(/\s+/)) {
      const resolved = resolvePath(word, dirname(path))
      if (!resolved.includes('*')) {
        parts.push(...readConfig(resolved, depth + 1))
        continue
      }
      // An Include may name a glob in its last segment (`config.d/*.conf`).
      const directory = dirname(resolved)
      const pattern = resolved.slice(directory.length + 1)
      if (!existsSync(directory)) continue
      const matcher = new RegExp(`^${pattern.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`)
      for (const entry of readdirSync(directory).sort()) {
        if (!matcher.test(entry)) continue
        if (!statSync(join(directory, entry)).isFile()) continue
        parts.push(...readConfig(join(directory, entry), depth + 1))
      }
    }
  }
  return parts
}

/**
 * Host names the config defines literally.
 * @param configPath - the ssh config; defaults to `~/.ssh/config`.
 * @returns the names, sorted.
 */
export function aliasesFromSshConfig(configPath = join(homedir(), '.ssh', 'config')): string[] {
  const names = new Set<string>()
  for (const text of readConfig(configPath)) {
    for (const raw of text.split('\n')) {
      const line = raw.trim()
      if (line === '' || line.startsWith('#')) continue
      // `HostName` also begins with `Host`; the whitespace keeps them apart.
      const match = /^Host\s+(.+)$/i.exec(line)
      if (match === null) continue
      for (const word of match[1]!.trim().split(/\s+/)) {
        if (word.startsWith('!')) continue
        if (word.includes('*') || word.includes('?')) continue
        names.add(word)
      }
    }
  }
  return [...names].sort()
}
