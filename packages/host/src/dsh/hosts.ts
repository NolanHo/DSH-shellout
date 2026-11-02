/**
 * Named hosts the `shell` tool can target.
 *
 * A host entry is everything the attach command needs: how to reach the machine
 * over ssh, where its runner keeps state, and which shell to run commands in.
 * Aliases are the only thing the model names, so a typo is answered with the
 * closest configured names instead of a shell error from a wrong machine.
 */

/** One configured host. */
export interface HostEntry {
  /** ssh destination: an alias from `~/.ssh/config` or `user@host`. */
  readonly target: string
  /** ssh argv words before the destination; defaults to `['ssh', '-o', 'BatchMode=yes']`. */
  readonly ssh?: readonly string[]
  /** Remote state root; defaults to the host's `~/.dsh-shellout`. */
  readonly root?: string
  /** Runner binary on this controller used to attach; defaults to `dsh-shellout-runner`. */
  readonly runner?: string
  /** Working directory for commands here; defaults to the remote home. */
  readonly cwd?: string
  /** Shell that runs the command; defaults to `/bin/bash` locally, `bash` on a host. */
  readonly shell?: string
  /**
   * How commands run here. `runner` gives adoption and history and needs a Linux
   * host this plane can bootstrap; `ssh` runs through the host's own ssh without
   * installing anything. Omitted means choose by probing the host once.
   */
  readonly mode?: HostMode
}

/** How a host executes commands. */
export type HostMode = 'runner' | 'ssh'

/** Alias to host. */
export type Hosts = Readonly<Record<string, HostEntry>>

/** Distances above this are not worth offering as a suggestion. */
const MAX_SUGGESTION_DISTANCE = 3

/** Levenshtein distance, bounded by the shorter string. */
function distance(left: string, right: string): number {
  const a = [...left.toLowerCase()]
  const b = [...right.toLowerCase()]
  const rows = a.length + 1
  const columns = b.length + 1
  let previous = Array.from({ length: columns }, (_, index) => index)
  for (let row = 1; row < rows; row += 1) {
    const current = [row]
    for (let column = 1; column < columns; column += 1) {
      current[column] = Math.min(
        previous[column]! + 1,
        current[column - 1]! + 1,
        previous[column - 1]! + (a[row - 1] === b[column - 1] ? 0 : 1),
      )
    }
    previous = current
  }
  return previous[columns - 1]!
}

/**
 * Aliases that look like the one the caller asked for.
 * @param hosts - configured aliases.
 * @param wanted - the alias that does not exist.
 * @returns up to three candidates, closest first.
 */
export function suggestAliases(hosts: Hosts, wanted: string): string[] {
  return Object.keys(hosts)
    .map((alias) => ({ alias, cost: distance(alias, wanted) }))
    .filter(({ cost }) => cost <= MAX_SUGGESTION_DISTANCE)
    .sort((left, right) => left.cost - right.cost || left.alias.localeCompare(right.alias))
    .slice(0, 3)
    .map(({ alias }) => alias)
}

/**
 * Look up one host.
 * @param hosts - configured aliases.
 * @param alias - the name the caller used.
 * @returns the entry.
 * @throws when the alias is not configured, naming the closest ones.
 */
export function resolveHost(hosts: Hosts, alias: string): HostEntry {
  const entry = hosts[alias]
  if (entry !== undefined) return entry
  const suggestions = suggestAliases(hosts, alias)
  const hint = suggestions.length > 0 ? ` Did you mean ${suggestions.join(', ')}?` : ''
  const known = Object.keys(hosts)
  return fail(
    `unknown remote_host "${alias}".${hint}${known.length > 0 ? ` Configured hosts: ${known.join(', ')}.` : ' No hosts are configured.'}`,
  )
}

/** Throw, in a position the compiler still types as `HostEntry`. */
function fail(message: string): never {
  throw new Error(`shell: ${message}`)
}

/**
 * The attach argv that reaches one host.
 * @param entry - configured host.
 * @returns argv for the transport command.
 */
export function attachArgv(entry: HostEntry): string[] {
  const argv = [entry.runner ?? 'dsh-shellout-runner', '--mode', 'attach', '--host', entry.target]
  for (const word of entry.ssh ?? ['ssh', '-o', 'BatchMode=yes']) argv.push('--ssh', word)
  if (entry.root !== undefined) argv.push('--remote-root', entry.root)
  return argv
}
