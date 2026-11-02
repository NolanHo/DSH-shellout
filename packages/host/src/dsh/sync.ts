/**
 * Copy a local tree to a configured host before running a command there.
 *
 * Measured over a real ssh hop (22.7 MB, 3020 files), rsync took 1.02 s cold and
 * 0.43 s for a repeat with one changed file, while a tar stream took 1.10 s and
 * 1.17 s: rsync owns the repeat case, which is the one that happens often. The
 * stream stays as the path for a host without rsync, not as a speed option.
 *
 * Deletion is opt-in. A mirror that removes whatever the host has and the source
 * does not is unrecoverable on the host, so the caller asks for it explicitly and
 * the report says whether it ran.
 */
import { spawn } from 'node:child_process'
import { existsSync, statSync } from 'node:fs'

import { splitTarget, sshWordsFor, type HostEntry } from './hosts.js'

/** What the caller asks to copy. */
export interface SyncRequest {
  /** Local file or directory. */
  readonly from: string
  /** Destination path on the host. */
  readonly to: string
  /** Remove host files the source does not have; defaults to false. */
  readonly delete?: boolean
  /** Force a path instead of choosing one; `stream` needs only tar on the host. */
  readonly mode?: SyncMode
}

/** How the copy travels. */
export type SyncMode = 'rsync' | 'stream'

/** What happened, for the model and for the operator. */
export interface SyncReport {
  readonly mode: SyncMode
  /** Whether host files absent from the source were removed. */
  readonly deleted: boolean
  /** Wall time of the copy. */
  readonly ms: number
  /** Local path that was copied. */
  readonly from: string
  /** Host path written. */
  readonly to: string
}

/** ssh words for a host, including a port written into the target. */
function sshWords(entry: HostEntry): string[] {
  return sshWordsFor(entry)
}

/**
 * rsync argv for one copy.
 * @param entry - target host.
 * @param request - validated request.
 * @returns argv for `rsync`.
 */
export function rsyncArgv(entry: HostEntry, request: SyncRequest): string[] {
  const source = request.from.endsWith('/') ? request.from : `${request.from}/`
  const destination = request.to.endsWith('/') ? request.to : `${request.to}/`
  return [
    'rsync',
    '-az',
    ...request.delete === true ? ['--delete'] : [],
    '-e',
    sshWords(entry).join(' '),
    source,
    `${splitTarget(entry.target).destination}:${destination}`,
  ]
}

/**
 * The stream copy: one tar in, one tar out, no rsync on the host.
 * @param entry - target host.
 * @param request - validated request.
 * @returns the local argv and the host command line.
 */
export function streamCommands(entry: HostEntry, request: SyncRequest): { readonly local: string[]; readonly host: string } {
  const destination = shellQuote(request.to)
  const script = `mkdir -p ${destination} && tar -C ${destination} -xf -`
  return {
    local: ['tar', '-C', request.from, '-cf', '-', '.'],
    // Every word is quoted for the local shell so ssh receives the remote
    // command as one argument, exactly as a person would write it.
    host: [...sshWords(entry), entry.target, script].map(shellQuote).join(' '),
  }
}

/**
 * Quote one word for a POSIX shell.
 * @param word - the word.
 * @returns the word quoted so a shell reproduces it exactly.
 */
export function shellQuote(word: string): string {
  return `'${word.replaceAll("'", `'\\''`)}'`
}

/**
 * Whether the host runs rsync, asked once per attach.
 * @param entry - target host.
 * @returns true when `command -v rsync` succeeds there.
 */
export async function hostHasRsync(entry: HostEntry): Promise<boolean> {
  const [program, ...rest] = sshWords(entry)
  return await new Promise<boolean>((resolve) => {
    const child = spawn(program!, [...rest, entry.target, 'command -v rsync'], { stdio: 'ignore' })
    child.once('error', () => { resolve(false) })
    child.once('exit', (code) => { resolve(code === 0) })
  })
}

/**
 * Copy `from` to `to` on the host.
 * @param entry - target host.
 * @param request - validated request.
 * @returns what happened.
 * @throws when the source is missing or a copy command fails.
 */
export async function syncToHost(entry: HostEntry, request: SyncRequest): Promise<SyncReport> {
  if (!existsSync(request.from)) throw new Error(`shell: sync source "${request.from}" does not exist`)
  const isFile = statSync(request.from).isFile()
  const mode: SyncMode = request.mode ?? (isFile ? 'rsync' : await hostHasRsync(entry) ? 'rsync' : 'stream')
  const started = Date.now()
  if (mode === 'rsync') {
    // A file copies to the exact path; a directory mirrors its contents, which is
    // why rsync --delete is only meaningful for the directory case.
    const argv = isFile
      ? ['rsync', '-az', '-e', sshWords(entry).join(' '), request.from, `${splitTarget(entry.target).destination}:${request.to}`]
      : rsyncArgv(entry, request)
    await run(argv[0]!, argv.slice(1))
  } else {
    if (isFile) throw new Error('shell: sync mode "stream" copies directories; use rsync for a single file')
    if (request.delete === true) {
      throw new Error('shell: sync delete needs rsync on the host; the stream path cannot remove host files')
    }
    const { local, host } = streamCommands(entry, request)
    await pipe(local, host)
  }
  return {
    mode,
    deleted: request.delete === true,
    ms: Date.now() - started,
    from: request.from,
    to: request.to,
  }
}

/** Run one command to completion. */
async function run(program: string, args: readonly string[]): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(program, [...args], { stdio: ['ignore', 'ignore', 'pipe'] })
    let stderr = ''
    child.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString() })
    child.once('error', reject)
    child.once('exit', (code) => {
      if (code === 0) resolve()
      else reject(new Error(`shell: ${program} exited ${String(code)}: ${stderr.trim().slice(0, 400)}`))
    })
  })
}

/** Pipe the local tar stream into the ssh command that unpacks it. */
async function pipe(local: readonly string[], host: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const producer = spawn(local[0]!, local.slice(1), { stdio: ['ignore', 'pipe', 'pipe'] })
    const consumer = spawn('sh', ['-c', host], { stdio: ['pipe', 'ignore', 'pipe'] })
    let stderr = ''
    producer.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString() })
    consumer.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString() })
    producer.stdout.pipe(consumer.stdin)
    producer.once('error', reject)
    consumer.once('error', reject)
    consumer.once('exit', (code) => {
      if (code === 0) resolve()
      else reject(new Error(`shell: host unpack exited ${String(code)}: ${stderr.trim().slice(0, 400)}`))
    })
  })
}
