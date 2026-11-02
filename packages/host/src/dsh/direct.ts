/**
 * Run commands on a host that cannot run this plane's runner.
 *
 * The runner is a Linux executable; a macOS or Windows host cannot start it, and
 * demanding one would make the tool useless exactly where a person needs it. The
 * direct path keeps the parts a caller depends on — the command's real exit
 * status, stdout and stderr kept apart, a deadline that ends the process — and
 * gives up the parts that need state on the host: nothing is indexed, and a
 * command outliving the connection cannot be adopted.
 */
import { spawn } from 'node:child_process'

import type { HostEntry } from './hosts.js'

/** One direct run. */
export interface DirectRequest {
  /** Command line handed to the shell on the host. */
  readonly command: string
  /** Working directory on the host. */
  readonly cwd?: string
  /** Shell on the host; defaults to `bash`. */
  readonly shell?: string
  /** Deadline in milliseconds. */
  readonly timeoutMs?: number
  /** Caller cancellation. */
  readonly signal?: AbortSignal
}

/** What a direct run produced. */
export interface DirectResult {
  /** Standard output, as the host sent it. */
  readonly stdout: string
  /** Standard error, kept apart from stdout. */
  readonly stderr: string
  /** Remote exit status; `255` also means the ssh connection itself failed. */
  readonly exitCode: number | null
  /** Signal that ended the local ssh process, if any. */
  readonly signal: string | null
  /** True when the deadline ended the run. */
  readonly timedOut: boolean
}

/** Default deadline, matching the runner path's foreground cap. */
const DEFAULT_TIMEOUT_MS = 120_000

/** Bytes kept per stream. */
const MAX_STREAM_BYTES = 4 * 1024 * 1024

/**
 * Quote one word for a POSIX shell.
 * @param word - the word.
 * @returns the word quoted so a shell reproduces it exactly.
 */
function quote(word: string): string {
  return `'${word.replaceAll("'", `'\\''`)}'`
}

/**
 * The remote script for one run.
 *
 * `cwd` is applied with `cd` rather than ssh options so the same script works on
 * every shell this path can meet.
 *
 * @param request - what to run.
 * @returns the script handed to the host login shell.
 */
export function directScript(request: DirectRequest): string {
  const shell = request.shell ?? 'bash'
  const change = request.cwd === undefined ? '' : `cd ${quote(request.cwd)} || exit 255; `
  return `${change}exec ${shell} -c ${quote(request.command)}`
}

/**
 * argv for one direct run.
 * @param entry - target host.
 * @param request - what to run.
 * @returns the program and its arguments.
 */
export function directArgv(entry: HostEntry, request: DirectRequest): { readonly program: string; readonly args: readonly string[] } {
  const words = [...entry.ssh ?? ['ssh', '-o', 'BatchMode=yes'], entry.target, directScript(request)]
  return { program: words[0]!, args: words.slice(1) }
}

/**
 * Run one command through ssh without a runner on the host.
 * @param entry - target host.
 * @param request - what to run.
 * @returns both streams with the remote exit status.
 * @throws when ssh itself cannot be started.
 */
export async function runDirect(entry: HostEntry, request: DirectRequest): Promise<DirectResult> {
  const { program, args } = directArgv(entry, request)
  const child = spawn(program, [...args], { stdio: ['ignore', 'pipe', 'pipe'] })
  let stdout = ''
  let stderr = ''
  const keep = (current: string, chunk: Buffer): string =>
    current.length >= MAX_STREAM_BYTES ? current : current + chunk.toString()
  child.stdout.on('data', (chunk: Buffer) => { stdout = keep(stdout, chunk) })
  child.stderr.on('data', (chunk: Buffer) => { stderr = keep(stderr, chunk) })

  let timedOut = false
  const deadline = setTimeout(() => {
    timedOut = true
    child.kill('SIGTERM')
  }, request.timeoutMs ?? DEFAULT_TIMEOUT_MS)
  const onAbort = (): void => { child.kill('SIGTERM') }
  request.signal?.addEventListener('abort', onAbort, { once: true })

  try {
    const { code, signal } = await new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
      child.once('error', reject)
      child.once('exit', (exitCode, exitSignal) => { resolve({ code: exitCode, signal: exitSignal }) })
    })
    return { stdout, stderr, exitCode: code, signal, timedOut }
  } finally {
    clearTimeout(deadline)
    request.signal?.removeEventListener('abort', onAbort)
  }
}

/**
 * Ask a host what it is, so the caller can choose a path before running anything.
 * @param entry - target host.
 * @returns `uname -s` and `uname -m`, or undefined when the probe failed.
 */
export async function probePlatform(entry: HostEntry): Promise<{ readonly os: string; readonly arch: string } | undefined> {
  const result = await runDirect(entry, { command: 'uname -s; uname -m', timeoutMs: 20_000 })
  if (result.exitCode !== 0) return undefined
  const [os, arch] = result.stdout.trim().split('\n')
  return os === undefined || arch === undefined ? undefined : { os: os.trim(), arch: arch.trim() }
}
