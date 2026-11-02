/**
 * Run one command with a terminal attached, locally or on a configured host.
 *
 * A terminal is a different contract from a pipe: output arrives as one merged
 * stream carrying control sequences, and the process is bound to this
 * connection. Nothing is adopted after the connection ends, which is why `tty`
 * is opt-in and the session is never indexed for a later read.
 *
 * Both paths allocate the terminal at the outermost layer: `script` locally and
 * `ssh -tt` on a host, so the runner protocol keeps a single-stream contract and
 * needs no terminal support of its own.
 */
import { spawn, type ChildProcess } from 'node:child_process'

import { splitTarget, sshWordsFor, type HostEntry } from './hosts.js'

/** One terminal run. */
export interface TtyRequest {
  /** Command line handed to the shell. */
  readonly command: string
  /** Working directory; local default is the caller's, host default is the account's home. */
  readonly cwd?: string
  /** Shell for the local path; the host path uses the account's login shell. */
  readonly shell?: string
  /** Deadline in milliseconds; the process group is killed when it passes. */
  readonly timeoutMs?: number
  /** Caller cancellation. */
  readonly signal?: AbortSignal
}

/** What the terminal produced. */
export interface TtyResult {
  /** Merged stdout, stderr, and terminal control sequences. */
  readonly output: string
  /** Exit status, or null when a signal ended the process. */
  readonly exitCode: number | null
  /** Signal that ended it, if any. */
  readonly signal: string | null
  /** True when the deadline ended the run. */
  readonly timedOut: boolean
  /** Always true: a terminal merges the streams. */
  readonly merged: true
}

/** How much of a terminal stream is kept; a runaway program must not fill memory. */
const MAX_OUTPUT_BYTES = 1_048_576

/** Default deadline, matching the non-terminal path's foreground cap. */
const DEFAULT_TIMEOUT_MS = 120_000

/**
 * Quote one word for a POSIX shell.
 * @param word - the word.
 * @returns the word quoted so a shell reproduces it exactly.
 */
function quote(word: string): string {
  return `'${word.replaceAll("'", `'\\''`)}'`
}

/**
 * argv for one terminal run.
 *
 * The host path hands ssh a script rather than a word list, because `ssh -tt`
 * hands its argument to the login shell; quoting it here keeps a path with a
 * space intact on the other side.
 *
 * @param request - what to run.
 * @param host - host to run on; absent runs locally.
 * @returns the program and its arguments.
 */
export function ttyArgv(request: TtyRequest, host?: HostEntry): { readonly program: string; readonly args: readonly string[]; readonly cwd?: string } {
  const shell = request.shell ?? '/bin/bash'
  const changeDirectory = request.cwd === undefined ? '' : `cd ${quote(request.cwd)} && `
  const script = `${changeDirectory}exec ${shell} -c ${quote(request.command)}`
  if (host === undefined) {
    return { program: 'script', args: ['-qec', script, '/dev/null'], ...request.cwd === undefined ? {} : { cwd: request.cwd } }
  }
  const words = [...sshWordsFor(host), '-tt', splitTarget(host.target).destination, script]
  return { program: words[0]!, args: words.slice(1) }
}

/**
 * Run one command with a terminal attached.
 * @param request - what to run.
 * @param host - host to run on; absent runs locally.
 * @returns the merged output with its exit status.
 * @throws when the process cannot be started at all.
 */
export async function runInTty(request: TtyRequest, host?: HostEntry): Promise<TtyResult> {
  const { program, args, cwd } = ttyArgv(request, host)
  const child: ChildProcess = spawn(program, [...args], {
    ...cwd === undefined ? {} : { cwd },
    // A terminal owns the connection: the child gets its own group so a deadline
    // can end everything it started, not just the first process.
    detached: true,
    stdio: ['ignore', 'pipe', 'pipe'],
  })

  let output = ''
  let truncated = false
  const collect = (chunk: Buffer): void => {
    if (output.length >= MAX_OUTPUT_BYTES) { truncated = true; return }
    output += chunk.toString()
    if (output.length > MAX_OUTPUT_BYTES) {
      output = output.slice(0, MAX_OUTPUT_BYTES)
      truncated = true
    }
  }
  child.stdout?.on('data', collect)
  child.stderr?.on('data', collect)

  let timedOut = false
  const deadline = setTimeout(() => {
    timedOut = true
    killGroup(child)
  }, request.timeoutMs ?? DEFAULT_TIMEOUT_MS)

  const onAbort = (): void => { killGroup(child) }
  request.signal?.addEventListener('abort', onAbort, { once: true })

  try {
    const { code, signal } = await new Promise<{ code: number | null; signal: string | null }>((resolve, reject) => {
      child.once('error', reject)
      child.once('exit', (exitCode, exitSignal) => { resolve({ code: exitCode, signal: exitSignal }) })
    })
    return {
      output: truncated ? `${output}\n[terminal output truncated at ${String(MAX_OUTPUT_BYTES)} bytes]` : output,
      exitCode: code,
      signal,
      timedOut,
      merged: true,
    }
  } finally {
    clearTimeout(deadline)
    request.signal?.removeEventListener('abort', onAbort)
  }
}

/** End the child's whole process group; a terminal session starts more than one process. */
function killGroup(child: ChildProcess): void {
  if (child.pid === undefined) return
  try {
    process.kill(-child.pid, 'SIGTERM')
  } catch {
    // The group is already gone; nothing to end.
  }
  setTimeout(() => {
    try {
      process.kill(-child.pid!, 'SIGKILL')
    } catch {
      // The group ended between the two signals.
    }
  }, 2000).unref()
}
