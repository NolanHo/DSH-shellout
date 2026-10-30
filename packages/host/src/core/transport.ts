/**
 * How the client reaches a runner: a unix socket, or a command whose stdio
 * carries the same frames.
 *
 * The command form is what makes a remote host reachable without a new
 * protocol: `dsh-shellout-runner --mode attach --host …` bootstraps the host
 * and then speaks frames on its own stdin/stdout, so ssh keeps ownership of
 * identity and `ProxyJump`.
 */
import { spawn } from 'node:child_process'
import net from 'node:net'
import { Duplex } from 'node:stream'

/** Transport selection. */
export type TransportOptions =
  | { readonly kind: 'socket'; readonly path: string }
  | {
    readonly kind: 'command'
    readonly argv: readonly string[]
    readonly cwd?: string
    readonly env?: NodeJS.ProcessEnv
  }

/**
 * Open the transport.
 * @param options - socket path or command argv.
 * @param timeoutMs - bound on connect or spawn.
 * @returns a duplex carrying protocol frames.
 */
export async function openTransport(options: TransportOptions, timeoutMs: number): Promise<Duplex> {
  if (options.kind === 'socket') return await openSocketTransport(options.path, timeoutMs)
  return await openCommandTransport(options, timeoutMs)
}

/** Connect a unix socket. */
async function openSocketTransport(path: string, timeoutMs: number): Promise<Duplex> {
  return await new Promise<Duplex>((resolve, reject) => {
    const socket = net.connect(path)
    const timer = setTimeout(() => {
      socket.destroy()
      reject(new Error(`runner socket ${path} did not accept within ${timeoutMs}ms`))
    }, timeoutMs)
    timer.unref()
    socket.once('connect', () => {
      clearTimeout(timer)
      socket.removeAllListeners('error')
      resolve(socket)
    })
    socket.once('error', (error: NodeJS.ErrnoException) => {
      clearTimeout(timer)
      socket.destroy()
      reject(new Error(`runner socket ${path} unreachable: ${error.code ?? error.message}`))
    })
  })
}

/**
 * Spawn the command and use its stdio.
 *
 * stderr is forwarded rather than swallowed: the attach command reports its
 * bootstrap steps there, and a failed push is the first thing an operator needs.
 */
async function openCommandTransport(
  options: Extract<TransportOptions, { kind: 'command' }>,
  timeoutMs: number,
): Promise<Duplex> {
  const [program, ...args] = options.argv
  if (program === undefined) throw new Error('shellout: transport command is empty')
  const child = spawn(program, args, {
    cwd: options.cwd,
    env: options.env ?? process.env,
    stdio: ['pipe', 'pipe', 'pipe'],
  })
  child.stderr.on('data', (chunk: Buffer) => { process.stderr.write(chunk) })
  // The client speaks to one duplex: reads come from the child's stdout, writes
  // go to its stdin. Destroying that duplex must take the child with it, or a
  // reconnect would leave a second attach running.
  const stream = Duplex.from({
    readable: child.stdout as NodeJS.ReadableStream,
    writable: child.stdin as NodeJS.WritableStream,
  })
  stream.on('close', () => { if (child.exitCode === null) child.kill('SIGTERM') })
  return await new Promise<Duplex>((resolve, reject) => {
    const timer = setTimeout(() => {
      child.kill('SIGTERM')
      reject(new Error(`${program} did not start within ${timeoutMs}ms`))
    }, timeoutMs)
    timer.unref()
    child.once('spawn', () => {
      clearTimeout(timer)
      resolve(stream)
    })
    child.once('error', (error) => {
      clearTimeout(timer)
      reject(new Error(`cannot run ${program}: ${error.message}`))
    })
  })
}
