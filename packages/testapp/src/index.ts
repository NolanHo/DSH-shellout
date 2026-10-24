/**
 * Scenario driver for the end-to-end checks.
 *
 * This package exists only so an isolated harness instance can exercise the
 * shell seam without an agent or a model: the profile mounts it as the app and
 * it runs one scenario from `DSH_SHELLOUT_SCENARIO`, prints a single JSON line,
 * and exits.
 *
 * @module dsh-shellout-testapp
 */

import type { Context } from '@deepseek-ai/cordis'
import type { ShellExecutor } from '@deepseek-ai/dsh-shell'

/** Plugin name used by the loader. */
export const name = 'shellout-testapp'

/** The scenario needs the shell service. */
export const inject = ['shell']

/** Prefix of the single result line; the test harness greps for it. */
export const RESULT_PREFIX = 'SHELLOUT-RESULT '

/**
 * Run the configured scenario after the tree is mounted.
 * @param ctx - owning context.
 */
export function apply(ctx: Context): void {
  const scenario = process.env.DSH_SHELLOUT_SCENARIO ?? 'basic'
  setTimeout(() => { void run(ctx, scenario) }, 50)
}

async function run(ctx: Context, scenario: string): Promise<void> {
  const shell = ctx.get('shell') as ShellExecutor | undefined
  if (shell === undefined) {
    emit({ scenario, error: 'no shell service' })
    process.exit(2)
  }
  try {
    switch (scenario) {
      case 'basic': {
        const execution = await shell.execute(shell.resolve({ command: 'echo hello; echo oops >&2; exit 3', timeoutMs: 10_000 }))
        const result = await execution.result()
        emit({
          scenario,
          hostPid: process.pid,
          exitCode: result.exitCode,
          stdout: result.stdout.text,
          stderr: result.stderr.text,
          timedOut: result.timedOut,
        })
        process.exit(0)
        return
      }
      case 'background': {
        const started = Date.now()
        const execution = await shell.execute(shell.resolve({
          command: 'sleep 600 & echo started',
          timeoutMs: 60_000,
        }))
        const result = await execution.result()
        const elapsedMs = Date.now() - started
        emit({ scenario, hostPid: process.pid, exitCode: result.exitCode, stdout: result.stdout.text, elapsedMs })
        process.exit(0)
        return
      }
      case 'lag': {
        // Burn a core inside the runner while sampling this process's event loop:
        // execution load must not reach the host.
        const execution = await shell.execute(shell.resolve({ command: 'timeout 8 sh -c \'while :; do :; done\'', timeoutMs: 60_000 }))
        const lag = await sampleLag(3_000)
        emit({ scenario, hostPid: process.pid, ...lag })
        execution.kill()
        await execution.done
        process.exit(0)
        return
      }
      case 'hold-then-die': {
        // Leave a long-running command behind and die without drain, so the
        // runner's boot lease is the only thing that can clean it up.
        const pidFile = process.env.DSH_SHELLOUT_PIDFILE
        const command = pidFile === undefined
          ? 'sleep 300'
          : `sleep 300 & echo $! > ${pidFile}; wait`
        const execution = await shell.execute(shell.resolve({ command, timeoutMs: 600_000 }))
        emit({ scenario, hostPid: process.pid, started: true })
        setTimeout(() => { process.kill(process.pid, 'SIGKILL') }, 1_500)
        void execution
        return
      }
      case 'runner-crash': {
        // Start a long command, report it started, then wait for the runner to
        // vanish; the execution must settle as killed with a provider note.
        const execution = await shell.execute(shell.resolve({ command: 'sleep 60', timeoutMs: 120_000 }))
        emit({ scenario, hostPid: process.pid, started: true })
        const result = await execution.result()
        emit({ scenario, satisfied: true, exitCode: result.exitCode, status: execution.status, stderr: result.stderr.text.trim() })
        process.exit(0)
        return
      }
      default: {
        emit({ scenario, error: `unknown scenario ${scenario}` })
        process.exit(2)
      }
    }
  } catch (error: unknown) {
    emit({ scenario, error: error instanceof Error ? error.message : String(error) })
    process.exit(1)
  }
}

async function sampleLag(durationMs: number): Promise<{ maxLagMs: number; samples: number }> {
  let maxLagMs = 0
  let samples = 0
  let last = Date.now()
  const timer = setInterval(() => {
    const now = Date.now()
    maxLagMs = Math.max(maxLagMs, now - last - 50)
    last = now
    samples += 1
  }, 50)
  await new Promise<void>((resolve) => { setTimeout(resolve, durationMs) })
  clearInterval(timer)
  return { maxLagMs, samples }
}

function emit(value: Record<string, unknown>): void {
  process.stdout.write(`${RESULT_PREFIX}${JSON.stringify(value)}\n`)
}
