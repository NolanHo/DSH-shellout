/**
 * The `shell` tool reaches a named host through the same attach path a
 * deployment uses, and returns the local result shape.
 */
import { spawn, type ChildProcess } from 'node:child_process'
import { existsSync, mkdtempSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'

import { describe, expect, it } from 'vitest'

import { apply as mountPlugin } from '../src/index.ts'

const repo = resolve(import.meta.dirname, '../../..')
const runner = resolve(repo, 'runner-rs/target/x86_64-unknown-linux-musl/release/dsh-shellout-runner')
const fakeRemote = resolve(repo, 'scripts/fake-remote.sh')
const ready = existsSync(runner) && existsSync(fakeRemote)

interface CapturedTool {
  name: string
  execute: (args: Record<string, unknown>, exec: { signal: AbortSignal }) => Promise<string>
}

/** Minimal `tools` service: capture registrations so the test can call them. */
function toolsStub(captured: CapturedTool[]) {
  return {
    register(tool: CapturedTool) {
      captured.push(tool)
      return () => {}
    },
  }
}

/** Start a runner on a private socket so the plugin connects instead of falling back. */
async function startRunner(root: string): Promise<ChildProcess> {
  const socket = join(root, 'control.sock')
  const child = spawn(runner, ['--socket', socket, '--state', join(root, 'state'), '--history', join(root, 'history.sqlite')], {
    stdio: 'ignore',
  })
  const deadline = Date.now() + 20_000
  while (!existsSync(socket)) {
    if (Date.now() > deadline) throw new Error('the runner did not start')
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  return child
}

async function harness() {
  const cordis = await import(pathToFileURL(
    join(repo, 'node_modules/@deepseek-ai/cordis/index.js'),
  ).href).catch(async () => await import('@deepseek-ai/cordis'))
  const { Context } = cordis as { Context: new () => any }
  const ctx = new Context()
  const captured: CapturedTool[] = []
  ctx.provide('tools', toolsStub(captured))
  const root = mkdtempSync(join(tmpdir(), 'dsh-shell-tool-'))
  const runnerProcess = await startRunner(root)
  await mountPlugin(ctx, {
    socketPath: join(root, 'control.sock'),
    fallback: 'local',
    hosts: {
      'test-host': { target: 'fake', ssh: [fakeRemote], root: join(root, 'remote'), runner },
    },
  })
  return { ctx, captured, runnerProcess }
}

describe.skipIf(!ready)('shell tool', () => {
  it('runs locally without a host and remotely with one', async () => {
    const { ctx, captured, runnerProcess } = await harness()
    try {
      const tool = captured.find((candidate) => candidate.name === 'shell')
      expect(tool, `registered tools: ${captured.map((c) => c.name).join(', ')}`).toBeDefined()
      const local = await tool!.execute({ command: 'printf local-ok' }, { signal: new AbortController().signal })
      expect(local).toContain('host: local')
      expect(local).toContain('local-ok')

      const remote = await tool!.execute(
        { command: 'printf remote-ok', remote_host: 'test-host' },
        { signal: new AbortController().signal },
      )
      expect(remote).toContain('host: test-host')
      expect(remote).toContain('remote-ok')
      expect(remote).toContain('exit: 0')
    } finally {
      ctx.dispose?.()
      runnerProcess.kill('SIGTERM')
    }
  }, 120_000)

  it('refuses an unknown host with the configured names', async () => {
    const { ctx, captured, runnerProcess } = await harness()
    try {
      const tool = captured.find((candidate) => candidate.name === 'shell')!
      await expect(tool.execute({ command: 'true', remote_host: 'nope' }, { signal: new AbortController().signal }))
        .rejects.toThrow(/unknown remote_host "nope"/)
    } finally {
      ctx.dispose?.()
      runnerProcess.kill('SIGTERM')
    }
  }, 120_000)
})
