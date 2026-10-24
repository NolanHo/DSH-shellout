import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const protocolSource = fileURLToPath(new URL('./packages/protocol/src/index.ts', import.meta.url))
const runnerCoreSource = fileURLToPath(new URL('./packages/runner/src/core/index.ts', import.meta.url))

export default defineConfig({
  resolve: {
    alias: {
      'dsh-shellout-protocol': protocolSource,
      'dsh-shellout-runner/core': runnerCoreSource,
    },
  },
  test: {
    include: ['packages/*/tests/**/*.spec.ts'],
    testTimeout: 20_000,
    hookTimeout: 20_000,
  },
})
