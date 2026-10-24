import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/*/tests/**/*.spec.ts'],
    testTimeout: 20_000,
    hookTimeout: 20_000,
  },
})
