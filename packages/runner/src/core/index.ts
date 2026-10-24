/** Runner core: execution registry and socket server, free of harness imports. */

export { ExecutionRegistry, RunnerError, type RegistryOptions } from './registry.ts'
export { RunnerServer, type RunnerServerOptions } from './server.ts'
export type { BackendExecution, ShellBackend } from './types.ts'
