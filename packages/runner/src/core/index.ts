/** Runner core: execution registry and socket server, free of harness imports. */

export { ExecutionRegistry, RunnerError, type RegistryOptions } from './registry.js'
export { RunnerServer, type RunnerServerOptions } from './server.js'
export type { BackendExecution, ShellBackend } from './types.js'
