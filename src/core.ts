// acto/core — Runtime abstraction + ActorSystem exports.
// Web runtime: cooperative event-loop (default).
// ponytail: pluggable runtimes for Node/Bun/Deno true parallelism.

export { ActorSystem } from './system';
import { ActorSystem } from './system';
export type { ProcessLimits, SpawnOptions } from './types';

// Runtime abstraction for pluggable execution environments.
// The Web runtime uses cooperative scheduling; other backends may offer true parallelism.
export interface Runtime {
  // Human-readable name of the runtime backend (e.g. "web", "node-worker").
  name: string;
}

// Default runtime: single-threaded, cooperative, event-loop based.
// Suitable for browsers and single-threaded JS environments.
export class WebRuntime implements Runtime {
  name = 'web';
}

let currentRuntime: Runtime = new WebRuntime();

// Swap the active runtime backend for the current process.
export function setRuntime(rt: Runtime): void {
  currentRuntime = rt;
}

// Return the currently configured runtime.
export function getRuntime(): Runtime {
  return currentRuntime;
}

// Whether process.memoryUsage() is available (Node, Bun, Deno).
// False in browsers where process is undefined.
export function hasMemoryAPI(): boolean {
  return ActorSystem.hasMemoryAPI;
}
