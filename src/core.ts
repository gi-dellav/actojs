// acto/core — Runtime abstraction + ActorSystem exports.
// Web runtime: cooperative event-loop (default).
// ponytail: pluggable runtimes for Node/Bun/Deno true parallelism.

export { ActorSystem } from './system';
import { ActorSystem } from './system';
export type { ProcessLimits, SpawnOptions } from './types';

export interface Runtime {
  name: string;
}

export class WebRuntime implements Runtime {
  name = 'web';
}

let currentRuntime: Runtime = new WebRuntime();

export function setRuntime(rt: Runtime): void {
  currentRuntime = rt;
}

export function getRuntime(): Runtime {
  return currentRuntime;
}

// Whether process.memoryUsage() is available (Node, Bun, Deno).
// False in browsers.
export function hasMemoryAPI(): boolean {
  return ActorSystem.hasMemoryAPI;
}
