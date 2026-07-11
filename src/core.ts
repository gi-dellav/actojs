// acto/core — Runtime abstraction + ActorSystem exports.
// Web runtime: cooperative event-loop (default).
// ponytail: pluggable runtimes for Node/Bun/Deno true parallelism.

export { ActorSystem } from "./system";
import { ActorSystem as AS } from "./system";
export type { ProcessLimits, SpawnOptions } from "./types";
import type { PID, SpawnOpt, SpawnOptions } from "./types";

/**
 * Runtime abstraction for pluggable execution environments.
 * The Web runtime uses cooperative scheduling; other backends may offer true parallelism.
 */
export interface Runtime {
  /** Human-readable name of the runtime backend (e.g. "web", "node-worker"). */
  name: string;
  /** Optionally provide an alternative spawn implementation. */
  spawnProcess?(
    fn: () => void | Promise<void>,
    opts?: SpawnOpt[] | SpawnOptions,
  ): PID | undefined;
}

/**
 * Default runtime: single-threaded, cooperative, event-loop based.
 * Suitable for browsers and single-threaded JS environments.
 */
export class WebRuntime implements Runtime {
  name = "web";
}

export { WorkerRuntime } from "./worker_runtime";

let currentRuntime: Runtime = new WebRuntime();

/** Swap the active runtime backend for the current process. */
export function setRuntime(rt: Runtime): void {
  currentRuntime = rt;
}

/** Return the currently configured runtime. */
export function getRuntime(): Runtime {
  return currentRuntime;
}

/** Whether process.memoryUsage() is available (Node, Bun, Deno). False in browsers. */
export function hasMemoryAPI(): boolean {
  return AS.hasMemoryAPI;
}
