// acto/core — Runtime abstraction.
// Web runtime: cooperative event-loop (default).
// ponytail: pluggable runtimes for Node/Bun/Deno true parallelism.

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
