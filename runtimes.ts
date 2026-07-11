// actojs — Runtime abstraction and implementations.
export {
  WebRuntime,
  WorkerRuntime,
  setRuntime,
  getRuntime,
  hasMemoryAPI,
} from "./src/core";
export type { Runtime } from "./src/core";
export { NodeRuntime } from "./src/node_runtime";
