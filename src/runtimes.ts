// actojs — Runtime abstraction and implementations.
export {
  WebRuntime,
  WorkerRuntime,
  setRuntime,
  getRuntime,
  hasMemoryAPI,
} from "./core";
export type { Runtime } from "./core";
export { NodeRuntime } from "./node_runtime";
