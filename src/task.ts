// acto/task — Fire-and-forget computation that can be awaited.
// Web runtime: cooperative event-loop.

import type { PID, Ref, TaskHandle, OnStart } from "./types";
import { ActorSystem, TimeoutError } from "./system";
import * as Proc from "./process";

export { TimeoutError } from "./system";

/** Start a fire-and-forget asynchronous computation. Returns a handle for awaiting. */
export function async<R>(fn: () => Promise<R>): TaskHandle<R> {
  const sys = ActorSystem.current;
  const ref: Ref = Symbol("task");
  sys.taskResults.set(ref, { status: "pending" });

  const pid = Proc.spawn(async () => {
    try {
      const value = await fn();
      const entry = sys.taskResults.get(ref);
      if (entry) {
        entry.status = "done";
        entry.value = value;
        // Notify waiters
        (entry as any)._resolve?.(value);
      }
    } catch (err) {
      const entry = sys.taskResults.get(ref);
      if (entry) {
        entry.status = "error";
        entry.error = err;
        (entry as any)._reject?.(err);
      }
    }
  });

  return { pid, ref };
}

/** Block until the task completes, returning its result. Supports an optional timeout. */
export function await_<R>(task: TaskHandle<R>, timeout?: number): Promise<R> {
  const sys = ActorSystem.current;
  const result = sys.taskResults.get(task.ref);
  if (!result) return Promise.reject(new Error("task not found"));

  if (result.status === "done") return Promise.resolve(result.value as R);
  if (result.status === "error") return Promise.reject(result.error);

  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (timeout != null) {
      timer = setTimeout(() => {
        reject(new TimeoutError("task await timed out"));
      }, timeout);
    }
    (result as any)._resolve = (value: R) => {
      if (timer) clearTimeout(timer);
      resolve(value);
    };
    (result as any)._reject = (err: unknown) => {
      if (timer) clearTimeout(timer);
      reject(err);
    };
  });
}

/** Non-blocking poll: return the result if done, or null if still pending/errored. */
export function yield_<R>(task: TaskHandle<R>): Promise<R | null> {
  const sys = ActorSystem.current;
  const result = sys.taskResults.get(task.ref);
  if (!result) return Promise.resolve(null);
  if (result.status === "done") return Promise.resolve(result.value as R);
  if (result.status === "error") return Promise.resolve(null);
  return Promise.resolve(null);
}

/** Terminate the task's process and clean up its result entry. */
export async function shutdown(task: TaskHandle<unknown>): Promise<void> {
  if (Proc.alive(task.pid)) {
    Proc.exit(task.pid, "shutdown");
  }
  ActorSystem.current.taskResults.delete(task.ref);
}

/** Start a fire-and-forget task without linking to the caller.
 *  The task runs independently; use Task.await_ if you need the result. */
export function start(fn: () => void | Promise<void>): OnStart {
  const pid = Proc.spawn(async () => {
    await fn();
  });
  return { ok: pid };
}

/** Start a fire-and-forget task linked to the caller.
 *  If the caller exits abnormally, the task is killed. */
export function start_link(fn: () => void | Promise<void>): OnStart {
  const pid = Proc.spawn_link(async () => {
    await fn();
  });
  return { ok: pid };
}

/** Await all tasks concurrently. Results are returned in the same order as input.
 *  Rejects immediately if any task fails. */
export function await_many<R>(
  tasks: TaskHandle<R>[],
  timeout?: number,
): Promise<R[]> {
  return Promise.all(tasks.map((t) => await_<R>(t, timeout)));
}

/** Non-blocking poll of multiple tasks. Returns an array with the result (or null
 *  for each task that is still pending or errored). */
export function yield_many<R>(tasks: TaskHandle<R>[]): Promise<(R | null)[]> {
  return Promise.all(tasks.map((t) => yield_<R>(t)));
}
