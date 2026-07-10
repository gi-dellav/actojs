// acto/task — Fire-and-forget computation that can be awaited.
// Web runtime: cooperative event-loop.

import type { PID, Ref, TaskHandle } from './types';
import { ActorSystem, TimeoutError } from './system';
import * as Proc from './process';

export { TimeoutError } from './system';

// Start a fire-and-forget asynchronous computation. Returns a handle for awaiting.
export function async<R>(fn: () => Promise<R>): TaskHandle<R> {
  const sys = ActorSystem.current;
  const ref: Ref = Symbol('task');
  sys.taskResults.set(ref, { status: 'pending' });

  const pid = Proc.spawn(async () => {
    try {
      const value = await fn();
      const entry = sys.taskResults.get(ref);
      if (entry) {
        entry.status = 'done';
        entry.value = value;
        // Notify waiters
        (entry as any)._resolve?.(value);
      }
    } catch (err) {
      const entry = sys.taskResults.get(ref);
      if (entry) {
        entry.status = 'error';
        entry.error = err;
        (entry as any)._reject?.(err);
      }
    }
  });

  return { pid, ref };
}

// Block until the task completes, returning its result. Supports an optional timeout.
export function await_<R>(task: TaskHandle<R>, timeout?: number): Promise<R> {
  const sys = ActorSystem.current;
  const result = sys.taskResults.get(task.ref);
  if (!result) return Promise.reject(new Error('task not found'));

  if (result.status === 'done') return Promise.resolve(result.value as R);
  if (result.status === 'error') return Promise.reject(result.error);

  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    if (timeout != null) {
      timer = setTimeout(() => {
        reject(new TimeoutError('task await timed out'));
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

// Non-blocking poll: return the result if done, or null if still pending/errored.
export function yield_<R>(task: TaskHandle<R>): Promise<R | null> {
  const sys = ActorSystem.current;
  const result = sys.taskResults.get(task.ref);
  if (!result) return Promise.resolve(null);
  if (result.status === 'done') return Promise.resolve(result.value as R);
  if (result.status === 'error') return Promise.resolve(null);
  return Promise.resolve(null);
}

// Terminate the task's process and clean up its result entry.
export async function shutdown(task: TaskHandle<unknown>): Promise<void> {
  if (Proc.alive(task.pid)) {
    Proc.exit(task.pid, 'shutdown');
  }
  ActorSystem.current.taskResults.delete(task.ref);
}
