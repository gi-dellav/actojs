// acto/task — Fire-and-forget computation that can be awaited.
// Web runtime: cooperative event-loop.

import type { PID, Ref, TaskHandle } from './types';
import * as Proc from './process';

const taskResults = new Map<Ref, { status: 'pending' | 'done' | 'error'; value?: unknown; error?: unknown }>();

export function async<R>(fn: () => Promise<R>): TaskHandle<R> {
  const ref: Ref = Symbol('task');
  taskResults.set(ref, { status: 'pending' });

  const pid = Proc.spawn(async () => {
    try {
      const value = await fn();
      taskResults.set(ref, { status: 'done', value });
    } catch (err) {
      taskResults.set(ref, { status: 'error', error: err });
    }
  });

  return { pid, ref };
}

export function await_<R>(task: TaskHandle<R>): Promise<R> {
  const result = taskResults.get(task.ref);
  if (!result) return Promise.reject(new Error('task not found'));

  if (result.status === 'done') return Promise.resolve(result.value as R);
  if (result.status === 'error') return Promise.reject(result.error);

  // Still pending — poll until done
  return new Promise((resolve, reject) => {
    const check = () => {
      const r = taskResults.get(task.ref);
      if (!r) {
        reject(new Error('task not found'));
      } else if (r.status === 'done') {
        resolve(r.value as R);
      } else if (r.status === 'error') {
        reject(r.error);
      } else {
        setTimeout(check, 0);
      }
    };
    check();
  });
}

export function yield_<R>(task: TaskHandle<R>): Promise<R | null> {
  const result = taskResults.get(task.ref);
  if (!result) return Promise.resolve(null);
  if (result.status === 'done') return Promise.resolve(result.value as R);
  if (result.status === 'error') return Promise.resolve(null);
  return Promise.resolve(null);
}

export async function shutdown(task: TaskHandle<unknown>): Promise<void> {
  if (Proc.alive(task.pid)) {
    Proc.exit(task.pid, 'shutdown');
  }
  taskResults.delete(task.ref);
}

// Exported as await_/yield_ to avoid JS reserved word issues.
// When imported via namespace: import * as Task from 'acto/task'
// Use as: Task.await(task), Task.yield(task)
