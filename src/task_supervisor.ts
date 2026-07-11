// actojs/task_supervisor — Supervised task pool built on DynamicSupervisor.
//
// A Task.Supervisor manages a pool of supervised tasks. Tasks are started as
// children of a DynamicSupervisor, which handles cleanup and restart logic.
//
// Usage:
//   const { ok: sup } = await TaskSupervisor.start_link();
//   const handle = await TaskSupervisor.async(sup, async () => 42);
//   const result = await Task.await_(handle);

import type { PID, Ref, ChildSpec, ChildInfo, OnStart, OnStartChild, TaskHandle } from './types';
import type { SupervisorInitOptions } from './types';
import { ActorSystem } from './system';
import * as Proc from './process';
import * as DynamicSupervisor from './dynamic_supervisor';

// ---------------------------------------------------------------------------

export interface TaskSupervisorOpts {
  name?: string;
  max_restarts?: number;
  max_seconds?: number;
  max_children?: number;
}

// ---------------------------------------------------------------------------
// start_link
// ---------------------------------------------------------------------------

/** Start a Task.Supervisor process. Returns { ok: PID } on success. */
export async function start_link(opts: TaskSupervisorOpts = {}): Promise<OnStart> {
  const dsOpts: SupervisorInitOptions = {
    strategy: 'one_for_one',
    name: opts.name,
    max_restarts: opts.max_restarts,
    max_seconds: opts.max_seconds,
    max_children: opts.max_children,
  };
  return DynamicSupervisor.start_link(dsOpts);
}

// ---------------------------------------------------------------------------
// async / async_nolink
// ---------------------------------------------------------------------------

/**
 * Start a task under the supervisor and link it to the caller.
 * Returns a TaskHandle that can be awaited via Task.await_ / Task.yield_.
 * If the caller exits abnormally, the task is killed.
 */
export async function async<R>(
  sup: PID,
  fn: () => Promise<R>,
  opts?: { timeout?: number },
): Promise<TaskHandle<R>> {
  return _startTask(sup, fn, { link: true, timeout: opts?.timeout });
}

/**
 * Start a task under the supervisor without linking to the caller.
 * The task runs independently; the caller won't affect it.
 */
export async function async_nolink<R>(
  sup: PID,
  fn: () => Promise<R>,
  opts?: { timeout?: number },
): Promise<TaskHandle<R>> {
  return _startTask(sup, fn, { link: false, timeout: opts?.timeout });
}

// ---------------------------------------------------------------------------
// start_child
// ---------------------------------------------------------------------------

/** Start a fire-and-forget task under the supervisor. No handle is returned. */
export async function start_child(
  sup: PID,
  fn: () => void | Promise<void>,
  opts?: { timeout?: number },
): Promise<OnStartChild> {
  const wrappedFn = async () => { await fn(); };
  return _spawnChild(sup, wrappedFn, opts?.timeout);
}

// ---------------------------------------------------------------------------
// children / terminate_child / stop
// ---------------------------------------------------------------------------

/** Return the PIDs of all running children under this supervisor. */
export async function children(sup: PID, timeout?: number): Promise<PID[]> {
  const infos = await DynamicSupervisor.which_children(sup, timeout);
  return infos.map((c: ChildInfo) => c.pid);
}

/** Terminate a specific child by PID. */
export async function terminate_child(
  sup: PID,
  pid: PID,
  timeout?: number,
): Promise<void | { error: string }> {
  return DynamicSupervisor.terminate_child(sup, pid, timeout);
}

/** Gracefully shut down the supervisor and all its children. */
export async function stop(sup: PID, reason?: unknown, timeout?: number): Promise<void> {
  await DynamicSupervisor.stop(sup, reason, timeout);
}

/** Return counts of active children. */
export async function count_children(sup: PID, timeout?: number): Promise<{ specs: number; active: number; supervisors: number; workers: number }> {
  return DynamicSupervisor.count_children(sup, timeout);
}

// ---------------------------------------------------------------------------
// internals
// ---------------------------------------------------------------------------

function _uniqueId(): string {
  return `task-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function _startTask<R>(
  sup: PID,
  fn: () => Promise<R>,
  opts: { link: boolean; timeout?: number },
): Promise<TaskHandle<R>> {
  const sys = ActorSystem.current;
  const ref: Ref = Symbol('task');
  sys.taskResults.set(ref, { status: 'pending' });

  // Capture caller PID before any await — the PID stack may change across
  // the async boundary because other processes can run in between.
  const callerPid = _trySelf();

  const workerModule = {
    async start(taskFn: () => Promise<R>, taskRef: Ref) {
      const pid = Proc.spawn(async () => {
        try {
          const value = await taskFn();
          const entry = sys.taskResults.get(taskRef);
          if (entry) { entry.status = 'done'; entry.value = value; }
        } catch (err) {
          const entry = sys.taskResults.get(taskRef);
          if (entry) { entry.status = 'error'; entry.error = err; }
        }
      });
      return { ok: pid };
    },
  };

  const spec: ChildSpec = {
    id: _uniqueId(),
    start: [workerModule, 'start', [fn, ref]],
    restart: 'temporary',
    type: 'worker',
  };

  const result = await DynamicSupervisor.start_child(sup, spec, opts.timeout);
  if ('error' in result) {
    sys.taskResults.delete(ref);
    throw result.error;
  }

  if (opts.link && callerPid) {
    Proc.link(result.ok, callerPid);
  }

  return { pid: result.ok, ref };
}

function _trySelf(): PID | null {
  try { return Proc.self(); } catch { return null; }
}

async function _spawnChild(
  sup: PID,
  fn: () => Promise<void>,
  timeout?: number,
): Promise<OnStartChild> {
  const workerModule = {
    async start(taskFn: () => Promise<void>) {
      const pid = Proc.spawn(async () => { await taskFn(); });
      return { ok: pid };
    },
  };

  const spec: ChildSpec = {
    id: _uniqueId(),
    start: [workerModule, 'start', [fn]],
    restart: 'temporary',
    type: 'worker',
  };

  return DynamicSupervisor.start_child(sup, spec, timeout);
}
