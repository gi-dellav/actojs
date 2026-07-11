// acto/dynamic_supervisor — Supervisor optimized for dynamic children.
// Web runtime: cooperative event-loop, built on GenServer.

import type {
  PID, ChildSpec, ChildInfo, Counts,
  SupervisorStartOptions, SupervisorInitOptions, SupervisorSpec,
  OnStart, OnStartChild,
  DownMessage,
} from './types';
import type { From } from './system';
import * as Proc from './process';
import * as GS from './gen_server';

interface DynamicSupervisorModule {
  init: (arg?: unknown) => SupervisorSpec;
}

interface DynamicChildState {
  pid: PID;
  spec: ChildSpec;
}

interface DynamicSupervisorState {
  children: Map<PID, DynamicChildState>;
  maxChildren: number;
  maxRestarts: number;
  maxSeconds: number;
  extraArguments: unknown[];
  restartCounters: { time: number; count: number }[];
  isShuttingDown: boolean;
}

const DEFAULT_MAX_RESTARTS = 3;
const DEFAULT_MAX_SECONDS = 5;

// ---- start_link -----------------------------------------------------------

/**
 * Start a dynamic supervisor that manages children spawned at runtime.
 * Only supports the one_for_one restart strategy.
 */
export async function start_link(
  optsOrModule?: SupervisorInitOptions | DynamicSupervisorModule,
  initArg?: unknown,
  maybeOpts?: SupervisorStartOptions,
): Promise<OnStart> {
  let opts: SupervisorInitOptions;

  if (optsOrModule && typeof optsOrModule === 'object' && 'init' in optsOrModule) {
    // Module-based start
    const mod = optsOrModule;
    if (typeof mod.init === 'function') {
      const spec: SupervisorSpec = mod.init(initArg);
      if (spec.strategy && spec.strategy !== 'one_for_one') {
        return { error: new Error('DynamicSupervisor only supports one_for_one strategy') };
      }
      opts = {
        strategy: 'one_for_one',
        max_restarts: spec.max_restarts,
        max_seconds: spec.max_seconds,
        max_children: spec.max_children,
        extra_arguments: spec.extra_arguments,
        name: maybeOpts?.name,
      };
    } else {
      return { error: new Error('module must have an init method') };
    }
  } else {
    opts = (optsOrModule as SupervisorInitOptions) ?? { strategy: 'one_for_one' };
  }

  return startDynamicSupervisor(opts);
}

async function startDynamicSupervisor(opts: SupervisorInitOptions): Promise<OnStart> {
  if (opts.strategy && opts.strategy !== 'one_for_one') {
    return { error: new Error('DynamicSupervisor only supports one_for_one strategy') };
  }

  const maxRestarts = opts.max_restarts ?? DEFAULT_MAX_RESTARTS;
  const maxSeconds = opts.max_seconds ?? DEFAULT_MAX_SECONDS;
  const maxChildren = opts.max_children ?? Infinity;
  const extraArguments = opts.extra_arguments ?? [];

  const initState: DynamicSupervisorState = {
    children: new Map(),
    maxChildren,
    maxRestarts,
    maxSeconds,
    extraArguments,
    restartCounters: [],
    isShuttingDown: false,
  };

  const result = GS.start_link<DynamicSupervisorState>(
    {
      init(_args: unknown): DynamicSupervisorState {
        return initState;
      },

      async handle_call(msg: unknown, from: From, s: DynamicSupervisorState, supPid: PID): Promise<{ reply: unknown; state: DynamicSupervisorState } | { noreply: unknown; state: DynamicSupervisorState }> {
        const { type, payload } = msg as { type: string; payload: unknown };

        if (type === 'start_child') {
          if (s.children.size >= s.maxChildren) {
            return { reply: { error: new Error('max_children reached') }, state: s };
          }

          const spec = payload as ChildSpec;
          const fullArgs = [...s.extraArguments, ...spec.start[2]];
          const fullSpec: ChildSpec = { ...spec, start: [spec.start[0], spec.start[1], fullArgs] };

          const childResult = startChildSpec(fullSpec);
          const resolved = childResult instanceof Promise ? await childResult : childResult;
          if ('error' in resolved) {
            return { reply: resolved, state: s };
          }

          const pid = resolved.ok;
          Proc.monitor(pid, supPid);
          s.children.set(pid, { pid, spec: fullSpec });
          return { reply: resolved, state: s };
        }

        if (type === 'terminate_child') {
          const pid = payload as PID;
          const child = s.children.get(pid);
          if (!child) return { reply: { error: 'not_found' }, state: s };
          Proc.exit(pid, 'shutdown');
          s.children.delete(pid);
          return { reply: undefined, state: s };
        }

        if (type === 'count_children') {
          return { reply: countChildren(s), state: s };
        }

        if (type === 'which_children') {
          return { reply: whichChildren(s), state: s };
        }

        if (type === 'stop') {
          s.isShuttingDown = true;
          const { reason } = payload as { reason?: unknown };
          s.children.forEach((_, pid) => {
            Proc.exit(pid, reason ?? 'shutdown');
          });          s.children.clear();
          return { reply: undefined, state: s };
        }

        return { reply: undefined, state: s };
      },

      async handle_info(msg: unknown, s: DynamicSupervisorState, supPid: PID): Promise<{ noreply: unknown; state: DynamicSupervisorState }> {
        if (s.isShuttingDown) return { noreply: undefined, state: s };

        if (msg && typeof msg === 'object' && msg !== null && (msg as DownMessage).type === 'DOWN') {
          const { pid: downPid, reason } = msg as DownMessage;
          if (reason === 'normal' || reason === 'shutdown' || reason === 'killed') {
            s.children.delete(downPid);
            return { noreply: undefined, state: s };
          }

          // Check restart rate
          if (!checkRestartRate(s)) {
            s.isShuttingDown = true;
            s.children.forEach((_, pid) => {
              Proc.exit(pid, 'shutdown');
            });            s.children.clear();
            return { noreply: undefined, state: s };
          }

          const child = s.children.get(downPid);
          s.children.delete(downPid);
          if (child) {
            const result = startChildSpec(child.spec);
            const resolved = result instanceof Promise ? await result : result;
            if ('ok' in resolved) {
              Proc.monitor(resolved.ok, supPid);
              s.children.set(resolved.ok, { pid: resolved.ok, spec: child.spec });
            }
          }
        }

        return { noreply: undefined, state: s };
      },

      async terminate(reason: unknown, s: DynamicSupervisorState): Promise<void> {
        s.children.forEach((_, pid) => {
          try { Proc.exit(pid, reason ?? 'shutdown'); } catch (_) {}
        });      },
    },
    null,
    { name: opts.name, link: true },
  );

  return result;
}

// ---- init -----------------------------------------------------------------

/** Build a SupervisorSpec for module-based dynamic supervisors. */
export function init(opts: SupervisorInitOptions = { strategy: 'one_for_one' }): SupervisorSpec {
  return {
    children: [],
    strategy: 'one_for_one',
    max_restarts: opts.max_restarts ?? DEFAULT_MAX_RESTARTS,
    max_seconds: opts.max_seconds ?? DEFAULT_MAX_SECONDS,
    max_children: opts.max_children ?? Infinity,
    extra_arguments: opts.extra_arguments ?? [],
  };
}

// ---- public API -----------------------------------------------------------

/** Spawn a new child under the dynamic supervisor at runtime. */
export function start_child(sup: PID, spec: ChildSpec, timeout?: number): Promise<OnStartChild> {
  return GS.call(sup, { type: 'start_child', payload: spec }, timeout) as Promise<OnStartChild>;
}

/** Stop a child by its PID and remove it from the supervisor. */
export function terminate_child(
  sup: PID,
  pid: PID,
  timeout?: number,
): Promise<void | { error: string }> {
  return GS.call(sup, { type: 'terminate_child', payload: pid }, timeout) as Promise<void | { error: string }>;
}

/** Return counts of specs, active children, supervisors, and workers. */
export function count_children(sup: PID, timeout?: number): Promise<Counts> {
  return GS.call(sup, { type: 'count_children', payload: null }, timeout) as Promise<Counts>;
}

/** Return information about all alive children managed by the supervisor. */
export function which_children(sup: PID, timeout?: number): Promise<ChildInfo[]> {
  return GS.call(sup, { type: 'which_children', payload: null }, timeout) as Promise<ChildInfo[]>;
}

/** Gracefully shut down the dynamic supervisor and all its children. */
export async function stop(sup: PID, reason?: unknown, timeout?: number): Promise<void> {
  await GS.call(sup, { type: 'stop', payload: { reason } }, timeout);
}

// ---- helpers --------------------------------------------------------------

function startChildSpec(spec: ChildSpec): OnStartChild | Promise<OnStartChild> {
  const [module, fnName, args] = spec.start;
  if (typeof module !== 'object' || module === null) {
    return { error: new Error('invalid module') };
  }
  if (typeof module[fnName] !== 'function') {
    return { error: new Error(`function ${String(fnName)} not found`) };
  }
  try {
    const result = (module[fnName] as Function)(...args);
    if (result instanceof Promise) {
      return result.then(
        (v: OnStartChild) => v,
        (err: unknown) => ({ error: err instanceof Error ? err : new Error(String(err)) }),
      );
    }
    return result as OnStartChild;
  } catch (err) {
    return { error: err instanceof Error ? err : new Error(String(err)) };
  }
}

function countChildren(s: DynamicSupervisorState): Counts {
  let active = 0;
  s.children.forEach((_, pid) => {
    if (Proc.alive(pid)) active++;
  });
  return {
    specs: s.children.size,
    active,
    supervisors: 0,
    workers: active,
  };
}

function whichChildren(s: DynamicSupervisorState): ChildInfo[] {
  const result: ChildInfo[] = [];
  s.children.forEach((_, pid) => {
    if (Proc.alive(pid)) {
      result.push({
        id: undefined,
        pid,
        type: 'worker',
        modules: [],
      });
    }
  });
  return result;
}

function checkRestartRate(s: DynamicSupervisorState): boolean {
  const now = Date.now();
  const windowMs = s.maxSeconds * 1000;
  s.restartCounters = s.restartCounters.filter(c => now - c.time < windowMs);
  const total = s.restartCounters.reduce((sum, c) => sum + c.count, 0);
  if (total >= s.maxRestarts) return false;
  s.restartCounters.push({ time: now, count: 1 });
  return true;
}
