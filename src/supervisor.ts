// acto/supervisor — Manages child processes with restart strategies.
// Web runtime: cooperative event-loop, built on GenServer.

import type {
  PID, ChildSpec, ChildInfo, Counts,
  Strategy, SupervisorStartOptions, SupervisorInitOptions, SupervisorSpec,
  OnStart, OnStartChild,
} from './types';
import * as Proc from './process';
import * as GS from './gen_server';

interface ChildState {
  spec: ChildSpec;
  pid: PID;
}

interface SupervisorState {
  children: Map<string, ChildState>; // id -> { spec, pid }
  childOrder: string[]; // order of child ids (for rest_for_one)
  strategy: Strategy;
  maxRestarts: number;
  maxSeconds: number;
  restartCounters: { time: number; count: number }[];
  isShuttingDown: boolean;
}

const DEFAULT_MAX_RESTARTS = 3;
const DEFAULT_MAX_SECONDS = 5;

function normalizeSpec(spec: ChildSpec): ChildSpec {
  return {
    restart: 'permanent',
    shutdown: 5000,
    type: 'worker',
    ...spec,
  };
}

// ---- start_link (static children) -----------------------------------------

export async function start_link(
  childrenOrModule: ChildSpec[] | any,
  initArgOrOpts?: unknown,
  maybeOpts?: SupervisorStartOptions,
): Promise<OnStart> {
  let children: ChildSpec[];
  let opts: SupervisorStartOptions;

  if (Array.isArray(childrenOrModule)) {
    // Static child list
    children = childrenOrModule;
    opts = (initArgOrOpts as SupervisorStartOptions) ?? { strategy: 'one_for_one' };
  } else {
    // Module-based supervisor
    const mod = childrenOrModule;
    const initArg = initArgOrOpts;
    opts = (maybeOpts as SupervisorStartOptions) ?? {};
    if (typeof mod.init === 'function') {
      const spec: SupervisorSpec = mod.init(initArg);
      children = spec.children;
      opts = {
        strategy: spec.strategy,
        max_restarts: spec.max_restarts,
        max_seconds: spec.max_seconds,
      };
    } else {
      return { error: new Error('module must have an init method') };
    }
  }

  return startSupervisor(children, opts);
}

async function startSupervisor(children: ChildSpec[], opts: SupervisorStartOptions): Promise<OnStart> {
  const strategy = opts.strategy ?? 'one_for_one';
  const maxRestarts = opts.max_restarts ?? DEFAULT_MAX_RESTARTS;
  const maxSeconds = opts.max_seconds ?? DEFAULT_MAX_SECONDS;

  const initState: SupervisorState = {
    children: new Map(),
    childOrder: [],
    strategy,
    maxRestarts,
    maxSeconds,
    restartCounters: [],
    isShuttingDown: false,
  };

  const result = GS.startGenServer<SupervisorState>(
    {
      async init(_args: unknown): Promise<{ ok: SupervisorState } | { error: unknown }> {
        // Start all children
        const started = new Map<string, ChildState>();
        const order: string[] = [];

        for (const spec of children) {
          const normalized = normalizeSpec(spec);
          const childResult = await startChildSpec(normalized);
          if ('error' in childResult) {
            // Cleanup already-started children
            for (const [id, cs] of started) {
              Proc.exit(cs.pid, 'shutdown');
            }
            return { error: childResult.error };
          }
          const pid = (childResult as { ok: PID }).ok;
          Proc.monitor(pid); // Monitor child for exit signals
          started.set(normalized.id, { spec: normalized, pid });
          order.push(normalized.id);
        }

        initState.children = started;
        initState.childOrder = order;
        return { ok: initState };
      },

      async handle_call(msg: unknown, from: PID | null, s: SupervisorState): Promise<{ reply: unknown; state: SupervisorState } | { noreply: unknown; state: SupervisorState }> {
        const { type, payload } = msg as any;

        if (type === 'count_children') {
          return { reply: countChildren(s), state: s };
        }

        if (type === 'which_children') {
          return { reply: whichChildren(s), state: s };
        }

        if (type === 'start_child') {
          const spec = normalizeSpec(payload as ChildSpec);
          const childResult = await startChildSpec(spec);
          if ('error' in childResult) {
            return { reply: childResult, state: s };
          }
          const pid = (childResult as { ok: PID }).ok;
          Proc.monitor(pid);
          s.children.set(spec.id, { spec, pid });
          s.childOrder.push(spec.id);
          return { reply: childResult, state: s };
        }

        if (type === 'terminate_child') {
          const childId = payload as string;
          const child = s.children.get(childId);
          if (!child) return { reply: { error: 'not_found' }, state: s };
          const shutdown = child.spec.shutdown;
          if (shutdown === 'brutal_kill') {
            Proc.exit(child.pid, 'killed');
          } else if (shutdown === 'infinity') {
            // Wait indefinitely — send shutdown and wait for DOWN
            Proc.exit(child.pid, 'shutdown');
          } else {
            const ms = typeof shutdown === 'number' ? shutdown : 5000;
            Proc.exit(child.pid, 'shutdown');
            // ponytail: kill after timeout. For now, just send shutdown.
          }
          s.children.delete(childId);
          s.childOrder = s.childOrder.filter(id => id !== childId);
          return { reply: undefined, state: s };
        }

        if (type === 'delete_child') {
          const childId = payload as string;
          const child = s.children.get(childId);
          if (child) return { reply: { error: 'child_running' }, state: s };
          // Already terminated — just acknowledge
          return { reply: undefined, state: s };
        }

        if (type === 'restart_child') {
          const childId = payload as string;
          const child = s.children.get(childId);
          if (child) {
            // Child still running — terminate first
            Proc.exit(child.pid, 'shutdown');
            s.children.delete(childId);
          }
          // Find the original spec in the child order (it may have been removed)
          // ponytail: full idempotent restart. For now, just acknowledge.
          return { reply: { error: 'not_found' }, state: s };
        }

        if (type === 'stop') {
          s.isShuttingDown = true;
          const { reason } = payload as any;
          // Terminate all children in reverse order
          const reversed = [...s.childOrder].reverse();
          for (const childId of reversed) {
            const child = s.children.get(childId);
            if (child) {
              Proc.exit(child.pid, reason ?? 'shutdown');
            }
          }
          s.children.clear();
          s.childOrder = [];
          return { reply: undefined, state: s };
        }

        return { reply: undefined, state: s };
      },

      async handle_info(msg: unknown, s: SupervisorState): Promise<{ noreply: unknown; state: SupervisorState }> {
        if (s.isShuttingDown) return { noreply: undefined, state: s };

        // Handle child exit (DOWN message from monitor)
        if (msg && typeof msg === 'object' && msg !== null && (msg as any).type === 'DOWN') {
          const { pid: downPid, reason } = msg as any;
          if (reason === 'normal' || reason === 'shutdown') {
            return { noreply: undefined, state: s };
          }

          // Find which child exited
          let failedId: string | null = null;
          for (const [id, child] of s.children) {
            if (child.pid === downPid) {
              failedId = id;
              break;
            }
          }
          if (!failedId) return { noreply: undefined, state: s };

          const failedSpec = s.children.get(failedId)?.spec;
          s.children.delete(failedId);
          s.childOrder = s.childOrder.filter(id => id !== failedId);

          // Check restart rate limit
          if (!checkRestartRate(s)) {
            // Too many restarts — supervisor shuts down
            s.isShuttingDown = true;
            const reversed = [...s.childOrder].reverse();
            for (const childId of reversed) {
              const child = s.children.get(childId);
              if (child) Proc.exit(child.pid, 'shutdown');
            }
            return { noreply: undefined, state: s };
          }

          if (failedSpec && failedSpec.restart !== 'temporary') {
            await applyRestartStrategy(s, failedId, failedSpec);
          }
        }

        return { noreply: undefined, state: s };
      },

      async terminate(reason: unknown, s: SupervisorState): Promise<void> {
        for (const [_, child] of s.children) {
          try { Proc.exit(child.pid, reason ?? 'shutdown'); } catch (_) {}
        }
      },
    },
    null,
    { name: opts.name, link: true },
  );

  return result;
}

// ---- init (for module-based supervisors) ----------------------------------

export function init(children: ChildSpec[], opts: SupervisorInitOptions = { strategy: 'one_for_one' }): SupervisorSpec {
  return {
    children,
    strategy: opts.strategy,
    max_restarts: opts.max_restarts ?? DEFAULT_MAX_RESTARTS,
    max_seconds: opts.max_seconds ?? DEFAULT_MAX_SECONDS,
    max_children: opts.max_children ?? Infinity,
    extra_arguments: opts.extra_arguments ?? [],
  };
}

// ---- child_spec -----------------------------------------------------------

export function child_spec(
  moduleOrSpec: any,
  overrides?: Partial<ChildSpec>,
): ChildSpec {
  if (typeof moduleOrSpec === 'object' && 'id' in moduleOrSpec) {
    return { ...moduleOrSpec, ...overrides };
  }
  // Module with child_spec method or defaults
  if (typeof moduleOrSpec?.child_spec === 'function') {
    return { ...moduleOrSpec.child_spec(), ...overrides };
  }
  // Default: assume [module, functionName, args]
  return {
    id: moduleOrSpec?.name ?? 'child',
    start: [moduleOrSpec, 'start_link', []],
    ...overrides,
  };
}

// ---- query functions (for external callers) -------------------------------

export function count_children(sup: PID): Promise<Counts> {
  return GS.genCall(sup, { type: 'count_children', payload: null }) as Promise<Counts>;
}

export function which_children(sup: PID): Promise<ChildInfo[]> {
  return GS.genCall(sup, { type: 'which_children', payload: null }) as Promise<ChildInfo[]>;
}

export function start_child(sup: PID, spec: ChildSpec): Promise<OnStartChild> {
  return GS.genCall(sup, { type: 'start_child', payload: spec }) as Promise<OnStartChild>;
}

export function terminate_child(
  sup: PID,
  childId: string,
): Promise<void | { error: string }> {
  return GS.genCall(sup, { type: 'terminate_child', payload: childId }) as Promise<any>;
}

export function delete_child(
  sup: PID,
  childId: string,
): Promise<void | { error: string }> {
  return GS.genCall(sup, { type: 'delete_child', payload: childId }) as Promise<any>;
}

export function restart_child(sup: PID, childId: string): Promise<OnStartChild> {
  return GS.genCall(sup, { type: 'restart_child', payload: childId }) as Promise<OnStartChild>;
}

export async function stop(sup: PID, reason?: unknown): Promise<void> {
  await GS.genCall(sup, { type: 'stop', payload: { reason } });
}

// ---- internal helpers -----------------------------------------------------

async function startChildSpec(spec: ChildSpec): Promise<OnStartChild> {
  const [module, fnName, args] = spec.start;
  if (typeof module !== 'object' || module === null) {
    return { error: new Error('invalid start spec: module must be an object') };
  }
  if (typeof module[fnName] !== 'function') {
    return { error: new Error(`function ${String(fnName)} not found on module`) };
  }
  try {
    return await module[fnName](...args);
  } catch (err) {
    return { error: err instanceof Error ? err : new Error(String(err)) };
  }
}

function countChildren(s: SupervisorState): Counts {
  let specs = 0;
  let active = 0;
  let supervisors = 0;
  let workers = 0;

  for (const [_, child] of s.children) {
    specs++;
    if (Proc.alive(child.pid)) {
      active++;
      if (child.spec.type === 'supervisor') supervisors++;
      else workers++;
    }
  }

  return { specs, active, supervisors, workers };
}

function whichChildren(s: SupervisorState): ChildInfo[] {
  const result: ChildInfo[] = [];
  for (const [id, child] of s.children) {
    if (Proc.alive(child.pid)) {
      result.push({
        id,
        pid: child.pid,
        type: child.spec.type ?? 'worker',
        modules: [child.spec.start[0]],
      });
    }
  }
  return result;
}

function checkRestartRate(s: SupervisorState): boolean {
  const now = Date.now();
  const windowMs = s.maxSeconds * 1000;

  // Prune old entries
  s.restartCounters = s.restartCounters.filter(c => now - c.time < windowMs);

  // Count restarts in window
  const total = s.restartCounters.reduce((sum, c) => sum + c.count, 0);
  if (total >= s.maxRestarts) return false;

  // Add this restart
  s.restartCounters.push({ time: now, count: 1 });
  return true;
}

async function applyRestartStrategy(s: SupervisorState, failedId: string, failedSpec: ChildSpec): Promise<void> {
  const failedIdx = s.childOrder.indexOf(failedId);

  switch (s.strategy) {
    case 'one_for_one': {
      await restartChild(s, failedId, failedSpec);
      break;
    }
    case 'one_for_all': {
      // Terminate all children first, then restart all
      const allIds = [...s.childOrder];
      for (const id of allIds) {
        const child = s.children.get(id);
        if (child) {
          Proc.exit(child.pid, 'shutdown');
          s.children.delete(id);
        }
      }
      s.childOrder = [];
      // Restart all
      for (const id of allIds) {
        const spec = id === failedId ? failedSpec : findOriginalSpec(s, id);
        if (spec) await restartChild(s, id, spec);
      }
      break;
    }
    case 'rest_for_one': {
      // Terminate failed child and all after it
      const toRestart: { id: string; spec: ChildSpec }[] = [];
      const afterIds = s.childOrder.slice(failedIdx);
      for (const id of afterIds) {
        const child = s.children.get(id);
        if (child) {
          Proc.exit(child.pid, 'shutdown');
          s.children.delete(id);
          const spec = id === failedId ? failedSpec : findOriginalSpec(s, id);
          if (spec) toRestart.push({ id, spec });
        }
      }
      // Filter out the terminated from order
      s.childOrder = s.childOrder.slice(0, failedIdx);
      // Restart in order
      for (const { id, spec } of toRestart) {
        await restartChild(s, id, spec);
      }
      break;
    }
  }
}

async function restartChild(s: SupervisorState, id: string, spec: ChildSpec): Promise<void> {
  const result = await startChildSpec(spec);
  if ('ok' in result) {
    Proc.monitor(result.ok);
    s.children.set(id, { spec, pid: result.ok });
    s.childOrder.push(id);
  }
  // ponytail: if restart fails, escalate to parent supervisor
}

function findOriginalSpec(s: SupervisorState, id: string): ChildSpec | null {
  // ponytail: store original specs for restart. Currently we lose them.
  // For now, children that were started via start_child are lost on restart.
  return null;
}
