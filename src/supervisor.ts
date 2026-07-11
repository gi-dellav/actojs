// acto/supervisor — Manages child processes with restart strategies.
// Web runtime: cooperative event-loop, built on GenServer.

import type {
  PID, ChildSpec, ChildInfo, Counts,
  Strategy, SupervisorStartOptions, SupervisorInitOptions, SupervisorSpec,
  OnStart, OnStartChild,
  DownMessage, Module,
} from './types';
import type { From } from './system';
import { ActorSystem } from './system';
import * as Proc from './process';
import * as GS from './gen_server';

interface SupervisorModule {
  init: (arg?: unknown) => SupervisorSpec;
}

interface ChildState {
  spec: ChildSpec;
  pid: PID;
}

interface SupervisorState {
  children: Map<string, ChildState>;
  specs: Map<string, ChildSpec>;
  childOrder: string[];
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

/**
 * Start a supervisor process that manages static children with the given restart strategy.
 * Accepts either a direct list of ChildSpecs or a module with an init() method.
 */
export async function start_link(
  childrenOrModule: ChildSpec[] | SupervisorModule,
  initArgOrOpts?: unknown,
  maybeOpts?: SupervisorStartOptions,
): Promise<OnStart> {
  let children: ChildSpec[];
  let opts: SupervisorStartOptions;

  if (Array.isArray(childrenOrModule)) {
    children = childrenOrModule;
    opts = (initArgOrOpts as SupervisorStartOptions) ?? { strategy: 'one_for_one' };
  } else {
    const mod = childrenOrModule;
    const initArg = initArgOrOpts;
    opts = (maybeOpts as SupervisorStartOptions) ?? { strategy: 'one_for_one' };
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
    specs: new Map(),
    childOrder: [],
    strategy,
    maxRestarts,
    maxSeconds,
    restartCounters: [],
    isShuttingDown: false,
  };

  const result = GS.start_link<SupervisorState>(
    {
      async init(_args: unknown): Promise<{ ok: SupervisorState } | { error: unknown }> {
        const supPid = Proc.self();

        const started = new Map<string, ChildState>();
        const order: string[] = [];

        for (const spec of children) {
          const normalized = normalizeSpec(spec);
          const childResult = await startChildSpec(normalized);
          if ('error' in childResult) {
            started.forEach((cs, id) => {
              Proc.exit(cs.pid, 'shutdown');
            });            return { error: childResult.error };
          }
          const pid = (childResult as { ok: PID }).ok;
          Proc.monitor(pid, supPid);
          started.set(normalized.id, { spec: normalized, pid });
          order.push(normalized.id);
        }

        initState.children = started;
        initState.childOrder = order;
        started.forEach((cs, id) => {
          initState.specs.set(id, cs.spec);
        });
        return { ok: initState };
      },

      async handle_call(msg: unknown, from: From, s: SupervisorState, supPid: PID): Promise<{ reply: unknown; state: SupervisorState } | { noreply: unknown; state: SupervisorState }> {
        const { type, payload } = msg as { type: string; payload: unknown };

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
          Proc.monitor(pid, supPid);
          s.children.set(spec.id, { spec, pid });
          s.specs.set(spec.id, spec);
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
            Proc.exit(child.pid, 'shutdown');
          } else {
            const ms = typeof shutdown === 'number' ? shutdown : 5000;
            Proc.exit(child.pid, 'shutdown');
            setTimeout(() => {
              if (Proc.alive(child.pid)) {
                Proc.exit(child.pid, 'killed');
              }
            }, ms);
          }
          s.children.delete(childId);
          return { reply: undefined, state: s };
        }

        if (type === 'delete_child') {
          const childId = payload as string;
          const child = s.children.get(childId);
          if (child) return { reply: { error: 'child_running' }, state: s };
          return { reply: undefined, state: s };
        }

        if (type === 'restart_child') {
          const childId = payload as string;
          const child = s.children.get(childId);
          if (child) {
            Proc.exit(child.pid, 'shutdown');
            s.children.delete(childId);
          }
          const originalSpec = s.specs.get(childId);
          if (!originalSpec) {
            return { reply: { error: 'not_found' }, state: s };
          }
          const childResult = await startChildSpec(originalSpec);
          if ('error' in childResult) {
            return { reply: childResult, state: s };
          }
          const pid = (childResult as { ok: PID }).ok;
          Proc.monitor(pid, supPid);
          s.children.set(childId, { spec: originalSpec, pid });
          if (!s.childOrder.includes(childId)) {
            s.childOrder.push(childId);
          }
          return { reply: childResult, state: s };
        }

        if (type === 'stop') {
          s.isShuttingDown = true;
          const { reason } = payload as { reason?: unknown };
          const reversed = [...s.childOrder].reverse();
          for (const childId of reversed) {
            const child = s.children.get(childId);
            if (child) {
              Proc.exit(child.pid, reason ?? 'shutdown');
            }
          }
          s.children.clear();
          s.specs.clear();
          s.childOrder = [];
          return { reply: undefined, state: s };
        }

        return { reply: undefined, state: s };
      },

      async handle_info(msg: unknown, s: SupervisorState, supPid: PID): Promise<{ noreply: unknown; state: SupervisorState }> {
        if (s.isShuttingDown) return { noreply: undefined, state: s };

        if (msg && typeof msg === 'object' && msg !== null && (msg as DownMessage).type === 'DOWN') {
          const { pid: downPid, reason } = msg as DownMessage;
          if (reason === 'normal' || reason === 'shutdown' || reason === 'killed') {
            let exitedId: string | null = null;
            s.children.forEach((child, id) => {
              if (child.pid === downPid) {
                exitedId = id;
                return;
              }
            });
            if (exitedId) {
              s.children.delete(exitedId);
              s.childOrder = s.childOrder.filter(id => id !== exitedId);
            }
            return { noreply: undefined, state: s };
          }

          let failedId: string | null = null;
          s.children.forEach((child, id) => {
            if (child.pid === downPid) {
              failedId = id;
              return;
            }
          });
          if (!failedId) return { noreply: undefined, state: s };

          const failedSpec = s.children.get(failedId)?.spec;
          const failedIdx = s.childOrder.indexOf(failedId);
          s.children.delete(failedId);
          s.childOrder = s.childOrder.filter(id => id !== failedId);

          if (!checkRestartRate(s)) {
            s.isShuttingDown = true;
            const reversed = [...s.childOrder].reverse();
            for (const childId of reversed) {
              const child = s.children.get(childId);
              if (child) Proc.exit(child.pid, 'shutdown');
            }
            Proc.exit(supPid, 'shutdown');
            return { noreply: undefined, state: s };
          }

          if (failedSpec && failedSpec.restart !== 'temporary') {
            await applyRestartStrategy(s, failedId, failedSpec, failedIdx, supPid);
          }
        }

        return { noreply: undefined, state: s };
      },

      async terminate(reason: unknown, s: SupervisorState): Promise<void> {
        s.children.forEach((child) => {
          try { Proc.exit(child.pid, reason ?? 'shutdown'); } catch (_) {}
        });
      },
    },
    null,
    { name: opts.name, link: true },
  );

  return result;
}

// ---- init (for module-based supervisors) ----------------------------------

/** Build a SupervisorSpec for module-based supervisors from child specs and options. */
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

/** Build a ChildSpec from a module object, calling child_spec() if available. */
export function child_spec(
  moduleOrSpec: ChildSpec | Record<string, unknown>,
  overrides?: Partial<ChildSpec>,
): ChildSpec {
  if (typeof moduleOrSpec === 'object' && 'id' in moduleOrSpec) {
    return { ...(moduleOrSpec as ChildSpec), ...overrides };
  }
  if (typeof (moduleOrSpec as Record<string, unknown>).child_spec === 'function') {
    return { ...((moduleOrSpec as Record<string, unknown>).child_spec as () => ChildSpec)(), ...overrides };
  }
  return {
    id: (moduleOrSpec as Record<string, unknown>)?.name as string ?? 'child',
    start: [moduleOrSpec as Module, 'start_link', []],
    ...overrides,
  };
}

// ---- query functions (for external callers) -------------------------------

/** Return counts of specs, active children, supervisors, and workers. */
export function count_children(sup: PID, timeout?: number): Promise<Counts> {
  return GS.call(sup, { type: 'count_children', payload: null }, timeout) as Promise<Counts>;
}

/** Return information about all alive children managed by the supervisor. */
export function which_children(sup: PID, timeout?: number): Promise<ChildInfo[]> {
  return GS.call(sup, { type: 'which_children', payload: null }, timeout) as Promise<ChildInfo[]>;
}

/** Dynamically add a new child to the supervisor at runtime. */
export function start_child(sup: PID, spec: ChildSpec, timeout?: number): Promise<OnStartChild> {
  return GS.call(sup, { type: 'start_child', payload: spec }, timeout) as Promise<OnStartChild>;
}

/** Stop a child by its spec id, respecting the shutdown value from its ChildSpec. */
export function terminate_child(
  sup: PID,
  childId: string,
  timeout?: number,
): Promise<void | { error: string }> {
  return GS.call(sup, { type: 'terminate_child', payload: childId }, timeout) as Promise<void | { error: string }>;
}

/** Remove a child's spec from the supervisor. Fails if the child is still running. */
export function delete_child(
  sup: PID,
  childId: string,
  timeout?: number,
): Promise<void | { error: string }> {
  return GS.call(sup, { type: 'delete_child', payload: childId }, timeout) as Promise<void | { error: string }>;
}

/** Stop and restart a child by its spec id. */
export function restart_child(sup: PID, childId: string, timeout?: number): Promise<OnStartChild> {
  return GS.call(sup, { type: 'restart_child', payload: childId }, timeout) as Promise<OnStartChild>;
}

/** Gracefully shut down the supervisor and all its children. */
export async function stop(sup: PID, reason?: unknown, timeout?: number): Promise<void> {
  await GS.call(sup, { type: 'stop', payload: { reason } }, timeout);
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

  s.children.forEach((child) => {
    specs++;
    if (Proc.alive(child.pid)) {
      active++;
      if (child.spec.type === 'supervisor') supervisors++;
      else workers++;
    }
  });

  return { specs, active, supervisors, workers };
}

function whichChildren(s: SupervisorState): ChildInfo[] {
  const result: ChildInfo[] = [];
  s.children.forEach((child, id) => {
    if (Proc.alive(child.pid)) {
      result.push({
        id,
        pid: child.pid,
        type: child.spec.type ?? 'worker',
        modules: [child.spec.start[0]],
      });
    }
  });
  return result;
}

function checkRestartRate(s: SupervisorState): boolean {
  const now = Date.now();
  const windowMs = s.maxSeconds * 1000;

  s.restartCounters = s.restartCounters.filter(c => now - c.time < windowMs);

  const total = s.restartCounters.reduce((sum, c) => sum + c.count, 0);
  if (total >= s.maxRestarts) return false;

  s.restartCounters.push({ time: now, count: 1 });
  return true;
}

async function applyRestartStrategy(s: SupervisorState, failedId: string, failedSpec: ChildSpec, failedIdx: number, supPid: PID): Promise<void> {
  // Check significant flag
  if (failedSpec.significant) {
    s.isShuttingDown = true;
    s.children.forEach((child) => {
      Proc.exit(child.pid, 'shutdown');
    });
    Proc.exit(supPid, 'shutdown');
    return;
  }

  switch (s.strategy) {
    case 'one_for_one': {
      const oldPid = s.children.get(failedId)?.pid;
      await restartChild(s, failedId, failedSpec, supPid);
      const newChild = s.children.get(failedId);
      if (oldPid && newChild && newChild.pid !== oldPid) {
        sendRestartNotification(supPid, failedId, oldPid, newChild.pid);
      }
      break;
    }
    case 'one_for_all': {
      const oldPids = new Map<string, PID>();
      const allIds: string[] = [];
      s.specs.forEach((_, id) => {
        allIds.push(id);
      });
      for (const id of allIds) {
        const child = s.children.get(id);
        if (child) {
          oldPids.set(id, child.pid);
          killChildWithGrace(child.pid, child.spec);
          s.children.delete(id);
        }
      }
      s.childOrder = [];
      for (const id of allIds) {
        const spec = s.specs.get(id);
        if (spec) {
          await restartChild(s, id, spec, supPid);
          const newChild = s.children.get(id);
          const old = oldPids.get(id);
          if (old && newChild && newChild.pid !== old) {
            sendRestartNotification(supPid, id, old, newChild.pid);
          }
        }
      }
      break;
    }
    case 'rest_for_one': {
      const oldPids = new Map<string, PID>();
      const afterIds: string[] = [];
      let foundFailed = false;
      s.specs.forEach((_, id) => {
        if (!foundFailed) {
          if (id === failedId) foundFailed = true;
          return;
        }
        afterIds.push(id);
      });

      // Restart failed child first
      const oldFailedPid = s.children.get(failedId)?.pid;
      await restartChild(s, failedId, failedSpec, supPid);
      const newFailedChild = s.children.get(failedId);
      if (oldFailedPid && newFailedChild && newFailedChild.pid !== oldFailedPid) {
        sendRestartNotification(supPid, failedId, oldFailedPid, newFailedChild.pid);
      }

      // Then kill and restart all children after it
      for (const id of afterIds) {
        const child = s.children.get(id);
        if (child) {
          oldPids.set(id, child.pid);
          killChildWithGrace(child.pid, child.spec);
          s.children.delete(id);
        }
        s.childOrder = s.childOrder.filter(oid => oid !== id);
      }
      for (const id of afterIds) {
        const spec = s.specs.get(id);
        if (spec) {
          await restartChild(s, id, spec, supPid);
          const newChild = s.children.get(id);
          const old = oldPids.get(id);
          if (old && newChild && newChild.pid !== old) {
            sendRestartNotification(supPid, id, old, newChild.pid);
          }
        }
      }
      break;
    }
  }
}

function killChildWithGrace(pid: PID, spec: ChildSpec): void {
  const shutdown = spec.shutdown ?? 5000;
  if (shutdown === 'brutal_kill') {
    Proc.exit(pid, 'killed');
  } else if (shutdown === 'infinity') {
    Proc.exit(pid, 'shutdown');
  } else {
    const ms = typeof shutdown === 'number' ? shutdown : 5000;
    Proc.exit(pid, 'shutdown');
    setTimeout(() => {
      if (Proc.alive(pid)) {
        Proc.exit(pid, 'killed');
      }
    }, ms);
  }
}

function sendRestartNotification(supPid: PID, childId: string, oldPid: PID, newPid: PID): void {
  const sys = ActorSystem.current;
  const sup = sys.getProcess(supPid);
  if (sup) {
    sup.links.forEach((linkedPid) => {
      sys.deliverMessage(linkedPid, {
        type: 'RESTARTED',
        childId,
        oldPid,
        newPid,
      });
    });
    sup.monitoredBy.forEach((_, monitorPid) => {
      sys.deliverMessage(monitorPid, {
        type: 'RESTARTED',
        childId,
        oldPid,
        newPid,
      });
    });
  }
}

async function restartChild(s: SupervisorState, id: string, spec: ChildSpec, supPid: PID): Promise<void> {
  const result = await startChildSpec(spec);
  if ('ok' in result) {
    Proc.monitor(result.ok, supPid);
    s.children.set(id, { spec, pid: result.ok });
    if (!s.childOrder.includes(id)) {
      s.childOrder.push(id);
    }
  }
}
