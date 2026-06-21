// acto/dynamic_supervisor — Supervisor optimized for dynamic children.
// Web runtime: cooperative event-loop, built on GenServer.

import type {
  PID, ChildSpec, ChildInfo, Counts,
  SupervisorStartOptions, SupervisorInitOptions, SupervisorSpec,
  OnStart, OnStartChild,
} from './types';
import * as Proc from './process';
import * as GS from './gen_server';

interface DynamicChildState {
  pid: PID;
}

interface DynamicSupervisorState {
  children: Map<PID, DynamicChildState>;
  maxChildren: number;
  maxRestarts: number;
  maxSeconds: number;
  extraArguments: any[];
  restartCounters: { time: number; count: number }[];
  isShuttingDown: boolean;
}

const DEFAULT_MAX_RESTARTS = 3;
const DEFAULT_MAX_SECONDS = 5;

// ---- start_link -----------------------------------------------------------

export async function start_link(
  optsOrModule?: SupervisorStartOptions | any,
  initArg?: unknown,
  maybeOpts?: SupervisorStartOptions,
): Promise<OnStart> {
  let opts: SupervisorStartOptions;

  if (optsOrModule && typeof optsOrModule === 'object' && 'init' in optsOrModule) {
    // Module-based start
    const mod = optsOrModule;
    if (typeof mod.init === 'function') {
      const spec: SupervisorSpec = mod.init(initArg);
      opts = {
        strategy: 'one_for_one',
        max_restarts: spec.max_restarts,
        max_seconds: spec.max_seconds,
        name: (maybeOpts as any)?.name,
      };
    } else {
      return { error: new Error('module must have an init method') };
    }
  } else {
    opts = (optsOrModule as SupervisorStartOptions) ?? { strategy: 'one_for_one' };
  }

  return startDynamicSupervisor(opts);
}

async function startDynamicSupervisor(opts: SupervisorStartOptions): Promise<OnStart> {
  const maxRestarts = opts.max_restarts ?? DEFAULT_MAX_RESTARTS;
  const maxSeconds = opts.max_seconds ?? DEFAULT_MAX_SECONDS;

  const initState: DynamicSupervisorState = {
    children: new Map(),
    maxChildren: Infinity,
    maxRestarts,
    maxSeconds,
    extraArguments: [],
    restartCounters: [],
    isShuttingDown: false,
  };

  const result = GS.startGenServer<DynamicSupervisorState>(
    {
      init(_args: unknown): DynamicSupervisorState {
        return initState;
      },

      async handle_call(msg: unknown, from: PID | null, s: DynamicSupervisorState): Promise<{ reply: unknown; state: DynamicSupervisorState } | { noreply: unknown; state: DynamicSupervisorState }> {
        const { type, payload } = msg as any;

        if (type === 'start_child') {
          if (s.children.size >= s.maxChildren) {
            return { reply: { error: new Error('max_children reached') }, state: s };
          }

          const spec: ChildSpec = payload;
          // Prepend extra_arguments
          const fullArgs = [...s.extraArguments, ...spec.start[2]];
          const fullSpec: ChildSpec = { ...spec, start: [spec.start[0], spec.start[1], fullArgs] };

          const [module, fnName, args] = fullSpec.start;
          if (typeof module !== 'object' || module === null) {
            return { reply: { error: new Error('invalid module') }, state: s };
          }
          if (typeof module[fnName] !== 'function') {
            return { reply: { error: new Error(`function ${String(fnName)} not found`) }, state: s };
          }

          let childResult: OnStartChild;
          try {
            childResult = await module[fnName](...args);
          } catch (err) {
            childResult = { error: err instanceof Error ? err : new Error(String(err)) };
          }

          if ('error' in childResult) {
            return { reply: childResult, state: s };
          }

          const pid = (childResult as { ok: PID }).ok;
          Proc.monitor(pid);
          s.children.set(pid, { pid });
          return { reply: childResult, state: s };
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
          const { reason } = payload as any;
          for (const [pid, _] of s.children) {
            Proc.exit(pid, reason ?? 'shutdown');
          }
          s.children.clear();
          return { reply: undefined, state: s };
        }

        return { reply: undefined, state: s };
      },

      async handle_info(msg: unknown, s: DynamicSupervisorState): Promise<{ noreply: unknown; state: DynamicSupervisorState }> {
        if (s.isShuttingDown) return { noreply: undefined, state: s };

        if (msg && typeof msg === 'object' && msg !== null && (msg as any).type === 'DOWN') {
          const { pid: downPid, reason } = msg as any;
          if (reason === 'normal' || reason === 'shutdown' || reason === 'killed') {
            s.children.delete(downPid);
            return { noreply: undefined, state: s };
          }

          // Check restart rate
          if (!checkRestartRate(s)) {
            s.isShuttingDown = true;
            for (const [pid, _] of s.children) {
              Proc.exit(pid, 'shutdown');
            }
            s.children.clear();
            return { noreply: undefined, state: s };
          }

          // DynamicSupervisor can't restart without the original spec
          // ponytail: store child specs for automatic restart
          s.children.delete(downPid);
        }

        return { noreply: undefined, state: s };
      },

      async terminate(reason: unknown, s: DynamicSupervisorState): Promise<void> {
        for (const [pid, _] of s.children) {
          try { Proc.exit(pid, reason ?? 'shutdown'); } catch (_) {}
        }
      },
    },
    null,
    { name: opts.name, link: true },
  );

  return result;
}

// ---- init -----------------------------------------------------------------

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

export function start_child(sup: PID, spec: ChildSpec): Promise<OnStartChild> {
  return GS.genCall(sup, { type: 'start_child', payload: spec }) as Promise<OnStartChild>;
}

export function terminate_child(
  sup: PID,
  pid: PID,
): Promise<void | { error: string }> {
  return GS.genCall(sup, { type: 'terminate_child', payload: pid }) as Promise<any>;
}

export function count_children(sup: PID): Promise<Counts> {
  return GS.genCall(sup, { type: 'count_children', payload: null }) as Promise<Counts>;
}

export function which_children(sup: PID): Promise<ChildInfo[]> {
  return GS.genCall(sup, { type: 'which_children', payload: null }) as Promise<ChildInfo[]>;
}

export async function stop(sup: PID, reason?: unknown): Promise<void> {
  await GS.genCall(sup, { type: 'stop', payload: { reason } });
}

// ---- helpers --------------------------------------------------------------

function countChildren(s: DynamicSupervisorState): Counts {
  let active = 0;
  for (const [pid, _] of s.children) {
    if (Proc.alive(pid)) active++;
  }
  return {
    specs: s.children.size,
    active,
    supervisors: 0,
    workers: active,
  };
}

function whichChildren(s: DynamicSupervisorState): ChildInfo[] {
  const result: ChildInfo[] = [];
  for (const [pid, _] of s.children) {
    if (Proc.alive(pid)) {
      result.push({
        id: undefined,
        pid,
        type: 'worker',
        modules: [],
      });
    }
  }
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
