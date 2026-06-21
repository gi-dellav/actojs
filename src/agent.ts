// acto/agent — Simple state-holding actor.
// Web runtime: cooperative event-loop, built on GenServer.

import type { PID, OnStart } from './types';
import * as GS from './gen_server';

type AgentFn<S, R = unknown> = (state: S) => R;
type AgentFnAndUpdate<S, R = unknown> = (state: S) => [R, S];
type AgentArg<S, R = unknown> = AgentFn<S, R> | [any, string, any[]]; // fn or [module, fnName, args]

// ---- resolve agent PID ----------------------------------------------------

function resolvePid(agent: PID | { module: any; args: any[] }): PID {
  if (typeof agent === 'string') return agent;
  throw new Error('module-based agent resolution not supported in Web runtime');
}

function resolveFn<S, R>(arg: AgentArg<S, R>): AgentFn<S, R> {
  if (typeof arg === 'function') return arg;
  const [mod, fnName, args] = arg;
  if (typeof mod[fnName] === 'function') {
    return (state: S) => mod[fnName](state, ...args);
  }
  throw new Error(`function ${String(fnName)} not found on module`);
}

// ---- start / start_link ---------------------------------------------------

export async function start<S>(init: () => S, opts?: { name?: string }): Promise<OnStart> {
  return startGen(init, { ...opts, link: false });
}

export async function start_link<S>(init: () => S, opts?: { name?: string }): Promise<OnStart> {
  return startGen(init, { ...opts, link: true });
}

async function startGen<S>(init: () => S, opts?: { name?: string; link?: boolean }): Promise<OnStart> {
  let state: S;
  try {
    state = init();
  } catch (err) {
    return { error: err instanceof Error ? err : new Error(String(err)) };
  }

  return GS.startGenServer<S>(
    {
      init(_args: unknown): S {
        return state;
      },
      handle_call(msg: unknown, _from: PID, s: S) {
        const { type, payload } = msg as any;
        if (type === 'get') {
          const fn = typeof payload === 'function' ? payload : resolveFn<S, unknown>(payload);
          const result = fn(s);
          return { reply: result, state: s };
        }
        if (type === 'update') {
          const fn = typeof payload === 'function' ? payload : resolveFn<S, S>(payload);
          const newState = fn(s);
          return { reply: undefined, state: newState };
        }
        if (type === 'get_and_update') {
          const fn = typeof payload === 'function' ? payload : resolveFn<S, [unknown, S]>(payload);
          const [reply, newState] = fn(s);
          return { reply, state: newState };
        }
        return { reply: undefined, state: s };
      },
      handle_cast(msg: unknown, s: S) {
        const { payload } = msg as any;
        const fn = typeof payload === 'function' ? payload : resolveFn<S, S>(payload);
        return { noreply: undefined, state: fn(s) };
      },
    },
    null,
    { name: opts?.name, link: opts?.link ?? false },
  );
}

// ---- get ------------------------------------------------------------------

export async function get<S, R>(agent: PID | { module: any; args: any[] }, fn: AgentArg<S, R>): Promise<R> {
  const pid = resolvePid(agent);
  return GS.genCall(pid, { type: 'get', payload: fn }) as Promise<R>;
}

// ---- update ---------------------------------------------------------------

export async function update<S>(agent: PID | { module: any; args: any[] }, fn: AgentArg<S, S>): Promise<void> {
  const pid = resolvePid(agent);
  await GS.genCall(pid, { type: 'update', payload: fn });
}

// ---- get_and_update -------------------------------------------------------

export async function get_and_update<S, R>(
  agent: PID | { module: any; args: any[] },
  fn: AgentArg<S, [R, S]>,
): Promise<R> {
  const pid = resolvePid(agent);
  return GS.genCall(pid, { type: 'get_and_update', payload: fn }) as Promise<R>;
}

// ---- cast -----------------------------------------------------------------

export function cast<S>(agent: PID | { module: any; args: any[] }, fn: AgentArg<S, S>): void {
  const pid = resolvePid(agent);
  GS.genCast(pid, { type: 'cast', payload: fn });
}

// ---- stop -----------------------------------------------------------------

export async function stop(agent: PID | { module: any; args: any[] }, reason?: unknown): Promise<void> {
  const pid = resolvePid(agent);
  await GS.genStop(pid, reason);
}
