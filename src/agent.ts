// acto/agent — Simple state-holding actor.
// Web runtime: cooperative event-loop, built on GenServer.

import type { PID, OnStart, MFA, Module } from './types';
import type { From } from './system';
import * as GS from './gen_server';

type AgentFn<S, R = unknown> = (state: S) => R;
type AgentArg<S, R = unknown> = AgentFn<S, R> | MFA; // fn or [module, fnName, args]

// ---- resolve agent PID ----------------------------------------------------

function resolvePid(agent: PID | { module: Module; args: unknown[] }): PID {
  if (typeof agent === 'string') return agent;
  throw new Error('module-based agent resolution not supported in Web runtime');
}

function resolveFn<S, R>(arg: AgentArg<S, R>): AgentFn<S, R> {
  if (typeof arg === 'function') return arg;
  const [mod, fnName, args] = arg;
  if (typeof mod[fnName] === 'function') {
    return (state: S) => (mod[fnName] as Function)(state, ...args) as R;
  }
  throw new Error(`function ${String(fnName)} not found on module`);
}

// ---- start / start_link ---------------------------------------------------

/** Start an agent process initialized with the given state factory. */
export async function start<S>(init: () => S, opts?: { name?: string }): Promise<OnStart> {
  return startGen(init, { ...opts, link: false });
}

/** Start an agent process and link it to the caller. */
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

  const startFn = opts?.link ? GS.start_link : GS.start;

  return startFn<S>(
    {
      init(_args: unknown): S {
        return state;
      },
      handle_call(msg: unknown, _from: From, s: S, _myPid: PID) {
        const { type, payload } = (msg as { type: string; payload: unknown });
        if (type === 'get') {
          const fn = typeof payload === 'function' ? payload as AgentFn<S, unknown> : resolveFn<S, unknown>(payload as AgentArg<S, unknown>);
          const result = fn(s);
          return { reply: result, state: s };
        }
        if (type === 'update') {
          const fn = typeof payload === 'function' ? payload as AgentFn<S, S> : resolveFn<S, S>(payload as AgentArg<S, S>);
          const newState = fn(s);
          return { reply: undefined, state: newState };
        }
        if (type === 'get_and_update') {
          const fn = typeof payload === 'function' ? payload as AgentFn<S, [unknown, S]> : resolveFn<S, [unknown, S]>(payload as AgentArg<S, [unknown, S]>);
          const [reply, newState] = fn(s);
          return { reply, state: newState };
        }
        return { reply: undefined, state: s };
      },
      handle_cast(msg: unknown, s: S, _myPid: PID) {
        const { payload } = (msg as { payload: unknown });
        const fn = typeof payload === 'function' ? payload as AgentFn<S, S> : resolveFn<S, S>(payload as AgentArg<S, S>);
        return { noreply: undefined, state: fn(s) };
      },
    },
    null,
    { name: opts?.name },
  );
}

// ---- get ------------------------------------------------------------------

/** Synchronously read from the agent's state by applying the given function. */
export async function get<S, R>(agent: PID | { module: Module; args: unknown[] }, fn: AgentArg<S, R>, timeout?: number): Promise<R> {
  const pid = resolvePid(agent);
  return GS.call(pid, { type: 'get', payload: fn }, timeout) as Promise<R>;
}

// ---- update ---------------------------------------------------------------

/** Transform the agent's state by applying the given function. */
export async function update<S>(agent: PID | { module: Module; args: unknown[] }, fn: AgentArg<S, S>, timeout?: number): Promise<void> {
  const pid = resolvePid(agent);
  await GS.call(pid, { type: 'update', payload: fn }, timeout);
}

// ---- get_and_update -------------------------------------------------------

/** Atomically read and update the agent's state in one operation. */
export async function get_and_update<S, R>(
  agent: PID | { module: Module; args: unknown[] },
  fn: AgentArg<S, [R, S]>,
  timeout?: number,
): Promise<R> {
  const pid = resolvePid(agent);
  return GS.call(pid, { type: 'get_and_update', payload: fn }, timeout) as Promise<R>;
}

// ---- cast -----------------------------------------------------------------

/** Asynchronously apply an update to the agent's state without waiting for a reply. */
export function cast<S>(agent: PID | { module: Module; args: unknown[] }, fn: AgentArg<S, S>): void {
  const pid = resolvePid(agent);
  GS.cast(pid, { type: 'cast', payload: fn });
}

// ---- stop -----------------------------------------------------------------

/** Gracefully stop the agent process with an optional reason and timeout. */
export async function stop(agent: PID | { module: Module; args: unknown[] }, reason?: unknown, timeout?: number): Promise<void> {
  const pid = resolvePid(agent);
  await GS.stop(pid, reason, timeout);
}
