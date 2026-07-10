// actojs — Internal GenServer base for Agent, Registry, Supervisor, etc.
// Provides a receive loop with handle_call / handle_cast / handle_info.
// Web runtime: cooperative event-loop.
// Includes fault-isolation: message budget yielding, execution timeouts,
// and memory-limit checks at yield points.

import type { PID, Ref } from './types';
import { ActorSystem, type From, type PendingCall, TimeoutError } from './system';
import * as Proc from './process';
import * as M from './mailbox';

export type { From } from './system';

interface GenCallMsg {
  __gen_server__: 'call';
  ref: Ref;
  payload: unknown;
  replyTo: PID | null;
}

interface GenCastMsg {
  __gen_server__: 'cast';
  payload: unknown;
}

interface GenStopMsg {
  __gen_server__: 'stop';
  reason?: unknown;
  __stop_ref?: Ref;
}

export interface GenServerCallbacks<S> {
  init(args: unknown): S | { ok: S } | { error: unknown; reason?: unknown } | Promise<S | { ok: S } | { error: unknown; reason?: unknown }>;
  handle_call?(msg: unknown, from: From, state: S, myPid: PID): { reply: unknown; state: S } | { noreply: unknown; state: S } | Promise<{ reply: unknown; state: S } | { noreply: unknown; state: S }>;
  handle_cast?(msg: unknown, state: S, myPid: PID): { noreply: unknown; state: S } | Promise<{ noreply: unknown; state: S }>;
  handle_info?(msg: unknown, state: S, myPid: PID): { noreply: unknown; state: S } | Promise<{ noreply: unknown; state: S }>;
  terminate?(reason: unknown, state: S): void | Promise<void>;
}

export interface TerminateInfo {
  reason: unknown;
  exitType: 'stop' | 'exit' | 'shutdown';
}

// ---- start (async) -------------------------------------------------------

export async function startGenServer<S>(
  callbacks: GenServerCallbacks<S>,
  initArg: unknown,
  opts?: { name?: string; link?: boolean },
): Promise<{ ok: PID } | { error: Error }> {
  let initDone: () => void;
  let initFailed: (err: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    initDone = resolve;
    initFailed = reject;
  });

  const spawnFn = async () => {
    const me = Proc.self();

    if (opts?.name) {
      Proc.register(me, opts.name);
    }

    let state: S;
    try {
      const result = await callbacks.init(initArg);
      if (typeof result === 'object' && result !== null && 'ok' in result) {
        state = (result as { ok: S }).ok;
      } else if (typeof result === 'object' && result !== null && 'error' in result) {
        Proc.exit(me, (result as { error: unknown }).error);
        initFailed(new Error('init returned error'));
        return;
      } else {
        state = result as S;
      }
    } catch (err) {
      Proc.exit(me, err);
      initFailed(err instanceof Error ? err : new Error(String(err)));
      return;
    }

    initDone();

    // Resolve the current ActorSystem once for the lifetime of the loop.
    const sys = ActorSystem.current;

    const loop = async () => {
      while (Proc.alive(me)) {
        // Try synchronous dequeue first; only block on await if mailbox is empty.
        let msg = sys.shiftMessage(me);
        if (msg === undefined) {
          msg = await M.receiveMessage(me);
        }

        // Dispatch — no per-message yield; batch-drain via the outer while.
        if (msg && typeof msg === 'object' && msg !== null) {
          const tagged = msg as { __gen_server__?: string; replyTo?: PID; ref?: Ref };

          if (tagged.__gen_server__ === 'call' && callbacks.handle_call) {
            const { replyTo, ref, payload } = msg as GenCallMsg;
            const from: From = { pid: replyTo, ref };
            try {
              const proc = sys.getProcess(me);
              const timeout = proc?.execTimeout ?? 0;
              const handler = callbacks.handle_call(payload, from, state, me);
              const result = timeout > 0
                ? await Promise.race([
                    handler,
                    new Promise<never>((_, rej) =>
                      setTimeout(() => rej(new TimeoutError('execution timeout')), timeout),
                    ),
                  ])
                : await handler;
              state = result.state;
              if ('reply' in result) {
                resolvePending(ref, { ok: result.reply });
              }
            } catch (err) {
              if (err instanceof TimeoutError && err.message === 'execution timeout') {
                const proc = sys.getProcess(me);
                if (proc) {
                  proc.execTimeoutCount++;
                  console.error(
                    `[actojs] execution timeout in ${me} (${proc.execTimeoutCount})`,
                  );
                  if (proc.execTimeoutCount >= 3) {
                    resolvePending(ref, { error: new Error('too many execution timeouts') });
                    Proc.exit(me, 'too_many_exec_timeouts');
                    return;
                  }
                }
                resolvePending(ref, { error: err });
              } else {
                resolvePending(ref, { error: err });
              }
            }
          } else if (tagged.__gen_server__ === 'cast' && callbacks.handle_cast) {
            const { payload } = msg as GenCastMsg;
            try {
              const proc = sys.getProcess(me);
              const timeout = proc?.execTimeout ?? 0;
              const handler = callbacks.handle_cast(payload, state, me);
              const result = timeout > 0
                ? await Promise.race([
                    handler,
                    new Promise<never>((_, rej) =>
                      setTimeout(() => rej(new TimeoutError('execution timeout')), timeout),
                    ),
                  ])
                : await handler;
              if (result) state = result.state;
            } catch (err) {
              if (err instanceof TimeoutError && err.message === 'execution timeout') {
                const proc = sys.getProcess(me);
                if (proc) {
                  proc.execTimeoutCount++;
                  console.error(
                    `[actojs] execution timeout in ${me} (${proc.execTimeoutCount})`,
                  );
                  if (proc.execTimeoutCount >= 3) {
                    Proc.exit(me, 'too_many_exec_timeouts');
                    return;
                  }
                }
              }
            }
          } else if (tagged.__gen_server__ === 'stop') {
            const { reason, __stop_ref } = msg as GenStopMsg;
            Proc.put('__terminate_info__', { reason, exitType: 'stop' } satisfies TerminateInfo);
            try {
              await callbacks.terminate?.(reason, state);
            } catch (err) {
              console.error(`[actojs] terminate error in ${me}: ${String(err)}`);
            }
            Proc.deleteKey('__terminate_info__');
            if (__stop_ref) resolvePending(__stop_ref, { ok: undefined });
            Proc.exit(me, reason ?? 'normal');
            return;
          } else if (callbacks.handle_info) {
            try {
              const result = await callbacks.handle_info(msg, state, me);
              if (result) state = result.state;
            } catch (_) {}
          }
        } else if (callbacks.handle_info) {
          try {
            const result = await callbacks.handle_info(msg, state, me);
            if (result) state = result.state;
          } catch (_) {}
        }

        // Only yield at budget boundaries, not after every single message.
        if (sys.countMessage(me)) {
          await sys.doYield(me);
        }
      }
    };

    await loop();
  };

  const pid = opts?.link ? Proc.spawn_link(spawnFn) : Proc.spawn(spawnFn);

  try {
    await ready;
  } catch (err) {
    return { error: err instanceof Error ? err : new Error(String(err)) };
  }

  if (!Proc.alive(pid)) {
    return { error: new Error('process exited during init') };
  }

  return { ok: pid };
}

// ---- Call / Cast / Stop helpers ------------------------------------------

function resolvePending(ref: Ref, result: { ok: unknown } | { error: unknown }): void {
  const sys = ActorSystem.current;
  const pending = sys.pendingCalls.get(ref);
  if (!pending) return;
  sys.pendingCalls.delete(ref);
  if (pending.timer) clearTimeout(pending.timer);
  if ('error' in result) {
    pending.reject(result.error);
  } else {
    pending.resolve(result.ok);
  }
}

export function genCall(pid: PID, msg: unknown, timeout?: number): Promise<unknown> {
  const ref: Ref = Symbol('gen_call');
  let replyTo: PID | null = null;
  try { replyTo = Proc.self(); } catch (_) { /* outside process, no replyTo */ }

  let resolve: (v: unknown) => void;
  let reject: (e: unknown) => void;
  const promise = new Promise<unknown>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  const pending: PendingCall = { resolve: resolve!, reject: reject! };
  if (timeout != null) {
    pending.timer = setTimeout(() => {
      ActorSystem.current.pendingCalls.delete(ref);
      reject!(new TimeoutError('genCall timed out'));
    }, timeout);
  }
  ActorSystem.current.pendingCalls.set(ref, pending);

  Proc.send(pid, { __gen_server__: 'call', ref, payload: msg, replyTo });
  return promise;
}

export function genCast(pid: PID, msg: unknown): void {
  Proc.send(pid, { __gen_server__: 'cast', payload: msg });
}

// ---- reply (deferred) ----------------------------------------------------

export function reply(from: From, msg: unknown): void {
  resolvePending(from.ref, { ok: msg });
}

// ---- stop ----------------------------------------------------------------

export function genStop(pid: PID, reason?: unknown, timeout?: number): Promise<void> {
  return new Promise((resolve, reject) => {
    const ref: Ref = Symbol('gen_stop');
    const pending: PendingCall = { resolve: resolve as (v: unknown) => void, reject };
    if (timeout != null) {
      pending.timer = setTimeout(() => {
        ActorSystem.current.pendingCalls.delete(ref);
        reject(new TimeoutError('genStop timed out'));
      }, timeout);
    }
    ActorSystem.current.pendingCalls.set(ref, pending);
    Proc.send(pid, { __gen_server__: 'stop', reason, __stop_ref: ref });
    if (!Proc.alive(pid)) {
      if (pending.timer) clearTimeout(pending.timer);
      ActorSystem.current.pendingCalls.delete(ref);
      resolve();
    }
  });
}
