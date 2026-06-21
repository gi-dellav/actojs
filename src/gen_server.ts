// actojs — Internal GenServer base for Agent, Registry, Supervisor, etc.
// Provides a receive loop with handle_call / handle_cast / handle_info.
// Web runtime: cooperative event-loop.

import type { PID, Ref } from './types';
import * as Proc from './process';
import * as M from './mailbox';

export interface GenServerCallbacks<S> {
  init(args: unknown): S | { ok: S } | { error: unknown; reason?: unknown } | Promise<S | { ok: S } | { error: unknown; reason?: unknown }>;
  handle_call?(msg: unknown, from: PID | null, state: S): { reply: unknown; state: S } | { noreply: unknown; state: S } | Promise<{ reply: unknown; state: S } | { noreply: unknown; state: S }>;
  handle_cast?(msg: unknown, state: S): { noreply: unknown; state: S } | Promise<{ noreply: unknown; state: S }>;
  handle_info?(msg: unknown, state: S): { noreply: unknown; state: S } | Promise<{ noreply: unknown; state: S }>;
  terminate?(reason: unknown, state: S): void | Promise<void>;
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

    const loop = async () => {
      while (Proc.alive(me)) {
        const msg = await M.receiveMessage(me);

        if (msg && typeof msg === 'object' && msg !== null) {
          const tagged = msg as { __gen_server__?: string; replyTo?: PID; ref?: Ref };

          if (tagged.__gen_server__ === 'call' && callbacks.handle_call) {
            const { replyTo, ref, payload } = msg as any;
            try {
              const result = await callbacks.handle_call(payload, replyTo, state);
              state = result.state;
              const reply = 'reply' in result ? result.reply : undefined;
              resolvePending(ref, { ok: reply });
            } catch (err) {
              resolvePending(ref, { error: err });
            }
            continue;
          }

          if (tagged.__gen_server__ === 'cast' && callbacks.handle_cast) {
            const { payload } = msg as any;
            try {
              const result = await callbacks.handle_cast(payload, state);
              if (result) state = result.state;
            } catch (_) {}
            continue;
          }

          if (tagged.__gen_server__ === 'stop') {
            const { reason } = msg as any;
            try {
              await callbacks.terminate?.(reason, state);
            } catch (_) {}
            Proc.exit(me, reason ?? 'normal');
            return;
          }
        }

        if (callbacks.handle_info) {
          try {
            const result = await callbacks.handle_info(msg, state);
            if (result) state = result.state;
          } catch (_) {}
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

type PendingCall = {
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
  timer?: ReturnType<typeof setTimeout>;
};

const pendingCalls = new Map<symbol, PendingCall>();

function resolvePending(ref: Ref, result: { ok: unknown } | { error: unknown }): void {
  const pending = pendingCalls.get(ref);
  if (!pending) return;
  pendingCalls.delete(ref);
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
  if (timeout) {
    pending.timer = setTimeout(() => {
      pendingCalls.delete(ref);
      reject!(new Error('timeout'));
    }, timeout);
  }
  pendingCalls.set(ref, pending);

  Proc.send(pid, { __gen_server__: 'call', ref, payload: msg, replyTo });
  return promise;
}

export function genCast(pid: PID, msg: unknown): void {
  Proc.send(pid, { __gen_server__: 'cast', payload: msg });
}

export function genStop(pid: PID, reason?: unknown): Promise<void> {
  return new Promise(resolve => {
    Proc.send(pid, { __gen_server__: 'stop', reason });
    const check = () => {
      if (!Proc.alive(pid)) {
        resolve();
      } else {
        setTimeout(check, 1);
      }
    };
    check();
  });
}
