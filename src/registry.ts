// acto/registry — Local, decentralised key-value process store.
// Web runtime: cooperative event-loop, built on GenServer.

import type { PID, Ref, RegistryStartOptions, RegistryKeyMode, DownMessage } from './types';
import type { From } from './system';
import * as Proc from './process';
import * as GS from './gen_server';

type RegistryCallback = (pid: PID, value: unknown) => void;

interface RegistryEntry {
  pid: PID;
  value: unknown;
}

interface RegistryState {
  keys: RegistryKeyMode;
  partitions: Map<number, Map<string, RegistryEntry[]>>;
  listeners: string[];
  meta: Record<string, unknown>;
}

function resolveKeyMode(mode: RegistryKeyMode): 'unique' | 'duplicate_key' | 'duplicate_pid' {
  if (mode === 'unique') return 'unique';
  if (mode === 'duplicate') return 'duplicate_key';
  if (typeof mode === 'object' && 'duplicate' in mode) {
    if ((mode as { duplicate: string }).duplicate === 'key') return 'duplicate_key';
    if ((mode as { duplicate: string }).duplicate === 'pid') return 'duplicate_pid';
  }
  return 'unique';
}

function getPartition(key: string, numPartitions: number): number {
  let hash = 0;
  for (let i = 0; i < key.length; i++) {
    hash = ((hash << 5) - hash + key.charCodeAt(i)) | 0;
  }
  return Math.abs(hash) % numPartitions;
}

// ---- start_link -----------------------------------------------------------

/** Start a local, decentralised key-value registry process and link it to the caller. */
export async function start_link(opts: RegistryStartOptions): Promise<{ ok: PID } | { error: Error }> {
  const numPartitions = opts.partitions ?? 1;
  const partitions = new Map<number, Map<string, RegistryEntry[]>>();
  for (let i = 0; i < numPartitions; i++) {
    partitions.set(i, new Map());
  }

  const initState: RegistryState = {
    keys: opts.keys,
    partitions,
    listeners: opts.listeners ?? [],
    meta: opts.meta ?? {},
  };

  return GS.startGenServer<RegistryState>(
    {
      init(): RegistryState {
        return initState;
      },

      handle_call(msg: unknown, from: From, s: RegistryState, myPid: PID) {
        const { type, payload } = msg as { type: string; payload: unknown };
        const caller = from.pid;
        if (!caller) return { reply: { error: 'not_in_process' }, state: s };
        const keyMode = resolveKeyMode(s.keys);

        if (type === 'register') {
          const { key, value } = payload as { key: string; value: unknown };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          let entries = part.get(key) ?? [];

          if (keyMode === 'unique') {
            if (entries.length > 0) {
              return { reply: { error: 'already_registered' }, state: s };
            }
            entries.push({ pid: caller, value });
          } else if (keyMode === 'duplicate_key') {
            entries.push({ pid: caller, value });
          } else {
            // duplicate_pid: one entry per pid per key
            const existing = entries.find(e => e.pid === caller);
            if (existing) {
              return { reply: { error: 'already_registered' }, state: s };
            }
            entries.push({ pid: caller, value });
          }

          part.set(key, entries);
          Proc.monitor(caller, myPid);
          notifyListeners(s, 'register', key, caller, value);
          return { reply: { ok: caller }, state: s };
        }

        if (type === 'unregister') {
          const { key } = payload as { key: string };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          let entries = part.get(key) ?? [];
          const before = entries.length;
          const removed = entries.find(e => e.pid === caller);
          entries = entries.filter(e => e.pid !== caller);
          if (entries.length > 0) part.set(key, entries);
          else part.delete(key);
          if (removed) {
            notifyListeners(s, 'unregister', key, caller, removed.value);
          }
          return { reply: before !== entries.length ? undefined : { error: 'not_found' }, state: s };
        }

        if (type === 'lookup') {
          const { key } = payload as { key: string };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          const entries = part.get(key) ?? [];
          return { reply: entries.map(e => ({ pid: e.pid, value: e.value })), state: s };
        }

        if (type === 'match') {
          const { key, pattern, guards } = payload as { key: string; pattern: unknown; guards?: (value: unknown) => boolean };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          const entries = part.get(key) ?? [];
          const matched = entries.filter(e => {
            try {
              const passesPattern = matchPattern(e.value, pattern);
              if (!passesPattern) return false;
              if (guards && !guards(e.value)) return false;
              return true;
            } catch {
              return false;
            }
          });
          return { reply: matched.map(e => ({ pid: e.pid, value: e.value })), state: s };
        }

        if (type === 'dispatch') {
          const { key, callback, opts: dispOpts } = payload as { key: string; callback: RegistryCallback; opts?: { limit?: number } };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          const entries = part.get(key) ?? [];
          const limit = dispOpts?.limit ?? entries.length;
          for (const entry of entries.slice(0, limit)) {
            try {
              callback(entry.pid, entry.value);
            } catch (_) {}
          }
          return { reply: undefined, state: s };
        }

        if (type === 'keys') {
          const { pid } = payload as { pid: PID };
          const result: string[] = [];
          for (const part of s.partitions.values()) {
            for (const [k, entries] of part) {
              if (entries.some(e => e.pid === pid)) {
                result.push(k);
              }
            }
          }
          return { reply: result, state: s };
        }

        if (type === 'values') {
          const { key, pid } = payload as { key: string; pid: PID };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          const entries = part.get(key) ?? [];
          const vals = entries.filter(e => e.pid === pid).map(e => e.value);
          return { reply: vals, state: s };
        }

        if (type === 'count') {
          let total = 0;
          for (const part of s.partitions.values()) {
            for (const entries of part.values()) {
              total += entries.length;
            }
          }
          return { reply: total, state: s };
        }

        if (type === 'update_value') {
          const { key, fn } = payload as { key: string; fn: (value: unknown) => unknown };
          const partIdx = getPartition(key, s.partitions.size);
          const part = s.partitions.get(partIdx)!;
          const entries = part.get(key) ?? [];
          const idx = entries.findIndex(e => e.pid === caller);
          if (idx === -1) {
            return { reply: { error: 'not_found' }, state: s };
          }
          const oldValue = entries[idx]!.value;
          const newValue = fn(oldValue);
          entries[idx]!.value = newValue;
          part.set(key, entries);
          return { reply: { newValue, oldValue }, state: s };
        }

        return { reply: undefined, state: s };
      },

      handle_info(msg: unknown, s: RegistryState, _myPid: PID) {
        // Handle EXIT signals from monitored processes
        if (msg && typeof msg === 'object' && msg !== null && (msg as DownMessage).type === 'DOWN') {
          const { pid: downPid } = msg as DownMessage;
          cleanupPid(s, downPid);
        }
        return { noreply: undefined, state: s };
      },
    },
    null,
    { name: opts.name, link: true },
  );
}

function cleanupPid(s: RegistryState, pid: PID): void {
  for (const part of s.partitions.values()) {
    for (const [key, entries] of part) {
      const before = entries.length;
      const remaining = entries.filter(e => e.pid !== pid);
      if (remaining.length === 0) part.delete(key);
      else if (remaining.length !== before) part.set(key, remaining);
    }
  }
}

function notifyListeners(s: RegistryState, event: string, key: string, pid: PID, value: unknown): void {
  for (const listenerName of s.listeners) {
    const listenerPid = Proc.whereis(listenerName);
    if (listenerPid) {
      Proc.send(listenerPid, { __registry_event__: true, event, key, pid, value });
    }
  }
}

// ---- match helper ---------------------------------------------------------

function matchPattern(value: unknown, pattern: unknown): boolean {
  if (pattern === null || pattern === undefined) return value === pattern;
  if (typeof pattern === 'object' && pattern !== null) {
    if (typeof value !== 'object' || value === null) return false;
    const p = pattern as Record<string, unknown>;
    const v = value as Record<string, unknown>;
    for (const key of Object.keys(p)) {
      if (!(key in v)) return false;
      if (!matchPattern(v[key], p[key])) return false;
    }
    return true;
  }
  return value === pattern;
}

// ---- public API -----------------------------------------------------------

/** Register the caller's PID under a key with an associated value. */
export function register(
  reg: PID,
  key: string,
  value: unknown,
  timeout?: number,
): Promise<{ ok: PID } | { error: string }> {
  return GS.genCall(reg, { type: 'register', payload: { key, value } }, timeout) as Promise<{ ok: PID } | { error: string }>;
}

/** Remove the caller's registration for the given key. */
export function unregister(reg: PID, key: string, timeout?: number): Promise<void> {
  return GS.genCall(reg, { type: 'unregister', payload: { key } }, timeout) as Promise<void>;
}

/** Return all {pid, value} entries registered under the given key. */
export function lookup(
  reg: PID,
  key: string,
  timeout?: number,
): Promise<{ pid: PID; value: unknown }[]> {
  return GS.genCall(reg, { type: 'lookup', payload: { key } }, timeout) as Promise<{ pid: PID; value: unknown }[]>;
}

/** Find entries whose values match a pattern object, with optional guard functions. */
export function match(
  reg: PID,
  key: string,
  pattern: unknown,
  guards?: (value: unknown) => boolean,
  timeout?: number,
): Promise<{ pid: PID; value: unknown }[]> {
  return GS.genCall(reg, { type: 'match', payload: { key, pattern, guards } }, timeout) as Promise<{ pid: PID; value: unknown }[]>;
}

/** Invoke a callback for each entry under the key, up to an optional limit. */
export function dispatch(
  reg: PID,
  key: string,
  callback: RegistryCallback,
  opts?: { limit?: number },
  timeout?: number,
): Promise<void> {
  return GS.genCall(reg, { type: 'dispatch', payload: { key, callback, opts } }, timeout) as Promise<void>;
}

/** Return all keys under which the given PID is registered. */
export function keys(reg: PID, pid: PID, timeout?: number): Promise<string[]> {
  return GS.genCall(reg, { type: 'keys', payload: { pid } }, timeout) as Promise<string[]>;
}

/** Return all values associated with a specific PID under the given key. */
export function values(reg: PID, key: string, pid: PID, timeout?: number): Promise<unknown[]> {
  return GS.genCall(reg, { type: 'values', payload: { key, pid } }, timeout) as Promise<unknown[]>;
}

/** Return the total number of registered entries across all partitions. */
export function count(reg: PID, timeout?: number): Promise<number> {
  return GS.genCall(reg, { type: 'count', payload: {} }, timeout) as Promise<number>;
}

/** Atomically update the value for the caller's registration under the given key. */
export function update_value(
  reg: PID,
  key: string,
  fn: (value: unknown) => unknown,
  timeout?: number,
): Promise<{ newValue: unknown; oldValue: unknown } | { error: string }> {
  return GS.genCall(reg, { type: 'update_value', payload: { key, fn } }, timeout) as Promise<{ newValue: unknown; oldValue: unknown } | { error: string }>;
}
