// acto/process — Low-level process primitives.
// Web runtime: cooperative event-loop scheduler.

import type { PID, Ref, Dest, SpawnOpt, SpawnOptions, ProcessLimits, ProcessInfo } from './types';
import { ActorSystem } from './system';
import * as M from './mailbox';
import { getRuntime } from './core';

export { TimeoutError } from './system';
export type { ProcessLimits, SpawnOptions } from './types';

// ---- spawn ----------------------------------------------------------------

/**
 * Spawn a new process running the given function asynchronously.
 * Supports link/monitor options and per-process resource limits.
 * When a WorkerRuntime is active, delegates to the runtime's spawn.
 */
export function spawn(fn: () => void | Promise<void>, opts?: SpawnOpt[] | SpawnOptions): PID {
  const rt = getRuntime();
  if (rt.spawnProcess) {
    const pid = rt.spawnProcess(fn, opts);
    if (pid != null) return pid;
  }

  const sys = ActorSystem.current;
  const pid = sys.generatePid();
  const proc = sys.createProcess(pid);
  sys.registerProcess(pid, proc);

  // Parse opts: accept legacy SpawnOpt[] or new SpawnOptions object.
  let link = false;
  let monitor = false;
  if (Array.isArray(opts)) {
    link = opts.includes('link');
    monitor = opts.includes('monitor');
  } else if (opts && typeof opts === 'object') {
    link = opts.link ?? false;
    monitor = opts.monitor ?? false;
    // Apply per-process resource limits on top of system defaults.
    const limits: ProcessLimits | undefined = opts.limits;
    if (limits) {
      if (limits.messageBudget != null) proc.messageBudget = limits.messageBudget;
      if (limits.maxMailboxSize != null) proc.maxMailboxSize = limits.maxMailboxSize;
      if (limits.execTimeout != null) proc.execTimeout = limits.execTimeout;
      if (limits.maxMemory != null) proc.maxMemory = limits.maxMemory;
    }
  }
  const caller = sys.getCurrentPid();

  if (link && caller) {
    const callerProc = sys.getProcess(caller);
    if (callerProc) {
      proc.links.add(caller);
      callerProc.links.add(pid);
    }
  }

  if (monitor && caller) {
    const ref: Ref = Symbol('monitor');
    const callerProc = sys.getProcess(caller);
    if (callerProc) {
      proc.monitoredBy.set(caller, [ref]);
      callerProc.monitors.set(ref, pid);
    }
  }

  // Schedule execution within the captured system
  queueMicrotask(() => {
    ActorSystem.run(sys, () => {
    const result = sys.runWithPid(pid, () => {
      try {
        return fn();
      } catch (err) {
        proc.exitReason = err;
        return undefined;
      }
    });

    const finish = (err?: unknown) => {
      if (proc.status === 'running') {
        proc.status = 'exiting';
        proc.exitReason = proc.exitReason ?? err ?? 'normal';
      }
      sys.handleExit(proc);
    };

    if (result instanceof Promise) {
      result.then(finish, finish);
    } else {
      finish();
    }
    }); // ActorSystem.run
  });

  return pid;
}

/** Spawn a new process and link it to the caller. */
export function spawn_link(fn: () => void): PID {
  return spawn(fn, ['link']);
}

/** Spawn a new process and monitor it, returning the PID and the monitor reference. */
export function spawn_monitor(fn: () => void | Promise<void>): { pid: PID; ref: Ref } {
  const rt = getRuntime();
  if (rt.spawnProcess) {
    const pid = rt.spawnProcess(fn, ['monitor']);
    if (pid != null) {
      const caller = ActorSystem.current.getCurrentPid();
      if (caller) {
        const callerProc = ActorSystem.current.getProcess(caller);
        if (callerProc) {
          const found = Array.from(callerProc.monitors.entries()).find(([, t]) => t === pid);
          if (found) return { pid, ref: found[0] };
        }
      }
    }
  }

  const sys = ActorSystem.current;
  const pid = sys.generatePid();
  const proc = sys.createProcess(pid);
  sys.registerProcess(pid, proc);

  const ref: Ref = Symbol('monitor');
  const caller = sys.getCurrentPid();
  if (caller) {
    const callerProc = sys.getProcess(caller);
    if (callerProc) {
      proc.monitoredBy.set(caller, [ref]);
      callerProc.monitors.set(ref, pid);
    }
  }

  queueMicrotask(() => {
    ActorSystem.run(sys, () => {
    const result = sys.runWithPid(pid, () => {
      try {
        return fn();
      } catch (err) {
        proc.exitReason = err;
        return undefined;
      }
    });

    const finish = (err?: unknown) => {
      if (proc.status === 'running') {
        proc.status = 'exiting';
        proc.exitReason = proc.exitReason ?? err ?? 'normal';
      }
      sys.handleExit(proc);
    };

    if (result instanceof Promise) {
      result.then(finish, finish);
    } else {
      finish();
    }
    });
  });

  return { pid, ref };
}

// ---- send -----------------------------------------------------------------

/** Deliver a message to a destination, resolving registered names to PIDs. */
export function send(dest: Dest, msg: unknown): void {
  if (typeof dest === 'string') {
    // Fast-path: PIDs have the format #PID<...> — skip name registry lookup.
    if (dest.startsWith('#PID<')) {
      M.deliverMessage(dest, msg);
      return;
    }
    // Could be a registered name
    const named = M.whereisName(dest);
    if (named) {
      M.deliverMessage(named, msg);
      return;
    }
    // Assume it's a raw PID (e.g. custom format)
    M.deliverMessage(dest, msg);
  }
}

// ---- self -----------------------------------------------------------------

/** Return the PID of the currently executing process. Throws if called outside a process. */
export function self(): PID {
  const pid = M.getCurrentPid();
  if (!pid) throw new Error('self() called outside of a process');
  return pid;
}

// ---- alive? ---------------------------------------------------------------

/** Check whether a process is still alive (not exited or exiting). */
export function alive(pid: PID): boolean {
  const proc = M.getProcess(pid);
  return proc != null && proc.status !== 'exited' && proc.status !== 'exiting';
}

// ---- exit -----------------------------------------------------------------

/** Force a process to exit with the given reason. Triggers the full exit protocol. */
export function exit(pid: PID, reason: unknown): void {
  const proc = M.getProcess(pid);
  if (proc && proc.status !== 'exited' && proc.status !== 'exiting') {
    proc.status = 'exiting';
    proc.exitReason = reason;
    M.handleExit(proc);
  }
}

// ---- link / unlink --------------------------------------------------------

/** Establish a bidirectional link between the caller and the target process. */
export function link(pid: PID, callerPid?: PID): void {
  const caller = callerPid ?? M.getCurrentPid();
  if (!caller) throw new Error('link() called outside of a process');
  const callerProc = M.getProcess(caller);
  const other = M.getProcess(pid);
  if (callerProc && other) {
    callerProc.links.add(pid);
    other.links.add(caller);
  }
}

/** Remove a link between the caller and the target process. */
export function unlink(pid: PID): void {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('unlink() called outside of a process');
  const callerProc = M.getProcess(caller);
  const other = M.getProcess(pid);
  if (callerProc) callerProc.links.delete(pid);
  if (other) other.links.delete(caller);
}

// ---- monitor / demonitor --------------------------------------------------

/** Monitor a process. Returns a ref that can be pattern-matched on DOWN messages. */
export function monitor(pid: PID, callerPid?: PID): Ref {
  const caller = callerPid ?? M.getCurrentPid();
  if (!caller) throw new Error('monitor() called outside of a process');
  const callerProc = M.getProcess(caller);
  const other = M.getProcess(pid);
  if (!callerProc) throw new Error('caller process not found');
  const ref: Ref = Symbol('monitor');
  callerProc.monitors.set(ref, pid);
  if (other) {
    const refs = other.monitoredBy.get(caller) ?? [];
    refs.push(ref);
    other.monitoredBy.set(caller, refs);
  }
  return ref;
}

/** Stop monitoring a process by its monitor reference. */
export function demonitor(ref: Ref): void {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('demonitor() called outside of a process');
  const callerProc = M.getProcess(caller);
  if (!callerProc) return;
  const monitoredPid = callerProc.monitors.get(ref);
  callerProc.monitors.delete(ref);
  if (monitoredPid) {
    const other = M.getProcess(monitoredPid);
    if (other) {
      const refs = (other.monitoredBy.get(caller) ?? []).filter(r => r !== ref);
      if (refs.length > 0) other.monitoredBy.set(caller, refs);
      else other.monitoredBy.delete(caller);
    }
  }
}

// ---- flag -----------------------------------------------------------------

/** Get or set process flags and resource limits (trap_exit, message_budget, etc.). */
export function flag(flag: string, value: unknown): unknown {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('flag() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  if (flag === 'trap_exit') {
    const prev = proc.trapExit;
    proc.trapExit = !!value;
    return prev;
  }
  // Fault-isolation flags: return the previous numeric value.
  if (flag === 'message_budget') {
    const prev = proc.messageBudget;
    proc.messageBudget = Number(value) || 0;
    return prev;
  }
  if (flag === 'max_mailbox_size') {
    const prev = proc.maxMailboxSize;
    proc.maxMailboxSize = Number(value) || 0;
    return prev;
  }
  if (flag === 'exec_timeout') {
    const prev = proc.execTimeout;
    proc.execTimeout = Number(value) || 0;
    return prev;
  }
  if (flag === 'max_memory') {
    const prev = proc.maxMemory;
    proc.maxMemory = Number(value) || 0;
    return prev;
  }
  return false;
}

// ---- name registry --------------------------------------------------------

/** Register a process under a human-readable name for later lookup via whereis. */
export function register(pid: PID, name: string, callerPid?: PID): void {
  const caller = callerPid ?? M.getCurrentPid();
  if (!caller) throw new Error('register() called outside of a process');
  const proc = M.getProcess(pid);
  if (!proc) throw new Error('process not found');
  if (proc.registeredName) {
    M.unregisterName(proc.registeredName);
  }
  M.registerName(name, pid);
  proc.registeredName = name;
}

/** Remove a name from the registry and clear it from the owning process. */
export function unregister(name: string): void {
  M.unregisterName(name);
  // Also clear the process's registered name
  for (const pid of M.allPids()) {
    const proc = M.getProcess(pid);
    if (proc && proc.registeredName === name) {
      proc.registeredName = null;
      break;
    }
  }
}

/** Look up a PID by its registered name, or null if not found. */
export function whereis(name: string): PID | null {
  return M.whereisName(name);
}

// ---- list -----------------------------------------------------------------

/** Return the PIDs of all currently alive processes in the system. */
export function list(): PID[] {
  return M.allPids().filter(pid => alive(pid));
}

// ---- sleep ----------------------------------------------------------------

/** Suspend the current process for the given number of milliseconds. */
export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---- send_after / cancel_timer --------------------------------------------

/** Schedule a message to be delivered after the given delay. Returns a timer reference. */
export function send_after(dest: Dest, msg: unknown, ms: number): Ref {
  const sys = ActorSystem.current;
  const ref: Ref = Symbol('timer');
  const handle = setTimeout(() => {
    sys.timers.delete(ref);
    send(dest, msg);
  }, ms);
  sys.timers.set(ref, handle);
  return ref;
}

/** Cancel a scheduled timer by its reference. */
export function cancel_timer(ref: Ref): void {
  const sys = ActorSystem.current;
  const handle = sys.timers.get(ref);
  if (handle) {
    clearTimeout(handle);
    sys.timers.delete(ref);
  }
}

// ---- info -----------------------------------------------------------------

/** Return a snapshot of public information for the given process. */
export function info(pid: PID): ProcessInfo | null {
  return M.getProcessInfo(pid);
}

// ---- process dictionary ---------------------------------------------------

/** Read a value from the current process's dictionary, with an optional default. */
export function get(key: string, defaultValue?: unknown): unknown {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('get() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  const val = proc.processDict.get(key);
  return val !== undefined ? val : defaultValue;
}

/** Store a value in the current process's dictionary. Returns the previous value. */
export function put(key: string, value: unknown): unknown {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('put() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  const prev = proc.processDict.get(key);
  proc.processDict.set(key, value);
  return prev;
}

/** Return all keys in the current process's dictionary, optionally matching a value. */
export function get_keys(value?: unknown): string[] {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('get_keys() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  if (arguments.length === 0) {
    return Array.from(proc.processDict.keys());
  }
  const keys: string[] = [];
  proc.processDict.forEach((v, k) => {
    if (v === value) keys.push(k);
  });
  return keys;
}

/** Remove a key from the current process's dictionary. Returns the previous value. */
export function deleteKey(key: string): unknown {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('delete() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  const prev = proc.processDict.get(key);
  proc.processDict.delete(key);
  return prev;
}

// ---- receive (internal, for GenServer-style loops) ------------------------

/**
 * Block the current process until a message arrives in its mailbox, with optional timeout.
 */
export function receive(timeout?: number): Promise<unknown> {
  const caller = M.getCurrentPid();
  if (!caller) return Promise.reject(new Error('receive() called outside of a process'));
  if (timeout != null) {
    return M.receiveMessage(undefined, timeout);
  }
  return M.receiveMessage();
}
