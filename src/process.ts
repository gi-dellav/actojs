// acto/process — Low-level process primitives.
// Web runtime: cooperative event-loop scheduler.

import type { PID, Ref, Dest, SpawnOpt, ProcessInfo } from './types';
import { ActorSystem } from './system';
import * as M from './mailbox';

export { TimeoutError } from './system';

// ---- spawn ----------------------------------------------------------------

export function spawn(fn: () => void | Promise<void>, opts?: SpawnOpt[]): PID {
  const sys = ActorSystem.current;
  const pid = sys.generatePid();
  const proc = sys.createProcess(pid);
  sys.registerProcess(pid, proc);

  const link = opts?.includes('link') ?? false;
  const monitor = opts?.includes('monitor') ?? false;
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

export function spawn_link(fn: () => void): PID {
  return spawn(fn, ['link']);
}

// ---- send -----------------------------------------------------------------

export function send(dest: Dest, msg: unknown): void {
  if (typeof dest === 'string') {
    // Could be a PID or a registered name
    const named = M.whereisName(dest);
    if (named) {
      M.deliverMessage(named, msg);
      return;
    }
    // Assume it's a PID
    M.deliverMessage(dest, msg);
  }
}

// ---- self -----------------------------------------------------------------

export function self(): PID {
  const pid = M.getCurrentPid();
  if (!pid) throw new Error('self() called outside of a process');
  return pid;
}

// ---- alive? ---------------------------------------------------------------

export function alive(pid: PID): boolean {
  const proc = M.getProcess(pid);
  return proc != null && proc.status !== 'exited' && proc.status !== 'exiting';
}

// ---- exit -----------------------------------------------------------------

export function exit(pid: PID, reason: unknown): void {
  const proc = M.getProcess(pid);
  if (proc && proc.status !== 'exited' && proc.status !== 'exiting') {
    proc.status = 'exiting';
    proc.exitReason = reason;
    M.handleExit(proc);
  }
}

// ---- link / unlink --------------------------------------------------------

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

export function unlink(pid: PID): void {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('unlink() called outside of a process');
  const callerProc = M.getProcess(caller);
  const other = M.getProcess(pid);
  if (callerProc) callerProc.links.delete(pid);
  if (other) other.links.delete(caller);
}

// ---- monitor / demonitor --------------------------------------------------

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

export function flag(flag: string, value: boolean): boolean {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('flag() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  if (flag === 'trap_exit') {
    const prev = proc.trapExit;
    proc.trapExit = value;
    return prev;
  }
  return false;
}

// ---- name registry --------------------------------------------------------

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

export function whereis(name: string): PID | null {
  return M.whereisName(name);
}

// ---- list -----------------------------------------------------------------

export function list(): PID[] {
  return M.allPids().filter(pid => alive(pid));
}

// ---- sleep ----------------------------------------------------------------

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ---- send_after / cancel_timer --------------------------------------------

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

export function cancel_timer(ref: Ref): void {
  const sys = ActorSystem.current;
  const handle = sys.timers.get(ref);
  if (handle) {
    clearTimeout(handle);
    sys.timers.delete(ref);
  }
}

// ---- info -----------------------------------------------------------------

export function info(pid: PID): ProcessInfo | null {
  return M.getProcessInfo(pid);
}

// ---- process dictionary ---------------------------------------------------

export function get(key: string): unknown {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('get() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  return proc.processDict.get(key);
}

export function put(key: string, value: unknown): unknown {
  const caller = M.getCurrentPid();
  if (!caller) throw new Error('put() called outside of a process');
  const proc = M.getProcess(caller);
  if (!proc) throw new Error('process not found');
  const prev = proc.processDict.get(key);
  proc.processDict.set(key, value);
  return prev;
}

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

export function receive(timeout?: number): Promise<unknown> {
  const caller = M.getCurrentPid();
  if (!caller) return Promise.reject(new Error('receive() called outside of a process'));
  if (timeout != null) {
    return M.receiveMessage(undefined, timeout);
  }
  return M.receiveMessage();
}
