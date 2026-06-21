// actojs — Mailbox: per-process message queue with blocking receive.
// Web runtime: promise-based cooperative multiplexing.

import type { PID, Ref, ProcessInfo } from './types';

export interface ProcessState {
  pid: PID;
  mailbox: unknown[];
  // promise resolve for blocking receive()
  recvResolve: ((msg: unknown) => void) | null;
  links: Set<PID>;
  // refs we hold to monitor others: ref -> monitored pid
  monitors: Map<Ref, PID>;
  // other processes monitoring us: pid -> refs they gave us
  monitoredBy: Map<PID, Ref[]>;
  trapExit: boolean;
  status: 'running' | 'alive' | 'exiting' | 'exited';
  exitReason: unknown;
  processDict: Map<string, unknown>;
  registeredName: string | null;
}

const processes = new Map<PID, ProcessState>();
const nameRegistry = new Map<string, PID>();
let nextPidCounter = 0;

// Stack of current process PIDs. The top is the "current" process.
// Nested spawns push/pop to avoid clobbering.
const pidStack: (PID | null)[] = [];

export function generatePid(): PID {
  return `#PID<0.${nextPidCounter++}.0>`;
}

export function createProcess(pid: PID): ProcessState {
  return {
    pid,
    mailbox: [],
    recvResolve: null,
    links: new Set(),
    monitors: new Map(),
    monitoredBy: new Map(),
    trapExit: false,
    status: 'running',
    exitReason: null,
    processDict: new Map(),
    registeredName: null,
  };
}

export function getProcess(pid: PID): ProcessState | undefined {
  return processes.get(pid);
}

export function getCurrentPid(): PID | null {
  if (pidStack.length === 0) return null;
  return pidStack[pidStack.length - 1] ?? null;
}

export function setCurrentPid(pid: PID | null): void {
  // replaced by push/pop; kept for backward compat
  pidStack.push(pid);
}

export function pushPid(pid: PID): void {
  pidStack.push(pid);
}

export function popPid(): void {
  pidStack.pop();
}

export function clearPidStack(): void {
  pidStack.length = 0;
}

export function registerProcess(pid: PID, proc: ProcessState): void {
  processes.set(pid, proc);
}

export function deregisterProcess(pid: PID): void {
  const proc = processes.get(pid);
  if (proc && proc.registeredName) {
    nameRegistry.delete(proc.registeredName);
  }
  processes.delete(pid);
}

export function allPids(): PID[] {
  return Array.from(processes.keys());
}

// Name registry
export function registerName(name: string, pid: PID): void {
  nameRegistry.set(name, pid);
}

export function unregisterName(name: string): void {
  nameRegistry.delete(name);
}

export function whereisName(name: string): PID | null {
  return nameRegistry.get(name) ?? null;
}

// Deliver a message to a process mailbox
export function deliverMessage(pid: PID, msg: unknown): void {
  const proc = processes.get(pid);
  if (!proc) return;
  if (proc.status === 'exited' || proc.status === 'exiting') return;

  if (proc.recvResolve) {
    // Process is waiting in receive() — deliver immediately
    const resolve = proc.recvResolve;
    proc.recvResolve = null;
    resolve(msg);
  } else {
    proc.mailbox.push(msg);
  }
}

// Block until a message arrives
export function receiveMessage(pid?: PID): Promise<unknown> {
  const effectivePid = pid ?? getCurrentPid();
  if (!effectivePid) return Promise.reject(new Error('not inside a process'));

  const proc = processes.get(effectivePid);
  if (!proc) return Promise.reject(new Error('process not found'));

  if (proc.mailbox.length > 0) {
    return Promise.resolve(proc.mailbox.shift());
  }

  return new Promise(resolve => {
    proc.recvResolve = resolve;
  });
}

export function getMailboxLength(pid: PID): number {
  const proc = processes.get(pid);
  return proc ? proc.mailbox.length + (proc.recvResolve ? 0 : 0) : 0;
}

// Handle process exit: notify linked and monitoring processes
export function handleExit(proc: ProcessState): void {
  proc.status = 'exited';

  // Notify linked processes
  for (const linkedPid of proc.links) {
    const linked = processes.get(linkedPid);
    if (linked && linked.status !== 'exited' && linked.status !== 'exiting') {
      if (linked.trapExit) {
        deliverMessage(linkedPid, {
          type: 'EXIT',
          from: proc.pid,
          reason: proc.exitReason,
        });
      } else {
        // Propagate exit
        linked.status = 'exiting';
        linked.exitReason = proc.exitReason;
        handleExit(linked);
      }
    }
  }

  // Notify monitoring processes
  for (const [monitorPid, refs] of proc.monitoredBy) {
    const monitor = processes.get(monitorPid);
    if (monitor && monitor.status !== 'exited') {
      for (const ref of refs) {
        deliverMessage(monitorPid, {
          type: 'DOWN',
          ref,
          pid: proc.pid,
          reason: proc.exitReason,
        });
      }
    }
  }

  // Cleanup
  deregisterProcess(proc.pid);
}

export function getProcessInfo(pid: PID): ProcessInfo | null {
  const proc = processes.get(pid);
  if (!proc) return null;
  return {
    status: proc.status,
    messageQueueLength: getMailboxLength(pid),
    links: Array.from(proc.links),
    monitors: Array.from(proc.monitors.entries()).map(([ref, p]) => ({ ref, pid: p })),
    monitoredBy: Array.from(proc.monitoredBy.entries()).map(([p, refs]) => ({ pid: p, ref: refs })),
    trapExit: proc.trapExit,
    registeredName: proc.registeredName,
  };
}

// Wrap a function execution with currentPid tracking
export function runWithPid<T>(pid: PID, fn: () => T): T {
  pidStack.push(pid);
  try {
    const result = fn();
    if (result instanceof Promise) {
      return result
        .then(v => {
          pidStack.pop();
          return v;
        })
        .catch(e => {
          pidStack.pop();
          throw e;
        }) as unknown as T;
    }
    pidStack.pop();
    return result;
  } catch (e) {
    pidStack.pop();
    throw e;
  }
}
