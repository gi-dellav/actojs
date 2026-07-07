// actojs — ActorSystem: isolated actor universe.
// Each ActorSystem owns its own process table, name registry, PID counter,
// pending calls, task results, and timers. Systems are fully isolated.

import type { PID, Ref, ProcessInfo } from './types';

// ---- ProcessState (moved from mailbox.ts) --------------------------------

export interface ProcessState {
  pid: PID;
  mailbox: unknown[];
  recvResolve: ((msg: unknown) => void) | null;
  links: Set<PID>;
  monitors: Map<Ref, PID>;
  monitoredBy: Map<PID, Ref[]>;
  trapExit: boolean;
  status: 'running' | 'alive' | 'exiting' | 'exited';
  exitReason: unknown;
  processDict: Map<string, unknown>;
  registeredName: string | null;
}

export interface PendingCall {
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
  timer?: ReturnType<typeof setTimeout>;
}

export interface TaskResult {
  status: 'pending' | 'done' | 'error';
  value?: unknown;
  error?: unknown;
}

export interface From {
  pid: PID | null;
  ref: Ref;
}

// ---- ActorSystem class ---------------------------------------------------

let _current: ActorSystem | null = null;

export class ActorSystem {
  readonly name: string;
  readonly systemId: string;

  processes: Map<PID, ProcessState> = new Map();
  nameRegistry: Map<string, PID> = new Map();
  nextPidCounter = 0;
  pidStack: (PID | null)[] = [];
  pendingCalls: Map<symbol, PendingCall> = new Map();
  taskResults: Map<Ref, TaskResult> = new Map();
  timers: Map<Ref, ReturnType<typeof setTimeout>> = new Map();

  private static _default: ActorSystem | null = null;

  constructor(name?: string) {
    this.name = name ?? '';
    this.systemId = name ? name : '0';
  }

  // ---- PID generation ----------------------------------------------------

  generatePid(): PID {
    const c = this.nextPidCounter++;
    if (this.systemId === '0') {
      return `#PID<0.${c}.0>`;
    }
    return `#PID<${this.systemId}@0.${c}.0>`;
  }

  // ---- Process helpers (delegated from mailbox.ts) -----------------------

  createProcess(pid: PID): ProcessState {
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

  getProcess(pid: PID): ProcessState | undefined {
    return this.processes.get(pid);
  }

  getCurrentPid(): PID | null {
    if (this.pidStack.length === 0) return null;
    return this.pidStack[this.pidStack.length - 1] ?? null;
  }

  pushPid(pid: PID): void {
    this.pidStack.push(pid);
  }

  popPid(): void {
    this.pidStack.pop();
  }

  clearPidStack(): void {
    this.pidStack.length = 0;
  }

  registerProcess(pid: PID, proc: ProcessState): void {
    this.processes.set(pid, proc);
  }

  deregisterProcess(pid: PID): void {
    const proc = this.processes.get(pid);
    if (proc && proc.registeredName) {
      this.nameRegistry.delete(proc.registeredName);
    }
    this.processes.delete(pid);
  }

  allPids(): PID[] {
    return Array.from(this.processes.keys());
  }

  // ---- Name registry -----------------------------------------------------

  registerName(name: string, pid: PID): void {
    this.nameRegistry.set(name, pid);
  }

  unregisterName(name: string): void {
    this.nameRegistry.delete(name);
  }

  whereisName(name: string): PID | null {
    return this.nameRegistry.get(name) ?? null;
  }

  // ---- Message delivery --------------------------------------------------

  deliverMessage(pid: PID, msg: unknown): void {
    const proc = this.processes.get(pid);
    if (!proc) return;
    if (proc.status === 'exited' || proc.status === 'exiting') return;

    if (proc.recvResolve) {
      const resolve = proc.recvResolve;
      proc.recvResolve = null;
      resolve(msg);
    } else {
      proc.mailbox.push(msg);
    }
  }

  receiveMessage(pid?: PID): Promise<unknown> {
    const effectivePid = pid ?? this.getCurrentPid();
    if (!effectivePid) return Promise.reject(new Error('not inside a process'));

    const proc = this.processes.get(effectivePid);
    if (!proc) return Promise.reject(new Error('process not found'));

    if (proc.mailbox.length > 0) {
      return Promise.resolve(proc.mailbox.shift());
    }

    return new Promise(resolve => {
      proc.recvResolve = resolve;
    });
  }

  getMailboxLength(pid: PID): number {
    const proc = this.processes.get(pid);
    return proc ? proc.mailbox.length + (proc.recvResolve ? 0 : 0) : 0;
  }

  // ---- Exit handling -----------------------------------------------------

  handleExit(proc: ProcessState): void {
    proc.status = 'exited';

    // Notify linked processes
    for (const linkedPid of proc.links) {
      const linked = this.processes.get(linkedPid);
      if (linked && linked.status !== 'exited' && linked.status !== 'exiting') {
        if (linked.trapExit) {
          this.deliverMessage(linkedPid, {
            type: 'EXIT',
            from: proc.pid,
            reason: proc.exitReason,
          });
        } else {
          linked.status = 'exiting';
          linked.exitReason = proc.exitReason;
          this.handleExit(linked);
        }
      }
    }

    // Notify monitoring processes
    for (const [monitorPid, refs] of proc.monitoredBy) {
      const monitor = this.processes.get(monitorPid);
      if (monitor && monitor.status !== 'exited') {
        for (const ref of refs) {
          this.deliverMessage(monitorPid, {
            type: 'DOWN',
            ref,
            pid: proc.pid,
            reason: proc.exitReason,
          });
        }
      }
    }

    this.deregisterProcess(proc.pid);
  }

  getProcessInfo(pid: PID): ProcessInfo | null {
    const proc = this.processes.get(pid);
    if (!proc) return null;
    return {
      status: proc.status,
      messageQueueLength: this.getMailboxLength(pid),
      links: Array.from(proc.links),
      monitors: Array.from(proc.monitors.entries()).map(([ref, p]) => ({ ref, pid: p })),
      monitoredBy: Array.from(proc.monitoredBy.entries()).map(([p, refs]) => ({ pid: p, ref: refs })),
      trapExit: proc.trapExit,
      registeredName: proc.registeredName,
    };
  }

  // ---- PID context tracking ----------------------------------------------

  runWithPid<T>(pid: PID, fn: () => T): T {
    this.pidStack.push(pid);
    try {
      const result = fn();
      if (result instanceof Promise) {
        return result
          .then(v => { this.pidStack.pop(); return v; })
          .catch(e => { this.pidStack.pop(); throw e; }) as unknown as T;
      }
      this.pidStack.pop();
      return result;
    } catch (e) {
      this.pidStack.pop();
      throw e;
    }
  }

  // ---- Static: current system --------------------------------------------

  static get current(): ActorSystem {
    if (!_current) {
      _current = ActorSystem.default;
    }
    return _current!;
  }

  static set current(sys: ActorSystem) {
    _current = sys;
  }

  static get default(): ActorSystem {
    if (!ActorSystem._default) {
      ActorSystem._default = new ActorSystem();
    }
    return ActorSystem._default;
  }

  static run<T>(sys: ActorSystem, fn: () => T): T {
    const prev = _current;
    _current = sys;
    try {
      const result = fn();
      if (result instanceof Promise) {
        return result
          .then(v => { _current = prev; return v; })
          .catch(e => { _current = prev; throw e; }) as unknown as T;
      }
      _current = prev;
      return result;
    } catch (e) {
      _current = prev;
      throw e;
    }
  }
}
