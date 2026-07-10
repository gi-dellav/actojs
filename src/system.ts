// actojs — ActorSystem: isolated actor universe.
// Each ActorSystem owns its own process table, name registry, PID counter,
// pending calls, task results, and timers. Systems are fully isolated.

import type { PID, Ref, ProcessInfo } from './types';

export class TimeoutError extends Error {
  constructor(message = 'timeout') {
    super(message);
    this.name = 'TimeoutError';
  }
}

export interface ExitReport {
  pid: PID;
  reason: unknown;
  registeredName: string | null;
  timestamp: number;
  links: PID[];
}

export type OnExitHandler = (report: ExitReport) => void;

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
  recvTimer?: ReturnType<typeof setTimeout>;
  // --- fault-isolation limits (0 = unlimited / disabled) ---
  // Max messages processed before yielding to the event loop.
  messageBudget: number;
  // Max mailbox items; overflow triggers a SYSTEM alert and drops messages.
  maxMailboxSize: number;
  // Per handle_call/handle_cast timeout in milliseconds.
  execTimeout: number;
  // Max RSS memory in bytes; checked at yield points.
  maxMemory: number;
  // Running counter incremented every processed message; reset on yield.
  messageCount: number;
  // How many execution timeouts this process has accumulated.
  execTimeoutCount: number;
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
  onExit: OnExitHandler | null = null;

  // System-wide defaults for process resource limits.
  // Individual processes override these via SpawnOptions.limits or Process.flag().
  // 0 = unlimited / disabled.
  defaultMessageBudget = 100;   // yield every 100 messages
  defaultMaxMailboxSize = 0;    // unlimited (backward-compatible)
  defaultExecTimeout = 0;       // no timeout (backward-compatible)
  defaultMaxMemory = 0;         // no limit (backward-compatible)

  private static _default: ActorSystem | null = null;

  // Whether process.memoryUsage() is available (Node, Bun, Deno).
  // False in browsers where process is undefined.
  private static _memApi: boolean | null = null;
  static get hasMemoryAPI(): boolean {
    if (ActorSystem._memApi === null) {
      try {
        ActorSystem._memApi =
          typeof process !== 'undefined' &&
          typeof (process as unknown as Record<string, unknown>).memoryUsage === 'function';
      } catch {
        ActorSystem._memApi = false;
      }
    }
    return ActorSystem._memApi;
  }

  // Calls process.memoryUsage() if available; returns null on web targets.
  static getMemoryUsage(): { rss: number; heapTotal: number; heapUsed: number; external: number } | null {
    if (!ActorSystem.hasMemoryAPI) return null;
    try {
      return (process as unknown as { memoryUsage(): { rss: number; heapTotal: number; heapUsed: number; external: number } }).memoryUsage();
    } catch {
      return null;
    }
  }

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
      messageBudget: this.defaultMessageBudget,
      maxMailboxSize: this.defaultMaxMailboxSize,
      execTimeout: this.defaultExecTimeout,
      maxMemory: this.defaultMaxMemory,
      messageCount: 0,
      execTimeoutCount: 0,
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

    // Mailbox overflow guard — skip for SYSTEM messages to avoid recursion.
    const isSystem = msg != null && typeof msg === 'object' && (msg as Record<string, unknown>).type === 'SYSTEM';
    if (!isSystem && proc.maxMailboxSize > 0 && proc.mailbox.length >= proc.maxMailboxSize) {
      const overflowMsg = {
        type: 'SYSTEM',
        subtype: 'mailbox_overflow',
        size: proc.mailbox.length,
        limit: proc.maxMailboxSize,
        droppedType: typeof msg,
      };
      // Wake a waiting receiver directly; otherwise push (one-time overage).
      if (proc.recvResolve) {
        if (proc.recvTimer) {
          clearTimeout(proc.recvTimer);
          proc.recvTimer = undefined;
        }
        const resolve = proc.recvResolve;
        proc.recvResolve = null;
        resolve(overflowMsg);
      } else {
        proc.mailbox.push(overflowMsg);
      }
      return;
    }

    if (proc.recvTimer) {
      clearTimeout(proc.recvTimer);
      proc.recvTimer = undefined;
    }

    if (proc.recvResolve) {
      const resolve = proc.recvResolve;
      proc.recvResolve = null;
      resolve(msg);
    } else {
      proc.mailbox.push(msg);
    }
  }

  receiveMessage(pid?: PID, timeout?: number): Promise<unknown> {
    const effectivePid = pid ?? this.getCurrentPid();
    if (!effectivePid) return Promise.reject(new Error('not inside a process'));

    const proc = this.processes.get(effectivePid);
    if (!proc) return Promise.reject(new Error('process not found'));

    if (proc.mailbox.length > 0) {
      return Promise.resolve(proc.mailbox.shift());
    }

    return new Promise((resolve, reject) => {
      proc.recvResolve = resolve;
      if (timeout != null) {
        proc.recvTimer = setTimeout(() => {
          proc.recvResolve = null;
          proc.recvTimer = undefined;
          reject(new TimeoutError('receive timed out'));
        }, timeout);
      }
    });
  }

  getMailboxLength(pid: PID): number {
    const proc = this.processes.get(pid);
    return proc ? proc.mailbox.length + (proc.recvResolve ? 0 : 0) : 0;
  }

  // ---- Exit handling -----------------------------------------------------

  handleExit(proc: ProcessState): void {
    proc.status = 'exited';

    const report: ExitReport = {
      pid: proc.pid,
      reason: proc.exitReason,
      registeredName: proc.registeredName,
      timestamp: Date.now(),
      links: Array.from(proc.links),
    };

    if (proc.exitReason !== 'normal' && proc.exitReason !== 'shutdown') {
      console.error(`[actojs] Process ${proc.pid}${proc.registeredName ? ` (${proc.registeredName})` : ''} exited: ${String(proc.exitReason)}`);
    }

    if (this.onExit) {
      try { this.onExit(report); } catch (_) {}
    }

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
      maxMailboxSize: proc.maxMailboxSize,
      messageBudget: proc.messageBudget,
      messageCount: proc.messageCount,
      execTimeout: proc.execTimeout,
      execTimeoutCount: proc.execTimeoutCount,
      maxMemory: proc.maxMemory,
      links: Array.from(proc.links),
      monitors: Array.from(proc.monitors.entries()).map(([ref, p]) => ({ ref, pid: p })),
      monitoredBy: Array.from(proc.monitoredBy.entries()).map(([p, refs]) => ({ pid: p, ref: refs })),
      trapExit: proc.trapExit,
      registeredName: proc.registeredName,
    };
  }

  // ---- PID context tracking ----------------------------------------------

  // Yield to the event loop when the message budget is exhausted,
  // then check the process memory usage against its limit.
  // Called by the GenServer receive loop after each message.
  async yieldIfNeeded(pid: PID): Promise<void> {
    const proc = this.processes.get(pid);
    if (!proc) return;

    if (proc.messageBudget > 0) {
      proc.messageCount++;
      if (proc.messageCount < proc.messageBudget) return;
      proc.messageCount = 0;
    } else {
      return; // unlimited budget, never force-yield
    }

    // Yield to the event loop so other actors get a turn.
    await new Promise<void>(resolve => setTimeout(resolve, 0));

    // Memory budget check at every yield point.
    if (proc.maxMemory > 0) {
      const mem = ActorSystem.getMemoryUsage();
      if (mem && mem.rss > proc.maxMemory) {
        this.deliverMessage(pid, {
          type: 'SYSTEM',
          subtype: 'memory_limit',
          usage: mem.rss,
          limit: proc.maxMemory,
        });
      }
    }
  }

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
