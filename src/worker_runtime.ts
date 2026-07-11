// actojs — WorkerRuntime: true-parallelism backend for Browsers, Bun, and Deno.
// Each spawn() creates a real Worker thread. The main ActorSystem routes
// messages across thread boundaries via postMessage / structured clone.
//
// IMPORTANT: Functions serialized into workers run in an isolated scope.
// They CANNOT capture closure variables from their spawning context.
// Use Wr.* stubs (send/receive/register/etc.) for cross-thread communication.
// Spawn args can carry serializable data: spawn((Wr, arg1, arg2) => {...}, opts, [arg1, arg2]).

import type { PID, Ref, Dest, SpawnOpt, SpawnOptions, ProcessInfo } from './types';
import { ActorSystem, type ProcessState } from './system';

// ---- Environment detection ------------------------------------------------

let _env: 'bun' | 'deno' | 'browser' | 'unknown' | null = null;
export function detectEnv(): 'bun' | 'deno' | 'browser' | 'unknown' {
  if (_env !== null) return _env;
  try {
    if (typeof (globalThis as any).Bun !== 'undefined') {
      _env = 'bun';
    } else if (typeof (globalThis as any).Deno !== 'undefined') {
      _env = 'deno';
    } else if (typeof Worker !== 'undefined') {
      _env = 'browser';
    } else {
      _env = 'unknown';
    }
  } catch {
    _env = 'unknown';
  }
  return _env;
}

// ---- Worker shell code (runs inside each Worker) --------------------------
//
// The shell provides globalThis.Wr stubs that bridge to the main thread via postMessage.
// Process primitives: self, send, receive, sleep, link, monitor, register, etc.

function workerShellSrc(): string {
  return `
let _pid = null;
let _mailbox = [];
let _recvResolve = null;
let _callId = 0;
const _pendingCalls = new Map();

function _postSys(op, args) {
  const cid = _callId++;
  return new Promise((resolve, reject) => {
    _pendingCalls.set(cid, { resolve, reject });
    postMessage({ __wr: 'sys_call', callId: cid, op, args });
  });
}

globalThis.Wr = {
  self() { if (!_pid) throw new Error('self() called before worker init'); return _pid; },
  pid() { return _pid; },
  send(dest, msg) { postMessage({ __wr: 'send', target: String(dest), msg }); },
  receive(timeout) {
    if (_mailbox.length > 0) return Promise.resolve(_mailbox.shift());
    return new Promise((resolve, reject) => {
      _recvResolve = resolve;
      if (timeout != null) setTimeout(() => { if (_recvResolve === resolve) { _recvResolve = null; reject(new Error('receive timed out')); } }, timeout);
    });
  },
  sleep(ms) { return new Promise(r => setTimeout(r, ms)); },
  exit(pid, reason) { postMessage({ __wr: 'exit', pid: String(pid), reason }); },
  register(name) { return _postSys('register', [name]); },
  unregister(name) { return _postSys('unregister', [name]); },
  whereis(name) { return _postSys('whereis', [name]); },
  alive(pid) { return _postSys('alive', [pid]); },
  flag(flag, value) { return _postSys('flag', [flag, value]); },
  link(pid) { return _postSys('link', [pid]); },
  unlink(pid) { return _postSys('unlink', [pid]); },
  async monitor(target) { const key = await _postSys('monitor', [target]); return Symbol.for(key); },
  demonitor(ref) { return _postSys('demonitor', [Symbol.keyFor(ref) || '']); },
  send_after(dest, msg, ms) {
    globalThis.__wrTimerId = (globalThis.__wrTimerId || 0) + 1;
    const key = 'timer_' + globalThis.__wrTimerId;
    _postSys('send_after', [String(dest), msg, ms, key]);
    return Symbol.for(key);
  },
  cancel_timer(ref) { return _postSys('cancel_timer', [Symbol.keyFor(ref) || '']); },
  info(pid) { return _postSys('info', [pid]); },
  list() { return _postSys('list', []); },
  get(key) { return _postSys('get', [key]); },
  put(key, value) { return _postSys('put', [key, value]); },
  deleteKey(key) { return _postSys('delete', [key]); },
};

self.onmessage = function(e) {
  const d = e.data;
  if (!d || typeof d.__wr !== 'string') return;
  switch (d.__wr) {
    case 'init':
      _pid = d.pid;
      postMessage({ __wr: 'ready', pid: _pid });
      break;
    case 'msg':
      if (_recvResolve) { const r = _recvResolve; _recvResolve = null; r(d.msg); }
      else _mailbox.push(d.msg);
      break;
    case 'sys_reply': {
      const p = _pendingCalls.get(d.callId);
      if (p) { _pendingCalls.delete(d.callId); if (d.error) p.reject(d.result); else p.resolve(d.result); }
      break;
    }
    case 'run': {
      const code = d.code;
      const args = d.args || [];
      const fn = (0, eval)('(' + code + ')');
      Promise.resolve(fn(globalThis.Wr, ...args)).then(
        () => postMessage({ __wr: 'exit', pid: _pid, reason: 'normal' }),
        (err) => postMessage({ __wr: 'exit', pid: _pid, reason: (err && typeof err === 'object' && 'message' in err) ? err.message : String(err) }),
      );
      break;
    }
  }
};
`;
}

// ---- Worker creation per environment --------------------------------------

let _sharedBlobUrl: string | null = null;
let _blobRefCount = 0;

function buildWorkerUrl(): string | URL {
  if (detectEnv() === 'bun' || detectEnv() === 'deno') {
    const src = workerShellSrc();
    return new URL(`data:text/javascript;base64,${btoa(src)}`);
  }
  if (!_sharedBlobUrl) {
    const blob = new Blob([workerShellSrc()], { type: 'application/javascript' });
    _sharedBlobUrl = URL.createObjectURL(blob);
  }
  _blobRefCount++;
  return _sharedBlobUrl;
}

function releaseBlobRef(): void {
  if (--_blobRefCount <= 0) {
    _blobRefCount = 0;
    if (_sharedBlobUrl) {
      URL.revokeObjectURL(_sharedBlobUrl);
      _sharedBlobUrl = null;
    }
  }
}

function createWorker(): Worker {
  const url = buildWorkerUrl();
  if (detectEnv() === 'deno') {
    return new Worker(url, { type: 'module' } as any);
  }
  return new Worker(url as string);
}

// ---- WorkerRuntime class --------------------------------------------------

export class WorkerRuntime {
  readonly name: string;
  readonly available: boolean;

  private workers: Map<PID, Worker> = new Map();

  constructor() {
    const env = detectEnv();
    if (env === 'bun') this.name = 'bun-worker';
    else if (env === 'deno') this.name = 'deno-worker';
    else if (env === 'browser') this.name = 'web-worker';
    else this.name = 'worker';
    this.available = env !== 'unknown' && typeof Worker !== 'undefined';
  }

  // ---- Message routing ----------------------------------------------------

  deliver(pid: PID, msg: unknown): void {
    const sys = ActorSystem.current;
    const proc = sys.getProcess(pid);
    if (proc && (proc.status === 'exited' || proc.status === 'exiting')) return;
    const w = this.workers.get(pid);
    if (w) {
      w.postMessage({ __wr: 'msg', msg });
    }
  }

  // ---- System calls (main-thread side) ------------------------------------

  private handleSysCall(sys: ActorSystem, pid: PID, op: string, args: unknown[]): unknown {
    switch (op) {
      case 'register': {
        const name = args[0] as string;
        sys.registerName(name, pid);
        const proc = sys.getProcess(pid);
        if (proc) proc.registeredName = name;
        return;
      }
      case 'unregister': {
        const name = args[0] as string;
        sys.unregisterName(name);
        const proc = sys.getProcess(pid);
        if (proc && proc.registeredName === name) proc.registeredName = null;
        return;
      }
      case 'whereis':
        return sys.whereisName(args[0] as string);
      case 'alive': {
        const p = sys.getProcess(args[0] as PID);
        return p != null && p.status !== 'exited' && p.status !== 'exiting';
      }
      case 'flag': {
        const proc = sys.getProcess(pid);
        if (!proc) return false;
        const flag = args[0] as string;
        const val = args[1];
        switch (flag) {
          case 'trap_exit': { const p = proc.trapExit; proc.trapExit = !!val; return p; }
          case 'message_budget': { const p = proc.messageBudget; proc.messageBudget = Number(val) || 0; return p; }
          case 'max_mailbox_size': { const p = proc.maxMailboxSize; proc.maxMailboxSize = Number(val) || 0; return p; }
          case 'exec_timeout': { const p = proc.execTimeout; proc.execTimeout = Number(val) || 0; return p; }
          case 'max_memory': { const p = proc.maxMemory; proc.maxMemory = Number(val) || 0; return p; }
          default: return false;
        }
      }
      case 'link': {
        const t = args[0] as PID;
        const c = sys.getProcess(pid);
        const o = sys.getProcess(t);
        if (c && o) { c.links.add(t); o.links.add(pid); }
        return;
      }
      case 'unlink': {
        const t = args[0] as PID;
        const c = sys.getProcess(pid);
        const o = sys.getProcess(t);
        if (c) c.links.delete(t);
        if (o) o.links.delete(pid);
        return;
      }
      case 'monitor': {
        const target = args[0] as PID;
        const ref: Ref = Symbol('monitor');
        const c = sys.getProcess(pid);
        const o = sys.getProcess(target);
        if (c) {
          c.monitors.set(ref, target);
          if (o) {
            const refs = o.monitoredBy.get(pid) ?? [];
            refs.push(ref);
            o.monitoredBy.set(pid, refs);
          }
        }
        return ref.description;
      }
      case 'demonitor': {
        const refKey = args[0] as string;
        const c = sys.getProcess(pid);
        if (!c) return;
        c.monitors.forEach((monitoredPid, ref) => {
          if (ref.description === refKey) {
            c.monitors.delete(ref);
            const o = sys.getProcess(monitoredPid);
            if (o) {
              const refs = (o.monitoredBy.get(pid) ?? []).filter(r => r !== ref);
              if (refs.length > 0) o.monitoredBy.set(pid, refs);
              else o.monitoredBy.delete(pid);
            }
            return;
          }
        });
        return;
      }
      case 'send_after': {
        const dest = args[0] as string;
        const pld = args[1];
        const ms = args[2] as number;
        const key = args[3] as string;
        const timerRef: Ref = Symbol.for(key);
        const h = setTimeout(() => {
          sys.timers.delete(timerRef);
          const target = dest.startsWith('#PID<') ? dest : sys.whereisName(dest);
          if (target) this.deliver(target, pld);
        }, ms);
        sys.timers.set(timerRef, h);
        return key;
      }
      case 'cancel_timer': {
        const key = args[0] as string;
        const ref = Symbol.for(key);
        const h = sys.timers.get(ref);
        if (h) { clearTimeout(h); sys.timers.delete(ref); }
        return;
      }
      case 'info': {
        const target = args[0] as PID;
        const proc = sys.getProcess(target);
        if (!proc) return null;
        return {
          status: proc.status,
          messageQueueLength: proc.mailbox.length - proc.mailboxHead,
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
        } satisfies ProcessInfo;
      }
      case 'list':
        return Array.from(sys.processes.keys()).filter(p => {
          const proc = sys.getProcess(p);
          return proc && proc.status !== 'exited' && proc.status !== 'exiting';
        });
      case 'get':
        return sys.getProcess(pid)?.processDict.get(args[0] as string);
      case 'put': {
        const proc = sys.getProcess(pid);
        if (!proc) return;
        const key = args[0] as string;
        const prev = proc.processDict.get(key);
        proc.processDict.set(key, args[1]);
        return prev;
      }
      case 'delete': {
        const proc = sys.getProcess(pid);
        if (!proc) return;
        const key = args[0] as string;
        const prev = proc.processDict.get(key);
        proc.processDict.delete(key);
        return prev;
      }
      default:
        return;
    }
  }

  // ---- Spawn (public, used by tests) --------------------------------------

  /**
   * Spawn a process in a new Worker thread.
   * fn receives (Wr, ...args) where Wr is the worker-side process-stub object
   * and args are optional serializable values.
   */
  spawn(
    fn: (Wr: any, ...args: any[]) => void | Promise<void>,
    opts?: SpawnOpt[] | SpawnOptions,
    args: any[] = [],
  ): PID {
    const { pid, ref } = this._spawnImpl(fn, opts, args);
    if (ref != null) {
      const sys = ActorSystem.current;
      const proc = sys.getProcess(pid);
      if (proc) proc.processDict.set('__monitor_ref', ref);
    }
    return pid;
  }

  // ---- spawnProcess (Runtime interface method) ----------------------------

  /**
   * Implementation of Runtime.spawnProcess.
   * Called transparently by Process.spawn() when this runtime is active.
   */
  spawnProcess(fn: () => void | Promise<void>, opts?: SpawnOpt[] | SpawnOptions): PID {
    const fnCode = fn.toString();
    const { pid, ref } = this._spawnImpl((Wr: any, __code: any) => (0, eval)('(' + (__code as string) + ')')() as void, opts, [fnCode]);
    if (ref != null) {
      const sys = ActorSystem.current;
      const proc = sys.getProcess(pid);
      if (proc) proc.processDict.set('__monitor_ref', ref);
    }
    return pid;
  }

  // ---- Internal spawn implementation --------------------------------------

  private _spawnImpl(
    fn: (...args: any[]) => void | Promise<void>,
    opts: SpawnOpt[] | SpawnOptions | undefined,
    args: any[] = [],
  ): { pid: PID; ref?: Ref } {
    const sys = ActorSystem.current;
    const pid = sys.generatePid();
    const proc = sys.createProcess(pid);
    sys.registerProcess(pid, proc);

    let link = false;
    let monitor = false;
    if (Array.isArray(opts)) {
      link = opts.includes('link');
      monitor = opts.includes('monitor');
    } else if (opts && typeof opts === 'object') {
      link = opts.link ?? false;
      monitor = opts.monitor ?? false;
      const limits = opts.limits;
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
    let monitorRef: Ref | undefined;
    if (monitor && caller) {
      monitorRef = Symbol('monitor');
      const callerProc = sys.getProcess(caller);
      if (callerProc) {
        proc.monitoredBy.set(caller, [monitorRef]);
        callerProc.monitors.set(monitorRef, pid);
      }
    }

    const worker = createWorker();
    this.workers.set(pid, worker);

    const fnStr = fn.toString();

    const finish = (reason: unknown) => {
      if (proc.status === 'running') {
        proc.status = 'exiting';
        proc.exitReason = proc.exitReason ?? reason ?? 'normal';
      }
      sys.handleExit(proc);
      worker.terminate();
      this.workers.delete(pid);
      releaseBlobRef();
    };

    worker.onmessage = (e: MessageEvent) => {
      const d = e.data;
      if (!d || typeof d.__wr !== 'string') return;
      switch (d.__wr) {
        case 'ready':
          worker.postMessage({ __wr: 'run', code: fnStr, args });
          break;
        case 'send':
          {
            let w = this.workers.get(d.target);
            if (!w) {
              const resolved = sys.whereisName(d.target);
              if (resolved) w = this.workers.get(resolved);
            }
            if (w) {
              w.postMessage({ __wr: 'msg', msg: d.msg });
            } else {
              sys.deliverMessage(d.target, d.msg);
            }
          }
          break;
        case 'exit':
          finish(d.reason);
          break;
        case 'sys_call': {
          try {
            const result = this.handleSysCall(sys, pid, d.op, d.args);
            Promise.resolve(result).then(
              (val) => worker.postMessage({ __wr: 'sys_reply', callId: d.callId, result: val }),
              (err) => worker.postMessage({ __wr: 'sys_reply', callId: d.callId, result: String(err), error: true }),
            );
          } catch (err) {
            worker.postMessage({ __wr: 'sys_reply', callId: d.callId, result: String(err), error: true });
          }
          break;
        }
      }
    };

    worker.onerror = (evt: ErrorEvent) => {
      proc.status = 'exiting';
      proc.exitReason = evt.error ?? evt.message ?? 'worker error';
      finish(proc.exitReason);
    };

    worker.onmessageerror = () => {
      proc.status = 'exiting';
      proc.exitReason = proc.exitReason ?? 'message deserialization error';
      finish(proc.exitReason);
    };

    worker.postMessage({ __wr: 'init', pid });
    return { pid, ref: monitorRef };
  }

  // ---- Stop ----------------------------------------------------------------

  stop(): void {
    const sys = ActorSystem.current;
    const entries = Array.from(this.workers);
    for (const [pid] of entries) {
      const proc = sys.getProcess(pid);
      if (proc && proc.status === 'running') {
        proc.status = 'exiting';
        proc.exitReason = 'shutdown';
      }
    }
    for (const [pid, worker] of entries) {
      const proc = sys.getProcess(pid);
      if (proc && proc.status === 'exiting') {
        sys.handleExit(proc);
      }
      worker.terminate();
      releaseBlobRef();
    }
    this.workers.clear();
  }
}
