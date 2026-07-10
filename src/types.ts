// actojs — Core types
// Web runtime: cooperative event-loop scheduler.

export type PID = string;
export type Ref = symbol;
export type Dest = PID | string; // PID or registered name
export type SpawnOpt = 'link' | 'monitor';

// Per-process resource limits for fault isolation.
// Set at spawn() time or via Process.flag() at runtime.
// All fields default to 0 (unlimited / disabled).
export interface ProcessLimits {
  // Maximum messages to process before yielding to the event loop.
  // A yield point lets other actors run and triggers a memory check.
  // 0 disables the budget — the actor yields only on await points.
  messageBudget?: number;
  // Maximum mailbox items. When exceeded, incoming messages are
  // dropped and a SYSTEM alert is delivered instead.
  // 0 = unlimited (default).
  maxMailboxSize?: number;
  // Per handle_call/handle_cast timeout in milliseconds.
  // When exceeded, the caller gets an error and the process
  // accumulates a timeout counter. 3 timeouts = process exit.
  // 0 = no timeout (default).
  execTimeout?: number;
  // Maximum RSS memory in bytes for this process. Checked at
  // yield points. When exceeded, a SYSTEM alert is delivered.
  // Requires process.memoryUsage() (Node / Bun). Ignored in browsers.
  // 0 = no limit (default).
  maxMemory?: number;
}

// Spawn options object (alternative to SpawnOpt[]).
export interface SpawnOptions {
  link?: boolean;
  monitor?: boolean;
  limits?: ProcessLimits;
}

export type Module = Record<string, unknown>;
export type MFA = [Module, string, unknown[]]; // [module, functionName, args]

export interface DownMessage {
  type: 'DOWN';
  ref: Ref;
  pid: PID;
  reason: unknown;
}

export interface ProcessInfo {
  status: 'running' | 'alive' | 'exiting' | 'exited';
  messageQueueLength: number;
  maxMailboxSize: number;
  messageBudget: number;
  messageCount: number;
  execTimeout: number;
  execTimeoutCount: number;
  maxMemory: number;
  links: PID[];
  monitors: { ref: Ref; pid: PID }[];
  monitoredBy: { pid: PID; ref: Ref[] }[];
  trapExit: boolean;
  registeredName: string | null;
}

export type OnStart = { ok: PID } | { error: Error };
export type OnStartChild = { ok: PID } | { error: Error };

export type Strategy = 'one_for_one' | 'one_for_all' | 'rest_for_one';

export interface ChildSpec {
  id: string;
  start: MFA; // [module, functionName, args]
  restart?: 'permanent' | 'transient' | 'temporary';
  shutdown?: number | 'brutal_kill' | 'infinity';
  type?: 'worker' | 'supervisor';
  significant?: boolean;
}

export interface ChildInfo {
  id: string | undefined;
  pid: PID;
  type: 'worker' | 'supervisor';
  modules: Module[];
}

export interface Counts {
  specs: number;
  active: number;
  supervisors: number;
  workers: number;
}

export interface TaskHandle<R> {
  pid: PID;
  ref: Ref;
}

export type RegistryKeyMode = 'unique' | 'duplicate' | { duplicate: 'key' } | { duplicate: 'pid' };

export interface RegistryStartOptions {
  keys: RegistryKeyMode;
  name?: string;
  partitions?: number;
  listeners?: string[];
  meta?: Record<string, unknown>;
}

export interface SupervisorStartOptions {
  strategy: Strategy;
  name?: string;
  max_restarts?: number;
  max_seconds?: number;
}

export interface SupervisorInitOptions extends SupervisorStartOptions {
  max_children?: number;
  extra_arguments?: unknown[];
}

export interface SupervisorSpec {
  children: ChildSpec[];
  strategy: Strategy;
  max_restarts: number;
  max_seconds: number;
  max_children: number;
  extra_arguments: unknown[];
}

export interface NodeStartOpts {
  name_domain?: 'shortnames' | 'longnames';
}

export type NodeState = 'visible' | 'hidden' | 'connected' | 'this' | 'known';
