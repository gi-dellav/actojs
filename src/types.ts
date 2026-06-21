// actojs — Core types
// Web runtime: cooperative event-loop scheduler.

export type PID = string;
export type Ref = symbol;
export type Dest = PID | string; // PID or registered name
export type SpawnOpt = 'link' | 'monitor';

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
