// actojs — Mailbox: per-process message queue with blocking receive.
// Delegates to ActorSystem.current for all state.
// Web runtime: promise-based cooperative multiplexing.

import type { PID, ProcessInfo } from './types';
import { ActorSystem, type ProcessState } from './system';

export type { ProcessState } from './system';

export function generatePid(): PID {
  return ActorSystem.current.generatePid();
}

export function createProcess(pid: PID): ProcessState {
  return ActorSystem.current.createProcess(pid);
}

export function getProcess(pid: PID): ProcessState | undefined {
  return ActorSystem.current.getProcess(pid);
}

export function getCurrentPid(): PID | null {
  return ActorSystem.current.getCurrentPid();
}

export function pushPid(pid: PID): void {
  ActorSystem.current.pushPid(pid);
}

export function popPid(): void {
  ActorSystem.current.popPid();
}

export function clearPidStack(): void {
  ActorSystem.current.clearPidStack();
}

export function registerProcess(pid: PID, proc: ProcessState): void {
  ActorSystem.current.registerProcess(pid, proc);
}

export function deregisterProcess(pid: PID): void {
  ActorSystem.current.deregisterProcess(pid);
}

export function allPids(): PID[] {
  return ActorSystem.current.allPids();
}

export function registerName(name: string, pid: PID): void {
  ActorSystem.current.registerName(name, pid);
}

export function unregisterName(name: string): void {
  ActorSystem.current.unregisterName(name);
}

export function whereisName(name: string): PID | null {
  return ActorSystem.current.whereisName(name);
}

export function deliverMessage(pid: PID, msg: unknown): void {
  ActorSystem.current.deliverMessage(pid, msg);
}

export function receiveMessage(pid?: PID, timeout?: number): Promise<unknown> {
  return ActorSystem.current.receiveMessage(pid, timeout);
}

export function getMailboxLength(pid: PID): number {
  return ActorSystem.current.getMailboxLength(pid);
}

export function handleExit(proc: ProcessState): void {
  ActorSystem.current.handleExit(proc);
}

export function getProcessInfo(pid: PID): ProcessInfo | null {
  return ActorSystem.current.getProcessInfo(pid);
}

export function runWithPid<T>(pid: PID, fn: () => T): T {
  return ActorSystem.current.runWithPid(pid, fn);
}
