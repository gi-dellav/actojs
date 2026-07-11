// actojs — Mailbox: per-process message queue with blocking receive.
// Delegates to ActorSystem.current for all state.
// Web runtime: promise-based cooperative multiplexing.

import type { PID, ProcessInfo } from "./types";
import { ActorSystem, type ProcessState } from "./system";

export type { ProcessState } from "./system";

/** Generate a new unique process identifier. */
export function generatePid(): PID {
  return ActorSystem.current.generatePid();
}

/** Create a new process state entry for the given PID. */
export function createProcess(pid: PID): ProcessState {
  return ActorSystem.current.createProcess(pid);
}

/** Retrieve a process state by PID, or undefined if not found. */
export function getProcess(pid: PID): ProcessState | undefined {
  return ActorSystem.current.getProcess(pid);
}

/** Return the PID of the currently executing process, or null if none. */
export function getCurrentPid(): PID | null {
  return ActorSystem.current.getCurrentPid();
}

/** Push a PID onto the implicit call stack for self() context. */
export function pushPid(pid: PID): void {
  ActorSystem.current.pushPid(pid);
}

/** Pop a PID from the implicit call stack. */
export function popPid(): void {
  ActorSystem.current.popPid();
}

/** Clear the entire PID call stack. */
export function clearPidStack(): void {
  ActorSystem.current.clearPidStack();
}

/** Register a process state in the process table. */
export function registerProcess(pid: PID, proc: ProcessState): void {
  ActorSystem.current.registerProcess(pid, proc);
}

/** Remove a process from the process table and name registry. */
export function deregisterProcess(pid: PID): void {
  ActorSystem.current.deregisterProcess(pid);
}

/** Return all currently registered PIDs. */
export function allPids(): PID[] {
  return ActorSystem.current.allPids();
}

/** Register a name-to-PID mapping for named process lookup. */
export function registerName(name: string, pid: PID): void {
  ActorSystem.current.registerName(name, pid);
}

/** Remove a name-to-PID mapping. */
export function unregisterName(name: string): void {
  ActorSystem.current.unregisterName(name);
}

/** Look up a PID by registered name, or null if not found. */
export function whereisName(name: string): PID | null {
  return ActorSystem.current.whereisName(name);
}

/** Deliver a message to a process's mailbox or resolve a pending receive. */
export function deliverMessage(pid: PID, msg: unknown): void {
  ActorSystem.current.deliverMessage(pid, msg);
}

/** Block until a message arrives or timeout expires. */
export function receiveMessage(pid?: PID, timeout?: number): Promise<unknown> {
  return ActorSystem.current.receiveMessage(pid, timeout);
}

/** Return the number of pending messages in a process mailbox. */
export function getMailboxLength(pid: PID): number {
  return ActorSystem.current.getMailboxLength(pid);
}

/** Dequeue the oldest message from a process mailbox. */
export function shiftMessage(pid: PID): unknown | undefined {
  return ActorSystem.current.shiftMessage(pid);
}

/** Check whether a process mailbox has pending messages. */
export function hasMessages(pid: PID): boolean {
  return ActorSystem.current.hasMessages(pid);
}

/** Increment the message counter for budget tracking; returns true when exhausted. */
export function countMessage(pid: PID): boolean {
  return ActorSystem.current.countMessage(pid);
}

/** Yield to the event loop and check process memory usage. */
export function doYield(pid: PID): Promise<void> {
  return ActorSystem.current.doYield(pid);
}

/** Perform the full exit protocol: notify links and monitors, then deregister. */
export function handleExit(proc: ProcessState): void {
  ActorSystem.current.handleExit(proc);
}

/** Build a snapshot of process information for inspection. */
export function getProcessInfo(pid: PID): ProcessInfo | null {
  return ActorSystem.current.getProcessInfo(pid);
}

/** Yield to the event loop if the message budget is exhausted, then check memory. */
export function yieldIfNeeded(pid: PID): Promise<void> {
  return ActorSystem.current.yieldIfNeeded(pid);
}

/** Execute a function with a PID on the call stack, restoring it afterward. */
export function runWithPid<T>(pid: PID, fn: () => T): T {
  return ActorSystem.current.runWithPid(pid, fn);
}
