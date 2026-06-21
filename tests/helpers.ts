import * as Process from '../src/process';

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function waitUntil(fn: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  return new Promise((resolve, reject) => {
    const check = () => {
      if (fn()) return resolve();
      if (Date.now() - start > timeoutMs) return reject(new Error('waitUntil timed out'));
      setTimeout(check, 10);
    };
    check();
  });
}

export function waitForProcessExit(pid: string, timeoutMs = 5000): Promise<void> {
  return waitUntil(() => !Process.alive(pid), timeoutMs);
}

export function waitForMessage(ms = 50): Promise<void> {
  return sleep(ms);
}

export function nextTick(): Promise<void> {
  return new Promise(resolve => queueMicrotask(resolve));
}
