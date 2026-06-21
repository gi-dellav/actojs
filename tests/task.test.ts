import { describe, test, expect, beforeEach } from 'bun:test';
import * as Task from '../src/task';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep } from './helpers';

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('task', () => {
  describe('async', () => {
    test('returns a TaskHandle with pid and ref', () => {
      const handle = Task.async(async () => 42);
      expect(handle.pid).toMatch(/^#PID</);
      expect(typeof handle.ref).toBe('symbol');
    });
  });

  describe('await_', () => {
    test('awaits the result of a task', async () => {
      const handle = Task.async(async () => 42);
      const result = await Task.await_(handle);
      expect(result).toBe(42);
    });

    test('rejects if task throws', async () => {
      const handle = Task.async(async () => {
        throw new Error('task error');
      });
      await expect(Task.await_(handle)).rejects.toThrow('task error');
    });

    test('rejects if task not found', async () => {
      await expect(Task.await_({ pid: '#PID<999.0.0>', ref: Symbol('x') })).rejects.toThrow('task not found');
    });
  });

  describe('yield_', () => {
    test('returns null while task is pending', async () => {
      const handle = Task.async(async () => {
        await sleep(50);
        return 42;
      });
      const immediate = await Task.yield_(handle);
      expect(immediate).toBeNull();
    });

    test('returns value when task is done', async () => {
      const handle = Task.async(async () => 77);
      await sleep(20);
      const result = await Task.yield_(handle);
      expect(result).toBe(77);
    });

    test('returns null for unknown task', async () => {
      const result = await Task.yield_({ pid: '#PID<999.0.0>', ref: Symbol('x') });
      expect(result).toBeNull();
    });

    test('returns null for errored task', async () => {
      const handle = Task.async(async () => {
        throw new Error('fail');
      });
      await sleep(20);
      const result = await Task.yield_(handle);
      expect(result).toBeNull();
    });
  });

  describe('shutdown', () => {
    test('kills the task process and cleans up', async () => {
      const handle = Task.async(async () => {
        await sleep(500);
        return 42;
      });
      expect(Process.alive(handle.pid)).toBe(true);
      await Task.shutdown(handle);
      expect(Process.alive(handle.pid)).toBe(false);
    });

    test('await on shutdown task rejects', async () => {
      const handle = Task.async(async () => {
        await sleep(200);
        return 1;
      });
      await Task.shutdown(handle);
      await expect(Task.await_(handle)).rejects.toThrow('task not found');
    });
  });
});
