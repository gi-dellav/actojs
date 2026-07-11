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

  describe('start / start_link', () => {
    test('Task.start runs fire-and-forget', async () => {
      let ran = false;
      const result = Task.start(async () => { ran = true; });
      expect('ok' in result).toBe(true);
      await sleep(20);
      expect(ran).toBe(true);
    });

    test('Task.start_link links the task to the caller', async () => {
      const callerPid = 'task_link_caller';
      const procCaller = M.createProcess(callerPid);
      procCaller.status = 'alive';
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const result = Task.start_link(async () => { await sleep(500); });
      expect('ok' in result).toBe(true);

      M.popPid();
      if ('ok' in result) {
        const spawnedProc = M.getProcess(result.ok);
        expect(spawnedProc!.links.has(callerPid)).toBe(true);
      }
    });
  });

  describe('await_many', () => {
    test('awaits all tasks and returns results in order', async () => {
      const t1 = Task.async(async () => 1);
      const t2 = Task.async(async () => 2);
      const t3 = Task.async(async () => 3);
      const results = await Task.await_many([t1, t2, t3]);
      expect(results).toEqual([1, 2, 3]);
    });

    test('rejects if any task fails', async () => {
      const t1 = Task.async(async () => 1);
      const t2 = Task.async(async () => { throw new Error('boom'); });
      await expect(Task.await_many([t1, t2])).rejects.toThrow('boom');
    });
  });

  describe('yield_many', () => {
    test('polls multiple tasks', async () => {
      const t1 = Task.async(async () => 42);
      await sleep(20);
      const t2 = Task.async(async () => { await sleep(500); });
      const results = await Task.yield_many([t1, t2]);
      expect(results[0]).toBe(42);
      expect(results[1]).toBeNull();
      await Task.shutdown(t2);
    });
  });
});
