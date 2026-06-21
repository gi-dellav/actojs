import { describe, test, expect, beforeEach } from 'bun:test';
import * as Agent from '../src/agent';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep } from './helpers';

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('agent', () => {
  describe('start', () => {
    test('starts an agent', async () => {
      const result = await Agent.start(() => 0);
      expect('ok' in result).toBe(true);
      if ('ok' in result) {
        expect(Process.alive(result.ok)).toBe(true);
      }
    });

    test('handles init throwing', async () => {
      const result = await Agent.start(() => {
        throw new Error('bad init');
      });
      expect('error' in result).toBe(true);
    });

    test('with name option', async () => {
      const result = await Agent.start(() => 42, { name: 'my_agent' });
      expect('ok' in result).toBe(true);
      expect(Process.whereis('my_agent')).toBe((result as any).ok);
    });
  });

  describe('start_link', () => {
    test('starts a linked agent', async () => {
      const callerPid = 'ag_caller';
      const procCaller = M.createProcess(callerPid);
      procCaller.status = 'alive';
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const result = await Agent.start_link(() => 0);
      M.popPid();

      expect('ok' in result).toBe(true);
      if ('ok' in result) {
        const spawned = M.getProcess(result.ok);
        expect(spawned!.links.has(callerPid)).toBe(true);
      }
    });
  });

  describe('get', () => {
    test('gets the agent state', async () => {
      const result = await Agent.start(() => 99);
      if ('error' in result) throw result.error;
      const value = await Agent.get(result.ok, (s: number) => s);
      expect(value).toBe(99);
    });

    test('applies function to state', async () => {
      const result = await Agent.start(() => ({ a: 1, b: 2 }));
      if ('error' in result) throw result.error;
      const a = await Agent.get(result.ok, (s: any) => s.a);
      expect(a).toBe(1);
    });

    test('supports [module, fn, args] style', async () => {
      const mod = {
        getA(state: any, offset: number) {
          return state.a + offset;
        },
      };
      const result = await Agent.start(() => ({ a: 10 }));
      if ('error' in result) throw result.error;
      const val = await Agent.get(result.ok, [mod, 'getA', [5]]);
      expect(val).toBe(15);
    });
  });

  describe('update', () => {
    test('updates the agent state', async () => {
      const result = await Agent.start(() => 0);
      if ('error' in result) throw result.error;
      await Agent.update(result.ok, (s: number) => s + 1);
      const value = await Agent.get(result.ok, (s: number) => s);
      expect(value).toBe(1);
    });

    test('supports [module, fn, args] style', async () => {
      const mod = {
        add(state: number, amount: number) {
          return state + amount;
        },
      };
      const result = await Agent.start(() => 5);
      if ('error' in result) throw result.error;
      await Agent.update(result.ok, [mod, 'add', [3]]);
      const value = await Agent.get(result.ok, (s: number) => s);
      expect(value).toBe(8);
    });
  });

  describe('get_and_update', () => {
    test('gets and updates atomically', async () => {
      const result = await Agent.start(() => 10);
      if ('error' in result) throw result.error;
      const prev = await Agent.get_and_update(result.ok, (s: number) => [s, s + 1]);
      expect(prev).toBe(10);
      const current = await Agent.get(result.ok, (s: number) => s);
      expect(current).toBe(11);
    });

    test('supports [module, fn, args] style', async () => {
      const mod = {
        inc(state: number) {
          return [state, state + 1] as [number, number];
        },
      };
      const result = await Agent.start(() => 0);
      if ('error' in result) throw result.error;
      const prev = await Agent.get_and_update(result.ok, [mod, 'inc', []]);
      expect(prev).toBe(0);
      const current = await Agent.get(result.ok, (s: number) => s);
      expect(current).toBe(1);
    });
  });

  describe('cast', () => {
    test('sends fire-and-forget update', async () => {
      const result = await Agent.start(() => 0);
      if ('error' in result) throw result.error;
      Agent.cast(result.ok, (s: number) => s + 1);
      await sleep(20);
      const value = await Agent.get(result.ok, (s: number) => s);
      expect(value).toBe(1);
    });
  });

  describe('stop', () => {
    test('stops the agent', async () => {
      const result = await Agent.start(() => 0);
      if ('error' in result) throw result.error;
      await Agent.stop(result.ok);
      expect(Process.alive(result.ok)).toBe(false);
    });
  });
});
