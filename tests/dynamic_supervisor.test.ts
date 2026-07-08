import { describe, test, expect, beforeEach } from 'bun:test';
import * as DynamicSupervisor from '../src/dynamic_supervisor';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep } from './helpers';

function waitFor(fn: () => boolean, ms = 2000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (fn()) return resolve();
      if (Date.now() - start > ms) return reject(new Error('waitFor timed out'));
      setTimeout(check, 10);
    };
    check();
  });
}

function makeWorkerMod() {
  return {
    start_link() {
      const pid = Process.spawn(async () => {
        await Process.receive();
      });
      return { ok: pid };
    },
  };
}

beforeEach(() => {
  M.clearPidStack();
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('dynamic_supervisor', () => {
  describe('start_link', () => {
    test('starts with options', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      expect('ok' in result).toBe(true);
    });

    test('starts with name', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one', name: 'dyn_sup' });
      expect('ok' in result).toBe(true);
      expect(Process.whereis('dyn_sup')).toBe((result as any).ok);
    });

    test('starts with module-based init', async () => {
      const mod = {
        init() {
          return DynamicSupervisor.init({ strategy: 'one_for_one', max_children: 5 });
        },
      };
      const result = await DynamicSupervisor.start_link(mod, null);
      expect('ok' in result).toBe(true);
    });

    test('returns error if module has no init', async () => {
      // Pass an object without 'init' — the start_link checks for 'init' in optsOrModule
      // If it's an object without 'init', it falls through to the else branch (treated as opts)
      // To trigger the error, we need an object WITH 'init' that is not a function
      const mod = { init: 'not_a_function' } as any;
      const result = await DynamicSupervisor.start_link(mod, null);
      // With init as a string, typeof mod.init === 'function' is false,
      // so it goes to 'module must have an init method'
      expect('error' in result).toBe(true);
    });
  });

  describe('start_child', () => {
    test('starts a child dynamically', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const mod = makeWorkerMod();
      const childSpec = {
        id: 'dyn_child',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      const childResult = await DynamicSupervisor.start_child(sup, childSpec);
      expect('ok' in childResult).toBe(true);

      const counts = await DynamicSupervisor.count_children(sup);
      expect(counts.active).toBe(1);
      expect(counts.specs).toBe(1);
    });

    test('returns error for invalid module', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const childSpec = { id: 'bad', start: [null, 'nope', []] as any };
      const childResult = await DynamicSupervisor.start_child(sup, childSpec);
      expect('error' in childResult).toBe(true);
    });

    test('returns error when child start throws', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const mod = {
        start_link() { throw new Error('child crash'); },
      };
      const childSpec = {
        id: 'thrower',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      const childResult = await DynamicSupervisor.start_child(sup, childSpec);
      expect('error' in childResult).toBe(true);
    });
  });

  describe('terminate_child', () => {
    test('terminates a child by PID', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const mod = makeWorkerMod();
      const childSpec = {
        id: 'to_kill',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      const childResult = await DynamicSupervisor.start_child(sup, childSpec);
      if ('error' in childResult) throw childResult.error;
      const childPid = childResult.ok;

      await DynamicSupervisor.terminate_child(sup, childPid);
      await sleep(20);

      expect(Process.alive(childPid)).toBe(false);
      const counts = await DynamicSupervisor.count_children(sup);
      expect(counts.active).toBe(0);
    });

    test('returns error for unknown PID', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const termResult = await DynamicSupervisor.terminate_child(sup, '#PID<999.0.0>');
      expect('error' in (termResult as any)).toBe(true);
    });
  });

  describe('count_children', () => {
    test('returns counts', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const counts = await DynamicSupervisor.count_children(sup);
      expect(counts.specs).toBe(0);
      expect(counts.active).toBe(0);
    });
  });

  describe('which_children', () => {
    test('returns info for alive children', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const mod = makeWorkerMod();
      const childSpec = {
        id: 'd1',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      await DynamicSupervisor.start_child(sup, childSpec);

      const children = await DynamicSupervisor.which_children(sup);
      expect(children.length).toBe(1);
      expect(children[0]!.type).toBe('worker');
      expect(children[0]!.pid).toMatch(/^#PID</);
    });
  });

  describe('stop', () => {
    test('stop kills all children', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      const mod = makeWorkerMod();
      const childSpec = {
        id: 'c1',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      await DynamicSupervisor.start_child(sup, childSpec);
      await DynamicSupervisor.stop(sup);
      await sleep(30);

      const counts = await DynamicSupervisor.count_children(sup);
      expect(counts.active).toBe(0);
    });
  });

  describe('init', () => {
    test('returns a SupervisorSpec with defaults', () => {
      const spec = DynamicSupervisor.init();
      expect(spec.strategy).toBe('one_for_one');
      expect(spec.children).toEqual([]);
      expect(spec.max_restarts).toBe(3);
      expect(spec.max_seconds).toBe(5);
      expect(spec.max_children).toBe(Infinity);
      expect(spec.extra_arguments).toEqual([]);
    });

    test('accepts custom options', () => {
      const spec = DynamicSupervisor.init({ strategy: 'one_for_one', max_children: 10, max_restarts: 5 });
      expect(spec.max_children).toBe(10);
      expect(spec.max_restarts).toBe(5);
    });
  });

  describe('cleanup on child exit', () => {
    test('removes child from tracking on normal exit', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      let childPid = '';
      const mod = {
        start_link() {
          const pid = Process.spawn(() => {
            childPid = Process.self();
          });
          return { ok: pid };
        },
      };

      const childSpec = {
        id: 'ephemeral',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      await DynamicSupervisor.start_child(sup, childSpec);
      await sleep(10);
      expect(childPid).toBeTruthy();
      await waitFor(() => !Process.alive(childPid), 1000);
      await sleep(20);

      const counts = await DynamicSupervisor.count_children(sup);
      expect(counts.active).toBe(0);
    });

    test('restarts child on abnormal exit', async () => {
      const result = await DynamicSupervisor.start_link({ strategy: 'one_for_one' });
      if ('error' in result) throw result.error;
      const sup = result.ok;

      let startCount = 0;
      const mod = {
        start_link() {
          startCount++;
          const pid = Process.spawn(async () => {
            if (startCount === 1) throw new Error('crash');
            await new Promise(() => {}); // stay alive
          });
          return { ok: pid };
        },
      };

      const childSpec = {
        id: 'crasher',
        start: [mod, 'start_link', []] as [any, string, any[]],
      };

      await DynamicSupervisor.start_child(sup, childSpec);
      await sleep(20);

      // Child crashed once, supervisor should have restarted it
      expect(startCount).toBe(2);

      const counts = await DynamicSupervisor.count_children(sup);
      expect(counts.active).toBe(1);
    });
  });
});
