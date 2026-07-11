import { describe, test, expect, beforeEach } from 'bun:test';
import * as Supervisor from '../src/supervisor';
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

// Create a module with start_link that spawns a long-running process
function makeWorkerMod() {
  return {
    start_link() {
      const pid = Process.spawn(async () => {
        await Process.receive(); // block forever
      });
      return { ok: pid };
    },
  };
}

function makeChildSpec(id: string, mod: any) {
  return {
    id,
    start: [mod, 'start_link', []] as [any, string, any[]],
    restart: 'permanent' as const,
  };
}

beforeEach(() => {
  M.clearPidStack();
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('supervisor', () => {
  describe('start_link with static children', () => {
    test('starts supervisor with children', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      expect('ok' in result).toBe(true);
    });

    test('returns error if child init fails', async () => {
      const mod = {
        start_link() {
          return { error: new Error('child init failed') };
        },
      };
      const child = makeChildSpec('bad', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      expect('error' in result).toBe(true);
    });

    test('starts with name', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one', name: 'my_sup' });
      expect('ok' in result).toBe(true);
      expect(Process.whereis('my_sup')).toBe((result as any).ok);
    });
  });

  describe('start_link with module-based init', () => {
    test('uses module.init to get spec', async () => {
      const childMod = makeWorkerMod();
      const supMod = {
        init() {
          return Supervisor.init([makeChildSpec('c1', childMod)], { strategy: 'one_for_one' });
        },
      };
      const result = await Supervisor.start_link(supMod, null);
      expect('ok' in result).toBe(true);
    });

    test('returns error if module has no init', async () => {
      const result = await Supervisor.start_link({} as any, null);
      expect('error' in result).toBe(true);
    });
  });

  describe('count_children', () => {
    test('returns correct counts', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.specs).toBe(1);
      expect(counts.active).toBe(1);
      expect(counts.workers).toBe(1);
      expect(counts.supervisors).toBe(0);
    });
  });

  describe('which_children', () => {
    test('returns info for alive children', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const children = await Supervisor.which_children(result.ok);
      expect(children.length).toBe(1);
      expect(children[0]!.id).toBe('w1');
    });
  });

  describe('start_child', () => {
    test('adds a child dynamically', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('initial', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const newChild = makeChildSpec('dynamic', mod);
      const startResult = await Supervisor.start_child(result.ok, newChild);
      expect('ok' in startResult).toBe(true);

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.specs).toBe(2);
      expect(counts.active).toBe(2);
    });

    test('returns error for invalid module', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('initial', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const badChild = { id: 'bad', start: [null, 'nope', []] as any };
      const startResult = await Supervisor.start_child(result.ok, badChild);
      expect('error' in startResult).toBe(true);
    });
  });

  describe('terminate_child', () => {
    test('terminates a child by id', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      await Supervisor.terminate_child(result.ok, 'w1');
      await sleep(20);

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(0);
    });

    test('returns error for unknown child', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const termResult = await Supervisor.terminate_child(result.ok, 'nobody');
      expect('error' in (termResult as any)).toBe(true);
    });
  });

  describe('delete_child', () => {
    test('returns error if child still running', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const delResult = await Supervisor.delete_child(result.ok, 'w1');
      expect('error' in (delResult as any)).toBe(true);
      if ('error' in (delResult as any)) {
        expect((delResult as any).error).toBe('child_running');
      }
    });
  });

  describe('stop', () => {
    test('stop kills all children', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      await Supervisor.stop(result.ok);
      await sleep(30);

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(0);
    });
  });

  describe('child_spec', () => {
    test('returns spec with defaults', () => {
      const mod = { name: 'TestMod' };
      const spec = Supervisor.child_spec(mod);
      expect(spec.id).toBe('TestMod');
      expect(spec.start).toEqual([mod, 'start_link', []]);
    });

    test('uses module.child_spec if available', () => {
      const mod = {
        name: 'TestMod',
        child_spec() {
          return { id: 'custom', start: [mod, 'custom_start', [1, 2]] };
        },
      };
      const spec = Supervisor.child_spec(mod);
      expect(spec.id).toBe('custom');
      expect(spec.start).toEqual([mod, 'custom_start', [1, 2]]);
    });

    test('merges overrides', () => {
      const mod = { name: 'TestMod' };
      const spec = Supervisor.child_spec(mod, { restart: 'temporary' });
      expect(spec.id).toBe('TestMod');
      expect(spec.restart).toBe('temporary');
    });
  });

  describe('init', () => {
    test('returns a SupervisorSpec', () => {
      const spec = Supervisor.init([], { strategy: 'one_for_all' });
      expect(spec.strategy).toBe('one_for_all');
      expect(spec.children).toEqual([]);
      expect(spec.max_restarts).toBe(3);
      expect(spec.max_seconds).toBe(5);
    });
  });

  describe('one_for_one restart strategy', () => {
    test('restarts only the failed child', async () => {
      const mod = makeWorkerMod();
      const child1 = makeChildSpec('c1', mod);
      const child2 = makeChildSpec('c2', mod);

      const result = await Supervisor.start_link([child1, child2], {
        strategy: 'one_for_one',
        max_restarts: 10,
      });
      if ('error' in result) throw result.error;

      let counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(2);

      const children = await Supervisor.which_children(result.ok);
      expect(children.length).toBe(2);
      Process.exit(children[0]!.pid, 'abnormal');

      await sleep(200);

      counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(2);
    });
  });

  describe('one_for_all restart strategy', () => {
    test('restarts all children when one fails', async () => {
      const mod = makeWorkerMod();
      const child1 = makeChildSpec('c1', mod);
      const child2 = makeChildSpec('c2', mod);

      const result = await Supervisor.start_link([child1, child2], {
        strategy: 'one_for_all',
        max_restarts: 10,
      });
      if ('error' in result) throw result.error;

      let counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(2);

      const children = await Supervisor.which_children(result.ok);
      Process.exit(children[0]!.pid, 'abnormal');

      await sleep(200);

      // one_for_all restarts all children, so both should be alive
      counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(2);
    });
  });

  describe('rest_for_one restart strategy', () => {
    test('restarts failed child and all after it', async () => {
      const mod = makeWorkerMod();
      const child1 = makeChildSpec('c1', mod);
      const child2 = makeChildSpec('c2', mod);
      const child3 = makeChildSpec('c3', mod);

      const result = await Supervisor.start_link([child1, child2, child3], {
        strategy: 'rest_for_one',
        max_restarts: 10,
      });
      if ('error' in result) throw result.error;

      let counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(3);

      // Kill child2 (index 1), rest_for_one should restart child2 AND child3
      const children = await Supervisor.which_children(result.ok);
      const child2Pid = children.find(c => c.id === 'c2')?.pid;
      expect(child2Pid).toBeDefined();
      Process.exit(child2Pid!, 'abnormal');

      await sleep(200);

      counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(3);
    });
  });

  describe('restart rate limiting', () => {
    test('shuts down supervisor when max_restarts exceeded', async () => {
      // Child that dies immediately with abnormal reason, causing rapid restarts
      const mod = {
        start_link() {
          const pid = Process.spawn(async () => {
            await Process.sleep(0);
            throw new Error('abnormal');
          });
          return { ok: pid };
        },
      };
      const child = {
        id: 'unstable',
        start: [mod, 'start_link', []] as [any, string, any[]],
        restart: 'permanent' as const,
      };

      const result = await Supervisor.start_link([child], {
        strategy: 'one_for_one',
        max_restarts: 2,
        max_seconds: 60,
      });
      if ('error' in result) throw result.error;

      // Wait for multiple restarts
      await sleep(300);

      // The supervisor should shut down after exceeding max_restarts
      expect(Process.alive(result.ok)).toBe(false);
    });

    test('significant child causes supervisor shutdown', async () => {
      let startCount = 0;
      const mod = {
        start_link() {
          startCount++;
          const pid = Process.spawn(async () => {
            if (startCount <= 1) throw new Error('crash');
            await Process.receive();
          });
          return { ok: pid };
        },
      };
      const child = {
        id: 'important',
        start: [mod, 'start_link', []] as [any, string, any[]],
        restart: 'permanent' as const,
        significant: true,
      };

      const result = await Supervisor.start_link([child], {
        strategy: 'one_for_one',
        max_restarts: 10,
      });
      if ('error' in result) throw result.error;

      await sleep(100);

      expect(Process.alive(result.ok)).toBe(false);
    });
  });

  describe('restart_child', () => {
    test('restarts a running child', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('r1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const restartResult = await Supervisor.restart_child(result.ok, 'r1');
      expect('ok' in restartResult).toBe(true);

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(1);
    });

    test('returns error for unknown child', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('r1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const restartResult = await Supervisor.restart_child(result.ok, 'nope');
      expect('error' in (restartResult as any)).toBe(true);
    });
  });

  describe('terminate_child shutdown modes', () => {
    test('handles brutal_kill shutdown', async () => {
      const mod = makeWorkerMod();
      const result = await Supervisor.start_link([
        { id: 'bk_child', start: [mod, 'start_link', []] as [any, string, any[]],
          restart: 'permanent', shutdown: 'brutal_kill' },
      ], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const before = await Supervisor.which_children(result.ok);
      expect(before.length).toBe(1);

      await Supervisor.terminate_child(result.ok, 'bk_child');
      await sleep(20);

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(0);
    });

    test('handles infinity shutdown', async () => {
      const mod = makeWorkerMod();
      const result = await Supervisor.start_link([
        { id: 'inf_child', start: [mod, 'start_link', []] as [any, string, any[]],
          restart: 'permanent', shutdown: 'infinity' },
      ], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      await Supervisor.terminate_child(result.ok, 'inf_child');
      await sleep(20);

      const counts = await Supervisor.count_children(result.ok);
      expect(counts.active).toBe(0);
    });
  });

  describe('delete_child', () => {
    test('succeeds when child is not running', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('w1', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const children = await Supervisor.which_children(result.ok);
      Process.exit(children[0]!.pid, 'killed');
      await sleep(30);

      const delResult = await Supervisor.delete_child(result.ok, 'w1');
      expect(delResult).toBe(undefined);
    });
  });

  describe('child_spec edge cases', () => {
    test('returns spec with id when id exists', () => {
      const spec = Supervisor.child_spec({ id: 'custom', start: [] as any });
      expect(spec.id).toBe('custom');
    });
  });

  describe('startChildSpec error paths', () => {
    test('returns error for null module', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('ok_child', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const badChild = { id: 'bad', start: [null, 'start_link', []] as any };
      const startResult = await Supervisor.start_child(result.ok, badChild);
      expect('error' in startResult).toBe(true);
    });

    test('returns error for missing function', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('ok_child', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const badChild = { id: 'bad2', start: [mod, 'nonexistent', []] as any };
      const startResult = await Supervisor.start_child(result.ok, badChild);
      expect('error' in startResult).toBe(true);
    });

    test('returns error when start function throws', async () => {
      const mod = makeWorkerMod();
      const child = makeChildSpec('ok_child', mod);
      const result = await Supervisor.start_link([child], { strategy: 'one_for_one' });
      if ('error' in result) throw result.error;

      const throwerMod = {
        start_link() { throw new Error('sync throw'); },
      };
      const badChild = { id: 'thrower', start: [throwerMod, 'start_link', []] as any };
      const startResult = await Supervisor.start_child(result.ok, badChild);
      expect('error' in startResult).toBe(true);
    });
  });
});
