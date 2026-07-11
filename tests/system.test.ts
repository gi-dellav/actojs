import { describe, test, expect, beforeEach } from 'bun:test';
import { ActorSystem } from '../src/system';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep } from './helpers';

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

function waitFor(fn: () => boolean, ms = 1000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (fn()) return resolve();
      if (Date.now() - start > ms) return reject(new Error('timeout'));
      setTimeout(check, 5);
    };
    check();
  });
}

describe('ActorSystem', () => {
  describe('default system', () => {
    test('ActorSystem.current returns the default system', () => {
      const sys = ActorSystem.current;
      expect(sys).toBeInstanceOf(ActorSystem);
      expect(sys.systemId).toBe('0');
    });

    test('ActorSystem.default always returns the same instance', () => {
      const a = ActorSystem.default;
      const b = ActorSystem.default;
      expect(a).toBe(b);
    });
  });

  describe('process isolation', () => {
    test('processes spawned in system A are not visible in system B', async () => {
      const sysA = new ActorSystem('a');
      const sysB = new ActorSystem('b');

      let pidA = '';
      ActorSystem.run(sysA, () => {
        pidA = Process.spawn(async () => { await Process.receive(); });
      });

      await sleep(10);

      // pidA should be alive in sysA
      ActorSystem.run(sysA, () => {
        expect(Process.alive(pidA)).toBe(true);
      });

      // pidA should NOT be alive in sysB
      ActorSystem.run(sysB, () => {
        expect(Process.alive(pidA)).toBe(false);
      });

      // Cleanup
      ActorSystem.run(sysA, () => {
        Process.exit(pidA, 'done');
      });
    });

    test('name registrations are isolated between systems', async () => {
      const sysA = new ActorSystem('a');
      const sysB = new ActorSystem('b');

      let pidA = '';
      ActorSystem.run(sysA, () => {
        pidA = Process.spawn(async () => {
          Process.register(Process.self(), 'shared_name');
          await Process.receive(); // stay alive
        });
      });

      await sleep(10);

      ActorSystem.run(sysA, () => {
        expect(Process.whereis('shared_name')).toBe(pidA);
      });

      ActorSystem.run(sysB, () => {
        expect(Process.whereis('shared_name')).toBeNull();
      });

      ActorSystem.run(sysA, () => {
        Process.exit(pidA, 'done');
      });
    });

    test('exit cascades do not cross system boundaries', async () => {
      const sysA = new ActorSystem('a');
      const sysB = new ActorSystem('b');

      let pidA1 = '', pidA2 = '';
      let pidB = '';

      // Spawn processes in sysA and link them via direct state manipulation
      ActorSystem.run(sysA, () => {
        pidA1 = Process.spawn(async () => { await Process.receive(); });
        pidA2 = Process.spawn(async () => { await Process.receive(); });
        // Link them together
        const p1 = sysA.getProcess(pidA1);
        const p2 = sysA.getProcess(pidA2);
        if (p1 && p2) {
          p1.links.add(pidA2);
          p2.links.add(pidA1);
        }
      });

      ActorSystem.run(sysB, () => {
        pidB = Process.spawn(async () => { await Process.receive(); });
      });

      await sleep(10);

      // Kill pidA2 in sysA — should cascade to pidA1 but NOT pidB
      ActorSystem.run(sysA, () => {
        Process.exit(pidA2, 'crash');
      });

      await sleep(20);

      ActorSystem.run(sysA, () => {
        expect(Process.alive(pidA2)).toBe(false);
        expect(Process.alive(pidA1)).toBe(false);
      });

      // pidB in sysB should still be alive
      ActorSystem.run(sysB, () => {
        expect(Process.alive(pidB)).toBe(true);
      });

      ActorSystem.run(sysB, () => {
        Process.exit(pidB, 'done');
      });
    });

    test('ActorSystem.run restores previous system after exception', () => {
      const prev = ActorSystem.current;
      const sys = new ActorSystem('test');

      try {
        ActorSystem.run(sys, () => {
          throw new Error('boom');
        });
      } catch (_) {
        // expected
      }

      expect(ActorSystem.current).toBe(prev);
      expect(ActorSystem.current.systemId).toBe('0');
    });

    test('ActorSystem.run restores previous system after async success', async () => {
      const prev = ActorSystem.current;
      const sys = new ActorSystem('test');

      await ActorSystem.run(sys, async () => {
        await sleep(5);
      });

      expect(ActorSystem.current).toBe(prev);
    });

    test('ActorSystem.run restores previous system after async failure', async () => {
      const prev = ActorSystem.current;
      const sys = new ActorSystem('test');

      try {
        await ActorSystem.run(sys, async () => {
          await sleep(5);
          throw new Error('async boom');
        });
      } catch (_) {
        // expected
      }

      expect(ActorSystem.current).toBe(prev);
    });
  });

  describe('PID generation', () => {
    test('default system PIDs use old format', () => {
      const pid = ActorSystem.current.generatePid();
      expect(pid).toMatch(/^#PID<0\.\d+\.0>$/);
    });

    test('named system PIDs include system name', () => {
      const sys = new ActorSystem('myapp');
      const pid = sys.generatePid();
      expect(pid).toMatch(/^#PID<myapp@0\.\d+\.0>$/);
    });
  });

  describe('message delivery isolation', () => {
    test('send delivers only within current system', async () => {
      const sysA = new ActorSystem('a');
      const sysB = new ActorSystem('b');

      let pidA = '';
      let pidB = '';
      let receivedA: unknown;
      let receivedB: unknown;

      ActorSystem.run(sysA, () => {
        pidA = Process.spawn(async () => {
          receivedA = await Process.receive();
        });
      });

      ActorSystem.run(sysB, () => {
        pidB = Process.spawn(async () => {
          receivedB = await Process.receive();
        });
      });

      await sleep(10);

      // Send within sysA
      ActorSystem.run(sysA, () => {
        Process.send(pidA, 'hello_from_A');
      });

      await waitFor(() => receivedA !== undefined, 500);
      expect(receivedA).toBe('hello_from_A');
      expect(receivedB).toBeUndefined();

      ActorSystem.run(sysA, () => Process.exit(pidA, 'done'));
      ActorSystem.run(sysB, () => Process.exit(pidB, 'done'));
    });
  });

  describe('hasMessages', () => {
    test('returns false for unknown PID', () => {
      expect(M.hasMessages('#PID<9999.0.0>')).toBe(false);
    });

    test('returns true when mailbox has messages', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      proc.mailbox.push('msg');
      expect(M.hasMessages(pid)).toBe(true);
    });

    test('returns false for empty mailbox', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      expect(M.hasMessages(pid)).toBe(false);
    });
  });

  describe('onExit handler', () => {
    test('onExit is called when process exits abnormally', async () => {
      const sys = new ActorSystem();

      let exitReport: any = null;
      sys.onExit = (report) => { exitReport = report; };

      ActorSystem.run(sys, () => {
        const pid = Process.spawn(() => { throw new Error('bang'); });
      });
      await sleep(20);

      expect(exitReport).not.toBeNull();
      expect(exitReport.reason).toBeInstanceOf(Error);
    });

    test('onExit is called for normal exit too', async () => {
      const sys = new ActorSystem();

      let called = false;
      sys.onExit = () => { called = true; };

      ActorSystem.run(sys, () => {
        Process.spawn(() => {});
      });
      await sleep(20);

      expect(called).toBe(true);
    });

    test('onExit errors are caught', async () => {
      const sys = new ActorSystem();
      sys.onExit = () => { throw new Error('bad handler'); };

      ActorSystem.run(sys, () => {
        Process.spawn(() => { throw new Error('boom'); });
      });
      await sleep(20);

      expect(true).toBe(true);
    });
  });

  describe('message budget and yielding', () => {
    test('countMessage returns true when budget exhausted', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.messageBudget = 3;
      proc.messageCount = 1;
      M.registerProcess(pid, proc);

      expect(M.countMessage(pid)).toBe(false);
      expect(M.countMessage(pid)).toBe(true);
      expect(proc.messageCount).toBe(0);
    });

    test('countMessage returns false when budget is 0', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.messageBudget = 0;
      M.registerProcess(pid, proc);

      expect(M.countMessage(pid)).toBe(false);
    });

    test('countMessage returns false for unknown PID', () => {
      expect(M.countMessage('#PID<9999.0.0>')).toBe(false);
    });

    test('doYield runs yield without throwing', async () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);

      await M.doYield(pid);
    });

    test('yieldIfNeeded yields when budget exhausted', async () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.messageBudget = 1;
      proc.messageCount = 0;
      M.registerProcess(pid, proc);

      await M.yieldIfNeeded(pid);
      expect(proc.messageCount).toBe(0);
    });

    test('yieldIfNeeded does nothing for unknown PID', async () => {
      await M.yieldIfNeeded('#PID<9999.0.0>');
    });

    test('yieldIfNeeded skips when budget is 0', async () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.messageBudget = 0;
      proc.messageCount = 5;
      M.registerProcess(pid, proc);

      await M.yieldIfNeeded(pid);
      expect(proc.messageCount).toBe(5);
    });
  });

  describe('receiveMessage timeout', () => {
    test('receiveMessage rejects on timeout', async () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.status = 'alive';
      M.registerProcess(pid, proc);

      await expect(M.receiveMessage(pid, 5)).rejects.toThrow('timed out');
    });
  });
});
