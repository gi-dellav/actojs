import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import { NodeRuntime } from '../src/node_runtime';
import { getRuntime, setRuntime, WebRuntime } from '../src/core';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep } from './helpers';

const nr = new NodeRuntime();

beforeEach(() => {
  M.clearPidStack();
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

afterEach(() => {
  setRuntime(new WebRuntime());
  nr.stop();
  M.clearPidStack();
});

function createListener(): string {
  const pid = M.generatePid();
  const proc = M.createProcess(pid);
  proc.status = 'running';
  M.registerProcess(pid, proc);
  return pid;
}

describe('NodeRuntime', () => {
  describe('constructor', () => {
    test('has name node-worker', () => {
      expect(nr.name).toBe('node-worker');
    });

    test('available is true', () => {
      expect(nr.available).toBe(true);
    });
  });

  describe('spawn', () => {
    test('returns a PID', () => {
      const pid = nr.spawn(() => {});
      expect(pid).toMatch(/^#PID</);
    });

    test('runs the function in a worker (communicates via Wr.send)', async () => {
      const listener = createListener();
      nr.spawn((Wr: any, targetPid: string) => {
        Wr.send(targetPid, 'ok');
      }, undefined, [listener]);
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe('ok');
    });

    test('process exits after completion', async () => {
      const pid = nr.spawn(() => {});
      await sleep(250);
      const proc = M.getProcess(pid);
      expect(proc?.status === 'exited' || !proc).toBeTruthy();
    });

    test('catches sync errors', async () => {
      const pid = nr.spawn(() => { throw new Error('boom'); });
      await sleep(150);
      const proc = M.getProcess(pid);
      if (proc) {
        expect(proc.status === 'exiting' || proc.status === 'exited').toBe(true);
      }
    });

    test('args are passed to the function', async () => {
      const listener = createListener();
      nr.spawn((Wr: any, target: string, x: number, y: number) => {
        Wr.send(target, x + y);
      }, undefined, [listener, 3, 4]);
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe(7);
    });

    test('spawn with link option links processes', () => {
      const callerPid = 'caller_link_test';
      const procCaller = M.createProcess(callerPid);
      procCaller.status = 'running';
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = nr.spawn(() => {}, ['link']);
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc).toBeDefined();
      expect(spawnedProc!.links.has(callerPid)).toBe(true);
      expect(procCaller.links.has(pid)).toBe(true);

      M.popPid();
    });

    test('spawn with monitor option monitors the process', () => {
      const callerPid = M.generatePid();
      const procCaller = M.createProcess(callerPid);
      procCaller.status = 'running';
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = nr.spawn(() => {}, ['monitor']);
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc).toBeDefined();
      expect(spawnedProc!.monitoredBy.has(callerPid)).toBe(true);

      M.popPid();
      nr.stop();
    });
  });

  describe('message routing', () => {
    test('delivers messages to worker processes', async () => {
      const listener = createListener();
      const pid = nr.spawn(async (Wr: any, targetPid: string) => {
        const msg = await Wr.receive();
        Wr.send(targetPid, msg);
      }, undefined, [listener]);
      nr.deliver(pid, 'hello');
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe('hello');
    });

    test('worker can send messages to a main-thread process', async () => {
      const listener = createListener();
      nr.spawn((Wr: any, targetPid: string) => {
        Wr.send(targetPid, 'from_worker');
      }, undefined, [listener]);
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe('from_worker');
    });

    test('worker-to-worker messaging works', async () => {
      const listener = createListener();
      const pidA = nr.spawn(async (Wr: any, targetPid: string) => {
        const msg = await Wr.receive();
        Wr.send(targetPid, msg);
      }, undefined, [listener]);

      await sleep(50);
      nr.spawn((Wr: any, destPid: string) => {
        Wr.send(destPid, 'hello_from_B');
      }, undefined, [pidA]);

      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe('hello_from_B');
    });
  });

  describe('process primitives in worker', () => {
    test('self() returns the assigned PID', async () => {
      const listener = createListener();
      const pid = nr.spawn((Wr: any, targetPid: string) => {
        Wr.send(targetPid, Wr.self());
      }, undefined, [listener]);
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe(pid);
    });

    test('sleep() works in worker', async () => {
      const listener = createListener();
      nr.spawn(async (Wr: any, targetPid: string) => {
        const start = Date.now();
        await Wr.sleep(50);
        Wr.send(targetPid, Date.now() - start);
      }, undefined, [listener]);
      await sleep(250);
      const elapsed = M.shiftMessage(listener) as number;
      expect(elapsed).toBeGreaterThanOrEqual(40);
    });

    test('send with PID target works from worker', async () => {
      const listener = createListener();
      nr.spawn((Wr: any, targetPid: string) => {
        Wr.send(targetPid, { data: 42 });
      }, undefined, [listener]);
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toEqual({ data: 42 });
    });
  });

  describe('sys_call routing', () => {
    test('worker can register a name', async () => {
      const listener = createListener();
      const pid = nr.spawn(async (Wr: any, targetPid: string) => {
        await Wr.register('test_nr_svc');
        Wr.send(targetPid, 'done');
        await Wr.receive();
      }, undefined, [listener]);
      await sleep(250);
      expect(M.whereisName('test_nr_svc')).toBe(pid);
      M.unregisterName('test_nr_svc');
      nr.deliver(pid, 'exit');
      await sleep(50);
    });

    test('worker can flag trap_exit', async () => {
      const listener = createListener();
      const pid = nr.spawn(async (Wr: any, targetPid: string) => {
        const prev = await Wr.flag('trap_exit', true);
        Wr.send(targetPid, prev);
        await Wr.receive();
      }, undefined, [listener]);
      await sleep(250);
      const prev = M.shiftMessage(listener);
      expect(prev).toBe(false);
      const proc = M.getProcess(pid);
      expect(proc?.trapExit).toBe(true);
      nr.deliver(pid, 'exit');
      await sleep(50);
    });

    test('worker can get/put process dictionary', async () => {
      const listener = createListener();
      nr.spawn(async (Wr: any, targetPid: string) => {
        const prevPut = await Wr.put('x', 10);
        const val = await Wr.get('x');
        Wr.send(targetPid, [prevPut, val]);
      }, undefined, [listener]);
      await sleep(150);
      const [prevPut, val] = M.shiftMessage(listener) as [unknown, unknown];
      expect(prevPut).toBeUndefined();
      expect(val).toBe(10);
    });

    test('worker can check alive', async () => {
      const listener = createListener();
      const pid = nr.spawn(async (Wr: any, targetPid: string) => {
        const alive = await Wr.alive(Wr.self());
        Wr.send(targetPid, alive);
        await Wr.receive();
      }, undefined, [listener]);
      await sleep(250);
      const isAlive = M.shiftMessage(listener);
      expect(isAlive).toBe(true);
      nr.deliver(pid, 'exit');
      await sleep(50);
    });

    test('worker can get info', async () => {
      const listener = createListener();
      const pid = nr.spawn(async (Wr: any, targetPid: string) => {
        const info = await Wr.info(Wr.self());
        Wr.send(targetPid, info);
        await Wr.receive();
      }, undefined, [listener]);
      await sleep(250);
      const info = M.shiftMessage(listener) as any;
      expect(info).not.toBeNull();
      expect(info.status).toBe('running');
      nr.deliver(pid, 'exit');
      await sleep(50);
    });

    test('worker can list processes', async () => {
      const listener = createListener();
      const pid = nr.spawn(async (Wr: any, targetPid: string) => {
        const list = await Wr.list();
        Wr.send(targetPid, list);
        await Wr.receive();
      }, undefined, [listener]);
      await sleep(250);
      const list = M.shiftMessage(listener) as string[];
      expect(list).toBeDefined();
      expect(list).toContain(pid);
      nr.deliver(pid, 'exit');
      await sleep(50);
    });
  });

  describe('stop', () => {
    test('terminates all workers', async () => {
      const pid1 = nr.spawn(async (Wr: any) => {
        await Wr.receive();
      });
      const pid2 = nr.spawn(async (Wr: any) => {
        await Wr.receive();
      });
      await sleep(250);

      nr.stop();

      await sleep(150);
      const procs = [pid1, pid2].map(p => M.getProcess(p));
      for (const p of procs) {
        if (p) {
          expect(p.status === 'exited' || p.status === 'exiting').toBe(true);
        }
      }
    });
  });

  describe('integration with Process.spawn', () => {
    test('Process.spawn delegates to NodeRuntime when set', async () => {
      setRuntime(nr);
      const listener = createListener();
      const body = `
        const Wr = globalThis.Wr;
        if (Wr) Wr.send('${listener}', 'ran');
      `;
      // eslint-disable-next-line no-new-func
      const fn = new Function(body) as () => void;
      const pid = Process.spawn(fn);
      await sleep(150);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe('ran');
      expect(pid).toMatch(/^#PID</);
    });

    test('Process.spawn returns correct PID with link option', () => {
      setRuntime(nr);
      const callerPid = 'caller_nr_integration';
      const proc = M.createProcess(callerPid);
      proc.status = 'running';
      M.registerProcess(callerPid, proc);
      M.pushPid(callerPid);

      const pid = Process.spawn(() => {}, ['link']);
      expect(pid).toMatch(/^#PID</);
      const spawned = M.getProcess(pid);
      expect(spawned!.links.has(callerPid)).toBe(true);

      M.popPid();
    });
  });
});
