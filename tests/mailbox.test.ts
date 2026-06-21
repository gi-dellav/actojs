import { describe, test, expect, beforeEach } from 'bun:test';
import * as M from '../src/mailbox';

function pids(): string[] {
  return M.allPids();
}

beforeEach(() => {
  for (const pid of pids()) {
    M.deregisterProcess(pid);
  }
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('mailbox', () => {
  describe('generatePid', () => {
    test('generates unique PIDs in sequence', () => {
      const a = M.generatePid();
      const b = M.generatePid();
      expect(a).not.toBe(b);
      expect(a).toMatch(/^#PID<0\.\d+\.0>$/);
      expect(b).toMatch(/^#PID<0\.\d+\.0>$/);
    });
  });

  describe('createProcess', () => {
    test('creates a process with correct initial state', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      expect(proc.pid).toBe(pid);
      expect(proc.mailbox).toEqual([]);
      expect(proc.recvResolve).toBeNull();
      expect(proc.links).toBeInstanceOf(Set);
      expect(proc.links.size).toBe(0);
      expect(proc.monitors).toBeInstanceOf(Map);
      expect(proc.monitoredBy).toBeInstanceOf(Map);
      expect(proc.trapExit).toBe(false);
      expect(proc.status).toBe('running');
      expect(proc.exitReason).toBeNull();
      expect(proc.registeredName).toBeNull();
    });
  });

  describe('registerProcess / getProcess / deregisterProcess', () => {
    test('registers, retrieves, and deregisters a process', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      expect(M.getProcess(pid)).toBe(proc);
      M.deregisterProcess(pid);
      expect(M.getProcess(pid)).toBeUndefined();
    });

    test('deregisterProcess cleans up the name registry', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.registeredName = 'test_name';
      M.registerProcess(pid, proc);
      M.registerName('test_name', pid);
      expect(M.whereisName('test_name')).toBe(pid);
      M.deregisterProcess(pid);
      expect(M.whereisName('test_name')).toBeNull();
    });

    test('getProcess returns undefined for unknown PID', () => {
      expect(M.getProcess('#PID<9999.0.0>')).toBeUndefined();
    });
  });

  describe('name registry', () => {
    test('registerName and whereisName', () => {
      const pid = M.generatePid();
      M.registerName('alice', pid);
      expect(M.whereisName('alice')).toBe(pid);
    });

    test('unregisterName removes the name', () => {
      const pid = M.generatePid();
      M.registerName('bob', pid);
      M.unregisterName('bob');
      expect(M.whereisName('bob')).toBeNull();
    });

    test('whereisName returns null for unknown name', () => {
      expect(M.whereisName('nobody')).toBeNull();
    });
  });

  describe('PID stack', () => {
    test('pushPid / popPid / getCurrentPid', () => {
      const pid = M.generatePid();
      M.pushPid(pid);
      expect(M.getCurrentPid()).toBe(pid);
      const pid2 = M.generatePid();
      M.pushPid(pid2);
      expect(M.getCurrentPid()).toBe(pid2);
      M.popPid();
      expect(M.getCurrentPid()).toBe(pid);
      M.popPid();
      expect(M.getCurrentPid()).toBeNull();
    });
  });

  describe('deliverMessage', () => {
    test('queues message when no receiver waiting', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      M.deliverMessage(pid, 'hello');
      expect(proc.mailbox).toEqual(['hello']);
      M.deliverMessage(pid, 'world');
      expect(proc.mailbox).toEqual(['hello', 'world']);
    });

    test('does nothing for unknown PID', () => {
      expect(() => M.deliverMessage('#PID<9999.0.0>', 'msg')).not.toThrow();
    });

    test('does not deliver to exited process', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.status = 'exited';
      M.registerProcess(pid, proc);
      M.deliverMessage(pid, 'msg');
      expect(proc.mailbox).toEqual([]);
    });

    test('does not deliver to exiting process', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.status = 'exiting';
      M.registerProcess(pid, proc);
      M.deliverMessage(pid, 'msg');
      expect(proc.mailbox).toEqual([]);
    });

    test('resolves waiting receiver immediately', async () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      M.pushPid(pid);

      const promise = M.receiveMessage(pid);
      M.deliverMessage(pid, 'instant');

      const val = await promise;
      expect(val).toBe('instant');
      expect(proc.recvResolve).toBeNull();
      expect(proc.mailbox.length).toBe(0);

      M.popPid();
    });
  });

  describe('receiveMessage', () => {
    test('returns queued message immediately without promise overhead', async () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.mailbox.push('queued');
      M.registerProcess(pid, proc);

      const val = await M.receiveMessage(pid);
      expect(val).toBe('queued');
      expect(proc.mailbox.length).toBe(0);
    });

    test('rejects when not inside a process and no PID given', async () => {
      expect(M.getCurrentPid()).toBeNull();
      await expect(M.receiveMessage()).rejects.toThrow('not inside a process');
    });

    test('rejects when process not found', async () => {
      await expect(M.receiveMessage('#PID<9999.0.0>')).rejects.toThrow('process not found');
    });
  });

  describe('getMailboxLength', () => {
    test('returns 0 for unknown PID', () => {
      expect(M.getMailboxLength('#PID<9999.0.0>')).toBe(0);
    });

    test('returns correct queue length', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      proc.mailbox.push('a', 'b', 'c');
      expect(M.getMailboxLength(pid)).toBe(3);
    });
  });

  describe('handleExit', () => {
    test('marks process as exited and deregs', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      M.registerProcess(pid, proc);
      proc.exitReason = 'normal';
      M.handleExit(proc);
      expect(proc.status).toBe('exited');
      expect(M.getProcess(pid)).toBeUndefined();
    });

    test('notifies linked processes with EXIT message when trapExit', async () => {
      const pidA = M.generatePid();
      const procA = M.createProcess(pidA);
      procA.status = 'alive';
      M.registerProcess(pidA, procA);

      const pidB = M.generatePid();
      const procB = M.createProcess(pidB);
      procB.status = 'alive';
      procB.trapExit = true;
      M.registerProcess(pidB, procB);

      procA.links.add(pidB);
      procB.links.add(pidA);
      procA.exitReason = 'oops';

      M.handleExit(procA);

      expect(procB.mailbox.length).toBe(1);
      const msg = procB.mailbox[0] as any;
      expect(msg.type).toBe('EXIT');
      expect(msg.from).toBe(pidA);
      expect(msg.reason).toBe('oops');
    });

    test('propagates exit to linked process without trapExit', () => {
      const pidA = M.generatePid();
      const procA = M.createProcess(pidA);
      procA.status = 'alive';
      M.registerProcess(pidA, procA);

      const pidB = M.generatePid();
      const procB = M.createProcess(pidB);
      procB.status = 'alive';
      procB.trapExit = false;
      M.registerProcess(pidB, procB);

      procA.links.add(pidB);
      procB.links.add(pidA);
      procA.exitReason = 'crash';

      M.handleExit(procA);

      expect(procB.status as string).toBe('exited');
      expect(procB.exitReason).toBe('crash');
    });

    test('sends DOWN to monitoring processes', () => {
      const pidA = M.generatePid();
      const procA = M.createProcess(pidA);
      procA.status = 'alive';
      M.registerProcess(pidA, procA);

      const pidB = M.generatePid();
      const procB = M.createProcess(pidB);
      procB.status = 'alive';
      M.registerProcess(pidB, procB);

      const ref = Symbol('monitor');
      procA.monitoredBy.set(pidB, [ref]);
      procA.exitReason = 'killed';

      M.handleExit(procA);

      expect(procB.mailbox.length).toBe(1);
      const msg = procB.mailbox[0] as any;
      expect(msg.type).toBe('DOWN');
      expect(msg.ref).toBe(ref);
      expect(msg.pid).toBe(pidA);
      expect(msg.reason).toBe('killed');
    });
  });

  describe('getProcessInfo', () => {
    test('returns null for unknown PID', () => {
      expect(M.getProcessInfo('#PID<9999.0.0>')).toBeNull();
    });

    test('returns correct info for a process', () => {
      const pid = M.generatePid();
      const proc = M.createProcess(pid);
      proc.status = 'alive';
      proc.trapExit = true;
      proc.registeredName = 'test';
      M.registerProcess(pid, proc);

      const info = M.getProcessInfo(pid);
      expect(info).not.toBeNull();
      expect(info!.status).toBe('alive');
      expect(info!.messageQueueLength).toBe(0);
      expect(info!.trapExit).toBe(true);
      expect(info!.registeredName).toBe('test');
      expect(info!.links).toEqual([]);
    });
  });

  describe('runWithPid', () => {
    test('sets pid during synchronous execution', () => {
      const pid = M.generatePid();
      let captured: string | null = null;
      M.runWithPid(pid, () => {
        captured = M.getCurrentPid();
      });
      expect(captured!).toBe(pid);
      expect(M.getCurrentPid()).toBeNull();
    });

    test('restores stack after exception', () => {
      const pid = M.generatePid();
      expect(() => {
        M.runWithPid(pid, () => {
          throw new Error('boom');
        });
      }).toThrow('boom');
      expect(M.getCurrentPid()).toBeNull();
    });

    test('works with async functions', async () => {
      const pid = M.generatePid();
      const result = await M.runWithPid(pid, async () => {
        return M.getCurrentPid();
      });
      expect(result).toBe(pid);
      expect(M.getCurrentPid()).toBeNull();
    });
  });
});
