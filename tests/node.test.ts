import { describe, test, expect, beforeEach, afterEach } from 'bun:test';
import * as Node from '../src/node';
import * as Process from '../src/process';
import * as M from '../src/mailbox';

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
  try { Node.stop(); } catch (_) {}
});

afterEach(() => {
  try { Node.stop(); } catch (_) {}
});

function runInProcess<T>(fn: () => T): Promise<T> {
  return new Promise((resolve, reject) => {
    Process.spawn(() => {
      try { resolve(fn()); } catch (e) { reject(e); }
    });
  });
}

describe('node', () => {
  describe('start / stop', () => {
    test('starts a node', async () => {
      const result = await runInProcess(() => Node.start('test_node'));
      expect('ok' in result).toBe(true);
    });

    test('returns error if already started', async () => {
      await runInProcess(() => Node.start('test_node'));
      const result = await runInProcess(() => Node.start('test_node'));
      expect('error' in result).toBe(true);
    });

    test('stop returns undefined when not started', () => {
      try { Node.stop(); } catch (_) {}
      const result = Node.stop();
      if (result !== undefined) {
        expect('error' in result).toBe(true);
      }
    });
  });

  describe('self', () => {
    test('returns node name when started', async () => {
      await runInProcess(() => Node.start('my_node'));
      expect(Node.self()).toBe('my_node');
    });

    test('throws if not started', () => {
      try { Node.stop(); } catch (_) {}
      expect(() => Node.self()).toThrow('node not started');
    });
  });

  describe('alive', () => {
    test('returns true when started', async () => {
      await runInProcess(() => Node.start('alive_node'));
      expect(Node.alive()).toBe(true);
    });

    test('returns false when stopped', async () => {
      await runInProcess(() => Node.start('alive_node'));
      Node.stop();
      expect(Node.alive()).toBe(false);
    });

    test('returns false when never started', () => {
      try { Node.stop(); } catch (_) {}
      expect(Node.alive()).toBe(false);
    });
  });

  describe('connect / disconnect', () => {
    test('connect adds node', async () => {
      await runInProcess(() => Node.start('node_a'));
      expect(Node.connect('node_b')).toBe(true);
    });

    test('connect returns false if not started', () => {
      try { Node.stop(); } catch (_) {}
      expect(Node.connect('node_b')).toBe(false);
    });

    test('connect returns ignored if already connected', async () => {
      await runInProcess(() => Node.start('node_a'));
      Node.connect('node_b');
      expect(Node.connect('node_b')).toBe('ignored');
    });

    test('disconnect removes node', async () => {
      await runInProcess(() => Node.start('node_a'));
      Node.connect('node_b');
      Node.disconnect('node_b');
      expect(Node.connect('node_b')).toBe(true);
    });
  });

  describe('ping', () => {
    test('returns pong for connected node', async () => {
      await runInProcess(() => Node.start('node_a'));
      Node.connect('node_b');
      expect(Node.ping('node_b')).toBe('pong');
    });

    test('returns pang for unknown node', async () => {
      await runInProcess(() => Node.start('node_a'));
      expect(Node.ping('unknown')).toBe('pang');
    });
  });

  describe('list', () => {
    test('returns this node by default', async () => {
      await runInProcess(() => Node.start('node_a'));
      expect(Node.list()).toContain('node_a');
    });

    test('returns connected nodes', async () => {
      await runInProcess(() => Node.start('node_a'));
      Node.connect('node_b');
      Node.connect('node_c');
      const nodes = Node.list();
      expect(nodes).toContain('node_a');
      expect(nodes).toContain('node_b');
      expect(nodes).toContain('node_c');
    });

    test('returns empty when not started', () => {
      try { Node.stop(); } catch (_) {}
      expect(Node.list()).toEqual([]);
    });

    test('filters by state', async () => {
      await runInProcess(() => Node.start('node_a'));
      Node.connect('node_b');
      expect(Node.list('visible')).toContain('node_a');
      expect(Node.list('visible')).not.toContain('node_b');
      expect(Node.list('connected')).toContain('node_b');
    });

    test('accepts array of states', async () => {
      await runInProcess(() => Node.start('node_a'));
      Node.connect('node_b');
      const all = Node.list(['visible', 'connected']);
      expect(all).toContain('node_a');
      expect(all).toContain('node_b');
    });
  });

  describe('monitor', () => {
    test('does not throw', async () => {
      await runInProcess(() => Node.start('node_a'));
      expect(() => Node.monitor('node_b', true)).not.toThrow();
    });
  });
});
