import { describe, test, expect, beforeEach } from 'bun:test';
import * as PubSub from '../src/pubsub';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep, waitUntil } from './helpers';

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('pubsub', () => {
  describe('start_link', () => {
    test('starts a pubsub instance', async () => {
      const pid = await PubSub.start_link();
      expect(Process.alive(pid)).toBe(true);
    });

    test('starts with a registered name', async () => {
      const pid = await PubSub.start_link({ name: 'my_pubsub' });
      expect(Process.whereis('my_pubsub')).toBe(pid);
    });
  });

  describe('subscribe / publish', () => {
    test('subscriber receives published messages', async () => {
      const ps = await PubSub.start_link();

      const received: unknown[] = [];
      const sub = Process.spawn(async () => {
        // wait for subscription to be processed before expecting messages
        while (true) {
          const msg = await Process.receive();
          if (PubSub.isPubSubMessage(msg)) {
            received.push(msg.message);
          }
        }
      });
      await sleep(10);

      // Subscribe from inside the process
      M.pushPid(sub);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      PubSub.publish(ps, 'topic1', 'hello');
      await sleep(10);

      expect(received.length).toBe(1);
      expect(received[0]).toBe('hello');

      Process.exit(sub, 'normal');
    });

    test('unsubscribed process does not receive messages', async () => {
      const ps = await PubSub.start_link();

      const received: unknown[] = [];
      const sub = Process.spawn(async () => {
        while (true) {
          const msg = await Process.receive();
          if (PubSub.isPubSubMessage(msg)) {
            received.push(msg.message);
          }
        }
      });
      await sleep(10);

      M.pushPid(sub);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      M.pushPid(sub);
      PubSub.unsubscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      PubSub.publish(ps, 'topic1', 'should not arrive');
      await sleep(10);

      expect(received.length).toBe(0);

      Process.exit(sub, 'normal');
    });

    test('multiple subscribers receive messages', async () => {
      const ps = await PubSub.start_link();
      const received1: unknown[] = [];
      const received2: unknown[] = [];

      const sub1 = Process.spawn(async () => {
        while (true) {
          const msg = await Process.receive();
          if (PubSub.isPubSubMessage(msg)) received1.push(msg.message);
        }
      });
      const sub2 = Process.spawn(async () => {
        while (true) {
          const msg = await Process.receive();
          if (PubSub.isPubSubMessage(msg)) received2.push(msg.message);
        }
      });
      await sleep(10);

      M.pushPid(sub1);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      M.pushPid(sub2);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      PubSub.publish(ps, 'topic1', 42);
      await sleep(10);

      expect(received1).toEqual([42]);
      expect(received2).toEqual([42]);

      Process.exit(sub1, 'normal');
      Process.exit(sub2, 'normal');
    });

    test('publishing to topic with no subscribers does not error', async () => {
      const ps = await PubSub.start_link();
      PubSub.publish(ps, 'empty_topic', 'nobody');
      await sleep(5);
      // no error thrown
    });
  });

  describe('auto-cleanup on subscriber exit', () => {
    test('exited subscriber is removed from topic', async () => {
      const ps = await PubSub.start_link();

      const sub = Process.spawn(async () => {
        await Process.receive(); // stay alive until killed
      });
      await sleep(10);

      M.pushPid(sub);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      const subs1 = await PubSub.subscribers(ps, 'topic1');
      expect(subs1).toContain(sub);

      Process.exit(sub, 'normal');
      await sleep(20);

      const subs2 = await PubSub.subscribers(ps, 'topic1');
      expect(subs2).toEqual([]);
    });

    test('topic is cleaned up when last subscriber exits', async () => {
      const ps = await PubSub.start_link();

      const sub = Process.spawn(async () => {
        await Process.receive(); // stay alive until killed
      });
      await sleep(10);

      M.pushPid(sub);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      Process.exit(sub, 'normal');
      await sleep(20);

      const ts = await PubSub.topics(ps);
      expect(ts).toEqual([]);
    });
  });

  describe('topics / subscribers queries', () => {
    test('lists active topics', async () => {
      const ps = await PubSub.start_link();

      const sub = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(sub);
      PubSub.subscribe(ps, 'a');
      PubSub.subscribe(ps, 'b');
      M.popPid();
      await sleep(10);

      const ts = await PubSub.topics(ps);
      expect(ts.sort()).toEqual(['a', 'b']);
    });

    test('lists subscribers for a topic', async () => {
      const ps = await PubSub.start_link();

      const sub = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(sub);
      PubSub.subscribe(ps, 'topic1');
      M.popPid();
      await sleep(10);

      const subs = await PubSub.subscribers(ps, 'topic1');
      expect(subs).toEqual([sub]);
    });
  });

  describe('resolve by name', () => {
    test('accepts registered name for all operations', async () => {
      const ps = await PubSub.start_link({ name: 'named_ps' });
      const received: unknown[] = [];

      const sub = Process.spawn(async () => {
        while (true) {
          const msg = await Process.receive();
          if (PubSub.isPubSubMessage(msg)) received.push(msg.message);
        }
      });
      await sleep(10);

      M.pushPid(sub);
      PubSub.subscribe('named_ps', 'topic');
      M.popPid();
      await sleep(10);

      PubSub.publish('named_ps', 'topic', 'works');
      await sleep(10);

      expect(received).toEqual(['works']);

      const subs = await PubSub.subscribers('named_ps', 'topic');
      expect(subs).toContain(sub);

      Process.exit(sub, 'normal');
    });
  });
});
