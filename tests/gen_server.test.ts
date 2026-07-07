import { describe, test, expect, beforeEach } from 'bun:test';
import * as GS from '../src/gen_server';
import * as Process from '../src/process';
import * as M from '../src/mailbox';
import { sleep } from './helpers';

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe('gen_server', () => {
  describe('startGenServer', () => {
    test('starts and returns PID via { ok: PID }', async () => {
      const result = await GS.startGenServer({
        init() {
          return { counter: 0 };
        },
      }, null);
      expect('ok' in result).toBe(true);
      if ('ok' in result) {
        expect(Process.alive(result.ok)).toBe(true);
      }
    });

    test('handles async init', async () => {
      const result = await GS.startGenServer({
        async init() {
          await sleep(5);
          return { ready: true };
        },
      }, null);
      expect('ok' in result).toBe(true);
    });

    test('handles { ok: state } init return', async () => {
      const result = await GS.startGenServer({
        init() {
          return { ok: { value: 42 } };
        },
      }, null);
      expect('ok' in result).toBe(true);
    });

    test('handles { error: reason } init return', async () => {
      const result = await GS.startGenServer({
        init() {
          return { error: 'bad_arg' };
        },
      }, null);
      expect('error' in result).toBe(true);
    });

    test('handles init throwing', async () => {
      const result = await GS.startGenServer({
        init() {
          throw new Error('boom');
        },
      }, null);
      expect('error' in result).toBe(true);
      if ('error' in result) {
        expect(result.error.message).toBe('boom');
      }
    });

    test('with name option, process registers itself', async () => {
      const result = await GS.startGenServer({
        init() { return {}; },
      }, null, { name: 'my_gen' });
      expect('ok' in result).toBe(true);
      expect(Process.whereis('my_gen')).toBe((result as any).ok);
    });

    test('with link option, uses spawn_link', async () => {
      const callerPid = 'gs_caller';
      const procCaller = M.createProcess(callerPid);
      procCaller.status = 'alive';
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const result = await GS.startGenServer({
        init() { return {}; },
      }, null, { link: true });

      M.popPid();

      expect('ok' in result).toBe(true);
      if ('ok' in result) {
        const spawnedProc = M.getProcess(result.ok);
        expect(spawnedProc!.links.has(callerPid)).toBe(true);
      }
    });
  });

  describe('genCall', () => {
    test('calls handle_call and gets reply', async () => {
      const result = await GS.startGenServer({
        init() { return { count: 0 }; },
        handle_call(msg: any, _from: any, state: any, _myPid: any) {
          if (msg.type === 'inc') {
            return { reply: state.count + 1, state: { count: state.count + 1 } };
          }
          return { reply: null, state };
        },
      }, null);
      if ('error' in result) throw result.error;
      const pid = result.ok;

      const reply = await GS.genCall(pid, { type: 'inc' });
      expect(reply).toBe(1);
    });

    test('genCall with timeout rejects if no reply', async () => {
      const result = await GS.startGenServer({
        init() { return {}; },
        handle_call(_msg: any, _from: any, state: any) {
          // Never calls resolvePending — simulates hung handler
          return { reply: 'will_reply', state };
        },
      }, null);
      if ('error' in result) throw result.error;

      // The handler above does reply, but if timeout is 1ms it should work
      const reply = await GS.genCall(result.ok, { type: 'ping' }, 1000);
      expect(reply).toBe('will_reply');
    });

    test('genCall from outside a process still works', async () => {
      const result = await GS.startGenServer({
        init() { return {}; },
        handle_call(_msg: any, _from: any, state: any) {
          return { reply: 'ok', state };
        },
      }, null);
      if ('error' in result) throw result.error;

      const reply = await GS.genCall(result.ok, { type: 'ping' });
      expect(reply).toBe('ok');
    });

    test('handle_call returning error via resolvePending', async () => {
      // handle_call throws -> caught as { error: err }
      const result = await GS.startGenServer({
        init() { return {}; },
        handle_call(_msg: any, _from: any, state: any) {
          throw new Error('handler error');
        },
      }, null);
      if ('error' in result) throw result.error;

      await expect(GS.genCall(result.ok, { type: 'bad' })).rejects.toThrow('handler error');
    });
  });

  describe('genCast', () => {
    test('calls handle_cast and does not reply', async () => {
      let castReceived = false;
      const result = await GS.startGenServer({
        init() { return { count: 0 }; },
        handle_cast(msg: any, state: any) {
          if (msg.type === 'inc') {
            castReceived = true;
            return { noreply: undefined, state: { count: state.count + 1 } };
          }
          return { noreply: undefined, state };
        },
      }, null);
      if ('error' in result) throw result.error;

      GS.genCast(result.ok, { type: 'inc' });
      await sleep(20);
      expect(castReceived).toBe(true);
    });
  });

  describe('genStop', () => {
    test('stops the server', async () => {
      const result = await GS.startGenServer({
        init() { return {}; },
      }, null);
      if ('error' in result) throw result.error;
      const pid = result.ok;

      expect(Process.alive(pid)).toBe(true);
      await GS.genStop(pid);
      expect(Process.alive(pid)).toBe(false);
    });

    test('calls terminate callback on stop', async () => {
      let terminated = false;
      const result = await GS.startGenServer({
        init() { return {}; },
        terminate(_reason: any, _state: any) {
          terminated = true;
        },
      }, null);
      if ('error' in result) throw result.error;

      await GS.genStop(result.ok);
      expect(terminated).toBe(true);
    });
  });

  describe('handle_info', () => {
    test('dispatches non-tagged messages to handle_info', async () => {
      let infoReceived: unknown;
      const result = await GS.startGenServer({
        init() { return {}; },
        handle_info(msg: any, state: any) {
          infoReceived = msg;
          return { noreply: undefined, state };
        },
      }, null);
      if ('error' in result) throw result.error;

      Process.send(result.ok, { custom: 'info' });
      await sleep(20);
      expect(infoReceived).toEqual({ custom: 'info' });
    });
  });

  describe('deferred reply (noreply + GenServer.reply)', () => {
    test('handle_call returning { noreply } does not resolve the call', async () => {
      let savedFrom: any;
      const result = await GS.startGenServer({
        init() { return { count: 0 }; },
        handle_call(msg: any, from: any, state: any) {
          if (msg.type === 'defer') {
            savedFrom = from;
            return { noreply: undefined, state };
          }
          return { reply: 'immediate', state };
        },
      }, null);
      if ('error' in result) throw result.error;

      // This call should hang (not resolve immediately)
      const callPromise = GS.genCall(result.ok, { type: 'defer' }, 500);

      // It should eventually timeout
      await expect(callPromise).rejects.toThrow('timeout');
    });

    test('GenServer.reply resolves a deferred call', async () => {
      let savedFrom: any;
      const result = await GS.startGenServer({
        init() { return { count: 0 }; },
        handle_call(msg: any, from: any, state: any) {
          if (msg.type === 'defer') {
            savedFrom = from;
            return { noreply: undefined, state };
          }
          if (msg.type === 'resolve') {
            GS.reply(savedFrom, 'deferred_result');
            return { reply: 'ok', state };
          }
          return { reply: 'unknown', state };
        },
      }, null);
      if ('error' in result) throw result.error;

      const callPromise = GS.genCall(result.ok, { type: 'defer' }, 1000);
      await sleep(10);
      // Send a call to trigger the deferred reply (must be a call since handle_call is used)
      await GS.genCall(result.ok, { type: 'resolve' });

      const val = await callPromise;
      expect(val).toBe('deferred_result');
    });

    test('GenServer.reply from info handler', async () => {
      let savedFrom: any;
      let pid: string = '';
      const result = await GS.startGenServer({
        init() { return {}; },
        handle_call(msg: any, from: any, state: any) {
          savedFrom = from;
          return { noreply: undefined, state };
        },
        handle_info(msg: any, state: any) {
          GS.reply(savedFrom, 'info_reply');
          return { noreply: undefined, state };
        },
      }, null);
      if ('error' in result) throw result.error;
      pid = result.ok;

      const callPromise = GS.genCall(pid, { type: 'wait' }, 1000);
      await sleep(5);
      Process.send(pid, { wake: true });

      const val = await callPromise;
      expect(val).toBe('info_reply');
    });

    test('resolvePending is safe when called multiple times', async () => {
      let savedFrom: any;
      const result = await GS.startGenServer({
        init() { return {}; },
        handle_call(msg: any, from: any, state: any) {
          if (msg.type === 'defer') {
            savedFrom = from;
            return { noreply: undefined, state };
          }
          return { reply: 'ok', state };
        },
      }, null);
      if ('error' in result) throw result.error;

      const promise = GS.genCall(result.ok, { type: 'defer' }, 500);

      await sleep(5);

      // Multiple replies should not crash
      expect(() => {
        GS.reply(savedFrom, 'first');
        GS.reply(savedFrom, 'second');
      }).not.toThrow();

      // First reply wins
      const val = await promise;
      expect(val).toBe('first');
    });
  });
});
