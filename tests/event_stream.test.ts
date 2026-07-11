import { describe, test, expect, beforeEach } from "bun:test";
import * as EventStream from "../src/event_stream";
import * as Process from "../src/process";
import * as M from "../src/mailbox";
import { sleep } from "./helpers";

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe("event_stream", () => {
  describe("receive", () => {
    test("yields messages via for-await-of", async () => {
      const received: unknown[] = [];
      const procRan = Promise.withResolvers<void>();

      const pid = Process.spawn(async () => {
        for await (const msg of EventStream.receive()) {
          received.push(msg);
          if (msg === "stop") break;
        }
        procRan.resolve();
      });
      await sleep(10);

      Process.send(pid, "hello");
      await sleep(5);
      Process.send(pid, "world");
      await sleep(5);
      Process.send(pid, "stop");
      await sleep(10);

      expect(received).toEqual(["hello", "world", "stop"]);
    });

    test("stops when process exits", async () => {
      const procRan = Promise.withResolvers<void>();

      const pid = Process.spawn(async () => {
        for await (const _msg of EventStream.receive()) {
          // never breaks, should stop when process exits
        }
        procRan.resolve();
      });
      await sleep(10);

      Process.exit(pid, "normal");
      await procRan.promise;

      // If we get here without hanging, the loop stopped
    });

    test("works in a linked process", async () => {
      const procRan = Promise.withResolvers<void>();

      const pid = Process.spawn(async () => {
        for await (const _msg of EventStream.receive()) {
          // just consume
        }
        procRan.resolve();
      });
      await sleep(10);

      Process.exit(pid, "normal");
      await procRan.promise;
    });
  });
});
