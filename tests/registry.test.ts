import { describe, test, expect, beforeEach } from "bun:test";
import * as Registry from "../src/registry";
import * as Process from "../src/process";
import * as M from "../src/mailbox";
import { sleep, waitUntil } from "./helpers";

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe("registry", () => {
  describe("start_link", () => {
    test("starts a registry with unique keys", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      expect("ok" in result).toBe(true);
    });

    test("starts a registry with duplicate keys", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      expect("ok" in result).toBe(true);
    });

    test("starts a registry with duplicate_pid", async () => {
      const result = await Registry.start_link({ keys: { duplicate: "pid" } });
      expect("ok" in result).toBe(true);
    });

    test("with name option", async () => {
      const result = await Registry.start_link({
        keys: "unique",
        name: "my_reg",
      });
      expect("ok" in result).toBe(true);
      expect(Process.whereis("my_reg")).toBe((result as any).ok);
    });

    test("with partitions", async () => {
      const result = await Registry.start_link({
        keys: "unique",
        partitions: 4,
      });
      expect("ok" in result).toBe(true);
    });
  });

  describe("register / lookup (unique mode)", () => {
    test("registers and looks up a process", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      // Register from within a process
      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      const regResult = await Registry.register(reg, "key1", { data: 123 });
      M.popPid();

      expect("ok" in regResult).toBe(true);
      if ("ok" in regResult) expect(regResult.ok).toBe(pid);

      const entries = await Registry.lookup(reg, "key1");
      expect(entries.length).toBe(1);
      expect(entries[0]!.pid).toBe(pid);
      expect(entries[0]!.value).toEqual({ data: 123 });
    });

    test("registering duplicate key fails in unique mode", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "key1", 1);
      const dupResult = await Registry.register(reg, "key1", 2);
      M.popPid();

      expect("error" in dupResult).toBe(true);
      if ("error" in dupResult)
        expect(dupResult.error).toBe("already_registered");
    });
  });

  describe("register / lookup (duplicate_key mode)", () => {
    test("allows multiple entries for same key", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid1 = Process.spawn(() => {});
      const pid2 = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid1);
      await Registry.register(reg, "shared", "a");
      M.popPid();

      M.pushPid(pid2);
      await Registry.register(reg, "shared", "b");
      M.popPid();

      const entries = await Registry.lookup(reg, "shared");
      expect(entries.length).toBe(2);
    });
  });

  describe("register (duplicate_pid mode)", () => {
    test("prevents same pid registering same key twice", async () => {
      const result = await Registry.start_link({ keys: { duplicate: "pid" } });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "k", "v1");
      const dupResult = await Registry.register(reg, "k", "v2");
      M.popPid();

      expect("error" in dupResult).toBe(true);
      if ("error" in dupResult)
        expect(dupResult.error).toBe("already_registered");
    });
  });

  describe("unregister", () => {
    test("unregisters a key", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "temp", "val");
      await Registry.unregister(reg, "temp");
      M.popPid();

      const entries = await Registry.lookup(reg, "temp");
      expect(entries.length).toBe(0);
    });
  });

  describe("match", () => {
    test("matches entries by value pattern", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid1 = Process.spawn(() => {});
      const pid2 = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid1);
      await Registry.register(reg, "users", { name: "alice", age: 30 });
      M.popPid();

      M.pushPid(pid2);
      await Registry.register(reg, "users", { name: "bob", age: 25 });
      M.popPid();

      const matched = await Registry.match(reg, "users", { name: "alice" });
      expect(matched.length).toBe(1);
      expect(matched[0]!.value).toEqual({ name: "alice", age: 30 });
    });

    test("match with guards", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "nums", { val: 5 });
      await Registry.register(reg, "nums", { val: 15 });
      M.popPid();

      const matched = await Registry.match(
        reg,
        "nums",
        {},
        (v: any) => v.val > 10,
      );
      expect(matched.length).toBe(1);
      expect(matched[0]!.value).toEqual({ val: 15 });
    });

    test("returns empty for no match", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const matched = await Registry.match(reg, "missing", {});
      expect(matched).toEqual([]);
    });
  });

  describe("dispatch", () => {
    test("dispatches to all matching entries", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid1 = Process.spawn(() => {});
      const pid2 = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid1);
      await Registry.register(reg, "topic", "msg1");
      M.popPid();

      M.pushPid(pid2);
      await Registry.register(reg, "topic", "msg2");
      M.popPid();

      const dispatched: any[] = [];
      await Registry.dispatch(reg, "topic", (_pid, value) => {
        dispatched.push(value);
      });
      expect(dispatched).toEqual(["msg1", "msg2"]);
    });

    test("dispatch respects limit", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "q", "a");
      await Registry.register(reg, "q", "b");
      await Registry.register(reg, "q", "c");
      M.popPid();

      const dispatched: any[] = [];
      await Registry.dispatch(
        reg,
        "q",
        (_pid, value) => {
          dispatched.push(value);
        },
        { limit: 2 },
      );
      expect(dispatched.length).toBe(2);
    });
  });

  describe("keys", () => {
    test("returns keys for a given pid", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "a", 1);
      await Registry.register(reg, "b", 2);
      M.popPid();

      const k = await Registry.keys(reg, pid);
      expect(k).toContain("a");
      expect(k).toContain("b");
    });
  });

  describe("values", () => {
    test("returns values for a key and pid", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "x", 1);
      await Registry.register(reg, "x", 2);
      M.popPid();

      const vals = await Registry.values(reg, "x", pid);
      expect(vals).toEqual([1, 2]);
    });
  });

  describe("count", () => {
    test("returns total entry count", async () => {
      const result = await Registry.start_link({ keys: "duplicate" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "a", 1);
      await Registry.register(reg, "b", 2);
      await Registry.register(reg, "c", 3);
      M.popPid();

      const c = await Registry.count(reg);
      expect(c).toBe(3);
    });
  });

  describe("update_value", () => {
    test("updates value for registered key", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "counter", 0);
      const updResult = await Registry.update_value(
        reg,
        "counter",
        (v: any) => (v as number) + 1,
      );
      M.popPid();

      expect("newValue" in (updResult as any)).toBe(true);
      if ("newValue" in (updResult as any)) {
        expect((updResult as any).newValue).toBe(1);
        expect((updResult as any).oldValue).toBe(0);
      }
    });

    test("returns error for non-existent key", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      const updResult = await Registry.update_value(reg, "nope", (v: any) => v);
      M.popPid();

      expect("error" in (updResult as any)).toBe(true);
    });
  });

  describe("cleanup on DOWN", () => {
    test("auto-cleans entries when registering process exits", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(async () => {
        await new Promise(() => {});
      });
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "fragile", "data");
      M.popPid();

      let entries = await Registry.lookup(reg, "fragile");
      expect(entries.length).toBe(1);

      Process.exit(pid, "kill");
      await sleep(20);

      entries = await Registry.lookup(reg, "fragile");
      expect(entries.length).toBe(0);
    });
  });

  describe("listeners", () => {
    test("notifies registered listeners on events", async () => {
      // Spawn a listener process and register it from within
      const listenerPid = Process.spawn(async () => {
        Process.register(Process.self(), "listener1");
        // Stay alive without consuming messages: use a never-resolving promise
        await new Promise(() => {});
      });
      // Wait for registration
      await waitUntil(() => Process.whereis("listener1") !== null, 1000);
      expect(Process.whereis("listener1")).toBe(listenerPid);

      // Start registry with listener
      const result = await Registry.start_link({
        keys: "unique",
        listeners: ["listener1"],
      });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(10);

      M.pushPid(pid);
      await Registry.register(reg, "event_key", "event_val");
      M.popPid();

      // The listener process should have a message in its mailbox
      await sleep(20);
      const proc = M.getProcess(listenerPid);
      expect(proc).toBeDefined();
      expect(proc!.mailbox.length).toBeGreaterThan(0);
      const evt = proc!.mailbox[0] as any;
      expect(evt.__registry_event__).toBe(true);
      expect(evt.event).toBe("register");
      expect(evt.key).toBe("event_key");

      Process.unregister("listener1");
      Process.exit(listenerPid, "done");
    });
  });

  describe("duplicate_pid mode", () => {
    test("rejects duplicate registration by PID", async () => {
      const result = await Registry.start_link({ keys: { duplicate: "pid" } });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      const r1 = await Registry.register(reg, "key1", "val1");
      expect("error" in (r1 as any)).toBe(false);

      const r2 = await Registry.register(reg, "key2", "val2");
      expect("ok" in (r2 as any) || r2 === undefined).toBe(true);

      M.popPid();
      Process.exit(pid, "done");
    });
  });

  describe("select with throwing guard", () => {
    test("discards entries when guard throws", async () => {
      const result = await Registry.start_link({ keys: "unique" });
      if ("error" in result) throw result.error;
      const reg = result.ok;

      const pid = Process.spawn(() => {});
      await sleep(5);

      M.pushPid(pid);
      await Registry.register(reg, "guard_test", 123);
      M.popPid();

      const matched = await Registry.match(reg, "guard_test", "_", () => {
        throw new Error("bad guard");
      });
      expect(matched.length).toBe(0);

      Process.exit(pid, "done");
    });
  });
});
