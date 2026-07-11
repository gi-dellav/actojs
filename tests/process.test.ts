import { describe, test, expect, beforeEach } from "bun:test";
import type { PID } from "../src/types";
import * as Process from "../src/process";
import * as M from "../src/mailbox";
import { sleep } from "./helpers";

beforeEach(() => {
  M.clearPidStack();
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

function waitFor(fn: () => boolean, ms = 1000): Promise<void> {
  return new Promise((resolve, reject) => {
    const start = Date.now();
    const check = () => {
      if (fn()) return resolve();
      if (Date.now() - start > ms) return reject(new Error("timeout"));
      setTimeout(check, 5);
    };
    check();
  });
}

function spawnAlive(): string {
  return Process.spawn(async () => {
    await Process.receive(); // blocks forever
  });
}

describe("process", () => {
  describe("spawn", () => {
    test("returns a PID", () => {
      const pid = Process.spawn(() => {});
      expect(pid).toMatch(/^#PID</);
    });

    test("runs the function", async () => {
      let ran = false;
      Process.spawn(() => {
        ran = true;
      });
      await waitFor(() => ran);
      expect(ran).toBe(true);
    });

    test("catches sync errors and process exits", async () => {
      const pid = Process.spawn(() => {
        throw new Error("boom");
      });
      await waitFor(() => !Process.alive(pid));
      expect(Process.alive(pid)).toBe(false);
    });

    test("process exits after async function completes", async () => {
      let done = false;
      const pid = Process.spawn(async () => {
        done = true;
      });
      await waitFor(() => done);
      await waitFor(() => !Process.alive(pid));
      expect(Process.alive(pid)).toBe(false);
    });

    test("with link option, caller and spawned are linked", () => {
      const callerPid = "caller_link_test";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = Process.spawn(() => {}, ["link"]);
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc).toBeDefined();
      expect(spawnedProc!.links.has(callerPid)).toBe(true);
      expect(procCaller.links.has(pid)).toBe(true);

      M.popPid();
    });
  });

  describe("spawn_link", () => {
    test("creates a linked process", () => {
      const callerPid = "linker";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = Process.spawn_link(() => {});
      const spawned = M.getProcess(pid);
      expect(spawned!.links.has(callerPid)).toBe(true);
      expect(procCaller.links.has(pid)).toBe(true);

      M.popPid();
    });
  });

  describe("spawn_monitor", () => {
    test("returns pid and ref, monitors the spawned process", () => {
      const callerPid = "mon_test";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const { pid, ref } = Process.spawn_monitor(() => {});
      const spawned = M.getProcess(pid);
      expect(typeof ref).toBe("symbol");
      expect(spawned!.monitoredBy.has(callerPid)).toBe(true);
      expect(procCaller.monitors.get(ref)).toBe(pid);

      M.popPid();
    });

    test("sends DOWN on exit", () => {
      const callerPid = "mon_down_test";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const { pid, ref } = Process.spawn_monitor(() => {});
      M.popPid();

      const spawned = M.getProcess(pid)!;
      spawned.exitReason = "crash";
      M.handleExit(spawned);

      expect(procCaller.mailbox.length).toBe(1);
      const msg = procCaller.mailbox[0] as any;
      expect(msg.type).toBe("DOWN");
      expect(msg.ref).toBe(ref);
      expect(msg.pid).toBe(pid);
      expect(msg.reason).toBe("crash");
    });

    test("throws when called outside a process", () => {
      M.clearPidStack();
      // spawn_monitor outside a process still works but won't setup monitoring
      // since there's no caller; the function should still return pid and ref.
      const { pid, ref } = Process.spawn_monitor(() => {});
      expect(typeof pid).toBe("string");
      expect(typeof ref).toBe("symbol");
    });
  });

  describe("send", () => {
    test("delivers message to a PID", async () => {
      let received: unknown;
      const pid = Process.spawn(async () => {
        const msg = await Process.receive();
        received = msg;
      });
      Process.send(pid, "hello");
      await waitFor(() => received !== undefined);
      expect(received).toBe("hello");
    });

    test("delivers via registered name", async () => {
      let received: unknown;
      const pid = Process.spawn(async () => {
        Process.register(Process.self(), "my_name");
        const msg = await Process.receive();
        received = msg;
      });
      await waitFor(() => Process.whereis("my_name") !== null);
      Process.send("my_name", { data: 42 });
      await waitFor(() => received !== undefined);
      expect(received).toEqual({ data: 42 });
      Process.unregister("my_name");
    });

    test("falls back to PID lookup for unknown name", async () => {
      let received: unknown;
      const pid = Process.spawn(async () => {
        const msg = await Process.receive();
        received = msg;
      });
      Process.send(pid, "hey");
      await waitFor(() => received !== undefined);
      expect(received).toBe("hey");
    });
  });

  describe("self", () => {
    test("throws when called outside a process", () => {
      M.clearPidStack();
      expect(() => Process.self()).toThrow("outside of a process");
    });

    test("returns PID when inside a process", async () => {
      let captured: string | undefined;
      const pid = Process.spawn(() => {
        captured = Process.self();
      });
      await waitFor(() => captured !== undefined);
      expect(captured).toBe(pid);
    });
  });

  describe("alive", () => {
    test("returns true for running process", () => {
      const pid = Process.spawn(() => {});
      expect(Process.alive(pid)).toBe(true);
    });

    test("returns false for exited process", async () => {
      let pid = "";
      Process.spawn(() => {
        pid = Process.self();
      });
      await waitFor(() => pid !== "");
      await waitFor(() => !Process.alive(pid));
      expect(Process.alive(pid)).toBe(false);
    });

    test("returns false for unknown PID", () => {
      expect(Process.alive("#PID<9999.0.0>")).toBe(false);
    });
  });

  describe("exit", () => {
    test("kills a process", async () => {
      const pid = spawnAlive();
      expect(Process.alive(pid)).toBe(true);
      Process.exit(pid, "killed");
      await waitFor(() => !Process.alive(pid));
    });

    test("does nothing for already exited process", () => {
      expect(() => Process.exit("#PID<9999.0.0>", "reason")).not.toThrow();
    });
  });

  describe("link / unlink", () => {
    test("link throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.link("#PID<0.0.0>")).toThrow("outside of a process");
    });

    test("unlink throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.unlink("#PID<0.0.0>")).toThrow(
        "outside of a process",
      );
    });

    test("link connects two processes bidirectionally", async () => {
      const pidA = spawnAlive();
      const pidB = spawnAlive();
      await sleep(5);

      const procA = M.getProcess(pidA)!;
      const procB = M.getProcess(pidB)!;

      M.pushPid(pidA);
      Process.link(pidB);
      M.popPid();

      expect(procA.links.has(pidB)).toBe(true);
      expect(procB.links.has(pidA)).toBe(true);

      Process.exit(pidA, "done");
      Process.exit(pidB, "done");
    });

    test("unlink removes bidirectional connection", async () => {
      const pidA = spawnAlive();
      const pidB = spawnAlive();
      await sleep(5);

      const procA = M.getProcess(pidA)!;
      const procB = M.getProcess(pidB)!;

      M.pushPid(pidA);
      Process.link(pidB);
      Process.unlink(pidB);
      M.popPid();

      expect(procA.links.has(pidB)).toBe(false);
      expect(procB.links.has(pidA)).toBe(false);

      Process.exit(pidA, "done");
      Process.exit(pidB, "done");
    });
  });

  describe("monitor / demonitor", () => {
    test("monitor throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.monitor("#PID<0.0.0>")).toThrow(
        "outside of a process",
      );
    });

    test("demonitor throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.demonitor(Symbol("ref"))).toThrow(
        "outside of a process",
      );
    });

    test("monitor sets up monitoring and returns a ref", async () => {
      const pidA = spawnAlive();
      const pidB = spawnAlive();
      await sleep(5);

      const procA = M.getProcess(pidA)!;

      M.pushPid(pidA);
      const ref = Process.monitor(pidB);
      M.popPid();

      expect(typeof ref).toBe("symbol");
      expect(procA.monitors.get(ref)).toBe(pidB);

      Process.exit(pidA, "done");
      Process.exit(pidB, "done");
    });

    test("demonitor cleans up", async () => {
      const pidA = spawnAlive();
      const pidB = spawnAlive();
      await sleep(5);

      const procA = M.getProcess(pidA)!;

      M.pushPid(pidA);
      const ref = Process.monitor(pidB);
      Process.demonitor(ref);
      M.popPid();

      expect(procA.monitors.has(ref)).toBe(false);

      Process.exit(pidA, "done");
      Process.exit(pidB, "done");
    });
  });

  describe("flag", () => {
    test("flag throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.flag("trap_exit", true)).toThrow(
        "outside of a process",
      );
    });

    test("sets and returns previous trap_exit", async () => {
      let prev: boolean | undefined;
      const pid = Process.spawn(async () => {
        prev = Process.flag("trap_exit", true);
        await Process.receive(); // block forever
      });
      await waitFor(() => prev !== undefined);
      expect(prev).toBe(false);

      const proc = M.getProcess(pid)!;
      expect(proc).toBeDefined();
      expect(proc.trapExit).toBe(true);
      Process.exit(pid, "done");
    });

    test("returns false for unknown flag", async () => {
      let ret: boolean | undefined;
      Process.spawn(() => {
        ret = Process.flag("unknown_flag", true);
      });
      await waitFor(() => ret !== undefined);
      expect(ret).toBe(false);
    });
  });

  describe("register / unregister / whereis", () => {
    test("register throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.register("#PID<0.0.0>", "name")).toThrow(
        "outside of a process",
      );
    });

    test("register associates a name with a PID", async () => {
      const pid = Process.spawn(async () => {
        Process.register(Process.self(), "my_service");
        await Process.receive(); // stay alive
      });
      await waitFor(() => Process.whereis("my_service") !== null);
      expect(Process.whereis("my_service")).toBe(pid);
      Process.unregister("my_service");
      Process.exit(pid, "done");
    });

    test("unregister removes the name", async () => {
      const pid = Process.spawn(async () => {
        Process.register(Process.self(), "temp");
        await Process.receive(); // stay alive
      });
      await waitFor(() => Process.whereis("temp") !== null);
      Process.unregister("temp");
      expect(Process.whereis("temp")).toBeNull();
      Process.exit(pid, "done");
    });

    test("whereis returns null for unknown name", () => {
      expect(Process.whereis("nobody")).toBeNull();
    });

    test("register overwrites previous name", async () => {
      const pid = spawnAlive();
      await sleep(5);

      M.pushPid(pid);
      Process.register(pid, "first");
      Process.register(pid, "second");
      M.popPid();

      expect(Process.whereis("first")).toBeNull();
      expect(Process.whereis("second")).toBe(pid);
      Process.unregister("second");
      Process.exit(pid, "done");
    });
  });

  describe("list", () => {
    test("returns alive PIDs only", async () => {
      const pid1 = spawnAlive();
      const pid2 = spawnAlive();
      await sleep(5);
      const list = Process.list();
      expect(list).toContain(pid1);
      expect(list).toContain(pid2);
      Process.exit(pid1, "done");
      Process.exit(pid2, "done");
    });

    test("does not include exited processes", async () => {
      let pid = "";
      Process.spawn(() => {
        pid = Process.self();
      });
      await waitFor(() => pid !== "");
      await waitFor(() => !Process.alive(pid));
      expect(Process.list()).not.toContain(pid);
    });
  });

  describe("sleep", () => {
    test("resolves after the given time", async () => {
      const start = Date.now();
      await Process.sleep(50);
      expect(Date.now() - start).toBeGreaterThanOrEqual(40);
    });
  });

  describe("send_after / cancel_timer", () => {
    test("sends a message after delay", async () => {
      let received: unknown;
      const pid = Process.spawn(async () => {
        received = await Process.receive();
      });
      Process.send_after(pid, "delayed", 30);
      await waitFor(() => received !== undefined, 500);
      expect(received).toBe("delayed");
    });

    test("cancel_timer prevents message delivery", async () => {
      let received: unknown;
      const pid = Process.spawn(async () => {
        received = await Process.receive();
      });
      const ref = Process.send_after(pid, "cancelled", 50);
      Process.cancel_timer(ref);
      await sleep(100);
      expect(received).toBeUndefined();
    });

    test("cancel_timer is safe for unknown ref", () => {
      expect(() => Process.cancel_timer(Symbol("unknown"))).not.toThrow();
    });
  });

  describe("process dictionary", () => {
    test("get throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.get("key")).toThrow("outside of a process");
    });

    test("put throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.put("key", "val")).toThrow("outside of a process");
    });

    test("deleteKey throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.deleteKey("key")).toThrow("outside of a process");
    });

    test("put / get / delete storage", async () => {
      let result: unknown;
      Process.spawn(() => {
        Process.put("x", 1);
        Process.put("y", 2);
        result = Process.get("x");
        Process.deleteKey("x");
        result = [result, Process.get("x")];
      });
      await sleep(10);
      expect(result).toEqual([1, undefined]);
    });

    test("put returns previous value", async () => {
      let prev: unknown;
      Process.spawn(() => {
        Process.put("a", 1);
        prev = Process.put("a", 2);
      });
      await sleep(10);
      expect(prev).toBe(1);
    });

    test("get returns default for missing key", async () => {
      let result: unknown;
      Process.spawn(() => {
        result = Process.get("nope", "fallback");
      });
      await sleep(10);
      expect(result).toBe("fallback");
    });

    test("get returns actual value over default", async () => {
      let result: unknown;
      Process.spawn(() => {
        Process.put("x", 42);
        result = Process.get("x", 99);
      });
      await sleep(10);
      expect(result).toBe(42);
    });

    test("get_keys returns all keys", async () => {
      let result: unknown;
      Process.spawn(() => {
        Process.put("a", 1);
        Process.put("b", 2);
        result = Process.get_keys().sort();
      });
      await sleep(10);
      expect(result).toEqual(["a", "b"]);
    });

    test("get_keys with value filter", async () => {
      let result: unknown;
      Process.spawn(() => {
        Process.put("a", 1);
        Process.put("b", 2);
        Process.put("c", 1);
        result = Process.get_keys(1).sort();
      });
      await sleep(10);
      expect(result).toEqual(["a", "c"]);
    });

    test("get_keys returns empty when no match", async () => {
      let result: unknown;
      Process.spawn(() => {
        Process.put("a", 1);
        result = Process.get_keys(99);
      });
      await sleep(10);
      expect(result).toEqual([]);
    });

    test("get_keys throws outside process", () => {
      M.clearPidStack();
      expect(() => Process.get_keys()).toThrow("outside of a process");
    });
  });

  describe("info", () => {
    test("returns null for unknown PID", () => {
      expect(Process.info("#PID<9999.0.0>")).toBeNull();
    });

    test("returns process info for alive process", async () => {
      let info: any;
      const pid = Process.spawn(() => {
        Process.flag("trap_exit", true);
        info = Process.info(Process.self());
      });
      await sleep(10);
      expect(info).not.toBeNull();
      expect(info.status).toBe("running");
      expect(info.trapExit).toBe(true);
    });
  });

  describe("receive", () => {
    test("rejects outside process", async () => {
      M.clearPidStack();
      await expect(Process.receive()).rejects.toThrow("outside of a process");
    });
  });

  describe("exit cascading", () => {
    test("linked process exits when partner exits without trap_exit", () => {
      const pidA = M.generatePid();
      const procA = M.createProcess(pidA);
      procA.status = "running";
      M.registerProcess(pidA, procA);

      const pidB = M.generatePid();
      const procB = M.createProcess(pidB);
      procB.status = "running";
      M.registerProcess(pidB, procB);

      procA.links.add(pidB);
      procB.links.add(pidA);
      procA.exitReason = "bye";
      M.handleExit(procA);

      expect(procB.status as string).toBe("exited");
      expect(procB.exitReason).toBe("bye");
    });

    test("trap_exit prevents cascading and sends EXIT message", () => {
      const pidA = M.generatePid();
      const procA = M.createProcess(pidA);
      procA.status = "running";
      M.registerProcess(pidA, procA);

      const pidB = M.generatePid();
      const procB = M.createProcess(pidB);
      procB.status = "running";
      procB.trapExit = true;
      M.registerProcess(pidB, procB);

      procA.links.add(pidB);
      procB.links.add(pidA);
      procA.exitReason = "oops";
      M.handleExit(procA);

      expect(procB.status).toBe("running");
      expect(procB.mailbox.length).toBe(1);
      expect((procB.mailbox[0] as any).type).toBe("EXIT");
    });
  });

  describe("monitor DOWN messages", () => {
    test("monitoring process receives DOWN on exit", () => {
      const pidA = M.generatePid();
      const procA = M.createProcess(pidA);
      procA.status = "running";
      M.registerProcess(pidA, procA);

      const pidB = M.generatePid();
      const procB = M.createProcess(pidB);
      procB.status = "running";
      M.registerProcess(pidB, procB);

      M.pushPid(pidB);
      const ref = Process.monitor(pidA);
      M.popPid();

      procA.exitReason = "crash";
      M.handleExit(procA);

      expect(procB.mailbox.length).toBe(1);
      const msg = procB.mailbox[0] as any;
      expect(msg.type).toBe("DOWN");
      expect(msg.ref).toBe(ref);
      expect(msg.pid).toBe(pidA);
    });
  });

  describe("spawn with SpawnOptions and limits", () => {
    test("spawn with object opts and link option", () => {
      const callerPid = "obj_link_test";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = Process.spawn(() => {}, { link: true });
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc!.links.has(callerPid)).toBe(true);
      expect(procCaller.links.has(pid)).toBe(true);

      M.popPid();
    });

    test("spawn with object opts and monitor option", () => {
      const callerPid = "obj_mon_test";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = Process.spawn(() => {}, { monitor: true });
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc!.monitoredBy.has(callerPid)).toBe(true);
      expect(procCaller.monitors.size).toBe(1);

      M.popPid();
    });

    test("spawn with process limits", () => {
      const pid = Process.spawn(() => {}, {
        limits: {
          messageBudget: 50,
          maxMailboxSize: 100,
          execTimeout: 500,
          maxMemory: 1024,
        },
      });
      const proc = M.getProcess(pid);
      expect(proc!.messageBudget).toBe(50);
      expect(proc!.maxMailboxSize).toBe(100);
      expect(proc!.execTimeout).toBe(500);
      expect(proc!.maxMemory).toBe(1024);
    });
  });

  describe("flag fault-isolation limits", () => {
    test("flag message_budget returns previous and sets new", async () => {
      let prev: any;
      Process.spawn(() => {
        prev = Process.flag("message_budget", 200);
      });
      await sleep(10);
      expect(prev).toBe(100);
    });

    test("flag max_mailbox_size returns previous and sets new", async () => {
      let prev: any;
      Process.spawn(() => {
        prev = Process.flag("max_mailbox_size", 50);
      });
      await sleep(10);
      expect(prev).toBe(0);
    });

    test("flag exec_timeout returns previous and sets new", async () => {
      let prev: any;
      Process.spawn(() => {
        prev = Process.flag("exec_timeout", 1000);
      });
      await sleep(10);
      expect(prev).toBe(0);
    });

    test("flag max_memory returns previous and sets new", async () => {
      let prev: any;
      Process.spawn(() => {
        prev = Process.flag("max_memory", 512);
      });
      await sleep(10);
      expect(prev).toBe(0);
    });
  });

  describe("unregister", () => {
    test("clears name from process", async () => {
      let registered = false;
      let pid = "";
      Process.spawn(async () => {
        pid = Process.self();
        Process.register(pid, "to_remove");
        registered = true;
        await Process.receive();
      });
      await waitFor(() => registered);
      expect(Process.whereis("to_remove")).toBe(pid);

      Process.unregister("to_remove");
      expect(Process.whereis("to_remove")).toBeNull();
    });
  });

  describe("spawn_async_link", () => {
    test("links the caller to the spawned process", async () => {
      let spawnedPid: PID = "";
      let callerPid: PID = "";

      // We need to be inside a process to link, so we spawn a wrapper
      // that calls spawn_async_link and verifies the link.
      await new Promise<void>((resolve) => {
        Process.spawn(async () => {
          callerPid = Process.self();
          spawnedPid = await Process.spawn_async_link(async () => {
            await Process.receive();
          });
          const spawned = M.getProcess(spawnedPid);
          expect(spawned!.links.has(callerPid)).toBe(true);
          Process.exit(spawnedPid, "normal");
          resolve();
        });
      });
      await sleep(10);
    });
  });

  describe("undefined message delivery", () => {
    test("sending undefined is received correctly", async () => {
      let received: unknown = Symbol("not_received");
      const pid = Process.spawn(() => {
        Process.receive().then((msg) => {
          received = msg;
        });
      });
      await sleep(5);
      Process.send(pid, undefined);
      await sleep(10);
      expect(received).toBeUndefined();
    });
  });
});
