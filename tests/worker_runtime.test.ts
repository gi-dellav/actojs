import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { WorkerRuntime, detectEnv } from "../src/worker_runtime";
import { getRuntime, setRuntime, WebRuntime } from "../src/core";
import * as Process from "../src/process";
import * as M from "../src/mailbox";
import { sleep } from "./helpers";

const wr = new WorkerRuntime();

beforeEach(() => {
  M.clearPidStack();
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

afterEach(() => {
  setRuntime(new WebRuntime());
  wr.stop();
  M.clearPidStack();
});

// Helper: create a main-thread "listener" process that collects messages.
function createListener(): string {
  const pid = M.generatePid();
  const proc = M.createProcess(pid);
  proc.status = "running";
  M.registerProcess(pid, proc);
  return pid;
}

describe("WorkerRuntime", () => {
  describe("environment detection", () => {
    test("returns a known environment", () => {
      const env = detectEnv();
      expect(["bun", "deno", "browser"]).toContain(env);
    });
  });

  describe("constructor", () => {
    test("has a name matching the environment", () => {
      const env = detectEnv();
      const w = new WorkerRuntime();
      if (env === "bun") expect(w.name).toBe("bun-worker");
      else if (env === "deno") expect(w.name).toBe("deno-worker");
      else if (env === "browser") expect(w.name).toBe("web-worker");
    });

    test("available is true when Worker is supported", () => {
      expect(wr.available).toBe(true);
    });
  });

  describe("spawn", () => {
    test("returns a PID", () => {
      const pid = wr.spawn(() => {});
      expect(pid).toMatch(/^#PID</);
    });

    test("runs the function in a worker (communicates via Wr.send)", async () => {
      const listener = createListener();
      wr.spawn(
        (Wr: any, targetPid: string) => {
          Wr.send(targetPid, "ok");
        },
        undefined,
        [listener],
      );
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe("ok");
    });

    test("process exits after completion", async () => {
      const pid = wr.spawn(() => {});
      await sleep(200);
      const proc = M.getProcess(pid);
      expect(proc?.status === "exited" || !proc).toBeTruthy();
    });

    test("catches sync errors", async () => {
      const pid = wr.spawn(() => {
        throw new Error("boom");
      });
      await sleep(100);
      const proc = M.getProcess(pid);
      if (proc) {
        expect(proc.status === "exiting" || proc.status === "exited").toBe(
          true,
        );
      }
    });

    test("args are passed to the function", async () => {
      const listener = createListener();
      wr.spawn(
        (Wr: any, target: string, x: number, y: number) => {
          Wr.send(target, x + y);
        },
        undefined,
        [listener, 3, 4],
      );
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe(7);
    });

    test("spawn with link option links processes", () => {
      const callerPid = "caller_link_test";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = wr.spawn(() => {}, ["link"]);
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc).toBeDefined();
      expect(spawnedProc!.links.has(callerPid)).toBe(true);
      expect(procCaller.links.has(pid)).toBe(true);

      M.popPid();
    });

    test("spawn with monitor option monitors the process", () => {
      const callerPid = M.generatePid();
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "running";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const pid = wr.spawn(() => {}, ["monitor"]);
      const spawnedProc = M.getProcess(pid);
      expect(spawnedProc).toBeDefined();
      expect(spawnedProc!.monitoredBy.has(callerPid)).toBe(true);

      M.popPid();
      wr.stop();
    });
  });

  describe("message routing", () => {
    test("delivers messages to worker processes", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const msg = await Wr.receive();
          Wr.send(targetPid, msg);
        },
        undefined,
        [listener],
      );
      wr.deliver(pid, "hello");
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe("hello");
    });

    test("worker can send messages to a main-thread process", async () => {
      const listener = createListener();
      wr.spawn(
        (Wr: any, targetPid: string) => {
          Wr.send(targetPid, "from_worker");
        },
        undefined,
        [listener],
      );
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe("from_worker");
    });

    test("worker-to-worker messaging works", async () => {
      const listener = createListener();
      const pidA = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const msg = await Wr.receive();
          Wr.send(targetPid, msg);
        },
        undefined,
        [listener],
      );

      // Wait a tick so pidA's worker is ready, then spawn pidB
      await sleep(20);
      wr.spawn(
        (Wr: any, destPid: string) => {
          Wr.send(destPid, "hello_from_B");
        },
        undefined,
        [pidA],
      );

      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe("hello_from_B");
    });
  });

  describe("process primitives in worker", () => {
    test("self() returns the assigned PID", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        (Wr: any, targetPid: string) => {
          Wr.send(targetPid, Wr.self());
        },
        undefined,
        [listener],
      );
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe(pid);
    });

    test("sleep() works in worker", async () => {
      const listener = createListener();
      wr.spawn(
        async (Wr: any, targetPid: string) => {
          const start = Date.now();
          await Wr.sleep(50);
          Wr.send(targetPid, Date.now() - start);
        },
        undefined,
        [listener],
      );
      await sleep(200);
      const elapsed = M.shiftMessage(listener) as number;
      expect(elapsed).toBeGreaterThanOrEqual(40);
    });

    test("send with PID target works from worker", async () => {
      const listener = createListener();
      wr.spawn(
        (Wr: any, targetPid: string) => {
          Wr.send(targetPid, { data: 42 });
        },
        undefined,
        [listener],
      );
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toEqual({ data: 42 });
    });
  });

  describe("sys_call routing", () => {
    test("worker can register a name", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          await Wr.register("test_svc");
          Wr.send(targetPid, "done");
          // Keep alive so the registration persists for the check
          await Wr.receive();
        },
        undefined,
        [listener],
      );
      await sleep(200);
      expect(M.whereisName("test_svc")).toBe(pid);
      M.unregisterName("test_svc");
      // Unblock and terminate
      wr.deliver(pid, "exit");
      await sleep(50);
    });

    test("worker can flag trap_exit", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const prev = await Wr.flag("trap_exit", true);
          Wr.send(targetPid, prev);
          // Keep alive so we can check the process state
          await Wr.receive();
        },
        undefined,
        [listener],
      );
      await sleep(200);
      const prev = M.shiftMessage(listener);
      expect(prev).toBe(false);
      const proc = M.getProcess(pid);
      expect(proc?.trapExit).toBe(true);
      wr.deliver(pid, "exit");
      await sleep(50);
    });

    test("worker can get/put process dictionary", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const prevPut = await Wr.put("x", 10);
          const val = await Wr.get("x");
          Wr.send(targetPid, [prevPut, val]);
        },
        undefined,
        [listener],
      );
      await sleep(100);
      const [prevPut, val] = M.shiftMessage(listener) as [unknown, unknown];
      expect(prevPut).toBeUndefined();
      expect(val).toBe(10);
    });

    test("worker can check alive", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const alive = await Wr.alive(Wr.self());
          Wr.send(targetPid, alive);
          await Wr.receive();
        },
        undefined,
        [listener],
      );
      await sleep(200);
      const isAlive = M.shiftMessage(listener);
      expect(isAlive).toBe(true);
      wr.deliver(pid, "exit");
      await sleep(50);
    });

    test("worker can get info", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const info = await Wr.info(Wr.self());
          Wr.send(targetPid, info);
          await Wr.receive();
        },
        undefined,
        [listener],
      );
      await sleep(200);
      const info = M.shiftMessage(listener) as any;
      expect(info).not.toBeNull();
      expect(info.status).toBe("running");
      wr.deliver(pid, "exit");
      await sleep(50);
    });

    test("worker can list processes", async () => {
      const listener = createListener();
      const pid = wr.spawn(
        async (Wr: any, targetPid: string) => {
          const list = await Wr.list();
          Wr.send(targetPid, list);
          await Wr.receive();
        },
        undefined,
        [listener],
      );
      await sleep(200);
      const list = M.shiftMessage(listener) as string[];
      expect(list).toBeDefined();
      expect(list).toContain(pid);
      wr.deliver(pid, "exit");
      await sleep(50);
    });
  });

  describe("stop", () => {
    test("terminates all workers", async () => {
      const pid1 = wr.spawn(async (Wr: any) => {
        await Wr.receive(); // block forever
      });
      const pid2 = wr.spawn(async (Wr: any) => {
        await Wr.receive(); // block forever
      });
      await sleep(200);

      wr.stop();

      await sleep(100);
      const procs = [pid1, pid2].map((p) => M.getProcess(p));
      for (const p of procs) {
        if (p) {
          expect(p.status === "exited" || p.status === "exiting").toBe(true);
        }
      }
    });
  });

  describe("integration with Process.spawn", () => {
    test("Process.spawn delegates to WorkerRuntime when {worker:true}", async () => {
      setRuntime(wr);
      const listener = createListener();
      // Build a function that has the listener PID embedded as a string literal,
      // since closures don't survive worker serialization.
      const body = `
        const Wr = globalThis.Wr;
        if (Wr) Wr.send('${listener}', 'ran');
      `;
      // eslint-disable-next-line no-new-func
      const fn = new Function(body) as () => void;
      const pid = Process.spawn(fn, { worker: true });
      await sleep(100);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe("ran");
      expect(pid).toMatch(/^#PID</);
    });

    test("Process.spawn returns correct PID with link option and worker", () => {
      setRuntime(wr);
      const callerPid = "caller_integration";
      const proc = M.createProcess(callerPid);
      proc.status = "running";
      M.registerProcess(callerPid, proc);
      M.pushPid(callerPid);

      const pid = Process.spawn(() => {}, { link: true, worker: true });
      expect(pid).toMatch(/^#PID</);
      const spawned = M.getProcess(pid);
      expect(spawned!.links.has(callerPid)).toBe(true);

      M.popPid();
    });

    test("Process.spawn falls back to WebRuntime when no WorkerRuntime", async () => {
      setRuntime(new WebRuntime());
      const listener = createListener();
      // With WebRuntime, closures work — direct variable capture is OK
      Process.spawn(() => {
        const Wr = (globalThis as any).Wr;
        if (!Wr) {
          // We're on the main thread (WebRuntime), use Process.send
          Process.send(listener, "ran_web");
        }
      });
      await sleep(20);
      const msg = M.shiftMessage(listener);
      expect(msg).toBe("ran_web");
    });

    test("Process.spawn does NOT delegate to WorkerRuntime without {worker:true}", async () => {
      setRuntime(wr);
      const listener = createListener();
      // Without {worker:true}, the function runs in the main thread.
      let ran = false;
      Process.spawn(() => { ran = true; });
      await sleep(30);
      expect(ran).toBe(true);
    });
  });
});
