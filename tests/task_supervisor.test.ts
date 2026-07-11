import { describe, test, expect, beforeEach } from "bun:test";
import * as TaskSupervisor from "../src/task_supervisor";
import * as Task from "../src/task";
import * as Process from "../src/process";
import * as M from "../src/mailbox";
import { sleep } from "./helpers";

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
});

describe("TaskSupervisor", () => {
  describe("start_link", () => {
    test("starts a supervisor process", async () => {
      const result = await TaskSupervisor.start_link();
      expect("ok" in result).toBe(true);
      if ("ok" in result) {
        expect(Process.alive(result.ok)).toBe(true);
      }
    });

    test("with name option, registers the supervisor", async () => {
      const result = await TaskSupervisor.start_link({ name: "my_task_sup" });
      expect("ok" in result).toBe(true);
      expect(Process.whereis("my_task_sup")).toBe((result as any).ok);
    });
  });

  describe("async", () => {
    test("starts a task and returns a handle that can be awaited", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const handle = await TaskSupervisor.async(sup, async () => 42);
      const result = await Task.await_(handle);
      expect(result).toBe(42);
    });

    test("links the task to the caller (behavioural)", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();

      const callerPid = "ts_caller";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "alive";
      procCaller.trapExit = true;
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const handle = await TaskSupervisor.async(sup, async () => {
        await sleep(500);
      });
      M.popPid();

      // Verify the task is alive and linked
      const taskProc = M.getProcess(handle.pid);
      expect(taskProc).toBeDefined();
      expect(taskProc!.links.has(callerPid)).toBe(true);

      // And the reverse link exists
      expect(procCaller.links.has(handle.pid)).toBe(true);
    });

    test("propagates errors via the handle", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const handle = await TaskSupervisor.async(sup, async () => {
        throw new Error("task_failed");
      });
      await expect(Task.await_(handle)).rejects.toThrow("task_failed");
    });

    test("rejects when supervisor is dead", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      Process.exit(sup, "kill");
      await sleep(10);

      // async should throw because the supervisor is dead
      await expect(
        TaskSupervisor.async(sup, async () => 1, { timeout: 500 }),
      ).rejects.toThrow();
    });
  });

  describe("async_nolink", () => {
    test("starts a task without linking to caller", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();

      const callerPid = "ts_nolink_caller";
      const procCaller = M.createProcess(callerPid);
      procCaller.status = "alive";
      M.registerProcess(callerPid, procCaller);
      M.pushPid(callerPid);

      const handle = await TaskSupervisor.async_nolink(sup, async () => {
        await sleep(500);
      });
      M.popPid();

      const proc = M.getProcess(handle.pid);
      expect(proc!.links.has(callerPid)).toBe(false);
    });

    test("returns a handle that can be awaited", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const handle = await TaskSupervisor.async_nolink(sup, async () => 99);
      const result = await Task.await_(handle);
      expect(result).toBe(99);
    });
  });

  describe("start_child", () => {
    test("starts a fire-and-forget task", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      let ran = false;
      const result = await TaskSupervisor.start_child(sup, async () => {
        ran = true;
      });
      expect("ok" in result).toBe(true);
      await sleep(20);
      expect(ran).toBe(true);
    });

    test("child appears in children()", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const { ok: pid } = await TaskSupervisor.start_child(sup, async () => {
        await sleep(500);
      });
      const kids = await TaskSupervisor.children(sup);
      expect(kids).toContain(pid);
    });
  });

  describe("children", () => {
    test("returns empty array when no children", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const kids = await TaskSupervisor.children(sup);
      expect(kids).toEqual([]);
    });

    test("returns list of PIDs", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const { ok: pid1 } = await TaskSupervisor.start_child(sup, async () => {
        await sleep(500);
      });
      const { ok: pid2 } = await TaskSupervisor.start_child(sup, async () => {
        await sleep(500);
      });
      const kids = await TaskSupervisor.children(sup);
      expect(kids.length).toBe(2);
      expect(kids).toContain(pid1);
      expect(kids).toContain(pid2);
    });
  });

  describe("terminate_child", () => {
    test("kills a child by PID", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const { ok: pid } = await TaskSupervisor.start_child(sup, async () => {
        await sleep(500);
      });
      expect(Process.alive(pid)).toBe(true);
      await TaskSupervisor.terminate_child(sup, pid);
      await sleep(10);
      expect(Process.alive(pid)).toBe(false);
    });

    test("returns error for unknown PID", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      const result = await TaskSupervisor.terminate_child(sup, "#PID<999.0.0>");
      expect(result).toEqual({ error: "not_found" });
    });
  });

  describe("stop", () => {
    test("stops supervisor and all children", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      await TaskSupervisor.start_child(sup, async () => {
        await sleep(500);
      });
      await TaskSupervisor.stop(sup);
      await sleep(50);
      // The supervisor process should be dead after stop.
      expect(Process.alive(sup)).toBe(false);
    });
  });

  describe("count_children", () => {
    test("returns counts", async () => {
      const { ok: sup } = await TaskSupervisor.start_link();
      await TaskSupervisor.start_child(sup, async () => {
        await sleep(500);
      });
      const counts = await TaskSupervisor.count_children(sup);
      expect(counts.active).toBeGreaterThanOrEqual(1);
    });
  });
});
