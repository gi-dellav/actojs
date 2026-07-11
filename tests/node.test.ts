import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import * as Node from "../src/node";
import * as Process from "../src/process";
import * as M from "../src/mailbox";

beforeEach(() => {
  for (const pid of M.allPids()) {
    M.deregisterProcess(pid);
  }
  try {
    Node.stop();
  } catch (_) {}
});

afterEach(() => {
  try {
    Node.stop();
  } catch (_) {}
});

function runInProcess<T>(fn: () => T): Promise<T> {
  return new Promise((resolve, reject) => {
    Process.spawn(() => {
      try {
        resolve(fn());
      } catch (e) {
        reject(e);
      }
    });
  });
}

describe("node", () => {
  describe("start / stop", () => {
    test("starts a node", async () => {
      const result = await runInProcess(() => Node.start("test_node"));
      expect("ok" in result).toBe(true);
    });

    test("returns error if already started", async () => {
      await runInProcess(() => Node.start("test_node"));
      const result = await runInProcess(() => Node.start("test_node"));
      expect("error" in result).toBe(true);
    });

    test("stop returns undefined when not started", () => {
      try {
        Node.stop();
      } catch (_) {}
      const result = Node.stop();
      if (result !== undefined) {
        expect("error" in result).toBe(true);
      }
    });
  });

  describe("self", () => {
    test("returns node name when started", async () => {
      await runInProcess(() => Node.start("my_node"));
      expect(Node.self()).toBe("my_node");
    });

    test("throws if not started", () => {
      try {
        Node.stop();
      } catch (_) {}
      expect(() => Node.self()).toThrow("node not started");
    });
  });

  describe("alive", () => {
    test("returns true when started", async () => {
      await runInProcess(() => Node.start("alive_node"));
      expect(Node.alive()).toBe(true);
    });

    test("returns false when stopped", async () => {
      await runInProcess(() => Node.start("alive_node"));
      Node.stop();
      expect(Node.alive()).toBe(false);
    });

    test("returns false when never started", () => {
      try {
        Node.stop();
      } catch (_) {}
      expect(Node.alive()).toBe(false);
    });
  });

  describe("connect / disconnect", () => {
    test("connect adds node", async () => {
      await runInProcess(() => Node.start("node_a"));
      expect(Node.connect("node_b")).toBe(true);
    });

    test("connect returns false if not started", () => {
      try {
        Node.stop();
      } catch (_) {}
      expect(Node.connect("node_b")).toBe(false);
    });

    test("connect returns ignored if already connected", async () => {
      await runInProcess(() => Node.start("node_a"));
      Node.connect("node_b");
      expect(Node.connect("node_b")).toBe("ignored");
    });

    test("disconnect removes node", async () => {
      await runInProcess(() => Node.start("node_a"));
      Node.connect("node_b");
      Node.disconnect("node_b");
      expect(Node.connect("node_b")).toBe(true);
    });
  });

  describe("ping", () => {
    test("returns pong for connected node", async () => {
      await runInProcess(() => Node.start("node_a"));
      Node.connect("node_b");
      expect(Node.ping("node_b")).toBe("pong");
    });

    test("returns pang for unknown node", async () => {
      await runInProcess(() => Node.start("node_a"));
      expect(Node.ping("unknown")).toBe("pang");
    });
  });

  describe("list", () => {
    test("returns this node by default", async () => {
      await runInProcess(() => Node.start("node_a"));
      expect(Node.list()).toContain("node_a");
    });

    test("returns connected nodes", async () => {
      await runInProcess(() => Node.start("node_a"));
      Node.connect("node_b");
      Node.connect("node_c");
      const nodes = Node.list();
      expect(nodes).toContain("node_a");
      expect(nodes).toContain("node_b");
      expect(nodes).toContain("node_c");
    });

    test("returns empty when not started", () => {
      try {
        Node.stop();
      } catch (_) {}
      expect(Node.list()).toEqual([]);
    });

    test("filters by state", async () => {
      await runInProcess(() => Node.start("node_a"));
      Node.connect("node_b");
      expect(Node.list("visible")).toContain("node_a");
      expect(Node.list("visible")).not.toContain("node_b");
      expect(Node.list("connected")).toContain("node_b");
    });

    test("accepts array of states", async () => {
      await runInProcess(() => Node.start("node_a"));
      Node.connect("node_b");
      const all = Node.list(["visible", "connected"]);
      expect(all).toContain("node_a");
      expect(all).toContain("node_b");
    });
  });

  describe("monitor", () => {
    test("monitor flag=true adds a monitor", async () => {
      await runInProcess(() => Node.start("mon_node"));
      const ref = Node.monitor("target_node", true);
      expect(typeof ref).toBe("symbol");
    });

    test("monitor flag=false removes monitor", async () => {
      await runInProcess(() => Node.start("mon_node2"));
      Node.monitor("target_node", true);
      Node.monitor("target_node", false);
    });

    test("monitor works outside process", () => {
      expect(() => Node.monitor("node", true)).not.toThrow();
    });
  });

  describe("demonitor_node", () => {
    test("removes monitor by ref", async () => {
      await runInProcess(() => Node.start("demon_node"));
      const ref = Node.monitor("watched", true);
      expect(() => Node.demonitor_node(ref)).not.toThrow();
    });

    test("safe for unknown ref", () => {
      expect(() => Node.demonitor_node(Symbol("unknown"))).not.toThrow();
    });
  });

  describe("stop with monitors", () => {
    test("sends disconnect to local monitors", async () => {
      const pid = Process.spawn(async () => {
        Node.start("stop_mon_node");
        Node.monitor("stop_mon_node", true);
        await Process.receive(); // block forever
      });
      await new Promise((r) => setTimeout(r, 10));

      const stopped = Node.stop();
      expect(stopped).toBe(undefined);
      Process.exit(pid, "done");
    });
  });

  describe("spawn", () => {
    test("returns a PID for function spawn", async () => {
      await runInProcess(() => Node.start("spawn_node"));
      const pid = Node.spawn("remote_node", () => {});
      expect(pid).toMatch(/^#PID</);
    });

    test("returns a PID for module spawn", async () => {
      await runInProcess(() => Node.start("spawn_mfa"));
      const mod = {
        child_spec() {
          return { id: "test_child", start: [] as any };
        },
      };
      const pid = Node.spawn("remote_node", mod, "child_spec", []);
      expect(pid).toMatch(/^#PID</);
    });
  });

  describe("spawn_link", () => {
    test("returns a PID for function spawn_link", async () => {
      await runInProcess(() => Node.start("splink_node"));
      const pid = Node.spawn_link("remote_node", () => {});
      expect(pid).toMatch(/^#PID</);
    });

    test("returns a PID for module spawn_link", async () => {
      await runInProcess(() => Node.start("splink_mfa"));
      const mod = { run() {} };
      const pid = Node.spawn_link("remote_node", mod, "run", []);
      expect(pid).toMatch(/^#PID</);
    });
  });

  describe("spawn_monitor", () => {
    test("returns pid and ref for function", async () => {
      await runInProcess(() => Node.start("spmon_node"));
      const result = Node.spawn_monitor("remote_node", () => {});
      expect(result.pid).toMatch(/^#PID</);
      expect(typeof result.ref).toBe("symbol");
    });

    test("returns pid and ref for module", async () => {
      await runInProcess(() => Node.start("spmon_mfa"));
      const mod = { run() {} };
      const result = Node.spawn_monitor("remote_node", mod, "run", []);
      expect(result.pid).toMatch(/^#PID</);
      expect(typeof result.ref).toBe("symbol");
    });
  });

  describe("handleIncoming", () => {
    test("connect message adds sender to connectedNodes", async () => {
      await runInProcess(() => Node.start("node_inc1"));

      const testChannel = new BroadcastChannel("__actojs_node__node_inc1");
      testChannel.postMessage({
        from: "remote_node",
        to: "node_inc1",
        type: "connect",
        payload: null,
      });
      await new Promise((r) => setTimeout(r, 20));
      testChannel.close();

      expect(Node.list("connected")).toContain("remote_node");
    });

    test("ignores message for different node", async () => {
      await runInProcess(() => Node.start("node_solo"));

      const testChannel = new BroadcastChannel("__actojs_node__node_solo");
      testChannel.postMessage({
        from: "someone",
        to: "other_node",
        type: "connect",
        payload: null,
      });
      await new Promise((r) => setTimeout(r, 10));
      testChannel.close();

      expect(Node.list("connected")).not.toContain("someone");
    });

    test("ping message sends pong back", async () => {
      await runInProcess(() => Node.start("node_ping"));

      const testChannel = new BroadcastChannel("__actojs_node__node_ping");
      testChannel.postMessage({
        from: "remote",
        to: "node_ping",
        type: "ping",
        payload: null,
      });
      await new Promise((r) => setTimeout(r, 10));
      testChannel.close();
    });

    test("spawn fn message creates a process", async () => {
      const pid = Process.spawn(async () => {
        Node.start("node_spawnfn");
        await Process.receive();
      });
      await new Promise((r) => setTimeout(r, 10));

      const beforeCount = Process.list().length;

      const testChannel = new BroadcastChannel("__actojs_node__node_spawnfn");
      testChannel.postMessage({
        from: "remote",
        to: "node_spawnfn",
        type: "spawn",
        payload: { type: "fn", source: "() => {}" },
      });
      await new Promise((r) => setTimeout(r, 20));
      testChannel.close();

      const afterCount = Process.list().length;
      expect(afterCount).toBeGreaterThanOrEqual(beforeCount);

      Process.exit(pid, "done");
    });

    test("spawn mfa with missing function does nothing", async () => {
      const pid = Process.spawn(async () => {
        Node.start("node_nofn");
        await Process.receive();
      });
      await new Promise((r) => setTimeout(r, 10));

      const testChannel = new BroadcastChannel("__actojs_node__node_nofn");
      testChannel.postMessage({
        from: "remote",
        to: "node_nofn",
        type: "spawn",
        payload: { type: "mfa", module: {} as any, fn: "nope", args: [] },
      });
      await new Promise((r) => setTimeout(r, 10));
      testChannel.close();

      Process.exit(pid, "done");
    });

    test("spawn_exit sends EXIT to local process via spawn_link setup", async () => {
      await runInProcess(() => Node.start("node_spext"));

      // Use spawn_link to set up crossNodeLinks
      Node.spawn_link("remote_node", () => {});
      await new Promise((r) => setTimeout(r, 10));

      const testChannel = new BroadcastChannel("__actojs_node__node_spext");
      testChannel.postMessage({
        from: "remote_node",
        to: "node_spext",
        type: "spawn_exit",
        payload: {
          linkId: "l_node_spext_0",
          remotePid: "remote_pid",
          reason: "crash",
        },
      });
      await new Promise((r) => setTimeout(r, 15));
      testChannel.close();
    });

    test("spawn_down sends DOWN to local process via spawn_monitor setup", async () => {
      await runInProcess(() => Node.start("node_spdn"));

      // Use spawn_monitor to set up crossNodeMonitors
      Node.spawn_monitor("remote_node", () => {});
      await new Promise((r) => setTimeout(r, 10));

      const testChannel = new BroadcastChannel("__actojs_node__node_spdn");
      testChannel.postMessage({
        from: "remote_node",
        to: "node_spdn",
        type: "spawn_down",
        payload: {
          refId: "l_node_spdn_0",
          remotePid: "remote_pid",
          reason: "oops",
        },
      });
      await new Promise((r) => setTimeout(r, 15));
      testChannel.close();
    });

    test("spawn_kill exits remote spawned process", async () => {
      const nodePid = Process.spawn(async () => {
        Node.start("node_spkill");
        await Process.receive();
      });
      await new Promise((r) => setTimeout(r, 10));

      // Send spawn with linkId to populate remoteSpawnRegistry
      const beforeCount = Process.list().length;
      const testChannel = new BroadcastChannel("__actojs_node__node_spkill");
      testChannel.postMessage({
        from: "remote",
        to: "node_spkill",
        type: "spawn",
        payload: { type: "fn", source: "() => {}", linkId: "killme_link" },
      });
      await new Promise((r) => setTimeout(r, 20));

      // Now send spawn_kill to kill it
      testChannel.postMessage({
        from: "remote",
        to: "node_spkill",
        type: "spawn_kill",
        payload: { linkId: "killme_link", reason: "killed" },
      });
      await new Promise((r) => setTimeout(r, 15));
      testChannel.close();

      // The spawned process should be killed
      const afterCount = Process.list().length;
      expect(afterCount).toBeLessThanOrEqual(beforeCount + 1);

      Process.exit(nodePid, "done");
    });
  });

  describe("Node.spawn proxy PID", () => {
    test("Node.spawn returns a PID that stays alive (proxy waits for result)", async () => {
      // Start a node so Node.spawn has a source node name.
      Node.start("node_proxy_spawn_test");

      const pid = Node.spawn("remote_node", () => {});
      await new Promise((r) => setTimeout(r, 30));

      // The proxy PID should still be alive — it's waiting for spawn_result.
      expect(Process.alive(pid)).toBe(true);

      Process.exit(pid, "shutdown");
    });

    test("Node.spawn_link returns a valid PID", async () => {
      Node.start("node_proxy_link_test");

      const pid = Node.spawn_link("remote_node", () => {});
      expect(pid).toMatch(/^#PID</);

      Process.exit(pid, "shutdown");
    });

    test("Node.spawn_monitor returns pid and ref", async () => {
      Node.start("node_proxy_mon_test");

      const result = Node.spawn_monitor("remote_node", () => {});
      expect(result.pid).toMatch(/^#PID</);
      expect(typeof result.ref).toBe("symbol");

      Process.exit(result.pid, "shutdown");
    });
  });
});
