// example: Supervisor — static child supervisor with restart strategies
// run: bun run examples/05_supervisor.ts

import * as Supervisor from "../src/supervisor";
import * as Process from "../src/process";

// Stable worker: blocks forever waiting for messages
const stableWorker = {
  start_link() {
    const pid = Process.spawn(async () => {
      Process.register(Process.self(), "stable_worker");
      console.log("[stable] ready");
      for (;;) {
        const msg = await Process.receive();
        if (msg && typeof msg === "object" && (msg as any).type === "ping") {
          console.log("[stable] received ping");
          Process.send((msg as any).replyTo, { type: "pong" });
        } else if (
          msg &&
          typeof msg === "object" &&
          (msg as any).type === "stop"
        ) {
          console.log("[stable] stopping");
          break;
        }
      }
    });
    return { ok: pid };
  },
};

// Start supervisor with a single child
console.log("[main] starting supervisor with one child...");
const { ok: sup } = await Supervisor.start_link(
  [{ id: "stable", start: [stableWorker, "start_link", []] }],
  { strategy: "one_for_one" },
);
console.log("[main] supervisor started");

// Query children
const counts = await Supervisor.count_children(sup);
console.log(`[main] children: ${counts.active} active`);

// Send a ping to the stable worker
const worker = Process.whereis("stable_worker");
if (worker) {
  const caller = Process.spawn(async () => {
    Process.send(worker, { type: "ping", replyTo: Process.self() });
    const reply = await Process.receive(2000);
    console.log(`[caller] got: ${JSON.stringify(reply)}`);
  });
}

// Dynamically add a second child with start_child
const extraWorker = {
  start_link() {
    const pid = Process.spawn(async () => {
      console.log("[extra] started");
      await Process.receive(); // block
    });
    return { ok: pid };
  },
};

await Process.sleep(200);

const startResult = await Supervisor.start_child(sup, {
  id: "extra",
  start: [extraWorker, "start_link", []],
});
if ("ok" in startResult) {
  console.log(`[main] added extra child: ${startResult.ok}`);
}

// Count again
const counts2 = await Supervisor.count_children(sup);
console.log(
  `[main] children: ${counts2.specs} specs, ${counts2.active} active`,
);

// List children details
const children = await Supervisor.which_children(sup);
for (const c of children) {
  console.log(
    `[main]   child=${c.id} pid=${c.pid} alive=${Process.alive(c.pid)}`,
  );
}

// Terminate and delete the extra child
await Supervisor.terminate_child(sup, "extra");
await Supervisor.delete_child(sup, "extra");
console.log("[main] terminated and deleted extra child");

const counts3 = await Supervisor.count_children(sup);
console.log(`[main] children after cleanup: ${counts3.active} active`);

await Supervisor.stop(sup);
console.log("[done]");
