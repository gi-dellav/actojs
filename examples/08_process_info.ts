// example: Process introspection — info, name registry, timers, process dictionary
// run: bun run examples/07_process_info.ts

import * as Process from "../src/process";

// 1. Spawn a worker and inspect it
const worker = Process.spawn(async () => {
  Process.register(Process.self(), "my_worker");
  await Process.receive(); // block forever
});

await Process.sleep(50);

// Inspect process info
const info = Process.info(worker);
console.log("[info] worker process:");
console.log(`  status:     ${info?.status}`);
console.log(`  mailbox:    ${info?.messageQueueLength} messages`);
console.log(`  name:       ${info?.registeredName}`);
console.log(`  msg budget: ${info?.messageBudget}`);
console.log(`  trap_exit:  ${info?.trapExit}`);
console.log(`  links:      ${info?.links?.length}`);
console.log(`  monitors:   ${info?.monitors?.length}`);

// 2. Name registry
const byName = Process.whereis("my_worker");
console.log(`\n[lookup] whereis('my_worker') = ${byName}`);
console.log(`  same as worker? ${byName === worker}`);

// 3. List all alive processes
console.log(`\n[list] alive processes: ${Process.list().length}`);

// 4. Timers: send_after and cancel_timer
const timerRef = Process.send_after(worker, { type: "timer_fired" }, 200);
console.log(`\n[timer] scheduled message for 200ms (ref: ${String(timerRef)})`);

// Cancel before it fires
Process.cancel_timer(timerRef);
console.log(`[timer] cancelled`);

// Send another timer we let fire
Process.send_after(worker, { type: "delayed" }, 250);
console.log(`[timer] scheduled another for 250ms`);

await Process.sleep(300);
console.log(
  `[timer] worker mailbox size: ${Process.info(worker)?.messageQueueLength}`,
);

// 5. Process dictionary
const dictPid = Process.spawn(async () => {
  Process.put("role", "worker");
  Process.put("started", Date.now().toString());

  const role = Process.get("role");
  console.log(`\n[dict] role = ${role}`);
  console.log(`[dict] started = ${Process.get("started")}`);

  const prev = Process.put("role", "leader");
  console.log(`[dict] role: ${prev} → ${Process.get("role")}`);

  Process.deleteKey("role");
  console.log(`[dict] role after delete: ${Process.get("role")}`);
});

await Process.sleep(100);

// 6. Clean up
Process.exit(worker, "shutdown");
await Process.sleep(50);

console.log("\n[done]");
