// example: basic spawn, send, receive
// run: bun run examples/01_hello_world.ts

import * as Process from "../src/process";

// Spawn a process that waits for messages
const pid = Process.spawn(async () => {
  console.log(`[worker] PID: ${Process.self()}`);

  const msg1 = await Process.receive(5000);
  console.log(`[worker] received: ${JSON.stringify(msg1)}`);

  const msg2 = await Process.receive(5000);
  console.log(`[worker] received: ${JSON.stringify(msg2)}`);

  const msg3 = await Process.receive(5000);
  console.log(`[worker] received: ${JSON.stringify(msg3)}`);

  console.log("[worker] done");
});

// Give the worker time to start
await Process.sleep(100);

console.log(`[main] sending 3 messages to ${pid}`);
Process.send(pid, { type: "greet", text: "hello" });
Process.send(pid, { type: "greet", text: "world" });
Process.send(pid, { type: "farewell", text: "goodbye" });

// Wait for worker to finish
await Process.sleep(500);

console.log(`[main] worker alive: ${Process.alive(pid)}`);
console.log("[main] done");
