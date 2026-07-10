// example: DynamicSupervisor — dynamically added children at runtime
// run: bun run examples/06_dynamic_supervisor.ts

import * as DynamicSupervisor from '../src/dynamic_supervisor';
import * as Process from '../src/process';

// Child module: a simple worker
function makeWorker(name: string) {
  return {
    start_link() {
      const pid = Process.spawn(async () => {
        console.log(`[${name}] started`);
        await Process.receive(); // block forever
        console.log(`[${name}] shutting down`);
      });
      return { ok: pid };
    },
  };
}

// Start a dynamic supervisor (starts with zero children)
const { ok: dynSup } = await DynamicSupervisor.start_link({
  strategy: 'one_for_one',
  max_children: 10,
  max_restarts: 3,
  max_seconds: 5,
});

console.log('[main] dynamic supervisor started');

// Dynamically add children one at a time
for (let i = 1; i <= 3; i++) {
  const worker = makeWorker(`worker-${i}`);
  const result = await DynamicSupervisor.start_child(dynSup, {
    id: `w${i}`,
    start: [worker, 'start_link', []],
  });
  if ('ok' in result) {
    console.log(`[main] added ${'worker-' + i}: ${result.ok}`);
  }
}

// Count children
const counts = await DynamicSupervisor.count_children(dynSup);
console.log(`[count_children] specs=${counts.specs} active=${counts.active}`);

// List children
const children = await DynamicSupervisor.which_children(dynSup);
console.log(`[which_children] ${children.length} children`);
for (const c of children) {
  console.log(`  pid=${c.pid} alive=${Process.alive(c.pid)}`);
}

// Terminate a child by PID
const firstChild = children[0]!.pid;
await DynamicSupervisor.terminate_child(dynSup, firstChild);
console.log(`[terminate_child] ${firstChild} terminated`);

await Process.sleep(200);

const counts2 = await DynamicSupervisor.count_children(dynSup);
console.log(`[count_children after terminate] specs=${counts2.specs} active=${counts2.active}`);

await DynamicSupervisor.stop(dynSup);
console.log('[done]');
