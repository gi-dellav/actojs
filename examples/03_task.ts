// example: Task — fire-and-forget async computation with await / yield
// run: bun run examples/03_task.ts

import * as Task from '../src/task';

// Run a task that does async work
const task = Task.async(async () => {
  console.log('[task] computing...');
  await new Promise(r => setTimeout(r, 200));
  return { result: 42 };
});

console.log('[main] task started, doing other work...');

// yield: non-blocking poll — returns null while task is pending
let poll = await Task.yield_(task);
console.log(`[main] first yield: ${JSON.stringify(poll)}`); // null (still running)

// await: blocks until task completes (with optional timeout)
try {
  const result = await Task.await_(task, 5000);
  console.log(`[main] task result: ${JSON.stringify(result)}`);
} catch (err) {
  console.error('[main] task failed:', err);
}

// yield after completion returns the value
poll = await Task.yield_(task);
console.log(`[main] final yield: ${JSON.stringify(poll)}`);

console.log('[done]');
