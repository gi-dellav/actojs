// example: Agent — state-holding actor with get/update/get_and_update/cast
// run: bun run examples/02_agent.ts

import * as Agent from '../src/agent';

// Start an agent with initial state { count: 0 }
const { ok: agent } = await Agent.start_link(() => ({ count: 0 }));

// get: read state without changing it
const before = await Agent.get(agent, state => state);
console.log(`[initial] count = ${before.count}`);

// update: change state
await Agent.update(agent, state => ({ ...state, count: state.count + 10 }));
console.log(`[after update] count = ${(await Agent.get(agent, s => s)).count}`);

// get_and_update: atomic read + write
const oldCount = await Agent.get_and_update(agent, state => {
  const old = state.count;
  const next = { ...state, count: state.count + 5 };
  return [old, next];
});
console.log(`[get_and_update] old count = ${oldCount}, new count = ${(await Agent.get(agent, s => s)).count}`);

// cast: fire-and-forget update (no reply)
Agent.cast(agent, state => ({ ...state, count: state.count * 2 }));
await new Promise(r => setTimeout(r, 50));
console.log(`[after cast] double = ${(await Agent.get(agent, s => s)).count}`);

await Agent.stop(agent);
console.log('[done]');
