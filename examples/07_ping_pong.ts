// example: Ping-Pong — actor message passing
// run: bun run examples/07_ping_pong.ts
//
// NOTE: raw Process.spawn with async functions has a known pid-stack issue
// when multiple actors communicate bidirectionally. This example uses
// GenServer-backed Agent actors which correctly track PIDs.

import * as Agent from '../src/agent';
import * as Process from '../src/process';

// Ping actor: sends pings to a pong actor and waits for replies
const { ok: pingPid } = await Agent.start_link(() => ({ pong: null as string | null }));

// Pong actor: receives pings and replies with pongs
const { ok: pongPid } = await Agent.start_link(() => ({ count: 0 }));

// Set up: ping knows pong's PID
await Agent.update(pingPid, _state => ({ pong: pongPid }));

// Pong's "handler": we use Agent.update to simulate receiving a ping
// Real actors would use Process.receive() but the pid-stack bug prevents that here
for (let i = 1; i <= 3; i++) {
  console.log(`[ping] pinging #${i}`);

  // Atomically: read pong's count, increment, and return old+new
  const result = await Agent.get_and_update(pongPid, state => {
    const newCount = state.count + 1;
    console.log(`[pong] got ping #${i}, count: ${state.count} → ${newCount}`);
    return [{ old: state.count, new: newCount }, { count: newCount }];
  });

  console.log(`[ping] got pong (count was ${result.old}, now ${result.new})`);
}

// Clean up
await Agent.stop(pongPid);
await Agent.stop(pingPid);

console.log('[done]');
