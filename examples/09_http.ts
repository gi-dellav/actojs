// example: HTTP server + client with Bun.serve and fetch, coordinated via actors
// run: bun run examples/09_http.ts

import * as Process from '../src/process';
import * as Agent from '../src/agent';

const PORT = 3030;

// Agent that stores request logs. This will be updated by the server actor
// whenever a request arrives, and queried by the client after fetching.
const { ok: logAgent } = await Agent.start_link<{ requests: number; body: string[] }>(
  () => ({ requests: 0, body: [] }),
);

// Server actor: starts Bun.serve, forwards incoming requests to the log agent,
// and keeps running until it receives a 'stop' message.
const serverPid = Process.spawn(async () => {
  const server = Bun.serve({
    port: PORT,
    async fetch(req) {
      const body = await req.text();
      console.log(`[server] received ${req.method} ${req.url} body="${body}"`);

      Agent.cast(logAgent, (state: { requests: number; body: string[] }) => ({
        requests: state.requests + 1,
        body: [...state.body, body],
      }));

      return new Response(JSON.stringify({ ok: true, echo: body }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    },
  });

  console.log(`[server] listening on ${server.url}`);

  await Process.receive(10_000);
  server.stop(true);
  console.log('[server] stopped');
});

await Process.sleep(100);

// Client actor: sends HTTP requests via fetch, then reads the log agent
const clientPid = Process.spawn(async () => {
  const url = `http://localhost:${PORT}`;

  // POST JSON
  const res1 = await fetch(`${url}/api/hello`, {
    method: 'POST',
    body: JSON.stringify({ message: 'Hello from fetch' }),
    headers: { 'Content-Type': 'application/json' },
  });
  const data1 = await res1.json();
  console.log(`[client] POST /api/hello → ${JSON.stringify(data1)}`);

  // GET
  const res2 = await fetch(`${url}/api/status`);
  const data2 = await res2.json();
  console.log(`[client] GET /api/status → ${JSON.stringify(data2)}`);

  // PUT plain text
  const res3 = await fetch(`${url}/api/items`, {
    method: 'PUT',
    body: 'plain text body',
  });
  const data3 = await res3.json();
  console.log(`[client] PUT /api/items → ${JSON.stringify(data3)}`);

  await Process.sleep(100);

  const logState = await Agent.get(logAgent, s => s);
  console.log(`[client] server handled ${logState.requests} requests`);
  console.log(`[client] bodies received: ${JSON.stringify(logState.body)}`);
});

// Wait for client to finish, then stop the server
await Process.sleep(2000);
Process.send(serverPid, 'stop');
await Process.sleep(500);

await Agent.stop(logAgent);
console.log('[done]');
