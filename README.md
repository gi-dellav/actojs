# actojs

Elixir-inspired actor concurrency for TypeScript; based on a coooperative event-loop scheduler (with optional parallelism support), it implements the [actor model](https://en.wikipedia.org/wiki/Actor_model), allowing for reliable, parallel and scalable applications.

```ts
import * as Process from "actojs/process";
import * as Agent from "actojs/agent";

const { ok: pid } = await Agent.start(() => 0);
await Agent.cast(pid, (s) => s + 1);
const value = await Agent.get(pid, (s) => s);
console.log(value); // 1
```

## Why Actors?

The actor model eliminates entire categories of concurrency bugs. Each actor is a sequential island of state with no shared memory, no locks and no data races.

Actors communicate only through asynchronous message passing, which means:
https://elixir-lang.org/blog/2020/09/24/paas-with-elixir-at-Heroku/
- **No deadlocks** from lock ordering
- **No race conditions** on shared mutable state
- **Fault isolation**, as one actor crashing can't corrupt another
- **Location transparency**, as the same API is used whether actors run locally, across tabs or on different server-side runtimes

*actojs* brings these guarantees to TypeScript with a familiar Elixir-inspired API, running entirely on the cooperative event loop (with support for Worker-based parallelism), allowing for reliable parallelism.

One of the great advantages of Actors is abstracting the code needed for scaling the same logic across multiple threads; an example could be a software providing AI Agents, where every AI Agent can be coded as its own Actor, and the failure of one Actor can be managed by the Supervisor, without breaking all other running threads; the same logic can be applied to backends where different database connectors can run in parallel, or to complex frontend applications where multiple independnent components can run at the same time.

Here are some interesting articles on the usage of the actor model in production environments (they talk about Elixir, but the same patterns are closely implemented by *actojs*): [When to use the Actor model](https://medium.com/@goyalarchana17/when-to-use-the-actor-model-in-software-development-key-scenarios-for-scalability-and-resilience-dfd048407c64), [Discord's Blog post on Elixir usage for their backend](https://discord.com/blog/tracing-discords-elixir-systems-without-melting-everything), [Heroku's usage of Elixir in production](https://elixir-lang.org/blog/2020/09/24/paas-with-elixir-at-Heroku/) and [Elixir for AI Agents](https://elixirator.com/blog/elixir-for-ai-agents/)


## Design

- **Flexible setup.** It ships both with a cooperative scheduler for single-loop applications and specific Runtime implementations for native multi-threaded applications.
- **Pluggable runtimes.** `WebRuntime` (default) for browsers; `WorkerRuntime` for Web Workers; `NodeRuntime` for Node.js.
- **Zero runtime dependencies**, allowing for great security and lower disk usage
- **Low memory overhead**; by using functional applicators instead of classes and objects, *actojs* requires almost no memory overhead
- **>90% test coverage**, allowing for real production usage comparable to Elixir.
- **PID as string.** `#PID<0.N.0>`: allows for human-readable and serialisable references
- **GenServer backbone.** Agent, Registry, Supervisor, DynamicSupervisor, PubSub, and TaskSupervisor are all built on the same internal GenServer framework, following Elixir's API; this allows for simpler migration from Elixir to *actojs* and viceversa.

## Concepts

**Process** — lightweight cooperative actor identified by a `#PID<0.N.0>` string. Spawn with `Process.spawn(fn)`, communicate via `send`/`receive`.

**Runtime** — pluggable execution backend. `WebRuntime` (default, cooperative event-loop for browsers), `WorkerRuntime` (Web Workers), `NodeRuntime` (Node.js). Swap with `setRuntime()`.

**Node** — distributed process communication across tabs/workers within the same origin via `BroadcastChannel`. Supports remote spawn, cross-node linking/monitoring, and message forwarding.

**Linking & monitoring** — `link` creates a bidirectional exit cascade (if one dies, the linked process exits or receives an `EXIT` message). `monitor` creates a unidirectional watch (`DOWN` message on exit).

**Agent** — GenServer-backed actor holding immutable state. `get`/`update`/`get_and_update` are synchronous calls; `cast` is fire-and-forget.

**Mailbox** — per-process ordered message queue. `receive()` blocks (via Promise) until a message arrives; messages arriving while the process is busy are queued. Supports receive timeouts and per-process message budget tracking.

**Registry** — decentralised key-value process store with hash-ring partitioning. Supports `unique`, `duplicate_key`, and `duplicate_pid` key modes. Registered processes are monitored and auto-removed on exit.

**Task** — run an async computation in a process, `await_` or `yield_` its result. Tasks support timeouts and concurrent awaiters.

**Limits** — per-process caps on message budget, mailbox size, execution time, and memory (RSS); supervisor restart-rate throttling; per-call timeouts on receive, genCall, and task await.

**Supervisor** — "let it crash" error recovery. Define child specs, choose a strategy. Restart rate limiting prevents infinite loops (default: 3 restarts / 5 seconds).


## Modules

| Module | Import | What it does |
|---|---|---|
| **Process** | `actojs/process` | `spawn`, `send`, `receive`, `link`, `monitor`, `exit`, timers |
| **Agent** | `actojs/agent` | State-holding actor: `start`, `get`, `update`, `get_and_update`, `cast` |
| **Task** | `actojs/task` | Fire-and-forget async computation: `async`, `await_`, `yield_` |
| **Supervisor** | `actojs/supervisor` | Static child supervision with `one_for_one`, `one_for_all`, `rest_for_one` |
| **DynamicSupervisor** | `actojs/dynamic_supervisor` | Runtime child management: `start_child`, `terminate_child` |
| **TaskSupervisor** | `actojs/task_supervisor` | Supervised async tasks with lifecycle management |
| **Registry** | `actojs/registry` | Decentralised key-value process registry with partitioning |
| **PubSub** | `actojs/pubsub` | Topic-based publish/subscribe with automatic subscriber cleanup |
| **EventStream** | `actojs/event_stream` | `AsyncIterable` wrapper over a process mailbox: `for await…of` |
| **Node** | `actojs/node` | Distributed processes across tabs/workers via `BroadcastChannel` |
| **Core** | `actojs/core` | `ActorSystem`, `WebRuntime`, `WorkerRuntime` |
| **Runtimes** | `actojs/runtimes` | Runtime selection helpers |
| **GenServer** | `actojs/gen_server` | Internal framework (public but low-level) |


## Quick tour

### Spawn a process

```ts
import * as Process from "actojs/process";

const pid = Process.spawn(async () => {
  while (true) {
    const msg = await Process.receive();
    console.log("got:", msg);
  }
});

Process.send(pid, "hello");
```

### Stateful agent

```ts
import * as Agent from "actojs/agent";

const { ok: counter } = await Agent.start(() => 0);

await Agent.cast(counter, (n) => n + 1);
await Agent.cast(counter, (n) => n + 1);

const value = await Agent.get(counter, (n) => n); // 2
```

### Supervisor with children

```ts
import * as Supervisor from "actojs/supervisor";
import * as Agent from "actojs/agent";

const mod = {
  start_link: () => Agent.start(() => 0, { name: "child" }),
};

const { ok: sup } = await Supervisor.start_link(
  [{ id: "child1", start: [mod, "start_link", []], restart: "permanent" }],
  { strategy: "one_for_one" }
);

// If "child" exits abnormally, it's restarted automatically.
```

### PubSub

```ts
import * as PubSub from "actojs/pubsub";
import * as Process from "actojs/process";
import * as EventStream from "actojs/event_stream";

const { ok: pub } = await PubSub.start();
const sub = Process.spawn(async () => {
  await PubSub.subscribe(pub, "chat");
  for await (const msg of EventStream.receive<PubSub.PubSubMessage>()) {
    console.log(msg.topic, msg.message);
  }
});

await PubSub.publish(pub, "chat", "hello");
```

### Task with timeout

```ts
import * as Task from "actojs/task";

const handle = Task.async(async () => {
  await fetch("https://api.example.com/data");
  return "done";
});

const result = await Task.await_(handle, 5000); // throws TimeoutError after 5s
```

### Named processes & process registry

```ts
import * as Process from "actojs/process";
import * as Registry from "actojs/registry";

Process.register("logger", Process.self());

const { ok: reg } = await Registry.start({ keys: "unique" });
await Registry.register(reg, "worker1", Process.self(), { role: "cache" });
const entries = await Registry.lookup(reg, "worker1");
```

## Requirements

- **Bun** (test runner, runtime), Deno or Node.js 18+
- TypeScript 5 (peer)

## Install

```sh
bun add actojs typescript
```

## Run tests

```sh
bun test
```

## License

MIT
