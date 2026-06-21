# ActoJS Reference

**ActoJS** is a TypeScript/JavaScript library that brings Elixir’s actor-based concurrency model to the JavaScript ecosystem. It runs identically on **Node.js**, **Bun**, **Deno**, and **browsers** – the same API, the same semantics, with the runtime automatically selecting the best underlying transport (Worker threads, Web Workers, or a cooperative event-loop scheduler).

All modules are accessible under the `acto` namespace (e.g., `acto/process`, `acto/agent`). Naming conventions follow Elixir’s standard library as closely as the language permits.

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Process (`acto/process`)](#process)
3. [Agent (`acto/agent`)](#agent)
4. [Task (`acto/task`)](#task)
5. [Registry (`acto/registry`)](#registry)
6. [Supervisor (`acto/supervisor`)](#supervisor)
7. [DynamicSupervisor (`acto/dynamic_supervisor`)](#dynamicsupervisor)
8. [Node (`acto/node`)](#node)
9. [Runtime Abstraction](#runtime)

---

## Core Concepts

### PID

Every actor is represented by an opaque `PID` value (a string or symbol). It can be used to `send` messages or query the process.

```ts
import { send, self } from 'acto/process';
send(somePid, { type: 'hello' });
const me: PID = self();
```

### Message Passing

Processes communicate exclusively through asynchronous message passing. A process has an internal mailbox (a queue) and processes messages one at a time. The library provides abstractions (GenServer, Agent, Task) so you rarely interact with the mailbox directly.

### Process Lifecycle

Processes are spawned via `spawn`, `spawn_link`, `Task.async`, etc. They can be linked or monitored. When a process terminates (normally or abnormally), supervisors react according to restart strategies.

---

## Process

> `import * as Process from 'acto/process';`

Low-level functions for working with processes, links, monitors, timers, and the process dictionary.

### Types

| Type               | JS equivalent                |
|---------------------|-----------------------------|
| `pid`               | `string` / `symbol`         |
| `dest`              | `PID \| string` (registered name) |
| `ref`               | `symbol` (monitor/timer reference) |
| `spawn_opts`        | array of `'link' \| 'monitor'` |

### Functions

| Function                        | Description (adapted) |
|----------------------------------|------------------------|
| `spawn(fn: () => void, opts?: SpawnOpts[]) : PID` | Spawn a new process from a function. |
| `spawn_link(fn: () => void) : PID` | Spawn and link to caller. |
| `send(dest: Dest, msg: unknown) : void` | Send a message to a process or named process. |
| `self() : PID` | Return the calling process’s PID. |
| `alive?(pid: PID) : boolean` | Check if a process is alive. |
| `exit(pid: PID, reason: unknown) : void` | Send an exit signal. |
| `link(pid: PID) : void` | Create a bidirectional link. |
| `unlink(pid: PID) : void` | Remove a link. |
| `monitor(pid: PID) : Ref` | Start monitoring a process. Returns a reference. |
| `demonitor(ref: Ref) : void` | Stop monitoring. |
| `flag(flag: string, value: boolean) : boolean` | Set process flags. Supported: `'trap_exit'`. |
| `register(pid: PID, name: string) : void` | Register a process under a local name. |
| `unregister(name: string) : void` | Remove a registered name. |
| `whereis(name: string) : PID \| null` | Look up a registered process. |
| `list() : PID[]` | Return all PIDs on the local node. |
| `sleep(ms: number) : Promise<void>` | Suspend the process for `ms` milliseconds. |
| `send_after(dest: Dest, msg: unknown, ms: number) : Ref` | Send a message after a delay. Returns a timer reference. |
| `cancel_timer(ref: Ref) : void` | Cancel a timer. |
| `hibernate(fn, ...args)` | **Not implemented** – JS has no per-process GC compaction. |
| `info(pid: PID)` | **Limited** – returns `{ status, messageQueueLength }`. |
| `get(key: string)` / `put(key, value)` / `delete(key)` | Process dictionary accessors. |

### Example Snippets

```ts
const pid = Process.spawn(() => { /* actor logic */ });
Process.send(pid, { type: 'inc' });
Process.register(pid, 'my_worker');
const p = Process.whereis('my_worker'); // => pid
```

---

## Agent

> `import * as Agent from 'acto/agent';`

A simple abstraction around a piece of mutable state. State is held inside an agent process and can be queried or updated via closures or (module, function, args) tuples.

### Types

| Type        | JS equivalent                                |
|-------------|----------------------------------------------|
| `agent`     | `PID \| { module, args }` (via Registry)     |
| `on_start`  | `{ ok: PID } \| { error: Error }`            |
| `state`     | `unknown`                                    |

### Functions

| Function | Description |
|----------|-------------|
| `start(init: () => S, opts?: { name?: string }) : OnStart` | Start an agent without linking. |
| `start_link(init: () => S, opts?: { name?: string }) : OnStart` | Start an agent linked to the caller. |
| `get(agent: Agent, fn: (s: S) => R) : Promise<R>` | Synchronously query the state. |
| `update(agent: Agent, fn: (s: S) => S) : Promise<void>` | Update the state. |
| `get_and_update(agent: Agent, fn: (s: S) => [R, S]) : Promise<R>` | Query and update atomically. |
| `cast(agent: Agent, fn: (s: S) => S) : void` | Fire-and-forget update. |
| `stop(agent: Agent, reason?: unknown) : Promise<void>` | Synchronously stop the agent. |

The `module/function/args` style is also supported for distributed scenarios:

```ts
Agent.get(agent, [MyModule, 'getUser', [userId]]);
```

### Snippet

```ts
const {:ok, pid} = await Agent.start_link(() => 0);
await Agent.update(pid, n => n + 1);
const val = await Agent.get(pid, n => n);
```

---

## Task

> `import * as Task from 'acto/task';`

A convenience for spawning a process that performs a single computation and can be awaited.

| Function | Description |
|----------|-------------|
| `async<R>(fn: () => Promise<R>) : Task<R>` | Start a task. |
| `await<R>(task: Task<R>) : Promise<R>` | Wait for the result. |
| `yield<R>(task: Task<R>) : Promise<R \| null>` | Non-blocking poll. |
| `shutdown(task: Task) : Promise<void>` | Stop a running task. |

```ts
const t = Task.async(() => fetch('/api'));
const data = await Task.await(t);
```

---

## Registry

> `import * as Registry from 'acto/registry';`

A local, decentralised key-value process store. Keys can be `:unique` (0 or 1 process) or `:duplicate` (many processes per key). Internal partitioning is used for scalability.

### Start Options

```ts
Registry.start_link({
  keys: 'unique' | 'duplicate' | { duplicate: 'key' } | { duplicate: 'pid' },
  name: string,
  partitions: number,   // default 1
  listeners: string[],   // named processes notified on register/unregister
  meta: Record<string, unknown>
});
```

### Functions

| Function | Description |
|----------|-------------|
| `start_link(opts: StartOptions) : OnStart` | Start a registry. |
| `register(reg: RegistryID, key: string, value: unknown) : { ok: PID } \| { error: ... }` | Register caller under key. |
| `unregister(reg: RegistryID, key: string) : void` | Unregister caller from key. |
| `lookup(reg: RegistryID, key: string) : { pid: PID, value: unknown }[]` | Look up all entries for a key. |
| `match(reg, key, pattern, guards?) : { pid, value }[]` | Match entries by value pattern. |
| `dispatch(reg, key, callback, opts?) : void` | Invoke callback with matching entries. |
| `keys(reg, pid) : string[]` | Keys registered by a given PID. |
| `values(reg, key, pid) : unknown[]` | Values for a key registered by a PID. |
| `count(reg) : number` | Total registered keys. |
| `update_value(reg, key, fn) : { newValue, oldValue }` | Update the value for the calling process’s key. |

### Snippet

```ts
const {:ok, reg} = await Registry.start_link({ keys: 'unique', name: 'MyReg' });
await Registry.register(reg, 'worker1', { role: 'db' });
const entries = Registry.lookup(reg, 'worker1');
```

---

## Supervisor

> `import * as Supervisor from 'acto/supervisor';`

A behaviour for managing child processes according to a restart strategy. Child specifications describe how to start, stop, and restart children.

### Child Specification

```ts
interface ChildSpec {
  id: string;
  start: [Module, string, unknown[]];  // [module, functionName, args]
  restart?: 'permanent' | 'transient' | 'temporary';
  shutdown?: number | 'brutal_kill' | 'infinity';
  type?: 'worker' | 'supervisor';
  significant?: boolean;
}
```

### Strategies

| Strategy | Behaviour |
|----------|-----------|
| `'one_for_one'` | Only the crashed child is restarted. |
| `'one_for_all'` | All children are terminated and restarted. |
| `'rest_for_one'` | The crashed child and all children after it are restarted. |

### Functions

| Function | Description |
|----------|-------------|
| `start_link(children: ChildSpec[], opts: StartOptions) : OnStart` | Start a supervisor with a static child list. |
| `start_link(module: Module, initArg: unknown, opts?) : OnStart` | Start a module-based supervisor (calls `module.init(arg)`). |
| `init(children: ChildSpec[], opts: InitOptions) : SupervisorSpec` | Used inside `init` callback of module-based supervisors. |
| `count_children(sup: PID) : Counts` | Return counts of active children. |
| `which_children(sup: PID) : ChildInfo[]` | List all children with PID, type, modules. |
| `start_child(sup: PID, spec: ChildSpec) : OnStartChild` | Dynamically add a child. |
| `terminate_child(sup: PID, childId: string) : void \| { error: ... }` | Terminate a running child. |
| `delete_child(sup: PID, childId: string) : void \| { error: ... }` | Remove a child specification (child must be stopped). |
| `restart_child(sup: PID, childId: string) : OnStartChild` | Restart a previously terminated child. |
| `stop(sup: PID, reason?) : Promise<void>` | Synchronously stop the supervisor. |
| `child_spec(moduleOrSpec, overrides?) : ChildSpec` | Build or override a child specification. |

### Module-based Supervisor

```ts
class MySup implements SupervisorModule {
  init(_arg: any) {
    return Supervisor.init([{ ChildModule, arg: 0 }], {
      strategy: 'one_for_one'
    });
  }
}
```

### Snippet

```ts
const {:ok, sup} = await Supervisor.start_link([
  { id: 'worker1', start: [MyWorker, 'start_link', []] }
], { strategy: 'one_for_one' });
```

---

## DynamicSupervisor

> `import * as DynamicSupervisor from 'acto/dynamic_supervisor';`

A supervisor optimised for starting children dynamically at runtime. It begins with no children; children are added via `start_child`.

### Options (init)

| Option | Description |
|--------|-------------|
| `strategy` | Always `'one_for_one'`. |
| `max_restarts` | Max restarts in a period (default `3`). |
| `max_seconds` | Period in seconds (default `5`). |
| `max_children` | Maximum simultaneous children. Default `Infinity`. |
| `extra_arguments` | Arguments prepended to every `start_child` call. |

### Functions

| Function | Description |
|----------|-------------|
| `start_link(opts: StartOptions) : OnStart` | Start a dynamic supervisor. |
| `start_link(module: Module, initArg: unknown, opts?) : OnStart` | Module-based start. |
| `init(opts: InitOptions) : SupervisorSpec` | Used inside `init` callback. |
| `start_child(sup: PID, spec: ChildSpec) : OnStartChild` | Dynamically start a child. |
| `terminate_child(sup: PID, pid: PID) : void \| { error: ... }` | Terminate by PID. |
| `count_children(sup: PID) : Counts` | Count children. |
| `which_children(sup: PID) : ChildInfo[]` | List children (id is always `undefined`). |
| `stop(sup: PID, reason?) : Promise<void>` | Synchronously stop. |

### Snippet

```ts
const {:ok, dynSup} = await DynamicSupervisor.start_link({
  name: 'MyDynSup',
  max_children: 1000
});
await DynamicSupervisor.start_child(dynSup, { id: 'c1', start: [MyWorker, 'start_link', [42]] });
```

---

## Node

> `import * as Node from 'acto/node';`

Provides an abstraction for connecting multiple ActoJS instances (nodes) together – either within the same process via Workers or across the network (WebSocket-backed). It mirrors the Elixir `Node` module conceptually while acknowledging that the JS runtime has no built-in distributed process registry.

### Types

| Type      | JS equivalent |
|-----------|---------------|
| `node`    | `string` (e.g., `"app@host"`) |
| `state`   | `'visible' \| 'hidden' \| 'connected' \| 'this' \| 'known'` |

### Functions

| Function | Description |
|----------|-------------|
| `start(name: string, opts?: NodeStartOpts) : { ok: PID } \| { error: ... }` | Turn a non-distributed instance into a named node. |
| `stop() : void \| { error: ... }` | Turn a named node back into a non-distributed one. |
| `self() : string` | Return the current node name. |
| `alive?() : boolean` | Check if the local node is named and connected. |
| `connect(node: string) : boolean \| 'ignored'` | Establish a connection to another node. |
| `disconnect(node: string) : void` | Force-disconnect a node. |
| `ping(node: string) : 'pong' \| 'pang'` | Test connectivity. |
| `list(state?: string | string[]) : string[]` | List visible nodes. |
| `monitor(node: string, flag: boolean) : void` | Turn on/off node monitoring. |
| `spawn(node: string, fn: () => void) : PID` | Spawn a process on a remote node. |
| `spawn(node: string, module: Module, fn: string, args: any[]) : PID` | Spawn a module function on a remote node. |
| `spawn_link(node: string, ...) : PID` | Spawn with link on remote node. |
| `spawn_monitor(node: string, ...) : { pid: PID, ref: Ref }` | Spawn with monitor on remote node. |

> **Note:** The JS runtime cannot treat remote processes exactly like local ones. `Node.spawn` serializes the function/arguments and executes them on the remote side via transports (Worker postMessage, WebSocket, etc.). Anonymous functions are supported only within a single Worker/parent boundary; for network nodes, only `[module, function, args]` style is supported.

### Snippet

```ts
const {:ok, pid} = await Node.start('myapp@localhost', { name_domain: 'shortnames' });
Node.connect('other@host');
const remotePid = Node.spawn('other@host', [MyModule, 'run', []]);
```

---

## Runtime Abstraction

ActoJS selects the best concurrency primitive for the current environment:

| Environment | Default Transport | Isolation |
|-------------|-------------------|-----------|
| Node.js     | `worker_threads`   | True parallelism |
| Bun         | `Worker` (native)  | True parallelism |
| Deno        | `Worker` (unstable)| Parallel if available |
| Browser     | `new Worker(...)`  | True parallelism |
| Fallback    | `setTimeout` / `queueMicrotask` | Cooperative (single-threaded) |

The runtime is auto-detected and can be overridden:

```ts
import { setRuntime } from 'acto/core';
import { NodeRuntime } from 'acto/runtimes/node';
setRuntime(new NodeRuntime());
```

All inter-process messages are serialized using the **structured clone algorithm** (when crossing thread boundaries) or passed by reference on the event-loop fallback.

---

## Incompatible / Omitted Features

Some Elixir process features have no direct equivalent in the JS runtime and are not implemented:

- **Hot code swapping** – No VM-level code reloading.
- **Hibernation** – JS cannot shrink a closure’s heap.
- **Group leaders** – No I/O group concept.
- **Aliases** – Not applicable; JS has no built-in alias mechanism.
- **Process dictionary replication across nodes** – Not supported.
- **Preemptive scheduling** – JS is cooperatively scheduled; long-running synchronous work blocks the event loop.
- **`:infinity` timeout** – Represented as `Infinity` but should be used sparingly.

---

This reference covers the complete ActoJS API surface, aligned with Elixir v1.20.1 semantics while respecting the constraints of JavaScript runtimes. For detailed usage guides and patterns, refer to the [ActoJS User Guide](https://actojs.dev).
