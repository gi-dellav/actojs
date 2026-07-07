# Architecture

actojs — Elixir-inspired actor concurrency for JavaScript/TypeScript.
Cooperative event-loop scheduler ("web runtime"); pluggable runtimes
for true parallelism are a future goal.

## Directory layout

```
src/
├── types.ts              Core types (PID, Ref, ChildSpec, Strategy, etc.)
├── system.ts             ActorSystem — isolated actor universe
├── core.ts               Runtime abstraction + ActorSystem re-export
├── mailbox.ts            Thin delegation layer to ActorSystem.current
├── process.ts            Low-level process primitives (spawn, send, link, etc.)
├── gen_server.ts         Internal GenServer framework (handle_call/cast/info)
├── agent.ts              State-holding actor (get/update/get_and_update/cast)
├── task.ts               Fire-and-forget async computation (await/yield)
├── registry.ts           Decentralised key-value process registry with partitioning
├── supervisor.ts         Static child supervisor with restart strategies
├── dynamic_supervisor.ts Dynamic child supervisor (children added at runtime)
├── node.ts               Distributed node (BroadcastChannel, same-origin)
└── index.ts              Namespaced re-exports of all modules
tests/                    One test file per module, using bun:test
index.ts                  Root re-exports (mirrors src/index.ts)
```

## Key types (`src/types.ts`)

| Type | Kind | Purpose |
|---|---|---|
| `PID` | `string` | Erlang-style process identifier (`#PID<0.N.0>`) |
| `Ref` | `symbol` | Unique reference for monitors, calls, tasks, timers |
| `From` | `{ pid, ref }` | Tracks the caller of a `genCall` for reply routing |
| `ChildSpec` | interface | Static child specification (id, start MFA, restart policy) |
| `Strategy` | union | `'one_for_one'` \| `'one_for_all'` \| `'rest_for_one'` |
| `ProcessState` | interface | Internal: mailbox queue, links, monitors, status, dict |
| `GenServerCallbacks<S>` | interface | `init`, `handle_call`, `handle_cast`, `handle_info`, `terminate` |
| `TaskHandle<R>` | `{ pid, ref }` | Handle to await/yield a task result |

## ActorSystem — the isolated universe (`src/system.ts`)

`ActorSystem` owns all mutable actor state and is accessed via a global
`ActorSystem.current` singleton (replaceable with `ActorSystem.run(sys, fn)`).

Each system holds:
- **processes**: `Map<PID, ProcessState>` — every spawned process
- **nameRegistry**: `Map<string, PID>` — named process lookup
- **pidStack**: implicit PID context for `self()` (LIFO)
- **pendingCalls**: `Map<symbol, PendingCall>` — outstanding `genCall` promises
- **taskResults**: `Map<Ref, TaskResult>` — task completion state
- **timers**: `Map<Ref, setTimeout handle>` — for `send_after` / `cancel_timer`

Multiple `ActorSystem` instances are fully isolated — processes, names,
and exit cascades never cross system boundaries.

## Control flow

### Spawning a process (`src/process.ts`)
1. `spawn(fn)` captures `ActorSystem.current`, generates a PID, creates a
   `ProcessState`, sets up optional link/monitor edges from the caller.
2. The function is scheduled via `queueMicrotask`, wrapped in
   `ActorSystem.run(sys, ...)` to restore system context.
3. Inside the microtask, `sys.runWithPid(pid, fn)` pushes the PID onto the
   stack so `self()` works. When the function returns (or the promise
   settles), `sys.handleExit(proc)` runs the exit cascade.

### Message delivery (`src/system.ts` → `src/mailbox.ts`)
- `send(dest, msg)` resolves `dest` via the name registry (if a string
  matches a registered name) or treats it as a raw PID.
- If the target process is blocked in `receive()`, the waiting promise is
  resolved immediately with the message (synchronous delivery within the
  cooperative event loop).
- Otherwise the message is pushed onto `proc.mailbox` for later retrieval.

### GenServer loop (`src/gen_server.ts`)
The GenServer is the backbone of Agent, Registry, Supervisor, and
DynamicSupervisor. `startGenServer(callbacks, initArg)` spawns a process
that runs an async `while (Proc.alive(me))` loop:
1. `await receiveMessage(me)` — blocks until a message arrives.
2. Dispatches on `msg.__gen_server__` tag:
   - `'call'` → `handle_call(payload, from, state)` — must reply or defer
   - `'cast'` → `handle_cast(payload, state)` — fire-and-forget
   - `'stop'` → `terminate(reason, state)`, then `exit()`
3. Any message without a gen_server tag goes to `handle_info`.

`genCall(pid, msg)` creates a pending Promise stored in
`ActorSystem.pendingCalls`, sends a tagged message, and the GenServer's
reply (or `GS.reply(from)`) resolves it. `genCast` sends and forgets.

### Exit cascading (`src/system.ts`)
When a process exits (`handleExit`):
1. **Linked processes**: each linked process receives an `EXIT` message if
   `trapExit` is true; otherwise it exits with the same reason (cascading).
2. **Monitoring processes**: each monitor receives a `DOWN` message with
   the ref, PID, and reason.
3. The process is deregistered from the process table and name registry.

### Supervisor restart flow (`src/supervisor.ts`)
The supervisor monitors all children. On a `DOWN` message with an abnormal
reason:
1. `checkRestartRate` enforces `max_restarts` within `max_seconds` window;
   if exceeded, the supervisor itself shuts down.
2. `applyRestartStrategy` restarts children per the strategy:
   - `one_for_one`: restart only the failed child
   - `one_for_all`: kill all children, restart all in order
   - `rest_for_one`: kill the failed child and all children started after
     it, restart them in order

## Data flow

```
caller                    ActorSystem              gen_server process
  |                            |                          |
  |-- genCall(pid, msg) ----->|                          |
  |   creates PendingCall     |                          |
  |   stores in pendingCalls  |                          |
  |                            |-- send({call, ref, msg})->|
  |                            |                          |-- handle_call()
  |                            |                          |-- returns {reply, state}
  |                            |<-- resolvePending(ref) --|
  |<-- promise resolves ------|                          |
```

Messages are plain objects. GenServer messages carry a `__gen_server__`
discriminant. Exit/DOWN messages carry `type: 'EXIT'` or `type: 'DOWN'`.

## Design decisions

- **Cooperative, not parallel.** All processes share one JS thread.
  `queueMicrotask` and Promise scheduling provide interleaving. True
  parallelism is gated behind the pluggable `Runtime` abstraction
  (`src/core.ts`).
- **PID as string.** Human-readable Erlang-style `#PID<0.N.0>` strings
  rather than opaque objects — easier debugging, can be serialised.
- **Ref as Symbol.** Guaranteed unique; no collision risk across monitors,
  calls, tasks, and timers.
- **ActorSystem as global singleton.** `ActorSystem.current` is a mutable
  static. `ActorSystem.run(sys, fn)` scopes a different system for the
  duration of `fn`. This avoids threading a system reference through every
  API call while supporting full isolation for testing.
- **mailbox.ts as delegation layer.** All state lives in `ActorSystem`;
  `mailbox.ts` exists so `process.ts` can call mailbox functions without
  importing `ActorSystem` directly, avoiding circular dependency issues.
- **GenServer is internal.** Not exported as a public API. Agent, Registry,
  Supervisor, and DynamicSupervisor are the user-facing abstractions built
  on it. Users implement custom servers by composing these or using raw
  `spawn` + `receive`.
- **Supervisor restart rate limiting.** Prevents infinite restart loops.
  Default: 3 restarts in 5 seconds. Exceeding this causes the supervisor
  itself to exit — "let it crash" with a safety fuse.
- **BroadcastChannel for distribution.** `node.ts` uses the
  `BroadcastChannel` API for same-origin cross-tab/worker communication.
  Full network distribution is a ponytail item (WebSocket, etc.).
- **Zero runtime dependencies.** Only devDependency is `@types/bun`.
  Targets Bun as the primary runtime.

## Dependencies

| Dependency | Role |
|---|---|
| `typescript ^5` (peer) | Type-checking for consumers |
| `@types/bun ^1.3.14` (dev) | Bun type definitions |
| Bun (runtime) | Test runner (`bun:test`), bundler, package manager |

No runtime npm dependencies.

## Entry points

- **`index.ts`** (root) — re-exports all modules as namespaced exports
- **`src/index.ts`** — same re-exports for the package's `"."` export
- **Subpath exports** (package.json `exports` map): `actojs/process`,
  `actojs/agent`, `actojs/task`, `actojs/registry`, `actojs/supervisor`,
  `actojs/dynamic_supervisor`, `actojs/node`, `actojs/core`,
  `actojs/system`
- **`src/process.ts`** — primary low-level API: `spawn`, `send`, `self`,
  `link`/`unlink`, `monitor`/`demonitor`, `register`, `receive`, timers
- **`src/gen_server.ts`** — internal framework; `startGenServer`, `genCall`,
  `genCast`, `genStop`, `reply` are available for building custom
  GenServer-based abstractions

## Instructions for agents

When you discover new architectural information, add it to this file.
Keep the document under ~300 lines. Keep entries concise and reference
specific source files.
