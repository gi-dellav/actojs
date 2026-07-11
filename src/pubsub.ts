// actojs — Topic-based publish/subscribe with automatic subscriber cleanup.
// Built on GenServer. Subscribers are monitored; exited processes are
// automatically removed from all topics.

import type { PID, Ref } from "./types";
import * as GS from "./gen_server";
import * as Proc from "./process";

// ---- message tags ---------------------------------------------------------

/** Tagged message delivered to pub/sub subscribers, with topic and payload. */
export interface PubSubMessage {
  __pubsub__: true;
  topic: string;
  message: unknown;
}

/** Type guard for messages published via PubSub. */
export function isPubSubMessage(msg: unknown): msg is PubSubMessage {
  return (
    typeof msg === "object" &&
    msg !== null &&
    "__pubsub__" in msg &&
    (msg as PubSubMessage).__pubsub__ === true
  );
}

// ---- internal protocol ----------------------------------------------------

interface SubscribeMsg {
  action: "subscribe";
  topic: string;
  pid: PID;
}
interface UnsubscribeMsg {
  action: "unsubscribe";
  topic: string;
  pid: PID;
}
interface PublishMsg {
  action: "publish";
  topic: string;
  message: unknown;
}
interface SubscribersMsg {
  action: "subscribers";
  topic: string;
}
interface TopicsMsg {
  action: "topics";
}

type PubSubCast = SubscribeMsg | UnsubscribeMsg | PublishMsg;

// ---- state ----------------------------------------------------------------

interface MonData {
  pid: PID;
  topics: Set<string>;
}

interface PubSubState {
  topics: Map<string, Set<PID>>;
  monitors: Map<Ref, MonData>;
}

// ---- subscribe / unsubscribe helpers --------------------------------------

function doSubscribe(
  state: PubSubState,
  topic: string,
  pid: PID,
  self: PID,
): void {
  let subs = state.topics.get(topic);
  if (!subs) {
    subs = new Set();
    state.topics.set(topic, subs);
  }
  subs.add(pid);

  let found = false;
  state.monitors.forEach((mon) => {
    if (!found && mon.pid === pid) {
      mon.topics.add(topic);
      found = true;
    }
  });
  if (!found) {
    const ref = Proc.monitor(pid, self);
    state.monitors.set(ref, { pid, topics: new Set([topic]) });
  }
}

function doUnsubscribe(
  state: PubSubState,
  topic: string,
  pid: PID,
  self: PID,
): void {
  const subs = state.topics.get(topic);
  if (subs) {
    subs.delete(pid);
    if (subs.size === 0) state.topics.delete(topic);
  }
  state.monitors.forEach((mon, ref) => {
    if (mon.pid === pid) {
      mon.topics.delete(topic);
      if (mon.topics.size === 0) {
        state.monitors.delete(ref);
        Proc.demonitor(ref);
      }
    }
  });
}

// ---- callbacks ------------------------------------------------------------

const callbacks: GS.GenServerCallbacks<PubSubState> = {
  init() {
    return { topics: new Map(), monitors: new Map() };
  },

  handle_call(msg, _from, state, _self) {
    const m = msg as SubscribersMsg | TopicsMsg;

    if (m.action === "subscribers") {
      const subs = state.topics.get(m.topic);
      return { reply: subs ? Array.from(subs) : [], state };
    }

    if (m.action === "topics") {
      return { reply: Array.from(state.topics.keys()), state };
    }

    return { reply: { error: "unknown_action" }, state };
  },

  handle_cast(msg, state, self) {
    const m = msg as PubSubCast;

    if (m.action === "subscribe") {
      doSubscribe(state, m.topic, m.pid, self);
    } else if (m.action === "unsubscribe") {
      doUnsubscribe(state, m.topic, m.pid, self);
    } else if (m.action === "publish") {
      const subs = state.topics.get(m.topic);
      if (subs) {
        const envelope: PubSubMessage = {
          __pubsub__: true,
          topic: m.topic,
          message: m.message,
        };
        subs.forEach((pid) => {
          Proc.send(pid, envelope);
        });
      }
    }

    return { noreply: false, state };
  },

  handle_info(msg, state, _self) {
    if (
      typeof msg === "object" &&
      msg !== null &&
      (msg as any).type === "DOWN"
    ) {
      const down = msg as { type: "DOWN"; ref: Ref; pid: PID; reason: unknown };
      const mon = state.monitors.get(down.ref);
      if (mon) {
        state.monitors.delete(down.ref);
        mon.topics.forEach((topic) => {
          const subs = state.topics.get(topic);
          if (subs) {
            subs.delete(mon.pid);
            if (subs.size === 0) state.topics.delete(topic);
          }
        });
      }
    }
    return { noreply: false, state };
  },
};

// ---- public API -----------------------------------------------------------

/** Options for starting a PubSub instance (optional registered name). */
export interface PubSubOptions {
  name?: string;
}

/** Start a PubSub instance. Returns the PID. */
export async function start_link(options?: PubSubOptions): Promise<PID> {
  const result = await GS.start_link(callbacks, undefined, {
    name: options?.name,
  });
  if ("error" in result) throw result.error;
  return result.ok;
}

function resolve(pubsub: PID | string): PID {
  if (typeof pubsub === "string" && !pubsub.startsWith("#PID<")) {
    const pid = Proc.whereis(pubsub);
    if (!pid) throw new Error(`PubSub not found: ${pubsub}`);
    return pid;
  }
  return pubsub as PID;
}

/** Subscribe the calling process to a topic. */
export function subscribe(pubsub: PID | string, topic: string): void {
  const pid = resolve(pubsub);
  GS.cast(pid, {
    action: "subscribe",
    topic,
    pid: Proc.self(),
  } satisfies SubscribeMsg);
}

/** Unsubscribe the calling process from a topic. */
export function unsubscribe(pubsub: PID | string, topic: string): void {
  const pid = resolve(pubsub);
  GS.cast(pid, {
    action: "unsubscribe",
    topic,
    pid: Proc.self(),
  } satisfies UnsubscribeMsg);
}

/** Publish a message to a topic. All subscribers receive a tagged message. */
export function publish(
  pubsub: PID | string,
  topic: string,
  message: unknown,
): void {
  const pid = resolve(pubsub);
  Proc.send(pid, {
    __gen_server__: "cast",
    payload: { action: "publish", topic, message } satisfies PublishMsg,
  });
}

/** Get the list of subscribers for a topic. */
export async function subscribers(
  pubsub: PID | string,
  topic: string,
  timeout?: number,
): Promise<PID[]> {
  const pid = resolve(pubsub);
  return GS.call(
    pid,
    { action: "subscribers", topic } satisfies SubscribersMsg,
    timeout,
  ) as Promise<PID[]>;
}

/** Get the list of active topics. */
export async function topics(
  pubsub: PID | string,
  timeout?: number,
): Promise<string[]> {
  const pid = resolve(pubsub);
  return GS.call(
    pid,
    { action: "topics" } satisfies TopicsMsg,
    timeout,
  ) as Promise<string[]>;
}
