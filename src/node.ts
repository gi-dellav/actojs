// acto/node — Distributed node abstraction.
// Web runtime: limited to same-origin BroadcastChannel communication.
// ponytail: full network-backed distribution via WebSocket when needed.

import type { PID, NodeStartOpts, NodeState, Ref, Module } from "./types";
import * as Proc from "./process";

let nodeName: string | null = null;
const connectedNodes = new Set<string>();
const nodeMonitors = new Map<string, { ref: Ref; pid: PID | null }[]>();
const channelNamePrefix = "__actojs_node__";

let broadcastChannel: BroadcastChannel | null = null;

interface WireMessage {
  from: string;
  to?: string;
  type: string;
  payload: unknown;
}

interface SpawnPayloadFn {
  type: "fn";
  source: string;
  linkId?: string;
  refId?: string;
}

interface SpawnPayloadMFA {
  type: "mfa";
  module: Module;
  fn: string;
  args: unknown[];
  linkId?: string;
  refId?: string;
}

type SpawnPayload = SpawnPayloadFn | SpawnPayloadMFA;

interface CrossNodeLink {
  localPid: PID;
  remoteNode: string;
}

interface CrossNodeMonitor {
  localRef: Ref;
  localPid: PID;
  remoteNode: string;
}

const crossNodeLinks = new Map<string, CrossNodeLink>();
const crossNodeMonitors = new Map<string, CrossNodeMonitor>();
const remoteSpawnRegistry = new Map<string, PID>();

let linkIdCounter = 0;
function generateLinkId(): string {
  return `l_${nodeName}_${linkIdCounter++}`;
}

// ---- start / stop ---------------------------------------------------------

/** Start a named node for distributed communication via BroadcastChannel. */
export function start(
  name: string,
  opts?: NodeStartOpts,
): { ok: PID } | { error: Error } {
  if (nodeName) {
    return { error: new Error("node already started") };
  }
  nodeName = name;

  try {
    broadcastChannel = new BroadcastChannel(channelNamePrefix + name);
    broadcastChannel.onmessage = (event) => {
      handleIncoming((event as MessageEvent).data as WireMessage);
    };
  } catch {
    broadcastChannel = null;
  }

  return { ok: Proc.self() };
}

/** Stop the node, closing all connections and sending disconnect notifications. */
export function stop(): void | { error: Error } {
  if (!nodeName) {
    return { error: new Error("node not started") };
  }
  if (broadcastChannel) {
    broadcastChannel.close();
    broadcastChannel = null;
  }
  const entries = nodeMonitors.get(nodeName);
  if (entries) {
    for (const { ref, pid } of entries) {
      if (pid) {
        Proc.send(pid, { type: "node_disconnected", ref, node: nodeName });
      }
    }
    nodeMonitors.delete(nodeName);
  }
  crossNodeLinks.forEach((link) => {
    Proc.send(link.localPid, {
      type: "EXIT",
      from: nodeName,
      reason: "nodedown",
    });
  });
  crossNodeLinks.clear();
  crossNodeMonitors.forEach((mon) => {
    Proc.send(mon.localPid, {
      type: "DOWN",
      ref: mon.localRef,
      pid: nodeName,
      reason: "nodedown",
    });
  });
  crossNodeMonitors.clear();
  remoteSpawnRegistry.clear();
  nodeName = null;
  connectedNodes.clear();
}

// ---- self / alive? --------------------------------------------------------

/** Return the name of the current node. Throws if no node has been started. */
export function self(): string {
  if (!nodeName) throw new Error("node not started");
  return nodeName;
}

/** Check whether a node has been started and is still alive. */
export function alive(): boolean {
  return nodeName !== null;
}

// ---- connect / disconnect -------------------------------------------------

/** Connect to a remote node by name, sending a handshake via BroadcastChannel. */
export function connect(node: string): boolean | "ignored" {
  if (!nodeName) return false;
  if (connectedNodes.has(node)) return "ignored";
  connectedNodes.add(node);

  if (broadcastChannel) {
    broadcastChannel.postMessage({
      from: nodeName,
      to: node,
      type: "connect",
      payload: null,
    });
  }
  return true;
}

/** Disconnect from a previously connected node. */
export function disconnect(node: string): void {
  connectedNodes.delete(node);
}

// ---- ping -----------------------------------------------------------------

/** Ping a connected node. Returns 'pong' if known, 'pang' otherwise. */
export function ping(node: string): "pong" | "pang" {
  if (!connectedNodes.has(node)) return "pang";
  if (broadcastChannel) {
    broadcastChannel.postMessage({
      from: nodeName!,
      to: node,
      type: "ping",
      payload: null,
    });
  }
  return "pong";
}

// ---- list -----------------------------------------------------------------

/** List nodes matching the given visibility states. Defaults to visible and connected. */
export function list(state?: string | string[]): string[] {
  if (!nodeName) return [];
  const states = state
    ? Array.isArray(state)
      ? state
      : [state]
    : ["visible", "connected"];
  const result: string[] = [];
  if (states.includes("this") || states.includes("visible")) {
    result.push(nodeName);
  }
  if (states.includes("connected")) {
    connectedNodes.forEach((n) => result.push(n));
  }
  return result;
}

// ---- monitor --------------------------------------------------------------

/** Start or stop monitoring a node. Pass flag=true to monitor (returns ref), false to stop. */
export function monitor(node: string, flag: boolean): Ref | void {
  if (flag) {
    const ref: Ref = Symbol("node_monitor");
    let pid: PID | null = null;
    try {
      pid = Proc.self();
    } catch (_) {}
    const entries = nodeMonitors.get(node) ?? [];
    entries.push({ ref, pid });
    nodeMonitors.set(node, entries);
    return ref;
  } else {
    let pid: PID | null = null;
    try {
      pid = Proc.self();
    } catch (_) {}
    const entries = nodeMonitors.get(node);
    if (entries) {
      const idx = entries.findIndex((e) => e.pid === pid);
      if (idx !== -1) {
        entries.splice(idx, 1);
        if (entries.length === 0) nodeMonitors.delete(node);
      }
    }
  }
}

/** Stop monitoring a node identified by the given monitor reference. */
export function demonitor_node(ref: Ref): void {
  nodeMonitors.forEach((entries, node) => {
    const idx = entries.findIndex((e) => e.ref === ref);
    if (idx !== -1) {
      entries.splice(idx, 1);
      if (entries.length === 0) nodeMonitors.delete(node);
      return;
    }
  });
}

// ---- spawn remote ---------------------------------------------------------

/** Spawn a function or MFA on a remote node via BroadcastChannel. */
export function spawn(
  node: string,
  fnOrModule: (() => void) | Module,
  fnName?: string,
  args?: unknown[],
): PID {
  if (!broadcastChannel) {
    throw new Error("BroadcastChannel not available");
  }

  let spawnPayload: SpawnPayload;

  if (typeof fnOrModule === "function") {
    spawnPayload = {
      type: "fn",
      source: fnOrModule.toString(),
    };
  } else {
    spawnPayload = {
      type: "mfa",
      module: fnOrModule,
      fn: fnName!,
      args: args ?? [],
    };
  }

  const fakePid = Proc.spawn(() => {
    broadcastChannel!.postMessage({
      from: nodeName!,
      to: node,
      type: "spawn",
      payload: spawnPayload,
    });
  });

  return fakePid;
}

/** Spawn a function or MFA on a remote node and link it to the caller. */
export function spawn_link(
  node: string,
  fnOrModule: (() => void) | Module,
  fnName?: string,
  args?: unknown[],
): PID {
  if (!broadcastChannel) {
    throw new Error("BroadcastChannel not available");
  }

  const linkId = generateLinkId();
  let callerPid: PID | null = null;
  try {
    callerPid = Proc.self();
  } catch (_) {}

  let spawnPayload: SpawnPayload;

  if (typeof fnOrModule === "function") {
    spawnPayload = {
      type: "fn",
      source: fnOrModule.toString(),
      linkId,
    };
  } else {
    spawnPayload = {
      type: "mfa",
      module: fnOrModule,
      fn: fnName!,
      args: args ?? [],
      linkId,
    };
  }

  const fakePid = Proc.spawn(() => {
    broadcastChannel!.postMessage({
      from: nodeName!,
      to: node,
      type: "spawn",
      payload: spawnPayload,
    });
  });

  if (callerPid) {
    crossNodeLinks.set(linkId, { localPid: callerPid, remoteNode: node });

    Proc.spawn(async () => {
      const monRef = Proc.monitor(callerPid!);
      const msg = await Proc.receive(30000);
      if (msg && (msg as { type: string; ref: Ref }).type === "DOWN") {
        const downMsg = msg as {
          type: "DOWN";
          ref: Ref;
          pid: PID;
          reason: unknown;
        };
        if (downMsg.ref === monRef) {
          crossNodeLinks.delete(linkId);
          if (broadcastChannel) {
            broadcastChannel.postMessage({
              from: nodeName,
              to: node,
              type: "spawn_kill",
              payload: { linkId, reason: downMsg.reason },
            });
          }
        }
      }
    });
  }

  return fakePid;
}

/** Spawn a function or MFA on a remote node and monitor it. Returns pid and ref. */
export function spawn_monitor(
  node: string,
  fnOrModule: (() => void) | Module,
  fnName?: string,
  args?: unknown[],
): { pid: PID; ref: Ref } {
  if (!broadcastChannel) {
    throw new Error("BroadcastChannel not available");
  }

  const refId = generateLinkId();
  const localRef: Ref = Symbol("monitor");
  let callerPid: PID | null = null;
  try {
    callerPid = Proc.self();
  } catch (_) {}

  let spawnPayload: SpawnPayload;

  if (typeof fnOrModule === "function") {
    spawnPayload = {
      type: "fn",
      source: fnOrModule.toString(),
      refId,
    };
  } else {
    spawnPayload = {
      type: "mfa",
      module: fnOrModule,
      fn: fnName!,
      args: args ?? [],
      refId,
    };
  }

  const fakePid = Proc.spawn(() => {
    broadcastChannel!.postMessage({
      from: nodeName!,
      to: node,
      type: "spawn",
      payload: spawnPayload,
    });
  });

  if (callerPid) {
    crossNodeMonitors.set(refId, {
      localRef,
      localPid: callerPid,
      remoteNode: node,
    });
  }

  return { pid: fakePid, ref: localRef };
}

// ---- internal -------------------------------------------------------------

function handleIncoming(msg: WireMessage): void {
  if (msg.to && msg.to !== nodeName) return;

  switch (msg.type) {
    case "connect": {
      if (msg.from && msg.from !== nodeName) {
        connectedNodes.add(msg.from);
      }
      break;
    }
    case "ping": {
      if (broadcastChannel && msg.from) {
        broadcastChannel.postMessage({
          from: nodeName,
          to: msg.from,
          type: "pong",
          payload: null,
        });
      }
      break;
    }
    case "pong": {
      break;
    }
    case "spawn": {
      const payload = msg.payload as SpawnPayload;
      const originNode = msg.from;
      let spawnedPid: PID | null = null;

      if (payload.type === "fn") {
        try {
          const fn = new Function(`return (${payload.source})`)();
          spawnedPid = Proc.spawn(fn);
        } catch (_) {}
      } else if (payload.type === "mfa") {
        const { module, fn: fnName, args } = payload;
        try {
          if (typeof module[fnName] === "function") {
            spawnedPid = Proc.spawn(() => {
              (module[fnName] as Function)(...args);
            });
          }
        } catch (_) {}
      }

      if (spawnedPid && originNode) {
        if (payload.linkId) {
          remoteSpawnRegistry.set(payload.linkId, spawnedPid);
          const capturedPid = spawnedPid;
          const capturedLinkId = payload.linkId;
          Proc.spawn(async () => {
            const monRef = Proc.monitor(capturedPid);
            const downMsg = await Proc.receive(30000);
            if (
              downMsg &&
              (downMsg as { type: string }).type === "DOWN" &&
              (downMsg as { ref: Ref }).ref === monRef
            ) {
              remoteSpawnRegistry.delete(capturedLinkId);
              if (broadcastChannel) {
                broadcastChannel.postMessage({
                  from: nodeName,
                  to: originNode,
                  type: "spawn_exit",
                  payload: {
                    linkId: capturedLinkId,
                    remotePid: capturedPid,
                    reason: (downMsg as { reason: unknown }).reason,
                  },
                });
              }
            }
          });
        }

        if (payload.refId) {
          remoteSpawnRegistry.set(payload.refId, spawnedPid);
          const capturedPid = spawnedPid;
          const capturedRefId = payload.refId;
          Proc.spawn(async () => {
            const monRef = Proc.monitor(capturedPid);
            const downMsg = await Proc.receive(30000);
            if (
              downMsg &&
              (downMsg as { type: string }).type === "DOWN" &&
              (downMsg as { ref: Ref }).ref === monRef
            ) {
              remoteSpawnRegistry.delete(capturedRefId);
              if (broadcastChannel) {
                broadcastChannel.postMessage({
                  from: nodeName,
                  to: originNode,
                  type: "spawn_down",
                  payload: {
                    refId: capturedRefId,
                    remotePid: capturedPid,
                    reason: (downMsg as { reason: unknown }).reason,
                  },
                });
              }
            }
          });
        }
      }
      break;
    }
    case "spawn_exit": {
      const { linkId, remotePid, reason } = msg.payload as {
        linkId: string;
        remotePid: string;
        reason: unknown;
      };
      const link = crossNodeLinks.get(linkId);
      if (link) {
        Proc.send(link.localPid, {
          type: "EXIT",
          from: remotePid,
          reason,
        });
        crossNodeLinks.delete(linkId);
      }
      break;
    }
    case "spawn_down": {
      const { refId, remotePid, reason } = msg.payload as {
        refId: string;
        remotePid: string;
        reason: unknown;
      };
      const mon = crossNodeMonitors.get(refId);
      if (mon) {
        Proc.send(mon.localPid, {
          type: "DOWN",
          ref: mon.localRef,
          pid: remotePid,
          reason,
        });
        crossNodeMonitors.delete(refId);
      }
      break;
    }
    case "spawn_kill": {
      const { linkId, reason } = msg.payload as {
        linkId: string;
        reason: unknown;
      };
      const pid = remoteSpawnRegistry.get(linkId);
      if (pid) {
        Proc.exit(pid, reason ?? "kill");
        remoteSpawnRegistry.delete(linkId);
      }
      break;
    }
  }
}
