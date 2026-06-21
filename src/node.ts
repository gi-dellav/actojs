// acto/node — Distributed node abstraction.
// Web runtime: limited to same-origin BroadcastChannel communication.
// ponytail: full network-backed distribution via WebSocket when needed.

import type { PID, NodeStartOpts, NodeState, Ref } from './types';
import * as Proc from './process';

let nodeName: string | null = null;
const connectedNodes = new Set<string>();
const nodeMonitors = new Set<Ref>();
const channelNamePrefix = '__actojs_node__';

let broadcastChannel: BroadcastChannel | null = null;

interface WireMessage {
  from: string;
  to?: string;
  type: string;
  payload: unknown;
}

// ---- start / stop ---------------------------------------------------------

export function start(name: string, opts?: NodeStartOpts): { ok: PID } | { error: Error } {
  if (nodeName) {
    return { error: new Error('node already started') };
  }
  nodeName = name;

  // Set up BroadcastChannel for same-origin communication
  try {
    broadcastChannel = new BroadcastChannel(channelNamePrefix + name);
    broadcastChannel.onmessage = (event: any) => {
      handleIncoming(event.data as WireMessage);
    };
  } catch {
    // BroadcastChannel not available (e.g., in workers)
    broadcastChannel = null;
  }

  return { ok: Proc.self() };
}

export function stop(): void | { error: Error } {
  if (!nodeName) {
    return { error: new Error('node not started') };
  }
  if (broadcastChannel) {
    broadcastChannel.close();
    broadcastChannel = null;
  }
  // Notify monitors
  for (const ref of nodeMonitors) {
    Proc.send(Proc.self(), { type: 'node_disconnected', ref, node: nodeName });
  }
  nodeName = null;
  connectedNodes.clear();
}

// ---- self / alive? --------------------------------------------------------

export function self(): string {
  if (!nodeName) throw new Error('node not started');
  return nodeName;
}

export function alive(): boolean {
  return nodeName !== null;
}

// ---- connect / disconnect -------------------------------------------------

export function connect(node: string): boolean | 'ignored' {
  if (!nodeName) return false;
  if (connectedNodes.has(node)) return 'ignored';
  connectedNodes.add(node);

  // Send ping via BroadcastChannel
  if (broadcastChannel) {
    broadcastChannel.postMessage({
      from: nodeName,
      to: node,
      type: 'connect',
      payload: null,
    });
  }
  return true;
}

export function disconnect(node: string): void {
  connectedNodes.delete(node);
}

// ---- ping -----------------------------------------------------------------

export function ping(node: string): 'pong' | 'pang' {
  // Ponytail: true bidirectional ping. For now, check if we think we're connected.
  return connectedNodes.has(node) ? 'pong' : 'pang';
}

// ---- list -----------------------------------------------------------------

export function list(state?: string | string[]): string[] {
  if (!nodeName) return [];
  const states = state ? (Array.isArray(state) ? state : [state]) : ['visible', 'connected'];
  const result: string[] = [];
  if (states.includes('this') || states.includes('visible')) {
    result.push(nodeName);
  }
  if (states.includes('connected')) {
    result.push(...connectedNodes);
  }
  return result;
}

// ---- monitor --------------------------------------------------------------

export function monitor(node: string, flag: boolean): void {
  if (flag) {
    const ref: Ref = Symbol('node_monitor');
    nodeMonitors.add(ref);
  }
  // ponytail: specific node monitor references
}

// ---- spawn remote ---------------------------------------------------------

export function spawn(
  node: string,
  fnOrModule: (() => void) | any,
  fnName?: string,
  args?: any[],
): PID {
  if (!broadcastChannel) {
    throw new Error('BroadcastChannel not available');
  }

  let spawnPayload: unknown;

  if (typeof fnOrModule === 'function') {
    // Inline function — serialize it
    spawnPayload = {
      type: 'fn',
      source: fnOrModule.toString(),
    };
  } else {
    // Module/function/args style
    spawnPayload = {
      type: 'mfa',
      module: fnOrModule,
      fn: fnName,
      args: args ?? [],
    };
  }

  const fakePid = Proc.spawn(() => {
    broadcastChannel!.postMessage({
      from: nodeName!,
      to: node,
    type: 'spawn',
    payload: spawnPayload,
    });
  });

  return fakePid;
}

export function spawn_link(
  node: string,
  fnOrModule: (() => void) | any,
  fnName?: string,
  args?: any[],
): PID {
  return spawn(node, fnOrModule, fnName, args);
  // ponytail: actual cross-node linking
}

export function spawn_monitor(
  node: string,
  fnOrModule: (() => void) | any,
  fnName?: string,
  args?: any[],
): { pid: PID; ref: Ref } {
  const pid = spawn(node, fnOrModule, fnName, args);
  const ref: Ref = Symbol('monitor');
  return { pid, ref };
}

// ---- internal -------------------------------------------------------------

function handleIncoming(msg: WireMessage): void {
  if (msg.to && msg.to !== nodeName) return; // Not for us

  switch (msg.type) {
    case 'connect': {
      if (msg.from && msg.from !== nodeName) {
        connectedNodes.add(msg.from);
      }
      break;
    }
    case 'spawn': {
      const payload = msg.payload as any;
      if (payload.type === 'fn') {
        try {
          const fn = new Function(`return (${payload.source})`)();
          Proc.spawn(fn);
        } catch (_) {}
      } else if (payload.type === 'mfa') {
        const { module, fn: fnName, args } = payload;
        try {
          if (typeof module[fnName] === 'function') {
            module[fnName](...args);
          }
        } catch (_) {}
      }
      break;
    }
    // ponytail: more message types (ping/pong, etc.)
  }
}
