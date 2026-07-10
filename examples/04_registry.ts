// example: Registry — decentralised key-value process store
// run: bun run examples/04_registry.ts

import * as Registry from '../src/registry';
import * as Process from '../src/process';

// Start a registry with unique keys
const { ok: reg } = await Registry.start_link({
  keys: 'unique',
  name: 'MyRegistry',
});

// Register ourselves under some keys
const me = Process.self();
await Registry.register(reg, 'role', 'worker');
await Registry.register(reg, 'region', 'us-east');

// Look up entries for a key
const roles = await Registry.lookup(reg, 'role');
console.log(`[lookup role] entries: ${JSON.stringify(roles)}`);

// Match entries by value pattern
const matchResult = await Registry.match(reg, 'region', 'us-east');
console.log(`[match region] entries: ${JSON.stringify(matchResult)}`);

// Count total registered keys
const total = await Registry.count(reg);
console.log(`[count] total entries: ${total}`);

// Get all keys for our PID
const ourKeys = await Registry.keys(reg, me);
console.log(`[keys] our keys: ${ourKeys}`);

// Update value for an existing key
const updated = await Registry.update_value(reg, 'role', (_old: unknown) => 'lead');
console.log(`[update_value] result: ${JSON.stringify(updated)}`);

// Unregister
await Registry.unregister(reg, 'role');
console.log(`[after unregister] count: ${await Registry.count(reg)}`);

console.log('[done]');
