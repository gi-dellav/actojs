// example: SQLite via bun:sqlite, wrapped in actor-based workers
// run: bun run examples/10_sqlite.ts

import { Database } from "bun:sqlite";
import * as Process from "../src/process";
import * as Agent from "../src/agent";

const db = new Database(":memory:");

db.run(`CREATE TABLE items (
  id    INTEGER PRIMARY KEY AUTOINCREMENT,
  name  TEXT NOT NULL,
  price REAL NOT NULL
)`);

const insert = db.prepare(
  "INSERT INTO items (name, price) VALUES ($name, $price)",
);
const selectAll = db.prepare("SELECT * FROM items");
const selectById = db.prepare("SELECT * FROM items WHERE id = $id");
const deleteById = db.prepare("DELETE FROM items WHERE id = $id");

// Seed some rows
insert.run({ $name: "Widget", $price: 9.99 });
insert.run({ $name: "Gadget", $price: 19.5 });
insert.run({ $name: "Doohickey", $price: 4.75 });

// Writer agent: performs insert/delete operations and tracks the row count
const { ok: writerAgent } = await Agent.start_link<{ count: number }>(() => ({
  count: 3,
}));

// Worker: writes a new row, reports count, then deletes an old row
const workerPid = Process.spawn(async () => {
  console.log(`[worker] PID: ${Process.self()}`);

  // Insert a row and update the agent count
  const { ok }: { ok: string } = await new Promise((resolve) => {
    insert.run({ $name: "Thingamajig", $price: 12.34 });
    const row = db.prepare("SELECT last_insert_rowid() as id").get() as {
      id: number;
    };
    resolve({ ok: `inserted row ${row.id}` });
  });
  console.log(`[worker] ${ok}`);

  await Agent.update(writerAgent, (state) => ({ count: state.count + 1 }));
  Process.send(Process.self(), { type: "read_all" });

  // Wait for the read_all message we sent ourselves
  await Process.receive(5000);

  const rows = selectAll.all();
  console.log(`[worker] current rows: ${JSON.stringify(rows)}`);
  const count = await Agent.get(writerAgent, (s) => s.count);
  console.log(`[worker] agent-tracked count: ${count}`);

  // Delete the first row
  deleteById.run({ $id: 1 });
  await Agent.update(writerAgent, (state) => ({ count: state.count - 1 }));
  console.log(
    `[worker] deleted id=1, agent count now: ${await Agent.get(writerAgent, (s) => s.count)}`,
  );

  // Final read
  const final = selectAll.all();
  console.log(`[worker] final rows: ${JSON.stringify(final)}`);
});

// Reader actor: queries specific rows concurrently
const readerPid = Process.spawn(async () => {
  await Process.sleep(50);

  const row2 = selectById.get({ $id: 2 }) as {
    id: number;
    name: string;
    price: number;
  } | null;
  console.log(
    `[reader] row id=2 → ${row2 ? JSON.stringify(row2) : "not found"}`,
  );

  const row99 = selectById.get({ $id: 99 }) as {
    id: number;
    name: string;
    price: number;
  } | null;
  console.log(
    `[reader] row id=99 → ${row99 ? JSON.stringify(row99) : "not found"}`,
  );

  // Transaction via actor proxy
  const insertMany = db.transaction(
    (items: { $name: string; $price: number }[]) => {
      let count = 0;
      for (const item of items) {
        insert.run(item);
        count++;
      }
      return count;
    },
  );

  const inserted = insertMany([
    { $name: "Batch-A", $price: 1.11 },
    { $name: "Batch-B", $price: 2.22 },
  ]);
  await Agent.update(writerAgent, (state) => ({
    count: state.count + inserted,
  }));
  console.log(`[reader] transaction inserted ${inserted} rows`);
});

await Process.sleep(1000);

console.log(
  `[main] final agent count = ${await Agent.get(writerAgent, (s) => s.count)}`,
);

await Agent.stop(writerAgent);
db.close();
console.log("[done]");
