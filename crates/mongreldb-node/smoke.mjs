import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { Database, ConditionKind } = require('./mongreldb.node');
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert';

function makeTempDir() {
  return mkdtempSync(join(tmpdir(), 'mongreldb-smoke-'));
}

// ── Multi-table API ───────────────────────────────────────────────────────

const dir1 = makeTempDir();
const db = Database.withPath(dir1);

// Create two tables.
const schemaA = {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
};
const schemaB = {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'tag', ty: 5, primaryKey: false, nullable: false },
  ],
  indexes: [{ name: 'tag_idx', columnId: 2, kind: 0 }],
};
db.createTable('a', schemaA);
db.createTable('b', schemaB);

assert(db.tableNames().length === 2, 'two tables');

// Write to each table.
const tableA = db.getTable('a');
const tableB = db.getTable('b');

for (let i = 0; i < 100; i++) {
  tableA.put([
    { columnId: 1, int64: i },
    { columnId: 2, int64: i * 10 },
  ]);
}
for (let i = 0; i < 50; i++) {
  const tag = i % 2 === 0 ? 'even' : 'odd';
  tableB.put([
    { columnId: 1, int64: i },
    { columnId: 2, bytes: Buffer.from(tag) },
  ]);
}

tableA.commit();
tableB.commit();

assert(tableA.count() === 100n, `tableA count ${tableA.count()}`);
assert(tableB.count() === 50n, `tableB count ${tableB.count()}`);

// Point read.
const row = tableA.get(5n);
assert(row !== undefined, 'get returns a row');
assert(row.cells[0].int64 === 5, `row id is 5, got ${row.cells[0].int64}`);

// Hybrid query on tableB.
const results = tableB.query([
  { kind: ConditionKind.BitmapEq, columnId: 2, text: 'even' },
]);
assert(results.length === 25, `25 even rows, got ${results.length}`);

// SQL query (cross-table).
const arrowBuf = await db.sql('SELECT COUNT(*) FROM a');
assert(arrowBuf.length > 0, 'SQL returns Arrow IPC bytes');

db.close();
rmSync(dir1, { recursive: true });
console.log('smoke: multi-table API ✓');

// ── Async variants ────────────────────────────────────────────────────────

const dir2 = makeTempDir();
const db2 = Database.withPath(dir2);
db2.createTable('t', schemaA);
const t = db2.getTable('t');

for (let i = 0; i < 50; i++) {
  await t.putAsync([
    { columnId: 1, int64: i },
    { columnId: 2, int64: i },
  ]);
}
await t.commitAsync();
const cnt = await t.countAsync();
assert(cnt === 50n, `async count ${cnt}`);

db2.close();
rmSync(dir2, { recursive: true });
console.log('smoke: async API ✓');

console.log('All smoke tests passed.');
