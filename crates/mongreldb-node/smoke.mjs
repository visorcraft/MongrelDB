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

// ── Cross-table Transaction + ConflictError ───────────────────────────────

const dir3 = makeTempDir();
const db3 = Database.withPath(dir3);
db3.createTable('orders', {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'amount', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
});
db3.createTable('customers', {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'orders', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
});

// Atomic cross-table transaction.
const { Transaction } = require('./mongreldb.node');
const tx = new Transaction(db3);
tx.put('orders', [
  { columnId: 1, int64: 1 },
  { columnId: 2, int64: 100 },
]);
tx.put('customers', [
  { columnId: 1, int64: 1 },
  { columnId: 2, int64: 1 },
]);
const epoch = tx.commit();
assert(typeof epoch === 'bigint', `epoch is bigint, got ${typeof epoch}`);

assert(db3.getTable('orders').count() === 1n, 'order committed');
assert(db3.getTable('customers').count() === 1n, 'customer committed');

// Rollback test.
const tx2 = new Transaction(db3);
tx2.put('orders', [
  { columnId: 1, int64: 2 },
  { columnId: 2, int64: 200 },
]);
tx2.rollback();
assert(db3.getTable('orders').count() === 1n, 'rollback leaves 1 row');

db3.close();
rmSync(dir3, { recursive: true });
console.log('smoke: cross-table Transaction ✓');

// ── WriteBuffer from Table + atomic visibility ─────────────────────────────

const dir4 = makeTempDir();
const db4 = Database.withPath(dir4);
db4.createTable('w', {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
});

const wTable = db4.getTable('w');
const { WriteBuffer } = require('./mongreldb.node');
const wb = new WriteBuffer(wTable, 10);
for (let i = 0; i < 25; i++) {
  wb.put([
    { columnId: 1, int64: i },
    { columnId: 2, int64: i * 2 },
  ]);
}
wb.flush();
assert(wTable.count() === 25n, `writebuffer count ${wTable.count()}`);

// Atomic cross-table visibility: a transaction's writes are not visible until commit.
db4.createTable('vis', {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
});
const tx3 = new Transaction(db4);
tx3.put('vis', [{ columnId: 1, int64: 1 }, { columnId: 2, int64: 42 }]);
// Before commit: vis table has 0 rows.
assert(db4.getTable('vis').count() === 0n, 'pre-commit: 0 rows');
tx3.commit();
// After commit: vis table has 1 row.
assert(db4.getTable('vis').count() === 1n, 'post-commit: 1 row');

db4.close();
rmSync(dir4, { recursive: true });
console.log('smoke: WriteBuffer + atomic visibility ✓');

console.log('All smoke tests passed.');
