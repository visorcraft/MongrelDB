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

// ── New surface: putBatch / bulkLoadTyped / queryArrow / begin / async ──────

const { ColumnType } = require('./mongreldb.node');
const dir5 = makeTempDir();
const db5 = Database.withPath(dir5);
db5.createTable('nums', {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [{ name: 'v_idx', columnId: 2, kind: 0 }],
});

const nums = db5.table('nums'); // table() alias for getTable()
const epoch0 = db5.snapshotEpoch();
assert(typeof epoch0 === 'bigint', 'snapshotEpoch is bigint');

// putBatch: three rows in one call → three row ids.
const batchIds = nums.putBatch([
  [{ columnId: 1, int64: 1 }, { columnId: 2, int64: 7 }],
  [{ columnId: 1, int64: 2 }, { columnId: 2, int64: 7 }],
  [{ columnId: 1, int64: 3 }, { columnId: 2, int64: 9 }],
]);
assert(batchIds.length === 3, `putBatch returns 3 ids, got ${batchIds.length}`);
await nums.flushAsync(); // flush to a sorted run (also exercises flushAsync)
assert(nums.count() === 3n, `putBatch count ${nums.count()}`);

// queryArrow: matching rows as Arrow IPC bytes — verify the IPC file magic.
const arrow = nums.queryArrow([
  { kind: ConditionKind.RangeInt, columnId: 2, int64Lo: 7, int64Hi: 7 },
]);
assert(arrow.length > 0, 'queryArrow returns bytes');
assert(arrow.subarray(0, 6).toString('ascii') === 'ARROW1', 'queryArrow emits Arrow IPC');

// bulkLoadTyped: typed Int64 columns straight from JS BigInt64Array buffers.
db5.createTable('bulk', {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
});
const bulk = db5.table('bulk');
const toBuf = (a) => Buffer.from(a.buffer, a.byteOffset, a.byteLength);
const bulkEpoch = bulk.bulkLoadTyped([
  { columnId: 1, ty: ColumnType.Int64, data: toBuf(new BigInt64Array([10n, 11n, 12n])) },
  { columnId: 2, ty: ColumnType.Int64, data: toBuf(new BigInt64Array([100n, 110n, 120n])) },
]);
assert(typeof bulkEpoch === 'bigint', 'bulkLoadTyped returns an epoch');
assert(bulk.count() === 3n, `bulkLoadTyped count ${bulk.count()}`);

// db.begin() factory + commitAsync().
const tx5 = db5.begin();
tx5.put('nums', [{ columnId: 1, int64: 99 }, { columnId: 2, int64: 5 }]);
const txEpoch = await tx5.commitAsync();
assert(typeof txEpoch === 'bigint', 'commitAsync returns an epoch');
assert(nums.count() === 4n, `after txn count ${nums.count()}`);
assert(db5.snapshotEpoch() >= epoch0, 'snapshotEpoch advances');

db5.close();
rmSync(dir5, { recursive: true });
console.log('smoke: putBatch / bulkLoadTyped / queryArrow / begin / async ✓');

// ── Encrypted database round-trip (encryption ships on by default) ──────────

const dir6 = makeTempDir();
const PASS = 'qa-verify-passphrase';
const secretSchema = {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
    { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
  ],
  indexes: [],
};
{
  const edb = Database.createEncrypted(dir6, PASS);
  edb.createTable('secret', secretSchema);
  const st = edb.getTable('secret');
  st.put([{ columnId: 1, int64: 7 }, { columnId: 2, int64: 42 }]);
  st.flush();
  edb.close();
}
// Reopen with the correct passphrase → data is readable.
{
  const edb = Database.openEncrypted(dir6, PASS);
  assert(edb.getTable('secret').count() === 1n, 'encrypted reopen sees the row');
  const r = edb.getTable('secret').get(0n);
  assert(r !== undefined && r.cells[1].int64 === 42, 'encrypted value round-trips');
  edb.close();
}
// Wrong passphrase is rejected.
assert.throws(() => Database.openEncrypted(dir6, 'wrong-passphrase'), 'wrong passphrase rejected');
rmSync(dir6, { recursive: true });
console.log('smoke: encrypted round-trip ✓');

// ── TxnTable sub-API: tx.table(name).put/delete — additive, backwards compatible ──
{
  const dir7 = makeTempDir();
  const db7 = Database.withPath(dir7);
  const sch = {
    columns: [
      { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
      { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
    ],
    indexes: [],
  };
  db7.createTable('a', sch);
  db7.createTable('b', sch);

  // Scope ops to a table once; put/putBatch stage into ONE transaction.
  const tx = db7.begin();
  const ta = tx.table('a');
  ta.put([{ columnId: 1, int64: 1 }, { columnId: 2, int64: 10 }]);
  ta.putBatch([
    [{ columnId: 1, int64: 2 }, { columnId: 2, int64: 20 }],
    [{ columnId: 1, int64: 3 }, { columnId: 2, int64: 30 }],
  ]);
  tx.table('b').put([{ columnId: 1, int64: 1 }, { columnId: 2, int64: 99 }]);
  // Atomic: nothing visible until the parent transaction commits.
  assert(db7.getTable('a').count() === 0n, 'pre-commit a: 0');
  tx.commit();
  assert(db7.getTable('a').count() === 3n, `a after commit: ${db7.getTable('a').count()}`);
  assert(db7.getTable('b').count() === 1n, `b after commit: ${db7.getTable('b').count()}`);

  // Flat API still works alongside the sub-API (backwards compatible).
  const tx2 = db7.begin();
  tx2.put('a', [{ columnId: 1, int64: 4 }, { columnId: 2, int64: 40 }]); // flat
  tx2.table('b').put([{ columnId: 1, int64: 2 }, { columnId: 2, int64: 88 }]); // sub-API
  tx2.commit();
  assert(db7.getTable('a').count() === 4n, `flat add: ${db7.getTable('a').count()}`);
  assert(db7.getTable('b').count() === 2n, `sub add: ${db7.getTable('b').count()}`);

  // Sub-API delete: capture a real row id via a direct put, then delete it.
  const rid = db7.getTable('a').put([{ columnId: 1, int64: 100 }, { columnId: 2, int64: 1 }]);
  db7.getTable('a').commit();
  const before = db7.getTable('a').count();
  const txDel = db7.begin();
  txDel.table('a').delete(rid);
  txDel.commit();
  assert(db7.getTable('a').count() === before - 1n, 'sub-API delete removed the row');

  db7.close();
  rmSync(dir7, { recursive: true });
}
console.log('smoke: TxnTable sub-API ✓');

console.log('All smoke tests passed.');
