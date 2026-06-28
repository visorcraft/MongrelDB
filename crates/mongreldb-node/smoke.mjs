import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { Database, ConditionKind, ColumnType, ConflictError } = require('./index.js');
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
    { columnId: 1, int64: BigInt(i) },
    { columnId: 2, int64: BigInt(i * 10) },
  ]);
}
for (let i = 0; i < 50; i++) {
  const tag = i % 2 === 0 ? 'even' : 'odd';
  tableB.put([
    { columnId: 1, int64: BigInt(i) },
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
assert(row.cells[0].int64 === 5n, `row id is 5, got ${row.cells[0].int64}`);

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
    { columnId: 1, int64: BigInt(i) },
    { columnId: 2, int64: BigInt(i) },
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
const { Transaction } = require('./index.js');
const tx = new Transaction(db3);
tx.put('orders', [
  { columnId: 1, int64: 1n },
  { columnId: 2, int64: 100n },
]);
tx.put('customers', [
  { columnId: 1, int64: 1n },
  { columnId: 2, int64: 1n },
]);
const epoch = tx.commit();
assert(typeof epoch === 'bigint', `epoch is bigint, got ${typeof epoch}`);

assert(db3.getTable('orders').count() === 1n, 'order committed');
assert(db3.getTable('customers').count() === 1n, 'customer committed');

// Rollback test.
const tx2 = new Transaction(db3);
tx2.put('orders', [
  { columnId: 1, int64: 2n },
  { columnId: 2, int64: 200n },
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
const { WriteBuffer } = require('./index.js');
const wb = new WriteBuffer(wTable, 10);
for (let i = 0; i < 25; i++) {
  wb.put([
    { columnId: 1, int64: BigInt(i) },
    { columnId: 2, int64: BigInt(i * 2) },
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
tx3.put('vis', [{ columnId: 1, int64: 1n }, { columnId: 2, int64: 42n }]);
// Before commit: vis table has 0 rows.
assert(db4.getTable('vis').count() === 0n, 'pre-commit: 0 rows');
tx3.commit();
// After commit: vis table has 1 row.
assert(db4.getTable('vis').count() === 1n, 'post-commit: 1 row');

db4.close();
rmSync(dir4, { recursive: true });
console.log('smoke: WriteBuffer + atomic visibility ✓');

// ── New surface: putBatch / bulkLoadTyped / queryArrow / begin / async ──────

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
  [{ columnId: 1, int64: 1n }, { columnId: 2, int64: 7n }],
  [{ columnId: 1, int64: 2n }, { columnId: 2, int64: 7n }],
  [{ columnId: 1, int64: 3n }, { columnId: 2, int64: 9n }],
]);
assert(batchIds.length === 3, `putBatch returns 3 ids, got ${batchIds.length}`);
await nums.flushAsync(); // flush to a sorted run (also exercises flushAsync)
assert(nums.count() === 3n, `putBatch count ${nums.count()}`);

// queryArrow: matching rows as Arrow IPC bytes — verify the IPC file magic.
const arrow = nums.queryArrow([
  { kind: ConditionKind.RangeInt, columnId: 2, int64Lo: 7n, int64Hi: 7n },
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
tx5.put('nums', [{ columnId: 1, int64: 99n }, { columnId: 2, int64: 5n }]);
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
  st.put([{ columnId: 1, int64: 7n }, { columnId: 2, int64: 42n }]);
  st.flush();
  edb.close();
}
// Reopen with the correct passphrase → data is readable.
{
  const edb = Database.openEncrypted(dir6, PASS);
  assert(edb.getTable('secret').count() === 1n, 'encrypted reopen sees the row');
  const r = edb.getTable('secret').get(0n);
  assert(r !== undefined && r.cells[1].int64 === 42n, 'encrypted value round-trips');
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
  ta.put([{ columnId: 1, int64: 1n }, { columnId: 2, int64: 10n }]);
  ta.putBatch([
    [{ columnId: 1, int64: 2n }, { columnId: 2, int64: 20n }],
    [{ columnId: 1, int64: 3n }, { columnId: 2, int64: 30n }],
  ]);
  tx.table('b').put([{ columnId: 1, int64: 1n }, { columnId: 2, int64: 99n }]);
  // Atomic: nothing visible until the parent transaction commits.
  assert(db7.getTable('a').count() === 0n, 'pre-commit a: 0');
  tx.commit();
  assert(db7.getTable('a').count() === 3n, `a after commit: ${db7.getTable('a').count()}`);
  assert(db7.getTable('b').count() === 1n, `b after commit: ${db7.getTable('b').count()}`);

  // Flat API still works alongside the sub-API (backwards compatible).
  const tx2 = db7.begin();
  tx2.put('a', [{ columnId: 1, int64: 4n }, { columnId: 2, int64: 40n }]); // flat
  tx2.table('b').put([{ columnId: 1, int64: 2n }, { columnId: 2, int64: 88n }]); // sub-API
  tx2.commit();
  assert(db7.getTable('a').count() === 4n, `flat add: ${db7.getTable('a').count()}`);
  assert(db7.getTable('b').count() === 2n, `sub add: ${db7.getTable('b').count()}`);

  // Sub-API delete: capture a real row id via a direct put, then delete it.
  const rid = db7.getTable('a').put([{ columnId: 1, int64: 100n }, { columnId: 2, int64: 1n }]);
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

// ── Full-range Int64 / BigInt round-trip ───────────────────────────────────
{
  const dir8 = makeTempDir();
  const db8 = Database.withPath(dir8);
  const i64Schema = {
    columns: [
      { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
      { id: 2, name: 'value', ty: 1, primaryKey: false, nullable: false },
    ],
    indexes: [{ name: 'value_idx', columnId: 2, kind: 0 }],
  };
  db8.createTable('i64', i64Schema);
  const i64 = db8.getTable('i64');

  const max = 9_223_372_036_854_775_807n;
  const min = -9_223_372_036_854_775_808n;
  const ridMax = i64.put([
    { columnId: 1, int64: 1n },
    { columnId: 2, int64: max },
  ]);
  const ridMin = i64.put([
    { columnId: 1, int64: 2n },
    { columnId: 2, int64: min },
  ]);
  i64.commit();

  const rMax = i64.get(ridMax);
  const rMin = i64.get(ridMin);
  assert(rMax !== undefined, 'max row read');
  assert(rMin !== undefined, 'min row read');
  assert(rMax.cells[1].int64 === max, `max round-trip: ${rMax.cells[1].int64}`);
  assert(rMin.cells[1].int64 === min, `min round-trip: ${rMin.cells[1].int64}`);

  // RangeInt query with BigInt bounds.
  const range = i64.query([
    { kind: ConditionKind.RangeInt, columnId: 2, int64Lo: min, int64Hi: 0n },
  ]);
  assert(range.length === 1, `one row in negative range, got ${range.length}`);
  assert(range[0].cells[1].int64 === min, 'RangeInt returns the negative min');

  const all = i64.query([
    { kind: ConditionKind.RangeInt, columnId: 2, int64Lo: min, int64Hi: max },
  ]);
  assert(all.length === 2, `both rows in full int64 range, got ${all.length}`);

  db8.close();
  rmSync(dir8, { recursive: true });
}
console.log('smoke: full-range Int64 / BigInt ✓');

// ── Typed primary-key get/delete ───────────────────────────────────────────
{
  const dir9 = makeTempDir();
  const db9 = Database.withPath(dir9);

  // Text primary-key table.
  db9.createTable('text_pk', {
    columns: [
      { id: 1, name: 'id', ty: 5, primaryKey: true, nullable: false },
      { id: 2, name: 'v', ty: 1, primaryKey: false, nullable: false },
    ],
    indexes: [],
  });
  const textPk = db9.getTable('text_pk');
  textPk.put([
    { columnId: 1, text: 'alpha' },
    { columnId: 2, int64: 100n },
  ]);
  textPk.put([
    { columnId: 1, text: 'beta' },
    { columnId: 2, int64: 200n },
  ]);
  textPk.commit();

  const gotAlpha = textPk.getByPkText('alpha');
  assert(gotAlpha !== null, 'getByPkText finds alpha');
  assert(gotAlpha.cells[1].int64 === 100n, 'alpha value round-trips');

  textPk.deleteByPkText('alpha');
  textPk.commit();
  assert(textPk.getByPkText('alpha') === null, 'alpha deleted');
  assert(textPk.getByPkText('beta') !== null, 'beta remains');

  // Int64 primary-key table.
  db9.createTable('int64_pk', {
    columns: [
      { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
      { id: 2, name: 'v', ty: 5, primaryKey: false, nullable: false },
    ],
    indexes: [],
  });
  const int64Pk = db9.getTable('int64_pk');
  int64Pk.put([
    { columnId: 1, int64: 42n },
    { columnId: 2, text: 'forty-two' },
  ]);
  int64Pk.put([
    { columnId: 1, int64: -7n },
    { columnId: 2, text: 'negative-seven' },
  ]);
  int64Pk.commit();

  const got42 = int64Pk.getByPkInt64(42n);
  assert(got42 !== null, 'getByPkInt64 finds 42');
  assert(got42.cells[1].text === 'forty-two', '42 value round-trips');

  const gotNeg = int64Pk.getByPkInt64(-7n);
  assert(gotNeg !== null, 'getByPkInt64 finds -7');

  int64Pk.deleteByPkInt64(42n);
  int64Pk.commit();
  assert(int64Pk.getByPkInt64(42n) === null, '42 deleted');
  assert(int64Pk.getByPkInt64(-7n) !== null, '-7 remains');

  // Direct row-id delete via TableHandle.delete.
  const rid = textPk.put([
    { columnId: 1, text: 'gamma' },
    { columnId: 2, int64: 300n },
  ]);
  textPk.commit();
  assert(textPk.get(rid) !== null, 'gamma inserted');
  textPk.delete(rid);
  textPk.commit();
  assert(textPk.get(rid) === null, 'gamma deleted by row id');

  db9.close();
  rmSync(dir9, { recursive: true });
}
console.log('smoke: typed primary-key get/delete ✓');

// ── A3: catalog-aware addColumn ────────────────────────────────────────────
{
  const dirA3 = makeTempDir();
  const dbA3 = Database.withPath(dirA3);
  dbA3.createTable('evolve', {
    columns: [
      { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
      { id: 2, name: 'v', ty: ColumnType.Int64, primaryKey: false, nullable: false },
    ],
    indexes: [],
  });
  const evolve = dbA3.getTable('evolve');
  evolve.put([
    { columnId: 1, int64: 1n },
    { columnId: 2, int64: 100n },
  ]);
  evolve.commit();

  // Adding a non-null column without a default must be rejected.
  assert.throws(
    () =>
      dbA3.addColumn('evolve', {
        id: 3,
        name: 'missing_default',
        ty: ColumnType.Int64,
        primaryKey: false,
        nullable: false,
      }),
    /non-null column added without default/
  );

  // Add a nullable Int64 column.
  const newColId = dbA3.addColumn('evolve', {
    id: 3,
    name: 'extra',
    ty: ColumnType.Int64,
    primaryKey: false,
    nullable: true,
  });
  assert(typeof newColId === 'bigint', 'addColumn returns a column id');

  // Existing row reads back with null in the new column.
  const oldRow = evolve.get(0n);
  assert(oldRow !== null, 'old row still readable');
  const extraCell = oldRow.cells.find((c) => c.columnId === 3);
  assert(extraCell !== undefined, 'new column present in row');
  assert(extraCell.int64 === undefined || extraCell.int64 === null, 'new column is null');

  dbA3.close();
  rmSync(dirA3, { recursive: true });
}
console.log('smoke: A3 catalog-aware addColumn ✓');

// ── A4: backup and integrity primitives ────────────────────────────────────
{
  const dirA4 = makeTempDir();
  const dbA4 = Database.withPath(dirA4);
  dbA4.createTable('check_t', {
    columns: [
      { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
    ],
    indexes: [],
  });
  dbA4.getTable('check_t').put([{ columnId: 1, int64: 1n }]);
  dbA4.getTable('check_t').commit();

  const checkJson = dbA4.check();
  const checkReport = JSON.parse(checkJson);
  assert(checkReport.ok === true, 'check reports ok on fresh db');
  assert(Array.isArray(checkReport.tables), 'check report has tables array');

  const doctorJson = dbA4.doctor();
  const doctorReport = JSON.parse(doctorJson);
  assert(doctorReport.ok === true, 'doctor reports ok on fresh db');
  assert(Array.isArray(doctorReport.quarantined), 'doctor report has quarantined array');

  assert(dbA4.directory() === dirA4, 'directory returns the creation path');

  dbA4.close();
  rmSync(dirA4, { recursive: true });
}
console.log('smoke: A4 check / doctor / directory ✓');

// ── A5: ConflictError + transaction(fn) retry wrapper ──────────────────────
{
  const dirA5 = makeTempDir();
  const dbA5 = Database.withPath(dirA5);
  dbA5.createTable('retry', {
    columns: [
      { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
      { id: 2, name: 'v', ty: ColumnType.Int64, primaryKey: false, nullable: false },
    ],
    indexes: [],
  });

  // Successful transaction helper run commits the staged write.
  const epoch = await dbA5.transaction((txn) => {
    txn.put('retry', [
      { columnId: 1, int64: 1n },
      { columnId: 2, int64: 42n },
    ]);
  });
  assert(typeof epoch === 'bigint', 'transaction helper returns epoch');
  assert(dbA5.getTable('retry').count() === 1n, 'transaction helper committed the write');

  // Non-conflict errors are re-thrown immediately.
  let threw = false;
  try {
    await dbA5.transaction(() => {
      throw new Error('boom');
    });
  } catch (e) {
    threw = true;
    assert(e.message === 'boom', 'non-conflict error is re-thrown');
  }
  assert(threw, 'non-conflict error propagated');

  // ConflictError class is exported.
  assert(new ConflictError('x') instanceof Error, 'ConflictError extends Error');

  dbA5.close();
  rmSync(dirA5, { recursive: true });
}
console.log('smoke: A5 ConflictError + transaction wrapper ✓');

// ── BigInt range validation ────────────────────────────────────────────────
{
  const dirRange = makeTempDir();
  const dbRange = Database.withPath(dirRange);
  dbRange.createTable('range', {
    columns: [
      { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
      { id: 2, name: 'v', ty: ColumnType.Int64, primaryKey: false, nullable: false },
    ],
    indexes: [{ name: 'v_idx', columnId: 2, kind: 0 }],
  });
  const range = dbRange.getTable('range');
  range.put([
    { columnId: 1, int64: 1n },
    { columnId: 2, int64: 1n },
  ]);
  range.commit();
  const rid = range.get(0n).rowId;

  const i64Max = 9_223_372_036_854_775_807n;
  const i64Over = i64Max + 1n;
  const u64Max = 18_446_744_073_709_551_615n;

  // Cell value out of i64 range.
  assert.throws(
    () => range.put([{ columnId: 1, int64: i64Over }]),
    /BigInt out of i64 range/,
    'put rejects out-of-range i64 cell'
  );

  // RangeInt bounds out of i64 range.
  assert.throws(
    () =>
      range.query([
        { kind: ConditionKind.RangeInt, columnId: 2, int64Lo: i64Over, int64Hi: i64Over },
      ]),
    /BigInt out of i64 range/,
    'RangeInt rejects out-of-range bound'
  );

  // Int64 primary-key lookup out of i64 range.
  assert.throws(
    () => range.getByPkInt64(i64Over),
    /BigInt out of i64 range/,
    'getByPkInt64 rejects out-of-range pk'
  );
  assert.throws(
    () => range.deleteByPkInt64(i64Over),
    /BigInt out of i64 range/,
    'deleteByPkInt64 rejects out-of-range pk'
  );

  // Row ids are u64; negative or too-large values are rejected.
  assert.throws(() => range.get(-1n), /BigInt out of u64 range/, 'get rejects negative row id');
  assert.throws(() => range.delete(-1n), /BigInt out of u64 range/, 'delete rejects negative row id');
  assert.throws(
    () => range.get(u64Max + 1n),
    /BigInt out of u64 range/,
    'get rejects too-large row id'
  );

  // Transaction delete also validates the row id.
  const txRange = dbRange.begin();
  assert.throws(
    () => txRange.delete('range', -1n),
    /BigInt out of u64 range/,
    'transaction delete rejects negative row id'
  );
  txRange.rollback();

  dbRange.close();
  rmSync(dirRange, { recursive: true });
}
console.log('smoke: BigInt range validation ✓');

console.log('All smoke tests passed.');
