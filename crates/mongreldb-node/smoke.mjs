// Smoke test for the built mongreldb NAPI addon: put/get/count + a hybrid query
// (bitmap ∩ range). Run: node smoke.mjs (after `napi build`).

import { Database, ConditionKind } from './index.js';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const dir = mkdtempSync(join(tmpdir(), 'mongreldb-'));
const db = new Database(
  join(dir, 't'),
  {
    columns: [
      { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false }, // Int64
      { id: 2, name: 'cat', ty: 5, primaryKey: false, nullable: false }, // Bytes
      { id: 3, name: 'cost', ty: 2, primaryKey: false, nullable: false }, // Float64
    ],
    indexes: [{ name: 'cat_bm', columnId: 2, kind: 0 }], // Bitmap
  },
  1,
);

for (let i = 0; i < 100; i++) {
  db.put([
    { columnId: 1, int64: i },
    { columnId: 2, text: i % 2 === 0 ? 'even' : 'odd' },
    { columnId: 3, float64: i },
  ]);
}
db.flush();
console.log('count =', db.count());

const row = db.get(5n);
console.log('get(5) =', JSON.stringify(row.cells));

const rows = db.query([
  { kind: ConditionKind.BitmapEq, columnId: 2, text: 'even' },
  { kind: ConditionKind.RangeInt, columnId: 1, int64Lo: 10, int64Hi: 20 },
]);
console.log('bitmap(even) ∩ range(10..20) =>', rows.length, 'rows');
console.log(
  'ids:',
  rows.map((r) => r.cells.find((c) => c.columnId === 1).int64),
);
db.close();

// ── async (Promise) variants — offloaded to the tokio blocking pool ────────
const dir2 = mkdtempSync(join(tmpdir(), 'mongreldb-'));
const db2 = new Database(
  join(dir2, 't'),
  {
    columns: [
      { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
      { id: 2, name: 'cat', ty: 5, primaryKey: false, nullable: false },
      { id: 3, name: 'cost', ty: 2, primaryKey: false, nullable: false },
    ],
    indexes: [{ name: 'cat_bm', columnId: 2, kind: 0 }],
  },
  1,
);
for (let i = 0; i < 50; i++) {
  await db2.putAsync([
    { columnId: 1, int64: 1000 + i },
    { columnId: 2, text: i % 2 === 0 ? 'even' : 'odd' },
    { columnId: 3, float64: i },
  ]);
}
await db2.flushAsync();
const asyncRows = await db2.queryAsync([
  { kind: ConditionKind.BitmapEq, columnId: 2, text: 'even' },
]);
console.log('async query (even) =>', asyncRows.length, 'rows; count =', db2.count());
db2.close();
