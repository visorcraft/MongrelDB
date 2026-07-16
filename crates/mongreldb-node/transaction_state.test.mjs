import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { ColumnType, Database } = require('./index.js');

test('typed transaction and table handle reject staging during and after commit', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'mongreldb-node-txn-state-'));
  const database = Database.withPath(directory);
  try {
    database.createTable('items', {
      columns: [
        { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
      ],
      indexes: [],
    });
    const transaction = database.begin();
    const table = transaction.table('items');
    table.put([{ columnId: 1, int64: 1n }]);

    const commit = transaction.commitAsync();
    assert.throws(() => table.delete(1n), /commit is in flight|already committed/);
    assert.throws(() => transaction.truncate('items'), /commit is in flight|already committed/);
    await commit;

    assert.throws(() => table.put([{ columnId: 1, int64: 2n }]), /already committed/);
    assert.throws(() => transaction.commit(), /already committed/);
    assert.throws(() => transaction.rollback(), /already committed/);
    assert.equal(database.table('items').count(), 1n);
  } finally {
    database.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test('async conflict keeps the losing transaction retryable', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'mongreldb-node-txn-retry-'));
  const database = Database.withPath(directory);
  try {
    database.createTable('items', {
      columns: [
        { id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
        { id: 2, name: 'value', ty: ColumnType.Int64, primaryKey: false, nullable: false },
      ],
      indexes: [],
    });
    const items = database.table('items');
    items.put([
      { columnId: 1, int64: 1n },
      { columnId: 2, int64: 0n },
    ]);
    items.commit();
    let loser;
    for (let attempt = 0; attempt < 20 && !loser; attempt += 1) {
      const first = database.begin();
      const second = database.begin();
      first.upsert(
        'items',
        [{ columnId: 1, int64: 1n }, { columnId: 2, int64: BigInt(attempt * 2 + 1) }],
        [{ columnId: 2, int64: BigInt(attempt * 2 + 1) }],
      );
      second.upsert(
        'items',
        [{ columnId: 1, int64: 1n }, { columnId: 2, int64: BigInt(attempt * 2 + 2) }],
        [{ columnId: 2, int64: BigInt(attempt * 2 + 2) }],
      );
      const results = await Promise.allSettled([first.commitAsync(), second.commitAsync()]);
      if (results[0].status === 'rejected' && results[0].reason.code === 'MONGRELDB_CONFLICT') loser = first;
      if (results[1].status === 'rejected' && results[1].reason.code === 'MONGRELDB_CONFLICT') loser = second;
    }
    assert.ok(loser, 'expected one concurrent write conflict');
    assert.equal(typeof loser.commit(), 'bigint');
  } finally {
    database.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
