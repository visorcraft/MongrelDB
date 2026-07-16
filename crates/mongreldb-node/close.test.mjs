import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { Database } = require('./index.js');

const schema = {
  columns: [
    { id: 1, name: 'id', ty: 1, primaryKey: true, nullable: false },
  ],
  indexes: [],
};

test('close releases the database and invalidates retained table and transaction handles', () => {
  const directory = mkdtempSync(join(tmpdir(), 'mongreldb-napi-close-'));
  const database = Database.withPath(directory);
  database.createTable('items', schema);
  const table = database.getTable('items');
  table.put([{ columnId: 1, int64: 7n }]);
  const transaction = database.begin();

  database.close();
  database.close();
  assert.throws(() => database.tableNames(), /MONGRELDB_DATABASE_CLOSED/);
  assert.throws(() => table.count(), /MONGRELDB_DATABASE_CLOSED/);
  assert.throws(() => transaction.commit(), /MONGRELDB_DATABASE_CLOSED/);

  const reopened = Database.open(directory);
  assert.equal(reopened.getTable('items').count(), 1n);
  reopened.close();
  rmSync(directory, { recursive: true, force: true });
});

test('close cancels an unstarted SQL handle without retaining the database lock', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'mongreldb-napi-close-query-'));
  const database = Database.withPath(directory);
  const query = database.startSql('SELECT 1');

  database.close();
  const reopened = Database.open(directory);
  reopened.close();
  await assert.rejects(query.result(), (error) => {
    assert.equal(error.code, 'MONGRELDB_DATABASE_CLOSED');
    return true;
  });
  rmSync(directory, { recursive: true, force: true });
});
