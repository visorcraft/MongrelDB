import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';

const require = createRequire(import.meta.url);
const { Database } = require('./index.js');

test('transaction retry uses conflict code without message marker', async () => {
  let attempts = 0;
  const database = {
    begin() {
      return {
        commit() {
          attempts += 1;
          if (attempts === 1) {
            throw Object.assign(new Error('plain write conflict'), {
              code: 'MONGRELDB_CONFLICT',
            });
          }
          return 7n;
        },
        rollback() {},
      };
    },
  };

  const epoch = await Database.prototype.transaction.call(database, () => {});
  assert.equal(epoch, 7n);
  assert.equal(attempts, 2);
});

test('NativeSqlQuery cancels during Arrow IPC encoding', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'mongreldb-napi-ipc-cancel-'));
  const database = Database.withPath(directory);
  try {
    const query = database.startSql(
      'SELECT * FROM generate_series(1, 2000000)',
      {
        queryId: '1234567890abcdef1234567890abcdef',
        timeoutMs: 30_000,
        maxOutputRows: 3_000_000,
        maxOutputBytes: 256 * 1024 * 1024,
      },
    );
    let cancelledDuringSerialization = false;
    const poll = setInterval(() => {
      const status = query.status();
      if (status.phase === 'serializing') {
        cancelledDuringSerialization = true;
        query.cancel();
        clearInterval(poll);
      }
    }, 1);

    await assert.rejects(query.resultArrow(), /cancel/i);
    clearInterval(poll);
    assert.equal(cancelledDuringSerialization, true);
    assert.deepEqual(
      {
        phase: query.status().phase,
        terminalState: query.status().terminalState,
        cancellationReason: query.status().cancellationReason,
      },
      {
        phase: 'cancelled',
        terminalState: 'cancelled_before_commit',
        cancellationReason: 'client_request',
      },
    );
  } finally {
    database.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test('NativeSqlQuery rejects with structured terminal fields', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'mongreldb-napi-error-fields-'));
  const database = Database.withPath(directory);
  try {
    const queryId = 'abcdef1234567890abcdef1234567890';
    const query = database.startSql(
      'SELECT 1 AS id UNION ALL SELECT 2 AS id',
      {
        queryId,
        timeoutMs: 30_000,
        maxOutputRows: 1,
        maxOutputBytes: 1024 * 1024,
      },
    );
    const error = await query.resultArrow().then(
      () => assert.fail('expected result limit'),
      (error) => error,
    );
    assert.deepEqual(
      {
        code: error.code,
        queryId: error.queryId,
        outcomeKnown: error.outcomeKnown,
        committed: error.committed,
        committedStatements: error.committedStatements,
        lastCommitEpoch: error.lastCommitEpoch,
        lastCommitEpochText: error.lastCommitEpochText,
        completedStatements: error.completedStatements,
        statementIndex: error.statementIndex,
        retryable: error.retryable,
        serverState: error.serverState,
        terminalState: error.terminalState,
      },
      {
        code: 'RESULT_LIMIT_EXCEEDED',
        queryId,
        outcomeKnown: true,
        committed: false,
        committedStatements: 0,
        lastCommitEpoch: null,
        lastCommitEpochText: null,
        completedStatements: 0,
        statementIndex: 0,
        retryable: false,
        serverState: 'failed',
        terminalState: 'failed_before_commit',
      },
    );
  } finally {
    database.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
