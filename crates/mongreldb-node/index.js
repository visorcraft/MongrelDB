/* eslint-disable */
/* Hand-written wrapper around the NAPI-generated native.js binding.
 * Adds a retryable `Database.prototype.transaction` helper and exports the
 * `ConflictError` class so callers can distinguish retryable write conflicts.
 */

const native = require('./native.js');

/** Retryable write-write conflict. */
class ConflictError extends Error {
  constructor(message) {
    super(message);
    this.name = 'ConflictError';
  }
}

function isConflict(err) {
  return err != null && err.code === 'MONGRELDB_CONFLICT';
}

function enrichSqlError(error, query) {
  if (!error || typeof error !== 'object') return error;
  if (String(error.message).includes('MONGRELDB_DATABASE_CLOSED')) {
    error.code = 'MONGRELDB_DATABASE_CLOSED';
    return error;
  }
  let status;
  try {
    status = query.status();
  } catch (_) {
    return error;
  }
  const outcomeKnown = status.outcomeKnown === true;
  const durable = status.durableOutcome ?? {};
  const lastCommitEpoch = outcomeKnown ? (durable.lastCommitEpoch ?? null) : null;
  Object.assign(error, {
    queryId: status.queryId,
    outcomeKnown,
    committed: outcomeKnown ? (status.committed ?? false) : null,
    committedStatements: outcomeKnown ? (durable.committedStatements ?? 0) : null,
    lastCommitEpoch,
    lastCommitEpochText: lastCommitEpoch === null ? null : lastCommitEpoch.toString(),
    firstCommitStatementIndex: outcomeKnown
      ? (durable.firstCommitStatementIndex ?? null)
      : null,
    lastCommitStatementIndex: outcomeKnown
      ? (durable.lastCommitStatementIndex ?? null)
      : null,
    completedStatements: outcomeKnown ? (status.completedStatements ?? 0) : null,
    statementIndex: outcomeKnown ? (status.statementIndex ?? 0) : null,
    cancelOutcome: status.cancelOutcome ?? null,
    cancellationReason: status.cancellationReason,
    retryable: status.retryable,
    serverState: status.serverState,
    terminalState: status.terminalState ?? null,
  });
  if (status.terminalErrorCode) error.code = status.terminalErrorCode;
  return error;
}

function enrichRemoteQueryError(error) {
  if (!error || typeof error !== 'object') return error;
  const details = error.remoteQueryError;
  if (!details || typeof details !== 'object') return error;
  Object.assign(error, details);
  return error;
}

for (const method of ['result', 'resultArrow', 'resultRows']) {
  const original = native.NativeSqlQuery.prototype[method];
  native.NativeSqlQuery.prototype[method] = async function (...args) {
    try {
      return await original.apply(this, args);
    } catch (error) {
      throw enrichSqlError(error, this);
    }
  };
}

const remoteResult = native.NativeRemoteSqlQuery.prototype.result;
native.NativeRemoteSqlQuery.prototype.result = async function (...args) {
  try {
    return await remoteResult.apply(this, args);
  } catch (error) {
    throw enrichRemoteQueryError(error);
  }
};

/** Database wrapper that adds a `transaction(fn, opts?)` retry helper. */
class Database extends native.Database {
  sql(sql) {
    return this.startSql(sql).result();
  }

  sqlWithOptions(sql, options) {
    return this.startSql(sql, options).result();
  }

  /**
   * Run `fn(txn)` inside a cross-table transaction, retrying on conflict.
   * `fn` may be sync or async; it must stage operations on `txn` but must not
   * call `commit`/`rollback` itself.
   */
  transaction(fn, opts = {}) {
    const maxRetries = opts.maxRetries ?? 3;
    const baseDelayMs = opts.baseDelayMs ?? 2;

    const runOnce = async () => {
      const txn = this.begin();
      try {
        await fn(txn);
        return txn.commit();
      } catch (err) {
        try {
          txn.rollback();
        } catch (_) {
          // ignore rollback errors
        }
        throw err;
      }
    };

    const attempt = async (retriesLeft) => {
      try {
        return await runOnce();
      } catch (err) {
        if (retriesLeft > 0 && isConflict(err)) {
          const delay = baseDelayMs * (maxRetries - retriesLeft + 1);
          await new Promise((resolve) => setTimeout(resolve, delay));
          return attempt(retriesLeft - 1);
        }
        throw err;
      }
    };

    return attempt(maxRetries);
  }
}

class RemoteDatabase extends native.RemoteDatabase {
  sql(sql) {
    return this.startSql(sql).result();
  }

  sqlWithOptions(sql, options) {
    return this.startSql(sql, options).result();
  }

  queryStatus(queryId) {
    try {
      return super.queryStatus(queryId);
    } catch (error) {
      throw enrichRemoteQueryError(error);
    }
  }
}

module.exports = {
  ...native,
  Database,
  RemoteDatabase,
  ConflictError,
};
