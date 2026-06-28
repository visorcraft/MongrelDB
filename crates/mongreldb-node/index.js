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

const CONFLICT_RE = /^__CONFLICT__:/;

function isConflict(err) {
  return err != null && typeof err.message === 'string' && CONFLICT_RE.test(err.message);
}

/** Database wrapper that adds a `transaction(fn, opts?)` retry helper. */
class Database extends native.Database {
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

module.exports = {
  ...native,
  Database,
  ConflictError,
};
