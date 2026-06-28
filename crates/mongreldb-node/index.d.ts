/* Hand-written wrapper around the NAPI-generated native.d.ts binding.
 * Adds a retryable `Database.prototype.transaction` helper and the
 * `ConflictError` class.
 */

export * from './native';

/** Retryable write-write conflict. */
export declare class ConflictError extends Error {
  constructor(message: string);
}

/** Callback used by `Database.prototype.transaction`. */
export type TransactionCallback = (txn: import('./native').Transaction) => void | Promise<void>;

/** Options for `Database.prototype.transaction`. */
export interface TransactionOptions {
  /** Maximum number of conflict retries before giving up. Default: 3. */
  maxRetries?: number;
  /** Base backoff delay in milliseconds. Default: 2. */
  baseDelayMs?: number;
}

export declare class Database extends import('./native').Database {
  /**
   * Run `fn(txn)` inside a cross-table transaction, retrying on conflict.
   * `fn` may be sync or async; it must stage operations on `txn` but must not
   * call `commit`/`rollback` itself.
   */
  transaction(fn: TransactionCallback, opts?: TransactionOptions): Promise<bigint>;
}
