/* Hand-written wrapper around the NAPI-generated native.d.ts binding.
 * Adds a retryable `Database.prototype.transaction` helper and the
 * `ConflictError` class.
 */

export * from './native';

import type { NativeRemoteQueryErrorDetails } from './native';

/** Retryable write-write conflict. */
export declare class ConflictError extends Error {
  constructor(message: string);
}

/** Structured rejection from NativeSqlQuery result methods. */
export interface NativeQueryError extends Error {
  code: string;
  queryId: string;
  outcomeKnown: boolean;
  committed: boolean | null;
  committedStatements: number | null;
  lastCommitEpoch: bigint | null;
  lastCommitEpochText: string | null;
  firstCommitStatementIndex: number | null;
  lastCommitStatementIndex: number | null;
  completedStatements: number | null;
  statementIndex: number | null;
  cancelOutcome: import('./native').NativeCancelOutcome | null;
  cancellationReason: string;
  retryable: boolean;
  serverState: string;
  terminalState: string | null;
}

/** Structured rejection from daemon-backed operations. */
export interface RemoteQueryError extends Error, NativeRemoteQueryErrorDetails {
  remoteQueryError: NativeRemoteQueryErrorDetails;
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

/** Remote wrapper that preserves structured server errors. */
export declare class RemoteDatabase extends import('./native').RemoteDatabase {}
