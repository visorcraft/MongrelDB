// NAPI addon vs node:sqlite (built into Node 24+): single-record
// insert/update/delete latency at N=100 and N=1,000,000 rows. Mirrors the
// engine's mongreldb-perf methodology (median of 7 durable single-op
// timings). "Update" is `put` with an existing PK -- the addon has no
// separate update verb, matching the core engine's own put-based upsert
// semantics.
//
// Run: node bench.mjs   (release addon must already be built: npm run build)

import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { Database, ColumnType } = require('./index.js');
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

function tempDir() {
	return mkdtempSync(join(tmpdir(), 'mongreldb-bench-'));
}

function usersSchema() {
	return {
		columns: [
			{ id: 1, name: 'id', ty: ColumnType.Int64, primaryKey: true, nullable: false },
			{ id: 2, name: 'name', ty: ColumnType.Bytes, primaryKey: false, nullable: false },
			{ id: 3, name: 'cost', ty: ColumnType.Float64, primaryKey: false, nullable: false },
		],
		indexes: [],
	};
}

function median(ns) {
	const sorted = [...ns].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
	return sorted[Math.floor(sorted.length / 2)];
}

function human(ns) {
	const n = Number(ns);
	if (n >= 1e9) return `${(n / 1e9).toFixed(2)} s`;
	if (n >= 1e6) return `${(n / 1e6).toFixed(2)} ms`;
	return `${(n / 1e3).toFixed(1)} us`;
}

function timeNs(fn) {
	const start = process.hrtime.bigint();
	fn();
	return process.hrtime.bigint() - start;
}

// ── NAPI addon ───────────────────────────────────────────────────────────

function benchNapi(n) {
	const dir = tempDir();
	const db = Database.withPath(dir);
	db.createTable('users', usersSchema());
	const table = db.getTable('users');

	for (let i = 1; i <= n; i++) {
		table.put([
			{ columnId: 1, int64: BigInt(i) },
			{ columnId: 2, bytes: Buffer.from('City') },
			{ columnId: 3, float64: 199.99 + i },
		]);
	}
	table.commit();

	const insertNs = [];
	for (let i = 0; i < 7; i++) {
		insertNs.push(
			timeNs(() => {
				table.put([
					{ columnId: 1, int64: BigInt(n + 1 + i) },
					{ columnId: 2, bytes: Buffer.from('CityX') },
					{ columnId: 3, float64: 1.0 },
				]);
				table.commit();
			}),
		);
	}

	const updateNs = [];
	for (let i = 0; i < 7; i++) {
		updateNs.push(
			timeNs(() => {
				table.put([
					{ columnId: 1, int64: BigInt(i + 1) },
					{ columnId: 2, bytes: Buffer.from('Updated') },
					{ columnId: 3, float64: 99.0 + i },
				]);
				table.commit();
			}),
		);
	}

	const deleteNs = [];
	for (let i = 0; i < 7; i++) {
		deleteNs.push(
			timeNs(() => {
				table.deleteByPkInt64(BigInt(n - 6 + i));
				table.commit();
			}),
		);
	}

	db.close();
	rmSync(dir, { recursive: true });
	return { insert: median(insertNs), update: median(updateNs), delete: median(deleteNs) };
}

// ── node:sqlite ──────────────────────────────────────────────────────────

function benchSqlite(n) {
	const dir = tempDir();
	const db = new DatabaseSync(join(dir, 's.db'));
	db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, cost REAL)');

	const insertStmt = db.prepare('INSERT INTO users VALUES (?, ?, ?)');
	db.exec('BEGIN');
	for (let i = 1; i <= n; i++) insertStmt.run(i, 'City', 199.99 + i);
	db.exec('COMMIT');

	const insertNs = [];
	for (let i = 0; i < 7; i++) {
		insertNs.push(timeNs(() => insertStmt.run(n + 1 + i, 'CityX', 1.0)));
	}

	const updateStmt = db.prepare('UPDATE users SET cost = ? WHERE id = ?');
	const updateNs = [];
	for (let i = 0; i < 7; i++) {
		updateNs.push(timeNs(() => updateStmt.run(99.0 + i, i + 1)));
	}

	const deleteStmt = db.prepare('DELETE FROM users WHERE id = ?');
	const deleteNs = [];
	for (let i = 0; i < 7; i++) {
		deleteNs.push(timeNs(() => deleteStmt.run(n - 6 + i)));
	}

	db.close();
	rmSync(dir, { recursive: true });
	return { insert: median(insertNs), update: median(updateNs), delete: median(deleteNs) };
}

console.log('NAPI addon vs node:sqlite: single-record write latency\n');
console.log('Notes: both durable (one fsync-backed commit per op). "update" is');
console.log('put() with an existing PK -- the addon has no separate update verb.');
console.log('node:sqlite is DatabaseSync (built into Node 24+), autocommit.\n');

for (const n of [100, 1_000_000]) {
	console.log(`### N = ${n} rows (median of 7)\n`);
	console.log('| engine | single_insert_commit | single_update_commit | delete_one |');
	console.log('|---|---:|---:|---:|');
	const napi = benchNapi(n);
	console.log(`| NAPI addon | ${human(napi.insert)} | ${human(napi.update)} | ${human(napi.delete)} |`);
	const sqlite = benchSqlite(n);
	console.log(`| node:sqlite | ${human(sqlite.insert)} | ${human(sqlite.update)} | ${human(sqlite.delete)} |`);
	console.log();
}
