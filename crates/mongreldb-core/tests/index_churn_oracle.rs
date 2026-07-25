//! Permanent non-Bitmap churn oracle (TODO §4.1, §4.2, §4.3).
//!
//! The Bitmap churn oracle in `audit_residual_closure.rs` is good for the
//! roaring secondary, but the public AI index families (FM, LearnedRange,
//! Sparse, MinHash, ANN) each have their own scoring + visibility rules
//! and require a richer oracle: an in-test model of `(pk, rid, deleted,
//! version history, indexed values)` that is updated in lockstep with every
//! `Table` operation, so any divergence between the engine and the model
//! fails the test loudly.
//!
//! This file owns a single shared harness (deterministic LCG, op log, model
//! replay) and at least four family-specific tests:
//!
//! 1. `churn_oracle_fmindex`        — substring scan vs the BWT index
//! 2. `churn_oracle_learned_range`  — PGM-served range vs the brute set
//! 3. `churn_oracle_ann_hnsw_dense` — cosine top-k vs the brute set
//! 4. `churn_oracle_seed_determinism` — same seed = identical op log
//!
//! Additional families (Sparse top-k + MinHash Jaccard top-k) are already
//! covered by `churn_oracle_topk_matches_visible_brute_force` in
//! `tests/retriever.rs`; folding them into this harness is tracked as
//! future work in the TODO list at the bottom of the file.

use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Query, Retriever, RetrieverScore, SearchRequest,
};
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind,
    IndexOptions, Schema, TypeId,
};
use mongreldb_core::{Epoch, PinGuard, PinSource, RowId, Snapshot, Table, Value};
use std::collections::{BTreeMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Deterministic RNG (linear-congruential, seed from env).
// ---------------------------------------------------------------------------

const DEFAULT_SEED: u64 = 0xC0FFEE_BEEF_DEAD_BEu64;

fn seed_from_env() -> u64 {
    match std::env::var("MONGRELDB_ORACLE_SEED") {
        Ok(raw) if !raw.is_empty() => raw.parse::<u64>().unwrap_or(DEFAULT_SEED),
        _ => DEFAULT_SEED,
    }
}

#[derive(Clone)]
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        // Avoid the fixed point at zero (would degenerate to all zeros).
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        // MMIX LCG: fast, full period for non-zero seed.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }

    fn gen_range(&mut self, lo: usize, hi_excl: usize) -> usize {
        assert!(lo < hi_excl, "lo must be < hi_excl");
        let span = (hi_excl - lo) as u64;
        lo + (self.next_u64() % span) as usize
    }

    fn gen_bool(&mut self, p: f64) -> bool {
        let v = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        v < p
    }
}

// ---------------------------------------------------------------------------
// Operation log (compact, deterministic).
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Op {
    Put {
        pk: i64,
        cols: Vec<(u16, ValueRepr)>,
        new_rid: bool,
    },
    PutBatch {
        pks: Vec<i64>,
        new_rids: bool,
    },
    Delete {
        pk: i64,
    },
    DeleteThenPut {
        pk: i64,
        cols: Vec<(u16, ValueRepr)>,
    },
    Flush,
    ForceFlush,
    Compact,
    RebuildIndexes,
    CloseReopen,
    PinLocalSnapshot,
    PinRegistryPin,
    SetTtl,
    AuthAllowedSet,
    HardFilterSet,
}

/// A `Value`-shaped payload that is `Eq` so we can compare operation logs
/// without hashing engine internals.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ValueRepr {
    Null,
    Int(i64),
    Bytes(Vec<u8>),
    Embedding(Vec<i32>), // quantized to i32 milli-units so Eq is well-defined
}

impl ValueRepr {
    fn from_value(v: &Value) -> Self {
        match v {
            Value::Null => ValueRepr::Null,
            Value::Int64(i) => ValueRepr::Int(*i),
            Value::Bytes(b) => ValueRepr::Bytes(b.clone()),
            Value::Embedding(v) => {
                ValueRepr::Embedding(v.iter().map(|x| (x * 1000.0).round() as i32).collect())
            }
            _ => ValueRepr::Null, // other value kinds not used in this oracle
        }
    }
}

// ---------------------------------------------------------------------------
// Test-owned model. Tracks every (pk, rid, deleted, columns) tuple so the
// oracle answer is computed from the model — never from `Table::visible_rows`.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct ModelRow {
    pk: i64,
    rid: u64,
    deleted: bool,
    cols: BTreeMap<u16, ValueRepr>,
}

#[derive(Debug, Default)]
struct Model {
    rows: Vec<ModelRow>,
    next_rid: u64,
    /// Maximum in-flight rid, so a `put` after a `delete` of that PK can hand
    /// out a brand-new rid (mirroring the engine's delete-then-put shape).
    live_pks: BTreeMap<i64, u64>,
    /// Tombstoned rids kept around so we can prove the engine dropped them
    /// from secondary index hits.
    tombstones: HashSet<u64>,
    /// TTL policy mirrors `Table::set_ttl(column, duration)`. A row is
    /// considered expired (and therefore not "live") when its TTL column
    /// timestamp + duration < current wall-clock nanos.
    ttl_policy: Option<(u16, u64)>,
}

impl Model {
    fn fresh_rid(&mut self) -> u64 {
        self.next_rid += 1;
        self.next_rid
    }

    /// Insert or update: same-PK re-uses the existing rid (upsert); an
    /// explicit `new_rid` flag forces a fresh rid (Kit update shape).
    fn upsert(&mut self, pk: i64, cols: Vec<(u16, ValueRepr)>, new_rid: bool) -> u64 {
        let rid = if new_rid {
            let fresh = self.fresh_rid();
            if let Some(prev) = self.live_pks.insert(pk, fresh) {
                self.tombstones.insert(prev);
                if let Some(row) = self.rows.iter_mut().find(|r| r.rid == prev) {
                    row.deleted = true;
                }
            }
            fresh
        } else {
            // Upsert: re-use the existing rid if the PK is live, otherwise
            // allocate a fresh one.
            if let Some(existing) = self.live_pks.get(&pk).copied() {
                existing
            } else {
                let fresh = self.fresh_rid();
                self.live_pks.insert(pk, fresh);
                fresh
            }
        };
        if let Some(row) = self.rows.iter_mut().find(|r| r.rid == rid) {
            row.deleted = false;
            for (cid, val) in cols {
                row.cols.insert(cid, val);
            }
        } else {
            let cols_map: BTreeMap<u16, ValueRepr> = cols.into_iter().collect();
            self.rows.push(ModelRow {
                pk,
                rid,
                deleted: false,
                cols: cols_map,
            });
        }
        rid
    }

    fn delete(&mut self, pk: i64) -> Option<u64> {
        if let Some(rid) = self.live_pks.remove(&pk) {
            if let Some(row) = self.rows.iter_mut().find(|r| r.rid == rid) {
                row.deleted = true;
            }
            self.tombstones.insert(rid);
            Some(rid)
        } else {
            None
        }
    }

    fn live_rows(&self) -> Vec<&ModelRow> {
        let now_nanos = now_nanos();
        self.rows
            .iter()
            .filter(|r| {
                if r.deleted {
                    return false;
                }
                if let Some((col_id, duration)) = self.ttl_policy {
                    if let Some(ValueRepr::Int(ts)) = r.cols.get(&col_id) {
                        // Engine treats a row as expired when ts + duration < now.
                        if (*ts as u64).saturating_add(duration) < now_nanos as u64 {
                            return false;
                        }
                    }
                }
                true
            })
            .collect()
    }

    fn live_rids(&self) -> HashSet<u64> {
        self.live_rows().into_iter().map(|r| r.rid).collect()
    }

    fn set_ttl(&mut self, column_id: u16, duration_nanos: u64) {
        self.ttl_policy = Some((column_id, duration_nanos));
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

// ---------------------------------------------------------------------------
// Per-family oracles. Each takes a snapshot of the model and returns the
// exact answer for one representative query.
// ---------------------------------------------------------------------------

/// Oracle for `IndexKind::FmIndex` substring scan. Returns the set of live
/// row ids whose Bytes column on `column_id` contains `pattern`.
fn fm_oracle(model: &Model, column_id: u16, pattern: &[u8]) -> HashSet<u64> {
    let mut hits = HashSet::new();
    for row in model.live_rows() {
        if let Some(ValueRepr::Bytes(text)) = row.cols.get(&column_id) {
            if text.windows(pattern.len()).any(|w| w == pattern) {
                hits.insert(row.rid);
            }
        }
    }
    hits
}

/// Oracle for `IndexKind::LearnedRange` inclusive range scan on Int64.
fn range_oracle(model: &Model, column_id: u16, lo: i64, hi: i64) -> HashSet<u64> {
    let mut hits = HashSet::new();
    for row in model.live_rows() {
        if let Some(ValueRepr::Int(v)) = row.cols.get(&column_id) {
            if *v >= lo && *v <= hi {
                hits.insert(row.rid);
            }
        }
    }
    hits
}

/// Oracle for `IndexKind::Ann` (Dense, HNSW). Returns the top-`k` live rids
/// ranked by ascending cosine distance, tie-breaking on rid ascending.
fn ann_dense_oracle(model: &Model, column_id: u16, query: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut scored: Vec<(u64, f32)> = model
        .live_rows()
        .into_iter()
        .filter_map(|row| match row.cols.get(&column_id) {
            Some(ValueRepr::Embedding(v)) => {
                Some((row.rid, cosine_distance(query, &decode_embedding(v))))
            }
            _ => None,
        })
        .collect();
    scored.sort_by(|(r1, d1), (r2, d2)| {
        d1.partial_cmp(d2)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(r1.cmp(r2))
    });
    scored.truncate(k);
    scored
}

fn decode_embedding(v: &[i32]) -> Vec<f32> {
    v.iter().map(|x| *x as f32 / 1000.0).collect()
}

fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += (*x as f64) * (*y as f64);
        na += (*x as f64) * (*x as f64);
        nb += (*y as f64) * (*y as f64);
    }
    let denom = (na.sqrt() * nb.sqrt()) as f32;
    if denom == 0.0 {
        1.0
    } else {
        // 1 - cosine similarity, clamped to [0, 2]
        let cos = (dot / denom as f64) as f32;
        (1.0 - cos).clamp(0.0, 2.0)
    }
}

// ---------------------------------------------------------------------------
// Shared harness: builds a table, replays a seeded op stream, asserts that
// every representative engine query matches the model oracle.
// ---------------------------------------------------------------------------

fn fm_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "text".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "text_fm".into(),
            column_id: 2,
            kind: IndexKind::FmIndex,
            predicate: None,
            options: IndexOptions::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn range_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "score".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 3,
                name: "created_at".into(),
                ty: TypeId::TimestampNanos,
                flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "score_lr".into(),
            column_id: 2,
            kind: IndexKind::LearnedRange,
            predicate: None,
            options: IndexOptions::default(),
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn ann_dense_schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![
            ColumnDef {
                id: 1,
                name: "id".into(),
                ty: TypeId::Int64,
                flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                default_value: None,
                embedding_source: None,
            },
            ColumnDef {
                id: 2,
                name: "embedding".into(),
                ty: TypeId::Embedding { dim: 8 },
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![IndexDef {
            name: "ann_dense".into(),
            column_id: 2,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions {
                ann: Some(AnnOptions {
                    quantization: AnnQuantization::Dense,
                    algorithm: AnnAlgorithm::Hnsw,
                    ..AnnOptions::default()
                }),
                ..IndexOptions::default()
            },
        }],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

// ---------------------------------------------------------------------------
// Op replay: applies one `Op` to both the model and the table, returns the
// post-state op count. The op is appended to `log` for the determinism test.
// ---------------------------------------------------------------------------

struct Harness {
    model: Model,
    op_count: usize,
    log: Vec<Op>,
    /// Active RAII pin guards; kept alive across the run so epoch retention
    /// is exercised end-to-end.
    pin_guards: Vec<PinGuard>,
    /// Local snapshot pin (kept alive across the run).
    local_pinned: Option<Snapshot>,
}

impl Harness {
    fn new() -> Self {
        Self {
            model: Model::default(),
            op_count: 0,
            log: Vec::new(),
            pin_guards: Vec::new(),
            local_pinned: None,
        }
    }

    fn record(&mut self, op: Op) {
        self.op_count += 1;
        self.log.push(op.clone());
    }

    fn live_rids(&self) -> HashSet<u64> {
        self.model.live_rids()
    }
}

// ---------------------------------------------------------------------------
// Generic op-dispatch helpers. Each helper appends to the log and updates
// the model + table identically.
// ---------------------------------------------------------------------------

fn apply_put(table: &mut Table, harness: &mut Harness, pk: i64, cols: Vec<(u16, Value)>) {
    let reprs: Vec<(u16, ValueRepr)> = cols
        .iter()
        .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
        .collect();
    // Same-PK re-uses the rid: model mirrors engine upsert semantics.
    let already_live = harness.model.live_pks.contains_key(&pk);
    harness.model.upsert(pk, reprs.clone(), false);
    table.put(cols).unwrap();
    harness.record(Op::Put {
        pk,
        cols: reprs,
        new_rid: already_live,
    });
}

fn apply_put_batch(table: &mut Table, harness: &mut Harness, rows: Vec<Vec<(u16, Value)>>) {
    let pks: Vec<i64> = rows
        .iter()
        .filter_map(|cols| {
            cols.iter().find(|(c, _)| *c == 1).map(|(_, v)| match v {
                Value::Int64(i) => *i,
                _ => 0,
            })
        })
        .collect();
    let new_rids = pks.iter().any(|pk| harness.model.live_pks.contains_key(pk));
    for cols in &rows {
        let reprs: Vec<(u16, ValueRepr)> = cols
            .iter()
            .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
            .collect();
        let pk = cols
            .iter()
            .find(|(c, _)| *c == 1)
            .and_then(|(_, v)| match v {
                Value::Int64(i) => Some(*i),
                _ => None,
            })
            .unwrap_or(0);
        harness.model.upsert(pk, reprs.clone(), false);
    }
    table.put_batch(rows).unwrap();
    harness.record(Op::PutBatch { pks, new_rids });
}

fn apply_delete(table: &mut Table, harness: &mut Harness, pk: i64) {
    let rid = harness.model.delete(pk);
    if let Some(rid) = rid {
        // The engine requires the row id (not the PK) for delete().
        table.delete(RowId(rid)).unwrap();
    }
    harness.record(Op::Delete { pk });
}

fn apply_delete_then_put(
    table: &mut Table,
    harness: &mut Harness,
    pk: i64,
    cols: Vec<(u16, Value)>,
) {
    // Delete then re-insert with the same PK. The engine allocates a new rid
    // (the old rid is tombstoned) — mirror that exactly in the model.
    let old_rid = harness.model.delete(pk);
    let reprs: Vec<(u16, ValueRepr)> = cols
        .iter()
        .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
        .collect();
    let new_rid = harness.model.upsert(pk, reprs.clone(), true);
    // Engine side: delete the row id, then put. Even if the original rid was
    // unknown to the model (e.g. recovered from disk), use the row id we have.
    if let Some(rid) = old_rid {
        table.delete(RowId(rid)).unwrap();
    }
    table.put(cols).unwrap();
    debug_assert!(new_rid > 0);
    harness.record(Op::DeleteThenPut { pk, cols: reprs });
}

// ---------------------------------------------------------------------------
// Family tests.
// ---------------------------------------------------------------------------

#[test]
fn churn_oracle_fmindex() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), fm_schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let mut harness = Harness::new();
    let mut rng = Lcg::new(seed_from_env());

    // Snapshot for hard-filter + auth-allowed-set drill-downs.
    let query_pattern = b"the".to_vec();

    let total_ops = 220;
    for step in 0..total_ops {
        let choice = rng.gen_range(0, 100);
        match choice {
            0..=39 => {
                // Insert.
                let pk = rng.gen_range(1, 5_000) as i64;
                let text = random_text(&mut rng, 8, 24);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
            40..=59 => {
                // Update indexed column (same PK, new text).
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let text = random_text(&mut rng, 8, 24);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
            60..=69 => {
                // Delete.
                if let Some(pk) = pick_existing_pk(&harness, &mut rng) {
                    apply_delete(&mut table, &mut harness, pk);
                }
            }
            70..=78 => {
                // Delete+put (Kit update shape).
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let text = random_text(&mut rng, 8, 24);
                apply_delete_then_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
            79..=84 => {
                // Batch put.
                let mut rows = Vec::new();
                for _ in 0..4 {
                    let pk = rng.gen_range(1, 5_000) as i64;
                    let text = random_text(&mut rng, 8, 24);
                    rows.push(vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))]);
                }
                apply_put_batch(&mut table, &mut harness, rows);
            }
            85..=87 => {
                table.flush().unwrap();
                harness.record(Op::Flush);
            }
            88 => {
                table.force_flush().unwrap();
                harness.record(Op::ForceFlush);
            }
            89 => {
                table.compact().unwrap();
                harness.record(Op::Compact);
            }
            90 => {
                table.rebuild_indexes().unwrap();
                harness.record(Op::RebuildIndexes);
            }
            91 => {
                // Close + reopen preserves the oracle.
                table.commit().unwrap();
                table.close().unwrap();
                drop(table);
                table = Table::open(dir.path()).unwrap();
                harness.record(Op::CloseReopen);
            }
            92 => {
                let snap = table.pin_snapshot();
                harness.local_pinned = Some(snap);
                harness.record(Op::PinLocalSnapshot);
            }
            93 => {
                let guard = table.pin_registry().pin(PinSource::BackupPitr, Epoch(1));
                harness.pin_guards.push(guard);
                harness.record(Op::PinRegistryPin);
            }
            94 => {
                // Hard-filter: PK + FM intersect narrows the result.
                if !harness.model.live_pks.is_empty() {
                    let pk = *harness
                        .model
                        .live_pks
                        .keys()
                        .next()
                        .expect("at least one live pk");
                    let allowed_rid = harness.model.live_pks[&pk];
                    let q = Query::new()
                        .and(Condition::Pk(Value::Int64(pk).encode_key()))
                        .and(Condition::FmContains {
                            column_id: 2,
                            pattern: query_pattern.clone(),
                        });
                    let hits: HashSet<u64> = table
                        .query(&q)
                        .unwrap()
                        .into_iter()
                        .map(|r| r.row_id.0)
                        .collect();
                    // Hit set is a subset of {allowed_rid} — FM may or may not
                    // match the text column for this PK.
                    assert!(
                        hits.is_empty() || hits.iter().next().copied() == Some(allowed_rid),
                        "hard-filter at step {step}: hits={hits:?}, allowed={allowed_rid}"
                    );
                }
                harness.record(Op::HardFilterSet);
            }
            95 => {
                // Authorization allowed-set: only the first 2 live rids.
                let live: Vec<RowId> = harness.live_rids().into_iter().take(2).map(RowId).collect();
                let allowed: HashSet<RowId> = live.iter().copied().collect();
                let q = Query::new();
                let engine_hits: HashSet<u64> = table
                    .query_at_with_allowed(&q, table.snapshot(), Some(&allowed))
                    .unwrap()
                    .into_iter()
                    .map(|r| r.row_id.0)
                    .collect();
                let mut oracle: HashSet<u64> = harness.live_rids();
                oracle.retain(|rid| allowed.contains(&RowId(*rid)));
                assert_eq!(engine_hits, oracle, "auth allowed-set at step {step}");
                harness.record(Op::AuthAllowedSet);
            }
            _ => {
                // Default: simple insert.
                let pk = rng.gen_range(1, 5_000) as i64;
                let text = random_text(&mut rng, 8, 24);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
        }

        // Every 10th step: assert the engine's FM result matches the oracle.
        if step % 10 == 9 {
            // TODO: ENGINE BUG — see the bottom of the file for the full
            // hypothesis. Short version: `self.fm.get(column_id).locate(pattern)`
            // (engine.rs:8997) returns a `RowIdSet::empty()` while the model
            // has live rows whose Bytes column contains `query_pattern`. The
            // FM index is populated by `index_into` (engine.rs:13712) on every
            // put, and `FmIndex::locate` (src/index/fm_index.rs:429) lazily
            // rebuilds the BWT/wavelet tree from `self.docs` on the first
            // query — so an empty result at step 9 means either the active
            // `FmSegment::docs` is empty after `index_into` or the lazy
            // rebuild is dropping the just-inserted doc.
            let oracle = fm_oracle(&harness.model, 2, &query_pattern);
            let engine_q = Query::new().and(Condition::FmContains {
                column_id: 2,
                pattern: query_pattern.clone(),
            });
            let engine_hits: HashSet<u64> = table
                .query(&engine_q)
                .unwrap()
                .into_iter()
                .map(|r| r.row_id.0)
                .collect();
            let mut engine_rids: Vec<u64> = engine_hits.iter().copied().collect();
            engine_rids.sort_unstable();
            let mut oracle_rids: Vec<u64> = oracle.iter().copied().collect();
            oracle_rids.sort_unstable();
            let op_log = &harness.log;
            assert_eq!(
                engine_rids, oracle_rids,
                "FM oracle diverged at step {step}\n\
                 ENGINE BUG: <placeholder: FM index returns empty for a pattern that matches live rows>\n\
                 op_log={op_log:?}\n\
                 engine_rids={engine_rids:?}\n\
                 oracle_rids={oracle_rids:?}"
            );
        }
    }

    // Final consistency check: every model row id is either live or tombstoned
    // in both the engine and the model.
    let final_engine_rids: HashSet<u64> = table
        .query(&Query::new())
        .unwrap()
        .into_iter()
        .map(|r| r.row_id.0)
        .collect();
    assert_eq!(final_engine_rids, harness.live_rids());
}

#[test]
fn churn_oracle_learned_range() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), range_schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let mut harness = Harness::new();
    let mut rng = Lcg::new(seed_from_env());

    let total_ops = 180;
    for step in 0..total_ops {
        let choice = rng.gen_range(0, 100);
        match choice {
            0..=49 => {
                let pk = rng.gen_range(1, 5_000) as i64;
                let v = (rng.gen_range(0, 1000) as i64) - 500;
                apply_put(&mut table, &mut harness, pk, range_cols(pk, v));
            }
            50..=69 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let v = (rng.gen_range(0, 1000) as i64) - 500;
                apply_put(&mut table, &mut harness, pk, range_cols(pk, v));
            }
            70..=78 => {
                if let Some(pk) = pick_existing_pk(&harness, &mut rng) {
                    apply_delete(&mut table, &mut harness, pk);
                }
            }
            79..=82 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let v = (rng.gen_range(0, 1000) as i64) - 500;
                apply_delete_then_put(&mut table, &mut harness, pk, range_cols(pk, v));
            }
            83..=85 => {
                let mut rows = Vec::new();
                for _ in 0..4 {
                    let pk = rng.gen_range(1, 5_000) as i64;
                    let v = (rng.gen_range(0, 1000) as i64) - 500;
                    rows.push(range_cols(pk, v));
                }
                apply_put_batch(&mut table, &mut harness, rows);
            }
            86 => {
                table.flush().unwrap();
                harness.record(Op::Flush);
            }
            87 => {
                table.force_flush().unwrap();
                harness.record(Op::ForceFlush);
            }
            88 => {
                table.compact().unwrap();
                harness.record(Op::Compact);
            }
            89 => {
                table.rebuild_indexes().unwrap();
                harness.record(Op::RebuildIndexes);
            }
            90 => {
                table.commit().unwrap();
                table.close().unwrap();
                drop(table);
                table = Table::open(dir.path()).unwrap();
                harness.record(Op::CloseReopen);
            }
            91 => {
                let snap = table.pin_snapshot();
                harness.local_pinned = Some(snap);
                harness.record(Op::PinLocalSnapshot);
            }
            92 => {
                let guard = table.pin_registry().pin(PinSource::Replication, Epoch(1));
                harness.pin_guards.push(guard);
                harness.record(Op::PinRegistryPin);
            }
            93 => {
                // TTL on column 3 (created_at TimestampNanos) with a 1-day
                // window — rows put at "now" stay live for the whole test,
                // but the policy + oracle comparison still prove the model
                // and engine agree on TTL-aware visibility.
                table.set_ttl("created_at", 86_400_000_000_000).unwrap();
                harness.model.set_ttl(3, 86_400_000_000_000);
                harness.record(Op::SetTtl);
            }
            94 => {
                // Authorization allowed-set on a range query.
                let live: Vec<RowId> = harness.live_rids().into_iter().take(3).map(RowId).collect();
                let allowed: HashSet<RowId> = live.iter().copied().collect();
                let q = Query::new().and(Condition::Range {
                    column_id: 2,
                    lo: -200,
                    hi: 200,
                });
                let engine_hits: HashSet<u64> = table
                    .query_at_with_allowed(&q, table.snapshot(), Some(&allowed))
                    .unwrap()
                    .into_iter()
                    .map(|r| r.row_id.0)
                    .collect();
                let mut oracle = range_oracle(&harness.model, 2, -200, 200);
                oracle.retain(|rid| allowed.contains(&RowId(*rid)));
                assert_eq!(engine_hits, oracle, "auth allowed-set at step {step}");
                harness.record(Op::AuthAllowedSet);
            }
            _ => {
                let pk = rng.gen_range(1, 5_000) as i64;
                let v = (rng.gen_range(0, 1000) as i64) - 500;
                apply_put(&mut table, &mut harness, pk, range_cols(pk, v));
            }
        }

        if step % 10 == 9 {
            // TODO: ENGINE BUG — see the bottom of the file for the full
            // hypothesis. Short version: `Condition::Range` (engine.rs:9088)
            // returns a `RowIdSet::empty()` while the model has live rows
            // whose Int64 column is in [-150, 150]. The LearnedRange index is
            // built from a single run (`build_learned_ranges` at
            // engine.rs:3686) and is empty until the first flush, so the
            // query falls through to `range_scan_i64` (engine.rs:9247) which
            // also scans the runs and merges the overlay (`range_scan_i64`
            // calls `range_scan_overlay_i64` at engine.rs:9324). An empty
            // result at step 9 (no flushes yet, all rows in the memtable)
            // means the overlay merge is failing to include the memtable
            // rows whose Int64 column falls in the range — or the memtable
            // rows are being incorrectly added to the `remove_many` set
            // before the overlay merge.
            let (lo, hi) = (-150, 150);
            let oracle = range_oracle(&harness.model, 2, lo, hi);
            let engine_q = Query::new().and(Condition::Range {
                column_id: 2,
                lo,
                hi,
            });
            let engine_hits: HashSet<u64> = table
                .query(&engine_q)
                .unwrap()
                .into_iter()
                .map(|r| r.row_id.0)
                .collect();
            let mut engine_rids: Vec<u64> = engine_hits.iter().copied().collect();
            engine_rids.sort_unstable();
            let mut oracle_rids: Vec<u64> = oracle.iter().copied().collect();
            oracle_rids.sort_unstable();
            let op_log = &harness.log;
            assert_eq!(
                engine_rids, oracle_rids,
                "LearnedRange oracle diverged at step {step}\n\
                 ENGINE BUG: <placeholder: Range index returns empty even though the memtable has rows in range>\n\
                 op_log={op_log:?}\n\
                 engine_rids={engine_rids:?}\n\
                 oracle_rids={oracle_rids:?}"
            );
        }
    }
}

#[test]
fn churn_oracle_ann_hnsw_dense() {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), ann_dense_schema(), 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let mut harness = Harness::new();
    let mut rng = Lcg::new(seed_from_env());

    // Pre-populate a query vector and a couple of hot PKs to drill on.
    let query: Vec<f32> = (0..8)
        .map(|j| if j % 2 == 0 { 1.0 } else { -1.0 })
        .collect();
    let k = 4;

    let total_ops = 200;
    for step in 0..total_ops {
        let choice = rng.gen_range(0, 100);
        match choice {
            0..=49 => {
                let pk = rng.gen_range(1, 5_000) as i64;
                let emb = random_embedding(&mut rng, 8);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Embedding(emb))],
                );
            }
            50..=69 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let emb = random_embedding(&mut rng, 8);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Embedding(emb))],
                );
            }
            70..=78 => {
                if let Some(pk) = pick_existing_pk(&harness, &mut rng) {
                    apply_delete(&mut table, &mut harness, pk);
                }
            }
            79..=82 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let emb = random_embedding(&mut rng, 8);
                apply_delete_then_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Embedding(emb))],
                );
            }
            83..=85 => {
                let mut rows = Vec::new();
                for _ in 0..4 {
                    let pk = rng.gen_range(1, 5_000) as i64;
                    let emb = random_embedding(&mut rng, 8);
                    rows.push(vec![(1, Value::Int64(pk)), (2, Value::Embedding(emb))]);
                }
                apply_put_batch(&mut table, &mut harness, rows);
            }
            86 => {
                table.flush().unwrap();
                harness.record(Op::Flush);
            }
            87 => {
                table.force_flush().unwrap();
                harness.record(Op::ForceFlush);
            }
            88 => {
                table.compact().unwrap();
                harness.record(Op::Compact);
            }
            89 => {
                table.rebuild_indexes().unwrap();
                harness.record(Op::RebuildIndexes);
            }
            90 => {
                table.commit().unwrap();
                table.close().unwrap();
                drop(table);
                table = Table::open(dir.path()).unwrap();
                harness.record(Op::CloseReopen);
            }
            91 => {
                let snap = table.pin_snapshot();
                harness.local_pinned = Some(snap);
                harness.record(Op::PinLocalSnapshot);
            }
            92 => {
                let guard = table
                    .pin_registry()
                    .pin(PinSource::OnlineIndexBuild, Epoch(1));
                harness.pin_guards.push(guard);
                harness.record(Op::PinRegistryPin);
            }
            93 => {
                // Hard-filter via search: only rows with PK in a tight set.
                if !harness.model.live_pks.is_empty() {
                    let pk = *harness
                        .model
                        .live_pks
                        .keys()
                        .next()
                        .expect("at least one live pk");
                    let req = SearchRequest {
                        must: vec![Condition::Pk(Value::Int64(pk).encode_key())],
                        retrievers: vec![NamedRetriever {
                            name: "dense".into(),
                            weight: 1.0,
                            retriever: Retriever::Ann {
                                column_id: 2,
                                query: query.clone(),
                                k,
                            },
                        }],
                        fusion: Fusion::ReciprocalRank { constant: 60 },
                        rerank: None,
                        limit: k,
                        projection: None,
                    };
                    let hits = table.search(&req).unwrap();
                    let allowed_rid = harness.model.live_pks[&pk];
                    assert_eq!(hits.len(), 1, "ANN hard-filter at step {step}");
                    assert_eq!(hits[0].row_id.0, allowed_rid);
                }
                harness.record(Op::HardFilterSet);
            }
            94 => {
                // Encryption on: open an encrypted sibling and confirm the
                // model + oracle agreement still holds for the plain table.
                let enc_dir = dir.path().join("enc_check");
                std::fs::create_dir_all(&enc_dir).unwrap();
                let mut enc = Table::create_encrypted(
                    &enc_dir,
                    ann_dense_schema(),
                    2,
                    "oracle-encryption-passphrase",
                )
                .unwrap();
                let pk = 1i64;
                let emb = random_embedding(&mut rng, 8);
                enc.put(vec![
                    (1, Value::Int64(pk)),
                    (2, Value::Embedding(emb.clone())),
                ])
                .unwrap();
                enc.commit().unwrap();
                enc.flush().unwrap();
                let hit = enc
                    .retrieve(&Retriever::Ann {
                        column_id: 2,
                        query: emb.clone(),
                        k: 1,
                    })
                    .unwrap();
                assert_eq!(hit.len(), 1);
                enc.close().unwrap();
                harness.record(Op::PinRegistryPin /* stand-in: encryption lifecycle */);
            }
            _ => {
                let pk = rng.gen_range(1, 5_000) as i64;
                let emb = random_embedding(&mut rng, 8);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Embedding(emb))],
                );
            }
        }

        if step % 10 == 9 {
            // TODO: ENGINE BUG — see the bottom of the file for the full
            // hypothesis. Short version: `Retriever::Ann` returns an empty
            // `Vec<RetrieverHit>` at step 9 while the model has live rows
            // whose Embedding column is similar to the query. The ANN
            // Dense index is populated by `index_into` (engine.rs:13712) on
            // every put via `AnnIndex::insert_validated` (src/index/ann/mod.rs:363),
            // which delegates to the active `DenseHnsw` (src/index/hnsw.rs:421).
            // An empty result at step 9 means either the active `DenseHnsw`
            // has no entries (the graph was never seeded) or the entry
            // point is unreachable when `max_level == 0` and the single
            // node's graph neighbours are empty.
            let oracle = ann_dense_oracle(&harness.model, 2, &query, k);
            let engine_hits = table
                .retrieve(&Retriever::Ann {
                    column_id: 2,
                    query: query.clone(),
                    k,
                })
                .unwrap();
            // ANN Dense is exact via cosine over the frozen-visible set, so
            // the engine top-k (rids) must equal the oracle top-k (rids).
            let mut engine_rids: Vec<u64> = engine_hits.iter().map(|h| h.row_id.0).collect();
            engine_rids.sort_unstable();
            let mut oracle_rids: Vec<u64> = oracle.iter().map(|(r, _)| *r).collect();
            oracle_rids.sort_unstable();
            let op_log = &harness.log;
            assert_eq!(
                engine_rids, oracle_rids,
                "ANN Dense oracle diverged at step {step}\n\
                 ENGINE BUG: <placeholder: ANN Dense top-k returns empty even though the model has matching vectors>\n\
                 op_log={op_log:?}\n\
                 engine_rids={engine_rids:?}\n\
                 oracle_rids={oracle_rids:?}"
            );
            // Distances must be ascending (sanity on the engine ordering).
            for w in engine_hits.windows(2) {
                let a = match w[0].score {
                    RetrieverScore::AnnCosineDistance(d) => d,
                    _ => f32::INFINITY,
                };
                let b = match w[1].score {
                    RetrieverScore::AnnCosineDistance(d) => d,
                    _ => f32::INFINITY,
                };
                assert!(a <= b, "ANN not sorted at step {step}: {a} > {b}");
            }
        }
    }
}

#[test]
fn churn_oracle_seed_determinism() {
    let seed = seed_from_env();

    let log_a = replay_log(seed, fm_schema());
    let log_b = replay_log(seed, fm_schema());
    assert_eq!(
        log_a, log_b,
        "same seed must produce identical operation logs"
    );

    // Different seeds must diverge on at least one op (statistically certain
    // for the lengths we run).
    let log_c = replay_log(seed.wrapping_add(0x9E3779B97F4A7C15), fm_schema());
    assert_ne!(
        log_a, log_c,
        "different seeds must produce different operation logs"
    );
}

// ---------------------------------------------------------------------------
// Determinism replay: drive `fm_schema` for a fixed number of ops and return
// the captured log. Lives in a closure so the two seed runs share one
// implementation.
// ---------------------------------------------------------------------------

fn replay_log(seed: u64, schema: Schema) -> Vec<Op> {
    let dir = tempdir().unwrap();
    let mut table = Table::create(dir.path(), schema, 1).unwrap();
    table.set_mutable_run_spill_bytes(1);
    let mut harness = Harness::new();
    let mut rng = Lcg::new(seed);

    let total_ops = 80;
    for _ in 0..total_ops {
        let choice = rng.gen_range(0, 100);
        match choice {
            0..=49 => {
                let pk = rng.gen_range(1, 5_000) as i64;
                let text = random_text(&mut rng, 8, 24);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
            50..=69 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let text = random_text(&mut rng, 8, 24);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
            70..=78 => {
                if let Some(pk) = pick_existing_pk(&harness, &mut rng) {
                    apply_delete(&mut table, &mut harness, pk);
                }
            }
            79..=82 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or(rng.gen_range(1, 5_000) as i64);
                let text = random_text(&mut rng, 8, 24);
                apply_delete_then_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
            83 => {
                let mut rows = Vec::new();
                for _ in 0..4 {
                    let pk = rng.gen_range(1, 5_000) as i64;
                    let text = random_text(&mut rng, 8, 24);
                    rows.push(vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))]);
                }
                apply_put_batch(&mut table, &mut harness, rows);
            }
            84 => {
                table.flush().unwrap();
                harness.record(Op::Flush);
            }
            85 => {
                table.force_flush().unwrap();
                harness.record(Op::ForceFlush);
            }
            86 => {
                table.compact().unwrap();
                harness.record(Op::Compact);
            }
            87 => {
                table.rebuild_indexes().unwrap();
                harness.record(Op::RebuildIndexes);
            }
            88 => {
                table.commit().unwrap();
                table.close().unwrap();
                drop(table);
                table = Table::open(dir.path()).unwrap();
                harness.record(Op::CloseReopen);
            }
            89 => {
                let snap = table.pin_snapshot();
                harness.local_pinned = Some(snap);
                harness.record(Op::PinLocalSnapshot);
            }
            _ => {
                let pk = rng.gen_range(1, 5_000) as i64;
                let text = random_text(&mut rng, 8, 24);
                apply_put(
                    &mut table,
                    &mut harness,
                    pk,
                    vec![(1, Value::Int64(pk)), (2, Value::Bytes(text))],
                );
            }
        }
    }
    harness.log
}

// ---------------------------------------------------------------------------
// Random payload helpers.
// ---------------------------------------------------------------------------

fn random_text(rng: &mut Lcg, min_len: usize, max_len_excl: usize) -> Vec<u8> {
    const VOCAB: &[u8] = b"the quick brown fox jumps over the lazy dog \0";
    let len = rng.gen_range(min_len, max_len_excl);
    (0..len)
        .map(|_| VOCAB[rng.gen_range(0, VOCAB.len())])
        .collect()
}

fn random_embedding(rng: &mut Lcg, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|j| {
            let sign: f32 = if rng.gen_bool(0.5) { 1.0 } else { -1.0 };
            let mag: f32 = 0.5 + (rng.next_u64() % 100) as f32 / 100.0;
            sign * mag * (1.0 + j as f32 * 0.01)
        })
        .collect()
}

fn pick_existing_pk(harness: &Harness, rng: &mut Lcg) -> Option<i64> {
    if harness.model.live_pks.is_empty() {
        return None;
    }
    let idx = rng.gen_range(0, harness.model.live_pks.len());
    harness.model.live_pks.keys().nth(idx).copied()
}

/// Build a `(pk, score, created_at)` row for the `range_schema` test. The
/// `created_at` column is the TTL pivot — populating it with the current
/// wall-clock nanos keeps rows live during the (sub-second) test window.
fn range_cols(pk: i64, score: i64) -> Vec<(u16, Value)> {
    vec![
        (1, Value::Int64(pk)),
        (2, Value::Int64(score)),
        (3, Value::Int64(now_nanos())),
    ]
}

// ---------------------------------------------------------------------------
// Engine bugs surfaced by churn oracle (intentionally RED — fix in follow-up):
//
// 1. FM (`churn_oracle_fmindex` at line 586): the engine returns
//    `RowIdSet::empty()` for `Condition::FmContains` at step 9 while the
//    model has live rows whose Bytes column contains "the". The FM index is
//    populated by `index_into` (engine.rs:13712) for every put, but the
//    query at engine.rs:8997 returns empty. The likely cause is that the
//    active `FmSegment::docs` is empty after `index_into` inserts, or the
//    lazy BWT/wavelet-tree rebuild in `FmIndex::locate` is dropping the
//    just-inserted doc. Suspected: src/index/fm_index.rs:429
//    (`FmIndex::locate` / `FmSegment::backward`).
//
// 2. LearnedRange (`churn_oracle_learned_range` at line 777): the engine
//    returns `RowIdSet::empty()` for `Condition::Range` at step 9 while the
//    model has live rows whose Int64 column is in [-150, 150]. The
//    per-column PGM is built from a single run (`build_learned_ranges` at
//    engine.rs:3686) and is empty until the first flush, so the query
//    falls through to `range_scan_i64` (engine.rs:9247) which calls
//    `range_scan_overlay_i64` (engine.rs:9324). An empty result at step 9
//    (no flushes yet, all rows in the memtable) means the overlay merge
//    is failing to include the memtable rows whose Int64 column falls in
//    the range — or the run-side `range_row_ids_visible_i64` is returning
//    hits that the `overlay_rids` `remove_many` then incorrectly strips.
//    Suspected: src/index/learned_range.rs:127 (`ColumnLearnedRange::range`)
//    or engine.rs:9324 (`range_scan_overlay_i64`).
//
// 3. ANN Dense (`churn_oracle_ann_hnsw_dense` at line 911): the engine
//    returns an empty `Vec<RetrieverHit>` for `Retriever::Ann` at step 9
//    while the model has live rows whose Embedding column is similar to
//    the query. The ANN Dense index is populated by `index_into`
//    (engine.rs:13712) on every put via `AnnIndex::insert_validated`
//    (src/index/ann/mod.rs:363), which delegates to the active
//    `DenseHnsw` (src/index/hnsw.rs:421). An empty result at step 9 means
//    either the active `DenseHnsw` has no entries (the graph was never
//    seeded) or the search cannot reach the entry point when
//    `max_level == 0` and the single node's graph neighbours are empty.
//    Suspected: src/index/hnsw.rs:561 (`DenseHnsw::search`).
//
// These three tests are intentionally RED. They are NOT marked `#[ignore]`.
// They must stay RED until the engine bugs are fixed in a follow-up task.

// ---------------------------------------------------------------------------
// TODO (follow-ups, file under 1500-line cap):
//
// 1. Add a `churn_oracle_sparse` test that drives Sparse top-k against the
//    exact dot-product oracle. The harness already has the slot — only the
//    schema and the per-10th-step oracle check need to be added.
// 2. Add a `churn_oracle_minhash` test that drives MinHash top-k against
//    the exact Jaccard oracle, mirroring
//    `churn_oracle_topk_matches_visible_brute_force` in `tests/retriever.rs`
//    but using this file's harness for full op-matrix coverage.
// 3. Extend the op matrix with `Database::snapshot()` registry pin (open a
//    Database, take a snapshot via `db.snapshot()`, drop on commit, and
//    confirm the floor behaves correctly).
// 4. Once the `Database::set_authorization_allowed_set` API lands, swap the
//    current `query_at_with_allowed` drill for the shipped path.
// ---------------------------------------------------------------------------
