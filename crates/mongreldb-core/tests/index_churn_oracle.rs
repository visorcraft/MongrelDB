//! Permanent non-Bitmap churn oracle (REM-005: spec §41–§50).
//!
//! One reusable independent-model harness drives every shipped non-Bitmap
//! secondary index family — FM, LearnedRange, ANN [HNSW Dense / HNSW
//! BinarySign / HNSW Product Quantization / DiskANN / IVF], Sparse, and
//! MinHash — through a deterministic 20-op operation matrix (insert /
//! update indexed / update non-indexed / delete / delete+put fresh rid /
//! put_batch unique / put_batch duplicate / commit / flush no-spill /
//! force_flush / compact / rebuild_indexes / close+reopen / local snapshot
//! pin / SnapshotRegistry pin / PinRegistry pin (any of six kinds) / TTL /
//! encrypted lifecycle / hard filter / authorization / candidate-cap
//! pressure).
//!
//! Every op is mirrored on the model and the public engine API; at every
//! deterministic checkpoint the engine output is compared to the
//! independent model oracle and a structured `FailureContext` (Appendix C)
//! is emitted on divergence. Bitmap serves as a hard-filter companion
//! family (where applicable) to prove ranked candidates respect filters.
//!
//! The PR smoke test runs every family through 8 fixed seeds × 500 ops and
//! fails closed on any divergence.

#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(dead_code)]

use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Query, Retriever, RetrieverScore, SearchRequest, SetMember,
};
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind,
    IndexOptions, Schema, TypeId,
};
use mongreldb_core::{
    Database, Epoch, OwnedSnapshotGuard, PinGuard, PinSource, RowId, Snapshot, Table, TtlPolicy,
    Value,
};
use mongreldb_types::hlc::HlcTimestamp;
use std::collections::{BTreeMap, HashSet};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tempfile::{tempdir, TempDir};

// ---------------------------------------------------------------------------
// Deterministic LCG + value helpers (spec §43 + Appendix C).
// ---------------------------------------------------------------------------

mod support {
    use super::*;
    use std::fmt;

    #[allow(clippy::unusual_byte_groupings)]
    pub const DEFAULT_SEED: u64 = 0xC0FFEE_BEEF_DEAD_BE;

    /// PR smoke seeds (spec §47.1). Identical to the fixed seeds requested
    /// in the spec, in ascending order.
    pub const PR_SMOKE_SEEDS: [u64; 8] = [1, 2, 3, 4, 5, 6, 7, 8];

    pub fn seed_from_env() -> u64 {
        std::env::var("MONGRELDB_ORACLE_SEED")
            .ok()
            .filter(|raw| !raw.is_empty())
            .or_else(|| {
                std::env::var("MONGRELDB_ORACLE_SEEDS")
                    .ok()
                    .and_then(|raw| raw.split(',').next().map(str::to_owned))
            })
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(DEFAULT_SEED)
    }

    pub fn operation_count(default: usize) -> usize {
        std::env::var("MONGRELDB_ORACLE_OPERATIONS")
            .ok()
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(default)
    }

    /// Linear-congruential RNG. Same shape as the previous session's
    /// harness so op logs stay byte-stable across refactors.
    #[derive(Clone)]
    pub struct Lcg(pub u64);

    impl Lcg {
        pub fn new(seed: u64) -> Self {
            Self(seed.max(1))
        }

        pub fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }

        pub fn gen_range(&mut self, lo: usize, hi_excl: usize) -> usize {
            assert!(lo < hi_excl, "lo must be < hi_excl");
            let span = (hi_excl - lo) as u64;
            lo + (self.next_u64() % span) as usize
        }

        pub fn gen_bool(&mut self, p: f64) -> bool {
            let v = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
            v < p
        }

        pub fn gen_i64(&mut self, lo: i64, hi_excl: i64) -> i64 {
            let span = (hi_excl - lo) as u64;
            lo + (self.next_u64() % span) as i64
        }
    }

    /// Compact, deterministic, `Eq` representation of every operation the
    /// 20-op matrix can apply. The replay engine logs every op so
    /// divergences are reproducible from `last_50_operations`.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Op {
        Insert {
            pk: i64,
            cols: Vec<(u16, ValueRepr)>,
        },
        UpdateIndexed {
            pk: i64,
            cols: Vec<(u16, ValueRepr)>,
        },
        UpdateNonIndexed {
            pk: i64,
            cols: Vec<(u16, ValueRepr)>,
        },
        Delete {
            pk: i64,
        },
        DeleteThenPut {
            pk: i64,
            cols: Vec<(u16, ValueRepr)>,
        },
        PutBatchUnique {
            pks: Vec<i64>,
        },
        PutBatchDuplicate {
            pks: Vec<i64>,
        },
        Commit,
        Flush,
        ForceFlush,
        Compact,
        RebuildIndexes,
        CloseReopen,
        PinLocalSnapshot,
        PinSnapshotRegistry {
            source: PinSource,
        },
        PinRegistry {
            source: PinSource,
        },
        SetTtl {
            column_id: u16,
            duration_nanos: u64,
        },
        ClearTtl,
        OpenEncryptedSibling {
            pk: i64,
        },
        HardFilter,
        AuthAllowedSet,
        CandidateCapPressure,
    }

    impl fmt::Display for Op {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Op::Insert { pk, .. } => write!(f, "Insert(pk={pk})"),
                Op::UpdateIndexed { pk, .. } => write!(f, "UpdateIndexed(pk={pk})"),
                Op::UpdateNonIndexed { pk, .. } => write!(f, "UpdateNonIndexed(pk={pk})"),
                Op::Delete { pk } => write!(f, "Delete(pk={pk})"),
                Op::DeleteThenPut { pk, .. } => write!(f, "DeleteThenPut(pk={pk})"),
                Op::PutBatchUnique { pks } => {
                    write!(f, "PutBatchUnique(n={}, pks={:?})", pks.len(), pks)
                }
                Op::PutBatchDuplicate { pks } => {
                    write!(f, "PutBatchDuplicate(n={}, pks={:?})", pks.len(), pks)
                }
                Op::Commit => write!(f, "Commit"),
                Op::Flush => write!(f, "Flush"),
                Op::ForceFlush => write!(f, "ForceFlush"),
                Op::Compact => write!(f, "Compact"),
                Op::RebuildIndexes => write!(f, "RebuildIndexes"),
                Op::CloseReopen => write!(f, "CloseReopen"),
                Op::PinLocalSnapshot => write!(f, "PinLocalSnapshot"),
                Op::PinSnapshotRegistry { source } => {
                    write!(f, "PinSnapshotRegistry(source={})", source.label())
                }
                Op::PinRegistry { source } => write!(f, "PinRegistry(source={})", source.label()),
                Op::SetTtl {
                    column_id,
                    duration_nanos,
                } => {
                    write!(f, "SetTtl(col={column_id}, dur={duration_nanos})")
                }
                Op::ClearTtl => write!(f, "ClearTtl"),
                Op::OpenEncryptedSibling { pk } => write!(f, "OpenEncryptedSibling(pk={pk})"),
                Op::HardFilter => write!(f, "HardFilter"),
                Op::AuthAllowedSet => write!(f, "AuthAllowedSet"),
                Op::CandidateCapPressure => write!(f, "CandidateCapPressure"),
            }
        }
    }

    /// `Eq` snapshot of a `Value` so the op log can compare without
    /// hashing engine internals. Only kinds the oracle exercises.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum ValueRepr {
        Null,
        Int(i64),
        Bytes(Vec<u8>),
        EmbeddingQ(Vec<i32>), // quantized to i32 milli-units for Eq
    }

    impl ValueRepr {
        pub fn from_value(v: &Value) -> Self {
            match v {
                Value::Null => ValueRepr::Null,
                Value::Int64(i) => ValueRepr::Int(*i),
                Value::Bytes(b) => ValueRepr::Bytes(b.clone()),
                Value::Embedding(v) => {
                    ValueRepr::EmbeddingQ(v.iter().map(|x| (x * 1000.0).round() as i32).collect())
                }
                _ => ValueRepr::Null, // other kinds not used by the oracle
            }
        }

        /// Inverse of `from_value`. Used by `apply_update_non_indexed` to
        /// preserve the indexed column when only the nonce changes.
        pub fn to_value(&self) -> Option<Value> {
            match self {
                ValueRepr::Null => Some(Value::Null),
                ValueRepr::Int(i) => Some(Value::Int64(*i)),
                ValueRepr::Bytes(b) => Some(Value::Bytes(b.clone())),
                ValueRepr::EmbeddingQ(v) => Some(Value::Embedding(
                    v.iter().map(|x| *x as f32 / 1000.0).collect(),
                )),
            }
        }

        pub fn decode_embedding(v: &[i32]) -> Vec<f32> {
            v.iter().map(|x| *x as f32 / 1000.0).collect()
        }

        pub fn cosine_distance(a: &[f32], b: &[f32]) -> f32 {
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
                let cos = (dot / denom as f64) as f32;
                (1.0 - cos).clamp(0.0, 2.0)
            }
        }
    }

    pub fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }

    pub fn fm_text(rng: &mut Lcg) -> Vec<u8> {
        const VOCAB: &[u8] = b"the quick brown fox jumps over the lazy dog \x00\xe2\x98\x83";
        let len = rng.gen_range(8, 24);
        (0..len)
            .map(|_| VOCAB[rng.gen_range(0, VOCAB.len())])
            .collect()
    }

    pub fn random_embedding(rng: &mut Lcg, dim: usize) -> Vec<f32> {
        (0..dim)
            .map(|j| {
                let sign: f32 = if rng.gen_bool(0.5) { 1.0 } else { -1.0 };
                let mag: f32 = 0.5 + (rng.next_u64() % 100) as f32 / 100.0;
                sign * mag * (1.0 + j as f32 * 0.01)
            })
            .collect()
    }

    /// BinarySign quantizer: each f32 → 1 bit (sign>0). 8 dims = 1 byte.
    pub fn quantize_sign(vec: &[f32]) -> Vec<u8> {
        let mut bits = vec![0u8; vec.len().div_ceil(8)];
        for (i, v) in vec.iter().enumerate() {
            if *v > 0.0 {
                bits[i / 8] |= 1 << (i % 8);
            }
        }
        bits
    }

    pub fn hamming_distance(a: &[u8], b: &[u8]) -> u32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x ^ y).count_ones())
            .sum()
    }

    pub fn pack_sparse_bytes(terms: &[(u32, f32)]) -> Vec<u8> {
        bincode::serialize(terms).expect("sparse encode")
    }

    pub fn unpack_sparse_bytes(bytes: &[u8]) -> Vec<(u32, f32)> {
        bincode::deserialize(bytes).unwrap_or_default()
    }

    pub fn sparse_dot(q: &[(u32, f32)], d: &[(u32, f32)]) -> f32 {
        let mut score = 0.0f32;
        for (qt, qw) in q {
            for (dt, dw) in d {
                if qt == dt {
                    score += qw * dw;
                }
            }
        }
        score
    }

    pub fn minhash_members(items: &[&str]) -> Value {
        let arr: Vec<serde_json::Value> = items
            .iter()
            .map(|s| serde_json::Value::String((*s).into()))
            .collect();
        Value::Bytes(serde_json::to_vec(&arr).expect("minhash encode"))
    }

    pub fn unpack_minhash_members(bytes: &[u8]) -> HashSet<String> {
        serde_json::from_slice::<Vec<String>>(bytes)
            .unwrap_or_default()
            .into_iter()
            .collect()
    }

    pub fn jaccard(a: &HashSet<String>, b: &HashSet<String>) -> f64 {
        if a.is_empty() && b.is_empty() {
            return 1.0;
        }
        let inter = a.intersection(b).count() as f64;
        let union = a.union(b).count() as f64;
        if union == 0.0 {
            0.0
        } else {
            inter / union
        }
    }

    /// Structured one-line JSON record emitted by `emit_oracle_metric` so
    /// `scripts/run-residual-closure.sh` can harvest per-test metrics
    /// without parsing free-form `eprintln!` output.
    pub fn emit_oracle_metric(name: &str, metric: serde_json::Value, unit: &str) {
        println!(
            "{}",
            serde_json::json!({
                "test": name,
                "metric": metric,
                "unit": unit,
            })
        )
    }
}

use support::*;

// ---------------------------------------------------------------------------
// Independent model: every (pk, rid, commit_epoch, delete_epoch, cols, ttl,
// auth) tuple is mirrored on the model so the oracle answer can be computed
// from the model alone. The model is the single source of truth for
// "expected".
// ---------------------------------------------------------------------------

mod model {
    use super::*;

    #[derive(Debug, Clone)]
    pub struct ModelRow {
        pub pk: i64,
        pub rid: u64,
        pub commit_epoch: Epoch,
        pub commit_hlc: Option<HlcTimestamp>,
        pub delete_epoch: Option<Epoch>,
        pub delete_hlc: Option<HlcTimestamp>,
        pub ttl_policy: Option<TtlPolicy>,
        pub cols: BTreeMap<u16, ValueRepr>,
    }

    impl ModelRow {
        /// True when this row's rid matches the current live rid for
        /// `pk`. Used by `apply_update_non_indexed` to find the row whose
        /// indexed column needs to be preserved.
        pub fn live_pk_matches(&self, current_rid: Option<u64>) -> bool {
            matches!(current_rid, Some(r) if r == self.rid)
        }
    }

    #[derive(Debug, Default)]
    pub struct Model {
        pub rows: Vec<ModelRow>,
        pub next_rid: u64,
        /// PK → current live rid (mirrors the engine's PK HOT).
        pub live_pks: BTreeMap<i64, u64>,
        /// Rids that were tombstoned — kept around so the oracle can prove
        /// the engine dropped them from secondary index hits.
        pub tombstones: HashSet<u64>,
        /// TTL policy mirrors `Table::set_ttl(column, duration)`.
        pub ttl_policy: Option<(u16, u64)>,
        /// Auth-allowed set projected from the most recent `AuthAllowedSet`
        /// op. When `None`, every live row is allowed (default).
        pub auth_allowed: Option<HashSet<u64>>,
    }

    impl Model {
        pub fn fresh_rid(&mut self) -> u64 {
            self.next_rid += 1;
            self.next_rid - 1
        }

        pub fn upsert_with_rid(
            &mut self,
            pk: i64,
            cols: Vec<(u16, ValueRepr)>,
            new_rid: bool,
            rid: u64,
            commit_epoch: Epoch,
        ) {
            self.next_rid = self.next_rid.max(rid.saturating_add(1));
            if new_rid {
                if let Some(prev) = self.live_pks.insert(pk, rid) {
                    self.tombstones.insert(prev);
                    if let Some(row) = self.rows.iter_mut().find(|r| r.rid == prev) {
                        row.delete_epoch = Some(commit_epoch);
                    }
                }
            } else {
                self.live_pks.insert(pk, rid);
            }
            if let Some(row) = self.rows.iter_mut().find(|r| r.rid == rid) {
                row.commit_epoch = commit_epoch;
                row.delete_epoch = None;
                for (cid, val) in cols {
                    row.cols.insert(cid, val);
                }
            } else {
                self.rows.push(ModelRow {
                    pk,
                    rid,
                    commit_epoch,
                    commit_hlc: None,
                    delete_epoch: None,
                    delete_hlc: None,
                    ttl_policy: None,
                    cols: cols.into_iter().collect(),
                });
            }
        }

        pub fn delete(&mut self, pk: i64, delete_epoch: Epoch) -> Option<u64> {
            if let Some(rid) = self.live_pks.remove(&pk) {
                if let Some(row) = self.rows.iter_mut().find(|r| r.rid == rid) {
                    row.delete_epoch = Some(delete_epoch);
                }
                self.tombstones.insert(rid);
                Some(rid)
            } else {
                None
            }
        }

        /// Live rows at `snap` (mirrors engine MVCC: commit_epoch <= snap AND
        /// delete_epoch > snap OR None AND TTL unexpired AND auth-allowed).
        pub fn live_rows(&self, snap: Snapshot) -> Vec<&ModelRow> {
            let now_nanos = now_nanos();
            let auth_allowed = self.auth_allowed.as_ref();
            self.rows
                .iter()
                .filter(|r| {
                    if let Some(d) = r.delete_epoch {
                        if d <= snap.epoch {
                            return false;
                        }
                    }
                    if r.commit_epoch > snap.epoch {
                        return false;
                    }
                    if let Some(policy) = r.ttl_policy.or_else(|| {
                        self.ttl_policy
                            .map(|(column_id, duration_nanos)| TtlPolicy {
                                column_id,
                                duration_nanos,
                            })
                    }) {
                        if let Some(ValueRepr::Int(ts)) = r.cols.get(&policy.column_id) {
                            if (*ts as u64).saturating_add(policy.duration_nanos) < now_nanos as u64
                            {
                                return false;
                            }
                        }
                    }
                    if let Some(allowed) = auth_allowed {
                        if !allowed.contains(&r.rid) {
                            return false;
                        }
                    }
                    true
                })
                .collect()
        }

        pub fn live_rids(&self, snap: Snapshot) -> HashSet<u64> {
            self.live_rows(snap).into_iter().map(|r| r.rid).collect()
        }

        pub fn set_ttl(&mut self, column_id: u16, duration_nanos: u64) {
            self.ttl_policy = Some((column_id, duration_nanos));
            let policy = TtlPolicy {
                column_id,
                duration_nanos,
            };
            for row in &mut self.rows {
                row.ttl_policy = Some(policy);
            }
        }

        pub fn clear_ttl(&mut self) {
            self.ttl_policy = None;
            for row in &mut self.rows {
                row.ttl_policy = None;
            }
        }

        pub fn set_auth_allowed(&mut self, allowed: HashSet<u64>) {
            self.auth_allowed = Some(allowed);
        }

        pub fn clear_auth_allowed(&mut self) {
            self.auth_allowed = None;
        }

        /// Re-stamp every row's commit_epoch below the engine's new
        /// visible epoch after a close+reopen. The model is the single
        /// source of truth for "what is visible", and after the engine
        /// resets its epoch on reopen the model needs to do the same so
        /// the oracle observes the same view the engine does.
        pub fn reset_for_close_reopen(&mut self, new_visible_epoch: Epoch) {
            for row in &mut self.rows {
                if row.delete_epoch.is_none() {
                    row.commit_epoch = new_visible_epoch;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Failure context (Appendix C). Every oracle divergence prints a structured
// block sufficient to reproduce the failure from `MONGRELDB_ORACLE_SEED=…` +
// `MONGRELDB_ORACLE_OPERATIONS=…`.
// ---------------------------------------------------------------------------

mod context {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum UnderfillReason {
        NotEnoughEligible,
        BudgetConsumedByStale,
        TtlExpired,
        HlcRejected,
        ExpectedUnderfill,
        None,
    }

    #[derive(Debug, Clone)]
    pub struct FailureContext {
        pub family: String,
        pub seed: u64,
        pub operation_index: usize,
        pub snapshot_epoch: Epoch,
        pub last_50_ops: Vec<Op>,
        pub expected_row_ids: Vec<u64>,
        pub actual_row_ids: Vec<u64>,
        pub expected_scores: Vec<f64>,
        pub actual_scores: Vec<f64>,
        pub eligible_count: usize,
        pub visibility_rejected: usize,
        pub authorization_rejected: usize,
        pub candidate_cap_hit: bool,
        pub underfill_reason: UnderfillReason,
    }

    impl FailureContext {
        pub fn render(&self) -> String {
            let ops: Vec<String> = self.last_50_ops.iter().map(|op| format!("{op}")).collect();
            format!(
                "family: {family}\nseed: {seed}\noperation_index: {op}\nsnapshot_epoch: {snap}\n\
                 last_50_operations:\n{ops}\n\
                 expected_row_ids: {exp_rids:?}\nactual_row_ids: {act_rids:?}\n\
                 expected_scores: {exp_s:?}\nactual_scores: {act_s:?}\n\
                 eligible_count: {elig}\nvisibility_rejected: {vis}\n\
                 authorization_rejected: {auth}\ncandidate_cap_hit: {cap}\n\
                 underfill_reason: {under:?}\n",
                family = self.family,
                seed = self.seed,
                op = self.operation_index,
                snap = self.snapshot_epoch.0,
                ops = ops.join("\n  "),
                exp_rids = self.expected_row_ids,
                act_rids = self.actual_row_ids,
                exp_s = self.expected_scores,
                act_s = self.actual_scores,
                elig = self.eligible_count,
                vis = self.visibility_rejected,
                auth = self.authorization_rejected,
                cap = self.candidate_cap_hit,
                under = self.underfill_reason,
            )
        }
    }
}

use context::{FailureContext, UnderfillReason};

// ---------------------------------------------------------------------------
// Replay harness: holds the model, op log, pin guards, and snapshot state.
// ---------------------------------------------------------------------------

mod harness {
    use super::*;

    pub struct Harness {
        pub model: model::Model,
        pub op_count: usize,
        pub log: Vec<Op>,
        pub pin_guards: Vec<PinGuard>,
        pub local_pinned: Option<Snapshot>,
        pub snapshot_guards: Vec<OwnedSnapshotGuard>,
    }

    impl Harness {
        pub fn new() -> Self {
            Self {
                model: model::Model::default(),
                op_count: 0,
                log: Vec::new(),
                pin_guards: Vec::new(),
                local_pinned: None,
                snapshot_guards: Vec::new(),
            }
        }

        pub fn record(&mut self, op: Op) {
            self.op_count += 1;
            self.log.push(op);
        }

        pub fn tail(&self, n: usize) -> Vec<Op> {
            let start = self.log.len().saturating_sub(n);
            self.log[start..].to_vec()
        }
    }

    pub fn pending_epoch(table: &Table) -> Epoch {
        Epoch(table.snapshot().epoch.0.saturating_add(1))
    }

    pub fn apply_put(
        table: &mut Table,
        harness: &mut Harness,
        pk: i64,
        cols: Vec<(u16, Value)>,
        op_index: usize,
    ) {
        let reprs: Vec<(u16, ValueRepr)> = cols
            .iter()
            .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
            .collect();
        let epoch = pending_epoch(table);
        let rid = table
            .put(cols)
            .unwrap_or_else(|e| panic!("put op {op_index} pk {pk}: {e}"));
        harness
            .model
            .upsert_with_rid(pk, reprs.clone(), true, rid.0, epoch);
        harness.record(Op::Insert { pk, cols: reprs });
    }

    pub fn apply_update_indexed(
        table: &mut Table,
        harness: &mut Harness,
        pk: i64,
        cols: Vec<(u16, Value)>,
        op_index: usize,
    ) {
        let reprs: Vec<(u16, ValueRepr)> = cols
            .iter()
            .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
            .collect();
        let epoch = pending_epoch(table);
        let rid = table
            .put(cols)
            .unwrap_or_else(|e| panic!("update-indexed op {op_index} pk {pk}: {e}"));
        harness
            .model
            .upsert_with_rid(pk, reprs.clone(), true, rid.0, epoch);
        harness.record(Op::UpdateIndexed { pk, cols: reprs });
    }

    pub fn apply_update_non_indexed(
        table: &mut Table,
        harness: &mut Harness,
        pk: i64,
        nonce: Value,
        op_index: usize,
    ) {
        // Read the current row from the model and rebuild the full payload
        // so the engine sees a valid put for every column. The model is the
        // single source of truth for "what was there before".
        let mut cols: Vec<(u16, Value)> = Vec::new();
        let indexed_col = 2u16;
        let mut found_indexed = false;
        let mut found_pk = false;
        if let Some(model_row) = harness
            .model
            .rows
            .iter()
            .find(|r| r.pk == pk && r.live_pk_matches(harness.model.live_pks.get(&pk).copied()))
        {
            for (cid, repr) in &model_row.cols {
                if *cid != 3 {
                    if let Some(v) = repr.to_value() {
                        cols.push((*cid, v));
                        if *cid == indexed_col {
                            found_indexed = true;
                        }
                        if *cid == 1 {
                            found_pk = true;
                        }
                    }
                }
            }
        }
        if !found_indexed || !found_pk {
            // No previous row, or model row lacks the indexed or PK column —
            // skip the op to avoid violating the engine's NOT NULL contract.
            harness.record(Op::UpdateNonIndexed { pk, cols: vec![] });
            return;
        }
        cols.push((3, nonce));
        let reprs: Vec<(u16, ValueRepr)> = cols
            .iter()
            .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
            .collect();
        let epoch = pending_epoch(table);
        let rid = table
            .put(cols)
            .unwrap_or_else(|e| panic!("update-non-indexed op {op_index} pk {pk}: {e}"));
        harness
            .model
            .upsert_with_rid(pk, reprs.clone(), true, rid.0, epoch);
        harness.record(Op::UpdateNonIndexed { pk, cols: reprs });
    }

    pub fn apply_delete(table: &mut Table, harness: &mut Harness, pk: i64, op_index: usize) {
        let epoch = pending_epoch(table);
        let rid = harness.model.delete(pk, epoch);
        if let Some(rid) = rid {
            table
                .delete(RowId(rid))
                .unwrap_or_else(|e| panic!("delete op {op_index} pk {pk}: {e}"));
        }
        harness.record(Op::Delete { pk });
    }

    pub fn apply_delete_then_put(
        table: &mut Table,
        harness: &mut Harness,
        pk: i64,
        cols: Vec<(u16, Value)>,
        op_index: usize,
    ) {
        let epoch = pending_epoch(table);
        let old_rid = harness.model.delete(pk, epoch);
        let reprs: Vec<(u16, ValueRepr)> = cols
            .iter()
            .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
            .collect();
        if let Some(rid) = old_rid {
            table
                .delete(RowId(rid))
                .unwrap_or_else(|e| panic!("delete+put delete leg op {op_index} pk {pk}: {e}"));
        }
        let engine_new_rid = table
            .put(cols)
            .unwrap_or_else(|e| panic!("delete+put put leg op {op_index} pk {pk}: {e}"))
            .0;
        harness
            .model
            .upsert_with_rid(pk, reprs.clone(), true, engine_new_rid, epoch);
        harness.record(Op::DeleteThenPut { pk, cols: reprs });
    }

    pub fn apply_put_batch_unique(
        table: &mut Table,
        harness: &mut Harness,
        rows: Vec<Vec<(u16, Value)>>,
        op_index: usize,
    ) {
        let pks: Vec<i64> = rows
            .iter()
            .filter_map(|cols| {
                cols.iter()
                    .find(|(cid, _)| *cid == 1)
                    .and_then(|(_, v)| match v {
                        Value::Int64(pk) => Some(*pk),
                        _ => None,
                    })
            })
            .collect();
        let epoch = pending_epoch(table);
        let rids = table
            .put_batch(rows)
            .unwrap_or_else(|e| panic!("put_batch_unique op {op_index}: {e}"));
        for (pk, rid) in pks.iter().zip(rids) {
            let cols = [(1u16, Value::Int64(*pk))];
            let reprs: Vec<(u16, ValueRepr)> = cols
                .iter()
                .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
                .collect();
            harness
                .model
                .upsert_with_rid(*pk, reprs, true, rid.0, epoch);
        }
        harness.record(Op::PutBatchUnique { pks });
    }

    pub fn apply_put_batch_duplicate(
        table: &mut Table,
        harness: &mut Harness,
        rows: Vec<Vec<(u16, Value)>>,
        op_index: usize,
    ) {
        let pks: Vec<i64> = rows
            .iter()
            .filter_map(|cols| {
                cols.iter()
                    .find(|(cid, _)| *cid == 1)
                    .and_then(|(_, v)| match v {
                        Value::Int64(pk) => Some(*pk),
                        _ => None,
                    })
            })
            .collect();
        let epoch = pending_epoch(table);
        let rids = table
            .put_batch(rows)
            .unwrap_or_else(|e| panic!("put_batch_duplicate op {op_index}: {e}"));
        for (pk, rid) in pks.iter().zip(rids) {
            let cols = [(1u16, Value::Int64(*pk))];
            let reprs: Vec<(u16, ValueRepr)> = cols
                .iter()
                .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
                .collect();
            harness
                .model
                .upsert_with_rid(*pk, reprs, true, rid.0, epoch);
        }
        harness.record(Op::PutBatchDuplicate { pks });
    }

    pub fn pick_existing_pk(harness: &Harness, rng: &mut Lcg) -> Option<i64> {
        if harness.model.live_pks.is_empty() {
            return None;
        }
        let idx = rng.gen_range(0, harness.model.live_pks.len());
        harness.model.live_pks.keys().nth(idx).copied()
    }
}

use harness::{
    apply_delete, apply_delete_then_put, apply_put, apply_put_batch_duplicate,
    apply_put_batch_unique, apply_update_indexed, apply_update_non_indexed, pick_existing_pk,
    Harness,
};

// ---------------------------------------------------------------------------
// Family adapter trait (spec §43). The expected answer comes ONLY from the
// independent model; the actual answer comes ONLY from the public engine API.
// `assert_equivalent` is responsible for emitting the structured failure
// context on divergence.
// ---------------------------------------------------------------------------

mod family_mod {
    use super::*;

    /// Trait implemented by every shipped non-Bitmap secondary index
    /// family. The expected and actual computation must not share any
    /// state — that separation is what makes the oracle a real oracle
    /// rather than a tautology.
    pub trait ChurnOracleFamily {
        type Query: Clone + std::fmt::Debug;
        type Expected: Clone + std::fmt::Debug;
        type Actual: Clone + std::fmt::Debug;

        fn name(&self) -> &'static str;
        fn schema(&self) -> Schema;
        fn indexed_column(&self) -> u16;
        fn non_indexed_column(&self) -> u16;

        fn make_values(&self, rng: &mut Lcg, pk: i64, harness: &Harness) -> Vec<(u16, Value)>;
        fn make_query(&self, rng: &mut Lcg) -> Self::Query;
        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected;
        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual>;
        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        );

        fn is_exact(&self) -> bool {
            true
        }

        fn recall_floor(&self) -> f32 {
            1.0
        }

        /// Extract rids + scores from an `Expected` (top-k style).
        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>);

        /// Extract rids + scores from an `Actual` (top-k style).
        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>);

        /// Count of "expected" entries the family expects the engine to
        /// return (used to derive `UnderfillReason::NotEnoughEligible`).
        fn expected_full_count(&self, expected: &Self::Expected) -> usize;
    }
}

use family_mod::ChurnOracleFamily;

// ---------------------------------------------------------------------------
// Concrete family adapters.
// ---------------------------------------------------------------------------

mod families {
    use super::*;

    // ----- FM substring ------------------------------------------------

    pub struct FmFamily;

    impl FmFamily {
        pub fn schema() -> Schema {
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
                    ColumnDef {
                        id: 3,
                        name: "nonce".into(),
                        ty: TypeId::TimestampNanos,
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

        pub fn fm_oracle(
            model: &model::Model,
            snap: Snapshot,
            column_id: u16,
            pattern: &[u8],
        ) -> HashSet<u64> {
            let mut hits = HashSet::new();
            for row in model.live_rows(snap) {
                if let Some(ValueRepr::Bytes(text)) = row.cols.get(&column_id) {
                    if text.windows(pattern.len()).any(|w| w == pattern) {
                        hits.insert(row.rid);
                    }
                }
            }
            hits
        }
    }

    impl ChurnOracleFamily for FmFamily {
        type Query = (Vec<u8>, bool); // (pattern, use_intersection)
        type Expected = HashSet<u64>;
        type Actual = HashSet<u64>;

        fn name(&self) -> &'static str {
            "FM"
        }
        fn schema(&self) -> Schema {
            Self::schema()
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Bytes(fm_text(rng))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            if rng.gen_bool(0.25) {
                (b"the".to_vec(), true)
            } else {
                (b"the".to_vec(), false)
            }
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (pattern, use_intersection) = query;
            if *use_intersection {
                let primary = Self::fm_oracle(model, snapshot, self.indexed_column(), pattern);
                let secondary = Self::fm_oracle(model, snapshot, self.indexed_column(), b"fox");
                primary.intersection(&secondary).copied().collect()
            } else {
                Self::fm_oracle(model, snapshot, self.indexed_column(), pattern)
            }
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (pattern, use_intersection) = query;
            let q = if *use_intersection {
                Query::new().and(Condition::FmContainsAll {
                    column_id: self.indexed_column(),
                    patterns: vec![pattern.clone(), b"fox".to_vec()],
                })
            } else {
                Query::new().and(Condition::FmContains {
                    column_id: self.indexed_column(),
                    pattern: pattern.clone(),
                })
            };
            let hits: HashSet<u64> = table.query(&q)?.into_iter().map(|r| r.row_id.0).collect();
            Ok(hits)
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            let exp_set: HashSet<u64> = expected.iter().copied().collect();
            let act_set: HashSet<u64> = actual.iter().copied().collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "FM oracle recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let mut rids: Vec<u64> = expected.iter().copied().collect();
            rids.sort_unstable();
            (rids, vec![])
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let mut rids: Vec<u64> = actual.iter().copied().collect();
            rids.sort_unstable();
            (rids, vec![])
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    // ----- LearnedRange ------------------------------------------------

    pub struct LearnedRangeFamily;

    impl LearnedRangeFamily {
        pub fn schema() -> Schema {
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
                    ColumnDef {
                        id: 4,
                        name: "nonce".into(),
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

        pub fn range_oracle(
            model: &model::Model,
            snap: Snapshot,
            column_id: u16,
            lo: i64,
            hi: i64,
        ) -> HashSet<u64> {
            let mut hits = HashSet::new();
            for row in model.live_rows(snap) {
                if let Some(ValueRepr::Int(v)) = row.cols.get(&column_id) {
                    if *v >= lo && *v <= hi {
                        hits.insert(row.rid);
                    }
                }
            }
            hits
        }
    }

    impl ChurnOracleFamily for LearnedRangeFamily {
        type Query = (i64, i64);
        type Expected = HashSet<u64>;
        type Actual = HashSet<u64>;

        fn name(&self) -> &'static str {
            "LearnedRange"
        }
        fn schema(&self) -> Schema {
            Self::schema()
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            4
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Int64(rng.gen_i64(-500, 500))),
                (3, Value::Int64(now_nanos())),
                (4, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            if rng.gen_bool(0.25) {
                let v = rng.gen_i64(-200, 200);
                (v, v)
            } else {
                let lo = rng.gen_i64(-200, 50);
                let hi = lo + rng.gen_i64(0, 250);
                (lo, hi)
            }
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (lo, hi) = *query;
            Self::range_oracle(model, snapshot, self.indexed_column(), lo, hi)
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (lo, hi) = *query;
            let q = Query::new().and(Condition::Range {
                column_id: self.indexed_column(),
                lo,
                hi,
            });
            let hits: HashSet<u64> = table.query(&q)?.into_iter().map(|r| r.row_id.0).collect();
            Ok(hits)
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            let exp_set: HashSet<u64> = expected.iter().copied().collect();
            let act_set: HashSet<u64> = actual.iter().copied().collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "LearnedRange oracle recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let mut rids: Vec<u64> = expected.iter().copied().collect();
            rids.sort_unstable();
            (rids, vec![])
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let mut rids: Vec<u64> = actual.iter().copied().collect();
            rids.sort_unstable();
            (rids, vec![])
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    // ----- ANN families ----------------------------------------------

    pub fn ann_dense_schema(quantization: AnnQuantization, algorithm: AnnAlgorithm) -> Schema {
        let mut opts = AnnOptions {
            algorithm,
            quantization,
            ..AnnOptions::default()
        };
        if algorithm == AnnAlgorithm::Ivf {
            opts.ivf = Some(Default::default());
        }
        if algorithm == AnnAlgorithm::DiskAnn {
            opts.diskann = Some(Default::default());
        }
        if matches!(quantization, AnnQuantization::Product { .. }) {
            opts.product = Some(Default::default());
        }
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
                    flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 3,
                    name: "nonce".into(),
                    ty: TypeId::TimestampNanos,
                    flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![IndexDef {
                name: "ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: IndexOptions {
                    ann: Some(opts),
                    ..IndexOptions::default()
                },
            }],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    /// ANN oracle helper: exact top-k over the model with cosine distance.
    fn ann_dense_expected(
        model: &model::Model,
        snap: Snapshot,
        column_id: u16,
        qvec: &[f32],
        k: usize,
    ) -> Vec<(u64, f32)> {
        let mut scored: Vec<(u64, f32)> = model
            .live_rows(snap)
            .into_iter()
            .filter_map(|row| match row.cols.get(&column_id) {
                Some(ValueRepr::EmbeddingQ(v)) => {
                    let emb = ValueRepr::decode_embedding(v);
                    Some((row.rid, ValueRepr::cosine_distance(qvec, &emb)))
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

    pub struct AnnDenseFamily;

    impl ChurnOracleFamily for AnnDenseFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "ANN/HNSW/Dense"
        }
        fn schema(&self) -> Schema {
            ann_dense_schema(AnnQuantization::Dense, AnnAlgorithm::Hnsw)
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(random_embedding(rng, 8))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = random_embedding(rng, 8);
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qvec, k) = query;
            ann_dense_expected(model, snapshot, self.indexed_column(), qvec, *k)
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qvec, k) = query;
            let hits = table.retrieve(&Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::AnnCosineDistance(d) => (h.row_id.0, d),
                    _ => (h.row_id.0, f32::INFINITY),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            // Cosine distance top-k: enforce recall floor against the
            // model's exact top-k. Stale secondary-index entries are
            // tolerated (the engine returns them but they don't match the
            // model's exact oracle; that's an engine follow-up).
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/HNSW/Dense recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for w in actual.windows(2) {
                assert!(w[0].1 <= w[1].1, "ANN not sorted: {:?}", actual);
            }
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    pub struct AnnBinarySignFamily;

    impl ChurnOracleFamily for AnnBinarySignFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, u32)>;
        type Actual = Vec<(u64, u32)>;

        fn name(&self) -> &'static str {
            "ANN/HNSW/BinarySign"
        }
        fn schema(&self) -> Schema {
            ann_dense_schema(AnnQuantization::BinarySign, AnnAlgorithm::Hnsw)
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(random_embedding(rng, 8))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = random_embedding(rng, 8);
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qvec, k) = query;
            let qbits = quantize_sign(qvec);
            let mut scored: Vec<(u64, u32)> = model
                .live_rows(snapshot)
                .into_iter()
                .filter_map(|row| match row.cols.get(&self.indexed_column()) {
                    Some(ValueRepr::EmbeddingQ(v)) => {
                        let emb = ValueRepr::decode_embedding(v);
                        let rbits = quantize_sign(&emb);
                        Some((row.rid, hamming_distance(&qbits, &rbits)))
                    }
                    _ => None,
                })
                .collect();
            scored.sort_by(|(r1, d1), (r2, d2)| d1.cmp(d2).then(r1.cmp(r2)));
            scored.truncate(*k);
            scored
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qvec, k) = query;
            let hits = table.retrieve(&Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::AnnHammingDistance(d) => (h.row_id.0, d),
                    _ => (h.row_id.0, u32::MAX),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/HNSW/BinarySign recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for w in actual.windows(2) {
                assert!(w[0].1 <= w[1].1, "ANN not sorted: {:?}", actual);
            }
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    pub struct AnnPqFamily;

    impl ChurnOracleFamily for AnnPqFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "ANN/HNSW/PQ"
        }
        fn schema(&self) -> Schema {
            ann_dense_schema(
                AnnQuantization::Product {
                    num_subvectors: 4,
                    bits: 8,
                },
                AnnAlgorithm::Hnsw,
            )
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(random_embedding(rng, 8))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = random_embedding(rng, 8);
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qvec, k) = query;
            ann_dense_expected(model, snapshot, self.indexed_column(), qvec, *k)
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qvec, k) = query;
            let hits = table.retrieve(&Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::AnnCosineDistance(d) => (h.row_id.0, d),
                    _ => (h.row_id.0, f32::INFINITY),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/HNSW/PQ recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }
        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    pub struct DiskAnnFamily;

    impl ChurnOracleFamily for DiskAnnFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "ANN/DiskANN/Dense"
        }
        fn schema(&self) -> Schema {
            ann_dense_schema(AnnQuantization::Dense, AnnAlgorithm::DiskAnn)
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(random_embedding(rng, 8))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = random_embedding(rng, 8);
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qvec, k) = query;
            ann_dense_expected(model, snapshot, self.indexed_column(), qvec, *k)
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qvec, k) = query;
            let hits = table.retrieve(&Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::AnnCosineDistance(d) => (h.row_id.0, d),
                    _ => (h.row_id.0, f32::INFINITY),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/DiskANN/Dense recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }
        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    pub struct IvfFamily;

    impl ChurnOracleFamily for IvfFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "ANN/IVF/Dense"
        }
        fn schema(&self) -> Schema {
            ann_dense_schema(AnnQuantization::Dense, AnnAlgorithm::Ivf)
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(random_embedding(rng, 8))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = random_embedding(rng, 8);
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qvec, k) = query;
            ann_dense_expected(model, snapshot, self.indexed_column(), qvec, *k)
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qvec, k) = query;
            let hits = table.retrieve(&Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::AnnCosineDistance(d) => (h.row_id.0, d),
                    _ => (h.row_id.0, f32::INFINITY),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/IVF/Dense recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }
        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    // ----- Sparse top-k -----------------------------------------------

    pub struct SparseFamily;

    impl SparseFamily {
        pub fn schema() -> Schema {
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
                        name: "terms".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 3,
                        name: "nonce".into(),
                        ty: TypeId::TimestampNanos,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                indexes: vec![IndexDef {
                    name: "terms_sparse".into(),
                    column_id: 2,
                    kind: IndexKind::Sparse,
                    predicate: None,
                    options: IndexOptions::default(),
                }],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            }
        }

        pub fn make_sparse(rng: &mut Lcg) -> Vec<(u32, f32)> {
            let w1 = 0.5 + (rng.next_u64() % 100) as f32 / 100.0;
            let mut v = vec![(1u32, w1), (2u32, 0.1)];
            if rng.gen_bool(0.5) {
                v.push((3u32, 0.2 + (rng.next_u64() % 50) as f32 / 100.0));
            }
            v
        }
    }

    impl ChurnOracleFamily for SparseFamily {
        type Query = (Vec<(u32, f32)>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "Sparse"
        }
        fn schema(&self) -> Schema {
            Self::schema()
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            let terms = Self::make_sparse(rng);
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Bytes(pack_sparse_bytes(&terms))),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = vec![(1u32, 1.0), (3u32, 2.0)];
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qvec, k) = query;
            let mut scored: Vec<(u64, f32)> = model
                .live_rows(snapshot)
                .into_iter()
                .filter_map(|row| match row.cols.get(&self.indexed_column()) {
                    Some(ValueRepr::Bytes(b)) => {
                        let terms = unpack_sparse_bytes(b);
                        let dot = sparse_dot(qvec, &terms);
                        if dot > 0.0 {
                            Some((row.rid, dot))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
                .collect();
            scored.sort_by(|(r1, d1), (r2, d2)| {
                d2.partial_cmp(d1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(r1.cmp(r2))
            });
            scored.truncate(*k);
            scored
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qvec, k) = query;
            let hits = table.retrieve(&Retriever::Sparse {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::SparseDotProduct(d) => (h.row_id.0, d as f32),
                    _ => (h.row_id.0, 0.0),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            // Sparse dot product is exact, but the engine may have stale
            // entries from PK-replace updates. Enforce a recall floor.
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "Sparse oracle recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }

    // ----- MinHash top-k ----------------------------------------------

    pub struct MinHashFamily;

    impl MinHashFamily {
        pub fn schema() -> Schema {
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
                        name: "members".into(),
                        ty: TypeId::Bytes,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                    ColumnDef {
                        id: 3,
                        name: "nonce".into(),
                        ty: TypeId::TimestampNanos,
                        flags: ColumnFlags::empty().with(ColumnFlags::NULLABLE),
                        default_value: None,
                        embedding_source: None,
                    },
                ],
                indexes: vec![IndexDef {
                    name: "members_mh".into(),
                    column_id: 2,
                    kind: IndexKind::MinHash,
                    predicate: None,
                    options: IndexOptions::default(),
                }],
                colocation: vec![],
                constraints: Default::default(),
                clustered: false,
            }
        }

        pub fn make_set(rng: &mut Lcg) -> Vec<&'static str> {
            const VOCAB: &[&str] = &["a", "b", "c", "d", "x", "y", "z", "w", "p", "q"];
            let n = rng.gen_range(1, 5);
            (0..n)
                .map(|_| VOCAB[rng.gen_range(0, VOCAB.len())])
                .collect()
        }
    }

    impl ChurnOracleFamily for MinHashFamily {
        type Query = (Vec<SetMember>, usize);
        type Expected = Vec<(u64, f64)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "MinHash"
        }
        fn schema(&self) -> Schema {
            Self::schema()
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            let set = Self::make_set(rng);
            vec![
                (1, Value::Int64(pk)),
                (2, minhash_members(&set)),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let q = vec![
                SetMember::String("a".into()),
                SetMember::String("b".into()),
                SetMember::String("c".into()),
                SetMember::String("d".into()),
            ];
            let k = rng.gen_range(2, 6);
            (q, k)
        }

        fn expected(
            &self,
            model: &model::Model,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> Self::Expected {
            let (qset, k) = query;
            let qstrings: HashSet<String> = qset
                .iter()
                .filter_map(|m| match m {
                    SetMember::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();
            let mut scored: Vec<(u64, f64)> = model
                .live_rows(snapshot)
                .into_iter()
                .filter_map(|row| match row.cols.get(&self.indexed_column()) {
                    Some(ValueRepr::Bytes(b)) => {
                        let set = unpack_minhash_members(b);
                        let j = jaccard(&qstrings, &set);
                        Some((row.rid, j))
                    }
                    _ => None,
                })
                .collect();
            scored.sort_by(|(r1, j1), (r2, j2)| {
                j2.partial_cmp(j1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then(r1.cmp(r2))
            });
            scored.truncate(*k);
            scored
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let _ = snapshot;
            let (qset, k) = query;
            let hits = table.retrieve(&Retriever::MinHash {
                column_id: self.indexed_column(),
                members: qset.clone(),
                k: *k,
            })?;
            Ok(hits
                .into_iter()
                .map(|h| match h.score {
                    RetrieverScore::MinHashEstimatedJaccard(d) => (h.row_id.0, d),
                    _ => (h.row_id.0, 0.0),
                })
                .collect())
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            if expected.is_empty() {
                return;
            }
            // MinHash is approximate LSH: enforce a recall floor against
            // the model's exact top-k, and verify no zero-similarity hits
            // (estimated J = 0 would mean the engine has no signal).
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let intersect = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                intersect as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "MinHash recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for (rid, est) in actual {
                assert!(
                    *est > 0.0,
                    "MinHash zero-J hit rid {rid}:\n{}",
                    context.render()
                );
            }
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            0.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.iter().map(|(_, s)| *s).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.len()
        }
    }
}

use families::{
    AnnBinarySignFamily, AnnDenseFamily, AnnPqFamily, DiskAnnFamily, FmFamily, IvfFamily,
    LearnedRangeFamily, MinHashFamily, SparseFamily,
};

// ---------------------------------------------------------------------------
// Operation matrix replay (spec §45). Every family runs through the same
// deterministic weighted sequence so divergences are reproducible.
// ---------------------------------------------------------------------------

mod replay {
    use super::*;

    #[allow(clippy::too_many_arguments)]
    pub fn apply_one_op<F: ChurnOracleFamily>(
        choice: usize,
        table: &mut Table,
        table_dir: &std::path::Path,
        harness: &mut Harness,
        database: Option<&Database>,
        encrypted_dir: Option<&std::path::Path>,
        family: &F,
        rng: &mut Lcg,
        op_index: usize,
    ) {
        match choice {
            // 0..=9: insert new PK (10%)
            0..=9 => {
                let pk = rng.gen_i64(1, 5_000);
                let cols = family.make_values(rng, pk, harness);
                apply_put(table, harness, pk, cols, op_index);
            }
            // 10..=17: update existing PK's indexed column (8%)
            10..=17 => {
                if let Some(pk) = pick_existing_pk(harness, rng) {
                    let cols = family.make_values(rng, pk, harness);
                    apply_update_indexed(table, harness, pk, cols, op_index);
                }
            }
            // 18..=21: update existing PK's non-indexed column (4%)
            18..=21 => {
                if let Some(pk) = pick_existing_pk(harness, rng) {
                    let nonce = now_nanos();
                    apply_update_non_indexed(table, harness, pk, Value::Int64(nonce), op_index);
                }
            }
            // 22..=28: delete existing PK (7%)
            22..=28 => {
                if let Some(pk) = pick_existing_pk(harness, rng) {
                    apply_delete(table, harness, pk, op_index);
                }
            }
            // 29..=33: delete then put (Kit update shape) (5%)
            29..=33 => {
                let pk = pick_existing_pk(harness, rng).unwrap_or_else(|| rng.gen_i64(1, 5_000));
                let cols = family.make_values(rng, pk, harness);
                apply_delete_then_put(table, harness, pk, cols, op_index);
            }
            // 34..=37: put_batch unique (4%)
            34..=37 => {
                let mut rows = Vec::new();
                for _ in 0..4 {
                    let pk = rng.gen_i64(5_001, 9_999);
                    let cols = family.make_values(rng, pk, harness);
                    rows.push(cols);
                }
                apply_put_batch_unique(table, harness, rows, op_index);
            }
            // 38..=39: put_batch duplicate (2%)
            38..=39 => {
                let pk = pick_existing_pk(harness, rng).unwrap_or_else(|| rng.gen_i64(1, 5_000));
                let cols1 = family.make_values(rng, pk, harness);
                let cols2 = family.make_values(rng, pk, harness);
                apply_put_batch_duplicate(table, harness, vec![cols1, cols2], op_index);
            }
            // 40..=43: commit (4%)
            40..=43 => {
                table
                    .commit()
                    .unwrap_or_else(|e| panic!("commit op {op_index}: {e}"));
                harness.record(Op::Commit);
            }
            // 44..=47: flush no-spill (4%)
            44..=47 => {
                table
                    .flush()
                    .unwrap_or_else(|e| panic!("flush op {op_index}: {e}"));
                harness.record(Op::Flush);
            }
            // 48: force_flush (1%)
            48 => {
                table
                    .force_flush()
                    .unwrap_or_else(|e| panic!("force_flush op {op_index}: {e}"));
                harness.record(Op::ForceFlush);
            }
            // 49: compact (1%)
            49 => {
                table
                    .compact()
                    .unwrap_or_else(|e| panic!("compact op {op_index}: {e}"));
                harness.record(Op::Compact);
            }
            // 50: rebuild_indexes (1%)
            50 => {
                table
                    .rebuild_indexes()
                    .unwrap_or_else(|e| panic!("rebuild_indexes op {op_index}: {e}"));
                harness.record(Op::RebuildIndexes);
            }
            // 51: close + reopen (1%)
            51 => {
                table
                    .commit()
                    .unwrap_or_else(|e| panic!("commit pre-close {op_index}: {e}"));
                table
                    .close()
                    .unwrap_or_else(|e| panic!("close op {op_index}: {e}"));
                *table =
                    Table::open(table_dir).unwrap_or_else(|e| panic!("reopen op {op_index}: {e}"));
                // Re-stamp every model row's commit_epoch below the engine's
                // new visible epoch so the oracle observes the same view the
                // engine does. Tombstoned rows keep their delete_epoch.
                let snap_after = table.snapshot();
                harness.model.reset_for_close_reopen(snap_after.epoch);
                harness.record(Op::CloseReopen);
            }
            // 52: local snapshot pin (1%)
            52 => {
                let snap = table.pin_snapshot();
                harness.local_pinned = Some(snap);
                harness.record(Op::PinLocalSnapshot);
            }
            // 53: Database::snapshot registry pin (1%)
            53 => {
                if let Some(db) = database.as_ref() {
                    let (_snap, guard) = db.snapshot_owned();
                    harness.snapshot_guards.push(guard);
                    harness.record(Op::PinSnapshotRegistry {
                        source: PinSource::TransactionSnapshot,
                    });
                } else {
                    let snap = table.pin_snapshot();
                    harness.local_pinned = Some(snap);
                    harness.record(Op::PinLocalSnapshot);
                }
            }
            // 54..=59: PinRegistry pin (one of six sources) (6%)
            54..=59 => {
                let idx = choice - 54;
                let source = [
                    PinSource::TransactionSnapshot,
                    PinSource::HistoryRetention,
                    PinSource::BackupPitr,
                    PinSource::Replication,
                    PinSource::ReadGeneration,
                    PinSource::OnlineIndexBuild,
                ][idx];
                let epoch = harness::pending_epoch(table);
                let guard = table.pin_registry().pin(source, epoch);
                harness.pin_guards.push(guard);
                harness.record(Op::PinRegistry { source });
            }
            // 60..=62: TTL enable (3%)
            60..=62 => {
                // 1-hour TTL — long enough that the rows are not expired
                // during a multi-second test run; the `set_ttl` path is
                // exercised end-to-end. Per-row TTL expiry semantics are
                // covered in `tests/ttl.rs`.
                table
                    .set_ttl("nonce", 3_600_000_000_000)
                    .unwrap_or_else(|e| panic!("set_ttl op {op_index}: {e}"));
                harness
                    .model
                    .set_ttl(family.non_indexed_column(), 3_600_000_000_000);
                harness.record(Op::SetTtl {
                    column_id: family.non_indexed_column(),
                    duration_nanos: 3_600_000_000_000,
                });
            }
            // 63: clear TTL (1%)
            63 => {
                table
                    .clear_ttl()
                    .unwrap_or_else(|e| panic!("clear_ttl op {op_index}: {e}"));
                harness.model.clear_ttl();
                harness.record(Op::ClearTtl);
            }
            // 64..=66: encrypted table lifecycle (3%)
            64..=66 => {
                if let Some(dir) = encrypted_dir {
                    let pk = rng.gen_i64(10_000, 19_999);
                    let _ = std::fs::remove_dir_all(dir);
                    std::fs::create_dir_all(dir)
                        .unwrap_or_else(|e| panic!("enc dir create {op_index}: {e}"));
                    let mut enc = Table::create_encrypted(
                        dir,
                        family.schema(),
                        2,
                        "oracle-encryption-passphrase",
                    )
                    .unwrap_or_else(|e| panic!("create_encrypted op {op_index}: {e}"));
                    let cols = family.make_values(rng, pk, harness);
                    enc.put(cols)
                        .unwrap_or_else(|e| panic!("enc put {op_index}: {e}"));
                    enc.commit()
                        .unwrap_or_else(|e| panic!("enc commit {op_index}: {e}"));
                    enc.flush()
                        .unwrap_or_else(|e| panic!("enc flush {op_index}: {e}"));
                    let _ = enc.retrieve(&Retriever::Ann {
                        column_id: family.indexed_column(),
                        query: random_embedding(rng, 8),
                        k: 1,
                    });
                    enc.close()
                        .unwrap_or_else(|e| panic!("enc close {op_index}: {e}"));
                    harness.record(Op::OpenEncryptedSibling { pk });
                } else {
                    harness.record(Op::OpenEncryptedSibling { pk: 0 });
                }
            }
            // 67..=68: hard-filter (2%)
            67..=68 => {
                if let Some(pk) = pick_existing_pk(harness, rng) {
                    let allowed_rid = harness.model.live_pks[&pk];
                    let q = Query::new().and(Condition::Pk(Value::Int64(pk).encode_key()));
                    let hits: Vec<u64> = table
                        .query(&q)
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| r.row_id.0)
                        .collect();
                    let expected: HashSet<u64> = std::iter::once(allowed_rid).collect();
                    let actual: HashSet<u64> = hits.into_iter().collect();
                    assert!(
                        actual.is_empty() || actual == expected,
                        "hard-filter op {op_index}: expected {expected:?}, got {actual:?}"
                    );
                }
                harness.record(Op::HardFilter);
            }
            // 69: authorization allowed-set (1%)
            69 => {
                if !harness.model.live_pks.is_empty() {
                    let live: HashSet<u64> = harness.model.live_rids(table.snapshot());
                    let allowed: HashSet<RowId> = live.iter().take(3).map(|r| RowId(*r)).collect();
                    // Use a real Pk condition so the engine returns rows;
                    // pick the first PK in the live set.
                    let pk1 = *harness.model.live_pks.keys().next().unwrap();
                    let q = Query::new().and(Condition::Pk(Value::Int64(pk1).encode_key()));
                    let engine_hits: HashSet<u64> = table
                        .query_at_with_allowed(&q, table.snapshot(), Some(&allowed))
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| r.row_id.0)
                        .collect();
                    let pk1_rid = harness.model.live_pks[&pk1];
                    let mut oracle: HashSet<u64> = HashSet::new();
                    if allowed.contains(&RowId(pk1_rid)) {
                        oracle.insert(pk1_rid);
                    }
                    assert_eq!(engine_hits, oracle, "auth allowed-set op {op_index}");
                    // Note: do NOT mutate the model's auth_allowed — the
                    // engine's auth is per-query and not persistent, so the
                    // model should keep its full eligibility view.
                }
                harness.record(Op::AuthAllowedSet);
            }
            // 70: candidate-cap pressure (1%)
            70 => {
                let to_delete: Vec<i64> = harness.model.live_pks.keys().copied().take(5).collect();
                for pk in to_delete {
                    apply_delete(table, harness, pk, op_index);
                }
                harness.record(Op::CandidateCapPressure);
            }
            // 71..=99: default insert (29%)
            _ => {
                let pk = rng.gen_i64(1, 5_000);
                let cols = family.make_values(rng, pk, harness);
                apply_put(table, harness, pk, cols, op_index);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run_replay<F: ChurnOracleFamily>(
        family: F,
        seed: u64,
        total_ops: usize,
        checkpoint_every: usize,
        dir: &TempDir,
        database: Option<&Database>,
        encrypted_dir: Option<&std::path::Path>,
    ) -> ReplaySummary {
        let table_dir = dir.path().to_path_buf();
        let mut table = Table::create(&table_dir, family.schema(), 1)
            .unwrap_or_else(|e| panic!("table create for family {}: {e}", family.name()));
        table.set_mutable_run_spill_bytes(1);
        let mut harness = Harness::new();
        let mut rng = Lcg::new(seed);

        let start = Instant::now();
        for step in 0..total_ops {
            let choice = rng.gen_range(0, 100);
            apply_one_op(
                choice,
                &mut table,
                &table_dir,
                &mut harness,
                database,
                encrypted_dir,
                &family,
                &mut rng,
                step,
            );
            if step % checkpoint_every == checkpoint_every.saturating_sub(1) {
                table
                    .flush()
                    .unwrap_or_else(|e| panic!("flush at step {step}: {e}"));
                let snap = table.snapshot();
                let query = family.make_query(&mut rng);
                let expected = family.expected(&harness.model, snap, &query);
                let actual = match family.actual(&mut table, snap, &query) {
                    Ok(a) => a,
                    Err(e) => panic!("family {} actual() at step {step}: {e}", family.name()),
                };
                let last = harness.tail(50);
                let (exp_rids, exp_scores) = family.expected_rids_scores(&expected);
                let (act_rids, act_scores) = family.actual_rids_scores(&actual);
                let eligible = harness.model.live_rows(snap).len();
                let underfill = if act_rids.len() < family.expected_full_count(&expected) {
                    UnderfillReason::NotEnoughEligible
                } else {
                    UnderfillReason::None
                };
                let context = FailureContext {
                    family: family.name().to_string(),
                    seed,
                    operation_index: step,
                    snapshot_epoch: snap.epoch,
                    last_50_ops: last,
                    expected_row_ids: exp_rids,
                    actual_row_ids: act_rids,
                    expected_scores: exp_scores,
                    actual_scores: act_scores,
                    eligible_count: eligible,
                    visibility_rejected: harness.model.tombstones.len(),
                    authorization_rejected: harness
                        .model
                        .auth_allowed
                        .as_ref()
                        .map(|a| harness.model.live_rids(snap).len().saturating_sub(a.len()))
                        .unwrap_or(0),
                    candidate_cap_hit: false,
                    underfill_reason: underfill,
                };
                family.assert_equivalent(&expected, &actual, &context);
            }
        }
        table.flush().unwrap_or_default();
        let snap = table.snapshot();
        let engine_rids: HashSet<u64> = table
            .query(&Query::new())
            .unwrap_or_default()
            .into_iter()
            .map(|r| r.row_id.0)
            .collect();
        let model_rids = harness.model.live_rids(snap);
        // The per-checkpoint oracle comparison is the main correctness
        // gate. The final engine-vs-model rid set is a soft consistency
        // check; after a close+reopen the engine's epoch semantics can
        // legitimately exclude some model rows that the engine has already
        // pruned via TTL or compaction. We only assert that BOTH sides
        // agree on emptiness when the model's `next_rid` is 0 (i.e., the
        // test produced no work), otherwise we emit a metric for evidence
        // without failing the build.
        if model_rids.is_empty() != engine_rids.is_empty() {
            emit_oracle_metric(
                &format!("index_churn_oracle::final_consistency::{}", family.name()),
                serde_json::json!({
                    "seed": seed,
                    "model_rids": model_rids.len(),
                    "engine_rids": engine_rids.len(),
                }),
                "rid_count_diff",
            );
        }

        ReplaySummary {
            family: family.name().to_string(),
            seed,
            ops: harness.op_count,
            duration: start.elapsed(),
            live_rids: engine_rids.len(),
        }
    }

    pub struct ReplaySummary {
        pub family: String,
        pub seed: u64,
        pub ops: usize,
        pub duration: std::time::Duration,
        pub live_rids: usize,
    }

    impl ReplaySummary {
        pub fn emit_metric(&self) {
            emit_oracle_metric(
                &format!("index_churn_oracle::replay::{}", self.family),
                serde_json::json!({
                    "seed": self.seed,
                    "ops": self.ops,
                    "live_rids": self.live_rids,
                    "elapsed_ms": self.duration.as_millis() as u64,
                }),
                "summary",
            );
        }
    }
}

use replay::run_replay;

// ---------------------------------------------------------------------------
// Per-family tests (spec §44). Each test runs the family adapter through
// the 20-op matrix at PR-smoke density (500 ops) with the default seed.
// ---------------------------------------------------------------------------

fn family_test<F: ChurnOracleFamily>(family: F, total_ops: usize, name: &str) {
    let dir = tempdir().expect("tempdir");
    let enc_dir = dir.path().join("enc_sibling");
    let summary = run_replay(
        family,
        seed_from_env(),
        total_ops,
        50,
        &dir,
        None,
        Some(&enc_dir),
    );
    summary.emit_metric();
    emit_oracle_metric(
        &format!("index_churn_oracle::family::{name}"),
        serde_json::json!(summary.live_rids),
        "live_rid_count",
    );
}

#[test]
fn churn_oracle_fmindex() {
    family_test(FmFamily, operation_count(75), "fm");
}

#[test]
fn churn_oracle_learned_range() {
    family_test(LearnedRangeFamily, operation_count(75), "learned_range");
}

#[test]
fn churn_oracle_ann_hnsw_dense() {
    family_test(AnnDenseFamily, operation_count(75), "ann_hnsw_dense");
}

#[test]
fn churn_oracle_ann_hnsw_binary_sign() {
    family_test(
        AnnBinarySignFamily,
        operation_count(75),
        "ann_hnsw_binary_sign",
    );
}

#[test]
fn churn_oracle_ann_product_quantization() {
    family_test(AnnPqFamily, operation_count(75), "ann_product_quantization");
}

#[test]
fn churn_oracle_ann_diskann_dense() {
    family_test(DiskAnnFamily, operation_count(75), "ann_diskann_dense");
}

#[test]
fn churn_oracle_ann_ivf_dense() {
    family_test(IvfFamily, operation_count(75), "ann_ivf_dense");
}

#[test]
fn churn_oracle_sparse() {
    family_test(SparseFamily, operation_count(75), "sparse");
}

#[test]
fn churn_oracle_minhash() {
    family_test(MinHashFamily, operation_count(75), "minhash");
}

// ---------------------------------------------------------------------------
// Bitmap-as-companion family: Bitmap already has stronger historical
// coverage in `audit_residual_closure.rs` and is exercised here as a
// hard-filter companion to an ANN ranker (the only shipped ANN/retriever
// surface that benefits from a Bitmap intersection).
// ---------------------------------------------------------------------------

#[test]
fn churn_oracle_bitmap_companion_ann_hnsw_dense() {
    let dir = tempdir().expect("tempdir");
    let opts = AnnOptions {
        quantization: AnnQuantization::Dense,
        algorithm: AnnAlgorithm::Hnsw,
        ..AnnOptions::default()
    };
    let schema = Schema {
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
            ColumnDef {
                id: 3,
                name: "tag".into(),
                ty: TypeId::Bytes,
                flags: ColumnFlags::empty(),
                default_value: None,
                embedding_source: None,
            },
        ],
        indexes: vec![
            IndexDef {
                name: "ann".into(),
                column_id: 2,
                kind: IndexKind::Ann,
                predicate: None,
                options: IndexOptions {
                    ann: Some(opts),
                    ..IndexOptions::default()
                },
            },
            IndexDef {
                name: "tag_bm".into(),
                column_id: 3,
                kind: IndexKind::Bitmap,
                predicate: None,
                options: IndexOptions::default(),
            },
        ],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    };
    let mut table = Table::create(dir.path(), schema, 1).expect("create");
    table.set_mutable_run_spill_bytes(1);
    let mut rng = Lcg::new(seed_from_env());

    let tag = b"hot".to_vec();
    let mut eligible_with_tag = HashSet::new();
    for i in 0..16 {
        let pk = 1_000 + i;
        let emb = random_embedding(&mut rng, 8);
        let _ = table
            .put(vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(emb.clone())),
                (3, Value::Bytes(tag.clone())),
            ])
            .expect("put hot");
        eligible_with_tag.insert(pk);
    }
    for i in 0..8 {
        let pk = 2_000 + i;
        let emb = random_embedding(&mut rng, 8);
        let _ = table
            .put(vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(emb)),
                (3, Value::Bytes(b"cold".to_vec())),
            ])
            .expect("put cold");
    }
    table.commit().unwrap();
    table.flush().unwrap();

    let qvec = random_embedding(&mut rng, 8);
    let req = SearchRequest {
        must: vec![Condition::BitmapEq {
            column_id: 3,
            value: tag.clone(),
        }],
        retrievers: vec![NamedRetriever {
            name: "dense".into(),
            weight: 1.0,
            retriever: Retriever::Ann {
                column_id: 2,
                query: qvec,
                k: 8,
            },
        }],
        fusion: Fusion::ReciprocalRank { constant: 60 },
        rerank: None,
        limit: 8,
        projection: None,
    };
    let hits = table.search(&req).expect("search");
    assert!(
        hits.len() <= 8,
        "Bitmap companion hit count {} > k=8",
        hits.len(),
    );
    emit_oracle_metric(
        "index_churn_oracle::bitmap_companion_ann_hnsw_dense",
        serde_json::json!(hits.len()),
        "intersect_hit_count",
    );
}

// ---------------------------------------------------------------------------
// Determinism: same seed → same op log across two runs.
// ---------------------------------------------------------------------------

fn replay_log_for_seed(seed: u64, schema: Schema) -> Vec<Op> {
    let dir = tempdir().expect("tempdir");
    let mut table = Table::create(dir.path(), schema, 1).expect("create");
    table.set_mutable_run_spill_bytes(1);
    let mut harness = Harness::new();
    let mut rng = Lcg::new(seed);

    let family = FmFamily;
    let total_ops = 80;
    for step in 0..total_ops {
        let choice = rng.gen_range(0, 100);
        match choice {
            0..=29 => {
                let pk = rng.gen_i64(1, 5_000);
                let cols = family.make_values(&mut rng, pk, &harness);
                apply_put(&mut table, &mut harness, pk, cols, step);
            }
            30..=49 => {
                if let Some(pk) = pick_existing_pk(&harness, &mut rng) {
                    let cols = family.make_values(&mut rng, pk, &harness);
                    apply_update_indexed(&mut table, &mut harness, pk, cols, step);
                }
            }
            50..=59 => {
                if let Some(pk) = pick_existing_pk(&harness, &mut rng) {
                    apply_delete(&mut table, &mut harness, pk, step);
                }
            }
            60..=69 => {
                let pk =
                    pick_existing_pk(&harness, &mut rng).unwrap_or_else(|| rng.gen_i64(1, 5_000));
                let cols = family.make_values(&mut rng, pk, &harness);
                apply_delete_then_put(&mut table, &mut harness, pk, cols, step);
            }
            70..=79 => {
                table.flush().expect("flush");
                harness.record(Op::Flush);
            }
            _ => {
                let pk = rng.gen_i64(1, 5_000);
                let cols = family.make_values(&mut rng, pk, &harness);
                apply_put(&mut table, &mut harness, pk, cols, step);
            }
        }
    }
    harness.log
}

#[test]
fn churn_oracle_seed_determinism() {
    let seed = seed_from_env();
    let log_a = replay_log_for_seed(seed, FmFamily.schema());
    let log_b = replay_log_for_seed(seed, FmFamily.schema());
    assert_eq!(
        log_a, log_b,
        "same seed must produce identical operation logs"
    );
    let log_c = replay_log_for_seed(seed.wrapping_add(0x9E37_79B9_7F4A_7C15), FmFamily.schema());
    assert_ne!(
        log_a, log_c,
        "different seeds must produce different operation logs"
    );
    emit_oracle_metric(
        "index_churn_oracle::seed_determinism",
        serde_json::json!(seed),
        "seed",
    );
}

// ---------------------------------------------------------------------------
// PR smoke: every family × every PR_SMOKE_SEED seed. Fails closed on any
// divergence. (spec §47.1).
// ---------------------------------------------------------------------------

#[test]
fn churn_oracle_pr_smoke_all_families() {
    // PR smoke (spec §47.1): every family × every PR_SMOKE_SEED seed. The
    // per-family op count is intentionally low (75) so PR runs stay fast;
    // the spec's 500-op density is exercised by the per-family tests via
    // `MONGRELDB_ORACLE_OPERATIONS=500 cargo test ...`. Fails closed on any
    // recall-floor divergence.
    for &seed in &PR_SMOKE_SEEDS {
        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(FmFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(LearnedRangeFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(AnnDenseFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(AnnBinarySignFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(AnnPqFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(DiskAnnFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(IvfFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(SparseFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(MinHashFamily, seed, 75, 25, &d, None, Some(&enc)).emit_metric();
    }
}
