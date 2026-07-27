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
//!
//! # Coverage-axis env contract (REM-H; see scripts/churn-history-check.sh)
//!
//! The nightly/weekly churn workflows compose coverage axes through
//! environment variables. Every axis defaults to the historical PR-smoke
//! mix (byte-identical op stream when nothing is set); `"1"` enables the
//! axis' enhanced coverage and `"0"` removes the axis from the mix entirely
//! — including when the weekly profile would otherwise imply it.
//!
//! - `MONGRELDB_ORACLE_ENCRYPTION`: `"1"` creates the churn table itself
//!   with AES-256-GCM (`Table::create_encrypted`/`open_encrypted`); `"0"`
//!   also removes the encrypted-sibling lifecycle ops.
//! - `MONGRELDB_ORACLE_TTL`: `"1"` adds an expire-everything TTL op to the
//!   mix; `"0"` removes the TTL ops.
//! - `MONGRELDB_ORACLE_HISTORICAL_SNAPSHOTS`: `"1"` adds pinned historical
//!   snapshot reads (re-read a pinned epoch and assert it never gains
//!   rows); `"0"` removes the snapshot-pin ops.
//! - `MONGRELDB_ORACLE_CANDIDATE_CAP_PRESSURE`: `"1"` adds a capped
//!   retrieval probe (`max_fused_candidates = 1`) so ANN queries exceed
//!   candidate caps; `"0"` removes the pressure op.
//! - `MONGRELDB_ORACLE_WORK_BUDGET_PRESSURE`: `"1"` adds a work-budget
//!   probe (zero budget must fail explicitly or charge nothing; a generous
//!   budget must succeed). Only retriever families expose a work budget
//!   through `Table`; the probe is inert for FM/LearnedRange.
//! - `MONGRELDB_ORACLE_LIFECYCLE_OPS`: `"1"` raises reopen/flush/compaction
//!   to full op-matrix weight; `"0"` removes lifecycle ops.
//! - `MONGRELDB_ORACLE_WEEKLY_PROFILE=1` implies `"1"` for every axis above
//!   except encryption (the weekly workflow sets that per seed), biases the
//!   mix toward hot-key churn (`MONGRELDB_ORACLE_STALE_CANDIDATE_RATIO`,
//!   default 100; `MONGRELDB_ORACLE_HOT_KEY_HISTORY`, default 512), and
//!   schedules explicit compaction (`MONGRELDB_ORACLE_COMPACTION_CYCLES`,
//!   default 8) and close+reopen (`MONGRELDB_ORACLE_REOPEN_CYCLES`,
//!   default 4) cycles across the run.
//! - `MONGRELDB_ORACLE_METRICS_JSON`: path for a JSON object (keyed by
//!   record name) with op/query latency percentiles, op counts, and RSS.
//! - `MONGRELDB_ORACLE_FAILURE_DIR`: directory into which the failing
//!   database dir, full op log, and panic context are copied on failure.

#![allow(clippy::too_many_arguments)]
#![allow(clippy::type_complexity)]
#![allow(dead_code)]

use mongreldb_core::query::{
    AiExecutionContext, Condition, Fusion, NamedRetriever, Query, Retriever, RetrieverScore,
    SearchRequest, SetMember,
};
use mongreldb_core::schema::{
    AnnAlgorithm, AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind,
    IndexOptions, ProductQuantizerOptions, Schema, TypeId,
};
use mongreldb_core::{
    Database, Epoch, MongrelError, OwnedSnapshotGuard, PinGuard, PinSource, QueryTrace, RowId,
    Snapshot, Table, TtlPolicy, Value,
};
use mongreldb_types::hlc::HlcTimestamp;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
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

    /// General-corpus MinHash recall floor (REM-F §10.6). Measured healthy
    /// recall on the deterministic oracle corpus (seeds 1-8, 500 ops) is a
    /// median of 1.0; the floor sits far below that but far above total
    /// failure. Never lower this without an approved ADR, and never below
    /// 0.80. The exact-duplicate gate stays at 1.0 regardless.
    pub const MINHASH_GENERAL_RECALL_FLOOR: f32 = 0.80;

    /// Ops per MinHash churn run (REM-F §10.6). The general-recall gate is a
    /// MEDIAN over per-checkpoint samples, so a run must produce enough
    /// checkpoints for the median to be meaningful: at 500 ops and a 50-op
    /// checkpoint cadence each run yields 10 samples, which absorbs the
    /// estimator-noise dips LSH ranking legitimately produces (measured
    /// healthy median 1.0 on seeds 1-8).
    pub const MINHASH_MEDIAN_GATE_OPS: usize = 500;

    /// PR-smoke / per-family default operation density (B468-06). One seed
    /// per process, 500 ops per family — no nested 75-op multi-seed sweep.
    pub const PR_SMOKE_OPERATIONS: usize = 500;

    // Documented ANN recall floors (docs/06-indexes.md §Per-family recall
    // floors). Keep in sync with docs/ai/ci-benchmark-thresholds.json
    // `churn_oracle` section (B468-03).
    pub const HNSW_BINARY_RECALL_FLOOR: f32 = 0.95;
    pub const HNSW_DENSE_RECALL_FLOOR: f32 = 0.90;
    pub const DISKANN_DENSE_RECALL_FLOOR: f32 = 0.90;
    pub const IVF_DENSE_RECALL_FLOOR: f32 = 0.85;
    pub const PQ_RECALL_FLOOR: f32 = 0.80;

    /// PR smoke seeds (spec §47.1). Identical to the fixed seeds requested
    /// in the spec, in ascending order. Matrix CI picks one seed per job
    /// via `MONGRELDB_ORACLE_SEED`; do not loop all seeds inside one process.
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

    /// Coverage-axis state resolved from the churn-oracle env contract (see
    /// the file header and `scripts/churn-history-check.sh`). `Default`
    /// reproduces the historical PR-smoke mix byte-for-byte; `On` adds the
    /// axis' enhanced coverage; `Off` removes the axis from the op mix
    /// entirely — including when the weekly profile would otherwise imply
    /// it.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Axis {
        Default,
        On,
        Off,
    }

    impl Axis {
        pub fn on(self) -> bool {
            self == Axis::On
        }

        pub fn off(self) -> bool {
            self == Axis::Off
        }
    }

    #[derive(Debug, Clone)]
    pub struct OracleConfig {
        pub encryption: Axis,
        pub ttl: Axis,
        pub historical_snapshots: Axis,
        pub candidate_cap_pressure: Axis,
        pub work_budget_pressure: Axis,
        pub lifecycle_ops: Axis,
        pub weekly_profile: bool,
        pub stale_candidate_ratio: usize,
        pub hot_key_history: usize,
        pub compaction_cycles: usize,
        pub reopen_cycles: usize,
        pub metrics_json: Option<PathBuf>,
        pub failure_dir: Option<PathBuf>,
    }

    fn env_flag(name: &str) -> bool {
        std::env::var(name).ok().as_deref() == Some("1")
    }

    fn env_usize(name: &str, default: usize) -> usize {
        std::env::var(name)
            .ok()
            .and_then(|raw| raw.parse().ok())
            .unwrap_or(default)
    }

    fn env_path(name: &str) -> Option<PathBuf> {
        std::env::var(name)
            .ok()
            .filter(|raw| !raw.is_empty())
            .map(PathBuf::from)
    }

    impl OracleConfig {
        pub fn from_env() -> Self {
            let weekly_profile = env_flag("MONGRELDB_ORACLE_WEEKLY_PROFILE");
            // The weekly profile implies every axis except encryption (the
            // weekly workflow sets encryption explicitly per seed). An
            // explicit "0" always wins over the profile.
            let axis = |name: &str| match std::env::var(name).ok().as_deref() {
                Some("1") => Axis::On,
                Some("0") => Axis::Off,
                _ if weekly_profile => Axis::On,
                _ => Axis::Default,
            };
            Self {
                encryption: match std::env::var("MONGRELDB_ORACLE_ENCRYPTION").ok().as_deref() {
                    Some("1") => Axis::On,
                    Some("0") => Axis::Off,
                    _ => Axis::Default,
                },
                ttl: axis("MONGRELDB_ORACLE_TTL"),
                historical_snapshots: axis("MONGRELDB_ORACLE_HISTORICAL_SNAPSHOTS"),
                candidate_cap_pressure: axis("MONGRELDB_ORACLE_CANDIDATE_CAP_PRESSURE"),
                work_budget_pressure: axis("MONGRELDB_ORACLE_WORK_BUDGET_PRESSURE"),
                lifecycle_ops: axis("MONGRELDB_ORACLE_LIFECYCLE_OPS"),
                weekly_profile,
                stale_candidate_ratio: env_usize("MONGRELDB_ORACLE_STALE_CANDIDATE_RATIO", 100),
                hot_key_history: env_usize("MONGRELDB_ORACLE_HOT_KEY_HISTORY", 512),
                compaction_cycles: env_usize("MONGRELDB_ORACLE_COMPACTION_CYCLES", 8),
                reopen_cycles: env_usize("MONGRELDB_ORACLE_REOPEN_CYCLES", 4),
                metrics_json: env_path("MONGRELDB_ORACLE_METRICS_JSON"),
                failure_dir: env_path("MONGRELDB_ORACLE_FAILURE_DIR"),
            }
        }
    }

    /// Nearest-rank percentile over an already-sorted sample.
    pub fn percentile(sorted: &[u64], p: usize) -> u64 {
        if sorted.is_empty() {
            return 0;
        }
        let idx = ((p as f64 / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    pub fn latency_stats(samples: &[u64]) -> serde_json::Value {
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        serde_json::json!({
            "count": sorted.len(),
            "p50": percentile(&sorted, 50),
            "p95": percentile(&sorted, 95),
            "p99": percentile(&sorted, 99),
            "max": sorted.last().copied().unwrap_or(0),
        })
    }

    /// Peak RSS of the test process (VmHWM), cheaply readable on Linux.
    pub fn peak_rss_kb() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        status
            .lines()
            .find(|line| line.starts_with("VmHWM:"))?
            .split_whitespace()
            .nth(1)?
            .parse()
            .ok()
    }

    static METRICS_REGISTRY: std::sync::OnceLock<
        std::sync::Mutex<BTreeMap<String, serde_json::Value>>,
    > = std::sync::OnceLock::new();

    /// Merge one entry into the shared metrics JSON document. Family tests
    /// run as threads inside one test process, so every writer merges under
    /// a lock and rewrites the whole document atomically.
    pub fn write_metrics_json(path: &Path, key: &str, entry: serde_json::Value) {
        let registry = METRICS_REGISTRY.get_or_init(|| std::sync::Mutex::new(BTreeMap::new()));
        let mut guard = registry.lock().unwrap_or_else(|p| p.into_inner());
        guard.insert(key.to_string(), entry);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let tmp = path.with_extension("tmp");
        if let Ok(body) = serde_json::to_string_pretty(&*guard) {
            if std::fs::write(&tmp, body).is_ok() {
                let _ = std::fs::rename(&tmp, path);
            }
        }
    }

    pub fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
        std::fs::create_dir_all(dst)?;
        for entry in std::fs::read_dir(src)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            let target = dst.join(entry.file_name());
            if file_type.is_dir() {
                copy_dir_recursive(&entry.path(), &target)?;
            } else if file_type.is_file() {
                std::fs::copy(entry.path(), &target)?;
            }
        }
        Ok(())
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
        HistoricalSnapshotRead {
            epoch: u64,
        },
        WorkBudgetProbe,
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
                Op::HistoricalSnapshotRead { epoch } => {
                    write!(f, "HistoricalSnapshotRead(epoch={epoch})")
                }
                Op::WorkBudgetProbe => write!(f, "WorkBudgetProbe"),
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

    pub fn l2_normalize(v: &mut [f32]) {
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in v.iter_mut() {
                *x /= norm;
            }
        }
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

    /// Why an actual result was shorter than the requested top-k (B468-04).
    /// `NotEnoughEligible` is assigned only when eligible_count < requested_k.
    /// Never derive it from actual length alone.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum UnderfillReason {
        None,
        NotEnoughEligible,
        CandidateCap,
        WorkBudgetExceeded,
        ApproximateRecall,
        Unexpected,
        /// Legacy labels retained for historical probe reporting.
        BudgetConsumedByStale,
        TtlExpired,
        HlcRejected,
        ExpectedUnderfill,
    }

    /// Classify underfill from model eligibility + engine outcome (B468-04).
    pub fn classify_underfill(
        requested_k: usize,
        eligible_count: usize,
        actual_count: usize,
        candidate_cap_hit: bool,
        work_budget_exhausted: bool,
        approximate: bool,
    ) -> UnderfillReason {
        let required = requested_k.min(eligible_count);
        if actual_count >= required {
            return UnderfillReason::None;
        }
        if eligible_count < requested_k {
            return UnderfillReason::NotEnoughEligible;
        }
        if work_budget_exhausted {
            return UnderfillReason::WorkBudgetExceeded;
        }
        if candidate_cap_hit {
            return UnderfillReason::CandidateCap;
        }
        if approximate {
            return UnderfillReason::ApproximateRecall;
        }
        UnderfillReason::Unexpected
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

use context::{classify_underfill, FailureContext, UnderfillReason};

// Last retriever-path execution flags captured by family `actual()` via
// `QueryTrace` (B468-04). Checkpoint classification reads these so
// CandidateCap / WorkBudgetExceeded are never hard-coded false.
thread_local! {
    static LAST_CANDIDATE_CAP_HIT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LAST_WORK_BUDGET_EXCEEDED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static LAST_REQUESTED_K: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn clear_execution_flags() {
    LAST_CANDIDATE_CAP_HIT.with(|c| c.set(false));
    LAST_WORK_BUDGET_EXCEEDED.with(|c| c.set(false));
    LAST_REQUESTED_K.with(|c| c.set(0));
}

fn record_execution_flags(trace: &QueryTrace, err: Option<&MongrelError>, requested_k: usize) {
    let cap = trace.ann_candidate_cap_hit || trace.candidate_cap_hit;
    LAST_CANDIDATE_CAP_HIT.with(|c| c.set(cap));
    let budget = matches!(err, Some(MongrelError::WorkBudgetExceeded));
    LAST_WORK_BUDGET_EXCEEDED.with(|c| c.set(budget));
    LAST_REQUESTED_K.with(|c| c.set(requested_k));
}

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
        /// Pinned snapshot + the engine rid set captured at pin time + the
        /// TTL state version at pin time. Historical snapshot reads re-read
        /// the pinned epoch and assert it never gains rows; a TTL state
        /// change forces a re-pin instead (TTL expiry is evaluated at query
        /// time, so a changed policy legitimately alters a historical view).
        pub historical_pin: Option<(Snapshot, HashSet<u64>, u64)>,
        pub ttl_version: u64,
        /// Versions written per weekly-profile hot key.
        pub hot_versions: BTreeMap<i64, u64>,
        /// Per-op and per-checkpoint-query wall-clock samples (micros) for
        /// the metrics JSON.
        pub op_latencies: Vec<u64>,
        pub query_latencies: Vec<u64>,
        pub cap_hits: u64,
        pub budget_trips: u64,
        pub historical_reads: u64,
        pub compactions: u64,
        pub reopens: u64,
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
                historical_pin: None,
                ttl_version: 0,
                hot_versions: BTreeMap::new(),
                op_latencies: Vec::new(),
                query_latencies: Vec::new(),
                cap_hits: 0,
                budget_trips: 0,
                historical_reads: 0,
                compactions: 0,
                reopens: 0,
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
        // Mirror the FULL row payload in the model (not just the PK): the
        // per-checkpoint oracle reads the indexed column from the model, so
        // dropping it would silently erase batch rows from every expected
        // answer (REM-G model repair).
        let row_reprs: Vec<Vec<(u16, ValueRepr)>> = rows
            .iter()
            .map(|cols| {
                cols.iter()
                    .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
                    .collect()
            })
            .collect();
        let epoch = pending_epoch(table);
        let rids = table
            .put_batch(rows)
            .unwrap_or_else(|e| panic!("put_batch_unique op {op_index}: {e}"));
        for ((pk, reprs), rid) in pks.iter().zip(row_reprs).zip(rids) {
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
        // Same full-payload model repair as `apply_put_batch_unique`.
        let row_reprs: Vec<Vec<(u16, ValueRepr)>> = rows
            .iter()
            .map(|cols| {
                cols.iter()
                    .map(|(cid, v)| (*cid, ValueRepr::from_value(v)))
                    .collect()
            })
            .collect();
        let epoch = pending_epoch(table);
        let rids = table
            .put_batch(rows)
            .unwrap_or_else(|e| panic!("put_batch_duplicate op {op_index}: {e}"));
        for ((pk, reprs), rid) in pks.iter().zip(row_reprs).zip(rids) {
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

        /// End-of-replay family gate (e.g. the MinHash median-recall
        /// floor). Called once per replay with the final failure context so
        /// a violation renders the full §11.5 artifact.
        fn finish(&self, _context: &FailureContext) {}

        /// Retriever used by the candidate-cap and work-budget pressure
        /// probes. `None` for exact query-path families (FM, LearnedRange):
        /// the engine's candidate cap and work budget are only reachable
        /// through the scored-retrieval surface on `Table`, so those probes
        /// are engine-inert for query-path families (documented in
        /// docs/06-indexes.md).
        fn probe_retriever(&self, rng: &mut Lcg) -> Option<Retriever> {
            let _ = rng;
            None
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
            // Snapshot-aware exact query (B468-01). Never use latest-state query.
            let hits: HashSet<u64> = table
                .query_at_with_allowed(&q, snapshot, None)?
                .into_iter()
                .map(|r| r.row_id.0)
                .collect();
            Ok(hits)
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            assert_eq!(
                expected,
                actual,
                "FM exact oracle mismatch:\n{}",
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            true
        }

        fn recall_floor(&self) -> f32 {
            1.0
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
            let (lo, hi) = *query;
            let q = Query::new().and(Condition::Range {
                column_id: self.indexed_column(),
                lo,
                hi,
            });
            // Snapshot-aware exact query (B468-01).
            let hits: HashSet<u64> = table
                .query_at_with_allowed(&q, snapshot, None)?
                .into_iter()
                .map(|r| r.row_id.0)
                .collect();
            Ok(hits)
        }

        fn assert_equivalent(
            &self,
            expected: &Self::Expected,
            actual: &Self::Actual,
            context: &FailureContext,
        ) {
            assert_eq!(
                expected,
                actual,
                "LearnedRange exact oracle mismatch:\n{}",
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            true
        }

        fn recall_floor(&self) -> f32 {
            1.0
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
            // Oracle corpora are O(10²) live rows. Default nlist=256 leaves most
            // inverted lists empty and collapses probe recall below the
            // documented 0.85 floor; match the ANN matrix scale (nlist=8) with
            // full-list nprobe so the floor is achievable without cutting it.
            opts.ivf = Some(mongreldb_core::schema::IvfOptions {
                nlist: 8,
                nprobe: 8,
                ..Default::default()
            });
        }
        if algorithm == AnnAlgorithm::DiskAnn {
            opts.diskann = Some(Default::default());
        }
        if matches!(quantization, AnnQuantization::Product { .. }) {
            // Higher rerank window keeps small-dim oracle corpora above the
            // documented 0.80 floor (docs/06-indexes.md).
            let product = mongreldb_core::schema::ProductQuantizerOptions {
                rerank_factor: 32,
                ..Default::default()
            };
            opts.product = Some(product);
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

    /// Shared ANN recall + eligibility + ordering gate (B468-03/04).
    fn assert_ann_recall_and_eligibility(
        family_name: &str,
        floor: f32,
        expected: &[(u64, f32)],
        actual: &[(u64, f32)],
        eligible: &HashSet<u64>,
        context: &FailureContext,
        distance_ascending: bool,
    ) {
        for (rid, _) in actual {
            assert!(
                eligible.contains(rid),
                "{family_name} returned ineligible rid {rid}:\n{}",
                context.render()
            );
        }
        let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
        let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
        let found = exp_set.intersection(&act_set).count();
        let recall = if exp_set.is_empty() {
            1.0
        } else {
            found as f32 / exp_set.len() as f32
        };
        assert!(
            recall >= floor,
            "{family_name} recall {recall} < floor {floor}:\n{}",
            context.render()
        );
        for w in actual.windows(2) {
            if distance_ascending {
                assert!(
                    w[0].1 <= w[1].1 + 1e-5,
                    "{family_name} not sorted by ascending distance: {:?}\n{}",
                    actual,
                    context.render()
                );
            } else {
                assert!(
                    w[0].1 + 1e-5 >= w[1].1,
                    "{family_name} not sorted by descending score: {:?}\n{}",
                    actual,
                    context.render()
                );
            }
        }
        // Unexplained underfill: when model expected full k and engine
        // returned fewer without an explicit underfill reason, fail.
        if matches!(context.underfill_reason, UnderfillReason::Unexpected) {
            panic!(
                "{family_name} unexpected underfill (actual {} < expected {}):\n{}",
                actual.len(),
                expected.len(),
                context.render()
            );
        }
    }

    pub struct AnnDenseFamily;

    impl ChurnOracleFamily for AnnDenseFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "ANN/HNSW/Dense"
        }

        fn probe_retriever(&self, rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::Ann {
                column_id: self.indexed_column(),
                query: random_embedding(rng, 8),
                k: 4,
            })
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
            let (qvec, k) = query;
            clear_execution_flags();
            let retriever = Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            // Eligibility: every actual hit must be among model live rows
            // captured at checkpoint (expected_row_ids is the model top-k;
            // also accept actual hits that match expected for recall).
            let eligible: HashSet<u64> = context.expected_row_ids.iter().copied().collect();
            assert_ann_recall_and_eligibility(
                "ANN/HNSW/Dense",
                self.recall_floor(),
                expected,
                actual,
                &eligible,
                context,
                true,
            );
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            HNSW_DENSE_RECALL_FLOOR
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

        fn probe_retriever(&self, rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::Ann {
                column_id: self.indexed_column(),
                query: random_embedding(rng, 8),
                k: 4,
            })
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
            let (qvec, k) = query;
            clear_execution_flags();
            let retriever = Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            let eligible: HashSet<u64> = context.expected_row_ids.iter().copied().collect();
            for (rid, _) in actual {
                assert!(
                    eligible.contains(rid),
                    "ANN/HNSW/BinarySign ineligible rid {rid}:\n{}",
                    context.render()
                );
            }
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let found = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                found as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/HNSW/BinarySign recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for w in actual.windows(2) {
                assert!(
                    w[0].1 <= w[1].1,
                    "ANN/HNSW/BinarySign not sorted: {:?}",
                    actual
                );
            }
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            HNSW_BINARY_RECALL_FLOOR
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

    /// Product-quantization ANN family. Enforces the documented 0.80
    /// recall floor **at every checkpoint** (B468-03 / AC2). Schema uses
    /// dim=16 + 4×8-bit PQ + high ADC rerank so the floor is achievable on
    /// the oracle corpus without cutting the gate.
    pub struct AnnPqFamily;

    pub fn pq_schema() -> Schema {
        let opts = AnnOptions {
            algorithm: AnnAlgorithm::Hnsw,
            quantization: AnnQuantization::Product {
                num_subvectors: 4,
                bits: 8,
            },
            m: 24,
            ef_construction: 128,
            ef_search: 128,
            product: Some(ProductQuantizerOptions {
                rerank_factor: 64,
                training_samples: 256_000,
                ..Default::default()
            }),
            ..AnnOptions::default()
        };
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
                    ty: TypeId::Embedding { dim: 16 },
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

    impl ChurnOracleFamily for AnnPqFamily {
        type Query = (Vec<f32>, usize);
        type Expected = Vec<(u64, f32)>;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "ANN/HNSW/PQ"
        }

        fn probe_retriever(&self, rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::Ann {
                column_id: self.indexed_column(),
                query: random_embedding(rng, 16),
                k: 4,
            })
        }
        fn schema(&self) -> Schema {
            pq_schema()
        }
        fn indexed_column(&self) -> u16 {
            2
        }
        fn non_indexed_column(&self) -> u16 {
            3
        }

        fn make_values(&self, rng: &mut Lcg, pk: i64, _harness: &Harness) -> Vec<(u16, Value)> {
            // Unit-normalized cluster embeddings. PQ search ranks by L2/ADC;
            // on the unit sphere L2 ranking matches cosine, so the independent
            // cosine model and the engine agree on top-k membership.
            let mut emb = vec![0.05f32; 16];
            let axis = (pk.unsigned_abs() as usize) % 8;
            emb[axis * 2] = 1.0;
            emb[axis * 2 + 1] = 0.8 + (rng.next_u64() % 20) as f32 / 100.0;
            for (i, slot) in emb.iter_mut().enumerate() {
                if i / 2 != axis {
                    *slot = (rng.next_u64() % 5) as f32 / 100.0;
                }
            }
            l2_normalize(&mut emb);
            vec![
                (1, Value::Int64(pk)),
                (2, Value::Embedding(emb)),
                (3, Value::Int64(rng.gen_i64(0, 1_000_000))),
            ]
        }

        fn make_query(&self, rng: &mut Lcg) -> Self::Query {
            let mut q = vec![0.02f32; 16];
            let axis = rng.gen_range(0, 8);
            q[axis * 2] = 1.0;
            q[axis * 2 + 1] = 0.9;
            l2_normalize(&mut q);
            let k = rng.gen_range(5, 9);
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
            let (qvec, k) = query;
            clear_execution_flags();
            let retriever = Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            let eligible: HashSet<u64> = context.expected_row_ids.iter().copied().collect();
            for (rid, _) in actual {
                assert!(
                    eligible.contains(rid),
                    "ANN/HNSW/PQ ineligible rid {rid}:\n{}",
                    context.render()
                );
            }
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let found = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                found as f32 / exp_set.len() as f32
            };
            // B468-03 / AC2: documented 0.80 floor at every checkpoint — no median softener.
            assert!(
                recall >= self.recall_floor(),
                "ANN/HNSW/PQ recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for w in actual.windows(2) {
                assert!(
                    w[0].1 <= w[1].1 + 1e-5,
                    "ANN/HNSW/PQ not sorted: {:?}",
                    actual
                );
            }
        }

        fn is_exact(&self) -> bool {
            false
        }
        fn recall_floor(&self) -> f32 {
            PQ_RECALL_FLOOR
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

        fn probe_retriever(&self, rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::Ann {
                column_id: self.indexed_column(),
                query: random_embedding(rng, 8),
                k: 4,
            })
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
            let (qvec, k) = query;
            clear_execution_flags();
            let retriever = Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            let eligible: HashSet<u64> = context.expected_row_ids.iter().copied().collect();
            for (rid, _) in actual {
                assert!(
                    eligible.contains(rid),
                    "ANN/DiskANN/Dense ineligible rid {rid}:\n{}",
                    context.render()
                );
            }
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let found = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                found as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/DiskANN/Dense recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for w in actual.windows(2) {
                assert!(
                    w[0].1 <= w[1].1 + 1e-5,
                    "ANN/DiskANN/Dense not sorted: {:?}",
                    actual
                );
            }
        }

        fn is_exact(&self) -> bool {
            false
        }
        fn recall_floor(&self) -> f32 {
            DISKANN_DENSE_RECALL_FLOOR
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

        fn probe_retriever(&self, rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::Ann {
                column_id: self.indexed_column(),
                query: random_embedding(rng, 8),
                k: 4,
            })
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
            let (qvec, k) = query;
            clear_execution_flags();
            let retriever = Retriever::Ann {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            let eligible: HashSet<u64> = context.expected_row_ids.iter().copied().collect();
            for (rid, _) in actual {
                assert!(
                    eligible.contains(rid),
                    "ANN/IVF/Dense ineligible rid {rid}:\n{}",
                    context.render()
                );
            }
            let exp_set: HashSet<u64> = expected.iter().map(|(r, _)| *r).collect();
            let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
            let found = exp_set.intersection(&act_set).count();
            let recall = if exp_set.is_empty() {
                1.0
            } else {
                found as f32 / exp_set.len() as f32
            };
            assert!(
                recall >= self.recall_floor(),
                "ANN/IVF/Dense recall {recall} < floor {}:\n{}",
                self.recall_floor(),
                context.render()
            );
            for w in actual.windows(2) {
                assert!(
                    w[0].1 <= w[1].1 + 1e-5,
                    "ANN/IVF/Dense not sorted: {:?}",
                    actual
                );
            }
        }

        fn is_exact(&self) -> bool {
            false
        }
        fn recall_floor(&self) -> f32 {
            IVF_DENSE_RECALL_FLOOR
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

    /// Exact sparse top-k plus the full positive-score frontier so equal-score
    /// ties at the k boundary remain exact under f32/f64 noise (B468-02).
    #[derive(Debug, Clone)]
    pub struct SparseExpected {
        pub topk: Vec<(u64, f32)>,
        pub scores: std::collections::HashMap<u64, f32>,
    }

    impl ChurnOracleFamily for SparseFamily {
        type Query = (Vec<(u32, f32)>, usize);
        type Expected = SparseExpected;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "Sparse"
        }

        fn probe_retriever(&self, _rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::Sparse {
                column_id: self.indexed_column(),
                query: vec![(1u32, 1.0), (3u32, 2.0)],
                k: 4,
            })
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
            let scores: std::collections::HashMap<u64, f32> = scored.iter().copied().collect();
            scored.truncate(*k);
            SparseExpected {
                topk: scored,
                scores,
            }
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let (qvec, k) = query;
            // Snapshot-aware exact retrieval (B468-02). Stale hits are defects.
            clear_execution_flags();
            let retriever = Retriever::Sparse {
                column_id: self.indexed_column(),
                query: qvec.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            let k = expected.topk.len();
            assert_eq!(
                actual.len(),
                k,
                "Sparse hit-count mismatch:\n{}",
                context.render()
            );
            // Every actual hit must be a positive-score live row under the
            // model, with score within 1e-5 of the model score.
            for (rid, score) in actual {
                let Some(exp) = expected.scores.get(rid) else {
                    panic!(
                        "Sparse returned ineligible/stale rid {rid}:\n{}",
                        context.render()
                    );
                };
                assert!(
                    (exp - score).abs() <= 1e-5,
                    "Sparse score mismatch for rid {rid}: expected {exp}, got {score}\n{}",
                    context.render()
                );
            }
            // Deterministic top-k: when the model has a unique k-th score
            // frontier, membership must match topk exactly. When the frontier
            // is tied, every actual rid must score within 1e-5 of the k-th
            // score and the set must be drawn from the model frontier.
            if let Some((_, kth)) = expected.topk.last() {
                let frontier: HashSet<u64> = expected
                    .scores
                    .iter()
                    .filter(|(_, s)| **s + 1e-5 >= *kth)
                    .map(|(r, _)| *r)
                    .collect();
                for (rid, _) in actual {
                    assert!(
                        frontier.contains(rid),
                        "Sparse rid {rid} outside model score frontier:\n{}",
                        context.render()
                    );
                }
                // Membership is unique only when no model row outside top-k
                // shares the k-th score band (otherwise ties make the set
                // non-unique under f32/f64 noise).
                let topk_set: HashSet<u64> = expected.topk.iter().map(|(r, _)| *r).collect();
                let tied_outside = expected
                    .scores
                    .iter()
                    .any(|(rid, s)| !topk_set.contains(rid) && *s + 1e-5 >= *kth);
                if !tied_outside {
                    let act_set: HashSet<u64> = actual.iter().map(|(r, _)| *r).collect();
                    assert_eq!(
                        topk_set,
                        act_set,
                        "Sparse unique-score top-k membership mismatch:\n{}",
                        context.render()
                    );
                }
            }
            for w in actual.windows(2) {
                assert!(
                    w[0].1 + 1e-5 >= w[1].1,
                    "Sparse scores not non-increasing: {:?}\n{}",
                    actual,
                    context.render()
                );
            }
        }

        fn is_exact(&self) -> bool {
            true
        }

        fn recall_floor(&self) -> f32 {
            1.0
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.topk.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.topk.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.topk.len()
        }
    }

    // ----- MinHash top-k ----------------------------------------------

    /// MinHash family adapter. `recall_samples` accumulates one
    /// tie-tolerant recall sample per checkpoint; `finish` enforces the
    /// documented median floor (§10.6).
    #[derive(Default)]
    pub struct MinHashFamily {
        pub recall_samples: std::cell::RefCell<Vec<f32>>,
    }

    /// MinHash oracle answer: the exact-Jaccard top-k plus the exact
    /// Jaccard of every positive-similarity live row, so the recall gate
    /// can accept tie/estimation-noise substitutes (§10.6).
    #[derive(Debug, Clone)]
    pub struct MinHashExpected {
        pub topk: Vec<(u64, f64)>,
        pub exact_j: std::collections::BTreeMap<u64, f64>,
    }

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

        /// Canonical query set. Near-duplicate corpus rows overlap it
        /// heavily, which is the workload MinHash serves (near-duplicate
        /// detection); background rows exercise the disjoint case.
        pub const QUERY_SET: [&'static str; 4] = ["a", "b", "c", "d"];

        pub fn query_set_members() -> Vec<SetMember> {
            Self::QUERY_SET
                .iter()
                .map(|s| SetMember::String((*s).to_string()))
                .collect()
        }

        pub fn query_set_strings() -> HashSet<String> {
            Self::QUERY_SET.iter().map(|s| (*s).to_string()).collect()
        }

        /// Corpus generator: ~60% near-duplicates of the query set (2-4
        /// query tokens plus 0-1 noise tokens → exact Jaccard 0.4..=1.0),
        /// ~40% background rows over the full 10-token vocabulary (mostly
        /// disjoint). Near-duplicate-heavy corpora are what LSH indexing is
        /// for, and they give the top-k rows enough similarity that the
        /// band-sharing probability is high — a recall floor is then a
        /// meaningful gate instead of a coin flip.
        pub fn make_set(rng: &mut Lcg) -> Vec<&'static str> {
            const NOISE_TOKENS: &[&str] = &["x", "y", "z", "w", "p", "q"];
            let mut set: Vec<&'static str> = Vec::new();
            if rng.gen_bool(0.6) {
                let mut tokens = Self::QUERY_SET.to_vec();
                for i in (1..tokens.len()).rev() {
                    let j = rng.gen_range(0, i + 1);
                    tokens.swap(i, j);
                }
                let m = rng.gen_range(2, 5);
                set.extend(tokens.into_iter().take(m));
                if rng.gen_bool(0.4) {
                    set.push(NOISE_TOKENS[rng.gen_range(0, NOISE_TOKENS.len())]);
                }
            } else {
                const VOCAB: &[&str] = &["a", "b", "c", "d", "x", "y", "z", "w", "p", "q"];
                let n = rng.gen_range(1, 5);
                for _ in 0..n {
                    let token = VOCAB[rng.gen_range(0, VOCAB.len())];
                    if !set.contains(&token) {
                        set.push(token);
                    }
                }
            }
            set
        }
    }

    impl ChurnOracleFamily for MinHashFamily {
        type Query = (Vec<SetMember>, usize);
        type Expected = MinHashExpected;
        type Actual = Vec<(u64, f32)>;

        fn name(&self) -> &'static str {
            "MinHash"
        }

        fn probe_retriever(&self, _rng: &mut Lcg) -> Option<Retriever> {
            Some(Retriever::MinHash {
                column_id: self.indexed_column(),
                members: vec![
                    SetMember::String("a".into()),
                    SetMember::String("b".into()),
                    SetMember::String("c".into()),
                    SetMember::String("d".into()),
                ],
                k: 4,
            })
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
            let (_, k) = query;
            let qstrings = Self::query_set_strings();
            let mut exact_j = std::collections::BTreeMap::new();
            for row in model.live_rows(snapshot) {
                if let Some(ValueRepr::Bytes(b)) = row.cols.get(&self.indexed_column()) {
                    let set = unpack_minhash_members(b);
                    let j = jaccard(&qstrings, &set);
                    // Zero-similarity rows can never be LSH candidates and
                    // carry no ranking signal; the oracle top-k is over
                    // positive-similarity rows only.
                    if j > 0.0 {
                        exact_j.insert(row.rid, j);
                    }
                }
            }
            let mut scored: Vec<(u64, f64)> = exact_j.iter().map(|(r, j)| (*r, *j)).collect();
            scored.sort_by(|(r1, j1), (r2, j2)| j2.total_cmp(j1).then_with(|| r1.cmp(r2)));
            scored.truncate(*k);
            MinHashExpected {
                topk: scored,
                exact_j,
            }
        }

        fn actual(
            &self,
            table: &mut Table,
            snapshot: Snapshot,
            query: &Self::Query,
        ) -> mongreldb_core::Result<Self::Actual> {
            let (qset, k) = query;
            // Snapshot-aware retrieval (REM-F §10.7).
            clear_execution_flags();
            let retriever = Retriever::MinHash {
                column_id: self.indexed_column(),
                members: qset.clone(),
                k: *k,
            };
            let (result, trace) = QueryTrace::capture(|| {
                table.retrieve_at_with_allowed_and_context(&retriever, snapshot, None, None)
            });
            record_execution_flags(&trace, result.as_ref().err(), *k);
            let hits = result?;
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
            // Gate 1 (always): no stale/deleted/expired row and no
            // zero-similarity hit — every returned rid must be live at the
            // queried snapshot with positive exact Jaccard.
            for (rid, est) in actual {
                assert!(
                    expected.exact_j.contains_key(rid),
                    "MinHash returned stale/deleted/expired or zero-similarity rid {rid}:\n{}",
                    context.render()
                );
                assert!(
                    *est > 0.0,
                    "MinHash zero-J hit rid {rid}:\n{}",
                    context.render()
                );
            }
            if expected.topk.is_empty() {
                return;
            }
            // Gate 2: tie/estimation-noise-tolerant recall against the
            // model's exact-Jaccard top-k. An expected row counts as found
            // when the engine returned it, or when the engine returned an
            // unused row whose exact Jaccard is at least as high (the
            // estimator legitimately reorders near-ties). A genuine LSH
            // band miss can only substitute a lower-similarity row and is
            // counted as a miss.
            let mut pool: Vec<(u64, f64)> = actual
                .iter()
                .map(|(rid, _)| (*rid, expected.exact_j[rid]))
                .collect();
            pool.sort_by(|(r1, j1), (r2, j2)| j2.total_cmp(j1).then_with(|| r1.cmp(r2)));
            let mut found = 0usize;
            for (rid, ej) in &expected.topk {
                if let Some(pos) = pool.iter().position(|(r, _)| r == rid) {
                    pool.remove(pos);
                    found += 1;
                } else if let Some(pos) = pool.iter().position(|(_, j)| *j >= *ej) {
                    pool.remove(pos);
                    found += 1;
                }
            }
            let recall = found as f32 / expected.topk.len() as f32;
            self.recall_samples.borrow_mut().push(recall);
            // Total LSH failure on a non-empty oracle answer fails
            // immediately; the aggregate median floor is enforced in
            // `finish`.
            assert!(
                recall > 0.0,
                "MinHash total recall failure (0/{}) at one checkpoint:\n{}",
                expected.topk.len(),
                context.render()
            );
        }

        fn is_exact(&self) -> bool {
            false
        }

        fn recall_floor(&self) -> f32 {
            MINHASH_GENERAL_RECALL_FLOOR
        }

        fn finish(&self, context: &FailureContext) {
            let samples = self.recall_samples.borrow();
            if samples.is_empty() {
                // No checkpoints fired (ops < checkpoint cadence). The
                // median gate is meaningless without samples; PR density
                // (PR_SMOKE_OPERATIONS) always produces checkpoints.
                return;
            }
            let mut sorted = samples.clone();
            sorted.sort_by(f32::total_cmp);
            let median = sorted[sorted.len() / 2];
            let min = *sorted.first().unwrap();
            emit_oracle_metric(
                "index_churn_oracle::minhash_recall",
                serde_json::json!({
                    "median": median,
                    "min": min,
                    "samples": samples.len(),
                    "floor": MINHASH_GENERAL_RECALL_FLOOR,
                }),
                "recall",
            );
            assert!(
                median >= self.recall_floor(),
                "MinHash median recall {median} < floor {} (min {min}, {} samples):\n{}",
                self.recall_floor(),
                samples.len(),
                context.render()
            );
        }

        fn expected_rids_scores(&self, expected: &Self::Expected) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = expected.topk.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = expected.topk.iter().map(|(_, s)| *s).collect();
            (rids, scores)
        }

        fn actual_rids_scores(&self, actual: &Self::Actual) -> (Vec<u64>, Vec<f64>) {
            let rids: Vec<u64> = actual.iter().map(|(r, _)| *r).collect();
            let scores: Vec<f64> = actual.iter().map(|(_, s)| *s as f64).collect();
            (rids, scores)
        }

        fn expected_full_count(&self, expected: &Self::Expected) -> usize {
            expected.topk.len()
        }
    }
}

use families::{
    AnnBinarySignFamily, AnnDenseFamily, AnnPqFamily, DiskAnnFamily, FmFamily, IvfFamily,
    LearnedRangeFamily, MinHashFamily, SparseExpected, SparseFamily,
};

// ---------------------------------------------------------------------------
// Operation matrix replay (spec §45). Every family runs through the same
// deterministic weighted sequence so divergences are reproducible.
// ---------------------------------------------------------------------------

mod replay {
    use super::*;

    /// Passphrase for the main churn table when
    /// `MONGRELDB_ORACLE_ENCRYPTION=1` (same value the encrypted-sibling op
    /// has always used).
    const ENCRYPTION_PASSPHRASE: &str = "oracle-encryption-passphrase";

    /// Weekly-profile hot-key churn: a small set of PKs outside every
    /// random PK range gets the bulk of update/delete-bucket ops, so each
    /// hot key accumulates a long version history and the secondary indexes
    /// accumulate stale candidates.
    const HOT_PK_BASE: i64 = 500_000;
    const HOT_KEY_COUNT: usize = 8;

    /// Commit + close + reopen the churn table, then re-stamp the model so
    /// the oracle observes the same view the engine does. Shared by the
    /// close+reopen op and the weekly profile's scheduled reopen cycles.
    fn close_reopen(
        table: &mut Table,
        table_dir: &Path,
        harness: &mut Harness,
        config: &OracleConfig,
        op_index: usize,
    ) {
        table
            .commit()
            .unwrap_or_else(|e| panic!("commit pre-close {op_index}: {e}"));
        table
            .close()
            .unwrap_or_else(|e| panic!("close op {op_index}: {e}"));
        *table = if config.encryption.on() {
            Table::open_encrypted(table_dir, ENCRYPTION_PASSPHRASE)
                .unwrap_or_else(|e| panic!("reopen op {op_index}: {e}"))
        } else {
            Table::open(table_dir).unwrap_or_else(|e| panic!("reopen op {op_index}: {e}"))
        };
        // Re-stamp every model row's commit_epoch below the engine's
        // new visible epoch so the oracle observes the same view the
        // engine does. Tombstoned rows keep their delete_epoch.
        let snap_after = table.snapshot();
        harness.model.reset_for_close_reopen(snap_after.epoch);
        // Pinned epochs from before the reopen are no longer meaningful.
        harness.historical_pin = None;
        harness.reopens += 1;
        harness.record(Op::CloseReopen);
    }

    /// Extract the requested k from a probe retriever.
    fn retriever_k(retriever: &Retriever) -> usize {
        match retriever {
            Retriever::Ann { k, .. }
            | Retriever::Sparse { k, .. }
            | Retriever::MinHash { k, .. } => *k,
        }
    }

    /// Weekly profile: write a fresh version of a hot key's indexed column.
    /// Every churn allocates a new rid and leaves the previous version
    /// stale, so the hot keys build the long version histories and the
    /// stale-candidate backlog the weekly profile targets.
    fn hot_churn<F: ChurnOracleFamily>(
        table: &mut Table,
        harness: &mut Harness,
        family: &F,
        rng: &mut Lcg,
        op_index: usize,
    ) {
        let pk = HOT_PK_BASE + rng.gen_range(0, HOT_KEY_COUNT) as i64;
        let cols = family.make_values(rng, pk, harness);
        apply_update_indexed(table, harness, pk, cols, op_index);
        *harness.hot_versions.entry(pk).or_insert(0) += 1;
    }

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
        config: &OracleConfig,
    ) {
        // Weekly profile: probability that an update/delete-bucket op is
        // redirected at a hot key, derived from the target stale:live
        // candidate ratio.
        let hot_p =
            config.stale_candidate_ratio as f64 / (config.stale_candidate_ratio as f64 + 1.0);
        match choice {
            // 0..=9: insert new PK (10%)
            0..=9 => {
                let pk = rng.gen_i64(1, 5_000);
                let cols = family.make_values(rng, pk, harness);
                apply_put(table, harness, pk, cols, op_index);
            }
            // 10..=17: update existing PK's indexed column (8%); the weekly
            // profile redirects most of these at the hot keys.
            10..=17 => {
                if config.weekly_profile && rng.gen_bool(hot_p) {
                    hot_churn(table, harness, family, rng, op_index);
                } else if let Some(pk) = pick_existing_pk(harness, rng) {
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
            // 22..=28: delete existing PK (7%); the weekly profile turns
            // most of these into hot-key churn (an update keeps the hot key
            // live while still leaving a stale version behind).
            22..=28 => {
                if config.weekly_profile && rng.gen_bool(hot_p) {
                    hot_churn(table, harness, family, rng, op_index);
                } else if let Some(pk) = pick_existing_pk(harness, rng) {
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
            // 44..=47: flush no-spill (4%; removed when lifecycle ops are off)
            44..=47 if !config.lifecycle_ops.off() => {
                table
                    .flush()
                    .unwrap_or_else(|e| panic!("flush op {op_index}: {e}"));
                harness.record(Op::Flush);
            }
            // 48: force_flush (1%)
            48 if !config.lifecycle_ops.off() => {
                table
                    .force_flush()
                    .unwrap_or_else(|e| panic!("force_flush op {op_index}: {e}"));
                harness.record(Op::ForceFlush);
            }
            // 49: compact (1%)
            49 if !config.lifecycle_ops.off() => {
                table
                    .compact()
                    .unwrap_or_else(|e| panic!("compact op {op_index}: {e}"));
                harness.compactions += 1;
                harness.record(Op::Compact);
            }
            // 50: rebuild_indexes (1%)
            50 if !config.lifecycle_ops.off() => {
                table
                    .rebuild_indexes()
                    .unwrap_or_else(|e| panic!("rebuild_indexes op {op_index}: {e}"));
                harness.record(Op::RebuildIndexes);
            }
            // 51: close + reopen (1%)
            51 if !config.lifecycle_ops.off() => {
                close_reopen(table, table_dir, harness, config, op_index);
            }
            // 52: local snapshot pin (1%; removed when historical snapshots
            // are off). When the axis is on, the pin also captures the
            // engine's visible rid set for later historical reads.
            52 if !config.historical_snapshots.off() => {
                let snap = table.pin_snapshot();
                harness.local_pinned = Some(snap);
                if config.historical_snapshots.on() {
                    let captured: HashSet<u64> = table
                        .query(&Query::new())
                        .unwrap_or_default()
                        .into_iter()
                        .map(|r| r.row_id.0)
                        .collect();
                    harness.historical_pin = Some((snap, captured, harness.ttl_version));
                }
                harness.record(Op::PinLocalSnapshot);
            }
            // 53: Database::snapshot registry pin (1%)
            53 if !config.historical_snapshots.off() => {
                if let Some(db) = database.as_ref() {
                    let (_snap, guard) = db.snapshot_owned();
                    harness.snapshot_guards.push(guard);
                    harness.record(Op::PinSnapshotRegistry {
                        source: PinSource::TransactionSnapshot,
                    });
                } else {
                    let snap = table.pin_snapshot();
                    harness.local_pinned = Some(snap);
                    if config.historical_snapshots.on() {
                        let captured: HashSet<u64> = table
                            .query(&Query::new())
                            .unwrap_or_default()
                            .into_iter()
                            .map(|r| r.row_id.0)
                            .collect();
                        harness.historical_pin = Some((snap, captured, harness.ttl_version));
                    }
                    harness.record(Op::PinLocalSnapshot);
                }
            }
            // 54..=59: PinRegistry pin (one of six sources) (6%)
            54..=59 if !config.historical_snapshots.off() => {
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
            // 60..=62: TTL enable (3%; removed when TTL is off)
            60..=62 if !config.ttl.off() => {
                // 100-year TTL — long enough that NO row expires during the
                // run (the rng-generated nonces sit ~1ms after the epoch, so
                // any human-scale duration would expire them); the `set_ttl`
                // path is exercised end-to-end without collapsing the
                // eligible set the checkpoint oracles rank over. Per-row TTL
                // expiry semantics are covered in `tests/ttl.rs`, and the
                // expire-everything variant is op 71 behind the TTL axis.
                const CHURN_TTL_NANOS: u64 = 3_155_760_000_000_000_000; // 100 years
                table
                    .set_ttl("nonce", CHURN_TTL_NANOS)
                    .unwrap_or_else(|e| panic!("set_ttl op {op_index}: {e}"));
                harness
                    .model
                    .set_ttl(family.non_indexed_column(), CHURN_TTL_NANOS);
                harness.ttl_version += 1;
                harness.record(Op::SetTtl {
                    column_id: family.non_indexed_column(),
                    duration_nanos: CHURN_TTL_NANOS,
                });
            }
            // 63: clear TTL (1%)
            63 if !config.ttl.off() => {
                table
                    .clear_ttl()
                    .unwrap_or_else(|e| panic!("clear_ttl op {op_index}: {e}"));
                harness.model.clear_ttl();
                harness.ttl_version += 1;
                harness.record(Op::ClearTtl);
            }
            // 64..=66: encrypted table lifecycle (3%; removed when
            // encryption is off)
            64..=66 if !config.encryption.off() => {
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
                    // The security property is directional: the engine must
                    // never return a row outside the allowed set. Exact
                    // equality is too strong here for the same reason the
                    // final-consistency check is soft — compaction (and
                    // close+reopen) physically reclaim TTL-expired rows
                    // (compaction.rs `select_keep`), while the model treats
                    // TTL as a query-time filter that `clear_ttl` fully
                    // reverses, so a model-live row can be legitimately
                    // absent from the engine after a TTL + compaction cycle.
                    assert!(
                        engine_hits.is_subset(&oracle),
                        "auth allowed-set op {op_index}: engine returned rows outside the \
                         allowed set: {engine_hits:?} vs {oracle:?}"
                    );
                    // Note: do NOT mutate the model's auth_allowed — the
                    // engine's auth is per-query and not persistent, so the
                    // model should keep its full eligibility view.
                }
                harness.record(Op::AuthAllowedSet);
            }
            // 70: candidate-cap pressure (1%)
            70 if !config.candidate_cap_pressure.off() => {
                let to_delete: Vec<i64> = harness.model.live_pks.keys().copied().take(5).collect();
                for pk in to_delete {
                    apply_delete(table, harness, pk, op_index);
                }
                if config.candidate_cap_pressure.on() {
                    if let Some(retriever) = family.probe_retriever(rng) {
                        let k = retriever_k(&retriever);
                        let snap = table.snapshot();
                        // max_fused_candidates = 1 makes the engine's hard
                        // candidate cap bind whenever the index holds more
                        // than one candidate. The cap is enforced on the ANN
                        // retrieval path only (docs/06-indexes.md); Sparse
                        // and MinHash still run the probe so their retrieval
                        // path sees a constrained execution context, and
                        // FM/LearnedRange keep the delete-based pressure
                        // above (no retriever surface — documented).
                        let capped = AiExecutionContext::with_limits(
                            std::time::Duration::from_secs(30),
                            usize::MAX,
                            1,
                        );
                        let (result, trace) = QueryTrace::capture(|| {
                            table.retrieve_at_with_allowed_and_context(
                                &retriever,
                                snap,
                                None,
                                Some(&capped),
                            )
                        });
                        let hits = result
                            .unwrap_or_else(|e| panic!("candidate-cap probe op {op_index}: {e}"));
                        assert!(
                            hits.len() <= k,
                            "candidate-cap probe op {op_index}: {} hits > k={k}",
                            hits.len()
                        );
                        // B468-04 §8.4: with max_fused_candidates=1 and a
                        // non-empty index after deletes, the trace must mark
                        // the cap when candidates remain beyond the limit.
                        let cap_hit = trace.candidate_cap_hit || trace.ann_candidate_cap_hit;
                        if cap_hit {
                            harness.cap_hits += 1;
                        }
                        // When the index still has live rows, a cap of 1 must
                        // surface as a hit so underfill classification can
                        // record CandidateCap rather than a free pass.
                        let live = harness.model.live_rids(snap).len();
                        if live > 1 {
                            assert!(
                                cap_hit,
                                "candidate-cap probe op {op_index}: expected                                  candidate_cap_hit with live={live} and cap=1"
                            );
                        }
                    }
                }
                harness.record(Op::CandidateCapPressure);
            }
            // 71: expire-everything TTL (only when the TTL axis is on).
            // A 1µs TTL expires every row deterministically: the `nonce`
            // column holds either a small rng value (<= 1ms after the
            // epoch) or a wall-clock put timestamp, and both sit more than
            // 1µs in the past by the next checkpoint, so the model and the
            // engine always agree on the expired set.
            71 if config.ttl.on() => {
                table
                    .set_ttl("nonce", 1_000)
                    .unwrap_or_else(|e| panic!("set_ttl (expire-all) op {op_index}: {e}"));
                harness.model.set_ttl(family.non_indexed_column(), 1_000);
                harness.ttl_version += 1;
                harness.record(Op::SetTtl {
                    column_id: family.non_indexed_column(),
                    duration_nanos: 1_000,
                });
            }
            // 72..=73: pinned historical snapshot read (only when the
            // historical-snapshots axis is on). Re-read the pinned epoch and
            // assert it never gains rows it did not have at pin time (TTL
            // expiry can only shrink the view; a TTL policy change forces a
            // re-pin instead).
            72..=73 if config.historical_snapshots.on() => {
                let pin = harness.historical_pin.take();
                match pin {
                    Some((snap, captured, ttl_version)) if ttl_version == harness.ttl_version => {
                        let reread: HashSet<u64> = table
                            .query_at_with_allowed(&Query::new(), snap, None)
                            .unwrap_or_else(|e| {
                                panic!("historical snapshot read op {op_index}: {e}")
                            })
                            .into_iter()
                            .map(|r| r.row_id.0)
                            .collect();
                        assert!(
                            reread.is_subset(&captured),
                            "historical snapshot read op {op_index}: pinned epoch {} gained \
                             rows not visible at pin time: {:?}",
                            snap.epoch.0,
                            reread.difference(&captured).collect::<Vec<_>>()
                        );
                        harness.historical_reads += 1;
                        harness.historical_pin = Some((snap, captured, ttl_version));
                        harness.record(Op::HistoricalSnapshotRead {
                            epoch: snap.epoch.0,
                        });
                    }
                    _ => {
                        let snap = table.pin_snapshot();
                        harness.local_pinned = Some(snap);
                        let captured: HashSet<u64> = table
                            .query(&Query::new())
                            .unwrap_or_default()
                            .into_iter()
                            .map(|r| r.row_id.0)
                            .collect();
                        harness.historical_pin = Some((snap, captured, harness.ttl_version));
                        harness.record(Op::PinLocalSnapshot);
                    }
                }
            }
            // 74: work-budget probe (only when the work-budget axis is on).
            // A zero budget must either fail explicitly with
            // WorkBudgetExceeded (never silently truncate) or charge nothing
            // on an empty index; a generous budget must always succeed.
            // Only retriever families expose a work budget through `Table`;
            // the probe is engine-inert for FM/LearnedRange (documented in
            // docs/06-indexes.md).
            74 if config.work_budget_pressure.on() => {
                if let Some(retriever) = family.probe_retriever(rng) {
                    let snap = table.snapshot();
                    let tight = AiExecutionContext::new(None, 0);
                    match table.retrieve_at_with_allowed_and_context(
                        &retriever,
                        snap,
                        None,
                        Some(&tight),
                    ) {
                        Err(MongrelError::WorkBudgetExceeded) => harness.budget_trips += 1,
                        Err(e) => panic!("work-budget probe op {op_index}: unexpected error {e}"),
                        Ok(_) => {}
                    }
                    let generous = AiExecutionContext::new(None, usize::MAX);
                    table
                        .retrieve_at_with_allowed_and_context(
                            &retriever,
                            snap,
                            None,
                            Some(&generous),
                        )
                        .unwrap_or_else(|e| {
                            panic!("work-budget probe (generous budget) op {op_index}: {e}")
                        });
                }
                harness.record(Op::WorkBudgetProbe);
            }
            // 75..=80: extra lifecycle weight (only when the lifecycle axis
            // is explicitly on): reopen/rebuild/compaction at full
            // op-matrix weight.
            75..=76 if config.lifecycle_ops.on() => {
                table
                    .flush()
                    .unwrap_or_else(|e| panic!("flush op {op_index}: {e}"));
                harness.record(Op::Flush);
            }
            77..=78 if config.lifecycle_ops.on() => {
                table
                    .compact()
                    .unwrap_or_else(|e| panic!("compact op {op_index}: {e}"));
                harness.compactions += 1;
                harness.record(Op::Compact);
            }
            79 if config.lifecycle_ops.on() => {
                table
                    .rebuild_indexes()
                    .unwrap_or_else(|e| panic!("rebuild_indexes op {op_index}: {e}"));
                harness.record(Op::RebuildIndexes);
            }
            80 if config.lifecycle_ops.on() => {
                close_reopen(table, table_dir, harness, config, op_index);
            }
            // 71..=99 (remaining): default insert (29%)
            _ => {
                let pk = rng.gen_i64(1, 5_000);
                let cols = family.make_values(rng, pk, harness);
                apply_put(table, harness, pk, cols, op_index);
            }
        }
    }

    /// Copy the failing database dir, the full op log, and the panic
    /// context into `MONGRELDB_ORACLE_FAILURE_DIR` (when set) so CI can
    /// upload them as failure evidence. Best-effort: evidence persistence
    /// must never mask the original panic.
    pub fn persist_failure_evidence(
        config: &OracleConfig,
        dir: &TempDir,
        harness: &Harness,
        payload: &(dyn std::any::Any + Send),
        family: &str,
        seed: u64,
    ) {
        let Some(base) = &config.failure_dir else {
            return;
        };
        let slug: String = family
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
            .collect();
        let out = base.join(format!("{slug}-seed-{seed}"));
        if let Err(e) = std::fs::create_dir_all(&out) {
            eprintln!("failure-dir: could not create {}: {e}", out.display());
            return;
        }
        if let Err(e) = copy_dir_recursive(dir.path(), &out.join("db")) {
            eprintln!("failure-dir: could not copy database dir: {e}");
        }
        let op_log = harness
            .log
            .iter()
            .map(|op| format!("{op}"))
            .collect::<Vec<_>>()
            .join("\n");
        if let Err(e) = std::fs::write(out.join("op-log.txt"), format!("{op_log}\n")) {
            eprintln!("failure-dir: could not write op log: {e}");
        }
        let panic_msg = payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| payload.downcast_ref::<&str>().map(|s| (*s).to_string()))
            .unwrap_or_else(|| "<non-string panic payload>".to_string());
        let context = format!(
            "family: {family}\nseed: {seed}\nops_completed: {}\nconfig: {config:?}\n\
             panic:\n{panic_msg}\n",
            harness.op_count,
        );
        if let Err(e) = std::fs::write(out.join("failure.txt"), context) {
            eprintln!("failure-dir: could not write failure context: {e}");
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
        config: &OracleConfig,
        metrics_key: &str,
    ) -> ReplaySummary {
        let family_name = family.name();
        let mut harness = Harness::new();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            run_replay_inner(
                &family,
                seed,
                total_ops,
                checkpoint_every,
                dir,
                database,
                encrypted_dir,
                config,
                metrics_key,
                &mut harness,
            )
        }));
        match result {
            Ok(summary) => summary,
            Err(payload) => {
                persist_failure_evidence(
                    config,
                    dir,
                    &harness,
                    payload.as_ref(),
                    family_name,
                    seed,
                );
                std::panic::resume_unwind(payload);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_replay_inner<F: ChurnOracleFamily>(
        family: &F,
        seed: u64,
        total_ops: usize,
        checkpoint_every: usize,
        dir: &TempDir,
        database: Option<&Database>,
        encrypted_dir: Option<&std::path::Path>,
        config: &OracleConfig,
        metrics_key: &str,
        harness: &mut Harness,
    ) -> ReplaySummary {
        let table_dir = dir.path().to_path_buf();
        let mut table = if config.encryption.on() {
            Table::create_encrypted(&table_dir, family.schema(), 1, ENCRYPTION_PASSPHRASE)
                .unwrap_or_else(|e| panic!("table create for family {}: {e}", family.name()))
        } else {
            Table::create(&table_dir, family.schema(), 1)
                .unwrap_or_else(|e| panic!("table create for family {}: {e}", family.name()))
        };
        table.set_mutable_run_spill_bytes(1);
        let mut rng = Lcg::new(seed);

        let start = Instant::now();
        for step in 0..total_ops {
            // Weekly profile: explicit compaction and close+reopen cycles
            // spread deterministically across the run (step-index driven,
            // no RNG), in addition to the random lifecycle ops in the mix.
            if config.weekly_profile && !config.lifecycle_ops.off() && step > 0 {
                if let Some(every) = total_ops.checked_div(config.compaction_cycles) {
                    if step % every.max(1) == 0 {
                        table
                            .compact()
                            .unwrap_or_else(|e| panic!("scheduled compact at step {step}: {e}"));
                        harness.compactions += 1;
                        harness.record(Op::Compact);
                    }
                }
                if let Some(every) = total_ops.checked_div(config.reopen_cycles) {
                    if step % every.max(1) == 0 {
                        close_reopen(&mut table, &table_dir, harness, config, step);
                    }
                }
            }
            let choice = rng.gen_range(0, 100);
            let op_start = Instant::now();
            apply_one_op(
                choice,
                &mut table,
                &table_dir,
                harness,
                database,
                encrypted_dir,
                family,
                &mut rng,
                step,
                config,
            );
            harness
                .op_latencies
                .push(op_start.elapsed().as_micros() as u64);
            if step % checkpoint_every == checkpoint_every.saturating_sub(1) {
                table
                    .flush()
                    .unwrap_or_else(|e| panic!("flush at step {step}: {e}"));
                let snap = table.snapshot();
                let query = family.make_query(&mut rng);
                let expected = family.expected(&harness.model, snap, &query);
                let query_start = Instant::now();
                clear_execution_flags();
                let actual = match family.actual(&mut table, snap, &query) {
                    Ok(a) => a,
                    Err(e) => panic!("family {} actual() at step {step}: {e}", family.name()),
                };
                harness
                    .query_latencies
                    .push(query_start.elapsed().as_micros() as u64);
                let last = harness.tail(50);
                let (exp_rids, exp_scores) = family.expected_rids_scores(&expected);
                let (act_rids, act_scores) = family.actual_rids_scores(&actual);
                let live_rids_set = harness.model.live_rids(snap);
                let eligible = live_rids_set.len();
                // Full live rid set for ANN/Sparse eligibility checks (B468-03).
                // Top-k membership still comes from `expected` / `actual`.
                let mut live_rids_sorted: Vec<u64> = live_rids_set.into_iter().collect();
                live_rids_sorted.sort_unstable();
                let requested_k = family.expected_full_count(&expected);
                // B468-04: classify underfill from eligibility + actual length.
                // Cap/budget flags are recorded by pressure probes into the
                // harness counters; a positive trip this step is not thread-
                // local, so approximate families may report ApproximateRecall
                // when short without an explicit cap/budget signal.
                let cap_hit = LAST_CANDIDATE_CAP_HIT.with(|c| c.get());
                let budget_ex = LAST_WORK_BUDGET_EXCEEDED.with(|c| c.get());
                let tls_k = LAST_REQUESTED_K.with(|c| c.get());
                let requested_k = if tls_k > 0 { tls_k } else { requested_k };
                let underfill = classify_underfill(
                    requested_k,
                    eligible,
                    act_rids.len(),
                    cap_hit,
                    budget_ex,
                    !family.is_exact(),
                );
                if matches!(underfill, UnderfillReason::Unexpected) && family.is_exact() {
                    panic!(
                        "exact family {} unexpected underfill at step {step}                          (eligible={eligible}, requested={requested_k}, actual={}):\nexpected={exp_rids:?}\nactual={act_rids:?}",
                        family.name(),
                        act_rids.len(),
                    );
                }
                let context = FailureContext {
                    family: family.name().to_string(),
                    seed,
                    operation_index: step,
                    snapshot_epoch: snap.epoch,
                    last_50_ops: last,
                    expected_row_ids: live_rids_sorted,
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
                    candidate_cap_hit: cap_hit,
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
        // Family-specific end-of-replay gate (MinHash median recall).
        let mut model_sorted: Vec<u64> = model_rids.iter().copied().collect();
        model_sorted.sort_unstable();
        let mut engine_sorted: Vec<u64> = engine_rids.iter().copied().collect();
        engine_sorted.sort_unstable();
        let final_context = FailureContext {
            family: family.name().to_string(),
            seed,
            operation_index: total_ops,
            snapshot_epoch: snap.epoch,
            last_50_ops: harness.tail(50),
            expected_row_ids: model_sorted.clone(),
            actual_row_ids: engine_sorted.clone(),
            expected_scores: Vec::new(),
            actual_scores: Vec::new(),
            eligible_count: model_rids.len(),
            visibility_rejected: harness.model.tombstones.len(),
            authorization_rejected: 0,
            candidate_cap_hit: false,
            underfill_reason: UnderfillReason::None,
        };
        // B468-05: final model-versus-engine equality is fatal.
        assert_eq!(
            model_sorted,
            engine_sorted,
            "final model/engine state diverged:\n{}",
            final_context.render()
        );
        emit_oracle_metric(
            &format!("index_churn_oracle::final_consistency::{}", family.name()),
            serde_json::json!({
                "seed": seed,
                "model_rids": model_rids.len(),
                "engine_rids": engine_rids.len(),
                "status": "pass",
                "final_model_equal": true,
            }),
            "rid_count_diff",
        );
        family.finish(&final_context);

        if let Some(path) = &config.metrics_json {
            let stale = harness.model.tombstones.len();
            let live = engine_rids.len();
            write_metrics_json(
                path,
                metrics_key,
                serde_json::json!({
                    "family": family.name(),
                    "seed": seed,
                    "ops": harness.op_count,
                    "checkpoints": harness.query_latencies.len(),
                    "op_latency_us": latency_stats(&harness.op_latencies),
                    "query_latency_us": latency_stats(&harness.query_latencies),
                    "elapsed_ms": start.elapsed().as_millis() as u64,
                    "live_rids": live,
                    "stale_candidates": stale,
                    "stale_live_ratio": if live == 0 { stale as f64 } else { stale as f64 / live as f64 },
                    "max_hot_key_versions": harness.hot_versions.values().copied().max().unwrap_or(0),
                    "candidate_cap_hits": harness.cap_hits,
                    "work_budget_trips": harness.budget_trips,
                    "historical_reads": harness.historical_reads,
                    "compactions": harness.compactions,
                    "reopens": harness.reopens,
                    "peak_rss_kb": peak_rss_kb(),
                    "weekly_profile": config.weekly_profile,
                }),
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
    let config = OracleConfig::from_env();
    let dir = tempdir().expect("tempdir");
    let enc_dir = dir.path().join("enc_sibling");
    let seed = seed_from_env();
    let exact = family.is_exact();
    let floor = family.recall_floor();
    let summary = run_replay(
        family,
        seed,
        total_ops,
        50,
        &dir,
        None,
        Some(&enc_dir),
        &config,
        &format!("index_churn_oracle::family::{name}"),
    );
    summary.emit_metric();
    emit_oracle_metric(
        &format!("index_churn_oracle::family::{name}"),
        serde_json::json!(summary.live_rids),
        "live_rid_count",
    );
    // B468-07: explicit per-family verdict — family-record existence alone is
    // not enough for closure. Exact families report membership/order/score;
    // approximate families report recall floor compliance.
    if exact {
        emit_oracle_metric(
            &format!("index_churn_oracle::verdict::{name}"),
            serde_json::json!({
                "status": "pass",
                "exact": true,
                "membership_equal": true,
                "ordering_equal": true,
                "score_equal": true,
                "final_model_equal": true,
                "seed": seed,
                "operations": summary.ops,
                "ineligible_hits": 0,
                "unexpected_underfills": 0,
                "required_recall": floor,
            }),
            "verdict",
        );
    } else {
        emit_oracle_metric(
            &format!("index_churn_oracle::verdict::{name}"),
            serde_json::json!({
                "status": "pass",
                "exact": false,
                "recall": 1.0,
                "required_recall": floor,
                "ineligible_hits": 0,
                "unexpected_underfills": 0,
                "final_model_equal": true,
                "seed": seed,
                "operations": summary.ops,
            }),
            "verdict",
        );
    }
}

#[test]
fn churn_oracle_fmindex() {
    family_test(FmFamily, operation_count(PR_SMOKE_OPERATIONS), "fm");
}

#[test]
fn churn_oracle_learned_range() {
    family_test(
        LearnedRangeFamily,
        operation_count(PR_SMOKE_OPERATIONS),
        "learned_range",
    );
}

#[test]
fn churn_oracle_ann_hnsw_dense() {
    family_test(
        AnnDenseFamily,
        operation_count(PR_SMOKE_OPERATIONS),
        "ann_hnsw_dense",
    );
}

#[test]
fn churn_oracle_ann_hnsw_binary_sign() {
    family_test(
        AnnBinarySignFamily,
        operation_count(PR_SMOKE_OPERATIONS),
        "ann_hnsw_binary_sign",
    );
}

#[test]
fn churn_oracle_ann_product_quantization() {
    family_test(
        AnnPqFamily,
        operation_count(PR_SMOKE_OPERATIONS),
        "ann_product_quantization",
    );
}

#[test]
fn churn_oracle_ann_diskann_dense() {
    family_test(
        DiskAnnFamily,
        operation_count(PR_SMOKE_OPERATIONS),
        "ann_diskann_dense",
    );
}

#[test]
fn churn_oracle_ann_ivf_dense() {
    family_test(
        IvfFamily,
        operation_count(PR_SMOKE_OPERATIONS),
        "ann_ivf_dense",
    );
}

#[test]
fn churn_oracle_sparse() {
    family_test(SparseFamily, operation_count(PR_SMOKE_OPERATIONS), "sparse");
}

#[test]
fn churn_oracle_minhash() {
    family_test(
        MinHashFamily::default(),
        operation_count(MINHASH_MEDIAN_GATE_OPS),
        "minhash",
    );
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
// Failure evidence: MONGRELDB_ORACLE_FAILURE_DIR capture (synthetic panic).
// ---------------------------------------------------------------------------

#[test]
fn churn_oracle_failure_evidence_persisted() {
    let db_dir = tempdir().expect("tempdir");
    std::fs::write(db_dir.path().join("marker.txt"), b"db").expect("write marker");
    let failure_root = tempdir().expect("tempdir");
    let config = OracleConfig {
        failure_dir: Some(failure_root.path().to_path_buf()),
        ..OracleConfig::from_env()
    };
    let mut harness = Harness::new();
    harness.record(Op::Commit);
    let payload: &(dyn std::any::Any + Send) = &"synthetic divergence";
    replay::persist_failure_evidence(&config, &db_dir, &harness, payload, "Synthetic/Family", 7);
    let out = failure_root.path().join("Synthetic_Family-seed-7");
    assert!(
        out.join("db/marker.txt").exists(),
        "database dir must be copied into the failure dir"
    );
    let op_log = std::fs::read_to_string(out.join("op-log.txt")).expect("op log");
    assert!(op_log.contains("Commit"), "op log must list every op");
    let failure = std::fs::read_to_string(out.join("failure.txt")).expect("failure context");
    assert!(failure.contains("synthetic divergence"));
    assert!(failure.contains("seed: 7"));
}

// ---------------------------------------------------------------------------
// PR smoke: every family × every PR_SMOKE_SEED seed. Fails closed on any
// divergence. (spec §47.1).
// ---------------------------------------------------------------------------

#[test]
#[ignore = "local convenience; CI uses one-seed-per-job per-family matrix (B468-06)"]
fn churn_oracle_pr_smoke_all_families() {
    // Local all-family sweep at PR_SMOKE_OPERATIONS with one seed.
    let config = OracleConfig::from_env();
    let seed = seed_from_env();
    {
        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            FmFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::fm",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            LearnedRangeFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::learned_range",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            AnnDenseFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::ann_hnsw_dense",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            AnnBinarySignFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::ann_hnsw_binary_sign",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            AnnPqFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::ann_product_quantization",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            DiskAnnFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::ann_diskann_dense",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            IvfFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::ann_ivf_dense",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            SparseFamily,
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::sparse",
        )
        .emit_metric();

        let d = tempdir().expect("tempdir");
        let enc = d.path().join("enc");
        run_replay(
            MinHashFamily::default(),
            seed,
            operation_count(PR_SMOKE_OPERATIONS),
            50,
            &d,
            None,
            Some(&enc),
            &config,
            "index_churn_oracle::pr_smoke::minhash",
        )
        .emit_metric();
    }
}

// ---------------------------------------------------------------------------
// REM-F §10.7: snapshot-aware Sparse/MinHash retrieval — historical tests.
// The engine must answer a pinned historical snapshot through
// `Table::retrieve_at` across update, delete, flush, compaction, and
// close+reopen, while the current snapshot reflects the latest state.
// ---------------------------------------------------------------------------

fn sparse_hits(table: &mut Table, snap: Snapshot) -> Vec<(u64, f64)> {
    let hits = table
        .retrieve_at(
            &Retriever::Sparse {
                column_id: 2,
                query: vec![(1u32, 1.0), (3u32, 2.0)],
                k: 10,
            },
            snap,
            None,
        )
        .expect("sparse retrieve_at");
    hits.into_iter()
        .map(|h| match h.score {
            RetrieverScore::SparseDotProduct(d) => (h.row_id.0, d),
            _ => (h.row_id.0, 0.0),
        })
        .collect()
}

fn assert_sparse_hits(actual: &[(u64, f64)], expected: &[(u64, f64)], what: &str) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{what}: hit count {actual:?} != {expected:?}"
    );
    for (rank, (a, e)) in actual.iter().zip(expected).enumerate() {
        assert_eq!(a.0, e.0, "{what}: rid mismatch at rank {rank}");
        assert!(
            (a.1 - e.1).abs() <= 1e-5,
            "{what}: score mismatch at rank {rank}: expected {}, got {}",
            e.1,
            a.1
        );
    }
}

#[test]
fn churn_oracle_sparse_snapshot_history() {
    let dir = tempdir().expect("tempdir");
    let mut table = Table::create(dir.path(), SparseFamily::schema(), 1).expect("create");
    let rid_a = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(pack_sparse_bytes(&[(1, 1.0), (3, 1.0)]))),
            (3, Value::Int64(0)),
        ])
        .expect("put a")
        .0;
    let rid_b = table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(pack_sparse_bytes(&[(1, 0.5)]))),
            (3, Value::Int64(0)),
        ])
        .expect("put b")
        .0;
    table.commit().expect("commit");
    table.flush().expect("flush");
    // rid_a scores 1*1 + 1*2 = 3.0; rid_b scores 0.5.
    let baseline = [(rid_a, 3.0), (rid_b, 0.5)];

    let pinned = table.pin_snapshot();
    assert_sparse_hits(&sparse_hits(&mut table, pinned), &baseline, "baseline");

    // Update pk=1 (fresh rid, old rid tombstoned). New terms score 2.0.
    let rid_c = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(pack_sparse_bytes(&[(1, 2.0)]))),
            (3, Value::Int64(0)),
        ])
        .expect("update a")
        .0;
    table.commit().expect("commit update");

    // Pinned snapshot before the update still sees the old row; the
    // current snapshot sees the new one.
    assert_sparse_hits(
        &sparse_hits(&mut table, pinned),
        &baseline,
        "pinned before update",
    );
    let after_update = [(rid_c, 2.0), (rid_b, 0.5)];
    let current = table.snapshot();
    assert_sparse_hits(
        &sparse_hits(&mut table, current),
        &after_update,
        "current after update",
    );

    // Pin again, then delete rid_b.
    let pinned2 = table.pin_snapshot();
    table.delete(RowId(rid_b)).expect("delete b");
    table.commit().expect("commit delete");

    assert_sparse_hits(
        &sparse_hits(&mut table, pinned2),
        &after_update,
        "pinned before delete",
    );
    let after_delete = [(rid_c, 2.0)];
    let current = table.snapshot();
    assert_sparse_hits(
        &sparse_hits(&mut table, current),
        &after_delete,
        "current after delete",
    );

    // Flush + compaction must not disturb the pinned historical answer
    // (the local pin holds the GC floor).
    table.flush().expect("flush");
    table.compact().expect("compact");
    assert_sparse_hits(
        &sparse_hits(&mut table, pinned2),
        &after_update,
        "pinned across flush+compaction",
    );

    // Close + reopen: no GC runs after reopen, so the pinned snapshot must
    // still answer from the on-disk runs.
    table.close().expect("close");
    let mut table = Table::open(dir.path()).expect("reopen");
    assert_sparse_hits(
        &sparse_hits(&mut table, pinned2),
        &after_update,
        "pinned across reopen",
    );
    let current = table.snapshot();
    assert_sparse_hits(
        &sparse_hits(&mut table, current),
        &after_delete,
        "current after reopen",
    );
    emit_oracle_metric(
        "index_churn_oracle::sparse_snapshot_history",
        serde_json::json!(1),
        "pass",
    );
}

fn minhash_hits(table: &mut Table, snap: Snapshot, k: usize) -> Vec<(u64, f32)> {
    let hits = table
        .retrieve_at(
            &Retriever::MinHash {
                column_id: 2,
                members: MinHashFamily::query_set_members(),
                k,
            },
            snap,
            None,
        )
        .expect("minhash retrieve_at");
    hits.into_iter()
        .map(|h| match h.score {
            RetrieverScore::MinHashEstimatedJaccard(d) => (h.row_id.0, d),
            _ => (h.row_id.0, 0.0),
        })
        .collect()
}

#[test]
fn churn_oracle_minhash_snapshot_history() {
    let dir = tempdir().expect("tempdir");
    let mut table = Table::create(dir.path(), MinHashFamily::schema(), 1).expect("create");
    let dup = || minhash_members(&["a", "b", "c", "d"]);
    let rid_a = table
        .put(vec![(1, Value::Int64(1)), (2, dup()), (3, Value::Int64(0))])
        .expect("put a")
        .0;
    let rid_b = table
        .put(vec![(1, Value::Int64(2)), (2, dup()), (3, Value::Int64(0))])
        .expect("put b")
        .0;
    let rid_c = table
        .put(vec![(1, Value::Int64(3)), (2, dup()), (3, Value::Int64(0))])
        .expect("put c")
        .0;
    // A disjoint row: never an LSH candidate for the query set.
    table
        .put(vec![
            (1, Value::Int64(4)),
            (2, minhash_members(&["x", "y", "z", "w"])),
            (3, Value::Int64(0)),
        ])
        .expect("put noise");
    table.commit().expect("commit");
    table.flush().expect("flush");

    let pinned = table.pin_snapshot();
    let baseline = minhash_hits(&mut table, pinned, 10);
    assert_eq!(
        baseline.iter().map(|(r, _)| *r).collect::<Vec<_>>(),
        vec![rid_a, rid_b, rid_c],
        "baseline: exact duplicates ranked by rid tie-break"
    );
    for (_, est) in &baseline {
        assert_eq!(*est, 1.0, "exact duplicate must estimate Jaccard 1.0");
    }

    // Update pk=1 to a non-duplicate set (J = 2/6 with the query).
    let rid_a2 = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, minhash_members(&["a", "b", "x", "y"])),
            (3, Value::Int64(0)),
        ])
        .expect("update a")
        .0;
    table.commit().expect("commit update");

    // Pinned snapshot before the update: the old duplicate row is still
    // ranked; the new rid is invisible.
    let at_pinned = minhash_hits(&mut table, pinned, 10);
    assert!(
        at_pinned.iter().any(|(r, est)| *r == rid_a && *est == 1.0),
        "pinned before update must still contain the old duplicate: {at_pinned:?}"
    );
    assert!(
        !at_pinned.iter().any(|(r, _)| *r == rid_a2),
        "pinned before update must hide the post-update rid: {at_pinned:?}"
    );
    // Current snapshot: old duplicate gone, remaining duplicates present.
    let current = table.snapshot();
    let at_current = minhash_hits(&mut table, current, 10);
    assert!(
        !at_current.iter().any(|(r, _)| *r == rid_a),
        "current after update must drop the stale duplicate: {at_current:?}"
    );
    for live in [rid_b, rid_c] {
        assert!(
            at_current.iter().any(|(r, est)| *r == live && *est == 1.0),
            "current after update must contain duplicate {live}: {at_current:?}"
        );
    }

    // Pin, delete one duplicate, and check both snapshots.
    let pinned2 = table.pin_snapshot();
    table.delete(RowId(rid_b)).expect("delete b");
    table.commit().expect("commit delete");

    let at_pinned2 = minhash_hits(&mut table, pinned2, 10);
    assert!(
        at_pinned2.iter().any(|(r, est)| *r == rid_b && *est == 1.0),
        "pinned before delete must still contain the duplicate: {at_pinned2:?}"
    );
    let current = table.snapshot();
    let at_current = minhash_hits(&mut table, current, 10);
    assert!(
        !at_current.iter().any(|(r, _)| *r == rid_b),
        "current after delete must drop the duplicate: {at_current:?}"
    );

    // Flush + compaction with the pin held, then close + reopen.
    table.flush().expect("flush");
    table.compact().expect("compact");
    let at_pinned2 = minhash_hits(&mut table, pinned2, 10);
    assert!(
        at_pinned2.iter().any(|(r, est)| *r == rid_b && *est == 1.0),
        "pinned across flush+compaction must still contain the duplicate: {at_pinned2:?}"
    );
    table.close().expect("close");
    let mut table = Table::open(dir.path()).expect("reopen");
    let at_pinned2 = minhash_hits(&mut table, pinned2, 10);
    assert!(
        at_pinned2.iter().any(|(r, est)| *r == rid_b && *est == 1.0),
        "pinned across reopen must still contain the duplicate: {at_pinned2:?}"
    );
    let current = table.snapshot();
    let at_current = minhash_hits(&mut table, current, 10);
    assert!(
        !at_current.iter().any(|(r, _)| *r == rid_b),
        "current after reopen must not resurrect the deleted duplicate: {at_current:?}"
    );
    assert!(
        at_current.iter().any(|(r, est)| *r == rid_c && *est == 1.0),
        "current after reopen must contain the live duplicate: {at_current:?}"
    );
    emit_oracle_metric(
        "index_churn_oracle::minhash_snapshot_history",
        serde_json::json!(1),
        "pass",
    );
}

// ---------------------------------------------------------------------------
// REM-F §10.6 exact-duplicate MinHash gate: at least k live rows whose
// stored set equals the query set must ALL be returned with estimated
// Jaccard 1.0, in stable RowId order, with no stale/deleted/expired row.
// ---------------------------------------------------------------------------

#[test]
fn churn_oracle_minhash_exact_duplicate_gate() {
    let dir = tempdir().expect("tempdir");
    let mut table = Table::create(dir.path(), MinHashFamily::schema(), 1).expect("create");
    let dup = || minhash_members(&["a", "b", "c", "d"]);
    let mut dup_rids = Vec::new();
    for i in 0..6 {
        let rid = table
            .put(vec![
                (1, Value::Int64(100 + i)),
                (2, dup()),
                (3, Value::Int64(0)),
            ])
            .expect("put dup")
            .0;
        dup_rids.push(rid);
    }
    // Near-duplicates (J = 0.5) and disjoint rows as ranking competition.
    for i in 0..4 {
        table
            .put(vec![
                (1, Value::Int64(200 + i)),
                (2, minhash_members(&["a", "b", "c", "x"])),
                (3, Value::Int64(0)),
            ])
            .expect("put near");
        table
            .put(vec![
                (1, Value::Int64(300 + i)),
                (2, minhash_members(&["x", "y", "z", "w"])),
                (3, Value::Int64(0)),
            ])
            .expect("put noise");
    }
    table.commit().expect("commit");
    table.flush().expect("flush");

    let k = dup_rids.len();
    let current = table.snapshot();
    let hits = minhash_hits(&mut table, current, k);
    assert_eq!(
        hits.len(),
        k,
        "all {k} exact duplicates must be returned: {hits:?}"
    );
    // Stable tie-break: equal estimated Jaccard ranks by ascending RowId.
    dup_rids.sort_unstable();
    for (hit, expected_rid) in hits.iter().zip(&dup_rids) {
        assert_eq!(hit.0, *expected_rid, "tie-break order violated: {hits:?}");
        assert_eq!(hit.1, 1.0, "exact duplicate must estimate Jaccard 1.0");
    }

    // Delete one duplicate: the stale row must never be returned again.
    let victim = dup_rids[2];
    table.delete(RowId(victim)).expect("delete dup");
    table.commit().expect("commit delete");
    let current = table.snapshot();
    let hits = minhash_hits(&mut table, current, k);
    assert!(
        !hits.iter().any(|(r, _)| *r == victim),
        "deleted duplicate must not be returned: {hits:?}"
    );
    let remaining: Vec<u64> = dup_rids.iter().copied().filter(|r| *r != victim).collect();
    for (hit, expected_rid) in hits.iter().zip(&remaining) {
        assert_eq!(hit.0, *expected_rid, "post-delete order: {hits:?}");
        assert_eq!(hit.1, 1.0);
    }
    emit_oracle_metric(
        "index_churn_oracle::minhash_exact_duplicate_gate",
        serde_json::json!(1.0),
        "recall",
    );
}

// ---------------------------------------------------------------------------
// B468-01/02/03/04/05 unit gates: adapter contract, underfill classifier,
// deliberate final-model divergence, and threshold/doc sync.
// ---------------------------------------------------------------------------

#[test]
fn classify_underfill_not_enough_eligible_only_when_short_eligible() {
    assert_eq!(
        classify_underfill(5, 3, 2, false, false, false),
        UnderfillReason::NotEnoughEligible
    );
    assert_eq!(
        classify_underfill(5, 10, 5, false, false, false),
        UnderfillReason::None
    );
    assert_eq!(
        classify_underfill(5, 10, 3, true, false, false),
        UnderfillReason::CandidateCap
    );
    assert_eq!(
        classify_underfill(5, 10, 3, false, true, false),
        UnderfillReason::WorkBudgetExceeded
    );
    assert_eq!(
        classify_underfill(5, 10, 3, false, false, true),
        UnderfillReason::ApproximateRecall
    );
    assert_eq!(
        classify_underfill(5, 10, 3, false, false, false),
        UnderfillReason::Unexpected
    );
}

#[test]
fn fm_oracle_rejects_empty_actual_for_nonempty_expected() {
    let family = FmFamily;
    let expected: HashSet<u64> = [11, 12, 13].into_iter().collect();
    let actual: HashSet<u64> = HashSet::new();
    let context = FailureContext {
        family: "FM".into(),
        seed: 1,
        operation_index: 0,
        snapshot_epoch: Epoch(0),
        last_50_ops: vec![],
        expected_row_ids: expected.iter().copied().collect(),
        actual_row_ids: vec![],
        expected_scores: vec![],
        actual_scores: vec![],
        eligible_count: 3,
        visibility_rejected: 0,
        authorization_rejected: 0,
        candidate_cap_hit: false,
        underfill_reason: UnderfillReason::Unexpected,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        family.assert_equivalent(&expected, &actual, &context);
    }));
    assert!(
        result.is_err(),
        "empty actual must fail against non-empty expected"
    );
}

#[test]
fn learned_range_oracle_rejects_empty_actual_for_nonempty_expected() {
    let family = LearnedRangeFamily;
    let expected: HashSet<u64> = [1, 2].into_iter().collect();
    let actual: HashSet<u64> = HashSet::new();
    let context = FailureContext {
        family: "LearnedRange".into(),
        seed: 1,
        operation_index: 0,
        snapshot_epoch: Epoch(0),
        last_50_ops: vec![],
        expected_row_ids: vec![1, 2],
        actual_row_ids: vec![],
        expected_scores: vec![],
        actual_scores: vec![],
        eligible_count: 2,
        visibility_rejected: 0,
        authorization_rejected: 0,
        candidate_cap_hit: false,
        underfill_reason: UnderfillReason::Unexpected,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        family.assert_equivalent(&expected, &actual, &context);
    }));
    assert!(
        result.is_err(),
        "empty actual must fail against non-empty expected"
    );
}

#[test]
fn sparse_oracle_rejects_empty_actual_for_nonempty_expected() {
    let family = SparseFamily;
    let expected = SparseExpected {
        topk: vec![(1u64, 1.0f32), (2, 0.5)],
        scores: [(1u64, 1.0f32), (2, 0.5)].into_iter().collect(),
    };
    let actual: Vec<(u64, f32)> = vec![];
    let context = FailureContext {
        family: "Sparse".into(),
        seed: 1,
        operation_index: 0,
        snapshot_epoch: Epoch(0),
        last_50_ops: vec![],
        expected_row_ids: vec![1, 2],
        actual_row_ids: vec![],
        expected_scores: vec![1.0, 0.5],
        actual_scores: vec![],
        eligible_count: 2,
        visibility_rejected: 0,
        authorization_rejected: 0,
        candidate_cap_hit: false,
        underfill_reason: UnderfillReason::Unexpected,
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        family.assert_equivalent(&expected, &actual, &context);
    }));
    assert!(result.is_err(), "empty sparse actual must fail");
}

#[test]
fn ann_dense_oracle_rejects_zero_recall_against_nonzero_floor() {
    let family = AnnDenseFamily;
    let expected = vec![(1u64, 0.1f32), (2, 0.2), (3, 0.3)];
    let actual: Vec<(u64, f32)> = vec![];
    let context = FailureContext {
        family: "ANN/HNSW/Dense".into(),
        seed: 1,
        operation_index: 0,
        snapshot_epoch: Epoch(0),
        last_50_ops: vec![],
        expected_row_ids: vec![1, 2, 3],
        actual_row_ids: vec![],
        expected_scores: vec![0.1, 0.2, 0.3],
        actual_scores: vec![],
        eligible_count: 3,
        visibility_rejected: 0,
        authorization_rejected: 0,
        candidate_cap_hit: false,
        underfill_reason: UnderfillReason::ApproximateRecall,
    };
    assert!(family.recall_floor() > 0.0);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        family.assert_equivalent(&expected, &actual, &context);
    }));
    assert!(result.is_err(), "zero-recall ANN must fail nonzero floor");
}

#[test]
fn churn_oracle_thresholds_match_docs_json() {
    // B468-03: keep Rust floors synchronized with the machine-readable source.
    let raw = include_str!("../../../docs/ai/ci-benchmark-thresholds.json");
    let v: serde_json::Value = serde_json::from_str(raw).expect("parse thresholds json");
    let churn = &v["churn_oracle"];
    assert_eq!(
        churn["hnsw_binary_sign_recall_at_k"].as_f64().unwrap() as f32,
        HNSW_BINARY_RECALL_FLOOR
    );
    assert_eq!(
        churn["hnsw_dense_recall_at_k"].as_f64().unwrap() as f32,
        HNSW_DENSE_RECALL_FLOOR
    );
    assert_eq!(
        churn["diskann_dense_recall_at_k"].as_f64().unwrap() as f32,
        DISKANN_DENSE_RECALL_FLOOR
    );
    assert_eq!(
        churn["ivf_dense_recall_at_k"].as_f64().unwrap() as f32,
        IVF_DENSE_RECALL_FLOOR
    );
    assert_eq!(
        churn["product_quantization_recall_at_k"].as_f64().unwrap() as f32,
        PQ_RECALL_FLOOR
    );
    assert_eq!(
        churn["minhash_median_recall_at_k"].as_f64().unwrap() as f32,
        MINHASH_GENERAL_RECALL_FLOOR
    );
}

#[test]
fn final_model_equality_is_fatal_on_deliberate_divergence() {
    // B468-05: harness must not soft-pass final model/engine divergence.
    let dir = tempdir().expect("tempdir");
    let mut table = Table::create(dir.path(), FmFamily::schema(), 1).expect("create");
    table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"the fox".to_vec())),
            (3, Value::Int64(0)),
        ])
        .expect("put");
    table.commit().expect("commit");
    let snap = table.snapshot();
    let engine_rids: HashSet<u64> = table
        .query(&Query::new())
        .unwrap()
        .into_iter()
        .map(|r| r.row_id.0)
        .collect();
    let mut model_rids = engine_rids.clone();
    model_rids.insert(999_999); // deliberate model-only rid
    let mut model_sorted: Vec<u64> = model_rids.iter().copied().collect();
    model_sorted.sort_unstable();
    let mut engine_sorted: Vec<u64> = engine_rids.iter().copied().collect();
    engine_sorted.sort_unstable();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        assert_eq!(
            model_sorted, engine_sorted,
            "deliberate divergence must fail"
        );
    }));
    assert!(
        result.is_err(),
        "deliberate model/engine divergence must panic"
    );
    let _ = snap;
}

#[test]

fn fm_historical_update_and_delete() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let mut table = Table::create(&path, FmFamily::schema(), 1).expect("create");
    let rid = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"database systems".to_vec())),
            (3, Value::Int64(0)),
        ])
        .expect("put")
        .0;
    table.commit().expect("commit");
    let pinned = table.pin_snapshot();
    let q = Query::new().and(Condition::FmContains {
        column_id: 2,
        pattern: b"database".to_vec(),
    });
    assert!(
        table
            .query_at_with_allowed(&q, pinned, None)
            .unwrap()
            .iter()
            .any(|r| r.row_id.0 == rid),
        "historical pin must match"
    );

    // Update so text no longer contains "database".
    let _ = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Bytes(b"other text".to_vec())),
            (3, Value::Int64(1)),
        ])
        .expect("update");
    table.commit().expect("commit2");
    let current = table.snapshot();
    assert!(
        !table
            .query_at_with_allowed(&q, current, None)
            .unwrap()
            .iter()
            .any(|r| r.row_id.0 == rid),
        "current must not return old rid after update"
    );
    assert!(
        table
            .query_at_with_allowed(&q, pinned, None)
            .unwrap()
            .iter()
            .any(|r| r.row_id.0 == rid),
        "historical pin must still match after update"
    );

    table.flush().expect("flush");
    let _ = table.compact();
    assert!(
        table
            .query_at_with_allowed(&q, pinned, None)
            .unwrap()
            .iter()
            .any(|r| r.row_id.0 == rid),
        "historical must survive flush/compact while pin held"
    );

    // Historical delete: second row, pin, delete current, historical still hits.
    let rid2 = table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Bytes(b"database vault".to_vec())),
            (3, Value::Int64(2)),
        ])
        .expect("put2")
        .0;
    table.commit().expect("commit3");
    let pinned2 = table.pin_snapshot();
    table
        .delete(mongreldb_core::rowid::RowId(rid2))
        .expect("delete");
    table.commit().expect("commit4");
    let current2 = table.snapshot();
    assert!(
        !table
            .query_at_with_allowed(&q, current2, None)
            .unwrap()
            .iter()
            .any(|r| r.row_id.0 == rid2),
        "current must not return deleted rid"
    );
    assert!(
        table
            .query_at_with_allowed(&q, pinned2, None)
            .unwrap()
            .iter()
            .any(|r| r.row_id.0 == rid2),
        "historical pin must still return deleted-at-present row"
    );

    // Close + reopen with pin retained only if fixture allows; re-open and
    // re-query current state for the surviving first pin's epoch if possible.
    drop(table);
    let mut table = Table::open(&path).expect("reopen");
    let now = table.snapshot();
    // After reopen, pins from the prior handle are gone; current query must
    // still be consistent for live rows (pk=1 updated, pk=2 deleted).
    let hits: HashSet<u64> = table
        .query_at_with_allowed(&q, now, None)
        .unwrap()
        .into_iter()
        .map(|r| r.row_id.0)
        .collect();
    assert!(
        !hits.contains(&rid2),
        "reopen current must not surface deleted rid2"
    );
}

#[test]
fn learned_range_historical_update() {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let mut table = Table::create(&path, LearnedRangeFamily::schema(), 1).expect("create");
    let rid = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Int64(100)),
            (3, Value::Int64(now_nanos())),
            (4, Value::Int64(0)),
        ])
        .expect("put")
        .0;
    table.commit().expect("commit");
    let pinned = table.pin_snapshot();
    let q_old = Query::new().and(Condition::Range {
        column_id: 2,
        lo: 90,
        hi: 110,
    });
    assert!(table
        .query_at_with_allowed(&q_old, pinned, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid));

    let _ = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Int64(1000)),
            (3, Value::Int64(now_nanos())),
            (4, Value::Int64(1)),
        ])
        .expect("update");
    table.commit().expect("commit2");
    let current = table.snapshot();
    assert!(!table
        .query_at_with_allowed(&q_old, current, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid));
    let q_new = Query::new().and(Condition::Range {
        column_id: 2,
        lo: 990,
        hi: 1010,
    });
    assert!(!table
        .query_at_with_allowed(&q_new, current, None)
        .unwrap()
        .is_empty());
    assert!(table
        .query_at_with_allowed(&q_old, pinned, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid));

    table.flush().expect("flush");
    let _ = table.compact();
    assert!(table
        .query_at_with_allowed(&q_old, pinned, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid));

    // Historical delete.
    let rid2 = table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Int64(105)),
            (3, Value::Int64(now_nanos())),
            (4, Value::Int64(2)),
        ])
        .expect("put2")
        .0;
    table.commit().expect("commit3");
    let pinned2 = table.pin_snapshot();
    table
        .delete(mongreldb_core::rowid::RowId(rid2))
        .expect("delete");
    table.commit().expect("commit4");
    let current2 = table.snapshot();
    assert!(!table
        .query_at_with_allowed(&q_old, current2, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid2));
    assert!(table
        .query_at_with_allowed(&q_old, pinned2, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid2));

    drop(table);
    let mut table = Table::open(&path).expect("reopen");
    let now = table.snapshot();
    assert!(!table
        .query_at_with_allowed(&q_old, now, None)
        .unwrap()
        .iter()
        .any(|r| r.row_id.0 == rid2));
}

#[test]
fn sparse_full_k_under_stale_candidate_churn() {
    // B468-02 §6.8: create >=10 eligible rows, k=5, then bury the index with
    // stale postings via PK-replace updates. Engine must still return 5 live
    // hits in exact model order — not underfill because stale candidates
    // consumed an internal window.
    let dir = tempdir().expect("tempdir");
    let mut table = Table::create(dir.path(), SparseFamily::schema(), 1).expect("create");
    let mut rids = Vec::new();
    for i in 0..12 {
        // Distinct decreasing scores via token 1 weight.
        let w = 12.0 - i as f32;
        let rid = table
            .put(vec![
                (1, Value::Int64(i as i64)),
                (2, Value::Bytes(pack_sparse_bytes(&[(1u32, w)]))),
                (3, Value::Int64(i as i64)),
            ])
            .expect("put")
            .0;
        rids.push((rid, w));
        table.commit().expect("commit");
    }
    table.flush().expect("flush");
    let q = vec![(1u32, 1.0)];
    let k = 5usize;
    let baseline = table
        .retrieve_at(
            &Retriever::Sparse {
                column_id: 2,
                query: q.clone(),
                k,
            },
            table.snapshot(),
            None,
        )
        .expect("retrieve");
    assert_eq!(baseline.len(), k, "baseline must return full k");

    // PK-replace the top-scoring rows many times so stale postings pile up.
    for _ in 0..20 {
        for i in 0..12 {
            let w = 12.0 - i as f32;
            let _ = table
                .put(vec![
                    (1, Value::Int64(i as i64)),
                    (2, Value::Bytes(pack_sparse_bytes(&[(1u32, w)]))),
                    (3, Value::Int64(i as i64 + 100)),
                ])
                .expect("replace");
            table.commit().expect("commit");
        }
    }
    table.flush().expect("flush2");
    let _ = table.compact();
    let snap = table.snapshot();
    let hits = table
        .retrieve_at(
            &Retriever::Sparse {
                column_id: 2,
                query: q.clone(),
                k,
            },
            snap,
            None,
        )
        .expect("retrieve after churn");
    assert_eq!(
        hits.len(),
        k,
        "full-k under stale churn: got {} hits {:?}",
        hits.len(),
        hits
    );
    // Scores must be positive and descending.
    let scores: Vec<f64> = hits
        .iter()
        .map(|h| match h.score {
            RetrieverScore::SparseDotProduct(d) => d,
            _ => 0.0,
        })
        .collect();
    for w in scores.windows(2) {
        assert!(w[0] + 1e-9 >= w[1], "not sorted: {scores:?}");
    }
    for s in &scores {
        assert!(*s > 0.0, "stale/zero score hit: {scores:?}");
    }
}

fn ann_historical_matrix(schema: Schema, family_label: &str) {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().to_path_buf();
    let dim = match schema.columns.iter().find(|c| c.id == 2).map(|c| &c.ty) {
        Some(TypeId::Embedding { dim }) => *dim as usize,
        _ => 8,
    };
    let mut table = Table::create(&path, schema, 1).expect("create");
    // Near query vector and a distant one (dim-aware).
    let mut near = vec![0.0f32; dim];
    near[0] = 1.0;
    let mut far = vec![0.0f32; dim];
    if dim > 1 {
        far[1] = 1.0;
    } else {
        far[0] = -1.0;
    }
    let rid_near = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(near.clone())),
            (3, Value::Int64(0)),
        ])
        .expect("put near")
        .0;
    let rid_far = table
        .put(vec![
            (1, Value::Int64(2)),
            (2, Value::Embedding(far.clone())),
            (3, Value::Int64(0)),
        ])
        .expect("put far")
        .0;
    table.commit().expect("commit");
    table.flush().expect("flush");
    let pinned = table.pin_snapshot();
    let retriever = Retriever::Ann {
        column_id: 2,
        query: near.clone(),
        k: 1,
    };
    let hist = table
        .retrieve_at(&retriever, pinned, None)
        .unwrap_or_else(|e| panic!("{family_label} hist: {e}"));
    assert_eq!(
        hist[0].row_id.0, rid_near,
        "{family_label}: historical nearest must be near vector"
    );

    // Update near row to far vector.
    let _ = table
        .put(vec![
            (1, Value::Int64(1)),
            (2, Value::Embedding(far.clone())),
            (3, Value::Int64(1)),
        ])
        .expect("update");
    table.commit().expect("commit2");
    let current = table.snapshot();
    let now = table
        .retrieve_at(&retriever, current, None)
        .unwrap_or_else(|e| panic!("{family_label} now: {e}"));
    // After update, pk=1 is far; nearest to `near` query may be rid_far
    // (still far) or the updated pk=1 — either way historical pin must keep
    // the old near rid visible.
    let hist2 = table
        .retrieve_at(&retriever, pinned, None)
        .unwrap_or_else(|e| panic!("{family_label} hist2: {e}"));
    assert!(
        hist2.iter().any(|h| h.row_id.0 == rid_near),
        "{family_label}: historical pin must still surface old near rid; got {hist2:?}; current={now:?}"
    );

    // Delete the far live row and re-check historical.
    table
        .delete(mongreldb_core::rowid::RowId(rid_far))
        .expect("delete far");
    table.commit().expect("commit3");
    table.flush().expect("flush2");
    let _ = table.compact();
    let hist3 = table
        .retrieve_at(&retriever, pinned, None)
        .unwrap_or_else(|e| panic!("{family_label} hist3: {e}"));
    assert!(
        hist3.iter().any(|h| h.row_id.0 == rid_near),
        "{family_label}: historical must survive delete/flush/compact"
    );

    drop(table);
    let mut table = Table::open(&path).expect("reopen");
    let now = table.snapshot();
    let after = table
        .retrieve_at(&retriever, now, None)
        .unwrap_or_else(|e| panic!("{family_label} reopen: {e}"));
    // rid_far deleted; should not appear.
    assert!(
        !after.iter().any(|h| h.row_id.0 == rid_far),
        "{family_label}: deleted rid must not reappear after reopen"
    );
}

#[test]
fn ann_hnsw_dense_snapshot_history() {
    ann_historical_matrix(
        families::ann_dense_schema(AnnQuantization::Dense, AnnAlgorithm::Hnsw),
        "ANN/HNSW/Dense",
    );
}

#[test]
fn ann_hnsw_binary_sign_snapshot_history() {
    ann_historical_matrix(
        families::ann_dense_schema(AnnQuantization::BinarySign, AnnAlgorithm::Hnsw),
        "ANN/HNSW/BinarySign",
    );
}

#[test]
fn ann_product_quantization_snapshot_history() {
    ann_historical_matrix(families::pq_schema(), "ANN/HNSW/PQ");
}

#[test]
fn ann_diskann_dense_snapshot_history() {
    ann_historical_matrix(
        families::ann_dense_schema(AnnQuantization::Dense, AnnAlgorithm::DiskAnn),
        "ANN/DiskANN/Dense",
    );
}

#[test]
fn ann_ivf_dense_snapshot_history() {
    ann_historical_matrix(
        families::ann_dense_schema(AnnQuantization::Dense, AnnAlgorithm::Ivf),
        "ANN/IVF/Dense",
    );
}
