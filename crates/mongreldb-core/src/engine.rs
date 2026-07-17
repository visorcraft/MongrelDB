//! The engine tying the write and read paths together.
//!
//! Sub-ms writes: [`Table::put`] appends to the WAL **without fsyncing**, upserts
//! the skip-list memtable, and updates the in-memory HOT index + secondary
//! indexes. A batch-driven [`Table::commit`] does the group `fsync` and bumps the
//! epoch. [`Table::flush`] commits, drains the memtable into an immutable sorted
//! run, and rotates the WAL. Reads merge versions across the live memtable and
//! all sorted runs ([`Table::get`], [`Table::visible_rows`]).

use crate::columnar;
use crate::cursor::NativePageCursor;
use crate::encryption::Kek;
use crate::encryption::DEK_LEN;
use crate::epoch::{Epoch, EpochAuthority, EpochGuard, MaintenanceReceipt, Snapshot};
use crate::global_idx;
use crate::index::{
    AnnIndex, BitmapIndex, ColumnLearnedRange, FmIndex, HotIndex, MinHashIndex, SparseIndex,
};
use crate::manifest::{self, Manifest, RunRef, TtlPolicy};
use crate::memtable::{Memtable, Row, Value};
use crate::mutable_run::MutableRun;
use crate::row_id_set::RowIdSet;
use crate::rowid::{RowId, RowIdAllocator};
use crate::schema::{AlterColumn, ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
use crate::sorted_run::{RunReader, RunVisibleVersion, RunVisibleVersionCursor, RunWriter};
use crate::txn::{GroupCommit, OwnedRow};
use crate::wal::{Op, SharedWal, Wal};
use crate::{MongrelError, Result};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use zeroize::Zeroizing;

pub const WAL_DIR: &str = "_wal";
pub const RUNS_DIR: &str = "_runs";
pub const CACHE_DIR: &str = "_cache";
pub const META_DIR: &str = "_meta";
pub const RCACHE_DIR: &str = "_rcache";
pub const KEYS_FILENAME: &str = "keys";
pub const SCHEMA_FILENAME: &str = "schema.json";

fn derive_next_run_id(
    dir: &Path,
    runs_root: Option<&crate::durable_file::DurableRoot>,
    active: &[RunRef],
    retiring: &[crate::manifest::RetiredRun],
) -> Result<u64> {
    let mut maximum = 0_u64;
    for run_id in active
        .iter()
        .map(|run| run.run_id)
        .chain(retiring.iter().map(|run| run.run_id))
    {
        let run_id = u64::try_from(run_id)
            .map_err(|_| MongrelError::Full("run-id namespace exhausted".into()))?;
        maximum = maximum.max(run_id);
    }
    let names = match runs_root {
        Some(root) => root.list_regular_files(".")?,
        None => std::fs::read_dir(dir.join(RUNS_DIR))?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?,
    };
    for name in names {
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(digits) = name
            .strip_prefix("r-")
            .and_then(|name| name.strip_suffix(".sr"))
        else {
            continue;
        };
        let Ok(run_id) = digits.parse::<u64>() else {
            continue;
        };
        if name == format!("r-{run_id}.sr") {
            maximum = maximum.max(run_id);
        }
    }
    maximum
        .checked_add(1)
        .map(|next| next.max(1))
        .ok_or_else(|| MongrelError::Full("run-id namespace exhausted".into()))
}

enum ControlledVisibleCandidate {
    Memory(Row),
    Run(RunVisibleVersion),
}

impl ControlledVisibleCandidate {
    fn row_id(&self) -> RowId {
        match self {
            Self::Memory(row) => row.row_id,
            Self::Run(version) => version.row_id,
        }
    }

    fn committed_epoch(&self) -> Epoch {
        match self {
            Self::Memory(row) => row.committed_epoch,
            Self::Run(version) => version.committed_epoch,
        }
    }

    fn deleted(&self) -> bool {
        match self {
            Self::Memory(row) => row.deleted,
            Self::Run(version) => version.deleted,
        }
    }
}

enum ControlledVisibleCursor {
    Memory(std::vec::IntoIter<Row>),
    Run(Box<RunVisibleVersionCursor>),
    #[cfg(test)]
    Synthetic {
        next: u64,
        end: u64,
    },
}

struct ControlledVisibleSource {
    cursor: ControlledVisibleCursor,
    current: Option<ControlledVisibleCandidate>,
}

impl ControlledVisibleSource {
    fn memory(rows: Vec<Row>) -> Self {
        Self {
            cursor: ControlledVisibleCursor::Memory(rows.into_iter()),
            current: None,
        }
    }

    fn run(cursor: RunVisibleVersionCursor) -> Self {
        Self {
            cursor: ControlledVisibleCursor::Run(Box::new(cursor)),
            current: None,
        }
    }

    #[cfg(test)]
    fn synthetic(end: u64) -> Self {
        Self {
            cursor: ControlledVisibleCursor::Synthetic { next: 1, end },
            current: None,
        }
    }

    fn advance(&mut self, control: &crate::ExecutionControl) -> Result<()> {
        self.current = match &mut self.cursor {
            ControlledVisibleCursor::Memory(rows) => {
                rows.next().map(ControlledVisibleCandidate::Memory)
            }
            ControlledVisibleCursor::Run(cursor) => cursor
                .next_visible_version(control)?
                .map(ControlledVisibleCandidate::Run),
            #[cfg(test)]
            ControlledVisibleCursor::Synthetic { next, end } => {
                if *next > *end {
                    None
                } else {
                    let row = Row::new(RowId(*next), Epoch(1));
                    *next += 1;
                    Some(ControlledVisibleCandidate::Memory(row))
                }
            }
        };
        Ok(())
    }

    fn pop(&mut self, control: &crate::ExecutionControl) -> Result<ControlledVisibleCandidate> {
        let current = self.current.take().ok_or_else(|| {
            MongrelError::Other("controlled visible source was not primed".into())
        })?;
        self.advance(control)?;
        Ok(current)
    }

    fn materialize(
        &mut self,
        candidate: ControlledVisibleCandidate,
        control: &crate::ExecutionControl,
    ) -> Result<Row> {
        match candidate {
            ControlledVisibleCandidate::Memory(row) => Ok(row),
            ControlledVisibleCandidate::Run(version) => match &mut self.cursor {
                ControlledVisibleCursor::Run(cursor) => cursor.materialize(version, control),
                _ => Err(MongrelError::Other(
                    "run candidate escaped its controlled cursor".into(),
                )),
            },
        }
    }
}

fn merge_controlled_visible_sources(
    sources: &mut [ControlledVisibleSource],
    control: &crate::ExecutionControl,
    mut expired: impl FnMut(&Row) -> bool,
    mut visit: impl FnMut(Row) -> Result<()>,
) -> Result<()> {
    let mut heap = BinaryHeap::new();
    for (source_index, source) in sources.iter_mut().enumerate() {
        source.advance(control)?;
        if let Some(candidate) = &source.current {
            heap.push(Reverse((candidate.row_id(), source_index)));
        }
    }
    let mut merged = 0_usize;
    while let Some(Reverse((row_id, source_index))) = heap.pop() {
        if merged.is_multiple_of(256) {
            control.checkpoint()?;
        }
        merged += 1;
        let mut best_source = source_index;
        let mut best = sources[source_index].pop(control)?;
        if let Some(next) = &sources[source_index].current {
            heap.push(Reverse((next.row_id(), source_index)));
        }
        while heap
            .peek()
            .is_some_and(|Reverse((candidate, _))| *candidate == row_id)
        {
            let Some(Reverse((_, source_index))) = heap.pop() else {
                break;
            };
            let candidate = sources[source_index].pop(control)?;
            if candidate.committed_epoch() > best.committed_epoch() {
                best = candidate;
                best_source = source_index;
            }
            if let Some(next) = &sources[source_index].current {
                heap.push(Reverse((next.row_id(), source_index)));
            }
        }
        if best.deleted() {
            continue;
        }
        let row = sources[best_source].materialize(best, control)?;
        if !expired(&row) {
            visit(row)?;
        }
    }
    control.checkpoint()
}

#[cfg(test)]
mod controlled_visible_cursor_tests {
    use super::*;

    #[test]
    fn streams_more_than_one_million_rows_without_a_source_cap() {
        let control = crate::ExecutionControl::new(None);
        let mut sources = vec![ControlledVisibleSource::synthetic(1_000_001)];
        let mut count = 0_u64;
        let mut last = 0_u64;
        merge_controlled_visible_sources(
            &mut sources,
            &control,
            |_| false,
            |row| {
                count += 1;
                assert!(row.row_id.0 > last);
                last = row.row_id.0;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(count, 1_000_001);
        assert_eq!(last, 1_000_001);
    }

    #[test]
    fn merge_orders_rows_and_honors_newest_tombstones() {
        let control = crate::ExecutionControl::new(None);
        let older = vec![
            Row::new(RowId(1), Epoch(1)),
            Row::new(RowId(2), Epoch(1)).with_column(1, Value::Int64(20)),
            Row::new(RowId(4), Epoch(1)),
        ];
        let mut deleted = Row::new(RowId(1), Epoch(2));
        deleted.deleted = true;
        let newer = vec![
            deleted,
            Row::new(RowId(2), Epoch(2)).with_column(1, Value::Int64(22)),
            Row::new(RowId(3), Epoch(2)),
        ];
        let mut sources = vec![
            ControlledVisibleSource::memory(older),
            ControlledVisibleSource::memory(newer),
        ];
        let mut rows = Vec::new();
        merge_controlled_visible_sources(
            &mut sources,
            &control,
            |_| false,
            |row| {
                rows.push(row);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.row_id.0).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(rows[0].columns.get(&1), Some(&Value::Int64(22)));
    }
}

/// Current UTC time as an ISO-8601 string in bytes (e.g. `b"2024-07-07T14:30:00Z"`).
/// Used by `DefaultExpr::Now` at stage time.
fn iso_now_bytes() -> Vec<u8> {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (hour, minute, second) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z").into_bytes()
}

pub(crate) fn unix_nanos_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

fn ann_candidate_cap(
    index_len: usize,
    context: Option<&crate::query::AiExecutionContext>,
) -> usize {
    index_len
        .min(crate::query::MAX_RAW_INDEX_CANDIDATES)
        .min(context.map_or(
            crate::query::MAX_RAW_INDEX_CANDIDATES,
            crate::query::AiExecutionContext::max_fused_candidates,
        ))
}

#[cfg(test)]
mod ann_candidate_cap_tests {
    use super::*;

    #[test]
    fn raw_and_request_candidate_ceilings_are_both_hard_bounds() {
        assert_eq!(
            ann_candidate_cap(crate::query::MAX_RAW_INDEX_CANDIDATES + 1, None),
            crate::query::MAX_RAW_INDEX_CANDIDATES,
        );
        let context = crate::query::AiExecutionContext::with_limits(
            std::time::Duration::from_secs(1),
            usize::MAX,
            17,
        );
        assert_eq!(ann_candidate_cap(1_000_000, Some(&context)), 17);
    }
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

const DEFAULT_SYNC_BYTE_THRESHOLD: u64 = 0; // manual commit only (pure group commit)
pub(crate) const PAGE_CACHE_CAPACITY: u64 = 64 * 1024 * 1024; // 64 MiB shared page cache
pub(crate) const DECODED_CACHE_CAPACITY: u64 = 64 * 1024 * 1024; // 64 MiB shared decoded-page cache (Phase 15.4)
/// Default byte watermark at which the PMA mutable-run tier spills to an
/// immutable `.sr` sorted run (Phase 11.1). Coalesces many small flushes into
/// one larger run so the read path merges fewer readers.
const DEFAULT_MUTABLE_RUN_SPILL_BYTES: u64 = 8 * 1024 * 1024;

/// Engine-managed `AUTO_INCREMENT` counter state for a table (present iff the
/// schema declares an `AUTO_INCREMENT` primary key).
///
/// `next` is the next value to hand out (1-based, monotonic, never reused). It
/// is `0` while *unseeded* — the counter has never been advanced (fresh table or
/// a legacy manifest predating `auto_inc_next`). When `seeded` is `false` the
/// first allocation scans `max(PK)` over all visible rows so the counter never
/// collides with pre-existing rows; a value of `0` after seeding never happens
/// (ids are never 0). The manifest persists `next` only when `seeded`, so a
/// reopen that reads `auto_inc_next > 0` is authoritative.
///
/// `seeded == false` but `next > 0` is a transient recovery-only state: WAL
/// replay may bump `next` past replayed ids without marking it seeded, so the
/// scan still runs to cover rows that were already flushed to sorted runs.
#[derive(Clone, Copy, Debug)]
struct AutoIncState {
    column_id: u16,
    next: i64,
    seeded: bool,
}

pub(crate) struct RecoveryMetadataPlan {
    live_count: u64,
    auto_inc: Option<AutoIncState>,
    changed: bool,
}

type FilledAutoIncRow = (Vec<(u16, Value)>, Option<i64>);

/// Resolve the auto-increment column (if any) from a schema into initial
/// counter state. Always called after [`crate::schema::Schema::validate_auto_increment`].
fn resolve_auto_inc(schema: &Schema) -> Option<AutoIncState> {
    schema.auto_increment_column().map(|c| AutoIncState {
        column_id: c.id,
        next: 0,
        seeded: false,
    })
}

/// When a bulk load (`bulk_load` / `bulk_load_columns` / `bulk_load_fast`)
/// builds the live in-memory indexes.
///
/// The engine is correct under either policy: with [`Self::Deferred`] the
/// indexes are rebuilt lazily by the first `query`/`flush` (Phase 14.7,
/// `ensure_indexes_complete`), with [`Self::Eager`] they are built — and
/// checkpointed to `_idx/global.idx` — inside the bulk load itself. The trade
/// is *where* the build cost lands: `Deferred` keeps the ingest critical path
/// minimal (write the run, persist the manifest, return); `Eager` gives
/// predictable first-query latency at the price of a slower load. Serving
/// deployments that load then immediately serve point queries (e.g. a warm
/// daemon) may prefer `Eager`; batch/ETL ingest wants `Deferred`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexBuildPolicy {
    /// Defer index building to the first query/flush — fastest ingest (default).
    #[default]
    Deferred,
    /// Build and checkpoint indexes inside the bulk load — fastest first query.
    Eager,
}

#[derive(Clone)]
struct ReversePkSegment {
    values: HashMap<RowId, Vec<u8>>,
    removed: HashSet<RowId>,
}

#[derive(Clone)]
struct ReversePkMap {
    frozen: Arc<Vec<Arc<ReversePkSegment>>>,
    active: ReversePkSegment,
}

impl ReversePkMap {
    fn new() -> Self {
        Self {
            frozen: Arc::new(Vec::new()),
            active: ReversePkSegment {
                values: HashMap::new(),
                removed: HashSet::new(),
            },
        }
    }

    fn from_entries(entries: impl IntoIterator<Item = (RowId, Vec<u8>)>) -> Self {
        let mut map = Self::new();
        map.active.values.extend(entries);
        map
    }

    fn insert(&mut self, row_id: RowId, key: Vec<u8>) {
        self.active.removed.remove(&row_id);
        self.active.values.insert(row_id, key);
    }

    fn get(&self, row_id: &RowId) -> Option<&Vec<u8>> {
        if let Some(key) = self.active.values.get(row_id) {
            return Some(key);
        }
        if self.active.removed.contains(row_id) {
            return None;
        }
        for segment in self.frozen.iter().rev() {
            if let Some(key) = segment.values.get(row_id) {
                return Some(key);
            }
            if segment.removed.contains(row_id) {
                return None;
            }
        }
        None
    }

    fn remove(&mut self, row_id: &RowId) -> Option<Vec<u8>> {
        let previous = self.get(row_id).cloned();
        self.active.values.remove(row_id);
        self.active.removed.insert(*row_id);
        previous
    }

    fn clear(&mut self) {
        *self = Self::new();
    }

    fn entries(&self) -> HashMap<RowId, Vec<u8>> {
        let mut entries = HashMap::new();
        for segment in self
            .frozen
            .iter()
            .map(Arc::as_ref)
            .chain(std::iter::once(&self.active))
        {
            for row_id in &segment.removed {
                entries.remove(row_id);
            }
            entries.extend(
                segment
                    .values
                    .iter()
                    .map(|(row_id, key)| (*row_id, key.clone())),
            );
        }
        entries
    }

    fn seal(&mut self) {
        if self.active.values.is_empty() && self.active.removed.is_empty() {
            return;
        }
        let active = std::mem::replace(
            &mut self.active,
            ReversePkSegment {
                values: HashMap::new(),
                removed: HashSet::new(),
            },
        );
        Arc::make_mut(&mut self.frozen).push(Arc::new(active));
        if self.frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
            self.frozen = Arc::new(vec![Arc::new(ReversePkSegment {
                values: self.entries(),
                removed: HashSet::new(),
            })]);
        }
    }
}

/// An open MongrelDB table.
#[derive(Clone)]
pub struct Table {
    dir: PathBuf,
    _root_guard: Option<Arc<crate::durable_file::DurableRoot>>,
    runs_root: Option<Arc<crate::durable_file::DurableRoot>>,
    idx_root: Option<Arc<crate::durable_file::DurableRoot>>,
    table_id: u64,
    /// The table's catalog name, set at mount time. Used by the auth
    /// enforcement layer to check `Select`/`Insert`/`Update`/`Delete`
    /// permissions against this specific table.
    name: String,
    /// Optional auth checker for per-operation enforcement. `None` on
    /// credentialless databases (the default); `Some` when the database has
    /// `require_auth = true`. The checker is shared (via `Arc`) so it sees
    /// live updates to the principal and the `require_auth` flag.
    auth: Option<Arc<dyn crate::auth_state::TableAuthChecker>>,
    /// Logical writes are forbidden when this table belongs to a replication
    /// follower. Replication itself appends through the database WAL API.
    read_only: bool,
    /// A WAL commit reached durable storage but its live publication failed.
    /// Reads may continue for diagnostics, but writes require a clean reopen so
    /// recovery can rebuild one coherent runtime state from the durable WAL.
    durable_commit_failed: bool,
    wal: WalSink,
    memtable: Memtable,
    /// PMA-backed mutable-run LSM tier (Phase 11.1). A flush drains the
    /// memtable into this in-memory sorted tier instead of immediately writing
    /// a `.sr` run; once it crosses `mutable_run_spill_bytes` it spills to an
    /// immutable run. Purely in-memory — rebuilt from WAL replay on reopen.
    mutable_run: MutableRun,
    /// Byte watermark controlling when `mutable_run` spills to a sorted run.
    mutable_run_spill_bytes: u64,
    /// Zstd compression level for compaction output (Phase 18.1: default 3;
    /// higher = better ratio but slower compaction).
    compaction_zstd_level: i32,
    allocator: RowIdAllocator,
    epoch: Arc<EpochAuthority>,
    /// Table-local content generation used by authorization caches. Unlike the
    /// shared MVCC epoch, unrelated table commits do not change this value.
    data_generation: u64,
    schema: Schema,
    hot: HotIndex,
    /// Table Key-Encryption Key (Argon2id+HKDF from the passphrase). Each run
    /// stores a fresh DEK wrapped by this KEK (see §7). `None` when plaintext.
    kek: Option<Arc<Kek>>,
    /// Per-column indexable-encryption keys + scheme (Phase 10.2) for every
    /// ENCRYPTED_INDEXABLE column, derived deterministically from the KEK so
    /// tokens are identical across runs. Empty when the table is plaintext.
    column_keys: HashMap<u16, ([u8; 32], u8)>,
    run_refs: Vec<RunRef>,
    /// Runs superseded by compaction, kept on disk for snapshot retention until
    /// `gc()` reaps them (spec §6.4). Persisted in the manifest (`retiring`).
    retiring: Vec<crate::manifest::RetiredRun>,
    next_run_id: u64,
    sync_byte_threshold: u64,
    /// Next transaction id to assign to a single-table auto-commit txn
    /// (`put`/`delete` then `commit`). 0 is reserved for [`wal::SYSTEM_TXN_ID`].
    /// The Database transaction layer (P2.5) assigns these globally; the
    /// single-table path uses this local counter.
    current_txn_id: u64,
    /// True after a standalone table appends a private-WAL mutation and until
    /// `commit_private` has durably sealed and published that transaction.
    /// Mounted tables use `pending_rows` / `pending_dels` instead.
    pending_private_mutations: bool,
    bitmap: HashMap<u16, BitmapIndex>,
    ann: HashMap<u16, AnnIndex>,
    fm: HashMap<u16, FmIndex>,
    sparse: HashMap<u16, SparseIndex>,
    minhash: HashMap<u16, MinHashIndex>,
    /// Per-column learned (PGM) range indexes for `IndexKind::LearnedRange`
    /// columns, built from the single sorted run.
    learned_range: Arc<HashMap<u16, ColumnLearnedRange>>,
    /// Reverse primary-key map for HOT cleanup on row-id deletes.
    pk_by_row: ReversePkMap,
    /// Refcounted pinned read snapshots (epoch → count); compaction must not GC
    /// versions an active snapshot still needs.
    pinned: BTreeMap<Epoch, usize>,
    /// Live (non-deleted) row count — maintained incrementally for O(1)
    /// `Table::count()` without a scan.
    pub(crate) live_count: u64,
    /// Uniform reservoir sample of row ids for approximate analytics
    /// (Phase 8.2). Maintained incrementally on insert; repopulated on open.
    reservoir: crate::reservoir::Reservoir,
    /// False when `reservoir` needs a full rebuild from `visible_rows` before
    /// [`Table::approx_aggregate`] can trust it (same lazy pattern as
    /// [`Table::ensure_indexes_complete`]). Open and WAL-replay leave this
    /// false instead of eagerly materializing every row — a full-table scan
    /// no plain insert/update/delete needs — and the first approximate-
    /// aggregate call pays the rebuild, after which `.offer()` calls maintain
    /// it incrementally.
    reservoir_complete: bool,
    /// True once any row has been deleted. The incremental aggregate cache
    /// (Phase 8.3) is only valid for append-only tables, so a single delete
    /// permanently disables incremental maintenance for this table.
    had_deletes: bool,
    /// Incremental aggregate cache (Phase 8.3): caller-supplied key → the
    /// mergeable aggregate state, the row-id watermark it covers, and the
    /// epoch. A re-query after more inserts processes only the delta and merges.
    agg_cache: Arc<HashMap<u64, CachedAgg>>,
    /// The manifest epoch the on-disk `_idx/global.idx` checkpoint covers (0 if
    /// there is no checkpoint). Updated by [`Table::checkpoint_indexes`]; persisted
    /// in the manifest so reopen loads the checkpoint instead of rebuilding.
    global_idx_epoch: u64,
    /// False when the live in-memory indexes are known to be incomplete (e.g.
    /// after [`Table::bulk_load_columns`], which bypasses per-row indexing). A
    /// flush in that state must NOT checkpoint; reopen rebuilds complete indexes
    /// from the runs and resets this to true.
    indexes_complete: bool,
    /// Where bulk loads put the index-build cost (see [`IndexBuildPolicy`]).
    index_build_policy: IndexBuildPolicy,
    /// False when `pk_by_row` may be missing entries for rows present in
    /// `hot`. Fresh tables start false and puts skip the reverse map — pure
    /// ingest never pays for it. The first delete that needs it rebuilds it
    /// from `hot` (the same lazy pattern as `ensure_indexes_complete`), after
    /// which puts maintain it incrementally so a delete-active workload pays
    /// the build exactly once.
    pk_by_row_complete: bool,
    /// Highest epoch whose data is durable in a sorted run (spec §7.1). Recovery
    /// skips replaying WAL records whose commit epoch is `<= flushed_epoch`.
    flushed_epoch: u64,
    /// Shared, MVCC content-addressed page cache (Phase 9.2). Fed by every
    /// `RunReader::read_page` so all readers share raw (decrypted) page bytes.
    page_cache: Arc<crate::cache::Sharded<crate::cache::PageCache>>,
    /// Global snapshot-retention registry shared across all tables in a
    /// `Database`. Single-table direct opens get a private one.
    snapshots: Arc<crate::retention::SnapshotRegistry>,
    /// Cross-table commit serializer (see [`SharedCtx::commit_lock`]).
    commit_lock: Arc<parking_lot::Mutex<()>>,
    /// Shared decoded-page cache (Phase 15.4): the post-decompress/decrypt typed
    /// page, so repeat scans skip decode. Keyed by `(run_id, column_id, page)`.
    decoded_cache: Arc<crate::cache::Sharded<crate::cache::DecodedPageCache>>,
    /// `run_id`s whose on-disk footer checksum has already been verified by a
    /// `RunReader` construction in this process. `.sr` runs are immutable once
    /// written, so re-hashing an already-verified run's full body on every
    /// repeat `open_reader` call (every query, every `remove_hot_for_row`) is
    /// pure waste for a warm/long-lived handle — this cache lets
    /// `read_header_cached` skip straight to the cheap header+footer-magic
    /// check after the first open. Scoped per-`Table` (not shared via
    /// `SharedCtx`) since `run_id` is only unique within one table's own
    /// manifest.
    verified_runs: Arc<parking_lot::Mutex<std::collections::HashSet<u128>>>,
    /// Table-level result cache (Phase 19.1): `canonical_query_key(conditions,
    /// projection, epoch)` → the survivor columns as typed `NativeColumn`s. Shared
    /// by the native `Condition` API and (via `query_cached`) the tool-call path,
    /// which previously had no caching (only the SQL `MongrelSession` cache did).
    /// Hardening (c): epoch is no longer in the key; instead, a `commit()`
    /// invalidates only entries whose footprint or condition-columns intersect
    /// the committed mutations, tracked in `pending_delete_rids` and
    /// `pending_put_cols`.
    result_cache: Arc<parking_lot::Mutex<ResultCache>>,
    /// WAL DEK (for frame-level encryption). None for plaintext tables.
    wal_dek: Option<Zeroizing<[u8; DEK_LEN]>>,
    /// RowIds deleted since the last `commit()` — used by fine-grained cache
    /// invalidation to check footprint intersection.
    pending_delete_rids: roaring::RoaringBitmap,
    /// Column IDs touched by `put`/`put_batch` since the last `commit()` — used
    /// by conservative insert-newly-matches invalidation.
    pending_put_cols: std::collections::HashSet<u16>,
    /// B1/B2: rows staged by `put`/`put_batch` on a mounted (shared-WAL) table
    /// but not yet applied to the memtable. They are re-stamped to the real
    /// assigned epoch in `commit` (never a speculative `visible+1`), so a
    /// concurrent reader can never observe them before their commit epoch.
    /// Always empty on a standalone (private-WAL) table, which applies inline.
    pending_rows: Vec<Row>,
    pending_rows_auto_inc: Vec<bool>,
    /// B1/B2: tombstones staged on a mounted table, applied at the assigned
    /// epoch in `commit` (mirror of `pending_rows`).
    pending_dels: Vec<RowId>,
    /// B1/B2: truncate staged on a mounted table, applied at the assigned epoch
    /// in `commit`; standalone tables also defer the physical clear until after
    /// the private WAL is fsynced.
    pending_truncate: Option<Epoch>,
    /// Engine-managed `AUTO_INCREMENT` counter (`None` for tables without an
    /// auto-increment primary key). See [`AutoIncState`].
    auto_inc: Option<AutoIncState>,
    /// Manifest-backed timestamp retention policy. Its wall-clock cutoff is
    /// evaluated once per read/compaction operation, never cached by epoch.
    ttl: Option<TtlPolicy>,
}

// `Table` is `Sync`: every field is either plain data, an `Arc`, a `Vec`/`HashMap`
// of `Sync` data, or a thread-safe interior-mutability cell (`parking_lot::Mutex`,
// `crossbeam`/`epoch` Arc-shared caches). The only `RefCell`-based type was
// `FmIndex` (lazy rebuild of the BWT), which now uses a `Mutex`, so a `&Table`
// can be safely shared across read threads (concurrent mutation still requires
// the caller's `Mutex<Table>`).
const _: () = {
    const fn assert_sync<T: ?Sized + Sync>() {}
    assert_sync::<Table>();
};

/// A cached query result — either survivor `Row`s (the tool-call/`query` path)
/// or typed survivor columns (the pushdown/`query_columns_native` path). One
/// canonical key maps to exactly one variant (a `query` with no projection vs a
/// `query_columns_native` with a specific projection produce different keys), so
/// there is no representation collision.
enum CachedData {
    Rows(Arc<Vec<Row>>),
    Columns(Arc<Vec<(u16, columnar::NativeColumn)>>),
}

impl CachedData {
    fn approx_bytes(&self) -> u64 {
        match self {
            CachedData::Rows(r) => r.iter().map(|r| r.estimated_bytes()).sum::<u64>(),
            CachedData::Columns(c) => c
                .iter()
                .map(|(_, c)| c.approx_bytes())
                .sum::<u64>()
                .saturating_add(c.len() as u64 * 16),
        }
    }
}

/// A cached entry carrying the survivor `RowId` **footprint** (for precise
/// delete-based invalidation) and the condition column IDs (for conservative
/// insert-based invalidation). Hardening (c).
struct CachedEntry {
    data: CachedData,
    footprint: roaring::RoaringBitmap,
    condition_cols: Vec<u16>,
}

/// Size-bounded **access-order LRU** result cache (Phase 19.1 + hardening (a)).
/// Every `get_*` promotes the key to the back (most-recently-used); eviction
/// pops from the front (least-recently-used) — a true LRU, not FIFO.
///
/// Hardening (b): an optional on-disk persistent tier (`dir = Some(_)`). On a
/// memory miss, the cache tries disk before falling through to re-resolution.
/// On `insert`, the entry is also written to disk atomically (write + fsync +
/// rename). On `invalidate`/`clear`, the matching disk files are deleted. On
/// `Table::open`, existing disk entries are pre-loaded so fine-grained invalidation
/// resumes across restart.
struct ResultCache {
    entries: std::collections::HashMap<u64, CachedEntry>,
    order: std::collections::VecDeque<u64>,
    bytes: u64,
    max_bytes: u64,
    dir: Option<std::path::PathBuf>,
    #[allow(dead_code)]
    cache_dek: Option<Zeroizing<[u8; DEK_LEN]>>,
}

/// Serialised form of a [`CachedEntry`] for the persistent on-disk tier (b).
#[derive(serde::Serialize, serde::Deserialize)]
struct SerializedEntry {
    condition_cols: Vec<u16>,
    footprint_bits: Vec<u32>,
    data: SerializedData,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum SerializedData {
    Rows(Vec<Row>),
    Columns(Vec<(u16, columnar::NativeColumn)>),
}

impl SerializedEntry {
    fn from_entry(entry: &CachedEntry) -> Self {
        let footprint_bits: Vec<u32> = entry.footprint.iter().collect();
        let data = match &entry.data {
            CachedData::Rows(r) => SerializedData::Rows((**r).clone()),
            CachedData::Columns(c) => SerializedData::Columns((**c).clone()),
        };
        Self {
            condition_cols: entry.condition_cols.clone(),
            footprint_bits,
            data,
        }
    }

    fn into_entry(self) -> Option<CachedEntry> {
        let footprint: roaring::RoaringBitmap = self.footprint_bits.into_iter().collect();
        let data = match self.data {
            SerializedData::Rows(r) => CachedData::Rows(Arc::new(r)),
            SerializedData::Columns(c) => {
                // Validate deserialized columns (hardening (b)): reject corrupt
                // data instead of panicking on access.
                if !c.iter().all(|(_, col)| col.validate()) {
                    return None;
                }
                CachedData::Columns(Arc::new(c))
            }
        };
        Some(CachedEntry {
            data,
            footprint,
            condition_cols: self.condition_cols,
        })
    }
}

impl ResultCache {
    const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;

    fn new() -> Self {
        Self::with_max_bytes(Self::DEFAULT_MAX_BYTES)
    }

    fn with_max_bytes(max_bytes: u64) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            order: std::collections::VecDeque::new(),
            bytes: 0,
            max_bytes,
            dir: None,
            cache_dek: None,
        }
    }

    fn with_dir(mut self, dir: std::path::PathBuf) -> Self {
        let _ = std::fs::create_dir_all(&dir);
        self.dir = Some(dir);
        self
    }

    fn with_cache_dek(mut self, dek: Option<Zeroizing<[u8; DEK_LEN]>>) -> Self {
        self.cache_dek = dek;
        self
    }

    fn disk_path(&self, key: u64) -> Option<std::path::PathBuf> {
        self.dir.as_ref().map(|d| d.join(format!("{key:016x}.bin")))
    }

    /// Atomically write `entry` to disk (write + rename). Best-effort: silently
    /// ignores I/O errors (the in-memory cache is authoritative; the cache is
    /// disposable — missing/stale files fall through to re-resolution).
    fn store_to_disk(&self, key: u64, entry: &CachedEntry) {
        let Some(path) = self.disk_path(key) else {
            return;
        };
        let serialized = match bincode::serialize(&SerializedEntry::from_entry(entry)) {
            Ok(s) => s,
            Err(_) => return,
        };
        // Encrypt if a cache DEK is present.
        let on_disk = if let Some(dek) = &self.cache_dek {
            match self.encrypt_cache(&serialized, dek) {
                Some(b) => b,
                None => return,
            }
        } else {
            serialized
        };
        let tmp = path.with_extension("tmp");
        use std::io::Write;
        let write = || -> std::io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&on_disk)?;
            f.flush()?;
            Ok(())
        };
        if write().is_err() {
            let _ = std::fs::remove_file(&tmp);
            return;
        }
        let _ = std::fs::rename(&tmp, &path);
    }

    /// Try loading `key` from disk. Returns `None` on miss or error.
    fn load_from_disk(&self, key: u64) -> Option<CachedEntry> {
        let path = self.disk_path(key)?;
        let bytes = std::fs::read(&path).ok()?;
        let plaintext = if let Some(dek) = &self.cache_dek {
            self.decrypt_cache(&bytes, dek)?
        } else {
            bytes
        };
        let serialized: SerializedEntry = bincode::deserialize(&plaintext).ok()?;
        serialized.into_entry()
    }

    /// Delete the on-disk file for `key` if it exists. Best-effort.
    fn remove_from_disk(&self, key: u64) {
        if let Some(path) = self.disk_path(key) {
            let _ = std::fs::remove_file(&path);
        }
    }

    /// Encrypt cache data: `[nonce: 12B][ciphertext + GCM tag]`.
    #[cfg(feature = "encryption")]
    fn encrypt_cache(&self, plaintext: &[u8], dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        use crate::encryption::Cipher;
        let cipher = crate::encryption::AesCipher::new(&dek[..]).ok()?;
        let mut nonce = [0u8; 12];
        crate::encryption::fill_random(&mut nonce).ok()?;
        let ct = cipher.encrypt_page(&nonce, plaintext).ok()?;
        let mut out = Vec::with_capacity(12 + ct.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ct);
        Some(out)
    }

    #[cfg(not(feature = "encryption"))]
    fn encrypt_cache(&self, _plaintext: &[u8], _dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        None
    }

    /// Decrypt cache data: reads nonce from first 12 bytes.
    #[cfg(feature = "encryption")]
    fn decrypt_cache(&self, bytes: &[u8], dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        use crate::encryption::Cipher;
        if bytes.len() < 28 {
            return None;
        }
        let cipher = crate::encryption::AesCipher::new(&dek[..]).ok()?;
        let nonce: [u8; 12] = bytes[..12].try_into().ok()?;
        let ct = &bytes[12..];
        cipher.decrypt_page(&nonce, ct).ok()
    }

    #[cfg(not(feature = "encryption"))]
    fn decrypt_cache(&self, _bytes: &[u8], _dek: &Zeroizing<[u8; DEK_LEN]>) -> Option<Vec<u8>> {
        None
    }

    /// Scan the cache directory and pre-load all entries into memory. Called
    /// once on `Table::open`. Best-effort: corrupt/unreadable files are deleted.
    fn load_persistent(&mut self) {
        let Some(dir) = self.dir.as_ref().cloned() else {
            return;
        };
        let entries = match std::fs::read_dir(&dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            // Clean up orphan .tmp files from crashed store_to_disk calls.
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("bin") {
                continue;
            }
            let stem = match path.file_stem().and_then(|s| s.to_str()) {
                Some(s) => s,
                None => continue,
            };
            let key = match u64::from_str_radix(stem, 16) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(_) => continue,
            };
            // Decrypt if cache DEK is present.
            let plaintext = if let Some(dek) = &self.cache_dek {
                match self.decrypt_cache(&bytes, dek) {
                    Some(p) => p,
                    None => {
                        let _ = std::fs::remove_file(&path);
                        continue;
                    }
                }
            } else {
                bytes
            };
            match bincode::deserialize::<SerializedEntry>(&plaintext) {
                Ok(serialized) => {
                    if let Some(entry) = serialized.into_entry() {
                        self.bytes = self.bytes.saturating_add(entry.data.approx_bytes());
                        self.entries.insert(key, entry);
                        self.order.push_back(key);
                    } else {
                        let _ = std::fs::remove_file(&path);
                    }
                }
                Err(_) => {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        self.evict();
    }

    fn set_max_bytes(&mut self, max_bytes: u64) {
        self.max_bytes = max_bytes;
        self.evict();
    }

    /// Promote `key` to most-recently-used position (back of the deque).
    fn touch(&mut self, key: u64) {
        self.order.retain(|k| *k != key);
        self.order.push_back(key);
    }

    fn get_rows(&mut self, key: u64) -> Option<Arc<Vec<Row>>> {
        let res = self.entries.get(&key).and_then(|e| match &e.data {
            CachedData::Rows(r) => Some(r.clone()),
            CachedData::Columns(_) => None,
        });
        if res.is_some() {
            self.touch(key);
            return res;
        }
        // Memory miss → try the persistent tier (b).
        if let Some(entry) = self.load_from_disk(key) {
            let res = match &entry.data {
                CachedData::Rows(r) => Some(r.clone()),
                CachedData::Columns(_) => None,
            };
            if res.is_some() {
                let approx = entry.data.approx_bytes();
                self.bytes = self.bytes.saturating_add(approx);
                self.entries.insert(key, entry);
                self.order.push_back(key);
                self.evict();
                return res;
            }
        }
        None
    }

    fn get_columns(&mut self, key: u64) -> Option<Arc<Vec<(u16, columnar::NativeColumn)>>> {
        let res = self.entries.get(&key).and_then(|e| match &e.data {
            CachedData::Columns(c) => Some(c.clone()),
            CachedData::Rows(_) => None,
        });
        if res.is_some() {
            self.touch(key);
            return res;
        }
        // Memory miss → try the persistent tier (b).
        if let Some(entry) = self.load_from_disk(key) {
            let res = match &entry.data {
                CachedData::Columns(c) => Some(c.clone()),
                CachedData::Rows(_) => None,
            };
            if res.is_some() {
                let approx = entry.data.approx_bytes();
                self.bytes = self.bytes.saturating_add(approx);
                self.entries.insert(key, entry);
                self.order.push_back(key);
                self.evict();
                return res;
            }
        }
        None
    }

    fn insert(&mut self, key: u64, entry: CachedEntry) {
        let approx = entry.data.approx_bytes();
        if self.entries.remove(&key).is_some() {
            self.order.retain(|k| *k != key);
            self.bytes = self.entries.values().map(|e| e.data.approx_bytes()).sum();
        }
        // Write to the persistent tier (b) before memory insert.
        self.store_to_disk(key, &entry);
        self.bytes = self.bytes.saturating_add(approx);
        self.entries.insert(key, entry);
        self.order.push_back(key);
        self.evict();
    }

    /// Fine-grained invalidation (hardening (c)). Drop only entries that are
    /// actually affected by the committed mutations:
    /// - **Delete path**: if `delete_rids` intersects an entry's footprint, a
    ///   survivor was deleted → stale. If the footprint is empty (multi-run or
    ///   non-empty memtable — we couldn't resolve it), **any** delete
    ///   conservatively invalidates the entry (correctness over precision).
    /// - **Insert path**: if `put_cols` intersects an entry's `condition_cols`,
    ///   a newly-inserted row might match the query → conservatively stale.
    fn invalidate(
        &mut self,
        delete_rids: &roaring::RoaringBitmap,
        put_cols: &std::collections::HashSet<u16>,
    ) {
        if self.entries.is_empty() {
            return;
        }
        let has_deletes = !delete_rids.is_empty();
        let to_remove: std::collections::HashSet<u64> = self
            .entries
            .iter()
            .filter(|(_, e)| {
                let delete_hit = if e.footprint.is_empty() {
                    has_deletes
                } else {
                    e.footprint.intersection_len(delete_rids) > 0
                };
                let col_hit = e.condition_cols.iter().any(|c| put_cols.contains(c));
                delete_hit || col_hit
            })
            .map(|(&k, _)| k)
            .collect();
        for key in &to_remove {
            if let Some(e) = self.entries.remove(key) {
                self.bytes = self.bytes.saturating_sub(e.data.approx_bytes());
            }
            self.remove_from_disk(*key);
        }
        if !to_remove.is_empty() {
            self.order.retain(|k| !to_remove.contains(k));
        }
    }

    fn clear(&mut self) {
        // Delete all persistent files (b).
        if let Some(dir) = &self.dir {
            if let Ok(entries) = std::fs::read_dir(dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("bin") {
                        let _ = std::fs::remove_file(&path);
                    }
                }
            }
        }
        self.entries.clear();
        self.order.clear();
        self.bytes = 0;
    }

    fn evict(&mut self) {
        while self.bytes > self.max_bytes {
            let Some(k) = self.order.pop_front() else {
                break;
            };
            if let Some(e) = self.entries.remove(&k) {
                self.bytes = self.bytes.saturating_sub(e.data.approx_bytes());
                // Also delete the disk file (hardening (b)): an evicted entry's
                // disk file must not survive, or invalidate() — which only scans
                // in-memory entries — would miss it and allow a stale disk hit.
                self.remove_from_disk(k);
            }
        }
    }
}

/// Derive per-column indexable-encryption keys (Phase 10.2) for every
/// ENCRYPTED_INDEXABLE column from the KEK. Scheme is `OPE_RANGE` if the column
/// has a `LearnedRange` index, else `HMAC_EQ` (equality). Keys are derived
/// deterministically from the KEK so tokens are stable across runs. Empty when
/// the table is plaintext (no KEK).
/// Derive WAL and cache DEKs from the KEK (None when no encryption).
type DekaOpt = Option<Zeroizing<[u8; DEK_LEN]>>;

fn derive_subkeys(kek: Option<&Kek>, _table_id: u64) -> (DekaOpt, DekaOpt) {
    let _ = kek;
    #[cfg(feature = "encryption")]
    {
        if let Some(k) = kek {
            return (
                Some(k.derive_table_wal_key(_table_id)),
                Some(k.derive_cache_key()),
            );
        }
    }
    (None, None)
}

#[cfg(feature = "encryption")]
fn read_table_encryption_salt_root(
    root: &crate::durable_file::DurableRoot,
) -> Result<[u8; crate::encryption::SALT_LEN]> {
    use std::io::Read;

    let mut file = root
        .open_regular(Path::new(META_DIR).join(KEYS_FILENAME))
        .map_err(|error| MongrelError::NotFound(format!("encryption salt file: {error}")))?;
    let length = file.metadata()?.len();
    if length != crate::encryption::SALT_LEN as u64 {
        return Err(MongrelError::InvalidArgument(format!(
            "salt file is {length} bytes, expected {}",
            crate::encryption::SALT_LEN
        )));
    }
    let mut salt = [0_u8; crate::encryption::SALT_LEN];
    file.read_exact(&mut salt)?;
    Ok(salt)
}

/// Create a boxed cipher from a DEK (encryption feature only).
#[cfg(feature = "encryption")]
fn make_cipher(dek: &Zeroizing<[u8; DEK_LEN]>) -> Box<dyn crate::encryption::Cipher> {
    Box::new(crate::encryption::AesCipher::new(&dek[..]).expect("DEK is 32 bytes"))
}

#[cfg(not(feature = "encryption"))]
fn make_cipher(_dek: &Zeroizing<[u8; DEK_LEN]>) -> Box<dyn crate::encryption::Cipher> {
    Box::new(crate::encryption::PlaintextCipher)
}

fn build_column_keys(kek: Option<&Kek>, schema: &Schema) -> HashMap<u16, ([u8; 32], u8)> {
    let Some(kek) = kek else {
        return HashMap::new();
    };
    #[cfg(feature = "encryption")]
    {
        use crate::encryption::{SCHEME_HMAC_EQ, SCHEME_OPE_RANGE};
        schema
            .columns
            .iter()
            .filter(|c| c.flags.contains(ColumnFlags::ENCRYPTED_INDEXABLE))
            .map(|c| {
                let scheme = if schema
                    .indexes
                    .iter()
                    .any(|i| i.column_id == c.id && i.kind == IndexKind::LearnedRange)
                {
                    SCHEME_OPE_RANGE
                } else {
                    SCHEME_HMAC_EQ
                };
                let key: [u8; 32] = *kek.derive_column_key(c.id);
                (c.id, (key, scheme))
            })
            .collect()
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = (kek, schema);
        HashMap::new()
    }
}

/// Shared services injected into every `Table` owned by a `Database`: one epoch
/// authority (single commit clock), one raw-page cache, one decoded-page cache,
/// one snapshot-retention registry, and the DB-wide KEK. A directly-opened
/// single table builds a private `SharedCtx` of its own.
pub(crate) struct SharedCtx {
    pub root_guard: Option<Arc<crate::durable_file::DurableRoot>>,
    pub epoch: Arc<EpochAuthority>,
    pub page_cache: Arc<crate::cache::Sharded<crate::cache::PageCache>>,
    pub decoded_cache: Arc<crate::cache::Sharded<crate::cache::DecodedPageCache>>,
    pub snapshots: Arc<crate::retention::SnapshotRegistry>,
    pub kek: Option<Arc<Kek>>,
    /// Serializes the commit critical section across all tables sharing this
    /// context so the dual-counter's in-order-publish invariant holds: the
    /// assigned ticket is reserved, the WAL fsynced, the manifest persisted,
    /// and `visible` published as one atomic unit. P3 replaces this with the
    /// bounded validate-first sequencer + group commit (overlapping fsync).
    pub commit_lock: Arc<parking_lot::Mutex<()>>,
    /// B1: when `Some`, the table is mounted in a `Database` and routes every
    /// write through the one shared WAL (no private `_wal/` dir is created).
    /// `None` for a directly-opened standalone table, which keeps a private WAL.
    pub shared: Option<SharedWalCtx>,
    /// The table's catalog name (for auth enforcement). `None` on standalone
    /// direct-open tables that have no catalog entry.
    pub table_name: Option<String>,
    /// Auth checker for per-operation enforcement. `None` on credentialless
    /// databases; cloned from the `Database`'s `auth_state` wrapper.
    pub auth: Option<Arc<dyn crate::auth_state::TableAuthChecker>>,
    /// Whether logical writes must be rejected for a replica database.
    pub read_only: bool,
}

/// Handles a mounted table needs to write to the database's single shared WAL
/// (B1): the WAL itself, the group-commit coordinator + poison flag (so a
/// single-table commit honors the same durability/§9.3e semantics as a cross-
/// table txn), and the shared txn-id allocator (so auto-commit ids never alias
/// cross-table ones in the merged log).
#[derive(Clone)]
pub(crate) struct SharedWalCtx {
    pub wal: Arc<parking_lot::Mutex<SharedWal>>,
    pub group: Arc<GroupCommit>,
    pub poisoned: Arc<AtomicBool>,
    pub txn_ids: Arc<parking_lot::Mutex<u64>>,
    pub change_wake: tokio::sync::broadcast::Sender<()>,
}

/// Where a table's WAL records go. A standalone table owns a `Private` WAL; a
/// `Database`-mounted table writes to the one `Shared` WAL (B1).
enum WalSink {
    Private(Wal),
    Shared(SharedWalCtx),
    ReadOnly,
}

impl Clone for WalSink {
    fn clone(&self) -> Self {
        match self {
            Self::Shared(shared) => Self::Shared(shared.clone()),
            Self::Private(_) | Self::ReadOnly => Self::ReadOnly,
        }
    }
}

impl SharedCtx {
    /// Build a fresh private (standalone) context. `cache_dir = Some(_)` enables
    /// on-disk page cache persistence (single-table direct open); `None` keeps
    /// it in-memory (shared across tables in a `Database`).
    pub(crate) fn new(kek: Option<Arc<Kek>>, cache_dir: Option<PathBuf>) -> Self {
        // §5.8: shard the caches to reduce lock contention under parallel
        // rayon scans. The persistent (single-table) path uses 1 shard (no
        // contention) so its on-disk load/spill stays simple.
        let n_shards = if cache_dir.is_some() {
            1
        } else {
            crate::cache::CACHE_SHARDS
        };
        let per_shard = PAGE_CACHE_CAPACITY / n_shards as u64;
        let page_cache = if let Some(d) = cache_dir {
            Arc::new(crate::cache::Sharded::new(1, || {
                crate::cache::PageCache::new(PAGE_CACHE_CAPACITY).with_persistence(d.clone())
            }))
        } else {
            Arc::new(crate::cache::Sharded::new(n_shards, || {
                crate::cache::PageCache::new(per_shard)
            }))
        };
        let decoded_per_shard = DECODED_CACHE_CAPACITY / crate::cache::CACHE_SHARDS as u64;
        let decoded_cache = Arc::new(crate::cache::Sharded::new(
            crate::cache::CACHE_SHARDS,
            || crate::cache::DecodedPageCache::new(decoded_per_shard),
        ));
        Self {
            root_guard: None,
            epoch: Arc::new(EpochAuthority::new(0)),
            page_cache,
            decoded_cache,
            snapshots: Arc::new(crate::retention::SnapshotRegistry::new()),
            kek,
            commit_lock: Arc::new(parking_lot::Mutex::new(())),
            shared: None,
            table_name: None,
            auth: None,
            read_only: false,
        }
    }
}

/// §5.5: estimated per-condition resolution cost for cheap-first conjunction
/// ordering. Lower is resolved first so a selective O(1) index lookup can
/// short-circuit an expensive range/FM/vector scan.
fn condition_cost_rank(c: &crate::query::Condition) -> u8 {
    use crate::query::Condition;
    match c {
        // O(1) index lookups — resolve first.
        Condition::Pk(_)
        | Condition::BitmapEq { .. }
        | Condition::BitmapIn { .. }
        | Condition::BytesPrefix { .. }
        | Condition::IsNull { .. }
        | Condition::IsNotNull { .. } => 0,
        // Page-pruned scan or LSH candidate lookup.
        Condition::Range { .. } | Condition::RangeF64 { .. } | Condition::MinHashSimilar { .. } => {
            1
        }
        // FM locate / vector scans — most expensive, resolve last.
        Condition::FmContains { .. }
        | Condition::FmContainsAll { .. }
        | Condition::Ann { .. }
        | Condition::SparseMatch { .. } => 2,
    }
}

impl Table {
    pub fn create(dir: impl AsRef<Path>, schema: Schema, table_id: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        crate::durable_file::create_directory_all(&dir)?;
        let root = Arc::new(crate::durable_file::DurableRoot::open(&dir)?);
        let pinned = root.io_path()?;
        let mut ctx = SharedCtx::new(None, Some(pinned.join(CACHE_DIR)));
        ctx.root_guard = Some(root);
        Self::create_in(&pinned, schema, table_id, ctx)
    }

    /// Create a new encrypted table, deriving the table Key-Encryption Key
    /// (KEK) from `passphrase` via Argon2id + HKDF (§7). A fresh random salt is
    /// generated and persisted under `_meta/keys` so the same passphrase
    /// recreates the KEK on reopen. Each run gets its own wrapped DEK.
    ///
    /// **Scope (§7):** encryption is *page-granular* — only sorted-run page
    /// payloads are encrypted. The live WAL (`_wal/`) holds rows as plaintext
    /// between `put` and `flush`; call `flush()` (which rotates the WAL) before
    /// treating sensitive data as fully at-rest-protected. Full WAL encryption
    /// is deferred.
    #[cfg(feature = "encryption")]
    pub fn create_encrypted(
        dir: impl AsRef<Path>,
        schema: Schema,
        table_id: u64,
        passphrase: &str,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        crate::durable_file::create_directory_all(&dir)?;
        let root = Arc::new(crate::durable_file::DurableRoot::open(&dir)?);
        root.create_directory_all(META_DIR)?;
        let salt = crate::encryption::random_salt()?;
        root.write_atomic(Path::new(META_DIR).join(KEYS_FILENAME), &salt)?;
        let kek: Arc<Kek> = Arc::new(Kek::derive(passphrase, &salt)?);
        let pinned = root.io_path()?;
        let mut ctx = SharedCtx::new(Some(kek), Some(pinned.join(CACHE_DIR)));
        ctx.root_guard = Some(root);
        Self::create_in(&pinned, schema, table_id, ctx)
    }

    /// Create a new encrypted table using a raw key (e.g. from a key file)
    /// instead of a passphrase. Skips Argon2id — the key must already be
    /// high-entropy (>= 32 bytes of random data). ~0.1ms vs ~50ms for the
    /// passphrase path.
    #[cfg(feature = "encryption")]
    pub fn create_with_key(
        dir: impl AsRef<Path>,
        schema: Schema,
        table_id: u64,
        key: &[u8],
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        crate::durable_file::create_directory_all(&dir)?;
        let root = Arc::new(crate::durable_file::DurableRoot::open(&dir)?);
        root.create_directory_all(META_DIR)?;
        let salt = crate::encryption::random_salt()?;
        root.write_atomic(Path::new(META_DIR).join(KEYS_FILENAME), &salt)?;
        let kek: Arc<Kek> = Arc::new(Kek::from_raw_key(key, &salt)?);
        let pinned = root.io_path()?;
        let mut ctx = SharedCtx::new(Some(kek), Some(pinned.join(CACHE_DIR)));
        ctx.root_guard = Some(root);
        Self::create_in(&pinned, schema, table_id, ctx)
    }

    /// Open an existing encrypted table using a raw key.
    #[cfg(feature = "encryption")]
    pub fn open_with_key(dir: impl AsRef<Path>, key: &[u8]) -> Result<Self> {
        let root = Arc::new(crate::durable_file::DurableRoot::open(dir.as_ref())?);
        let salt = read_table_encryption_salt_root(&root)?;
        let kek = Arc::new(Kek::from_raw_key(key, &salt)?);
        let pinned = root.io_path()?;
        let mut ctx = SharedCtx::new(Some(kek), Some(pinned.join(CACHE_DIR)));
        ctx.root_guard = Some(root);
        Self::open_in(&pinned, ctx)
    }

    pub(crate) fn create_in(
        dir: impl AsRef<Path>,
        schema: Schema,
        table_id: u64,
        ctx: SharedCtx,
    ) -> Result<Self> {
        schema.validate_auto_increment()?;
        schema.validate_defaults()?;
        schema.validate_ai()?;
        for index in &schema.indexes {
            index.validate_options()?;
        }
        let dir = dir.as_ref().to_path_buf();
        let runs_root = match ctx.root_guard.as_ref() {
            Some(root) => Some(Arc::new(root.create_directory_all_pinned(RUNS_DIR)?)),
            None => {
                crate::durable_file::create_directory_all(&dir)?;
                crate::durable_file::create_directory_all(&dir.join(RUNS_DIR))?;
                None
            }
        };
        match ctx.root_guard.as_deref() {
            Some(root) => write_schema_durable(root, &schema)?,
            None => write_schema(&dir, &schema)?,
        }
        let (wal_dek, cache_dek) = derive_subkeys(ctx.kek.as_deref(), table_id);
        // B1: a mounted table routes writes through the shared WAL and never
        // creates its own `_wal/` dir. A standalone table owns a private WAL.
        let (wal, current_txn_id) = match ctx.shared.clone() {
            Some(s) => (WalSink::Shared(s), 0),
            None => {
                let pinned_wal_root = match ctx.root_guard.as_deref() {
                    Some(root) => Some(root.create_directory_all_pinned(WAL_DIR)?),
                    None => None,
                };
                let wal_dir = if let Some(root) = pinned_wal_root.as_ref() {
                    root.io_path()?
                } else {
                    let wal_dir = dir.join(WAL_DIR);
                    crate::durable_file::create_directory_all(&wal_dir)?;
                    wal_dir
                };
                let mut w = if let Some(ref dk) = wal_dek {
                    Wal::create_with_cipher(
                        wal_dir.join("seg-000000.wal"),
                        Epoch(0),
                        Some(make_cipher(dk)),
                        0,
                    )?
                } else {
                    Wal::create(wal_dir.join("seg-000000.wal"), Epoch(0))?
                };
                w.set_sync_byte_threshold(DEFAULT_SYNC_BYTE_THRESHOLD);
                (WalSink::Private(w), 1)
            }
        };
        let mut manifest = Manifest::new(table_id, schema.schema_id);
        // Seal the create-time manifest with the meta DEK so an encrypted table
        // reopens even if no write/flush ever re-persists it (otherwise the
        // reopen's encrypted manifest read fails to authenticate a plaintext
        // blob — see `manifest_meta_dek`).
        let manifest_meta_dek = crate::encryption::meta_dek_for(ctx.kek.as_deref());
        match ctx.root_guard.as_deref() {
            Some(root) => manifest::write_durable(root, &mut manifest, manifest_meta_dek.as_ref())?,
            None => manifest::write_atomic(&dir, &mut manifest, manifest_meta_dek.as_ref())?,
        }
        let (bitmap, ann, fm, sparse, minhash) = empty_indexes(&schema);
        let column_keys = build_column_keys(ctx.kek.as_deref(), &schema);
        let auto_inc = resolve_auto_inc(&schema);
        let rcache_dir = dir.join(RCACHE_DIR);
        Ok(Self {
            dir,
            _root_guard: ctx.root_guard,
            runs_root,
            idx_root: None,
            table_id,
            name: ctx.table_name.unwrap_or_default(),
            auth: ctx.auth,
            read_only: ctx.read_only,
            durable_commit_failed: false,
            wal,
            memtable: Memtable::new(),
            mutable_run: MutableRun::new(),
            mutable_run_spill_bytes: DEFAULT_MUTABLE_RUN_SPILL_BYTES,
            compaction_zstd_level: 3,
            allocator: RowIdAllocator::new(0),
            epoch: ctx.epoch,
            data_generation: 0,
            schema,
            hot: HotIndex::new(),
            kek: ctx.kek,
            column_keys,
            run_refs: Vec::new(),
            retiring: Vec::new(),
            next_run_id: 1,
            sync_byte_threshold: DEFAULT_SYNC_BYTE_THRESHOLD,
            current_txn_id,
            pending_private_mutations: false,
            bitmap,
            ann,
            fm,
            sparse,
            minhash,
            learned_range: Arc::new(HashMap::new()),
            pk_by_row: ReversePkMap::new(),
            pinned: BTreeMap::new(),
            live_count: 0,
            reservoir: crate::reservoir::Reservoir::default(),
            reservoir_complete: true,
            had_deletes: false,
            agg_cache: Arc::new(HashMap::new()),
            global_idx_epoch: 0,
            indexes_complete: true,
            index_build_policy: IndexBuildPolicy::default(),
            pk_by_row_complete: false,
            flushed_epoch: 0,
            page_cache: ctx.page_cache,
            decoded_cache: ctx.decoded_cache,
            verified_runs: Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new())),
            snapshots: ctx.snapshots,
            commit_lock: ctx.commit_lock,
            result_cache: Arc::new(parking_lot::Mutex::new(
                ResultCache::new()
                    .with_dir(rcache_dir)
                    .with_cache_dek(cache_dek.clone()),
            )),
            pending_delete_rids: roaring::RoaringBitmap::new(),
            pending_put_cols: std::collections::HashSet::new(),
            pending_rows: Vec::new(),
            pending_rows_auto_inc: Vec::new(),
            pending_dels: Vec::new(),
            pending_truncate: None,
            wal_dek,
            auto_inc,
            ttl: None,
        })
    }

    /// Open an existing table: load the manifest, replay the active WAL segment
    /// into the memtable, and rebuild the HOT + secondary indexes from the runs
    /// and replayed rows.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let root = Arc::new(crate::durable_file::DurableRoot::open(dir.as_ref())?);
        let pinned = root.io_path()?;
        let mut ctx = SharedCtx::new(None, Some(pinned.join(CACHE_DIR)));
        ctx.root_guard = Some(root);
        Self::open_in(&pinned, ctx)
    }

    /// Open an existing encrypted table. `passphrase` must match the one used at
    /// create time (combined with the persisted salt to re-derive the KEK).
    #[cfg(feature = "encryption")]
    pub fn open_encrypted(dir: impl AsRef<Path>, passphrase: &str) -> Result<Self> {
        let root = Arc::new(crate::durable_file::DurableRoot::open(dir.as_ref())?);
        let salt = read_table_encryption_salt_root(&root)?;
        let kek: Arc<Kek> = Arc::new(Kek::derive(passphrase, &salt)?);
        let pinned = root.io_path()?;
        let mut ctx = SharedCtx::new(Some(kek), Some(pinned.join(CACHE_DIR)));
        ctx.root_guard = Some(root);
        let t = Self::open_in(&pinned, ctx)?;
        Ok(t)
    }

    pub(crate) fn open_in(dir: impl AsRef<Path>, ctx: SharedCtx) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let manifest_meta_dek = crate::encryption::meta_dek_for(ctx.kek.as_deref());
        let mut manifest = match ctx.root_guard.as_ref() {
            Some(root) => manifest::read_durable(root, "", manifest_meta_dek.as_ref())?,
            None => manifest::read(&dir, manifest_meta_dek.as_ref())?,
        };
        let schema: Schema = match ctx.root_guard.as_ref() {
            Some(root) => read_schema_file(root.open_regular(SCHEMA_FILENAME)?)?,
            None => read_schema(&dir)?,
        };
        // A standalone schema change publishes the schema before its matching
        // manifest. If the process dies in that narrow window, the newer,
        // fully validated schema is authoritative and the manifest identity is
        // repaired only after the rest of open has passed preflight. A manifest
        // claiming a schema newer than the durable schema remains corruption.
        let schema_manifest_repair = manifest.schema_id < schema.schema_id;
        let runs_root = match ctx.root_guard.as_ref() {
            Some(root) => Some(Arc::new(root.open_directory(RUNS_DIR)?)),
            None => None,
        };
        let idx_root = match ctx.root_guard.as_ref() {
            Some(root) => match root.open_directory(global_idx::IDX_DIR) {
                Ok(root) => Some(Arc::new(root)),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            },
            None => None,
        };
        schema.validate_auto_increment()?;
        schema.validate_defaults()?;
        schema.validate_ai()?;
        for index in &schema.indexes {
            index.validate_options()?;
        }
        let replay_epoch = Epoch(manifest.current_epoch);
        let (wal_dek, cache_dek) = derive_subkeys(ctx.kek.as_deref(), manifest.table_id);
        let private_replayed = if ctx.shared.is_none() {
            match latest_wal_segment(&dir.join(WAL_DIR))? {
                Some(path) => {
                    let cipher = wal_dek.as_ref().map(|dk| make_cipher(dk));
                    crate::wal::replay_with_cipher(path, cipher)?
                }
                None => Vec::new(),
            }
        } else {
            Vec::new()
        };
        if ctx.shared.is_none() {
            preflight_standalone_open(
                &dir,
                runs_root.as_deref(),
                idx_root.as_deref(),
                &manifest,
                &schema,
                &private_replayed,
                ctx.kek.clone(),
            )?;
        }
        let next_run_id = derive_next_run_id(
            &dir,
            runs_root.as_deref(),
            &manifest.runs,
            &manifest.retiring,
        )?;
        // B1: a mounted table has no private WAL — its committed records live in
        // the shared WAL and are replayed by `Database::recover_shared_wal`. A
        // standalone table replays + reopens its own `_wal/` segment here.
        let (wal, replayed, current_txn_id) = match ctx.shared.clone() {
            Some(s) => (WalSink::Shared(s), Vec::new(), 0),
            None => {
                let replayed = private_replayed;
                // Never truncate the only durable recovery source. Re-encode
                // every valid frame into a synced staging segment, then publish
                // it atomically under the next segment number. A crash before
                // publication leaves the old segment authoritative; a crash
                // afterward finds the complete replacement as the latest WAL.
                let wal_dir = dir.join(WAL_DIR);
                crate::durable_file::create_directory_all(&wal_dir)?;
                let segment = next_wal_segment(&wal_dir)?;
                let segment_no = wal_segment_number(&segment).unwrap_or(0);
                let temporary = wal_dir.join(format!(
                    ".recovery-{}-{}-{segment_no:06}.tmp",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ));
                let mut w = Wal::create_with_cipher(
                    &temporary,
                    replay_epoch,
                    wal_dek.as_ref().map(|dk| make_cipher(dk)),
                    segment_no,
                )?;
                for record in &replayed {
                    w.append_txn(record.txn_id, record.op.clone())?;
                }
                let mut w = w.publish_as(segment)?;
                w.set_sync_byte_threshold(DEFAULT_SYNC_BYTE_THRESHOLD);
                let next_txn_id = replayed
                    .iter()
                    .map(|record| record.txn_id)
                    .filter(|txn_id| *txn_id != crate::wal::SYSTEM_TXN_ID)
                    .max()
                    .map(|txn_id| txn_id.checked_add(1).unwrap_or(0))
                    .unwrap_or(1);
                (WalSink::Private(w), replayed, next_txn_id)
            }
        };

        let mut memtable = Memtable::new();
        let mut allocator = RowIdAllocator::new(manifest.next_row_id);
        let persisted_epoch = manifest.current_epoch;
        // Seed the auto-increment counter from the manifest. `auto_inc_next == 0`
        // means unseeded (fresh table, or a legacy manifest migrated forward) —
        // the first allocation scans `max(PK)` to avoid colliding with existing
        // rows. WAL replay (below) and `recover_apply` additionally bump `next`
        // past replayed ids without marking it seeded, so the scan still covers
        // any rows that were already flushed to sorted runs.
        let mut auto_inc = resolve_auto_inc(&schema).map(|mut s| {
            s.next = manifest.auto_inc_next;
            s.seeded = manifest.auto_inc_next > 0;
            s
        });

        // 1. Replay is two-phase and TxnCommit-gated: data records (Put/Delete)
        //    are staged per `txn_id` and only applied when a durable
        //    `TxnCommit{epoch}` for that txn is seen. Uncommitted / aborted /
        //    torn-tail txns are discarded. Indexing happens AFTER loading any
        //    checkpoint / run data (below) so the newer replayed versions
        //    overwrite the older run versions in the HOT index.
        let mut staged_puts: HashMap<u64, Vec<Row>> = HashMap::new();
        let mut staged_deletes: HashMap<u64, Vec<RowId>> = HashMap::new();
        let mut staged_truncates: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut replayed_puts: std::collections::BTreeMap<Epoch, Vec<Row>> =
            std::collections::BTreeMap::new();
        let mut replayed_deletes: Vec<(RowId, Epoch)> = Vec::new();
        let mut recovered_epoch = manifest.current_epoch;
        let mut recovered_manifest_dirty = schema_manifest_repair;
        let mut saw_delete = false;
        for record in replayed {
            let txn_id = record.txn_id;
            match record.op {
                Op::Put { rows, .. } => {
                    let rows: Vec<Row> = bincode::deserialize(&rows)?;
                    for row in &rows {
                        allocator.advance_to(row.row_id)?;
                        if let Some(ai) = auto_inc.as_mut() {
                            if let Some(Value::Int64(n)) = row.columns.get(&ai.column_id) {
                                let next = n.checked_add(1).ok_or_else(|| {
                                    MongrelError::Full("AUTO_INCREMENT namespace exhausted".into())
                                })?;
                                if next > ai.next {
                                    ai.next = next;
                                }
                            }
                        }
                    }
                    staged_puts.entry(txn_id).or_default().extend(rows);
                }
                Op::Delete { row_ids, .. } => {
                    staged_deletes.entry(txn_id).or_default().extend(row_ids);
                }
                Op::TxnCommit { epoch, .. } => {
                    let commit_epoch = Epoch(epoch);
                    recovered_epoch = recovered_epoch.max(epoch);
                    if staged_truncates.remove(&txn_id) && commit_epoch.0 > manifest.flushed_epoch {
                        memtable = Memtable::new();
                        replayed_puts.clear();
                        replayed_deletes.clear();
                        manifest.runs.clear();
                        manifest.retiring.clear();
                        manifest.live_count = 0;
                        manifest.global_idx_epoch = 0;
                        manifest.current_epoch = manifest.current_epoch.max(epoch);
                        recovered_manifest_dirty = true;
                        saw_delete = true;
                    }
                    if let Some(puts) = staged_puts.remove(&txn_id) {
                        if commit_epoch.0 > manifest.flushed_epoch {
                            for row in &puts {
                                memtable.upsert(row.clone());
                            }
                            replayed_puts.entry(commit_epoch).or_default().extend(puts);
                        }
                    }
                    if let Some(dels) = staged_deletes.remove(&txn_id) {
                        saw_delete = true;
                        if commit_epoch.0 > manifest.flushed_epoch {
                            for rid in dels {
                                memtable.tombstone(rid, commit_epoch);
                                replayed_deletes.push((rid, commit_epoch));
                            }
                        }
                    }
                }
                Op::TxnAbort => {
                    staged_puts.remove(&txn_id);
                    staged_deletes.remove(&txn_id);
                    staged_truncates.remove(&txn_id);
                }
                Op::TruncateTable { .. } => {
                    staged_puts.remove(&txn_id);
                    staged_deletes.remove(&txn_id);
                    staged_truncates.insert(txn_id);
                }
                Op::ExternalTableState { .. }
                | Op::Flush { .. }
                | Op::Ddl(_)
                | Op::BeforeImage { .. }
                | Op::CommitTimestamp { .. }
                | Op::SpilledRows { .. } => {}
            }
        }

        let rcache_dir = dir.join(RCACHE_DIR);
        let column_keys = build_column_keys(ctx.kek.as_deref(), &schema);
        let mut db = Self {
            dir,
            _root_guard: ctx.root_guard,
            runs_root,
            idx_root,
            table_id: manifest.table_id,
            name: ctx.table_name.unwrap_or_default(),
            auth: ctx.auth,
            read_only: ctx.read_only,
            durable_commit_failed: false,
            wal,
            memtable,
            mutable_run: MutableRun::new(),
            mutable_run_spill_bytes: DEFAULT_MUTABLE_RUN_SPILL_BYTES,
            compaction_zstd_level: 3,
            allocator,
            epoch: ctx.epoch,
            data_generation: persisted_epoch,
            schema,
            hot: HotIndex::new(),
            kek: ctx.kek,
            column_keys,
            run_refs: manifest.runs.clone(),
            retiring: manifest.retiring.clone(),
            next_run_id,
            sync_byte_threshold: DEFAULT_SYNC_BYTE_THRESHOLD,
            current_txn_id,
            pending_private_mutations: false,
            bitmap: HashMap::new(),
            ann: HashMap::new(),
            fm: HashMap::new(),
            sparse: HashMap::new(),
            minhash: HashMap::new(),
            learned_range: Arc::new(HashMap::new()),
            pk_by_row: ReversePkMap::new(),
            pinned: BTreeMap::new(),
            live_count: manifest.live_count,
            reservoir: crate::reservoir::Reservoir::default(),
            reservoir_complete: false,
            had_deletes: saw_delete
                || manifest.runs.iter().map(|run| run.row_count).sum::<u64>()
                    != manifest.live_count,
            agg_cache: Arc::new(HashMap::new()),
            global_idx_epoch: manifest.global_idx_epoch,
            indexes_complete: true,
            index_build_policy: IndexBuildPolicy::default(),
            pk_by_row_complete: false,
            flushed_epoch: manifest.flushed_epoch,
            page_cache: ctx.page_cache,
            decoded_cache: ctx.decoded_cache,
            verified_runs: Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new())),
            snapshots: ctx.snapshots,
            commit_lock: ctx.commit_lock,
            result_cache: Arc::new(parking_lot::Mutex::new(
                ResultCache::new()
                    .with_dir(rcache_dir)
                    .with_cache_dek(cache_dek.clone()),
            )),
            pending_delete_rids: roaring::RoaringBitmap::new(),
            pending_put_cols: std::collections::HashSet::new(),
            pending_rows: Vec::new(),
            pending_rows_auto_inc: Vec::new(),
            pending_dels: Vec::new(),
            pending_truncate: None,
            wal_dek,
            auto_inc,
            ttl: manifest.ttl,
        };

        // Advance the (possibly shared) epoch authority to this table's manifest
        // epoch so rebuild/index reads below observe the recovered watermark.
        db.epoch.advance_recovered(Epoch(recovered_epoch));

        // 2. Fast path: load the persisted global-index checkpoint (Phase 9.1).
        //    Valid only when its embedded epoch matches the manifest-endorsed
        //    `global_idx_epoch` and every run was created at or before it, so the
        //    checkpoint covers all run data. Otherwise rebuild from the runs.
        let checkpoint = match db.idx_root.as_deref() {
            Some(root) => {
                global_idx::read_root(root, db.table_id, &db.schema, db.idx_dek().as_deref())?
            }
            None => global_idx::read(&db.dir, db.table_id, &db.schema, db.idx_dek().as_deref())?,
        };
        let checkpoint_valid = checkpoint.as_ref().is_some_and(|c| {
            c.epoch_built == manifest.global_idx_epoch
                && manifest.global_idx_epoch > 0
                && manifest
                    .runs
                    .iter()
                    .all(|r| r.epoch_created <= manifest.global_idx_epoch)
        });
        if let Some(loaded) = checkpoint {
            if checkpoint_valid {
                db.hot = loaded.hot;
                db.bitmap = loaded.bitmap;
                db.ann = loaded.ann;
                db.fm = loaded.fm;
                db.sparse = loaded.sparse;
                db.minhash = loaded.minhash;
                db.learned_range = Arc::new(loaded.learned_range);
                // `pk_by_row` stays lazy (`pk_by_row_complete == false`): the
                // first delete rebuilds it from the loaded HOT.
            }
        }
        if !checkpoint_valid {
            let (bitmap, ann, fm, sparse, minhash) = empty_indexes(&db.schema);
            db.bitmap = bitmap;
            db.ann = ann;
            db.fm = fm;
            db.sparse = sparse;
            db.minhash = minhash;
            db.rebuild_indexes_from_runs()?;
            db.build_learned_ranges()?;
        }

        // 3. Index the replayed WAL rows on top so updates overwrite. Within a
        //    single transaction epoch duplicate PKs are upserted: only the last
        //    winner is indexed, losers are tombstoned in the already-replayed
        //    memtable.
        for (epoch, group) in replayed_puts {
            let (losers, winner_pks) = db.partition_pk_winners(&group);
            for (key, &row_id) in &winner_pks {
                if let Some(old_rid) = db.hot.get(key) {
                    if old_rid != row_id {
                        db.tombstone_row(old_rid, epoch, false);
                    }
                }
            }
            for &loser_rid in &losers {
                db.tombstone_row(loser_rid, epoch, false);
            }
            for (key, row_id) in winner_pks {
                db.insert_hot_pk(key, row_id);
            }
            if db.schema.primary_key().is_none() {
                for r in &group {
                    db.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
                }
            }
            for r in &group {
                if !losers.contains(&r.row_id) {
                    db.index_row(r);
                }
            }
        }
        // Apply replayed deletes after the puts: a delete targets a specific row
        // id and only removes the HOT entry if it still points to that id, so a
        // newer upsert for the same PK is not accidentally erased.
        for (rid, epoch) in &replayed_deletes {
            db.remove_hot_for_row(*rid, *epoch);
        }

        if recovered_manifest_dirty {
            let rows = db.visible_rows(Snapshot::at(Epoch(u64::MAX)))?;
            db.live_count = rows.len() as u64;
            db.persist_manifest(Epoch(recovered_epoch))?;
        }

        // The reservoir stays lazy (`reservoir_complete == false`, set above):
        // rebuilding it means materializing every visible row, which no plain
        // open/insert/update/delete needs. `ensure_reservoir_complete` pays
        // that cost on the first `approx_aggregate` call instead.
        // Load the persistent result-cache tier (hardening (b)) so fine-grained
        // invalidation resumes across restart.
        db.result_cache.lock().load_persistent();
        Ok(db)
    }

    /// Rebuild `reservoir` from every visible row if it isn't already
    /// complete (lazy — same pattern as [`Self::ensure_indexes_complete`]).
    /// Open and WAL replay leave the reservoir stale rather than eagerly
    /// paying a full-table scan; this pays it once, on the first
    /// [`Self::approx_aggregate`] call.
    fn ensure_reservoir_complete(&mut self) -> Result<()> {
        if self.reservoir_complete {
            return Ok(());
        }
        self.rebuild_reservoir()?;
        self.reservoir_complete = true;
        Ok(())
    }

    /// Repopulate the reservoir sample from all visible rows (used on open so a
    /// reopened table has an analytics sample without further inserts).
    fn rebuild_reservoir(&mut self) -> Result<()> {
        let snap = self.snapshot();
        let rows = self.visible_rows(snap)?;
        self.reservoir.reset();
        for r in rows {
            self.reservoir.offer(r.row_id.0);
        }
        Ok(())
    }

    pub(crate) fn rebuild_indexes_from_runs(&mut self) -> Result<()> {
        self.rebuild_indexes_from_runs_inner(None)
    }

    fn rebuild_indexes_from_runs_inner(
        &mut self,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        self.hot = HotIndex::new();
        self.pk_by_row.clear();
        let (bitmap, ann, fm, sparse, minhash) = empty_indexes(&self.schema);
        self.bitmap = bitmap;
        self.ann = ann;
        self.fm = fm;
        self.sparse = sparse;
        self.minhash = minhash;
        let snapshot = Epoch(u64::MAX);
        let ttl_now = unix_nanos_now();
        let mut scanned = 0_usize;
        for rr in self.run_refs.clone() {
            if let Some(control) = control {
                control.checkpoint()?;
            }
            let mut reader = self.open_reader(rr.run_id)?;
            for row in reader.visible_rows(snapshot)? {
                if scanned.is_multiple_of(256) {
                    if let Some(control) = control {
                        control.checkpoint()?;
                    }
                }
                scanned += 1;
                if self.row_expired_at(&row, ttl_now) {
                    continue;
                }
                let tok_row = self.tokenized_for_indexes(&row);
                index_into(
                    &self.schema,
                    &tok_row,
                    &mut self.hot,
                    &mut self.bitmap,
                    &mut self.ann,
                    &mut self.fm,
                    &mut self.sparse,
                    &mut self.minhash,
                );
            }
        }
        for row in self.mutable_run.visible_versions(snapshot) {
            if scanned.is_multiple_of(256) {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
            scanned += 1;
            if row.deleted {
                self.remove_hot_for_row(row.row_id, snapshot);
            } else if !self.row_expired_at(&row, ttl_now) {
                self.index_row(&row);
            }
        }
        for row in self.memtable.visible_versions(snapshot) {
            if scanned.is_multiple_of(256) {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
            scanned += 1;
            if row.deleted {
                self.remove_hot_for_row(row.row_id, snapshot);
            } else if !self.row_expired_at(&row, ttl_now) {
                self.index_row(&row);
            }
        }
        self.refresh_pk_by_row_from_hot();
        Ok(())
    }

    fn refresh_pk_by_row_from_hot(&mut self) {
        self.pk_by_row_complete = true;
        if self.schema.primary_key().is_none() {
            self.pk_by_row.clear();
            return;
        }
        // `.collect()` drives `HashMap`'s bulk-build `FromIterator` (reserves
        // once from the exact-size iterator), instead of growing-and-rehashing
        // through a one-at-a-time `insert()` loop — same fix as
        // `HotIndex::from_entries`, same hot path (first delete after a put
        // streak rebuilds this from the full HOT index).
        self.pk_by_row = ReversePkMap::from_entries(
            self.hot
                .entries()
                .into_iter()
                .map(|(key, row_id)| (row_id, key)),
        );
    }

    fn insert_hot_pk(&mut self, key: Vec<u8>, row_id: RowId) {
        if self.schema.primary_key().is_some() {
            self.pk_by_row.insert(row_id, key.clone());
        }
        self.hot.insert(key, row_id);
    }

    /// (Re)build per-column learned (PGM) range indexes for `LearnedRange`
    /// columns from the single sorted run. Serves `Condition::Range` sub-linearly
    /// on the fast path; no-op when there isn't exactly one run.
    pub(crate) fn build_learned_ranges(&mut self) -> Result<()> {
        self.build_learned_ranges_inner(None)
    }

    fn build_learned_ranges_inner(
        &mut self,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<()> {
        self.learned_range = Arc::new(HashMap::new());
        if self.run_refs.len() != 1 {
            return Ok(());
        }
        let cols: Vec<(u16, usize)> = self
            .schema
            .indexes
            .iter()
            .filter(|i| i.kind == IndexKind::LearnedRange)
            .map(|i| {
                (
                    i.column_id,
                    i.options
                        .learned_range
                        .as_ref()
                        .map(|options| options.epsilon)
                        .unwrap_or(16),
                )
            })
            .collect();
        if cols.is_empty() {
            return Ok(());
        }
        let mut reader = self.open_reader(self.run_refs[0].run_id)?;
        let row_ids: Vec<u64> = match reader.column_native(crate::sorted_run::SYS_ROW_ID)? {
            columnar::NativeColumn::Int64 { data, .. } => data.iter().map(|x| *x as u64).collect(),
            _ => return Ok(()),
        };
        for (column_index, (cid, epsilon)) in cols.into_iter().enumerate() {
            if column_index % 256 == 0 {
                if let Some(control) = control {
                    control.checkpoint()?;
                }
            }
            let ty = self
                .schema
                .columns
                .iter()
                .find(|c| c.id == cid)
                .map(|c| c.ty.clone())
                .unwrap_or(TypeId::Int64);
            match ty {
                TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                    if let columnar::NativeColumn::Int64 { data, .. } = reader.column_native(cid)? {
                        let pairs: Vec<(i64, u64)> = data
                            .iter()
                            .zip(row_ids.iter())
                            .map(|(v, r)| (*v, *r))
                            .collect();
                        Arc::make_mut(&mut self.learned_range).insert(
                            cid,
                            ColumnLearnedRange::build_i64_with_epsilon(&pairs, epsilon),
                        );
                    }
                }
                TypeId::Float64 => {
                    if let columnar::NativeColumn::Float64 { data, .. } =
                        reader.column_native(cid)?
                    {
                        let pairs: Vec<(f64, u64)> = data
                            .iter()
                            .zip(row_ids.iter())
                            .map(|(v, r)| (*v, *r))
                            .collect();
                        Arc::make_mut(&mut self.learned_range).insert(
                            cid,
                            ColumnLearnedRange::build_f64_with_epsilon(&pairs, epsilon),
                        );
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Phase 14.7: if the live indexes are known incomplete (after a bulk
    /// ingest that deferred index building — see [`IndexBuildPolicy`]),
    /// rebuild them from the runs now. Called lazily by `query` /
    /// `query_columns_native` / `flush`; public so external index consumers
    /// (SQL scans, joins, PK point lookups on a shared handle) can pay the
    /// one-time build before reading a `&self` index view.
    pub fn ensure_indexes_complete(&mut self) -> Result<()> {
        if self.indexes_complete {
            crate::trace::QueryTrace::record(|t| {
                t.index_rebuild = crate::trace::IndexRebuild::AlreadyComplete;
            });
            return Ok(());
        }
        crate::trace::QueryTrace::record(|t| {
            t.index_rebuild = crate::trace::IndexRebuild::Rebuilt;
        });
        self.rebuild_indexes_from_runs()?;
        self.build_learned_ranges()?;
        self.indexes_complete = true;
        let epoch = self.current_epoch();
        self.checkpoint_indexes(epoch);
        Ok(())
    }

    /// Rebuild derived indexes cooperatively, publishing their checkpoint only
    /// after `before_publish` succeeds.
    #[doc(hidden)]
    pub fn ensure_indexes_complete_controlled<F>(
        &mut self,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<bool>
    where
        F: FnOnce() -> bool,
    {
        self.ensure_indexes_complete_controlled_with_receipt(control, before_publish)
            .map(|(changed, _)| changed)
    }

    /// Rebuild derived indexes cooperatively and return the exact table
    /// snapshot used by the rebuild. No receipt is returned for a no-op.
    #[doc(hidden)]
    pub fn ensure_indexes_complete_controlled_with_receipt<F>(
        &mut self,
        control: &crate::ExecutionControl,
        before_publish: F,
    ) -> Result<(bool, Option<MaintenanceReceipt>)>
    where
        F: FnOnce() -> bool,
    {
        if self.indexes_complete {
            crate::trace::QueryTrace::record(|trace| {
                trace.index_rebuild = crate::trace::IndexRebuild::AlreadyComplete;
            });
            return Ok((false, None));
        }
        crate::trace::QueryTrace::record(|trace| {
            trace.index_rebuild = crate::trace::IndexRebuild::Rebuilt;
        });
        control.checkpoint()?;
        let maintenance_epoch = self.current_epoch();
        self.rebuild_indexes_from_runs_inner(Some(control))?;
        self.build_learned_ranges_inner(Some(control))?;
        control.checkpoint()?;
        if !before_publish() {
            return Err(MongrelError::Cancelled);
        }
        self.indexes_complete = true;
        self.checkpoint_indexes(maintenance_epoch);
        Ok((
            true,
            Some(MaintenanceReceipt {
                epoch: maintenance_epoch,
            }),
        ))
    }

    fn pending_epoch(&self) -> Epoch {
        Epoch(self.epoch.visible().0 + 1)
    }

    /// True when this table is mounted in a `Database` (writes route through the
    /// shared WAL).
    fn is_shared(&self) -> bool {
        matches!(self.wal, WalSink::Shared(_))
    }

    /// Return the current auto-commit txn id, allocating a fresh one from the
    /// shared allocator on a mounted table when a new span starts (sentinel 0).
    /// A standalone table uses its private monotonic counter (never 0).
    fn ensure_txn_id(&mut self) -> Result<u64> {
        if self.current_txn_id == 0 {
            let id = match &self.wal {
                WalSink::Shared(s) => crate::txn::allocate_txn_id(&s.txn_ids)?,
                WalSink::Private(_) => {
                    return Err(MongrelError::Full(
                        "standalone transaction id namespace exhausted".into(),
                    ))
                }
                WalSink::ReadOnly => return Err(MongrelError::ReadOnlyReplica),
            };
            self.current_txn_id = id;
        }
        Ok(self.current_txn_id)
    }

    /// Append a data record (`Put`/`Delete`) for the current auto-commit txn to
    /// whichever WAL backs this table.
    fn wal_append_data(&mut self, op: Op) -> Result<()> {
        self.ensure_writable()?;
        let txn_id = self.ensure_txn_id()?;
        let table_id = self.table_id;
        match &mut self.wal {
            WalSink::Private(w) => {
                w.append_txn(txn_id, op)?;
                self.pending_private_mutations = true;
            }
            WalSink::Shared(s) => {
                s.wal.lock().append(txn_id, table_id, op)?;
            }
            WalSink::ReadOnly => return Err(MongrelError::ReadOnlyReplica),
        }
        Ok(())
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.read_only || matches!(self.wal, WalSink::ReadOnly) {
            return Err(MongrelError::ReadOnlyReplica);
        }
        if self.durable_commit_failed {
            return Err(MongrelError::Other(
                "table poisoned by post-commit failure; reopen required".into(),
            ));
        }
        Ok(())
    }

    /// Upsert a row. Allocates a [`RowId`], appends a (non-fsynced) WAL record,
    /// and updates the memtable + indexes. Returns the new row id. Durability
    /// arrives at the next [`Table::commit`] (or [`Table::flush`]).
    ///
    /// For an `AUTO_INCREMENT` primary key, omit the column (or pass
    /// Auth enforcement helpers. Each delegates to the optional
    /// [`TableAuthChecker`] (set at mount time from the `Database`'s auth
    /// state). On a credentialless database (`auth = None`), these are
    /// no-ops. The `name` field provides the table name for the permission
    /// check without needing a reference back to `Database`.
    fn require(&self, perm: crate::auth_state::RequiredPermission) -> Result<()> {
        match &self.auth {
            Some(checker) => checker.check(&self.name, perm),
            None => Ok(()),
        }
    }
    /// Check `Select` permission on this table. Public so that read entry
    /// points that don't go through `Table::query` (e.g. `MongrelProvider::scan`,
    /// `Table::count`) can enforce before reading. On a credentialless database
    /// this is a no-op.
    pub fn require_select(&self) -> Result<()> {
        self.require(crate::auth_state::RequiredPermission::Select)
    }
    fn require_insert(&self) -> Result<()> {
        self.require(crate::auth_state::RequiredPermission::Insert)
    }
    /// Currently unused on `Table` directly (updates go through `Transaction`),
    /// but kept for API completeness — the four `require_*` helpers mirror the
    /// four table-level permission kinds.
    #[allow(dead_code)]
    fn require_update(&self) -> Result<()> {
        self.require(crate::auth_state::RequiredPermission::Update)
    }
    fn require_delete(&self) -> Result<()> {
        self.require(crate::auth_state::RequiredPermission::Delete)
    }

    /// [`Value::Null`]) and the engine assigns the next counter value; use
    /// [`Table::put_returning`] to learn that assigned value.
    pub fn put(&mut self, columns: Vec<(u16, Value)>) -> Result<RowId> {
        self.require_insert()?;
        Ok(self.put_returning(columns)?.0)
    }

    /// Like [`Table::put`] but also returns the engine-assigned `AUTO_INCREMENT`
    /// value (`Some` only when the column was omitted/null and the engine filled
    /// it; `None` when the table has no auto-increment column or the caller
    /// supplied an explicit value).
    pub fn put_returning(
        &mut self,
        mut columns: Vec<(u16, Value)>,
    ) -> Result<(RowId, Option<i64>)> {
        self.require_insert()?;
        let assigned = self.fill_auto_inc(&mut columns)?;
        self.apply_defaults(&mut columns)?;
        self.schema.validate_values(&columns)?;
        // For clustered (WITHOUT ROWID) tables, derive RowId deterministically
        // from the PK value so the same PK always maps to the same row (no
        // allocator waste, idempotent upserts). For standard tables, use the
        // monotonic allocator.
        let row_id = if self.schema.clustered {
            self.derive_clustered_row_id(&columns)?
        } else {
            self.allocator.alloc()?
        };
        let epoch = self.pending_epoch();
        let mut row = Row::new(row_id, epoch);
        for (col_id, val) in columns {
            row.columns.insert(col_id, val);
        }
        self.commit_rows(vec![row], assigned.is_some())?;
        Ok((row_id, assigned))
    }

    /// Bulk upsert: many rows under a single WAL record + one index pass. Far
    /// cheaper than `put` in a loop for batch ingest.
    pub fn put_batch(&mut self, batch: Vec<Vec<(u16, Value)>>) -> Result<Vec<RowId>> {
        self.require_insert()?;
        Ok(self
            .put_batch_returning(batch)?
            .into_iter()
            .map(|(r, _)| r)
            .collect())
    }

    /// Like [`Table::put_batch`] but each entry is paired with the engine-
    /// assigned `AUTO_INCREMENT` value (`Some` only when filled by the engine).
    pub fn put_batch_returning(
        &mut self,
        batch: Vec<Vec<(u16, Value)>>,
    ) -> Result<Vec<(RowId, Option<i64>)>> {
        let mut filled: Vec<FilledAutoIncRow> = Vec::with_capacity(batch.len());
        for mut cols in batch {
            let assigned = self.fill_auto_inc(&mut cols)?;
            self.apply_defaults(&mut cols)?;
            filled.push((cols, assigned));
        }
        for (cols, _) in &filled {
            self.schema.validate_values(cols)?;
        }
        let epoch = self.pending_epoch();
        let mut rows = Vec::with_capacity(filled.len());
        let mut ids = Vec::with_capacity(filled.len());
        let first_row_id = if self.schema.clustered {
            None
        } else {
            let count = u64::try_from(filled.len())
                .map_err(|_| MongrelError::Full("row-id allocation request is too large".into()))?;
            Some(self.allocator.alloc_range(count)?.0)
        };
        for (row_index, (cols, assigned)) in filled.into_iter().enumerate() {
            let row_id = match first_row_id {
                Some(first) => RowId(first + row_index as u64),
                None => self.derive_clustered_row_id(&cols)?,
            };
            let mut row = Row::new(row_id, epoch);
            for (c, v) in cols {
                row.columns.insert(c, v);
            }
            ids.push((row_id, assigned));
            rows.push(row);
        }
        let all_auto_generated = ids.iter().all(|(_, assigned)| assigned.is_some());
        self.commit_rows(rows, all_auto_generated)?;
        Ok(ids)
    }

    /// Fill the `AUTO_INCREMENT` column for an upcoming row. When the column is
    /// omitted or [`Value::Null`] the next counter value is allocated and the
    /// cell is appended/replaced in `columns`; an explicit `Int64` is honored
    /// and advances the counter past it. Returns `Some(value)` when the engine
    /// allocated (so the caller can surface it), `None` otherwise.
    pub fn fill_auto_inc(&mut self, columns: &mut Vec<(u16, Value)>) -> Result<Option<i64>> {
        self.ensure_writable()?;
        let Some(cid) = self.auto_inc.as_ref().map(|a| a.column_id) else {
            return Ok(None);
        };
        let pos = columns.iter().position(|(c, _)| *c == cid);
        let assigned = match pos {
            Some(i) => match &columns[i].1 {
                Value::Null => {
                    let next = self.alloc_auto_inc_value()?;
                    columns[i].1 = Value::Int64(next);
                    Some(next)
                }
                Value::Int64(n) => {
                    self.advance_auto_inc_past(*n)?;
                    None
                }
                other => {
                    return Err(MongrelError::InvalidArgument(format!(
                        "AUTO_INCREMENT column {cid} must be Int64 or NULL, got {:?}",
                        other
                    )))
                }
            },
            None => {
                let next = self.alloc_auto_inc_value()?;
                columns.push((cid, Value::Int64(next)));
                Some(next)
            }
        };
        Ok(assigned)
    }

    /// Apply column default expressions to `columns` at stage time (before
    /// NOT NULL validation). For each column carrying a `default_value`, if the
    /// column is omitted or explicitly `Null`, the default is applied. Explicit
    /// values are never overridden. Called after [`fill_auto_inc`](Self::fill_auto_inc)
    /// and before `validate_not_null`.
    pub fn apply_defaults(&self, columns: &mut Vec<(u16, Value)>) -> Result<()> {
        for col in &self.schema.columns {
            let Some(expr) = &col.default_value else {
                continue;
            };
            // Skip AUTO_INCREMENT columns — handled by fill_auto_inc.
            if col.flags.contains(ColumnFlags::AUTO_INCREMENT) {
                continue;
            }
            let pos = columns.iter().position(|(c, _)| *c == col.id);
            let needs_default = match pos {
                None => true,
                Some(i) => matches!(columns[i].1, Value::Null),
            };
            if !needs_default {
                continue;
            }
            let v = match expr {
                crate::schema::DefaultExpr::Static(v) => v.clone(),
                crate::schema::DefaultExpr::Now => Value::Bytes(iso_now_bytes()),
                crate::schema::DefaultExpr::Uuid => {
                    let mut buf = [0u8; 16];
                    getrandom::getrandom(&mut buf)
                        .map_err(|e| MongrelError::Other(format!("UUID generation failed: {e}")))?;
                    Value::Uuid(buf)
                }
            };
            match pos {
                None => columns.push((col.id, v)),
                Some(i) => columns[i].1 = v,
            }
        }
        Ok(())
    }

    /// Allocate the next identity value, seeding the counter first if needed.
    fn alloc_auto_inc_value(&mut self) -> Result<i64> {
        self.ensure_auto_inc_seeded()?;
        // Borrow checker: re-read after the mutable `ensure` call returns.
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let v = ai.next;
        ai.next = ai
            .next
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("AUTO_INCREMENT namespace exhausted".into()))?;
        Ok(v)
    }

    /// Advance the counter past an explicit id, seeding first if needed so a
    /// pre-existing higher id elsewhere is never ignored.
    fn advance_auto_inc_past(&mut self, used: i64) -> Result<()> {
        self.ensure_auto_inc_seeded()?;
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let floor = used
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("AUTO_INCREMENT namespace exhausted".into()))?
            .max(1);
        if ai.next < floor {
            ai.next = floor;
        }
        Ok(())
    }

    /// Seed the counter on first use by scanning `max(PK)` over all visible
    /// rows, so an upgraded table (legacy client-assigned ids, or a manifest
    /// migrated from `auto_inc_next == 0`) never hands out a colliding id.
    /// Idempotent: a no-op once seeded.
    fn ensure_auto_inc_seeded(&mut self) -> Result<()> {
        let needs_seed = match self.auto_inc {
            Some(ai) => !ai.seeded,
            None => return Ok(()),
        };
        if !needs_seed {
            return Ok(());
        }
        if self.seed_empty_auto_inc() {
            return Ok(());
        }
        let cid = self
            .auto_inc
            .as_ref()
            .expect("auto-inc column present")
            .column_id;
        let max = self.scan_max_int64(cid)?;
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let floor = max
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("AUTO_INCREMENT namespace exhausted".into()))?
            .max(1);
        if ai.next < floor {
            ai.next = floor;
        }
        ai.seeded = true;
        Ok(())
    }

    fn alloc_auto_inc_range(&mut self, n: usize) -> Result<Option<i64>> {
        if n == 0 || self.auto_inc.is_none() {
            return Ok(None);
        }
        self.ensure_auto_inc_seeded()?;
        let ai = self.auto_inc.as_mut().expect("auto-inc column present");
        let start = ai.next;
        let count = i64::try_from(n)
            .map_err(|_| MongrelError::Full("AUTO_INCREMENT range is too large".into()))?;
        ai.next = ai
            .next
            .checked_add(count)
            .ok_or_else(|| MongrelError::Full("AUTO_INCREMENT namespace exhausted".into()))?;
        Ok(Some(start))
    }

    /// One-time `max(Int64 column)` over all MVCC-visible rows. Used to seed the
    /// auto-increment counter. Runs at most once per table (the manifest then
    /// checkpoints the seeded counter).
    fn scan_max_int64(&mut self, column_id: u16) -> Result<i64> {
        let mut max: i64 = 0;
        for r in self.memtable.visible_versions(Epoch(u64::MAX)) {
            if let Some(Value::Int64(n)) = r.columns.get(&column_id) {
                if *n > max {
                    max = *n;
                }
            }
        }
        for r in self.mutable_run.visible_versions(Epoch(u64::MAX)) {
            if let Some(Value::Int64(n)) = r.columns.get(&column_id) {
                if *n > max {
                    max = *n;
                }
            }
        }
        for rr in self.run_refs.clone() {
            let reader = self.open_reader(rr.run_id)?;
            if let Some(stats) = reader.column_page_stats(column_id) {
                for s in stats {
                    if let Some(n) = crate::sorted_run::be_i64(s.max.as_deref()) {
                        if n > max {
                            max = n;
                        }
                    }
                }
            } else if reader.has_column(column_id) {
                if let columnar::NativeColumn::Int64 { data, validity } =
                    reader.column_native_shared(column_id)?
                {
                    for (i, n) in data.iter().enumerate() {
                        if (validity.is_empty() || columnar::validity_bit(&validity, i)) && *n > max
                        {
                            max = *n;
                        }
                    }
                }
            }
        }
        Ok(max)
    }

    fn seed_empty_auto_inc(&mut self) -> bool {
        let Some(ai) = self.auto_inc.as_mut() else {
            return false;
        };
        if ai.seeded || self.live_count != 0 {
            return false;
        }
        if ai.next < 1 {
            ai.next = 1;
        }
        ai.seeded = true;
        true
    }

    fn advance_auto_inc_from_native_columns(
        &mut self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
        live_before: u64,
    ) -> Result<()> {
        let Some(ai) = self.auto_inc.as_mut() else {
            return Ok(());
        };
        let Some((_, col)) = columns.iter().find(|(cid, _)| *cid == ai.column_id) else {
            return Ok(());
        };
        let columnar::NativeColumn::Int64 { data, validity } = col else {
            return Err(MongrelError::InvalidArgument(format!(
                "AUTO_INCREMENT column {} must be Int64",
                ai.column_id
            )));
        };
        let max = if native_int64_strictly_increasing(col, n) {
            data.get(n.saturating_sub(1)).copied()
        } else {
            data.iter()
                .take(n)
                .enumerate()
                .filter_map(|(i, v)| {
                    if validity.is_empty() || columnar::validity_bit(validity, i) {
                        Some(*v)
                    } else {
                        None
                    }
                })
                .max()
        };
        if let Some(max) = max {
            let floor = max
                .checked_add(1)
                .ok_or_else(|| MongrelError::Full("AUTO_INCREMENT namespace exhausted".into()))?
                .max(1);
            if ai.next < floor {
                ai.next = floor;
            }
            if ai.seeded || live_before == 0 {
                ai.seeded = true;
            }
        }
        Ok(())
    }

    fn fill_auto_inc_native_columns(
        &mut self,
        columns: &mut Vec<(u16, columnar::NativeColumn)>,
        n: usize,
    ) -> Result<()> {
        let Some(cid) = self.auto_inc.as_ref().map(|a| a.column_id) else {
            return Ok(());
        };
        let Some(pos) = columns.iter().position(|(id, _)| *id == cid) else {
            if let Some(start) = self.alloc_auto_inc_range(n)? {
                columns.push((
                    cid,
                    columnar::NativeColumn::Int64 {
                        data: (start..start.saturating_add(n as i64)).collect(),
                        validity: vec![0xFF; n.div_ceil(8)],
                    },
                ));
            }
            return Ok(());
        };

        let columnar::NativeColumn::Int64 { data, validity } = &mut columns[pos].1 else {
            return Err(MongrelError::InvalidArgument(format!(
                "AUTO_INCREMENT column {cid} must be Int64"
            )));
        };
        if data.len() < n {
            return Err(MongrelError::InvalidArgument(format!(
                "AUTO_INCREMENT column {cid} has {} rows, expected {n}",
                data.len()
            )));
        }
        if columnar::all_non_null(validity, n) {
            return Ok(());
        }
        if validity.iter().all(|b| *b == 0) {
            if let Some(start) = self.alloc_auto_inc_range(n)? {
                for (i, slot) in data.iter_mut().take(n).enumerate() {
                    *slot = start.saturating_add(i as i64);
                }
                *validity = vec![0xFF; n.div_ceil(8)];
            }
            return Ok(());
        }

        let new_validity = vec![0xFF; data.len().div_ceil(8)];
        for (i, slot) in data.iter_mut().enumerate().take(n) {
            if columnar::validity_bit(validity, i) {
                self.advance_auto_inc_past(*slot)?;
            } else {
                *slot = self.alloc_auto_inc_value()?;
            }
        }
        *validity = new_validity;
        Ok(())
    }

    /// Reserve (but do not insert) the next `AUTO_INCREMENT` value, advancing
    /// the in-memory counter. Returns `None` when the table has no
    /// auto-increment column.
    ///
    /// This is the escape hatch for callers that stage the row with an explicit
    /// id inside a cross-table [`crate::Transaction`] — where the engine cannot
    /// fill the column on the `put` path (the row id + cells are only assembled
    /// at commit). Unlike the old Kit `__kit_sequences` sequence row, the
    /// reservation is a pure in-memory counter bump: no hot row, no second
    /// commit. It becomes durable when a row carrying the reserved id commits
    /// (the counter is checkpointed to the manifest in the same commit); an
    /// aborted reservation simply leaves a gap, which the never-reuse rule
    /// permits.
    pub fn reserve_auto_inc(&mut self) -> Result<Option<i64>> {
        self.ensure_writable()?;
        if self.auto_inc.is_none() {
            return Ok(None);
        }
        Ok(Some(self.alloc_auto_inc_value()?))
    }

    /// Append `rows` under one WAL record. On a standalone table they are folded
    /// into the memtable + indexes immediately (single clock — no speculative-
    /// epoch hazard). On a mounted table (B1/B2) they are staged in
    /// `pending_rows` and applied at the real assigned epoch in `commit`, so a
    /// concurrent reader can never see them before their commit epoch.
    fn commit_rows(&mut self, rows: Vec<Row>, auto_inc_generated: bool) -> Result<()> {
        let payload = bincode::serialize(&rows)?;
        self.wal_append_data(Op::Put {
            table_id: self.table_id,
            rows: payload,
        })?;
        if self.is_shared() {
            self.pending_rows_auto_inc
                .extend(std::iter::repeat_n(auto_inc_generated, rows.len()));
            self.pending_rows.extend(rows);
        } else {
            self.apply_put_rows_inner(rows, !auto_inc_generated)?;
        }
        Ok(())
    }

    /// Complete every fallible read/index preparation before a WAL commit can
    /// become durable. After this succeeds, row application is in-memory only.
    pub(crate) fn prepare_durable_publish(&mut self) -> Result<()> {
        self.ensure_indexes_complete()
    }

    pub(crate) fn prepare_durable_publish_controlled(
        &mut self,
        control: &crate::ExecutionControl,
    ) -> Result<()> {
        self.ensure_indexes_complete_controlled(control, || true)?;
        Ok(())
    }

    pub(crate) fn apply_put_rows_prepared(&mut self, rows: Vec<Row>) {
        self.apply_put_rows_inner_prepared(rows, true);
    }

    fn apply_put_rows_inner(&mut self, rows: Vec<Row>, check_existing_pk: bool) -> Result<()> {
        if check_existing_pk {
            self.ensure_indexes_complete()?;
        }
        self.apply_put_rows_inner_prepared(rows, check_existing_pk);
        Ok(())
    }

    /// Apply rows after [`Self::ensure_indexes_complete`] has succeeded. Every
    /// operation below is in-memory and infallible, so durable publication can
    /// never stop halfway through a batch on an I/O error.
    fn apply_put_rows_inner_prepared(&mut self, rows: Vec<Row>, check_existing_pk: bool) {
        // Single-row puts — the hot operational path — cannot contain an
        // intra-batch duplicate, so the winner/loser partition maps are pure
        // overhead. Same semantics as the batch path below with `losers = ∅`.
        if rows.len() == 1 {
            let row = rows.into_iter().next().expect("len checked");
            self.apply_put_row_single(row, check_existing_pk);
            return;
        }
        // One pass per row: track mutated columns, tombstone the previous
        // owner of the row's PK, index (which places the HOT entry), sample,
        // and materialize. Each row is applied completely — including its
        // memtable upsert — before the next row processes, so "the last row
        // wins" falls out naturally for an intra-batch duplicate PK: the
        // earlier row is already materialized and gets tombstoned like any
        // other displaced owner (same visible state as pre-partitioning the
        // batch into winners and losers, without materializing a winner map
        // over the whole batch).
        //
        // Upsert probing is skipped entirely when no PK owner can be
        // displaced: `check_existing_pk == false` means every PK is a fresh
        // engine-assigned AUTO_INCREMENT value; an empty HOT index plus
        // strictly-increasing batch PKs (the append-style batch, mirroring
        // `bulk_pk_winner_indices`' fast path) rules out both pre-existing
        // owners and intra-batch duplicates.
        let pk_id = self.schema.primary_key().map(|c| c.id);
        let probe = match pk_id {
            Some(pid) => {
                check_existing_pk
                    && !(self.hot.is_empty() && rows_pk_strictly_increasing(&rows, pid))
            }
            None => false,
        };
        // The PK reverse map is maintained inline only once a delete has built
        // it (`pk_by_row_complete`); ingest-only tables never pay for it.
        let maintain_pk_by_row = pk_id.is_some() && self.pk_by_row_complete;
        for r in rows {
            for &cid in r.columns.keys() {
                self.pending_put_cols.insert(cid);
            }
            match pk_id {
                Some(pid) if probe || maintain_pk_by_row => {
                    if let Some(pk_val) = r.columns.get(&pid) {
                        let key = self.index_lookup_key(pid, pk_val);
                        if probe {
                            if let Some(old_rid) = self.hot.get(&key) {
                                if old_rid != r.row_id {
                                    self.tombstone_row(old_rid, r.committed_epoch, true);
                                }
                            }
                        }
                        if maintain_pk_by_row {
                            self.pk_by_row.insert(r.row_id, key);
                        }
                    }
                }
                Some(_) => {}
                None => {
                    self.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
                }
            }
            self.index_row(&r);
            self.reservoir.offer(r.row_id.0);
            self.memtable.upsert(r);
            // Count as each row lands so a later duplicate's tombstone
            // decrement (in `tombstone_row`) sees an up-to-date value.
            self.live_count = self.live_count.saturating_add(1);
        }
        self.data_generation = self.data_generation.wrapping_add(1);
    }

    /// One-row specialization of [`Table::apply_put_rows_inner`]: identical
    /// upsert semantics (tombstone the previous PK owner, insert into HOT,
    /// index, sample, materialize) without the per-batch winner/loser maps.
    fn apply_put_row_single(&mut self, row: Row, check_existing_pk: bool) {
        for &cid in row.columns.keys() {
            self.pending_put_cols.insert(cid);
        }
        let epoch = row.committed_epoch;
        if let Some(pk_col) = self.schema.primary_key() {
            let pk_id = pk_col.id;
            if let Some(pk_val) = row.columns.get(&pk_id) {
                // `index_row` below writes the HOT entry (`index_into` covers
                // the PK). The reverse map is maintained inline only once a
                // delete has built it; ingest-only tables never pay for it.
                let maintain_pk_by_row = self.pk_by_row_complete;
                if check_existing_pk || maintain_pk_by_row {
                    let key = self.index_lookup_key(pk_id, pk_val);
                    if check_existing_pk {
                        if let Some(old_rid) = self.hot.get(&key) {
                            if old_rid != row.row_id {
                                self.tombstone_row(old_rid, epoch, true);
                            }
                        }
                    }
                    if maintain_pk_by_row {
                        self.pk_by_row.insert(row.row_id, key);
                    }
                }
            }
        } else {
            self.hot
                .insert(row.row_id.0.to_be_bytes().to_vec(), row.row_id);
        }
        self.index_row(&row);
        self.reservoir.offer(row.row_id.0);
        self.memtable.upsert(row);
        self.live_count = self.live_count.saturating_add(1);
        self.data_generation = self.data_generation.wrapping_add(1);
    }

    /// Allocate a fresh row id (advancing the table's allocator). Used by the
    /// cross-table `Transaction` to assign ids before sealing a row.
    pub(crate) fn alloc_row_id(&mut self) -> Result<RowId> {
        self.allocator.alloc()
    }

    /// For clustered (WITHOUT ROWID) tables: derive a deterministic `RowId`
    /// from the primary-key value so the same PK always maps to the same row.
    /// Uses a stable hash of the PK's `encode_key()` bytes, cast to `u64`.
    /// This gives WITHOUT ROWID tables idempotent upsert semantics (same PK →
    /// same RowId, no allocator waste) without changing the storage format.
    fn derive_clustered_row_id(&self, columns: &[(u16, Value)]) -> Result<RowId> {
        let pk = self.schema.primary_key().ok_or_else(|| {
            MongrelError::Schema("clustered table requires a single-column primary key".into())
        })?;
        let pk_val = columns
            .iter()
            .find(|(id, _)| *id == pk.id)
            .map(|(_, v)| v)
            .ok_or_else(|| {
                MongrelError::Schema(format!(
                    "clustered table missing primary key column {} ({})",
                    pk.id, pk.name
                ))
            })?;
        let key_bytes = pk_val.encode_key();
        // Stable hash (FNV-1a 64-bit) — deterministic across runs and processes.
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in &key_bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        // Ensure non-zero (RowId 0 is valid but we want to avoid collision with
        // allocator-generated ids which start at 0 for non-clustered tables).
        Ok(RowId(hash.max(1)))
    }

    /// Apply the metadata for rows that were spilled to a linked uniform-epoch
    /// run (P3.4): update the HOT + secondary indexes, the reservoir, the
    /// allocator high-water mark, and `live_count` — but **do NOT** insert the
    /// rows into the memtable. The rows are served from the linked run (which the
    /// scan/merge path reads at the run's commit epoch), so materializing them in
    /// the memtable too would defeat the point of spilling (peak memory stays
    /// bounded). Caller must have linked the run before reads can resolve indexes.
    pub(crate) fn apply_run_metadata_prepared(&mut self, rows: &[Row]) -> Result<()> {
        if rows.iter().any(|row| row.row_id.0 >= u64::MAX - 1) {
            return Err(MongrelError::Full("row-id namespace exhausted".into()));
        }
        let n = rows.len();
        for r in rows {
            for &cid in r.columns.keys() {
                self.pending_put_cols.insert(cid);
            }
        }
        let (losers, winner_pks) = self.partition_pk_winners(rows);
        let epoch = rows.first().map(|r| r.committed_epoch).unwrap_or(Epoch(0));
        // Tombstone pre-existing rows that conflict with winners.
        for (key, &row_id) in &winner_pks {
            if let Some(old_rid) = self.hot.get(key) {
                if old_rid != row_id {
                    self.tombstone_row(old_rid, epoch, true);
                }
            }
        }
        // Hide duplicate-PK rows inside this uniform-epoch run by tombstoning
        // their row ids in the memtable overlay (the overlay wins over the run).
        for &loser_rid in &losers {
            self.tombstone_row(loser_rid, epoch, false);
        }
        // Insert the winners into HOT.
        for (key, row_id) in winner_pks {
            self.insert_hot_pk(key, row_id);
        }
        if self.schema.primary_key().is_none() {
            for r in rows {
                self.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
            }
        }
        for r in rows {
            self.allocator.advance_to(r.row_id)?;
            if !losers.contains(&r.row_id) {
                self.index_row(r);
            }
        }
        for r in rows {
            if !losers.contains(&r.row_id) {
                self.reservoir.offer(r.row_id.0);
            }
        }
        self.live_count = self.live_count.saturating_add((n - losers.len()) as u64);
        self.data_generation = self.data_generation.wrapping_add(1);
        Ok(())
    }

    /// Apply already-committed puts + tombstones during shared-WAL recovery
    /// (spec §15 pass 2). Advances the allocator, upserts/tombstones the
    /// memtable, and indexes the rows — but does NOT touch `live_count` (the
    /// manifest is authoritative) and does NOT append to the WAL.
    pub(crate) fn recover_apply(
        &mut self,
        rows: Vec<Row>,
        deletes: Vec<(RowId, Epoch)>,
    ) -> Result<()> {
        // Rows from different transactions have different epochs and can be
        // upserted sequentially. Rows inside one transaction share an epoch, so
        // duplicate PKs within that transaction must keep only the last winner.
        let mut by_epoch: std::collections::BTreeMap<Epoch, Vec<Row>> =
            std::collections::BTreeMap::new();
        for row in rows {
            if row.row_id.0 >= u64::MAX - 1 {
                return Err(MongrelError::Full("row-id namespace exhausted".into()));
            }
            self.allocator.advance_to(row.row_id)?;
            // Mirror the row-id advance for the AUTO_INCREMENT counter: WAL
            // replay must not hand out an id a recovered row already claimed.
            // `seeded` is intentionally left untouched so a still-unseeded
            // counter still scans `max(PK)` to cover already-flushed rows.
            if let Some(ai) = self.auto_inc.as_mut() {
                if let Some(Value::Int64(n)) = row.columns.get(&ai.column_id) {
                    let next = n.checked_add(1).ok_or_else(|| {
                        MongrelError::Full("AUTO_INCREMENT namespace exhausted".into())
                    })?;
                    if next > ai.next {
                        ai.next = next;
                    }
                }
            }
            by_epoch.entry(row.committed_epoch).or_default().push(row);
        }
        for (epoch, group) in by_epoch {
            let (losers, winner_pks) = self.partition_pk_winners(&group);
            // Tombstone pre-existing PK owners.
            for (key, &row_id) in &winner_pks {
                if let Some(old_rid) = self.hot.get(key) {
                    if old_rid != row_id {
                        self.tombstone_row(old_rid, epoch, false);
                    }
                }
            }
            for (key, row_id) in winner_pks {
                self.insert_hot_pk(key, row_id);
            }
            if self.schema.primary_key().is_none() {
                for r in &group {
                    self.hot.insert(r.row_id.0.to_be_bytes().to_vec(), r.row_id);
                }
            }
            for r in &group {
                if !losers.contains(&r.row_id) {
                    self.memtable.upsert(r.clone());
                    self.index_row(r);
                }
            }
        }
        for (rid, epoch) in deletes {
            self.memtable.tombstone(rid, epoch);
            self.remove_hot_for_row(rid, epoch);
        }
        // Reservoir stays lazy — see `ensure_reservoir_complete` — rather than
        // eagerly materializing every row on every WAL-replay batch.
        self.reservoir_complete = false;
        Ok(())
    }

    /// Highest epoch whose data is durable in a sorted run (spec §7.1).
    pub(crate) fn flushed_epoch(&self) -> u64 {
        self.flushed_epoch
    }

    pub(crate) fn set_flushed_epoch(&mut self, epoch: Epoch) {
        self.flushed_epoch = self.flushed_epoch.max(epoch.0);
    }

    /// Validate that `cells` satisfy the schema's NOT NULL constraints.
    pub(crate) fn validate_cells_not_null(&self, cells: &[(u16, Value)]) -> Result<()> {
        self.schema.validate_values(cells)
    }

    /// Column-major NOT NULL validation for the bulk-load paths. Every schema
    /// column that is not marked NULLABLE must be present in `columns` and have
    /// no null validity bits over its first `n` rows.
    fn validate_columns_not_null(
        &self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
    ) -> Result<()> {
        let by_id: HashMap<u16, &columnar::NativeColumn> =
            columns.iter().map(|(id, c)| (*id, c)).collect();
        for col in &self.schema.columns {
            if !col.flags.contains(ColumnFlags::NULLABLE) {
                match by_id.get(&col.id) {
                    None => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) is NOT NULL but was omitted from the bulk load",
                            col.name, col.id
                        )));
                    }
                    Some(c) => {
                        if c.null_count(n) != 0 {
                            return Err(MongrelError::InvalidArgument(format!(
                                "column '{}' ({}) is NOT NULL but the bulk load contains nulls",
                                col.name, col.id
                            )));
                        }
                    }
                }
            }
            if let TypeId::Enum { variants } = &col.ty {
                let Some(columnar::NativeColumn::Bytes { .. }) = by_id.get(&col.id).copied() else {
                    if by_id.contains_key(&col.id) {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) enum requires a bytes column",
                            col.name, col.id
                        )));
                    }
                    continue;
                };
                for index in 0..n {
                    let Some(value) = columnar::native_bytes_at(by_id[&col.id], index) else {
                        continue;
                    };
                    if !variants.iter().any(|variant| variant.as_bytes() == value) {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) enum value {:?} is not one of {:?}",
                            col.name,
                            col.id,
                            String::from_utf8_lossy(value),
                            variants
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// For a bulk-loaded batch, compute the row indices that survive primary-
    /// key upsert: for each PK value the last occurrence wins, earlier
    /// duplicates are dropped. Rows with a null PK value are always kept. Returns
    /// `None` when there is no primary key or no compaction is needed.
    fn bulk_pk_winner_indices(
        &self,
        columns: &[(u16, columnar::NativeColumn)],
        n: usize,
    ) -> Option<Vec<usize>> {
        let pk_col = self.schema.primary_key()?;
        let pk_id = pk_col.id;
        let pk_ty = pk_col.ty.clone();
        let by_id: HashMap<u16, &columnar::NativeColumn> =
            columns.iter().map(|(id, c)| (*id, c)).collect();
        let pk_native = by_id.get(&pk_id)?;
        if native_int64_strictly_increasing(pk_native, n) {
            return None;
        }
        // key -> index of the last row that carried that PK value.
        let mut last: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut null_pk_rows: Vec<usize> = Vec::new();
        for i in 0..n {
            match bulk_index_key(&self.column_keys, pk_id, pk_ty.clone(), pk_native, i) {
                Some(key) => {
                    last.insert(key, i);
                }
                None => null_pk_rows.push(i),
            }
        }
        let mut winners: HashSet<usize> = last.values().copied().collect();
        for i in null_pk_rows {
            winners.insert(i);
        }
        Some((0..n).filter(|i| winners.contains(i)).collect())
    }

    /// Logically delete `row_id` (effective at the next commit).
    pub fn delete(&mut self, row_id: RowId) -> Result<()> {
        self.require_delete()?;
        let epoch = self.pending_epoch();
        self.wal_append_data(Op::Delete {
            table_id: self.table_id,
            row_ids: vec![row_id],
        })?;
        if self.is_shared() {
            self.pending_dels.push(row_id);
        } else {
            self.apply_delete(row_id, epoch);
        }
        Ok(())
    }

    pub fn delete_returning(&mut self, row_id: RowId) -> Result<Option<OwnedRow>> {
        let pre = self.get(row_id, self.snapshot());
        self.delete(row_id)?;
        Ok(pre.map(|row| {
            let mut columns: Vec<_> = row.columns.into_iter().collect();
            columns.sort_by_key(|(id, _)| *id);
            OwnedRow { columns }
        }))
    }

    /// Durably remove every row in the table once the current write span commits.
    pub fn truncate(&mut self) -> Result<()> {
        self.require_delete()?;
        let epoch = self.pending_epoch();
        self.wal_append_data(Op::TruncateTable {
            table_id: self.table_id,
        })?;
        self.pending_rows.clear();
        self.pending_rows_auto_inc.clear();
        self.pending_dels.clear();
        self.pending_truncate = Some(epoch);
        Ok(())
    }

    /// Apply an already-durable truncate without appending to the WAL.
    pub(crate) fn apply_truncate(&mut self, _epoch: Epoch) {
        // Unlink active topology in the next manifest before removing any run
        // file. A crash before that manifest is durable must still be able to
        // open the old manifest and replay the durable truncate from WAL.
        // Unreferenced files are safe orphans and `gc()` removes them later.
        self.run_refs.clear();
        self.retiring.clear();
        self.memtable = Memtable::new();
        self.mutable_run = MutableRun::new();
        self.hot = HotIndex::new();
        let (bitmap, ann, fm, sparse, minhash) = empty_indexes(&self.schema);
        self.bitmap = bitmap;
        self.ann = ann;
        self.fm = fm;
        self.sparse = sparse;
        self.minhash = minhash;
        self.learned_range = Arc::new(HashMap::new());
        self.pk_by_row.clear();
        self.pk_by_row_complete = false;
        self.live_count = 0;
        self.reservoir = crate::reservoir::Reservoir::default();
        self.reservoir_complete = true;
        self.had_deletes = true;
        self.agg_cache = Arc::new(HashMap::new());
        self.global_idx_epoch = 0;
        self.indexes_complete = true;
        self.pending_delete_rids.clear();
        self.pending_put_cols.clear();
        self.pending_rows.clear();
        self.pending_rows_auto_inc.clear();
        self.pending_dels.clear();
        self.clear_result_cache();
        self.invalidate_index_checkpoint();
        self.data_generation = self.data_generation.wrapping_add(1);
    }

    /// Apply a tombstone (already-durable on the WAL) at `epoch` without
    /// appending to the per-table WAL. Used by the cross-table `Transaction`.
    pub(crate) fn apply_delete(&mut self, row_id: RowId, epoch: Epoch) {
        self.remove_hot_for_row(row_id, epoch);
        self.tombstone_row(row_id, epoch, true);
        self.data_generation = self.data_generation.wrapping_add(1);
    }

    /// Tombstone `row_id` at `epoch`. When `adjust_live_count` is true the
    /// table's `live_count` is decremented (used on the live write path); during
    /// recovery the manifest is authoritative so the flag is false.
    fn tombstone_row(&mut self, row_id: RowId, epoch: Epoch, adjust_live_count: bool) {
        let tombstone = Row {
            row_id,
            committed_epoch: epoch,
            columns: std::collections::HashMap::new(),
            deleted: true,
        };
        self.memtable.upsert(tombstone);
        self.pk_by_row.remove(&row_id);
        if adjust_live_count {
            self.live_count = self.live_count.saturating_sub(1);
        }
        // Track for fine-grained cache invalidation (c).
        self.pending_delete_rids.insert(row_id.0 as u32);
        // A delete makes the incremental aggregate cache (row-id watermark
        // delta) unsafe — permanently disable it for this table.
        self.had_deletes = true;
        self.agg_cache = Arc::new(HashMap::new());
    }

    /// If `row_id` has a primary-key value and the HOT index currently maps
    /// that PK to this row id, remove the entry. Keeps the PK→RowId mapping
    /// consistent after deletes and before upserts.
    fn remove_hot_for_row(&mut self, row_id: RowId, epoch: Epoch) {
        let Some(pk_col) = self.schema.primary_key() else {
            return;
        };
        // Warm path: a prior delete in this process already paid the
        // reverse-map rebuild below, so it's kept up to date — O(1).
        if self.pk_by_row_complete {
            if let Some(key) = self.pk_by_row.remove(&row_id) {
                if self.hot.get(&key) == Some(row_id) {
                    self.hot.remove(&key);
                }
            }
            return;
        }
        // Cold path (the common case: a short-lived process — CLI,
        // NAPI-per-call — that deletes once and exits): derive the PK
        // straight from the row's own pre-delete version via a targeted
        // get_version lookup (memtable -> mutable_run -> runs, the same
        // page-pruned lookup `Table::get` uses) instead of paying
        // `refresh_pk_by_row_from_hot`'s O(table-size) rebuild for a single
        // delete. `pk_by_row` is deliberately left incomplete here — same
        // "puts leave the reverse map stale" tradeoff, extended to this path.
        //
        // Look up at `epoch - 1`, not `epoch`: on the live-delete call site
        // this delete's own tombstone hasn't landed yet either way, but on
        // the WAL-replay call sites (`recover_apply`, `open_in`) the
        // memtable tombstone for this exact row/epoch is already applied
        // before this runs. Querying `epoch` would see that tombstone
        // (empty columns) and fall through to the full rebuild every time a
        // replayed delete exists; `epoch - 1` is still >= any real prior
        // version's committed_epoch (epochs are unique and monotonic), so it
        // finds the same pre-delete row either way.
        let lookup_epoch = Epoch(epoch.0.saturating_sub(1));
        if self.indexes_complete {
            let pk_val = self
                .memtable
                .get_version(row_id, lookup_epoch)
                .and_then(|(_, r)| r.columns.get(&pk_col.id).cloned())
                .or_else(|| {
                    self.mutable_run
                        .get_version(row_id, lookup_epoch)
                        .filter(|(_, r)| !r.deleted)
                        .and_then(|(_, r)| r.columns.get(&pk_col.id).cloned())
                })
                .or_else(|| {
                    self.run_refs.iter().find_map(|rr| {
                        let mut reader = self.open_reader(rr.run_id).ok()?;
                        let (_, deleted, val) = reader
                            .get_version_column(row_id, lookup_epoch, pk_col.id)
                            .ok()??;
                        if deleted {
                            return None;
                        }
                        val
                    })
                });
            if let Some(pk_val) = pk_val {
                let key = self.index_lookup_key(pk_col.id, &pk_val);
                if self.hot.get(&key) == Some(row_id) {
                    self.hot.remove(&key);
                }
                return;
            }
        }
        // Fallback: full reverse-map rebuild, guaranteed correct. Reached
        // when indexes aren't complete yet, or the row was already gone by
        // the time this ran (e.g. already tombstoned in an overlay ahead of
        // this HOT cleanup, as `rebuild_indexes_from_runs` does).
        self.refresh_pk_by_row_from_hot();
        if let Some(key) = self.pk_by_row.remove(&row_id) {
            if self.hot.get(&key) == Some(row_id) {
                self.hot.remove(&key);
            }
        }
    }

    /// For a batch of rows that share the same commit epoch, decide which rows
    /// win for each primary-key value. Returns the set of "loser" row ids that
    /// must be skipped/overwritten, and a map from PK lookup key to the winning
    /// row id. Rows without a PK value are always winners.
    fn partition_pk_winners(
        &self,
        rows: &[Row],
    ) -> (
        std::collections::HashSet<RowId>,
        std::collections::HashMap<Vec<u8>, RowId>,
    ) {
        let mut losers = std::collections::HashSet::new();
        let Some(pk_col) = self.schema.primary_key() else {
            return (losers, std::collections::HashMap::new());
        };
        let pk_id = pk_col.id;
        let mut winners: std::collections::HashMap<Vec<u8>, RowId> =
            std::collections::HashMap::new();
        for r in rows {
            let Some(pk_val) = r.columns.get(&pk_id) else {
                continue;
            };
            let key = self.index_lookup_key(pk_id, pk_val);
            if let Some(&old_rid) = winners.get(&key) {
                losers.insert(old_rid);
            }
            winners.insert(key, r.row_id);
        }
        (losers, winners)
    }

    fn index_row(&mut self, row: &Row) {
        if row.deleted {
            return;
        }
        // Partial index filtering: skip rows that don't match any index's
        // predicate. The predicate is a SQL WHERE clause string evaluated
        // against the row's column values. For now, we support a simple
        // "column_name IS NOT NULL" and "column_name = value" syntax that
        // covers the common partial-index patterns (e.g. WHERE deleted_at
        // IS NULL). More complex predicates require a full expression
        // evaluator in core (future work).
        let any_predicate = self
            .schema
            .indexes
            .iter()
            .any(|idx| idx.predicate.is_some());
        if any_predicate {
            let columns_map: HashMap<u16, &Value> =
                row.columns.iter().map(|(k, v)| (*k, v)).collect();
            let name_to_id: HashMap<&str, u16> = self
                .schema
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.id))
                .collect();
            for idx in &self.schema.indexes {
                if let Some(pred) = &idx.predicate {
                    if !eval_partial_predicate(pred, &columns_map, &name_to_id) {
                        continue; // skip this index for this row
                    }
                }
                // Index the row into this specific index only.
                index_into_single(
                    idx,
                    &self.schema,
                    row,
                    &mut self.hot,
                    &mut self.bitmap,
                    &mut self.ann,
                    &mut self.fm,
                    &mut self.sparse,
                    &mut self.minhash,
                );
            }
            return;
        }
        // Plaintext tables index the row as-is; only ENCRYPTED_INDEXABLE
        // columns need the tokenized copy (`tokenized_for_indexes` clones the
        // whole row, which would tax every put on unencrypted tables).
        if self.column_keys.is_empty() {
            index_into(
                &self.schema,
                row,
                &mut self.hot,
                &mut self.bitmap,
                &mut self.ann,
                &mut self.fm,
                &mut self.sparse,
                &mut self.minhash,
            );
            return;
        }
        let effective_row = self.tokenized_for_indexes(row);
        index_into(
            &self.schema,
            &effective_row,
            &mut self.hot,
            &mut self.bitmap,
            &mut self.ann,
            &mut self.fm,
            &mut self.sparse,
            &mut self.minhash,
        );
    }

    /// Produce the row view that indexes should see. For ENCRYPTED_INDEXABLE
    /// equality (HMAC-eq) columns the plaintext value is replaced by its token,
    /// so the bitmap/HOT indexes store tokens. OPE-range columns keep their raw
    /// value (their range index is rebuilt from runs over plaintext). Plaintext
    /// tables return the row unchanged.
    fn tokenized_for_indexes(&self, row: &Row) -> Row {
        if self.column_keys.is_empty() {
            return row.clone();
        }
        #[cfg(feature = "encryption")]
        {
            use crate::encryption::SCHEME_HMAC_EQ;
            let mut tok = row.clone();
            for (&cid, &(_, scheme)) in &self.column_keys {
                if scheme != SCHEME_HMAC_EQ {
                    continue;
                }
                if let Some(v) = tok.columns.get(&cid).cloned() {
                    if let Some(t) = self.tokenize_value(cid, &v) {
                        tok.columns.insert(cid, t);
                    }
                }
            }
            tok
        }
        #[cfg(not(feature = "encryption"))]
        {
            row.clone()
        }
    }

    /// Group-commit: make all pending writes durable, advance the epoch so they
    /// become visible, and persist the manifest. Dispatches on the WAL sink: a
    /// standalone table fsyncs its private WAL; a mounted table seals into the
    /// shared WAL and defers the fsync to the group-commit coordinator (B1).
    pub fn commit(&mut self) -> Result<Epoch> {
        self.commit_inner(None)
    }

    /// Prepare a pending commit cooperatively, then invoke `before_commit`
    /// immediately before the durable transaction marker is appended.
    #[doc(hidden)]
    pub fn commit_controlled<F>(
        &mut self,
        control: &crate::ExecutionControl,
        mut before_commit: F,
    ) -> Result<Epoch>
    where
        F: FnMut() -> Result<()>,
    {
        self.commit_inner(Some((control, &mut before_commit)))
    }

    fn commit_inner(
        &mut self,
        controlled: Option<(&crate::ExecutionControl, &mut dyn FnMut() -> Result<()>)>,
    ) -> Result<Epoch> {
        self.ensure_writable()?;
        if !self.has_pending_mutations() {
            if self.current_txn_id == 0 && matches!(&self.wal, WalSink::Private(_)) {
                return Err(MongrelError::Full(
                    "standalone transaction id namespace exhausted".into(),
                ));
            }
            return Ok(self.epoch.visible());
        }
        self.commit_new_epoch_inner(controlled)
    }

    /// Seal a real logical write at a fresh epoch. Bulk-load paths publish
    /// their run directly rather than staging rows in the WAL, so they call
    /// this after proving the input is non-empty.
    fn commit_new_epoch(&mut self) -> Result<Epoch> {
        self.commit_new_epoch_inner(None)
    }

    fn commit_new_epoch_inner(
        &mut self,
        controlled: Option<(&crate::ExecutionControl, &mut dyn FnMut() -> Result<()>)>,
    ) -> Result<Epoch> {
        self.ensure_writable()?;
        if self.is_shared() {
            self.commit_shared(controlled)
        } else {
            self.commit_private(controlled)
        }
    }

    /// Standalone commit: fsync the private WAL under the commit lock.
    fn commit_private(
        &mut self,
        controlled: Option<(&crate::ExecutionControl, &mut dyn FnMut() -> Result<()>)>,
    ) -> Result<Epoch> {
        // Serialize the assign→fsync→publish critical section across all tables
        // sharing the epoch authority so `visible` is published strictly in
        // assigned order (the dual-counter invariant).
        let commit_lock = Arc::clone(&self.commit_lock);
        let _g = commit_lock.lock();
        // Validate the private transaction namespace before allocating an
        // epoch or appending any terminal WAL record.
        let txn_id = self.ensure_txn_id()?;
        if let Some((control, before_commit)) = controlled {
            control.checkpoint()?;
            before_commit()?;
        }
        let new_epoch = self.epoch.bump_assigned();
        let epoch_authority = Arc::clone(&self.epoch);
        let mut epoch_guard = EpochGuard::new(epoch_authority.as_ref(), new_epoch);
        // Seal the staged records under a TxnCommit marker carrying the commit
        // epoch, then a single group fsync. Recovery applies only records whose
        // txn has a durable TxnCommit (uncommitted/torn tails are discarded).
        let wal_result = match &mut self.wal {
            WalSink::Private(w) => w
                .append_txn(
                    txn_id,
                    Op::TxnCommit {
                        epoch: new_epoch.0,
                        added_runs: Vec::new(),
                    },
                )
                .and_then(|_| w.sync()),
            WalSink::Shared(_) => unreachable!("commit_private on a shared sink"),
            WalSink::ReadOnly => Err(MongrelError::ReadOnlyReplica),
        };
        if let Err(error) = wal_result {
            self.durable_commit_failed = true;
            return Err(MongrelError::CommitOutcomeUnknown {
                epoch: new_epoch.0,
                message: error.to_string(),
            });
        }
        // The commit marker is durable. Resolve the assigned epoch even when a
        // live publish/checkpoint step fails, and report the exact outcome.
        if let Some(epoch) = self.pending_truncate.take() {
            self.apply_truncate(epoch);
        }
        self.invalidate_pending_cache();
        let publish_result = self.persist_manifest(new_epoch);
        // Publish through the shared in-order gate so a `Table::commit` can never
        // advance the watermark past an in-flight cross-table transaction's
        // lower assigned epoch whose writes are not yet applied (spec §9.3e).
        self.epoch.publish_in_order(new_epoch);
        epoch_guard.disarm();
        if let Err(error) = publish_result {
            self.durable_commit_failed = true;
            return Err(MongrelError::DurableCommit {
                epoch: new_epoch.0,
                message: error.to_string(),
            });
        }
        self.current_txn_id = txn_id.checked_add(1).unwrap_or(0);
        self.pending_private_mutations = false;
        self.data_generation = self.data_generation.wrapping_add(1);
        Ok(new_epoch)
    }

    /// Mounted commit (B1/B2): mirror the cross-table sequencer. Seal a
    /// `TxnCommit` into the shared WAL under the WAL lock (assigning the epoch in
    /// WAL-append order), make it durable via the group-commit coordinator (one
    /// leader fsync for the whole batch), then apply the staged rows at the
    /// assigned epoch and publish in order. Honors the shared poison flag.
    fn commit_shared(
        &mut self,
        controlled: Option<(&crate::ExecutionControl, &mut dyn FnMut() -> Result<()>)>,
    ) -> Result<Epoch> {
        use std::sync::atomic::Ordering;
        let s = match &self.wal {
            WalSink::Shared(s) => s.clone(),
            WalSink::Private(_) => unreachable!("commit_shared on a private sink"),
            WalSink::ReadOnly => return Err(MongrelError::ReadOnlyReplica),
        };
        if s.poisoned.load(Ordering::Relaxed) {
            return Err(MongrelError::Other(
                "database poisoned by fsync error".into(),
            ));
        }
        // Serialize the whole single-table commit critical section (assign →
        // durable → publish) under the shared commit lock so concurrent
        // `Table::commit`s publish strictly in assigned order and each returns
        // only once its epoch is visible (read-your-writes after commit). The
        // fsync still defers to the group-commit coordinator, which can batch a
        // held commit with concurrent cross-table `transaction()` committers.
        let commit_lock = Arc::clone(&self.commit_lock);
        let _g = commit_lock.lock();
        if !self.pending_rows.is_empty() {
            match controlled.as_ref() {
                Some((control, _)) => self.prepare_durable_publish_controlled(control)?,
                None => self.prepare_durable_publish()?,
            }
        }
        // Always seal a txn (allocating an id if this span had no writes) so the
        // epoch advances monotonically like the standalone path.
        let txn_id = self.ensure_txn_id()?;
        let mut wal = s.wal.lock();
        if let Some((control, before_commit)) = controlled {
            control.checkpoint()?;
            before_commit()?;
        }
        let new_epoch = self.epoch.bump_assigned();
        let epoch_authority = Arc::clone(&self.epoch);
        let mut epoch_guard = EpochGuard::new(epoch_authority.as_ref(), new_epoch);
        let commit_seq = match wal.append_commit(txn_id, new_epoch, &[]) {
            Ok(commit_seq) => commit_seq,
            Err(error) => {
                s.poisoned.store(true, Ordering::Relaxed);
                return Err(MongrelError::CommitOutcomeUnknown {
                    epoch: new_epoch.0,
                    message: error.to_string(),
                });
            }
        };
        drop(wal);
        if let Err(error) = s.group.await_durable(&s.wal, commit_seq) {
            s.poisoned.store(true, Ordering::Relaxed);
            return Err(MongrelError::CommitOutcomeUnknown {
                epoch: new_epoch.0,
                message: error.to_string(),
            });
        }

        // Apply staged state after durability, but never lose the durable
        // outcome if a live apply or manifest checkpoint fails.
        if self.pending_truncate.take().is_some() {
            self.apply_truncate(new_epoch);
        }
        let mut rows = std::mem::take(&mut self.pending_rows);
        if !rows.is_empty() {
            for r in &mut rows {
                r.committed_epoch = new_epoch;
            }
            let auto_inc_flags = std::mem::take(&mut self.pending_rows_auto_inc);
            let all_auto_generated =
                auto_inc_flags.len() == rows.len() && auto_inc_flags.iter().all(|b| *b);
            self.apply_put_rows_inner_prepared(rows, !all_auto_generated);
        } else {
            self.pending_rows_auto_inc.clear();
        }
        let dels = std::mem::take(&mut self.pending_dels);
        for rid in dels {
            self.apply_delete(rid, new_epoch);
        }

        self.invalidate_pending_cache();
        let publish_result = self.persist_manifest(new_epoch);
        self.epoch.publish_in_order(new_epoch);
        epoch_guard.disarm();
        let _ = s.change_wake.send(());
        if let Err(error) = publish_result {
            self.durable_commit_failed = true;
            s.poisoned.store(true, Ordering::Relaxed);
            return Err(MongrelError::DurableCommit {
                epoch: new_epoch.0,
                message: error.to_string(),
            });
        }
        // Next auto-commit span allocates a fresh shared txn id.
        self.current_txn_id = 0;
        self.data_generation = self.data_generation.wrapping_add(1);
        Ok(new_epoch)
    }

    /// Commit, then drain the memtable into the mutable-run LSM tier (Phase
    /// 11.1). The tier absorbs flushes in place and only spills to an immutable
    /// `.sr` sorted run once it crosses the spill watermark — coalescing many
    /// small flushes into fewer, larger runs. While the tier holds un-spilled
    /// data the WAL is **not** rotated: the Flush marker / WAL rotation is
    /// deferred until the data is durably in a run, so crash recovery replays
    /// those rows back into the memtable (the tier rebuilds from replay).
    pub fn flush(&mut self) -> Result<Epoch> {
        self.flush_with_outcome().map(|(epoch, _)| epoch)
    }

    /// Flush and report whether this call published pending logical mutations.
    pub fn flush_with_outcome(&mut self) -> Result<(Epoch, bool)> {
        self.flush_with_outcome_inner(None)
    }

    /// Cooperatively prepare a flush, entering the commit fence immediately
    /// before its transaction marker can become durable.
    #[doc(hidden)]
    pub fn flush_with_outcome_controlled<F>(
        &mut self,
        control: &crate::ExecutionControl,
        mut before_commit: F,
    ) -> Result<(Epoch, bool)>
    where
        F: FnMut() -> Result<()>,
    {
        self.flush_with_outcome_inner(Some((control, &mut before_commit)))
    }

    fn flush_with_outcome_inner(
        &mut self,
        controlled: Option<(&crate::ExecutionControl, &mut dyn FnMut() -> Result<()>)>,
    ) -> Result<(Epoch, bool)> {
        match controlled.as_ref() {
            Some((control, _)) => {
                self.ensure_indexes_complete_controlled(control, || true)?;
            }
            None => self.ensure_indexes_complete()?,
        }
        let committed = self.has_pending_mutations();
        let epoch = self.commit_inner(controlled)?;
        let finish: Result<(Epoch, bool)> = (|| {
            let rows = self.memtable.drain_sorted();
            if !rows.is_empty() {
                self.mutable_run.insert_many(rows);
            }
            if self.mutable_run.approx_bytes() >= self.mutable_run_spill_bytes {
                self.spill_mutable_run(epoch)?;
                // The tier is now empty and its data is durably in a run → safe to
                // mark the WAL flushed (and, for a private WAL, rotate to a fresh
                // segment so the flushed records aren't replayed).
                self.mark_flushed(epoch)?;
                self.persist_manifest(epoch)?;
                self.build_learned_ranges()?;
                // Memtable is drained and runs are stable → checkpoint the indexes so
                // the next open skips the full run scan (Phase 9.1).
                self.checkpoint_indexes(epoch);
            }
            // else: data coalesced in the in-memory tier; the WAL still covers it
            // and the manifest epoch was already persisted by `commit`.
            Ok((epoch, committed))
        })();
        match finish {
            Err(error) if committed => Err(MongrelError::DurableCommit {
                epoch: epoch.0,
                message: error.to_string(),
            }),
            result => result,
        }
    }

    fn has_pending_mutations(&self) -> bool {
        self.pending_private_mutations
            || !self.pending_rows.is_empty()
            || !self.pending_dels.is_empty()
            || self.pending_truncate.is_some()
    }

    pub fn has_pending_writes(&self) -> bool {
        self.has_pending_mutations()
    }

    /// Force a full flush to a `.sr` sorted run regardless of the spill
    /// threshold. Temporarily lowers `mutable_run_spill_bytes` to 1 so the
    /// threshold check in [`Self::flush`] always fires. Used by
    /// [`Self::close`] and the Kit's flush-on-close path (§4.4) so a
    /// short-lived process (CLI, one-shot script) leaves all pending writes
    /// durable in a run — keeping WAL segment count bounded across repeated
    /// invocations. Best-effort: errors are propagated but the threshold is
    /// always restored.
    pub fn force_flush(&mut self) -> Result<Epoch> {
        let saved = self.mutable_run_spill_bytes;
        self.mutable_run_spill_bytes = 1;
        let result = self.flush();
        self.mutable_run_spill_bytes = saved;
        result
    }

    /// Best-effort close: force-flush any pending writes to a sorted run so
    /// the WAL segments can be reaped on the next open. Never panics — a
    /// flush error is logged and returned but the threshold is always
    /// restored. Call this as the last action before a short-lived process
    /// exits (CLI, one-shot script). Not needed for the daemon (its
    /// background auto-compactor handles run management). (§4.4)
    pub fn close(&mut self) -> Result<()> {
        if self.memtable_len() > 0 || self.mutable_run_len() > 0 {
            self.force_flush()?;
        }
        Ok(())
    }

    /// Mark `epoch` as flushed: append a `Flush` marker to the WAL, advance
    /// `flushed_epoch`, and — for a private WAL only — rotate to a fresh segment
    /// so the now-durable-in-a-run records are not replayed. A mounted table's
    /// shared WAL is never rotated per-table; recovery skips its already-flushed
    /// records via the manifest `flushed_epoch` gate, and segment GC (B3c) reaps
    /// them once every table has flushed past them.
    fn mark_flushed(&mut self, epoch: Epoch) -> Result<()> {
        let op = Op::Flush {
            table_id: self.table_id,
            flushed_epoch: epoch.0,
        };
        match &mut self.wal {
            WalSink::Private(w) => {
                w.append_system(op)?;
                w.sync()?;
            }
            WalSink::Shared(s) => {
                // Informational in the shared log (recovery gates on the manifest
                // `flushed_epoch`); not separately fsynced — the run + manifest
                // are the durability point and the underlying rows were already
                // fsynced at their commit.
                s.wal.lock().append_system(op)?;
            }
            WalSink::ReadOnly => return Err(MongrelError::ReadOnlyReplica),
        }
        self.flushed_epoch = epoch.0;
        if matches!(self.wal, WalSink::Private(_)) {
            self.rotate_wal(epoch)?;
        }
        Ok(())
    }

    /// Spill the mutable-run tier to a new immutable level-0 sorted run. The
    /// caller owns the Flush-marker / WAL-rotation / manifest steps (only valid
    /// once all in-flight data is in runs). No-op when the tier is empty.
    fn spill_mutable_run(&mut self, epoch: Epoch) -> Result<()> {
        if self.mutable_run.is_empty() {
            return Ok(());
        }
        let run_id = self.alloc_run_id()?;
        let rows = self.mutable_run.drain_sorted();
        if rows.is_empty() {
            return Ok(());
        }
        let path = self.run_path(run_id);
        let mut writer = RunWriter::new(&self.schema, run_id as u128, epoch, 0);
        if let Some(kek) = &self.kek {
            writer = writer.with_encryption(kek.as_ref(), self.indexable_column_specs());
        }
        let header = match self.create_run_file(run_id)? {
            Some(file) => writer.write_file(file, &rows)?,
            None => writer.write(&path, &rows)?,
        };
        self.run_refs.push(RunRef {
            run_id: run_id as u128,
            level: 0,
            epoch_created: epoch.0,
            row_count: header.row_count,
        });
        Ok(())
    }

    /// Tune the mutable-run spill watermark (bytes). A smaller threshold spills
    /// sooner (more, smaller runs — closer to the pre-Phase-11.1 behavior); a
    /// larger one coalesces more flushes in memory.
    pub fn set_mutable_run_spill_bytes(&mut self, bytes: u64) {
        self.mutable_run_spill_bytes = bytes.max(1);
    }

    /// Set the zstd compression level for compaction output (Phase 18.1).
    /// Default 3; higher values give better compression ratio at the cost of
    /// slower compaction.
    pub fn set_compaction_zstd_level(&mut self, level: i32) {
        self.compaction_zstd_level = level;
    }

    /// Set the result-cache byte budget (Phase 19.1 hardening (a)). Entries are
    /// evicted in access-order LRU past this limit. Takes effect immediately
    /// (may evict entries if the new limit is smaller than the current footprint).
    pub fn set_result_cache_max_bytes(&mut self, max_bytes: u64) {
        self.result_cache.lock().set_max_bytes(max_bytes);
    }

    /// Drop every cached result (used by compaction, schema evolution, and bulk
    /// load — paths that change run layout or data without going through the
    /// fine-grained `pending_*` tracking).
    pub(crate) fn clear_result_cache(&mut self) {
        self.result_cache.lock().clear();
    }

    /// Number of versions currently held in the mutable-run tier.
    pub fn mutable_run_len(&self) -> usize {
        self.mutable_run.len()
    }

    /// Drain every version from the mutable-run tier (ascending `(RowId,
    /// Epoch)` order). Used by compaction to fold the tier into its merge.
    pub(crate) fn drain_mutable_run(&mut self) -> Vec<Row> {
        self.mutable_run.drain_sorted()
    }

    /// Snapshot the mutable-run tier without changing live table state.
    pub(crate) fn snapshot_mutable_run(&self) -> Vec<Row> {
        let mut snapshot = self.mutable_run.clone();
        snapshot.drain_sorted()
    }

    /// Bulk-load: write `batch` directly to a new sorted run, bypassing the WAL
    /// and the memtable entirely (no per-row bincode, no skip-list inserts). The
    /// run + a rotated WAL + the manifest are fsynced once — the fast ingest
    /// path for large analytical loads. Indexes are still maintained.
    pub fn bulk_load(&mut self, batch: Vec<Vec<(u16, Value)>>) -> Result<Epoch> {
        self.ensure_writable()?;
        let n = batch.len();
        if n == 0 {
            return Ok(self.current_epoch());
        }
        for row in &batch {
            self.schema.validate_values(row)?;
        }
        let epoch = self.commit_new_epoch()?;
        let live_before = self.live_count;
        // Spill any pending mutable-run data first: bulk_load writes a Flush
        // marker + rotates the WAL below, which is only safe once all in-flight
        // data is durably in a run.
        self.spill_mutable_run(epoch)?;
        let eager_index_build = self.index_build_policy == IndexBuildPolicy::Eager
            && self.indexes_complete
            && self.run_refs.is_empty()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty();
        // Phase 14.7: route the legacy Value API through the same parallel
        // encode + typed batch-index path as `bulk_load_columns`. Transpose the
        // row-major sparse batch → column-major typed columns (in parallel),
        // then `write_native` + `index_columns_bulk`, instead of per-row
        // `Row { HashMap }` + `index_into` + the sequential `Value` writer.
        let mut user_columns: Vec<(u16, columnar::NativeColumn)> = {
            use rayon::prelude::*;
            self.schema
                .columns
                .par_iter()
                .map(|cdef| {
                    (
                        cdef.id,
                        columnar::rows_to_native(cdef.ty.clone(), &batch, cdef.id),
                    )
                })
                .collect::<Vec<_>>()
        };
        drop(batch);
        // Enforce NOT NULL constraints and primary-key upsert semantics before
        // any row id is allocated or bytes hit the run file. Losers of a
        // duplicate primary key are dropped from the encoded run entirely so
        // the dedup survives reopen (no ephemeral memtable tombstone).
        self.fill_auto_inc_native_columns(&mut user_columns, n)?;
        self.validate_columns_not_null(&user_columns, n)?;
        let winner_idx = self
            .bulk_pk_winner_indices(&user_columns, n)
            .filter(|idx| idx.len() != n);
        let (write_columns, write_n): (Vec<(u16, columnar::NativeColumn)>, usize) =
            match winner_idx.as_deref() {
                Some(idx) => {
                    let compacted = user_columns
                        .iter()
                        .map(|(id, c)| (*id, c.gather(idx)))
                        .collect();
                    (compacted, idx.len())
                }
                None => (std::mem::take(&mut user_columns), n),
            };
        self.advance_auto_inc_from_native_columns(&write_columns, write_n, live_before)?;
        let first = self.allocator.alloc_range(write_n as u64)?.0;
        for rid in first..first + write_n as u64 {
            self.reservoir.offer(rid);
        }
        let run_id = self.alloc_run_id()?;
        let path = self.run_path(run_id);
        let mut writer = RunWriter::new(&self.schema, run_id as u128, epoch, 0)
            .clean(true)
            .with_lz4()
            .with_native_endian();
        if let Some(kek) = &self.kek {
            writer = writer.with_encryption(kek.as_ref(), self.indexable_column_specs());
        }
        let header = match self.create_run_file(run_id)? {
            Some(file) => writer.write_native_file(file, &write_columns, write_n, first)?,
            None => writer.write_native(&path, &write_columns, write_n, first)?,
        };
        self.run_refs.push(RunRef {
            run_id: run_id as u128,
            level: 0,
            epoch_created: epoch.0,
            row_count: header.row_count,
        });
        self.live_count = self.live_count.saturating_add(write_n as u64);
        if eager_index_build {
            let row_ids: Vec<u64> = (first..first + write_n as u64).collect();
            self.index_columns_bulk(&write_columns, &row_ids);
            self.indexes_complete = true;
            self.build_learned_ranges()?;
        } else {
            self.indexes_complete = false;
        }
        self.mark_flushed(epoch)?;
        self.persist_manifest(epoch)?;
        if eager_index_build {
            self.checkpoint_indexes(epoch);
        }
        self.clear_result_cache();
        Ok(epoch)
    }

    /// Rotate the private WAL to a fresh segment. Only valid for a standalone
    /// table — a mounted table never rotates the shared WAL per-table.
    fn rotate_wal(&mut self, epoch: Epoch) -> Result<()> {
        let segment = next_wal_segment(&self.dir.join(WAL_DIR))?;
        let cipher = self.wal_dek.as_ref().map(|dk| make_cipher(dk));
        // The segment number (from the filename) namespaces nonces under the
        // constant WAL DEK — pass it through to the writer.
        let segment_no = segment
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.strip_prefix("seg-"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let mut wal = Wal::create_with_cipher(segment, epoch, cipher, segment_no)?;
        wal.set_sync_byte_threshold(self.sync_byte_threshold);
        wal.sync()?;
        self.wal = WalSink::Private(wal);
        Ok(())
    }

    /// Fine-grained result-cache invalidation (hardening (c)): drop only
    /// entries whose footprint intersects a deleted RowId or whose
    /// condition-columns intersect a mutated column, then clear the pending
    /// sets. Called by `commit` and the cross-table transaction path.
    pub(crate) fn invalidate_pending_cache(&mut self) {
        self.result_cache
            .lock()
            .invalidate(&self.pending_delete_rids, &self.pending_put_cols);
        self.pending_delete_rids.clear();
        self.pending_put_cols.clear();
    }

    pub(crate) fn persist_manifest(&self, epoch: Epoch) -> Result<()> {
        let mut m = Manifest::new(self.table_id, self.schema.schema_id);
        m.current_epoch = epoch.0;
        m.next_row_id = self.allocator.current().0;
        m.runs = self.run_refs.clone();
        m.live_count = self.live_count;
        m.global_idx_epoch = self.global_idx_epoch;
        m.flushed_epoch = self.flushed_epoch;
        m.retiring = self.retiring.clone();
        // Persist the authoritative counter only when seeded; otherwise write 0
        // so the next open still scans `max(PK)` on first use (an unseeded
        // lower bound from WAL replay is not safe to trust across a flush).
        m.auto_inc_next = match self.auto_inc {
            Some(ai) if ai.seeded => ai.next,
            _ => 0,
        };
        m.ttl = self.ttl;
        let meta_dek = self.manifest_meta_dek();
        match self._root_guard.as_deref() {
            Some(root) => manifest::write_durable(root, &mut m, meta_dek.as_ref())?,
            None => manifest::write_atomic(&self.dir, &mut m, meta_dek.as_ref())?,
        }
        Ok(())
    }

    pub(crate) fn plan_recovered_metadata(&mut self) -> Result<RecoveryMetadataPlan> {
        // `live_count` tracks logical tombstones, not wall-clock TTL expiry.
        // Use a time before every representable timestamp so TTL cannot hide a
        // row while rebuilding authoritative manifest metadata.
        let rows = self.visible_rows_at_time(Snapshot::at(Epoch(u64::MAX)), i64::MIN)?;
        let live_count = u64::try_from(rows.len())
            .map_err(|_| MongrelError::Full("table live-row count exceeds u64".into()))?;
        let auto_inc = match self.auto_inc {
            Some(mut state) => {
                let maximum = self.scan_max_int64(state.column_id)?;
                let after_maximum = maximum.checked_add(1).ok_or_else(|| {
                    MongrelError::Full("AUTO_INCREMENT namespace exhausted".into())
                })?;
                state.next = state.next.max(after_maximum).max(1);
                state.seeded = true;
                Some(state)
            }
            None => None,
        };
        Ok(RecoveryMetadataPlan {
            live_count,
            auto_inc,
            changed: live_count != self.live_count
                || auto_inc.is_some_and(|planned| {
                    self.auto_inc.is_none_or(|current| {
                        current.next != planned.next || current.seeded != planned.seeded
                    })
                }),
        })
    }

    pub(crate) fn apply_recovered_metadata(
        &mut self,
        plan: RecoveryMetadataPlan,
        epoch: Epoch,
    ) -> Result<()> {
        if !plan.changed {
            return Ok(());
        }
        self.live_count = plan.live_count;
        self.auto_inc = plan.auto_inc;
        self.persist_manifest(epoch)
    }

    /// Checkpoint the in-memory secondary indexes to `_idx/global.idx` and stamp
    /// the manifest's `global_idx_epoch` (Phase 9.1). Call after the runs are
    /// stable and the memtable is drained (flush/bulk-load/compact) so the
    /// checkpoint exactly matches the run data; subsequent [`Table::open`] loads it
    /// directly instead of scanning every run.
    pub(crate) fn checkpoint_indexes(&mut self, epoch: Epoch) {
        // Never persist an incomplete index set (e.g. after bulk_load_columns,
        // which bypasses per-row indexing) — reopen rebuilds from the runs.
        if !self.indexes_complete {
            return;
        }
        // FND-006: a fired fault behaves like a failed checkpoint — the write
        // is best-effort and the next open simply rebuilds from the runs.
        if crate::catalog::inject_hook("index.publish.before").is_err() {
            return;
        }
        if self.idx_root.is_none() {
            if let Some(root) = self._root_guard.as_ref() {
                let Ok(idx_root) = root.create_directory_all_pinned(global_idx::IDX_DIR) else {
                    return;
                };
                self.idx_root = Some(Arc::new(idx_root));
            }
        }
        let snap = global_idx::IndexSnapshot {
            hot: &self.hot,
            bitmap: &self.bitmap,
            ann: &self.ann,
            fm: &self.fm,
            sparse: &self.sparse,
            minhash: &self.minhash,
            learned_range: &self.learned_range,
        };
        // Best-effort: a failed checkpoint just means the next open rebuilds.
        let idx_dek = self.idx_dek();
        let written = match self.idx_root.as_deref() {
            Some(root) => global_idx::write_atomic_root(
                root,
                self.table_id,
                epoch.0,
                snap,
                idx_dek.as_deref(),
            ),
            None => global_idx::write_atomic(
                &self.dir,
                self.table_id,
                epoch.0,
                snap,
                idx_dek.as_deref(),
            ),
        };
        if written.is_ok() {
            self.global_idx_epoch = epoch.0;
            let _ = self.persist_manifest(epoch);
            // FND-006: the index generation is published.
            let _ = crate::catalog::inject_hook("index.publish.after");
        }
    }

    /// Drop any on-disk index checkpoint so the next open rebuilds from runs
    /// (used when the live indexes are known stale, e.g. compaction to empty).
    pub(crate) fn invalidate_index_checkpoint(&mut self) {
        self.global_idx_epoch = 0;
        if let Some(root) = self.idx_root.as_deref() {
            let _ = root.remove_file(global_idx::IDX_FILENAME);
        } else {
            global_idx::remove(&self.dir);
        }
        let _ = self.persist_manifest(self.epoch.visible());
    }

    /// Prepare for replacing every run without publishing a second manifest.
    /// The caller persists the replacement topology after this returns.  An
    /// older checkpoint may remain on disk if deletion fails, but a manifest
    /// with `global_idx_epoch = 0` will never endorse it on reopen.
    pub(crate) fn prepare_indexes_for_run_replacement(&mut self) {
        self.indexes_complete = false;
        self.global_idx_epoch = 0;
        if let Some(root) = self.idx_root.as_deref() {
            let _ = root.remove_file(global_idx::IDX_FILENAME);
        } else {
            global_idx::remove(&self.dir);
        }
    }

    pub(crate) fn finish_indexes_for_run_replacement(&mut self) {
        self.indexes_complete = true;
    }

    /// A maintenance operation changed live run topology and could not prove
    /// the matching manifest publication.  Fail closed until recovery rebuilds
    /// one coherent view from durable state.  Mounted tables also poison their
    /// owning database so GC, DDL, and transactions cannot continue around the
    /// uncertain topology.
    pub(crate) fn poison_after_maintenance_publish_failure(&mut self) {
        self.durable_commit_failed = true;
        if let WalSink::Shared(shared) = &self.wal {
            shared
                .poisoned
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// Invalidate a stale handle after DOCTOR has durably dropped its catalog
    /// entry. Other tables remain usable, but this handle must never append new
    /// writes for the quarantined table id.
    pub(crate) fn mark_unavailable_after_quarantine(&mut self) {
        self.durable_commit_failed = true;
    }

    /// Read the row at `row_id` visible to `snapshot`, merging the newest
    /// version across the memtable and all sorted runs.
    pub fn get(&self, row_id: RowId, snapshot: Snapshot) -> Option<Row> {
        let mut best: Option<(Epoch, Row)> = self.memtable.get_version(row_id, snapshot.epoch);
        if let Some((epoch, row)) = self.mutable_run.get_version(row_id, snapshot.epoch) {
            if best.as_ref().map(|(be, _)| epoch > *be).unwrap_or(true) {
                best = Some((epoch, row));
            }
        }
        for rr in &self.run_refs {
            let Ok(mut reader) = self.open_reader(rr.run_id) else {
                continue;
            };
            let Ok(Some((epoch, row))) = reader.get_version(row_id, snapshot.epoch) else {
                continue;
            };
            if best.as_ref().map(|(be, _)| epoch > *be).unwrap_or(true) {
                best = Some((epoch, row));
            }
        }
        let now_nanos = unix_nanos_now();
        match best {
            Some((_, r)) if r.deleted || self.row_expired_at(&r, now_nanos) => None,
            Some((_, r)) => Some(r),
            None => None,
        }
    }

    /// All rows visible at `snapshot` (newest version per `RowId`, tombstones
    /// dropped), merged across the memtable, the mutable-run tier, and all
    /// runs. Ascending `RowId`.
    pub fn visible_rows(&self, snapshot: Snapshot) -> Result<Vec<Row>> {
        self.visible_rows_at_time(snapshot, unix_nanos_now())
    }

    /// Materialize visible rows with cooperative checkpoints while merging
    /// page-bounded, already ordered tier cursors.
    #[doc(hidden)]
    pub fn visible_rows_controlled(
        &self,
        snapshot: Snapshot,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<Row>> {
        let mut out = Vec::new();
        self.for_each_visible_row_controlled(snapshot, control, |row| {
            out.push(row);
            Ok(())
        })?;
        Ok(out)
    }

    /// Visit visible rows in row-id order with a k-way merge over ordered tier
    /// cursors. No full-table merge map or row-id sort is constructed.
    #[doc(hidden)]
    pub fn for_each_visible_row_controlled<F>(
        &self,
        snapshot: Snapshot,
        control: &crate::ExecutionControl,
        visit: F,
    ) -> Result<()>
    where
        F: FnMut(Row) -> Result<()>,
    {
        let mut sources = Vec::with_capacity(self.run_refs.len() + 2);
        control.checkpoint()?;
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        if !memtable.is_empty() {
            sources.push(ControlledVisibleSource::memory(memtable));
        }
        control.checkpoint()?;
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        if !mutable.is_empty() {
            sources.push(ControlledVisibleSource::memory(mutable));
        }
        for run in &self.run_refs {
            control.checkpoint()?;
            let reader = self.open_reader(run.run_id)?;
            sources.push(ControlledVisibleSource::run(
                reader.into_visible_version_cursor(snapshot.epoch)?,
            ));
        }
        let now_nanos = unix_nanos_now();
        merge_controlled_visible_sources(
            &mut sources,
            control,
            |row| self.row_expired_at(row, now_nanos),
            visit,
        )
    }

    #[doc(hidden)]
    pub fn visible_rows_at_time(&self, snapshot: Snapshot, now_nanos: i64) -> Result<Vec<Row>> {
        let mut best: HashMap<u64, (Epoch, Row)> = HashMap::new();
        let mut fold = |row: Row| {
            best.entry(row.row_id.0)
                .and_modify(|e| {
                    if row.committed_epoch > e.0 {
                        *e = (row.committed_epoch, row.clone());
                    }
                })
                .or_insert_with(|| (row.committed_epoch, row));
        };
        for row in self.memtable.visible_versions(snapshot.epoch) {
            fold(row);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            fold(row);
        }
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            for row in reader.visible_versions(snapshot.epoch)? {
                fold(row);
            }
        }
        let mut out: Vec<Row> = best
            .into_values()
            .filter_map(|(_, r)| {
                if r.deleted || self.row_expired_at(&r, now_nanos) {
                    None
                } else {
                    Some(r)
                }
            })
            .collect();
        out.sort_by_key(|r| r.row_id);
        Ok(out)
    }

    /// Visible data as columns (column_id → values) rather than rows — the
    /// vectorized scan path. Fast path: when the memtable is empty and there is
    /// exactly one run (the common post-flush analytical case), it computes the
    /// visible index set once and gathers each column, with **no per-row
    /// `HashMap`/`Row` materialization**. Falls back to [`Self::visible_rows`]
    /// pivoted to columns when the memtable is live or runs overlap.
    pub fn visible_columns(&self, snapshot: Snapshot) -> Result<Vec<(u16, Vec<Value>)>> {
        if self.ttl.is_none()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty()
            && self.run_refs.len() == 1
        {
            let rr = self.run_refs[0].clone();
            let mut reader = self.open_reader(rr.run_id)?;
            let idxs = reader.visible_indices(snapshot.epoch)?;
            let mut cols = Vec::with_capacity(self.schema.columns.len());
            for cdef in &self.schema.columns {
                cols.push((cdef.id, reader.gather_column(cdef.id, &idxs)?));
            }
            return Ok(cols);
        }
        // Fallback: row merge, then pivot to columns.
        let rows = self.visible_rows(snapshot)?;
        let mut cols: Vec<(u16, Vec<Value>)> = self
            .schema
            .columns
            .iter()
            .map(|c| (c.id, Vec::with_capacity(rows.len())))
            .collect();
        for r in &rows {
            for (cid, vec) in cols.iter_mut() {
                vec.push(r.columns.get(cid).cloned().unwrap_or(Value::Null));
            }
        }
        Ok(cols)
    }

    /// Resolve a primary-key value to a row id (latest version).
    pub fn lookup_pk(&self, key: &[u8]) -> Option<RowId> {
        let row_id = self.hot.get(key)?;
        if self.ttl.is_none() || self.get(row_id, Snapshot::at(Epoch(u64::MAX))).is_some() {
            Some(row_id)
        } else {
            None
        }
    }

    /// Run a conjunctive query over the shared row-id space: each condition
    /// yields a candidate row-id set, the sets are intersected, and the
    /// survivors are materialized at the current snapshot. This is the AI-native
    /// "compose primitives" surface (`semsearch ∩ fm_contains ∩ cat_in`).
    pub fn query(&mut self, q: &crate::query::Query) -> Result<Vec<Row>> {
        self.query_at_with_allowed(q, self.snapshot(), None)
    }

    /// Run a native conjunctive query with cooperative cancellation through
    /// index resolution, scans, filtering, and row materialization.
    pub fn query_controlled(
        &mut self,
        q: &crate::query::Query,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<Row>> {
        self.query_at_with_allowed_controlled(q, self.snapshot(), None, control)
    }

    /// Execute a conjunctive query at one snapshot, applying authorization
    /// before ranked ANN, Sparse, and MinHash top-k selection.
    pub fn query_at_with_allowed(
        &mut self,
        q: &crate::query::Query,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<Row>> {
        self.query_at_with_allowed_after(q, snapshot, allowed, None)
    }

    #[doc(hidden)]
    pub fn query_at_with_allowed_controlled(
        &mut self,
        q: &crate::query::Query,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<Row>> {
        self.require_select()?;
        self.ensure_indexes_complete_controlled(control, || true)?;
        self.validate_native_query(q)?;
        self.query_conditions_at(
            &q.conditions,
            snapshot,
            allowed,
            q.limit,
            q.offset,
            None,
            unix_nanos_now(),
            Some(control),
        )
    }

    #[doc(hidden)]
    pub fn query_at_with_allowed_after(
        &mut self,
        q: &crate::query::Query,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        after_row_id: Option<RowId>,
    ) -> Result<Vec<Row>> {
        self.query_at_with_allowed_after_at_time(
            q,
            snapshot,
            allowed,
            after_row_id,
            unix_nanos_now(),
        )
    }

    #[doc(hidden)]
    pub fn query_at_with_allowed_after_at_time(
        &mut self,
        q: &crate::query::Query,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        after_row_id: Option<RowId>,
        query_time_nanos: i64,
    ) -> Result<Vec<Row>> {
        self.require_select()?;
        self.ensure_indexes_complete()?;
        self.validate_native_query(q)?;
        self.query_conditions_at(
            &q.conditions,
            snapshot,
            allowed,
            q.limit,
            q.offset,
            after_row_id,
            query_time_nanos,
            None,
        )
    }

    fn validate_native_query(&self, q: &crate::query::Query) -> Result<()> {
        if q.conditions.len() > crate::query::MAX_HARD_CONDITIONS {
            return Err(MongrelError::InvalidArgument(format!(
                "query exceeds {} conditions",
                crate::query::MAX_HARD_CONDITIONS
            )));
        }
        if let Some(limit) = q.limit {
            if limit == 0 || limit > crate::query::MAX_FINAL_LIMIT {
                return Err(MongrelError::InvalidArgument(format!(
                    "query limit must be between 1 and {}",
                    crate::query::MAX_FINAL_LIMIT
                )));
            }
        }
        if q.offset > crate::query::MAX_QUERY_OFFSET {
            return Err(MongrelError::InvalidArgument(format!(
                "query offset exceeds {}",
                crate::query::MAX_QUERY_OFFSET
            )));
        }
        Ok(())
    }

    /// Unbounded internal SQL join helper. Public request surfaces must use
    /// [`Self::query_at_with_allowed`] and its result ceiling.
    #[doc(hidden)]
    pub fn query_all_at(
        &mut self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Vec<Row>> {
        self.require_select()?;
        self.ensure_indexes_complete()?;
        if conditions.len() > crate::query::MAX_HARD_CONDITIONS {
            return Err(MongrelError::InvalidArgument(format!(
                "query exceeds {} conditions",
                crate::query::MAX_HARD_CONDITIONS
            )));
        }
        self.query_conditions_at(
            conditions,
            snapshot,
            None,
            None,
            0,
            None,
            unix_nanos_now(),
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn query_conditions_at(
        &self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        limit: Option<usize>,
        offset: usize,
        after_row_id: Option<RowId>,
        query_time_nanos: i64,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Vec<Row>> {
        control
            .map(crate::ExecutionControl::checkpoint)
            .transpose()?;
        crate::trace::QueryTrace::record(|t| {
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
        });
        // A conjunction with no predicates matches every visible row (the
        // documented "Empty ⇒ all rows" contract); `intersect_sets` of zero
        // sets would otherwise wrongly yield the empty set.
        if conditions.is_empty() {
            crate::trace::QueryTrace::record(|t| {
                t.scan_mode = crate::trace::ScanMode::Materialized;
                t.row_materialized = true;
            });
            let mut rows = match control {
                Some(control) => self.visible_rows_controlled(snapshot, control)?,
                None => self.visible_rows_at_time(snapshot, query_time_nanos)?,
            };
            if let Some(allowed) = allowed {
                let mut filtered = Vec::with_capacity(rows.len());
                for (index, row) in rows.into_iter().enumerate() {
                    if index & 255 == 0 {
                        control
                            .map(crate::ExecutionControl::checkpoint)
                            .transpose()?;
                    }
                    if allowed.contains(&row.row_id) {
                        filtered.push(row);
                    }
                }
                rows = filtered;
            }
            if let Some(after_row_id) = after_row_id {
                rows.retain(|row| row.row_id > after_row_id);
            }
            rows.drain(..offset.min(rows.len()));
            if let Some(limit) = limit {
                rows.truncate(limit);
            }
            return Ok(rows);
        }
        crate::trace::QueryTrace::record(|t| {
            t.conditions_pushed = conditions.len();
            t.scan_mode = crate::trace::ScanMode::Materialized;
            t.row_materialized = true;
        });
        // §5.5: resolve conditions CHEAP-FIRST and early-exit the moment a
        // condition yields an empty survivor set. Previously every condition
        // (including an expensive range/FM page scan) was resolved before
        // `intersect_many` noticed an empty set; now a selective bitmap/PK that
        // eliminates all rows short-circuits the rest. Correctness is unchanged
        // (intersection with an empty set is empty either way).
        let mut ordered: Vec<&crate::query::Condition> = conditions.iter().collect();
        ordered.sort_by_key(|c| condition_cost_rank(c));
        let mut sets: Vec<RowIdSet> = Vec::with_capacity(ordered.len());
        for c in &ordered {
            control
                .map(crate::ExecutionControl::checkpoint)
                .transpose()?;
            let s = self.resolve_condition_with_allowed(c, snapshot, allowed)?;
            let empty = s.is_empty();
            sets.push(s);
            if empty {
                break;
            }
        }
        let mut rids = RowIdSet::intersect_many(sets).into_sorted_vec();
        if let Some(allowed) = allowed {
            rids.retain(|row_id| allowed.contains(&RowId(*row_id)));
        }
        if let Some(after_row_id) = after_row_id {
            let first = rids.partition_point(|row_id| *row_id <= after_row_id.0);
            rids.drain(..first);
        }
        rids.drain(..offset.min(rids.len()));
        if let Some(limit) = limit {
            rids.truncate(limit);
        }
        control
            .map(crate::ExecutionControl::checkpoint)
            .transpose()?;
        self.rows_for_rids_at_time(&rids, snapshot, query_time_nanos, control)
    }

    /// Return an index's ordered candidates without discarding scores.
    pub fn retrieve(
        &mut self,
        retriever: &crate::query::Retriever,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.retrieve_with_allowed(retriever, None)
    }

    pub fn retrieve_at(
        &mut self,
        retriever: &crate::query::Retriever,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.retrieve_at_with_allowed(retriever, snapshot, allowed)
    }

    /// Scored retrieval restricted to caller-authorized row IDs. Core MVCC,
    /// tombstone, and TTL eligibility is always applied before ranking.
    pub fn retrieve_with_allowed(
        &mut self,
        retriever: &crate::query::Retriever,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.retrieve_at_with_allowed(retriever, self.snapshot(), allowed)
    }

    pub fn retrieve_at_with_allowed(
        &mut self,
        retriever: &crate::query::Retriever,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.retrieve_at_with_allowed_and_context(retriever, snapshot, allowed, None)
    }

    pub fn retrieve_at_with_allowed_and_context(
        &mut self,
        retriever: &crate::query::Retriever,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.require_select()?;
        self.ensure_indexes_complete()?;
        self.validate_retriever(retriever)?;
        self.retrieve_filtered(retriever, snapshot, None, allowed, None, context)
    }

    pub fn retrieve_at_with_candidate_authorization_and_context(
        &mut self,
        retriever: &crate::query::Retriever,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.require_select()?;
        self.ensure_indexes_complete()?;
        self.retrieve_at_with_candidate_authorization_on_generation(
            retriever,
            snapshot,
            authorization,
            context,
        )
    }

    #[doc(hidden)]
    pub fn retrieve_at_with_candidate_authorization_on_generation(
        &self,
        retriever: &crate::query::Retriever,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        self.require_select()?;
        self.validate_retriever(retriever)?;
        self.retrieve_filtered(retriever, snapshot, None, None, authorization, context)
    }

    fn validate_retriever(&self, retriever: &crate::query::Retriever) -> Result<()> {
        use crate::query::{Retriever, MAX_RETRIEVER_K, MAX_SET_MEMBERS, MAX_SPARSE_TERMS};
        let (column_id, k) = match retriever {
            Retriever::Ann {
                column_id,
                query,
                k,
            } => {
                let index = self.ann.get(column_id).ok_or_else(|| {
                    MongrelError::InvalidArgument(format!("column {column_id} has no ANN index"))
                })?;
                if query.len() != index.dim() {
                    return Err(MongrelError::InvalidArgument(format!(
                        "ANN query dimension must be {}, got {}",
                        index.dim(),
                        query.len()
                    )));
                }
                if query.iter().any(|value| !value.is_finite()) {
                    return Err(MongrelError::InvalidArgument(
                        "ANN query values must be finite".into(),
                    ));
                }
                (*column_id, *k)
            }
            Retriever::Sparse {
                column_id,
                query,
                k,
            } => {
                if !self.sparse.contains_key(column_id) {
                    return Err(MongrelError::InvalidArgument(format!(
                        "column {column_id} has no Sparse index"
                    )));
                }
                if query.is_empty() || query.iter().any(|(_, weight)| !weight.is_finite()) {
                    return Err(MongrelError::InvalidArgument(
                        "Sparse query must be non-empty with finite weights".into(),
                    ));
                }
                if query.len() > MAX_SPARSE_TERMS {
                    return Err(MongrelError::InvalidArgument(format!(
                        "Sparse query exceeds {MAX_SPARSE_TERMS} terms"
                    )));
                }
                (*column_id, *k)
            }
            Retriever::MinHash {
                column_id,
                members,
                k,
            } => {
                if !self.minhash.contains_key(column_id) {
                    return Err(MongrelError::InvalidArgument(format!(
                        "column {column_id} has no MinHash index"
                    )));
                }
                if members.is_empty() {
                    return Err(MongrelError::InvalidArgument(
                        "MinHash members must not be empty".into(),
                    ));
                }
                if members.len() > MAX_SET_MEMBERS {
                    return Err(MongrelError::InvalidArgument(format!(
                        "MinHash query exceeds {MAX_SET_MEMBERS} members"
                    )));
                }
                let mut total_bytes = 0usize;
                for member in members {
                    let bytes = member.encoded_len();
                    if bytes > crate::query::MAX_SET_MEMBER_BYTES {
                        return Err(MongrelError::InvalidArgument(format!(
                            "MinHash member exceeds {} bytes",
                            crate::query::MAX_SET_MEMBER_BYTES
                        )));
                    }
                    total_bytes = total_bytes.checked_add(bytes).ok_or_else(|| {
                        MongrelError::InvalidArgument("MinHash input size overflow".into())
                    })?;
                }
                if total_bytes > crate::query::MAX_SET_INPUT_BYTES {
                    return Err(MongrelError::InvalidArgument(format!(
                        "MinHash input exceeds {} bytes",
                        crate::query::MAX_SET_INPUT_BYTES
                    )));
                }
                (*column_id, *k)
            }
        };
        if k == 0 {
            return Err(MongrelError::InvalidArgument(
                "retriever k must be > 0".into(),
            ));
        }
        if k > MAX_RETRIEVER_K {
            return Err(MongrelError::InvalidArgument(format!(
                "retriever k exceeds {MAX_RETRIEVER_K}"
            )));
        }
        debug_assert!(self
            .schema
            .columns
            .iter()
            .any(|column| column.id == column_id));
        Ok(())
    }

    fn validate_condition(&self, condition: &crate::query::Condition) -> Result<()> {
        use crate::query::Condition;
        match condition {
            Condition::Ann {
                column_id,
                query,
                k,
            } => self.validate_retriever(&crate::query::Retriever::Ann {
                column_id: *column_id,
                query: query.clone(),
                k: *k,
            }),
            Condition::SparseMatch {
                column_id,
                query,
                k,
            } => self.validate_retriever(&crate::query::Retriever::Sparse {
                column_id: *column_id,
                query: query.clone(),
                k: *k,
            }),
            Condition::MinHashSimilar {
                column_id,
                query,
                k,
            } => {
                if !self.minhash.contains_key(column_id) {
                    return Err(MongrelError::InvalidArgument(format!(
                        "column {column_id} has no MinHash index"
                    )));
                }
                if query.is_empty() || *k == 0 {
                    return Err(MongrelError::InvalidArgument(
                        "MinHash query must be non-empty and k must be > 0".into(),
                    ));
                }
                if query.len() > crate::query::MAX_SET_MEMBERS || *k > crate::query::MAX_RETRIEVER_K
                {
                    return Err(MongrelError::InvalidArgument(format!(
                        "MinHash query must have <= {} members and k <= {}",
                        crate::query::MAX_SET_MEMBERS,
                        crate::query::MAX_RETRIEVER_K
                    )));
                }
                Ok(())
            }
            Condition::BitmapIn { values, .. } if values.len() > crate::query::MAX_SET_MEMBERS => {
                Err(MongrelError::InvalidArgument(format!(
                    "bitmap IN exceeds {} values",
                    crate::query::MAX_SET_MEMBERS
                )))
            }
            Condition::FmContainsAll { patterns, .. }
                if patterns.len() > crate::query::MAX_HARD_CONDITIONS =>
            {
                Err(MongrelError::InvalidArgument(format!(
                    "FM query exceeds {} patterns",
                    crate::query::MAX_HARD_CONDITIONS
                )))
            }
            _ => Ok(()),
        }
    }

    fn retrieve_filtered(
        &self,
        retriever: &crate::query::Retriever,
        snapshot: Snapshot,
        hard_filter: Option<&RowIdSet>,
        allowed: Option<&std::collections::HashSet<RowId>>,
        candidate_authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::RetrieverHit>> {
        use crate::query::{Retriever, RetrieverHit, RetrieverScore};
        let started = std::time::Instant::now();
        let scored: Vec<(RowId, RetrieverScore)> = match retriever {
            Retriever::Ann {
                column_id,
                query,
                k,
            } => {
                let Some(index) = self.ann.get(column_id) else {
                    return Ok(Vec::new());
                };
                let cap = ann_candidate_cap(index.len(), context);
                if cap == 0 {
                    return Ok(Vec::new());
                }
                let mut breadth = (*k).max(1).min(cap);
                let mut eligibility = std::collections::HashMap::new();
                let mut filtered = loop {
                    let mut seen = std::collections::HashSet::new();
                    if let Some(context) = context {
                        context.checkpoint()?;
                    }
                    let raw = index.search_with_context(query, breadth, context)?;
                    let unchecked: Vec<_> = raw
                        .iter()
                        .map(|(row_id, _)| *row_id)
                        .filter(|row_id| !eligibility.contains_key(row_id))
                        .filter(|row_id| {
                            hard_filter.is_none_or(|filter| filter.contains(row_id.0))
                                && allowed.is_none_or(|allowed| allowed.contains(row_id))
                        })
                        .collect();
                    let eligible = self.eligible_and_authorized_candidate_ids(
                        &unchecked,
                        *column_id,
                        snapshot,
                        candidate_authorization,
                        context,
                    )?;
                    for row_id in unchecked {
                        eligibility.insert(row_id, eligible.contains(&row_id));
                    }
                    let filtered: Vec<_> = raw
                        .into_iter()
                        .filter(|(row_id, _)| {
                            seen.insert(*row_id)
                                && eligibility.get(row_id).copied().unwrap_or(false)
                        })
                        .map(|(row_id, score)| (row_id, RetrieverScore::AnnHammingDistance(score)))
                        .collect();
                    if filtered.len() >= *k || breadth >= cap {
                        if filtered.len() < *k && index.len() > cap && breadth >= cap {
                            crate::trace::QueryTrace::record(|trace| {
                                trace.ann_candidate_cap_hit = true;
                            });
                        }
                        break filtered;
                    }
                    breadth = breadth.saturating_mul(2).min(cap);
                };
                filtered.truncate(*k);
                filtered
            }
            Retriever::Sparse {
                column_id,
                query,
                k,
            } => self
                .sparse
                .get(column_id)
                .map(|index| -> Result<Vec<_>> {
                    let mut breadth = (*k).max(1);
                    let mut eligibility = std::collections::HashMap::new();
                    loop {
                        if let Some(context) = context {
                            context.checkpoint()?;
                        }
                        let raw = index.search_with_context(query, breadth, context)?;
                        let unchecked: Vec<_> = raw
                            .iter()
                            .map(|(row_id, _)| *row_id)
                            .filter(|row_id| !eligibility.contains_key(row_id))
                            .filter(|row_id| {
                                hard_filter.is_none_or(|filter| filter.contains(row_id.0))
                                    && allowed.is_none_or(|allowed| allowed.contains(row_id))
                            })
                            .collect();
                        let eligible = self.eligible_and_authorized_candidate_ids(
                            &unchecked,
                            *column_id,
                            snapshot,
                            candidate_authorization,
                            context,
                        )?;
                        for row_id in unchecked {
                            eligibility.insert(row_id, eligible.contains(&row_id));
                        }
                        let filtered: Vec<_> = raw
                            .iter()
                            .filter(|(row_id, _)| eligibility.get(row_id).copied().unwrap_or(false))
                            .take(*k)
                            .map(|(row_id, score)| {
                                (*row_id, RetrieverScore::SparseDotProduct(*score))
                            })
                            .collect();
                        if filtered.len() >= *k || raw.len() < breadth {
                            break Ok(filtered);
                        }
                        let next = breadth.saturating_mul(2);
                        if next == breadth {
                            break Ok(filtered);
                        }
                        breadth = next;
                    }
                })
                .transpose()?
                .unwrap_or_default(),
            Retriever::MinHash {
                column_id,
                members,
                k,
            } => self
                .minhash
                .get(column_id)
                .map(|index| -> Result<Vec<_>> {
                    let mut hashes = Vec::with_capacity(members.len());
                    for member in members {
                        if let Some(context) = context {
                            context.consume(crate::query::work_units(
                                member.encoded_len(),
                                crate::query::PARSE_WORK_QUANTUM,
                            ))?;
                        }
                        hashes.push(member.hash_v1());
                    }
                    let mut breadth = (*k).max(1);
                    let mut eligibility = std::collections::HashMap::new();
                    loop {
                        if let Some(context) = context {
                            context.checkpoint()?;
                        }
                        let raw = index.search_with_context(&hashes, breadth, context)?;
                        let unchecked: Vec<_> = raw
                            .iter()
                            .map(|(row_id, _)| *row_id)
                            .filter(|row_id| !eligibility.contains_key(row_id))
                            .filter(|row_id| {
                                hard_filter.is_none_or(|filter| filter.contains(row_id.0))
                                    && allowed.is_none_or(|allowed| allowed.contains(row_id))
                            })
                            .collect();
                        let eligible = self.eligible_and_authorized_candidate_ids(
                            &unchecked,
                            *column_id,
                            snapshot,
                            candidate_authorization,
                            context,
                        )?;
                        for row_id in unchecked {
                            eligibility.insert(row_id, eligible.contains(&row_id));
                        }
                        let filtered: Vec<_> = raw
                            .iter()
                            .filter(|(row_id, _)| eligibility.get(row_id).copied().unwrap_or(false))
                            .take(*k)
                            .map(|(row_id, score)| {
                                (*row_id, RetrieverScore::MinHashEstimatedJaccard(*score))
                            })
                            .collect();
                        if filtered.len() >= *k || raw.len() < breadth {
                            break Ok(filtered);
                        }
                        let next = breadth.saturating_mul(2);
                        if next == breadth {
                            break Ok(filtered);
                        }
                        breadth = next;
                    }
                })
                .transpose()?
                .unwrap_or_default(),
        };
        let elapsed = started.elapsed().as_nanos() as u64;
        crate::trace::QueryTrace::record(|trace| {
            match retriever {
                Retriever::Ann { .. } => {
                    trace.ann_candidate_nanos = trace.ann_candidate_nanos.saturating_add(elapsed)
                }
                Retriever::Sparse { .. } => {
                    trace.sparse_candidate_nanos =
                        trace.sparse_candidate_nanos.saturating_add(elapsed)
                }
                Retriever::MinHash { .. } => {
                    trace.minhash_candidate_nanos =
                        trace.minhash_candidate_nanos.saturating_add(elapsed)
                }
            }
            trace.candidate_count = trace.candidate_count.saturating_add(scored.len());
        });
        Ok(scored
            .into_iter()
            .enumerate()
            .map(|(rank, (row_id, score))| RetrieverHit {
                row_id,
                rank: rank + 1,
                score,
            })
            .collect())
    }

    fn eligible_candidate_ids(
        &self,
        candidates: &[RowId],
        _column_id: u16,
        snapshot: Snapshot,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<std::collections::HashSet<RowId>> {
        if !self.had_deletes
            && self.ttl.is_none()
            && self.pending_put_cols.is_empty()
            && snapshot.epoch == self.snapshot().epoch
        {
            return Ok(candidates.iter().copied().collect());
        }
        let mut readers: Vec<_> = self
            .run_refs
            .iter()
            .map(|run| self.open_reader(run.run_id))
            .collect::<Result<_>>()?;
        let now = context.map_or_else(unix_nanos_now, |context| context.query_time_nanos());
        let mut eligible = std::collections::HashSet::with_capacity(candidates.len());
        for &row_id in candidates {
            if let Some(context) = context {
                context.consume(1)?;
            }
            let mem = self.memtable.get_version(row_id, snapshot.epoch);
            let mutable = self.mutable_run.get_version(row_id, snapshot.epoch);
            let overlay = match (mem, mutable) {
                (Some(left), Some(right)) => Some(if left.0 >= right.0 { left } else { right }),
                (Some(value), None) | (None, Some(value)) => Some(value),
                (None, None) => None,
            };
            if let Some((_, row)) = overlay {
                if !row.deleted && !self.row_expired_at(&row, now) {
                    eligible.insert(row_id);
                }
                continue;
            }
            let mut best: Option<(Epoch, bool, usize)> = None;
            for (index, reader) in readers.iter_mut().enumerate() {
                if let Some((epoch, deleted)) =
                    reader.get_version_visibility(row_id, snapshot.epoch)?
                {
                    if best
                        .as_ref()
                        .map(|(best_epoch, ..)| epoch > *best_epoch)
                        .unwrap_or(true)
                    {
                        best = Some((epoch, deleted, index));
                    }
                }
            }
            let Some((_, false, reader_index)) = best else {
                continue;
            };
            if let Some(ttl) = self.ttl {
                if let Some((_, _, Some(Value::Int64(timestamp)))) = readers[reader_index]
                    .get_version_column(row_id, snapshot.epoch, ttl.column_id)?
                {
                    if timestamp.saturating_add(ttl.duration_nanos as i64) <= now {
                        continue;
                    }
                }
            }
            eligible.insert(row_id);
        }
        Ok(eligible)
    }

    fn eligible_and_authorized_candidate_ids(
        &self,
        candidates: &[RowId],
        column_id: u16,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<std::collections::HashSet<RowId>> {
        let eligible = self.eligible_candidate_ids(candidates, column_id, snapshot, context)?;
        let Some(authorization) = authorization else {
            return Ok(eligible);
        };
        let candidates: Vec<_> = eligible.into_iter().collect();
        self.policy_allowed_candidate_ids(&candidates, snapshot, authorization, context)
    }

    fn policy_allowed_candidate_ids(
        &self,
        candidates: &[RowId],
        snapshot: Snapshot,
        authorization: &crate::security::CandidateAuthorization<'_>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<std::collections::HashSet<RowId>> {
        let started = std::time::Instant::now();
        if candidates.is_empty()
            || authorization.principal.is_admin
            || !authorization.security.rls_enabled(authorization.table)
        {
            return Ok(candidates.iter().copied().collect());
        }
        if let Some(context) = context {
            context.checkpoint()?;
        }
        let row_ids: Vec<_> = candidates.iter().map(|row_id| row_id.0).collect();
        let mut rows: std::collections::HashMap<RowId, Row> = candidates
            .iter()
            .map(|row_id| {
                (
                    *row_id,
                    Row {
                        row_id: *row_id,
                        committed_epoch: snapshot.epoch,
                        columns: std::collections::HashMap::new(),
                        deleted: false,
                    },
                )
            })
            .collect();
        let columns = authorization
            .security
            .select_policy_columns(authorization.table, authorization.principal);
        let query_now = context.map_or_else(unix_nanos_now, |context| context.query_time_nanos());
        let mut decoded = 0usize;
        for column_id in &columns {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            for (row_id, value) in self.values_for_rids_batch_at_with_context(
                &row_ids, *column_id, snapshot, query_now, context,
            )? {
                if let Some(row) = rows.get_mut(&row_id) {
                    row.columns.insert(*column_id, value);
                    decoded = decoded.saturating_add(1);
                }
            }
        }
        if let Some(context) = context {
            context.consume(candidates.len().saturating_add(decoded))?;
        }
        let allowed = rows
            .into_values()
            .filter_map(|row| {
                authorization
                    .security
                    .row_allowed(
                        authorization.table,
                        crate::security::PolicyCommand::Select,
                        &row,
                        authorization.principal,
                        false,
                    )
                    .then_some(row.row_id)
            })
            .collect();
        crate::trace::QueryTrace::record(|trace| {
            trace.rls_rows_evaluated = trace.rls_rows_evaluated.saturating_add(candidates.len());
            trace.rls_policy_columns_decoded =
                trace.rls_policy_columns_decoded.saturating_add(decoded);
            trace.authorization_nanos = trace
                .authorization_nanos
                .saturating_add(started.elapsed().as_nanos() as u64);
        });
        Ok(allowed)
    }

    /// Filter-aware union and reciprocal-rank fusion over scored retrievers.
    pub fn search(
        &mut self,
        request: &crate::query::SearchRequest,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.search_with_allowed(request, None)
    }

    pub fn search_at(
        &mut self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorized: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.search_at_with_allowed(request, snapshot, authorized)
    }

    pub fn search_with_allowed(
        &mut self,
        request: &crate::query::SearchRequest,
        authorized: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.search_at_with_allowed(request, self.snapshot(), authorized)
    }

    pub fn search_at_with_allowed(
        &mut self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorized: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.search_at_with_allowed_and_context(request, snapshot, authorized, None)
    }

    pub fn search_at_with_allowed_and_context(
        &mut self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorized: Option<&std::collections::HashSet<RowId>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.ensure_indexes_complete()?;
        self.search_at_with_filters_and_context(request, snapshot, authorized, None, context, None)
    }

    pub fn search_at_with_candidate_authorization_and_context(
        &mut self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.ensure_indexes_complete()?;
        self.search_at_with_filters_and_context(
            request,
            snapshot,
            None,
            authorization,
            context,
            None,
        )
    }

    #[doc(hidden)]
    pub fn search_at_with_candidate_authorization_on_generation(
        &self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.search_at_with_filters_and_context(
            request,
            snapshot,
            None,
            authorization,
            context,
            None,
        )
    }

    #[doc(hidden)]
    pub fn search_at_with_candidate_authorization_on_generation_after(
        &self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
        after: Option<crate::query::SearchAfter>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        self.search_at_with_filters_and_context(
            request,
            snapshot,
            None,
            authorization,
            context,
            after,
        )
    }

    fn search_at_with_filters_and_context(
        &self,
        request: &crate::query::SearchRequest,
        snapshot: Snapshot,
        authorized: Option<&std::collections::HashSet<RowId>>,
        candidate_authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
        after: Option<crate::query::SearchAfter>,
    ) -> Result<Vec<crate::query::SearchHit>> {
        use crate::query::{
            ComponentScore, Condition, Fusion, SearchHit, MAX_FINAL_LIMIT, MAX_HARD_CONDITIONS,
            MAX_PROJECTION_COLUMNS, MAX_RETRIEVERS, MAX_RETRIEVER_WEIGHT,
        };
        let total_started = std::time::Instant::now();
        let rank_offset = after.map_or(0, |after| after.returned_count);
        self.require_select()?;
        if request.limit == 0 {
            return Err(MongrelError::InvalidArgument(
                "search limit must be > 0".into(),
            ));
        }
        if request.limit > MAX_FINAL_LIMIT {
            return Err(MongrelError::InvalidArgument(format!(
                "search limit exceeds {MAX_FINAL_LIMIT}"
            )));
        }
        if after.is_some_and(|cursor| !cursor.final_score.is_finite()) {
            return Err(MongrelError::InvalidArgument(
                "search-after score must be finite".into(),
            ));
        }
        if request.retrievers.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "search requires at least one retriever".into(),
            ));
        }
        if request.retrievers.len() > MAX_RETRIEVERS {
            return Err(MongrelError::InvalidArgument(format!(
                "search exceeds {MAX_RETRIEVERS} retrievers"
            )));
        }
        if request.must.len() > MAX_HARD_CONDITIONS {
            return Err(MongrelError::InvalidArgument(format!(
                "search exceeds {MAX_HARD_CONDITIONS} hard conditions"
            )));
        }
        for condition in &request.must {
            self.validate_condition(condition)?;
        }
        if request.must.iter().any(|condition| {
            matches!(
                condition,
                Condition::Ann { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::MinHashSimilar { .. }
            )
        }) {
            return Err(MongrelError::InvalidArgument(
                "ranked ANN, Sparse, and MinHash conditions must be retrievers, not must filters"
                    .into(),
            ));
        }
        let mut names = std::collections::HashSet::new();
        for named in &request.retrievers {
            if named.name.is_empty()
                || named.name.len() > crate::query::MAX_RETRIEVER_NAME_BYTES
                || !names.insert(named.name.as_str())
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "retriever names must be non-empty, unique, and at most {} UTF-8 bytes",
                    crate::query::MAX_RETRIEVER_NAME_BYTES
                )));
            }
            if !named.weight.is_finite()
                || named.weight < 0.0
                || named.weight > MAX_RETRIEVER_WEIGHT
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "retriever weight must be finite, non-negative, and <= {MAX_RETRIEVER_WEIGHT}"
                )));
            }
            self.validate_retriever(&named.retriever)?;
        }
        let projection = request
            .projection
            .clone()
            .unwrap_or_else(|| self.schema.columns.iter().map(|column| column.id).collect());
        if projection.len() > MAX_PROJECTION_COLUMNS {
            return Err(MongrelError::InvalidArgument(format!(
                "projection exceeds {MAX_PROJECTION_COLUMNS} columns"
            )));
        }
        for column_id in &projection {
            if !self
                .schema
                .columns
                .iter()
                .any(|column| column.id == *column_id)
            {
                return Err(MongrelError::ColumnNotFound(column_id.to_string()));
            }
        }
        if let Some(crate::query::Rerank::ExactVector {
            embedding_column,
            query,
            candidate_limit,
            weight,
            ..
        }) = &request.rerank
        {
            if *candidate_limit < request.limit || *candidate_limit > crate::query::MAX_RETRIEVER_K
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "rerank candidate_limit must be between search limit and {}",
                    crate::query::MAX_RETRIEVER_K
                )));
            }
            if !weight.is_finite() || *weight < 0.0 || *weight > MAX_RETRIEVER_WEIGHT {
                return Err(MongrelError::InvalidArgument(format!(
                    "rerank weight must be finite, non-negative, and <= {MAX_RETRIEVER_WEIGHT}"
                )));
            }
            let column = self
                .schema
                .columns
                .iter()
                .find(|column| column.id == *embedding_column)
                .ok_or_else(|| MongrelError::ColumnNotFound(embedding_column.to_string()))?;
            let crate::schema::TypeId::Embedding { dim } = column.ty else {
                return Err(MongrelError::InvalidArgument(format!(
                    "rerank column {embedding_column} is not an embedding"
                )));
            };
            if query.len() != dim as usize || query.iter().any(|value| !value.is_finite()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "rerank query must contain {dim} finite values"
                )));
            }
        }

        let hard_filter_started = std::time::Instant::now();
        let hard_filter = if request.must.is_empty() {
            None
        } else {
            let mut sets = Vec::with_capacity(request.must.len());
            for condition in &request.must {
                if let Some(context) = context {
                    context.checkpoint()?;
                }
                sets.push(self.resolve_condition(condition, snapshot)?);
            }
            Some(RowIdSet::intersect_many(sets))
        };
        crate::trace::QueryTrace::record(|trace| {
            trace.hard_filter_nanos = trace
                .hard_filter_nanos
                .saturating_add(hard_filter_started.elapsed().as_nanos() as u64);
        });
        if hard_filter.as_ref().is_some_and(RowIdSet::is_empty) {
            return Ok(Vec::new());
        }

        let constant = match request.fusion {
            Fusion::ReciprocalRank { constant } => constant,
        };
        let mut retrievers: Vec<_> = request.retrievers.iter().collect();
        retrievers.sort_by(|a, b| a.name.cmp(&b.name));
        let mut fusion_nanos = 0u64;
        let mut fused: std::collections::HashMap<RowId, (f64, Vec<ComponentScore>)> =
            std::collections::HashMap::new();
        for named in retrievers {
            if named.weight == 0.0 {
                continue;
            }
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let hits = self.retrieve_filtered(
                &named.retriever,
                snapshot,
                hard_filter.as_ref(),
                authorized,
                candidate_authorization,
                context,
            )?;
            let retriever_name: std::sync::Arc<str> = named.name.as_str().into();
            let fusion_started = std::time::Instant::now();
            for hit in hits {
                if let Some(context) = context {
                    context.consume(1)?;
                }
                let contribution = named.weight / (constant as f64 + hit.rank as f64);
                if !contribution.is_finite() {
                    return Err(MongrelError::InvalidArgument(
                        "retriever contribution must be finite".into(),
                    ));
                }
                let max_fused_candidates = context.map_or(
                    crate::query::MAX_FUSED_CANDIDATES,
                    crate::query::AiExecutionContext::max_fused_candidates,
                );
                if !fused.contains_key(&hit.row_id) && fused.len() >= max_fused_candidates {
                    return Err(MongrelError::WorkBudgetExceeded);
                }
                let entry = fused.entry(hit.row_id).or_default();
                entry.0 += contribution;
                if !entry.0.is_finite() {
                    return Err(MongrelError::InvalidArgument(
                        "fused score must be finite".into(),
                    ));
                }
                entry.1.push(ComponentScore {
                    retriever_name: retriever_name.clone(),
                    rank: hit.rank,
                    raw_score: hit.score,
                    contribution,
                });
            }
            fusion_nanos = fusion_nanos.saturating_add(fusion_started.elapsed().as_nanos() as u64);
        }
        let union_size = fused.len();
        let mut ranked: Vec<_> = fused
            .into_iter()
            .map(|(row_id, (fused_score, components))| {
                (row_id, fused_score, components, None, fused_score)
            })
            .collect();
        let order = |(a_row, _, _, _, a_score): &(
            RowId,
            f64,
            Vec<ComponentScore>,
            Option<f32>,
            f64,
        ),
                     (b_row, _, _, _, b_score): &(
            RowId,
            f64,
            Vec<ComponentScore>,
            Option<f32>,
            f64,
        )| { b_score.total_cmp(a_score).then_with(|| a_row.cmp(b_row)) };
        if let Some(crate::query::Rerank::ExactVector {
            embedding_column,
            query,
            metric,
            candidate_limit,
            weight,
        }) = &request.rerank
        {
            let fused_order = |(a_row, a_score, ..): &(
                RowId,
                f64,
                Vec<ComponentScore>,
                Option<f32>,
                f64,
            ),
                               (b_row, b_score, ..): &(
                RowId,
                f64,
                Vec<ComponentScore>,
                Option<f32>,
                f64,
            )| {
                b_score.total_cmp(a_score).then_with(|| a_row.cmp(b_row))
            };
            let selection_started = std::time::Instant::now();
            if let Some(context) = context {
                context.consume(ranked.len())?;
            }
            if ranked.len() > *candidate_limit {
                let (_, _, _) = ranked.select_nth_unstable_by(*candidate_limit, fused_order);
                ranked.truncate(*candidate_limit);
            }
            ranked.sort_by(fused_order);
            fusion_nanos =
                fusion_nanos.saturating_add(selection_started.elapsed().as_nanos() as u64);
            let row_ids: Vec<_> = ranked.iter().map(|(row_id, ..)| row_id.0).collect();
            if let Some(context) = context {
                context.consume(row_ids.len())?;
            }
            let query_now =
                context.map_or_else(unix_nanos_now, |context| context.query_time_nanos());
            let gather_started = std::time::Instant::now();
            let vectors = self.values_for_rids_batch_at_with_context(
                &row_ids,
                *embedding_column,
                snapshot,
                query_now,
                context,
            )?;
            let gather_nanos = gather_started.elapsed().as_nanos() as u64;
            let vector_work =
                crate::query::work_units(query.len(), crate::query::VECTOR_WORK_QUANTUM);
            let query_norm = if matches!(metric, crate::query::VectorMetric::Cosine) {
                if let Some(context) = context {
                    context.consume(vector_work)?;
                }
                query
                    .iter()
                    .map(|value| f64::from(*value).powi(2))
                    .sum::<f64>()
                    .sqrt()
            } else {
                0.0
            };
            let score_started = std::time::Instant::now();
            let mut scores = std::collections::HashMap::with_capacity(vectors.len());
            for (row_id, value) in vectors {
                let Value::Embedding(vector) = value else {
                    continue;
                };
                let score = match metric {
                    crate::query::VectorMetric::DotProduct => {
                        if let Some(context) = context {
                            context.consume(vector_work)?;
                        }
                        query
                            .iter()
                            .zip(&vector)
                            .map(|(left, right)| f64::from(*left) * f64::from(*right))
                            .sum::<f64>()
                    }
                    crate::query::VectorMetric::Cosine => {
                        if let Some(context) = context {
                            context.consume(vector_work.saturating_mul(2))?;
                        }
                        let dot = query
                            .iter()
                            .zip(&vector)
                            .map(|(left, right)| f64::from(*left) * f64::from(*right))
                            .sum::<f64>();
                        let norm = vector
                            .iter()
                            .map(|value| f64::from(*value).powi(2))
                            .sum::<f64>()
                            .sqrt();
                        if query_norm == 0.0 || norm == 0.0 {
                            0.0
                        } else {
                            dot / (query_norm * norm)
                        }
                    }
                    crate::query::VectorMetric::Euclidean => {
                        if let Some(context) = context {
                            context.consume(vector_work)?;
                        }
                        query
                            .iter()
                            .zip(&vector)
                            .map(|(left, right)| (f64::from(*left) - f64::from(*right)).powi(2))
                            .sum::<f64>()
                            .sqrt()
                    }
                };
                if !score.is_finite() {
                    return Err(MongrelError::InvalidArgument(
                        "exact rerank score must be finite".into(),
                    ));
                }
                scores.insert(row_id, score as f32);
            }
            let mut reranked = Vec::with_capacity(ranked.len());
            for (row_id, fused_score, components, _, _) in ranked.drain(..) {
                let Some(score) = scores.get(&row_id).copied() else {
                    continue;
                };
                let ordering_score = match metric {
                    crate::query::VectorMetric::Euclidean => -f64::from(score),
                    crate::query::VectorMetric::Cosine | crate::query::VectorMetric::DotProduct => {
                        f64::from(score)
                    }
                };
                let final_score = fused_score + *weight * ordering_score;
                if !final_score.is_finite() {
                    return Err(MongrelError::InvalidArgument(
                        "final rerank score must be finite".into(),
                    ));
                }
                reranked.push((row_id, fused_score, components, Some(score), final_score));
            }
            ranked = reranked;
            ranked.sort_by(order);
            crate::trace::QueryTrace::record(|trace| {
                trace.exact_vector_gather_nanos =
                    trace.exact_vector_gather_nanos.saturating_add(gather_nanos);
                trace.exact_vector_score_nanos = trace
                    .exact_vector_score_nanos
                    .saturating_add(score_started.elapsed().as_nanos() as u64);
            });
        }
        if let Some(after) = after {
            ranked.retain(|(row_id, _, _, _, final_score)| {
                final_score.total_cmp(&after.final_score).is_lt()
                    || (final_score.total_cmp(&after.final_score).is_eq() && *row_id > after.row_id)
            });
        }
        let projection_started = std::time::Instant::now();
        let sentinel = projection
            .first()
            .copied()
            .or_else(|| self.schema.columns.first().map(|column| column.id));
        let query_now = context.map_or_else(unix_nanos_now, |context| context.query_time_nanos());
        let mut out = Vec::with_capacity(request.limit.min(ranked.len()));
        let mut projection_rows = 0usize;
        let mut projection_cells = 0usize;
        while out.len() < request.limit && !ranked.is_empty() {
            if let Some(context) = context {
                context.checkpoint()?;
                context.consume(ranked.len())?;
            }
            let needed = request.limit - out.len();
            let window_size = ranked
                .len()
                .min(needed.saturating_mul(2).max(needed.saturating_add(8)));
            let selection_started = std::time::Instant::now();
            let mut remainder = if ranked.len() > window_size {
                let (_, _, _) = ranked.select_nth_unstable_by(window_size, order);
                ranked.split_off(window_size)
            } else {
                Vec::new()
            };
            ranked.sort_by(order);
            fusion_nanos =
                fusion_nanos.saturating_add(selection_started.elapsed().as_nanos() as u64);
            let row_ids: Vec<_> = ranked.iter().map(|(row_id, ..)| row_id.0).collect();
            let gathered_columns = projection.len().max(usize::from(sentinel.is_some()));
            if let Some(context) = context {
                context.consume(row_ids.len().saturating_mul(gathered_columns))?;
            }
            projection_rows = projection_rows.saturating_add(row_ids.len());
            projection_cells =
                projection_cells.saturating_add(row_ids.len().saturating_mul(gathered_columns));
            let mut cells: std::collections::HashMap<RowId, std::collections::HashMap<u16, Value>> =
                std::collections::HashMap::new();
            if let Some(column_id) = sentinel {
                for (row_id, value) in self.values_for_rids_batch_at_with_context(
                    &row_ids, column_id, snapshot, query_now, context,
                )? {
                    cells.entry(row_id).or_default().insert(column_id, value);
                }
            }
            for &column_id in &projection {
                if Some(column_id) == sentinel {
                    continue;
                }
                for (row_id, value) in self.values_for_rids_batch_at_with_context(
                    &row_ids, column_id, snapshot, query_now, context,
                )? {
                    cells.entry(row_id).or_default().insert(column_id, value);
                }
            }
            for (row_id, fused_score, mut components, exact_rerank_score, final_score) in
                ranked.drain(..)
            {
                let Some(row_cells) = cells.remove(&row_id) else {
                    continue;
                };
                components.sort_by(|a, b| a.retriever_name.cmp(&b.retriever_name));
                let final_rank = rank_offset.saturating_add(out.len()).saturating_add(1);
                out.push(SearchHit {
                    row_id,
                    cells: projection
                        .iter()
                        .filter_map(|column_id| {
                            row_cells
                                .get(column_id)
                                .cloned()
                                .map(|value| (*column_id, value))
                        })
                        .collect(),
                    components,
                    fused_score,
                    exact_rerank_score,
                    final_score,
                    final_rank,
                });
                if out.len() == request.limit {
                    break;
                }
            }
            ranked.append(&mut remainder);
        }
        crate::trace::QueryTrace::record(|trace| {
            trace.union_size = union_size;
            trace.fusion_nanos = trace.fusion_nanos.saturating_add(fusion_nanos);
            trace.projection_nanos = trace
                .projection_nanos
                .saturating_add(projection_started.elapsed().as_nanos() as u64);
            trace.total_nanos = trace
                .total_nanos
                .saturating_add(total_started.elapsed().as_nanos() as u64);
            trace.projection_rows = trace.projection_rows.saturating_add(projection_rows);
            trace.projection_cells = trace.projection_cells.saturating_add(projection_cells);
            if let Some(context) = context {
                trace.work_consumed = trace.work_consumed.saturating_add(context.consumed_work());
            }
        });
        Ok(out)
    }

    /// MinHash candidate generation followed by exact Jaccard verification.
    /// An empty query set returns no hits.
    pub fn set_similarity(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
    ) -> Result<Vec<crate::query::SetSimilarityHit>> {
        self.set_similarity_with_allowed(request, None)
    }

    pub fn set_similarity_at(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::SetSimilarityHit>> {
        self.set_similarity_explained_at(request, snapshot, allowed)
            .map(|(hits, _)| hits)
    }

    /// Binary ANN candidate generation followed by exact float-vector reranking.
    pub fn ann_rerank(
        &mut self,
        request: &crate::query::AnnRerankRequest,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.ann_rerank_with_allowed(request, None)
    }

    pub fn ann_rerank_with_allowed(
        &mut self,
        request: &crate::query::AnnRerankRequest,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.ann_rerank_at(request, self.snapshot(), allowed)
    }

    pub fn ann_rerank_at(
        &mut self,
        request: &crate::query::AnnRerankRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.ann_rerank_at_with_context(request, snapshot, allowed, None)
    }

    pub fn ann_rerank_at_with_context(
        &mut self,
        request: &crate::query::AnnRerankRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.ensure_indexes_complete()?;
        self.ann_rerank_at_with_filters_and_context(request, snapshot, allowed, None, context)
    }

    pub fn ann_rerank_at_with_candidate_authorization_and_context(
        &mut self,
        request: &crate::query::AnnRerankRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.ensure_indexes_complete()?;
        self.ann_rerank_at_with_filters_and_context(request, snapshot, None, authorization, context)
    }

    #[doc(hidden)]
    pub fn ann_rerank_at_with_candidate_authorization_on_generation(
        &self,
        request: &crate::query::AnnRerankRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        self.ann_rerank_at_with_filters_and_context(request, snapshot, None, authorization, context)
    }

    fn ann_rerank_at_with_filters_and_context(
        &self,
        request: &crate::query::AnnRerankRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        candidate_authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::AnnRerankHit>> {
        use crate::query::{
            AnnRerankHit, Retriever, RetrieverScore, VectorMetric, MAX_FINAL_LIMIT, MAX_RETRIEVER_K,
        };
        if request.candidate_k == 0 || request.limit == 0 {
            return Err(MongrelError::InvalidArgument(
                "candidate_k and limit must be > 0".into(),
            ));
        }
        if request.candidate_k > MAX_RETRIEVER_K || request.limit > MAX_FINAL_LIMIT {
            return Err(MongrelError::InvalidArgument(format!(
                "candidate_k must be <= {MAX_RETRIEVER_K} and limit <= {MAX_FINAL_LIMIT}"
            )));
        }
        let retriever = Retriever::Ann {
            column_id: request.column_id,
            query: request.query.clone(),
            k: request.candidate_k,
        };
        self.require_select()?;
        self.validate_retriever(&retriever)?;
        let hits = self.retrieve_filtered(
            &retriever,
            snapshot,
            None,
            allowed,
            candidate_authorization,
            context,
        )?;
        let distances: std::collections::HashMap<_, _> = hits
            .iter()
            .filter_map(|hit| match hit.score {
                RetrieverScore::AnnHammingDistance(distance) => Some((hit.row_id, distance)),
                _ => None,
            })
            .collect();
        let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
        if let Some(context) = context {
            context.consume(row_ids.len())?;
        }
        let gather_started = std::time::Instant::now();
        let query_now = context.map_or_else(unix_nanos_now, |context| context.query_time_nanos());
        let values = self.values_for_rids_batch_at_with_context(
            &row_ids,
            request.column_id,
            snapshot,
            query_now,
            context,
        )?;
        let gather_nanos = gather_started.elapsed().as_nanos() as u64;
        let score_started = std::time::Instant::now();
        let vector_work =
            crate::query::work_units(request.query.len(), crate::query::VECTOR_WORK_QUANTUM);
        let query_norm = if matches!(request.metric, VectorMetric::Cosine) {
            if let Some(context) = context {
                context.consume(vector_work)?;
            }
            request
                .query
                .iter()
                .map(|value| f64::from(*value).powi(2))
                .sum::<f64>()
                .sqrt()
        } else {
            0.0
        };
        let mut reranked = Vec::with_capacity(values.len().min(request.limit));
        for (row_id, value) in values {
            let Value::Embedding(vector) = value else {
                continue;
            };
            let exact_score = match request.metric {
                VectorMetric::DotProduct => {
                    if let Some(context) = context {
                        context.consume(vector_work)?;
                    }
                    request
                        .query
                        .iter()
                        .zip(&vector)
                        .map(|(left, right)| f64::from(*left) * f64::from(*right))
                        .sum::<f64>()
                }
                VectorMetric::Cosine => {
                    if let Some(context) = context {
                        context.consume(vector_work.saturating_mul(2))?;
                    }
                    let dot = request
                        .query
                        .iter()
                        .zip(&vector)
                        .map(|(left, right)| f64::from(*left) * f64::from(*right))
                        .sum::<f64>();
                    let norm = vector
                        .iter()
                        .map(|value| f64::from(*value).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    if query_norm == 0.0 || norm == 0.0 {
                        0.0
                    } else {
                        dot / (query_norm * norm)
                    }
                }
                VectorMetric::Euclidean => {
                    if let Some(context) = context {
                        context.consume(vector_work)?;
                    }
                    request
                        .query
                        .iter()
                        .zip(&vector)
                        .map(|(left, right)| (f64::from(*left) - f64::from(*right)).powi(2))
                        .sum::<f64>()
                        .sqrt()
                }
            };
            let exact_score = exact_score as f32;
            if !exact_score.is_finite() {
                return Err(MongrelError::InvalidArgument(
                    "exact ANN score must be finite".into(),
                ));
            }
            reranked.push(AnnRerankHit {
                row_id,
                hamming_distance: distances.get(&row_id).copied().unwrap_or_default(),
                exact_score,
            });
        }
        reranked.sort_by(|left, right| {
            let score = match request.metric {
                VectorMetric::Euclidean => left.exact_score.total_cmp(&right.exact_score),
                VectorMetric::Cosine | VectorMetric::DotProduct => {
                    right.exact_score.total_cmp(&left.exact_score)
                }
            };
            score.then_with(|| left.row_id.cmp(&right.row_id))
        });
        reranked.truncate(request.limit);
        crate::trace::QueryTrace::record(|trace| {
            trace.exact_vector_gather_nanos =
                trace.exact_vector_gather_nanos.saturating_add(gather_nanos);
            trace.exact_vector_score_nanos = trace
                .exact_vector_score_nanos
                .saturating_add(score_started.elapsed().as_nanos() as u64);
        });
        Ok(reranked)
    }

    pub fn set_similarity_with_allowed(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<Vec<crate::query::SetSimilarityHit>> {
        self.set_similarity_explained_at(request, self.snapshot(), allowed)
            .map(|(hits, _)| hits)
    }

    pub fn set_similarity_explained(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
    ) -> Result<(
        Vec<crate::query::SetSimilarityHit>,
        crate::query::SetSimilarityTrace,
    )> {
        self.set_similarity_explained_at(request, self.snapshot(), None)
    }

    fn set_similarity_explained_at(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<(
        Vec<crate::query::SetSimilarityHit>,
        crate::query::SetSimilarityTrace,
    )> {
        self.ensure_indexes_complete()?;
        self.set_similarity_explained_at_with_context(request, snapshot, allowed, None, None)
    }

    pub fn set_similarity_at_with_context(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::SetSimilarityHit>> {
        self.ensure_indexes_complete()?;
        self.set_similarity_explained_at_with_context(request, snapshot, allowed, None, context)
            .map(|(hits, _)| hits)
    }

    pub fn set_similarity_at_with_candidate_authorization_and_context(
        &mut self,
        request: &crate::query::SetSimilarityRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::SetSimilarityHit>> {
        self.ensure_indexes_complete()?;
        self.set_similarity_explained_at_with_context(
            request,
            snapshot,
            None,
            authorization,
            context,
        )
        .map(|(hits, _)| hits)
    }

    #[doc(hidden)]
    pub fn set_similarity_at_with_candidate_authorization_on_generation(
        &self,
        request: &crate::query::SetSimilarityRequest,
        snapshot: Snapshot,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<crate::query::SetSimilarityHit>> {
        self.set_similarity_explained_at_with_context(
            request,
            snapshot,
            None,
            authorization,
            context,
        )
        .map(|(hits, _)| hits)
    }

    fn set_similarity_explained_at_with_context(
        &self,
        request: &crate::query::SetSimilarityRequest,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
        candidate_authorization: Option<&crate::security::CandidateAuthorization<'_>>,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<(
        Vec<crate::query::SetSimilarityHit>,
        crate::query::SetSimilarityTrace,
    )> {
        use crate::query::{
            Retriever, RetrieverScore, SetSimilarityHit, MAX_FINAL_LIMIT, MAX_RETRIEVER_K,
            MAX_SET_MEMBERS,
        };
        let mut trace = crate::query::SetSimilarityTrace::default();
        if request.members.is_empty() {
            return Ok((Vec::new(), trace));
        }
        if request.candidate_k == 0 || request.limit == 0 {
            return Err(MongrelError::InvalidArgument(
                "candidate_k and limit must be > 0".into(),
            ));
        }
        if request.candidate_k > MAX_RETRIEVER_K
            || request.limit > MAX_FINAL_LIMIT
            || request.members.len() > MAX_SET_MEMBERS
        {
            return Err(MongrelError::InvalidArgument(format!(
                "candidate_k must be <= {MAX_RETRIEVER_K}, limit <= {MAX_FINAL_LIMIT}, and members <= {MAX_SET_MEMBERS}"
            )));
        }
        if !request.min_jaccard.is_finite() || !(0.0..=1.0).contains(&request.min_jaccard) {
            return Err(MongrelError::InvalidArgument(
                "min_jaccard must be finite and between 0 and 1".into(),
            ));
        }
        let started = std::time::Instant::now();
        let retriever = Retriever::MinHash {
            column_id: request.column_id,
            members: request.members.clone(),
            k: request.candidate_k,
        };
        self.require_select()?;
        self.validate_retriever(&retriever)?;
        let hits = self.retrieve_filtered(
            &retriever,
            snapshot,
            None,
            allowed,
            candidate_authorization,
            context,
        )?;
        trace.candidate_generation_us = started.elapsed().as_micros() as u64;
        trace.candidate_count = hits.len();
        let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
        if let Some(context) = context {
            context.consume(row_ids.len())?;
        }
        let started = std::time::Instant::now();
        let query_now = context.map_or_else(unix_nanos_now, |context| context.query_time_nanos());
        let values = self.values_for_rids_batch_at_with_context(
            &row_ids,
            request.column_id,
            snapshot,
            query_now,
            context,
        )?;
        trace.gather_us = started.elapsed().as_micros() as u64;
        if let Some(context) = context {
            context.consume(request.members.len())?;
        }
        let query: std::collections::HashSet<_> = request.members.iter().cloned().collect();
        let estimates: std::collections::HashMap<_, _> = hits
            .into_iter()
            .filter_map(|hit| match hit.score {
                RetrieverScore::MinHashEstimatedJaccard(score) => Some((hit.row_id, score)),
                _ => None,
            })
            .collect();
        let started = std::time::Instant::now();
        let mut parsed = Vec::with_capacity(values.len());
        for (row_id, value) in values {
            let Value::Bytes(bytes) = value else {
                continue;
            };
            if let Some(context) = context {
                context.consume(crate::query::work_units(
                    bytes.len(),
                    crate::query::PARSE_WORK_QUANTUM,
                ))?;
            }
            let Ok(serde_json::Value::Array(members)) = serde_json::from_slice(&bytes) else {
                continue;
            };
            if let Some(context) = context {
                context.consume(members.len())?;
            }
            let stored = members
                .into_iter()
                .filter_map(|member| match member {
                    serde_json::Value::String(value) => {
                        Some(crate::query::SetMember::String(value))
                    }
                    serde_json::Value::Number(value) => {
                        Some(crate::query::SetMember::Number(value))
                    }
                    serde_json::Value::Bool(value) => Some(crate::query::SetMember::Boolean(value)),
                    _ => None,
                })
                .collect::<std::collections::HashSet<_>>();
            parsed.push((row_id, stored));
        }
        trace.parse_us = started.elapsed().as_micros() as u64;
        trace.verified_count = parsed.len();
        let started = std::time::Instant::now();
        let mut exact = Vec::new();
        for (row_id, stored) in parsed {
            if let Some(context) = context {
                context.consume(query.len().saturating_add(stored.len()))?;
            }
            let union = query.union(&stored).count();
            let score = if union == 0 {
                1.0
            } else {
                query.intersection(&stored).count() as f32 / union as f32
            };
            if score >= request.min_jaccard {
                exact.push(SetSimilarityHit {
                    row_id,
                    estimated_jaccard: estimates.get(&row_id).copied().unwrap_or_default(),
                    exact_jaccard: score,
                });
            }
        }
        exact.sort_by(|a, b| {
            b.exact_jaccard
                .total_cmp(&a.exact_jaccard)
                .then_with(|| a.row_id.cmp(&b.row_id))
        });
        exact.truncate(request.limit);
        trace.score_us = started.elapsed().as_micros() as u64;
        crate::trace::QueryTrace::record(|query_trace| {
            query_trace.exact_set_gather_nanos = query_trace
                .exact_set_gather_nanos
                .saturating_add(trace.gather_us.saturating_mul(1_000));
            query_trace.exact_set_parse_nanos = query_trace
                .exact_set_parse_nanos
                .saturating_add(trace.parse_us.saturating_mul(1_000));
            query_trace.exact_set_score_nanos = query_trace
                .exact_set_score_nanos
                .saturating_add(trace.score_us.saturating_mul(1_000));
        });
        Ok((exact, trace))
    }

    /// Fetch one column for visible row ids without decoding unrelated columns.
    fn values_for_rids_batch_at(
        &self,
        row_ids: &[u64],
        column_id: u16,
        snapshot: Snapshot,
        now: i64,
    ) -> Result<Vec<(RowId, Value)>> {
        if self.ttl.is_none()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty()
            && self.run_refs.len() == 1
        {
            let mut reader = self.open_reader(self.run_refs[0].run_id)?;
            // Small projections should not decode and scan the run's entire
            // row-id column. Resolve each requested row through the page-pruned
            // point path until a full visibility pass becomes cheaper. Keep
            // this crossover aligned with `rows_for_rids_at_time`.
            if row_ids.len().saturating_mul(24) < reader.row_count() {
                let mut values = Vec::with_capacity(row_ids.len());
                for &raw_row_id in row_ids {
                    let row_id = RowId(raw_row_id);
                    if let Some((_, false, Some(value))) =
                        reader.get_version_column(row_id, snapshot.epoch, column_id)?
                    {
                        values.push((row_id, value));
                    }
                }
                return Ok(values);
            }
            let (positions, visible_row_ids) =
                reader.visible_positions_with_rids(snapshot.epoch)?;
            let requested: Vec<(RowId, usize)> = row_ids
                .iter()
                .filter_map(|raw| {
                    visible_row_ids
                        .binary_search(&(*raw as i64))
                        .ok()
                        .map(|index| (RowId(*raw), positions[index]))
                })
                .collect();
            let values = reader.gather_column(
                column_id,
                &requested
                    .iter()
                    .map(|(_, position)| *position)
                    .collect::<Vec<_>>(),
            )?;
            return Ok(requested
                .into_iter()
                .zip(values)
                .map(|((row_id, _), value)| (row_id, value))
                .collect());
        }
        self.values_for_rids_at(row_ids, column_id, snapshot, now)
    }

    fn values_for_rids_batch_at_with_context(
        &self,
        row_ids: &[u64],
        column_id: u16,
        snapshot: Snapshot,
        now: i64,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> Result<Vec<(RowId, Value)>> {
        let Some(context) = context else {
            return self.values_for_rids_batch_at(row_ids, column_id, snapshot, now);
        };
        let mut values = Vec::with_capacity(row_ids.len());
        for chunk in row_ids.chunks(256) {
            context.checkpoint()?;
            values.extend(self.values_for_rids_batch_at(chunk, column_id, snapshot, now)?);
        }
        Ok(values)
    }

    /// Fetch one column for visible row ids without decoding unrelated columns.
    fn values_for_rids_at(
        &self,
        row_ids: &[u64],
        column_id: u16,
        snapshot: Snapshot,
        now: i64,
    ) -> Result<Vec<(RowId, Value)>> {
        let mut readers: Vec<_> = self
            .run_refs
            .iter()
            .map(|run| self.open_reader(run.run_id))
            .collect::<Result<_>>()?;
        let mut out = Vec::with_capacity(row_ids.len());
        for &raw_row_id in row_ids {
            let row_id = RowId(raw_row_id);
            let mem = self.memtable.get_version(row_id, snapshot.epoch);
            let mutable = self.mutable_run.get_version(row_id, snapshot.epoch);
            let overlay = match (mem, mutable) {
                (Some((a_epoch, a)), Some((b_epoch, b))) => Some(if a_epoch >= b_epoch {
                    (a_epoch, a)
                } else {
                    (b_epoch, b)
                }),
                (Some(value), None) | (None, Some(value)) => Some(value),
                (None, None) => None,
            };
            if let Some((_, row)) = overlay {
                if !row.deleted && !self.row_expired_at(&row, now) {
                    if let Some(value) = row.columns.get(&column_id) {
                        out.push((row_id, value.clone()));
                    }
                }
                continue;
            }

            let mut best: Option<(Epoch, bool, Option<Value>, usize)> = None;
            for (index, reader) in readers.iter_mut().enumerate() {
                if let Some((epoch, deleted, value)) =
                    reader.get_version_column(row_id, snapshot.epoch, column_id)?
                {
                    if best
                        .as_ref()
                        .map(|(best_epoch, ..)| epoch > *best_epoch)
                        .unwrap_or(true)
                    {
                        best = Some((epoch, deleted, value, index));
                    }
                }
            }
            let Some((_, false, Some(value), reader_index)) = best else {
                continue;
            };
            if let Some(ttl) = self.ttl {
                if ttl.column_id != column_id {
                    if let Some((_, _, Some(Value::Int64(timestamp)))) = readers[reader_index]
                        .get_version_column(row_id, snapshot.epoch, ttl.column_id)?
                    {
                        if timestamp.saturating_add(ttl.duration_nanos as i64) <= now {
                            continue;
                        }
                    }
                } else if let Value::Int64(timestamp) = value {
                    if timestamp.saturating_add(ttl.duration_nanos as i64) <= now {
                        continue;
                    }
                }
            }
            out.push((row_id, value));
        }
        Ok(out)
    }

    /// Materialize the MVCC-visible, non-deleted rows for `rids` at `snapshot`,
    /// preserving the input order. Rows whose newest visible version is a
    /// tombstone, or that no longer exist, are omitted. Shared by index-served
    /// [`query`] and the Phase 8.1 FK-join path.
    pub fn rows_for_rids(&self, rids: &[u64], snapshot: Snapshot) -> Result<Vec<Row>> {
        self.rows_for_rids_at_time(rids, snapshot, unix_nanos_now(), None)
    }

    pub fn rows_for_rids_with_context(
        &self,
        rids: &[u64],
        snapshot: Snapshot,
        context: &crate::query::AiExecutionContext,
    ) -> Result<Vec<Row>> {
        context.consume(rids.len().saturating_mul(self.schema.columns.len()))?;
        self.rows_for_rids_at_time(rids, snapshot, context.query_time_nanos(), None)
    }

    fn rows_for_rids_at_time(
        &self,
        rids: &[u64],
        snapshot: Snapshot,
        ttl_now: i64,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Vec<Row>> {
        use std::collections::HashMap;
        let mut rows = Vec::with_capacity(rids.len());
        // Overlay (memtable + mutable-run) newest visible version per rid —
        // these shadow any stale version stored in a run. A rid may have an
        // older version in the mutable-run tier and a newer one in the memtable
        // (an update after a flush), so keep the **newest by epoch** across both
        // tiers, not whichever is inserted last.
        //
        // `rids` is already index-resolved (the caller's condition set), so it
        // is normally tiny relative to the memtable/mutable-run tiers — a
        // single-row PK/unique check feeding insert/update/delete resolves to
        // 0 or 1 rid. Materializing every version in both tiers (the old
        // behavior) cost O(tier size) regardless, which meant an unrelated
        // full-table-sized scan (plus the drop cost of the resulting map) on
        // every point lookup once the table grew large. Below the crossover,
        // a direct per-rid probe (`get_version`, O(log tier size) each) wins;
        // once `rids` approaches tier size, one linear materializing pass
        // beats `rids.len()` separate probes, so fall back to it.
        let tier_size = self.memtable.len() + self.mutable_run.len();
        let mut overlay: HashMap<u64, Row> = HashMap::with_capacity(rids.len());
        if rids.len().saturating_mul(24) < tier_size {
            for &rid in rids {
                if overlay.len() & 255 == 0 {
                    control
                        .map(crate::ExecutionControl::checkpoint)
                        .transpose()?;
                }
                let mem = self.memtable.get_version(RowId(rid), snapshot.epoch);
                let mrun = self.mutable_run.get_version(RowId(rid), snapshot.epoch);
                let newest = match (mem, mrun) {
                    (Some((me, mr)), Some((re, rr))) => Some(if me >= re { mr } else { rr }),
                    (Some((_, mr)), None) => Some(mr),
                    (None, Some((_, rr))) => Some(rr),
                    (None, None) => None,
                };
                if let Some(row) = newest {
                    overlay.insert(rid, row);
                }
            }
        } else {
            let fold_newest = |row: Row, overlay: &mut HashMap<u64, Row>| {
                overlay
                    .entry(row.row_id.0)
                    .and_modify(|e| {
                        if row.committed_epoch > e.committed_epoch {
                            *e = row.clone();
                        }
                    })
                    .or_insert(row);
            };
            for (index, row) in self
                .memtable
                .visible_versions(snapshot.epoch)
                .into_iter()
                .enumerate()
            {
                if index & 255 == 0 {
                    control
                        .map(crate::ExecutionControl::checkpoint)
                        .transpose()?;
                }
                fold_newest(row, &mut overlay);
            }
            for (index, row) in self
                .mutable_run
                .visible_versions(snapshot.epoch)
                .into_iter()
                .enumerate()
            {
                if index & 255 == 0 {
                    control
                        .map(crate::ExecutionControl::checkpoint)
                        .transpose()?;
                }
                fold_newest(row, &mut overlay);
            }
        }
        if self.run_refs.len() == 1 {
            let mut reader = self.open_reader(self.run_refs[0].run_id)?;
            // Same crossover as the overlay above: `visible_positions_with_rids`
            // decodes/scans the run's *entire* row-id column regardless of
            // `rids.len()`, so a point lookup (0 or 1 rid, the common
            // insert/update/delete case) paid an O(run size) tax for a single
            // row. Below the crossover, `get_version`'s page-pruned lookup
            // (`SYS_ROW_ID` pages carry exact row-id bounds) resolves each rid
            // by decoding only its page, no whole-column decode.
            if rids.len().saturating_mul(24) < reader.row_count() {
                for (index, &rid) in rids.iter().enumerate() {
                    if index & 255 == 0 {
                        control
                            .map(crate::ExecutionControl::checkpoint)
                            .transpose()?;
                    }
                    if let Some(r) = overlay.get(&rid) {
                        if !r.deleted {
                            rows.push(r.clone());
                        }
                        continue;
                    }
                    if let Some((_, row)) = reader.get_version(RowId(rid), snapshot.epoch)? {
                        if !row.deleted {
                            rows.push(row);
                        }
                    }
                }
                rows.retain(|row| !self.row_expired_at(row, ttl_now));
                return Ok(rows);
            }
            // Phase 16.3b: decode the system columns ONCE (via the clean-run-
            // shortcut visibility pass) and binary-search each requested rid,
            // instead of `get_version`-per-rid which re-decoded + cloned the
            // full system columns on every call (the ~350 ms native-query tax).
            // Phase 16.3b finish: batch the survivor positions into ONE
            // `materialize_batch` call so user columns are decoded once each via
            // the typed, page-cached path (not a per-rid `Vec<Value>` decode +
            // `.cloned()`).
            let (positions, vis_rids) = reader.visible_positions_with_rids(snapshot.epoch)?;
            // First pass: classify each input rid (overlay / run position /
            // not-found), recording the run positions to fetch in input order.
            enum Src {
                Overlay,
                Run,
            }
            let mut plan: Vec<Src> = Vec::with_capacity(rids.len());
            let mut fetch: Vec<usize> = Vec::with_capacity(rids.len());
            for (index, rid) in rids.iter().enumerate() {
                if index & 255 == 0 {
                    control
                        .map(crate::ExecutionControl::checkpoint)
                        .transpose()?;
                }
                if overlay.contains_key(rid) {
                    plan.push(Src::Overlay);
                    continue;
                }
                match vis_rids.binary_search(&(*rid as i64)) {
                    Ok(i) => {
                        plan.push(Src::Run);
                        fetch.push(positions[i]);
                    }
                    Err(_) => { /* not found — omitted from output */ }
                }
            }
            let fetched = reader.materialize_batch(&fetch)?;
            let mut fetched_iter = fetched.into_iter();
            for (index, (rid, src)) in rids.iter().zip(plan).enumerate() {
                if index & 255 == 0 {
                    control
                        .map(crate::ExecutionControl::checkpoint)
                        .transpose()?;
                }
                match src {
                    Src::Overlay => {
                        if let Some(r) = overlay.get(rid) {
                            if !r.deleted {
                                rows.push(r.clone());
                            }
                        }
                    }
                    Src::Run => {
                        if let Some(row) = fetched_iter.next() {
                            if !row.deleted {
                                rows.push(row);
                            }
                        }
                    }
                }
            }
            rows.retain(|row| !self.row_expired_at(row, ttl_now));
            return Ok(rows);
        }
        // Multi-run: one reader per run; newest visible version across all runs
        // + the overlay. (Per-rid `get_version` here is unavoidable without a
        // cross-run merge, but multi-run is the uncommon cold case.)
        let mut readers: Vec<_> = self
            .run_refs
            .iter()
            .map(|rr| self.open_reader(rr.run_id))
            .collect::<Result<Vec<_>>>()?;
        for (index, rid) in rids.iter().enumerate() {
            if index & 255 == 0 {
                control
                    .map(crate::ExecutionControl::checkpoint)
                    .transpose()?;
            }
            if let Some(r) = overlay.get(rid) {
                if !r.deleted {
                    rows.push(r.clone());
                }
                continue;
            }
            let mut best: Option<(Epoch, Row)> = None;
            for reader in readers.iter_mut() {
                if let Ok(Some((epoch, row))) = reader.get_version(RowId(*rid), snapshot.epoch) {
                    if best.as_ref().map(|(be, _)| epoch > *be).unwrap_or(true) {
                        best = Some((epoch, row));
                    }
                }
            }
            if let Some((_, r)) = best {
                if !r.deleted {
                    rows.push(r);
                }
            }
        }
        rows.retain(|row| !self.row_expired_at(row, ttl_now));
        Ok(rows)
    }

    /// Resolve the referencing (FK) side of a primary-key ↔ foreign-key join as
    /// a row-id set (Phase 8.1): union the roaring-bitmap entries of
    /// `fk_column_id` for every value in `pk_values` — the surviving
    /// primary-key values — then intersect with `fk_conditions`, i.e. any
    /// FK-side predicates (`ann_search ∩ fm_contains`, bitmap equality, range,
    /// …). Returns the survivor row-ids ascending. Requires a bitmap index on
    /// `fk_column_id`; returns an empty set when there is none.
    /// Whether live indexes are complete (Phase 14.7 + 17.2: the broadcast
    /// join path checks this before using the HOT index).
    pub fn indexes_complete(&self) -> bool {
        self.indexes_complete
    }

    /// Where bulk loads put the index-build cost (see [`IndexBuildPolicy`]).
    pub fn index_build_policy(&self) -> IndexBuildPolicy {
        self.index_build_policy
    }

    /// Set the bulk-load index-build policy. Takes effect on the next
    /// `bulk_load` / `bulk_load_columns` / `bulk_load_fast`; never changes
    /// already-built indexes.
    pub fn set_index_build_policy(&mut self, policy: IndexBuildPolicy) {
        self.index_build_policy = policy;
    }

    /// Phase 17.2: broadcast join — return the distinct values in this table's
    /// bitmap index for `column_id` that also exist as a key in `pk_db`'s HOT
    /// index. Avoids loading the entire PK table when the FK column has low
    /// cardinality. Returns `None` if no bitmap index exists for the column.
    pub fn broadcast_join_values(&self, column_id: u16, pk_db: &Table) -> Option<Vec<Vec<u8>>> {
        // A deferred bulk load leaves the bitmap unbuilt — its (empty) key set
        // would silently produce an empty join. Decline; the caller falls back
        // to the PK-side query path, which completes indexes lazily.
        if !self.indexes_complete {
            return None;
        }
        let b = self.bitmap.get(&column_id)?;
        let result: Vec<Vec<u8>> = b
            .keys()
            .into_iter()
            .filter(|k| pk_db.hot.get(k.as_slice()).is_some())
            .collect();
        Some(result)
    }

    pub fn fk_join_row_ids(
        &self,
        fk_column_id: u16,
        pk_values: &[Vec<u8>],
        fk_conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Vec<u64>> {
        let Some(b) = self.bitmap.get(&fk_column_id) else {
            return Ok(Vec::new());
        };
        let mut join_set = {
            let mut acc = roaring::RoaringBitmap::new();
            for v in pk_values {
                acc |= b.get(v);
            }
            RowIdSet::from_roaring(acc)
        };
        if !fk_conditions.is_empty() {
            let mut sets: Vec<RowIdSet> = Vec::with_capacity(fk_conditions.len() + 1);
            sets.push(join_set);
            for c in fk_conditions {
                sets.push(self.resolve_condition(c, snapshot)?);
            }
            join_set = RowIdSet::intersect_many(sets);
        }
        Ok(join_set.into_sorted_vec())
    }

    /// Like [`fk_join_row_ids`] but returns only the **cardinality** of the FK
    /// survivor set — without materializing or sorting it. For a bare
    /// `COUNT(*)` join with no FK-side filter this is O(1) on the bitmap union
    /// (Phase 17.4): the prior path built a `HashSet<u64>` + `Vec<u64>` +
    /// `sort_unstable` over up to N rows only to read `.len()`.
    pub fn fk_join_count(
        &self,
        fk_column_id: u16,
        pk_values: &[Vec<u8>],
        fk_conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<u64> {
        let Some(b) = self.bitmap.get(&fk_column_id) else {
            return Ok(0);
        };
        let mut acc = roaring::RoaringBitmap::new();
        for v in pk_values {
            acc |= b.get(v);
        }
        if fk_conditions.is_empty() {
            return Ok(acc.len());
        }
        let mut sets: Vec<RowIdSet> = Vec::with_capacity(fk_conditions.len() + 1);
        sets.push(RowIdSet::from_roaring(acc));
        for c in fk_conditions {
            sets.push(self.resolve_condition(c, snapshot)?);
        }
        Ok(RowIdSet::intersect_many(sets).len() as u64)
    }

    /// Resolve a single condition to its row-id set. Index-served conditions use
    /// the in-memory indexes; `Range`/`RangeF64` prefer the learned (PGM) index
    /// or the reader's page-index-skipping path on the single-run fast path, and
    /// only fall back to a `visible_rows` scan off the fast path (multi-run).
    fn resolve_condition(
        &self,
        c: &crate::query::Condition,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        self.resolve_condition_with_allowed(c, snapshot, None)
    }

    fn resolve_condition_with_allowed(
        &self,
        c: &crate::query::Condition,
        snapshot: Snapshot,
        allowed: Option<&std::collections::HashSet<RowId>>,
    ) -> Result<RowIdSet> {
        use crate::query::Condition;
        self.validate_condition(c)?;
        Ok(match c {
            Condition::Pk(key) => {
                let lookup = self
                    .schema
                    .primary_key()
                    .map(|pk| self.index_lookup_key_bytes(pk.id, key))
                    .unwrap_or_else(|| key.clone());
                self.hot
                    .get(&lookup)
                    .map(|r| RowIdSet::one(r.0))
                    .unwrap_or_else(RowIdSet::empty)
            }
            Condition::BitmapEq { column_id, value } => {
                let lookup = self.index_lookup_key_bytes(*column_id, value);
                self.bitmap
                    .get(column_id)
                    .map(|b| RowIdSet::from_roaring(b.get(&lookup)))
                    .unwrap_or_else(RowIdSet::empty)
            }
            Condition::BitmapIn { column_id, values } => {
                let bm = self.bitmap.get(column_id);
                let mut acc = roaring::RoaringBitmap::new();
                if let Some(b) = bm {
                    for v in values {
                        let lookup = self.index_lookup_key_bytes(*column_id, v);
                        acc |= b.get(&lookup);
                    }
                }
                RowIdSet::from_roaring(acc)
            }
            Condition::BytesPrefix { column_id, prefix } => {
                // §5.6: enumerate bitmap keys sharing the prefix for an exact
                // prefix match (anchored `LIKE 'prefix%'`), tighter than the
                // FM substring superset. The caller only emits this when the
                // column has a bitmap index.
                if let Some(b) = self.bitmap.get(column_id) {
                    let lookup_prefix = self.index_lookup_key_bytes(*column_id, prefix);
                    let mut acc = roaring::RoaringBitmap::new();
                    for key in b.keys() {
                        if key.starts_with(&lookup_prefix) {
                            acc |= b.get(&key);
                        }
                    }
                    RowIdSet::from_roaring(acc)
                } else {
                    RowIdSet::empty()
                }
            }
            Condition::FmContains { column_id, pattern } => self
                .fm
                .get(column_id)
                .map(|f| {
                    RowIdSet::from_unsorted(f.locate(pattern).into_iter().map(|r| r.0).collect())
                })
                .unwrap_or_else(RowIdSet::empty),
            Condition::FmContainsAll {
                column_id,
                patterns,
            } => {
                // Multi-segment intersection (Priority 12): resolve each segment
                // via FM and intersect — much tighter than the single longest.
                if let Some(f) = self.fm.get(column_id) {
                    let sets: Vec<RowIdSet> = patterns
                        .iter()
                        .map(|pat| {
                            RowIdSet::from_unsorted(
                                f.locate(pat).into_iter().map(|r| r.0).collect(),
                            )
                        })
                        .collect();
                    RowIdSet::intersect_many(sets)
                } else {
                    RowIdSet::empty()
                }
            }
            Condition::Ann {
                column_id,
                query,
                k,
            } => RowIdSet::from_unsorted(
                self.retrieve_filtered(
                    &crate::query::Retriever::Ann {
                        column_id: *column_id,
                        query: query.clone(),
                        k: *k,
                    },
                    snapshot,
                    None,
                    allowed,
                    None,
                    None,
                )?
                .into_iter()
                .map(|hit| hit.row_id.0)
                .collect(),
            ),
            Condition::SparseMatch {
                column_id,
                query,
                k,
            } => RowIdSet::from_unsorted(
                self.retrieve_filtered(
                    &crate::query::Retriever::Sparse {
                        column_id: *column_id,
                        query: query.clone(),
                        k: *k,
                    },
                    snapshot,
                    None,
                    allowed,
                    None,
                    None,
                )?
                .into_iter()
                .map(|hit| hit.row_id.0)
                .collect(),
            ),
            Condition::MinHashSimilar {
                column_id,
                query,
                k,
            } => match self.minhash.get(column_id) {
                Some(index) => {
                    let candidates = index.candidate_row_ids(query);
                    let eligible =
                        self.eligible_candidate_ids(&candidates, *column_id, snapshot, None)?;
                    RowIdSet::from_unsorted(
                        index
                            .search_filtered(query, *k, |row_id| {
                                eligible.contains(&row_id)
                                    && allowed.is_none_or(|allowed| allowed.contains(&row_id))
                            })
                            .into_iter()
                            .map(|(row_id, _)| row_id.0)
                            .collect(),
                    )
                }
                None => RowIdSet::empty(),
            },
            Condition::Range { column_id, lo, hi } => {
                // Build the candidate set from the durable tier — the learned
                // index (built from sorted runs) or a single page-pruned run —
                // then merge the memtable/mutable-run overlay. An overlay row
                // supersedes its run version (it may have been updated out of
                // range or deleted), so overlay rids are dropped from the run
                // set and re-evaluated from the overlay directly. Without this
                // merge, rows still in the memtable are invisible to a ranged
                // read whenever a LearnedRange index is present.
                let mut set = if let Some(li) = self.learned_range.get(column_id) {
                    RowIdSet::from_unsorted(li.range(*lo, *hi).into_iter().collect())
                } else if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.range_row_id_set_i64(*column_id, *lo, *hi)?
                } else {
                    return self.range_scan_i64(*column_id, *lo, *hi, snapshot);
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.range_scan_overlay_i64(&mut set, *column_id, *lo, *hi, snapshot);
                set
            }
            Condition::RangeF64 {
                column_id,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
            } => {
                // See the `Range` arm: merge the overlay over the durable
                // candidate set so memtable/mutable-run rows are visible.
                let mut set = if let Some(li) = self.learned_range.get(column_id) {
                    RowIdSet::from_unsorted(
                        li.range_f64(*lo, *lo_inclusive, *hi, *hi_inclusive)
                            .into_iter()
                            .collect(),
                    )
                } else if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.range_row_id_set_f64(*column_id, *lo, *lo_inclusive, *hi, *hi_inclusive)?
                } else {
                    return self.range_scan_f64(
                        *column_id,
                        *lo,
                        *lo_inclusive,
                        *hi,
                        *hi_inclusive,
                        snapshot,
                    );
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.range_scan_overlay_f64(
                    &mut set,
                    *column_id,
                    *lo,
                    *lo_inclusive,
                    *hi,
                    *hi_inclusive,
                    snapshot,
                );
                set
            }
            Condition::IsNull { column_id } => {
                let mut set = if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.null_row_id_set(*column_id, true)?
                } else {
                    return self.null_scan(*column_id, true, snapshot);
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.null_scan_overlay(&mut set, *column_id, true, snapshot);
                set
            }
            Condition::IsNotNull { column_id } => {
                let mut set = if self.run_refs.len() == 1 {
                    let mut r = self.open_reader(self.run_refs[0].run_id)?;
                    r.null_row_id_set(*column_id, false)?
                } else {
                    return self.null_scan(*column_id, false, snapshot);
                };
                set.remove_many(self.overlay_rid_set(snapshot));
                self.null_scan_overlay(&mut set, *column_id, false, snapshot);
                set
            }
        })
    }

    /// Vectorized range scan for Int64 columns (Phase 13.2 / 16.3). Resolves the
    /// survivor set via the reader's **page-pruned** path — pages whose `[min,max]`
    /// excludes `[lo,hi]` are never decoded — restricted to MVCC-visible rows.
    /// This is layout-independent: correct under any memtable / multi-run state,
    /// so it is always safe to call (no "single clean run" gate). Overlay rows
    /// (memtable / mutable-run) are excluded from the run portion and checked
    /// directly via [`Self::range_scan_overlay_i64`].
    fn range_scan_i64(
        &self,
        column_id: u16,
        lo: i64,
        hi: i64,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        let mut row_ids = Vec::new();
        let overlay_rids = self.overlay_rid_set(snapshot);
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let matched = reader.range_row_ids_visible_i64(column_id, lo, hi, snapshot.epoch)?;
            for rid in matched {
                if !overlay_rids.contains(&rid) {
                    row_ids.push(rid);
                }
            }
        }
        let mut s = RowIdSet::from_unsorted(row_ids);
        self.range_scan_overlay_i64(&mut s, column_id, lo, hi, snapshot);
        Ok(s)
    }

    /// Float64 analogue of [`Self::range_scan_i64`] with per-bound inclusivity
    /// (Phase 13.2 / 16.3).
    fn range_scan_f64(
        &self,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        let mut row_ids = Vec::new();
        let overlay_rids = self.overlay_rid_set(snapshot);
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let matched = reader.range_row_ids_visible_f64(
                column_id,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
                snapshot.epoch,
            )?;
            for rid in matched {
                if !overlay_rids.contains(&rid) {
                    row_ids.push(rid);
                }
            }
        }
        let mut s = RowIdSet::from_unsorted(row_ids);
        self.range_scan_overlay_f64(
            &mut s,
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
            snapshot,
        );
        Ok(s)
    }

    /// Collect the set of row-ids visible in the memtable / mutable-run overlay.
    fn overlay_rid_set(&self, snapshot: Snapshot) -> HashSet<u64> {
        let mut s = HashSet::new();
        for row in self.memtable.visible_versions(snapshot.epoch) {
            s.insert(row.row_id.0);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            s.insert(row.row_id.0);
        }
        s
    }

    fn range_scan_overlay_i64(
        &self,
        s: &mut RowIdSet,
        column_id: u16,
        lo: i64,
        hi: i64,
        snapshot: Snapshot,
    ) {
        // Collapse both overlay tiers to the newest visible version per row id
        // (the memtable supersedes the mutable run) before range-checking, so a
        // stale in-range mutable-run version cannot shadow a newer out-of-range
        // memtable version of the same row.
        let mut newest: HashMap<u64, &Row> = HashMap::new();
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        for r in &mutable {
            newest.entry(r.row_id.0).or_insert(r);
        }
        for r in &memtable {
            newest.insert(r.row_id.0, r);
        }
        for row in newest.values() {
            if !row.deleted {
                if let Some(Value::Int64(v)) = row.columns.get(&column_id) {
                    if *v >= lo && *v <= hi {
                        s.insert(row.row_id.0);
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn range_scan_overlay_f64(
        &self,
        s: &mut RowIdSet,
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
        snapshot: Snapshot,
    ) {
        // See `range_scan_overlay_i64`: dedup to the newest version per row id
        // across the memtable + mutable run before range-checking.
        let mut newest: HashMap<u64, &Row> = HashMap::new();
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        for r in &mutable {
            newest.entry(r.row_id.0).or_insert(r);
        }
        for r in &memtable {
            newest.insert(r.row_id.0, r);
        }
        for row in newest.values() {
            if !row.deleted {
                if let Some(Value::Float64(v)) = row.columns.get(&column_id) {
                    let ok_lo = if lo_inclusive { *v >= lo } else { *v > lo };
                    let ok_hi = if hi_inclusive { *v <= hi } else { *v < hi };
                    if ok_lo && ok_hi {
                        s.insert(row.row_id.0);
                    }
                }
            }
        }
    }

    /// Multi-run fallback for `IS NULL` / `IS NOT NULL`. Calls each run's
    /// MVCC-aware null scan and merges with the overlay.
    fn null_scan(&self, column_id: u16, want_nulls: bool, snapshot: Snapshot) -> Result<RowIdSet> {
        let mut row_ids = Vec::new();
        let overlay_rids = self.overlay_rid_set(snapshot);
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let matched = reader.null_row_ids_visible(column_id, want_nulls, snapshot.epoch)?;
            for rid in matched {
                if !overlay_rids.contains(&rid) {
                    row_ids.push(rid);
                }
            }
        }
        let mut s = RowIdSet::from_unsorted(row_ids);
        self.null_scan_overlay(&mut s, column_id, want_nulls, snapshot);
        Ok(s)
    }

    /// Merge overlay rows for `IS NULL` / `IS NOT NULL`. An overlay row
    /// supersedes its run version, so overlay rids are removed from the run
    /// set and re-evaluated from the overlay values directly.
    fn null_scan_overlay(
        &self,
        s: &mut RowIdSet,
        column_id: u16,
        want_nulls: bool,
        snapshot: Snapshot,
    ) {
        let mut newest: HashMap<u64, &Row> = HashMap::new();
        let mutable = self.mutable_run.visible_versions(snapshot.epoch);
        let memtable = self.memtable.visible_versions(snapshot.epoch);
        for r in &mutable {
            newest.entry(r.row_id.0).or_insert(r);
        }
        for r in &memtable {
            newest.insert(r.row_id.0, r);
        }
        for row in newest.values() {
            if row.deleted {
                continue;
            }
            let is_null = !row.columns.contains_key(&column_id)
                || matches!(row.columns.get(&column_id), Some(Value::Null) | None);
            if is_null == want_nulls {
                s.insert(row.row_id.0);
            }
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot::at(self.epoch.visible())
    }

    /// Generation of this table's row contents for table-local caches.
    pub fn data_generation(&self) -> u64 {
        self.data_generation
    }

    pub(crate) fn bump_data_generation(&mut self) {
        self.data_generation = self.data_generation.wrapping_add(1);
    }

    pub(crate) fn table_id(&self) -> u64 {
        self.table_id
    }

    pub(crate) fn clone_read_generation(&mut self) -> Result<Self> {
        self.ensure_indexes_complete()?;
        self.memtable.seal();
        self.mutable_run.seal();
        self.hot.seal();
        for index in self.bitmap.values_mut() {
            index.seal();
        }
        for index in self.ann.values_mut() {
            index.seal();
        }
        for index in self.fm.values_mut() {
            index.seal();
        }
        for index in self.sparse.values_mut() {
            index.seal();
        }
        for index in self.minhash.values_mut() {
            index.seal();
        }
        self.pk_by_row.seal();
        let mut generation = self.clone();
        generation.read_only = true;
        generation.wal = WalSink::ReadOnly;
        generation.pending_delete_rids.clear();
        generation.pending_put_cols.clear();
        generation.pending_rows.clear();
        generation.pending_rows_auto_inc.clear();
        generation.pending_dels.clear();
        generation.pending_truncate = None;
        generation.agg_cache = Arc::new(HashMap::new());
        Ok(generation)
    }

    pub(crate) fn estimated_clone_bytes(&self) -> u64 {
        (std::mem::size_of::<Self>() as u64)
            .saturating_add(self.memtable.approx_bytes())
            .saturating_add(self.mutable_run.approx_bytes())
            .saturating_add(self.live_count.saturating_mul(64))
    }

    /// Pin the current epoch as a read snapshot; compaction will preserve the
    /// versions it needs until [`Table::unpin_snapshot`] is called.
    pub fn pin_snapshot(&mut self) -> Snapshot {
        let e = self.epoch.visible();
        *self.pinned.entry(e).or_insert(0) += 1;
        Snapshot::at(e)
    }

    /// Release a pinned snapshot.
    pub fn unpin_snapshot(&mut self, snap: Snapshot) {
        if let Some(count) = self.pinned.get_mut(&snap.epoch) {
            *count -= 1;
            if *count == 0 {
                self.pinned.remove(&snap.epoch);
            }
        }
    }

    /// Oldest pinned snapshot epoch, or `None` if no snapshot is active.
    /// Lowest snapshot epoch that compaction must preserve a version for, or
    /// `None` when no reader is pinned anywhere. Considers BOTH the single-table
    /// local pin set (`self.pinned`, used by the standalone `pin_snapshot` API)
    /// AND the shared `Database` snapshot registry (`db.snapshot()` readers) —
    /// otherwise a multi-table reader's version could be dropped by a compaction
    /// triggered on its table (the registry-gated reaper would then keep the
    /// old run *files*, but readers only scan the merged run, so the version
    /// would still be lost).
    pub(crate) fn min_active_snapshot(&self) -> Option<Epoch> {
        let local = self.pinned.keys().next().copied();
        let global = self.snapshots.min_pinned();
        let history = self.snapshots.history_floor(self.current_epoch());
        [local, global, history].into_iter().flatten().min()
    }

    /// Configure timestamp-column retention on a standalone table. Mounted
    /// databases should use [`crate::Database::set_table_ttl`] so the DDL is
    /// WAL-replicated.
    pub fn set_ttl(&mut self, column_name: &str, duration_nanos: u64) -> Result<()> {
        self.ensure_writable()?;
        let policy = self.prepare_ttl_policy(column_name, duration_nanos)?;
        self.apply_ttl_policy_at(Some(policy), self.current_epoch())
    }

    pub fn clear_ttl(&mut self) -> Result<()> {
        self.ensure_writable()?;
        self.apply_ttl_policy_at(None, self.current_epoch())
    }

    pub fn ttl(&self) -> Option<TtlPolicy> {
        self.ttl
    }

    pub(crate) fn prepare_ttl_policy(
        &self,
        column_name: &str,
        duration_nanos: u64,
    ) -> Result<TtlPolicy> {
        if duration_nanos == 0 || duration_nanos > i64::MAX as u64 {
            return Err(MongrelError::InvalidArgument(
                "TTL duration must be between 1 and i64::MAX nanoseconds".into(),
            ));
        }
        let column = self
            .schema
            .columns
            .iter()
            .find(|column| column.name == column_name)
            .ok_or_else(|| MongrelError::Schema(format!("unknown TTL column {column_name}")))?;
        if column.ty != TypeId::TimestampNanos {
            return Err(MongrelError::Schema(format!(
                "TTL column {column_name} must be TimestampNanos, is {:?}",
                column.ty
            )));
        }
        Ok(TtlPolicy {
            column_id: column.id,
            duration_nanos,
        })
    }

    pub(crate) fn apply_ttl_policy_at(
        &mut self,
        policy: Option<TtlPolicy>,
        epoch: Epoch,
    ) -> Result<()> {
        if let Some(policy) = policy {
            let column = self
                .schema
                .columns
                .iter()
                .find(|column| column.id == policy.column_id)
                .ok_or_else(|| {
                    MongrelError::Schema(format!("unknown TTL column id {}", policy.column_id))
                })?;
            if column.ty != TypeId::TimestampNanos
                || policy.duration_nanos == 0
                || policy.duration_nanos > i64::MAX as u64
            {
                return Err(MongrelError::Schema("invalid TTL policy".into()));
            }
        }
        self.ttl = policy;
        self.agg_cache = Arc::new(HashMap::new());
        self.clear_result_cache();
        let _ = std::fs::remove_dir_all(self.dir.join("_shadow"));
        self.persist_manifest(epoch)
    }

    pub(crate) fn row_expired_at(&self, row: &Row, now_nanos: i64) -> bool {
        let Some(policy) = self.ttl else {
            return false;
        };
        let Some(Value::Int64(timestamp)) = row.columns.get(&policy.column_id) else {
            return false;
        };
        timestamp.saturating_add(policy.duration_nanos as i64) <= now_nanos
    }

    pub fn current_epoch(&self) -> Epoch {
        self.epoch.visible()
    }

    pub fn memtable_len(&self) -> usize {
        self.memtable.len()
    }

    /// Live row count. O(1) without TTL; TTL tables scan because wall-clock
    /// expiry can change without a commit epoch.
    pub fn count(&self) -> u64 {
        if self.ttl.is_none()
            && self.pending_put_cols.is_empty()
            && self.pending_delete_rids.is_empty()
            && self.pending_rows.is_empty()
            && self.pending_dels.is_empty()
            && self.pending_truncate.is_none()
        {
            self.live_count
        } else {
            self.visible_rows(self.snapshot())
                .map(|rows| rows.len() as u64)
                .unwrap_or(self.live_count)
        }
    }

    /// Count rows matching an index-backed conjunctive predicate without
    /// materializing projected columns. Returns `None` when a condition cannot
    /// be served by the native predicate resolver.
    pub fn count_conditions(
        &mut self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Option<u64>> {
        use crate::query::Condition;
        if self.ttl.is_some() {
            if conditions.is_empty() {
                return Ok(Some(self.visible_rows(snapshot)?.len() as u64));
            }
            let mut sets = Vec::with_capacity(conditions.len());
            for condition in conditions {
                sets.push(self.resolve_condition(condition, snapshot)?);
            }
            let survivors = RowIdSet::intersect_many(sets);
            let rows = self.visible_rows(snapshot)?;
            return Ok(Some(
                rows.into_iter()
                    .filter(|row| survivors.contains(row.row_id.0))
                    .count() as u64,
            ));
        }
        if conditions.is_empty() {
            return Ok(Some(self.count()));
        }
        let served = |c: &Condition| {
            matches!(
                c,
                Condition::Pk(_)
                    | Condition::BitmapEq { .. }
                    | Condition::BitmapIn { .. }
                    | Condition::BytesPrefix { .. }
                    | Condition::FmContains { .. }
                    | Condition::FmContainsAll { .. }
                    | Condition::Ann { .. }
                    | Condition::Range { .. }
                    | Condition::RangeF64 { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::MinHashSimilar { .. }
                    | Condition::IsNull { .. }
                    | Condition::IsNotNull { .. }
            )
        };
        if !conditions.iter().all(served) {
            return Ok(None);
        }
        self.ensure_indexes_complete()?;
        if !self.pending_put_cols.is_empty()
            || !self.pending_delete_rids.is_empty()
            || !self.pending_rows.is_empty()
            || !self.pending_dels.is_empty()
            || self.pending_truncate.is_some()
        {
            let mut sets = Vec::with_capacity(conditions.len());
            for condition in conditions {
                sets.push(self.resolve_condition(condition, snapshot)?);
            }
            let rids = RowIdSet::intersect_many(sets).into_sorted_vec();
            return Ok(Some(self.rows_for_rids(&rids, snapshot)?.len() as u64));
        }
        let mut sets = Vec::with_capacity(conditions.len());
        for condition in conditions {
            sets.push(self.resolve_condition(condition, snapshot)?);
        }
        let mut rids = RowIdSet::intersect_many(sets);
        // §5.1: the in-memory indexes (bitmap/FM/ANN/sparse/minhash) are
        // append-only across puts (`index_row` adds entries but
        // `tombstone_row` never removes them), so deletes and PK-displacing
        // updates leave behind entries for now-tombstoned row-ids. The
        // materialize paths (`query`, `query_columns_native`) already drop
        // these via MVCC visibility during row fetch; only the count fast
        // path trusts raw index cardinality, so prune tombstoned overlay
        // row-ids here. On a clean table (empty overlay) the bitmap was
        // rebuilt at flush and is authoritative — the prune is skipped.
        if !self.memtable.is_empty() || !self.mutable_run.is_empty() {
            rids.remove_many(self.overlay_tombstoned_rids(snapshot));
        }
        let count = rids.len() as u64;
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::CountSurvivors;
            t.survivor_count = Some(count as usize);
            t.conditions_pushed = conditions.len();
        });
        Ok(Some(count))
    }

    /// Row-ids whose newest visible overlay version is a tombstone. Used to
    /// prune stale entries left behind by the append-only in-memory indexes
    /// (see `count_conditions`). Only unflushed tombstones matter — a flush
    /// rebuilds indexes from runs and excludes tombstoned rows. (§5.1)
    fn overlay_tombstoned_rids(&self, snapshot: Snapshot) -> Vec<u64> {
        let mut out = Vec::new();
        for row in self.memtable.visible_versions(snapshot.epoch) {
            if row.deleted {
                out.push(row.row_id.0);
            }
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            if row.deleted {
                out.push(row.row_id.0);
            }
        }
        out
    }

    /// Bulk-load typed columns straight to a new run — the fast ingest path.
    /// Bypasses the WAL, the memtable, and the `Value` enum entirely; writes one
    /// compressed run (delta for sorted Int64, dictionary for low-card Bytes)
    /// with **LZ4** (Phase 15.3 — fast decode for scan-heavy analytical runs),
    /// rotates the WAL, and persists the manifest in a single fsync group.
    /// Index building follows [`Table::index_build_policy`]: deferred to the
    /// first query/flush by default, or bulk-built inline from the typed
    /// columns (Phase 14.2) under [`IndexBuildPolicy::Eager`].
    pub fn bulk_load_columns(
        &mut self,
        user_columns: Vec<(u16, columnar::NativeColumn)>,
    ) -> Result<Epoch> {
        self.bulk_load_columns_with(user_columns, 3, false, true)
    }

    /// Maximal-throughput bulk ingest (Phase 14.4): skip zstd entirely and write
    /// raw `ALGO_PLAIN` pages. ~3–4× the encode throughput of
    /// [`Self::bulk_load_columns`] at ~3–4× the on-disk size — the right choice
    /// when ingest latency dominates and a background compaction will re-compress
    /// later. Indexing, WAL rotation, and the manifest are identical to
    /// [`Self::bulk_load_columns`].
    pub fn bulk_load_fast(
        &mut self,
        user_columns: Vec<(u16, columnar::NativeColumn)>,
    ) -> Result<Epoch> {
        self.bulk_load_columns_with(user_columns, -1, true, false)
    }

    fn bulk_load_columns_with(
        &mut self,
        mut user_columns: Vec<(u16, columnar::NativeColumn)>,
        zstd_level: i32,
        force_plain: bool,
        lz4: bool,
    ) -> Result<Epoch> {
        self.ensure_writable()?;
        let n = user_columns.first().map(|(_, c)| c.len()).unwrap_or(0);
        if n == 0 {
            return Ok(self.current_epoch());
        }
        let epoch = self.commit_new_epoch()?;
        let live_before = self.live_count;
        // Spill pending mutable-run data before the Flush marker + WAL rotation.
        self.spill_mutable_run(epoch)?;
        let eager_index_build = self.index_build_policy == IndexBuildPolicy::Eager
            && self.indexes_complete
            && self.run_refs.is_empty()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty();
        // Enforce NOT NULL constraints and primary-key upsert semantics before
        // any row id is allocated or bytes hit the run file.
        self.fill_auto_inc_native_columns(&mut user_columns, n)?;
        self.validate_columns_not_null(&user_columns, n)?;
        let winner_idx = self
            .bulk_pk_winner_indices(&user_columns, n)
            .filter(|idx| idx.len() != n);
        let (write_columns, write_n): (Vec<(u16, columnar::NativeColumn)>, usize) =
            match winner_idx.as_deref() {
                Some(idx) => {
                    let compacted = user_columns
                        .iter()
                        .map(|(id, c)| (*id, c.gather(idx)))
                        .collect();
                    (compacted, idx.len())
                }
                None => (user_columns, n),
            };
        self.advance_auto_inc_from_native_columns(&write_columns, write_n, live_before)?;
        let first = self.allocator.alloc_range(write_n as u64)?.0;
        for rid in first..first + write_n as u64 {
            self.reservoir.offer(rid);
        }
        let run_id = self.alloc_run_id()?;
        let path = self.run_path(run_id);
        let mut writer =
            RunWriter::new(&self.schema, run_id as u128, epoch, 0).with_native_endian();
        if force_plain {
            writer = writer.with_plain();
        } else if lz4 {
            // Phase 15.3: bulk-loaded analytical runs are scan-heavy, so encode
            // them with LZ4 (3–5× faster decode, ~10% worse ratio than zstd).
            writer = writer.with_lz4();
        } else {
            writer = writer.with_zstd_level(zstd_level);
        }
        if let Some(kek) = &self.kek {
            writer = writer.with_encryption(kek.as_ref(), self.indexable_column_specs());
        }
        let header = match self.create_run_file(run_id)? {
            Some(file) => writer.write_native_file(file, &write_columns, write_n, first)?,
            None => writer.write_native(&path, &write_columns, write_n, first)?,
        };
        self.run_refs.push(RunRef {
            run_id: run_id as u128,
            level: 0,
            epoch_created: epoch.0,
            row_count: header.row_count,
        });
        self.live_count = self.live_count.saturating_add(write_n as u64);
        if eager_index_build {
            let row_ids: Vec<u64> = (first..first + write_n as u64).collect();
            self.index_columns_bulk(&write_columns, &row_ids);
            self.indexes_complete = true;
            self.build_learned_ranges()?;
        } else {
            // Phase 14.7: defer index building off the ingest critical path for
            // non-empty tables where cross-run PK/update semantics must be
            // reconstructed from durable state.
            self.indexes_complete = false;
        }
        self.mark_flushed(epoch)?;
        self.persist_manifest(epoch)?;
        if eager_index_build {
            self.checkpoint_indexes(epoch);
        }
        self.clear_result_cache();
        self.data_generation = self.data_generation.wrapping_add(1);
        Ok(epoch)
    }

    /// Bulk-build the live in-memory indexes (HOT/bitmap/FM/sparse) straight
    /// from typed columns — the deferred batch-indexing path (Phase 14.2).
    ///
    /// Replaces the per-row `index_into` loop: no `Row`, no per-row
    /// `HashMap<u16, Value>`, no `Value` enum. Index keys are computed directly
    /// from the typed buffers via [`columnar::encode_key_native`], tokenized for
    /// `ENCRYPTED_INDEXABLE` columns the same way `index_into` on a tokenized
    /// row would. FM is appended dirty and rebuilt once on the next query; the
    /// others are populated in a single typed pass. Entries are merged into the
    /// existing indexes so this is correct under multi-run loads and partial
    /// reindexes.
    ///
    /// `row_ids[i]` is the `RowId` of element `i` of every column. ANN
    /// (`IndexKind::Ann`) is intentionally skipped: the native codec carries no
    /// embeddings, so an `Embedding` column can never reach this path (a native
    /// bulk load of an embedding schema fails at encode). LearnedRange is built
    /// separately from the runs by [`Self::build_learned_ranges`].
    fn index_columns_bulk(&mut self, columns: &[(u16, columnar::NativeColumn)], row_ids: &[u64]) {
        let n = row_ids.len();
        if n == 0 {
            return;
        }
        let by_id: std::collections::HashMap<u16, &columnar::NativeColumn> =
            columns.iter().map(|(id, c)| (*id, c)).collect();
        let ty_of: std::collections::HashMap<u16, TypeId> = self
            .schema
            .columns
            .iter()
            .map(|c| (c.id, c.ty.clone()))
            .collect();
        let pk_id = self.schema.primary_key().map(|c| c.id);

        for (i, &rid) in row_ids.iter().enumerate() {
            let row_id = RowId(rid);
            if let Some(pid) = pk_id {
                if let Some(col) = by_id.get(&pid) {
                    let ty = ty_of.get(&pid).cloned().unwrap_or(TypeId::Int64);
                    if let Some(key) = bulk_index_key(&self.column_keys, pid, ty, col, i) {
                        self.insert_hot_pk(key, row_id);
                    }
                }
            }
            for idef in &self.schema.indexes {
                let Some(col) = by_id.get(&idef.column_id) else {
                    continue;
                };
                let ty = ty_of.get(&idef.column_id).cloned().unwrap_or(TypeId::Int64);
                match idef.kind {
                    IndexKind::Bitmap => {
                        if let Some(b) = self.bitmap.get_mut(&idef.column_id) {
                            if let Some(key) =
                                bulk_index_key(&self.column_keys, idef.column_id, ty, col, i)
                            {
                                b.insert(key, row_id);
                            }
                        }
                    }
                    IndexKind::FmIndex => {
                        if let Some(f) = self.fm.get_mut(&idef.column_id) {
                            if let Some(bytes) = columnar::native_bytes_at(col, i) {
                                f.insert(bytes.to_vec(), row_id);
                            }
                        }
                    }
                    IndexKind::Sparse => {
                        if let Some(s) = self.sparse.get_mut(&idef.column_id) {
                            if let Some(bytes) = columnar::native_bytes_at(col, i) {
                                if let Ok(terms) = bincode::deserialize::<Vec<(u32, f32)>>(bytes) {
                                    s.insert(&terms, row_id);
                                }
                            }
                        }
                    }
                    IndexKind::MinHash => {
                        if let Some(mh) = self.minhash.get_mut(&idef.column_id) {
                            if let Some(bytes) = columnar::native_bytes_at(col, i) {
                                let tokens = crate::index::token_hashes_from_bytes(bytes);
                                mh.insert(&tokens, row_id);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// no `Value`). Fast path: empty memtable + single run decodes columns
    /// directly and gathers visible indices; falls back to the `Value` path
    /// pivoted to native columns otherwise. `projection` (a set of column ids)
    /// limits decoding to the requested columns — `None` ⇒ all user columns.
    pub fn visible_columns_native(
        &self,
        snapshot: Snapshot,
        projection: Option<&[u16]>,
    ) -> Result<Vec<(u16, columnar::NativeColumn)>> {
        self.visible_columns_native_inner(snapshot, projection, None)
    }

    pub fn visible_columns_native_with_control(
        &self,
        snapshot: Snapshot,
        projection: Option<&[u16]>,
        control: &crate::ExecutionControl,
    ) -> Result<Vec<(u16, columnar::NativeColumn)>> {
        self.visible_columns_native_inner(snapshot, projection, Some(control))
    }

    fn visible_columns_native_inner(
        &self,
        snapshot: Snapshot,
        projection: Option<&[u16]>,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Vec<(u16, columnar::NativeColumn)>> {
        execution_checkpoint(control, 0)?;
        let wanted: Vec<u16> = match projection {
            Some(p) => p.to_vec(),
            None => self.schema.columns.iter().map(|c| c.id).collect(),
        };
        if self.ttl.is_none()
            && self.memtable.is_empty()
            && self.mutable_run.is_empty()
            && self.run_refs.len() == 1
        {
            let rr = self.run_refs[0].clone();
            let mut reader = self.open_reader(rr.run_id)?;
            let idxs = reader.visible_indices_native(snapshot.epoch)?;
            execution_checkpoint(control, 0)?;
            let all_visible = idxs.len() == reader.row_count();
            // Phase 15.1: decode every requested column in parallel when the
            // reader is mmap-backed. Each column already parallel-decodes its
            // own pages, so a wide table saturates the pool via nested rayon
            // without oversubscribing (work-stealing handles it). Falls back to
            // the sequential `&mut` path when mmap is unavailable.
            if reader.has_mmap() && control.is_none() {
                use rayon::prelude::*;
                // Pre-resolve the requested ids that exist in the schema (don't
                // capture `self` inside the rayon closure).
                let valid: Vec<u16> = wanted
                    .iter()
                    .filter(|cid| self.schema.columns.iter().any(|c| c.id == **cid))
                    .copied()
                    .collect();
                // Decode concurrently; `collect` preserves `valid` order.
                let decoded: Vec<(u16, columnar::NativeColumn)> = valid
                    .par_iter()
                    .filter_map(|cid| {
                        reader
                            .column_native_shared(*cid)
                            .ok()
                            .map(|col| (*cid, col))
                    })
                    .collect();
                let cols = decoded
                    .into_iter()
                    .map(|(id, col)| (id, if all_visible { col } else { col.gather(&idxs) }))
                    .collect();
                return Ok(cols);
            }
            let mut cols = Vec::with_capacity(wanted.len());
            for (index, cid) in wanted.iter().enumerate() {
                execution_checkpoint(control, index)?;
                let cdef = match self.schema.columns.iter().find(|c| c.id == *cid) {
                    Some(c) => c,
                    None => continue,
                };
                let col = reader.column_native(cdef.id)?;
                cols.push((cdef.id, if all_visible { col } else { col.gather(&idxs) }));
            }
            return Ok(cols);
        }
        let vcols = self.visible_columns(snapshot)?;
        execution_checkpoint(control, 0)?;
        let want_set: std::collections::HashSet<u16> = wanted.iter().copied().collect();
        let out: Vec<(u16, columnar::NativeColumn)> = vcols
            .into_iter()
            .filter(|(id, _)| want_set.contains(id))
            .map(|(id, vals)| {
                let ty = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.id == id)
                    .map(|c| c.ty.clone())
                    .unwrap_or(TypeId::Bytes);
                (id, columnar::values_to_native(ty, &vals))
            })
            .collect();
        Ok(out)
    }

    pub fn run_count(&self) -> usize {
        self.run_refs.len()
    }

    /// Whether the memtable is empty (no unflushed puts).
    pub fn memtable_is_empty(&self) -> bool {
        self.memtable.is_empty()
    }

    /// Cumulative raw-page-cache hit/miss counts (Priority 14: hit visibility).
    /// Useful for confirming a repeat scan is served from cache or measuring a
    /// query's locality after [`reset_page_cache_stats`](Self::reset_page_cache_stats).
    pub fn page_cache_stats(&self) -> crate::cache::CacheStats {
        self.page_cache.stats()
    }

    /// Zero the raw-page-cache hit/miss counters.
    pub fn reset_page_cache_stats(&self) {
        self.page_cache.reset_stats();
    }

    /// The run IDs in level order (Phase 15.5: used by the Arrow IPC shadow to
    /// key shadow files and detect stale shadows).
    pub fn run_ids(&self) -> Vec<u128> {
        self.run_refs.iter().map(|r| r.run_id).collect()
    }

    /// Whether the single run (if exactly one) is clean — i.e. has
    /// `RUN_FLAG_CLEAN` set (Phase 15.5: the shadow is zero-copy only for clean
    /// runs).
    pub fn single_run_is_clean(&self) -> bool {
        if self.ttl.is_some() || self.run_refs.len() != 1 {
            return false;
        }
        self.open_reader(self.run_refs[0].run_id)
            .map(|r| r.is_clean())
            .unwrap_or(false)
    }

    /// Best-effort resolve of the survivor RowId set for fine-grained cache
    /// invalidation (hardening (c)). On the single-run fast path, opens a reader
    /// and calls `resolve_survivor_rids`. On the multi-run/memtable path,
    /// returns an empty bitmap — conservative (condition_cols still catches
    /// column mutations, and deletes are caught by the epoch-free design falling
    /// through to the multi-run path which re-resolves).
    fn resolve_footprint(
        &self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> roaring::RoaringBitmap {
        if !self.memtable.is_empty() || !self.mutable_run.is_empty() {
            return roaring::RoaringBitmap::new();
        }
        if self.run_refs.is_empty() {
            return roaring::RoaringBitmap::new();
        }
        // Try the single-run fast path.
        if self.run_refs.len() == 1 {
            if let Ok(mut reader) = self.open_reader(self.run_refs[0].run_id) {
                if let Ok(rids) = self.resolve_survivor_rids(conditions, &mut reader, snapshot) {
                    return rids.to_roaring_lossy();
                }
            }
        }
        roaring::RoaringBitmap::new()
    }

    /// Phase 19.1 + hardening (c): a cached form of
    /// [`Table::query_columns_native`]. The cache key embeds the snapshot epoch
    /// so two queries at different pinned snapshots never share an entry;
    /// invalidation is fine-grained — a `commit()` drops only entries whose
    /// footprint intersects a deleted RowId or whose condition-columns intersect
    /// a mutated column. On a miss the underlying `query_columns_native` runs and
    /// the result is cached as typed `NativeColumn`s. Returns `None` exactly when
    /// the non-cached path would (conditions not pushdown-served). Strictly
    /// additive — callers wanting fresh results keep using
    /// `query_columns_native`.
    pub fn query_columns_native_cached(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        self.query_columns_native_cached_inner(conditions, projection, snapshot, None)
    }

    pub fn query_columns_native_cached_with_control(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
        control: &crate::ExecutionControl,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        self.query_columns_native_cached_inner(conditions, projection, snapshot, Some(control))
    }

    fn query_columns_native_cached_inner(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        execution_checkpoint(control, 0)?;
        // Wall-clock expiry changes without an MVCC epoch, so an epoch-keyed
        // result can become stale while sitting in the cache.
        if self.ttl.is_some() {
            return self.query_columns_native_inner(conditions, projection, snapshot, control);
        }
        if conditions.is_empty() {
            return self.query_columns_native_inner(conditions, projection, snapshot, control);
        }
        // The snapshot epoch is part of the key so two queries with identical
        // conditions/projection but pinned at different snapshots never share a
        // cached result (MVCC isolation for the explicit-snapshot API).
        let key = crate::query::canonical_query_key(conditions, projection, snapshot.epoch.0);
        if let Some(hit) = self.result_cache.lock().get_columns(key) {
            crate::trace::QueryTrace::record(|t| {
                t.result_cache_hit = true;
                t.scan_mode = crate::trace::ScanMode::NativePushdown;
            });
            return Ok(Some((*hit).clone()));
        }
        let res = self.query_columns_native_inner(conditions, projection, snapshot, control)?;
        execution_checkpoint(control, 0)?;
        if let Some(cols) = &res {
            let footprint = self.resolve_footprint(conditions, snapshot);
            let condition_cols = crate::query::condition_columns(conditions);
            execution_checkpoint(control, 0)?;
            self.result_cache.lock().insert(
                key,
                CachedEntry {
                    data: CachedData::Columns(Arc::new(cols.clone())),
                    footprint,
                    condition_cols,
                },
            );
        }
        Ok(res)
    }

    /// Phase 19.1 + hardening (c): a cached form of [`Table::query`]. The cache key
    /// is epoch-independent; invalidation is fine-grained (see
    /// [`Table::query_columns_native_cached`]). On a hit returns the cached rows (no
    /// re-resolve, no re-decode).
    pub fn query_cached(&mut self, q: &crate::query::Query) -> Result<Vec<Row>> {
        if self.ttl.is_some() {
            return self.query(q);
        }
        if q.conditions.is_empty() {
            return self.query(q);
        }
        let key = crate::query::canonical_query_key(&q.conditions, None, 0)
            ^ (q.limit.unwrap_or(usize::MAX) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (q.offset as u64).wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        if let Some(hit) = self.result_cache.lock().get_rows(key) {
            crate::trace::QueryTrace::record(|t| {
                t.result_cache_hit = true;
                t.scan_mode = crate::trace::ScanMode::Materialized;
            });
            return Ok((*hit).clone());
        }
        let rows = self.query(q)?;
        let footprint = rows.iter().map(|r| r.row_id.0 as u32).collect();
        let condition_cols = crate::query::condition_columns(&q.conditions);
        self.result_cache.lock().insert(
            key,
            CachedEntry {
                data: CachedData::Rows(Arc::new(rows.clone())),
                footprint,
                condition_cols,
            },
        );
        Ok(rows)
    }

    // -----------------------------------------------------------------------
    // Traced query wrappers (OPTIMIZATIONS.md Priority 0 / 16).
    //
    // Each `_traced` method runs its underlying query inside a
    // [`crate::trace::QueryTrace::capture`] scope and returns the result
    // alongside the captured path trace. The trace records which physical path
    // served the query (cursor / pushdown / materialized / count-shortcut),
    // whether indexes were rebuilt, whether the result cache hit, overlay size,
    // survivor count, and the fast row-id map usage. Recording is zero-cost
    // when no `_traced` method is on the call stack (the plain methods are
    // unchanged).
    // -----------------------------------------------------------------------

    /// [`Self::query_columns_native`] with a captured [`crate::trace::QueryTrace`].
    #[allow(clippy::type_complexity)]
    pub fn query_columns_native_traced(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<(
        Option<Vec<(u16, columnar::NativeColumn)>>,
        crate::trace::QueryTrace,
    )> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.query_columns_native(conditions, projection, snapshot)
        });
        Ok((result?, trace))
    }

    /// [`Self::query_columns_native_cached`] with a captured
    /// [`crate::trace::QueryTrace`] (records result-cache hits too).
    #[allow(clippy::type_complexity)]
    pub fn query_columns_native_cached_traced(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<(
        Option<Vec<(u16, columnar::NativeColumn)>>,
        crate::trace::QueryTrace,
    )> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.query_columns_native_cached(conditions, projection, snapshot)
        });
        Ok((result?, trace))
    }

    /// [`Self::native_page_cursor`] with a captured [`crate::trace::QueryTrace`].
    pub fn native_page_cursor_traced(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<(Option<NativePageCursor>, crate::trace::QueryTrace)> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.native_page_cursor(snapshot, projection, conditions)
        });
        Ok((result?, trace))
    }

    /// [`Self::native_multi_run_cursor`] with a captured [`crate::trace::QueryTrace`].
    pub fn native_multi_run_cursor_traced(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<(
        Option<crate::cursor::MultiRunCursor>,
        crate::trace::QueryTrace,
    )> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| {
            self.native_multi_run_cursor(snapshot, projection, conditions)
        });
        Ok((result?, trace))
    }

    /// [`Self::count_conditions`] with a captured [`crate::trace::QueryTrace`].
    pub fn count_conditions_traced(
        &mut self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<(Option<u64>, crate::trace::QueryTrace)> {
        let (result, trace) =
            crate::trace::QueryTrace::capture(|| self.count_conditions(conditions, snapshot));
        Ok((result?, trace))
    }

    /// [`Self::query`] with a captured [`crate::trace::QueryTrace`].
    pub fn query_traced(
        &mut self,
        q: &crate::query::Query,
    ) -> Result<(Vec<Row>, crate::trace::QueryTrace)> {
        let (result, trace) = crate::trace::QueryTrace::capture(|| self.query(q));
        Ok((result?, trace))
    }

    /// Predicate pushdown: resolve `conditions` via indexes to find the matching
    /// row-id set, then decode only those rows' columns — not the whole table.
    /// Returns `None` if the conditions can't be served by indexes (caller falls
    /// back to a full scan). This is the fast path for `WHERE col = 'value'`.
    pub fn query_columns_native(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        self.query_columns_native_inner(conditions, projection, snapshot, None)
    }

    pub fn query_columns_native_with_control(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
        control: &crate::ExecutionControl,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        self.query_columns_native_inner(conditions, projection, snapshot, Some(control))
    }

    fn query_columns_native_inner(
        &mut self,
        conditions: &[crate::query::Condition],
        projection: Option<&[u16]>,
        snapshot: Snapshot,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Option<Vec<(u16, columnar::NativeColumn)>>> {
        use crate::query::Condition;
        execution_checkpoint(control, 0)?;
        // TTL reads use the materialized visibility path so the wall-clock
        // cutoff is captured once and applied to every storage tier.
        if self.ttl.is_some() {
            return Ok(None);
        }
        if conditions.is_empty() {
            return Ok(None);
        }
        self.ensure_indexes_complete()?;

        // Only these conditions are pushdown-served. Range/RangeF64 need a
        // column read on the single-run fast path; off it they fall back to a
        // visible-rows scan via `resolve_condition` (still correct for any
        // layout, just not page-pruned).
        let served = |c: &Condition| {
            matches!(
                c,
                Condition::Pk(_)
                    | Condition::BitmapEq { .. }
                    | Condition::BitmapIn { .. }
                    | Condition::BytesPrefix { .. }
                    | Condition::FmContains { .. }
                    | Condition::FmContainsAll { .. }
                    | Condition::Ann { .. }
                    | Condition::Range { .. }
                    | Condition::RangeF64 { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::MinHashSimilar { .. }
                    | Condition::IsNull { .. }
                    | Condition::IsNotNull { .. }
            )
        };
        if !conditions.iter().all(served) {
            return Ok(None);
        }
        let fast_path =
            self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1;
        crate::trace::QueryTrace::record(|t| {
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
            t.conditions_pushed = conditions.len();
            t.learned_range_used = conditions.iter().any(|c| match c {
                Condition::Range { column_id, .. } | Condition::RangeF64 { column_id, .. } => {
                    self.learned_range.contains_key(column_id)
                }
                _ => false,
            });
        });
        // Build column list (projected or all user columns) + projection pairs.
        let col_ids: Vec<u16> = projection
            .map(|p| p.to_vec())
            .unwrap_or_else(|| self.schema.columns.iter().map(|c| c.id).collect());
        let proj_pairs: Vec<(u16, TypeId)> = col_ids
            .iter()
            .map(|&cid| {
                let ty = self
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.id == cid)
                    .map(|c| c.ty.clone())
                    .unwrap_or(TypeId::Bytes);
                (cid, ty)
            })
            .collect();

        // -----------------------------------------------------------------------
        // Fast path: single run, empty memtable/mutable-run → resolve survivors,
        // binary-search positions, gather only the projected columns from one
        // reader. This is the fastest pushdown path (no cursor overhead).
        // -----------------------------------------------------------------------
        if fast_path {
            // A Range/RangeF64 needs a column read *unless* its column has a
            // learned (PGM) range index, in which case it's served in-memory.
            let needs_column = conditions.iter().any(|c| match c {
                Condition::Range { column_id, .. } => !self.learned_range.contains_key(column_id),
                Condition::RangeF64 { column_id, .. } => {
                    !self.learned_range.contains_key(column_id)
                }
                _ => false,
            });
            let mut reader_opt: Option<RunReader> = if needs_column {
                Some(self.open_reader(self.run_refs[0].run_id)?)
            } else {
                None
            };
            let mut sets: Vec<RowIdSet> = Vec::new();
            for (index, c) in conditions.iter().enumerate() {
                execution_checkpoint(control, index)?;
                let s = match c {
                    Condition::Range { column_id, lo, hi }
                        if !self.learned_range.contains_key(column_id) =>
                    {
                        if reader_opt.is_none() {
                            reader_opt = Some(self.open_reader(self.run_refs[0].run_id)?);
                        }
                        reader_opt
                            .as_mut()
                            .expect("reader opened for range")
                            .range_row_id_set_i64(*column_id, *lo, *hi)?
                    }
                    Condition::RangeF64 {
                        column_id,
                        lo,
                        lo_inclusive,
                        hi,
                        hi_inclusive,
                    } if !self.learned_range.contains_key(column_id) => {
                        if reader_opt.is_none() {
                            reader_opt = Some(self.open_reader(self.run_refs[0].run_id)?);
                        }
                        reader_opt
                            .as_mut()
                            .expect("reader opened for range")
                            .range_row_id_set_f64(
                                *column_id,
                                *lo,
                                *lo_inclusive,
                                *hi,
                                *hi_inclusive,
                            )?
                    }
                    _ => self.resolve_condition(c, snapshot)?,
                };
                sets.push(s);
            }
            let candidates = RowIdSet::intersect_many(sets);
            crate::trace::QueryTrace::record(|t| {
                t.survivor_count = Some(candidates.len());
            });
            if candidates.is_empty() {
                let cols: Vec<(u16, columnar::NativeColumn)> = col_ids
                    .iter()
                    .map(|&id| {
                        (
                            id,
                            columnar::null_native(
                                proj_pairs
                                    .iter()
                                    .find(|(c, _)| c == &id)
                                    .map(|(_, t)| t.clone())
                                    .unwrap_or(TypeId::Bytes),
                                0,
                            ),
                        )
                    })
                    .collect();
                return Ok(Some(cols));
            }
            let mut reader = match reader_opt.take() {
                Some(r) => r,
                None => self.open_reader(self.run_refs[0].run_id)?,
            };
            let candidate_ids = candidates.into_sorted_vec();
            let (positions, fast_rid) = if let Some(positions) =
                reader.positions_for_row_ids_fast(&candidate_ids)
            {
                (positions, true)
            } else {
                let col = reader.column_native(crate::sorted_run::SYS_ROW_ID)?;
                match col {
                    columnar::NativeColumn::Int64 { data, .. } => {
                        let mut p = Vec::with_capacity(candidate_ids.len());
                        for (index, rid) in candidate_ids.iter().enumerate() {
                            execution_checkpoint(control, index)?;
                            if let Ok(position) = data.binary_search(&(*rid as i64)) {
                                p.push(position);
                            }
                        }
                        p.sort_unstable();
                        (p, false)
                    }
                    _ => return Err(MongrelError::InvalidArgument("sys row_id not int64".into())),
                }
            };
            crate::trace::QueryTrace::record(|t| {
                t.scan_mode = crate::trace::ScanMode::NativePushdown;
                t.fast_row_id_map = fast_rid;
            });
            let mut cols = Vec::with_capacity(col_ids.len());
            for (index, cid) in col_ids.iter().enumerate() {
                execution_checkpoint(control, index)?;
                let col = reader.column_native(*cid)?;
                cols.push((*cid, col.gather(&positions)));
            }
            return Ok(Some(cols));
        }

        // -----------------------------------------------------------------------
        // Non-fast path (multi-run / non-empty overlay). Route through the
        // columnar cursor (OPTIMIZATIONS.md Priority 1 + 4): the cursor builder
        // resolves MVCC, predicates, and overlay internally in batch, then
        // streams projected columns page-by-page. This avoids the per-rid
        // `rows_for_rids` `get_version`-across-all-runs cost that made multi-run
        // pushdown ~1000× slower than the single-run fast path.
        //
        // The cursor handles both single-run-with-overlay (`native_page_cursor`)
        // and multi-run (`native_multi_run_cursor`) layouts. The empty-table
        // (no runs, memtable-only) edge case falls through to `rows_for_rids`.
        // -----------------------------------------------------------------------
        if !self.run_refs.is_empty() {
            use crate::cursor::{
                drain_cursor_to_columns, drain_cursor_to_columns_with_control, Cursor,
            };
            let remaining: usize;
            let mut cursor: Box<dyn crate::cursor::Cursor> = if self.run_refs.len() == 1 {
                let c = self
                    .native_page_cursor(snapshot, proj_pairs.clone(), conditions)?
                    .expect("single-run cursor should build when run_refs.len() == 1");
                remaining = c.remaining_rows();
                Box::new(c)
            } else {
                let c = self
                    .native_multi_run_cursor(snapshot, proj_pairs.clone(), conditions)?
                    .expect("multi-run cursor should build when run_refs.len() >= 1");
                remaining = c.remaining_rows();
                Box::new(c)
            };
            crate::trace::QueryTrace::record(|t| {
                if t.survivor_count.is_none() {
                    t.survivor_count = Some(remaining);
                }
            });
            let cols = match control {
                Some(control) => {
                    drain_cursor_to_columns_with_control(cursor.as_mut(), &proj_pairs, control)?
                }
                None => drain_cursor_to_columns(cursor.as_mut(), &proj_pairs)?,
            };
            return Ok(Some(cols));
        }

        // Empty-table fallback (no sorted runs, memtable/mutable-run only): the
        // cursor builders return `None` for `run_refs.is_empty()`, so resolve
        // from overlay indexes and materialize via `rows_for_rids`. This is the
        // rare edge case (fresh table with only `put`s, no `flush`/`bulk_load`).
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::Materialized;
            t.row_materialized = true;
        });
        let mut sets: Vec<RowIdSet> = Vec::with_capacity(conditions.len());
        for (index, c) in conditions.iter().enumerate() {
            execution_checkpoint(control, index)?;
            sets.push(self.resolve_condition(c, snapshot)?);
        }
        let rids = RowIdSet::intersect_many(sets).into_sorted_vec();
        let rows = self.rows_for_rids(&rids, snapshot)?;
        let mut cols: Vec<(u16, columnar::NativeColumn)> = Vec::with_capacity(col_ids.len());
        for (index, (cid, ty)) in proj_pairs.iter().enumerate() {
            execution_checkpoint(control, index)?;
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(cid).cloned().unwrap_or(Value::Null))
                .collect();
            cols.push((*cid, columnar::values_to_native(ty.clone(), &vals)));
        }
        Ok(Some(cols))
    }

    /// Build a lazy, page-aware [`NativePageCursor`] for the single-run fast
    /// path. MVCC visibility and predicate survivor resolution are settled up
    /// front (so they see the live indexes under the DB lock); the cursor then
    /// owns the reader and decodes only the projected columns of pages that
    /// contain survivors, lazily. This is the fused-predicate + page-skip +
    /// late-materialization scan.
    ///
    /// Phase 13.1: the memtable / mutable-run overlay is now handled. Rows with
    /// a newer version in the overlay are excluded from the run's page plans
    /// (their run version is stale); the overlay rows are pre-materialized and
    /// appended as a final batch via [`NativePageCursor::new_with_overlay`].
    ///
    /// Returns `None` only for multiple sorted runs; the caller falls back to
    /// the materialize-then-stream scan for that layout.
    pub fn native_page_cursor(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<Option<NativePageCursor>> {
        use crate::cursor::build_page_plans;
        if self.ttl.is_some() {
            return Ok(None);
        }
        // See `scan_cursor`: incomplete (deferred) indexes cannot resolve
        // conditions — signal "can't serve" instead of empty survivor sets.
        if !conditions.is_empty() && !self.indexes_complete {
            return Ok(None);
        }
        if self.run_refs.len() != 1 {
            return Ok(None);
        }
        let mut reader = self.open_reader(self.run_refs[0].run_id)?;
        let (positions, rids) = reader.visible_positions_with_rids(snapshot.epoch)?;

        // Collect overlay rows from memtable + mutable_run (visible, newest
        // version per row). These shadow any stale version in the run.
        let overlay_rids: HashSet<u64> = {
            let mut s = HashSet::new();
            for row in self.memtable.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            for row in self.mutable_run.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            s
        };

        // Resolve survivor rids via indexes (covers overlay rows for index-
        // served conditions: PK, bitmap, FM, ANN, sparse — all maintained on
        // every put).
        let survivors = if conditions.is_empty() {
            None
        } else {
            Some(self.resolve_survivor_rids(conditions, &mut reader, snapshot)?)
        };

        // Exclude overlay rids from the run portion: their version in the run
        // is stale (updated/deleted in the overlay) or they don't exist in the
        // run (new inserts). When there are conditions, we remove overlay rids
        // from the survivor set. When there are no conditions, we synthesize a
        // survivor set = (all visible run rids) − (overlay rids) so the stale
        // run rows are pruned.
        let run_survivors: Option<RowIdSet> = if overlay_rids.is_empty() {
            survivors.clone()
        } else if let Some(s) = &survivors {
            let mut run_set = s.clone();
            run_set.remove_many(overlay_rids.iter().copied());
            Some(run_set)
        } else {
            Some(RowIdSet::from_unsorted(
                rids.iter()
                    .map(|&r| r as u64)
                    .filter(|r| !overlay_rids.contains(r))
                    .collect(),
            ))
        };

        let overlay_rows = if overlay_rids.is_empty() {
            Vec::new()
        } else {
            let bound = Self::overlay_materialization_bound(conditions, &survivors);
            self.overlay_visible_rows(snapshot, bound)
        };

        // Build page plans for the run portion.
        let plans = if positions.is_empty() {
            Vec::new()
        } else {
            let page_rows = reader.page_row_counts(crate::sorted_run::SYS_ROW_ID)?;
            build_page_plans(&positions, &rids, &page_rows, run_survivors.as_ref())
        };

        // Filter and materialize the overlay.
        let overlay = if overlay_rows.is_empty() {
            None
        } else {
            let filtered =
                self.filter_overlay_rows(overlay_rows, conditions, survivors.as_ref(), snapshot)?;
            if filtered.is_empty() {
                None
            } else {
                Some(self.materialize_overlay(&filtered, &projection))
            }
        };

        let overlay_row_count = overlay
            .as_ref()
            .map(|c| c.first().map(|c| c.len()).unwrap_or(0))
            .unwrap_or(0);
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::NativePageCursor;
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
            t.overlay_rows = overlay_row_count;
            t.conditions_pushed = conditions.len();
            t.pages_decoded = plans
                .iter()
                .map(|p| p.positions.len())
                .sum::<usize>()
                .min(1);
        });

        Ok(Some(NativePageCursor::new_with_overlay(
            reader, projection, plans, overlay,
        )))
    }
    /// Generalizes [`Self::native_page_cursor`] (single-run) to arbitrary run
    /// counts via a k-way merge by `RowId`. Cross-run MVCC resolution (newest
    /// visible version per `RowId`) and predicate survivor resolution are settled
    /// up front from the cheap system columns + global indexes; the cursor then
    /// lazily decodes the projected data columns of just the pages that own
    /// survivors, each page at most once. The memtable / mutable-run overlay is
    /// materialized and yielded as a final batch (mirroring the single-run path).
    ///
    /// Returns `None` only when there are no runs at all (caller falls back).
    #[allow(clippy::type_complexity)]
    pub fn native_multi_run_cursor(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<Option<crate::cursor::MultiRunCursor>> {
        use crate::cursor::{MultiRunCursor, RunStream};
        use crate::sorted_run::SYS_ROW_ID;
        use std::collections::{BinaryHeap, HashMap, HashSet};
        if self.ttl.is_some() {
            return Ok(None);
        }
        // See `scan_cursor`: incomplete (deferred) indexes cannot resolve
        // conditions — signal "can't serve" instead of empty survivor sets.
        if !conditions.is_empty() && !self.indexes_complete {
            return Ok(None);
        }
        if self.run_refs.is_empty() {
            return Ok(None);
        }

        // Open each run once; read its system columns + page layout.
        let mut run_meta: Vec<(RunReader, Vec<i64>, Vec<i64>, Vec<u8>, Vec<usize>)> =
            Vec::with_capacity(self.run_refs.len());
        for rr in &self.run_refs {
            let mut reader = self.open_reader(rr.run_id)?;
            let (rids, eps, del) = reader.system_columns_native()?;
            let page_rows = reader.page_row_counts(SYS_ROW_ID)?;
            run_meta.push((reader, rids, eps, del, page_rows));
        }

        // Global cross-run newest-version resolution: rid -> (epoch, run_idx,
        // position, deleted). Mirrors `visible_rows`, tracking which run owns
        // the newest MVCC-visible version.
        let mut best: HashMap<u64, (u64, usize, usize, bool)> = HashMap::new();
        for (run_idx, (_, rids, eps, del, _)) in run_meta.iter().enumerate() {
            for i in 0..rids.len() {
                let rid = rids[i] as u64;
                let e = eps[i] as u64;
                if e > snapshot.epoch.0 {
                    continue;
                }
                let is_del = del[i] != 0;
                best.entry(rid)
                    .and_modify(|cur| {
                        if e > cur.0 {
                            *cur = (e, run_idx, i, is_del);
                        }
                    })
                    .or_insert((e, run_idx, i, is_del));
            }
        }

        // Overlay rids (memtable + mutable-run) shadow every run version.
        let overlay_rids: HashSet<u64> = {
            let mut s = HashSet::new();
            for row in self.memtable.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            for row in self.mutable_run.visible_versions(snapshot.epoch) {
                s.insert(row.row_id.0);
            }
            s
        };

        // Predicate survivors (global, layout-independent).
        let survivors: Option<RowIdSet> = if conditions.is_empty() {
            None
        } else {
            let mut sets: Vec<RowIdSet> = Vec::with_capacity(conditions.len());
            for c in conditions {
                sets.push(self.resolve_condition(c, snapshot)?);
            }
            Some(RowIdSet::intersect_many(sets))
        };

        // Per-run owned survivors: (rid, position), ascending by rid. A row is
        // owned by the run holding its newest visible version, is not deleted,
        // is not shadowed by the overlay, and satisfies the predicate.
        let mut per_run: Vec<Vec<(u64, usize)>> = vec![Vec::new(); run_meta.len()];
        for (rid, (_, run_idx, pos, deleted)) in &best {
            if *deleted {
                continue;
            }
            if overlay_rids.contains(rid) {
                continue;
            }
            if let Some(s) = &survivors {
                if !s.contains(*rid) {
                    continue;
                }
            }
            per_run[*run_idx].push((*rid, *pos));
        }
        for v in per_run.iter_mut() {
            v.sort_unstable_by_key(|&(rid, _)| rid);
        }

        // Build the merge streams: map each owned position to (page_seq, within).
        let mut streams = Vec::with_capacity(run_meta.len());
        let mut heap: BinaryHeap<std::cmp::Reverse<(u64, usize)>> = BinaryHeap::new();
        let mut total = 0usize;
        for (run_idx, (reader, _, _, _, page_rows)) in run_meta.into_iter().enumerate() {
            let mut starts = Vec::with_capacity(page_rows.len());
            let mut acc = 0usize;
            for &r in &page_rows {
                starts.push(acc);
                acc += r;
            }
            let mut survivors_vec: Vec<(u64, usize, usize)> =
                Vec::with_capacity(per_run[run_idx].len());
            for &(rid, pos) in &per_run[run_idx] {
                let page_seq = match starts.partition_point(|&s| s <= pos) {
                    0 => continue,
                    p => p - 1,
                };
                let within = pos - starts[page_seq];
                survivors_vec.push((rid, page_seq, within));
            }
            total += survivors_vec.len();
            if let Some(&(rid, _, _)) = survivors_vec.first() {
                heap.push(std::cmp::Reverse((rid, run_idx)));
            }
            streams.push(RunStream::new(reader, survivors_vec, page_rows));
        }

        // Materialize the overlay (filtered + projected), yielded as the final batch.
        let overlay_rows = if overlay_rids.is_empty() {
            Vec::new()
        } else {
            let bound = Self::overlay_materialization_bound(conditions, &survivors);
            self.overlay_visible_rows(snapshot, bound)
        };
        let overlay = if overlay_rows.is_empty() {
            None
        } else {
            let filtered =
                self.filter_overlay_rows(overlay_rows, conditions, survivors.as_ref(), snapshot)?;
            if filtered.is_empty() {
                None
            } else {
                Some(self.materialize_overlay(&filtered, &projection))
            }
        };

        let overlay_row_count = overlay
            .as_ref()
            .map(|c| c.first().map(|c| c.len()).unwrap_or(0))
            .unwrap_or(0);
        crate::trace::QueryTrace::record(|t| {
            t.scan_mode = crate::trace::ScanMode::MultiRunCursor;
            t.run_count = self.run_refs.len();
            t.memtable_rows = self.memtable.len();
            t.mutable_run_rows = self.mutable_run.len();
            t.overlay_rows = overlay_row_count;
            t.conditions_pushed = conditions.len();
            t.survivor_count = Some(total);
        });

        Ok(Some(MultiRunCursor::new(
            streams, projection, heap, total, overlay,
        )))
    }

    /// Collect visible, non-deleted overlay rows from the memtable and mutable-
    /// run tier at `snapshot`. These are the rows whose data lives only in the
    /// in-memory buffers (not yet in a sorted run), or that shadow a stale
    /// version in the run.
    /// The survivor set that bounds overlay materialization (Priority 2), or
    /// `None` when overlay rows must be fully materialized — i.e. there is a
    /// `Range`/`RangeF64` residual, for which the index-served survivor set does
    /// not cover matching overlay rows (those are evaluated downstream). This
    /// mirrors the `all_index_served` branch of
    /// [`filter_overlay_rows`](Self::filter_overlay_rows), so bounding here is
    /// result-preserving.
    fn overlay_materialization_bound<'a>(
        conditions: &[crate::query::Condition],
        survivors: &'a Option<RowIdSet>,
    ) -> Option<&'a RowIdSet> {
        use crate::query::Condition;
        let has_range = conditions
            .iter()
            .any(|c| matches!(c, Condition::Range { .. } | Condition::RangeF64 { .. }));
        if has_range {
            None
        } else {
            survivors.as_ref()
        }
    }

    /// Materialize the visible overlay rows (memtable + mutable-run, newest
    /// version per row, non-deleted).
    ///
    /// Priority 2 (selective overlay probing): when `bound` is `Some`, only rows
    /// whose id is in it are materialized. The caller passes the index-resolved
    /// survivor set as `bound` exactly when every condition is index-served — in
    /// which case [`filter_overlay_rows`](Self::filter_overlay_rows) would discard
    /// any non-survivor overlay row anyway, so this prunes the materialization
    /// without changing the result. With a Range/RangeF64 residual the survivor
    /// set is incomplete for overlay rows, so the caller passes `None` (full
    /// materialization) and the range is re-evaluated downstream.
    fn overlay_visible_rows(&self, snapshot: Snapshot, bound: Option<&RowIdSet>) -> Vec<Row> {
        let mut best: HashMap<u64, (Epoch, Row)> = HashMap::new();
        let mut fold = |row: Row| {
            if let Some(b) = bound {
                if !b.contains(row.row_id.0) {
                    return;
                }
            }
            best.entry(row.row_id.0)
                .and_modify(|(be, br)| {
                    if row.committed_epoch > *be {
                        *be = row.committed_epoch;
                        *br = row.clone();
                    }
                })
                .or_insert_with(|| (row.committed_epoch, row));
        };
        for row in self.memtable.visible_versions(snapshot.epoch) {
            fold(row);
        }
        for row in self.mutable_run.visible_versions(snapshot.epoch) {
            fold(row);
        }
        let mut out: Vec<Row> = best
            .into_values()
            .filter_map(|(_, r)| if r.deleted { None } else { Some(r) })
            .collect();
        out.sort_by_key(|r| r.row_id);
        out
    }

    /// Filter overlay rows against the conjunctive predicate. Range / RangeF64
    /// are evaluated directly (the reader-served survivor set misses overlay
    /// rows). All other conditions are index-served (indexes maintained on
    /// every `put`) so the intersected `survivors` set includes overlay rows
    /// that match — but ONLY when every condition is index-served. When there
    /// is a mix, we compute per-condition index sets for non-range conditions
    /// and evaluate range conditions directly, so the intersection is correct.
    fn filter_overlay_rows(
        &self,
        rows: Vec<Row>,
        conditions: &[crate::query::Condition],
        survivors: Option<&RowIdSet>,
        snapshot: Snapshot,
    ) -> Result<Vec<Row>> {
        if conditions.is_empty() {
            return Ok(rows);
        }
        use crate::query::Condition;
        // Determine whether every condition is index-served (survivors set is
        // then complete for overlay rows). If so, a simple membership check
        // suffices and is cheapest.
        let all_index_served = !conditions
            .iter()
            .any(|c| matches!(c, Condition::Range { .. } | Condition::RangeF64 { .. }));
        if all_index_served {
            return Ok(rows
                .into_iter()
                .filter(|r| survivors.is_none_or(|s| s.contains(r.row_id.0)))
                .collect());
        }
        // Mixed: compute per-condition index sets for non-range conditions, and
        // evaluate range conditions directly on column values.
        let mut per_cond_sets: Vec<RowIdSet> = Vec::with_capacity(conditions.len());
        for c in conditions {
            let s = match c {
                Condition::Range { .. } | Condition::RangeF64 { .. } => RowIdSet::empty(),
                _ => self.resolve_condition(c, snapshot)?,
            };
            per_cond_sets.push(s);
        }
        Ok(rows
            .into_iter()
            .filter(|row| {
                conditions.iter().enumerate().all(|(i, c)| match c {
                    Condition::Range { column_id, lo, hi } => {
                        matches!(row.columns.get(column_id), Some(Value::Int64(v)) if *v >= *lo && *v <= *hi)
                    }
                    Condition::RangeF64 { column_id, lo, lo_inclusive, hi, hi_inclusive } => {
                        match row.columns.get(column_id) {
                            Some(Value::Float64(v)) => {
                                let lo_ok = if *lo_inclusive { *v >= *lo } else { *v > *lo };
                                let hi_ok = if *hi_inclusive { *v <= *hi } else { *v < *hi };
                                lo_ok && hi_ok
                            }
                            _ => false,
                        }
                    }
                    _ => per_cond_sets[i].contains(row.row_id.0),
                })
            })
            .collect())
    }

    /// Materialize overlay rows into typed `NativeColumn`s for the cursor's
    /// final batch.
    fn materialize_overlay(
        &self,
        rows: &[Row],
        projection: &[(u16, TypeId)],
    ) -> Vec<columnar::NativeColumn> {
        if projection.is_empty() {
            return vec![columnar::null_native(TypeId::Int64, rows.len())];
        }
        let mut cols = Vec::with_capacity(projection.len());
        for (cid, ty) in projection {
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(cid).cloned().unwrap_or(Value::Null))
                .collect();
            cols.push(columnar::values_to_native(ty.clone(), &vals));
        }
        cols
    }

    /// Resolve a conjunctive predicate to its surviving `RowId` set on the
    /// single-run fast path: each condition becomes a `RowId` set via the
    /// in-memory indexes or the reader's page-pruned range scan, then they are
    /// intersected. Mirrors the resolution inside [`Self::query_columns_native`].
    fn resolve_survivor_rids(
        &self,
        conditions: &[crate::query::Condition],
        reader: &mut RunReader,
        snapshot: Snapshot,
    ) -> Result<RowIdSet> {
        use crate::query::Condition;
        let mut sets: Vec<RowIdSet> = Vec::new();
        for c in conditions {
            self.validate_condition(c)?;
            let s: RowIdSet = match c {
                Condition::Pk(key) => {
                    let lookup = self
                        .schema
                        .primary_key()
                        .map(|pk| self.index_lookup_key_bytes(pk.id, key))
                        .unwrap_or_else(|| key.clone());
                    self.hot
                        .get(&lookup)
                        .map(|r| RowIdSet::one(r.0))
                        .unwrap_or_else(RowIdSet::empty)
                }
                Condition::BitmapEq { column_id, value } => {
                    let lookup = self.index_lookup_key_bytes(*column_id, value);
                    self.bitmap
                        .get(column_id)
                        .map(|b| RowIdSet::from_roaring(b.get(&lookup)))
                        .unwrap_or_else(RowIdSet::empty)
                }
                Condition::BitmapIn { column_id, values } => {
                    let bm = self.bitmap.get(column_id);
                    let mut acc = roaring::RoaringBitmap::new();
                    if let Some(b) = bm {
                        for v in values {
                            let lookup = self.index_lookup_key_bytes(*column_id, v);
                            acc |= b.get(&lookup);
                        }
                    }
                    RowIdSet::from_roaring(acc)
                }
                Condition::BytesPrefix { column_id, prefix } => {
                    if let Some(b) = self.bitmap.get(column_id) {
                        let lookup_prefix = self.index_lookup_key_bytes(*column_id, prefix);
                        let mut acc = roaring::RoaringBitmap::new();
                        for key in b.keys() {
                            if key.starts_with(&lookup_prefix) {
                                acc |= b.get(&key);
                            }
                        }
                        RowIdSet::from_roaring(acc)
                    } else {
                        RowIdSet::empty()
                    }
                }
                Condition::FmContains { column_id, pattern } => self
                    .fm
                    .get(column_id)
                    .map(|f| {
                        RowIdSet::from_unsorted(
                            f.locate(pattern).into_iter().map(|r| r.0).collect(),
                        )
                    })
                    .unwrap_or_else(RowIdSet::empty),
                Condition::FmContainsAll {
                    column_id,
                    patterns,
                } => {
                    if let Some(f) = self.fm.get(column_id) {
                        let sets: Vec<RowIdSet> = patterns
                            .iter()
                            .map(|pat| {
                                RowIdSet::from_unsorted(
                                    f.locate(pat).into_iter().map(|r| r.0).collect(),
                                )
                            })
                            .collect();
                        RowIdSet::intersect_many(sets)
                    } else {
                        RowIdSet::empty()
                    }
                }
                Condition::Ann {
                    column_id,
                    query,
                    k,
                } => RowIdSet::from_unsorted(
                    self.retrieve_filtered(
                        &crate::query::Retriever::Ann {
                            column_id: *column_id,
                            query: query.clone(),
                            k: *k,
                        },
                        snapshot,
                        None,
                        None,
                        None,
                        None,
                    )?
                    .into_iter()
                    .map(|hit| hit.row_id.0)
                    .collect(),
                ),
                Condition::SparseMatch {
                    column_id,
                    query,
                    k,
                } => RowIdSet::from_unsorted(
                    self.retrieve_filtered(
                        &crate::query::Retriever::Sparse {
                            column_id: *column_id,
                            query: query.clone(),
                            k: *k,
                        },
                        snapshot,
                        None,
                        None,
                        None,
                        None,
                    )?
                    .into_iter()
                    .map(|hit| hit.row_id.0)
                    .collect(),
                ),
                Condition::MinHashSimilar {
                    column_id,
                    query,
                    k,
                } => match self.minhash.get(column_id) {
                    Some(index) => {
                        let candidates = index.candidate_row_ids(query);
                        let eligible =
                            self.eligible_candidate_ids(&candidates, *column_id, snapshot, None)?;
                        RowIdSet::from_unsorted(
                            index
                                .search_filtered(query, *k, |row_id| eligible.contains(&row_id))
                                .into_iter()
                                .map(|(row_id, _)| row_id.0)
                                .collect(),
                        )
                    }
                    None => RowIdSet::empty(),
                },
                Condition::Range { column_id, lo, hi } => {
                    if let Some(li) = self.learned_range.get(column_id) {
                        RowIdSet::from_unsorted(li.range(*lo, *hi).into_iter().collect())
                    } else {
                        reader.range_row_id_set_i64(*column_id, *lo, *hi)?
                    }
                }
                Condition::RangeF64 {
                    column_id,
                    lo,
                    lo_inclusive,
                    hi,
                    hi_inclusive,
                } => {
                    if let Some(li) = self.learned_range.get(column_id) {
                        RowIdSet::from_unsorted(
                            li.range_f64(*lo, *lo_inclusive, *hi, *hi_inclusive)
                                .into_iter()
                                .collect(),
                        )
                    } else {
                        reader.range_row_id_set_f64(
                            *column_id,
                            *lo,
                            *lo_inclusive,
                            *hi,
                            *hi_inclusive,
                        )?
                    }
                }
                Condition::IsNull { column_id } => reader.null_row_id_set(*column_id, true)?,
                Condition::IsNotNull { column_id } => reader.null_row_id_set(*column_id, false)?,
            };
            sets.push(s);
        }
        Ok(RowIdSet::intersect_many(sets))
    }

    /// Native vectorized aggregate over a (possibly filtered) column on the
    /// single-run fast path (Phase 7.2). Resolves survivors via the same
    /// page-pruned cursor as the scan, then accumulates the aggregate in one
    /// pass over the typed buffer — no `Value`, no Arrow `RecordBatch`.
    ///
    /// `column` is `None` for `COUNT(*)`. Returns `Ok(None)` when the fast path
    /// does not apply (multi-run / non-empty memtable); the caller scans.
    /// Open the streaming [`Cursor`](crate::cursor::Cursor) matching the current
    /// run layout: the single-run page cursor when there is exactly one sorted
    /// run, otherwise the multi-run k-way merge cursor. Both fuse the predicate,
    /// skip non-surviving pages, and fold the memtable / mutable-run overlay, so
    /// callers stay columnar end-to-end and never materialize `Row`s. Returns
    /// `None` when no cursor applies (e.g. an overlay-only table with no sorted
    /// run), leaving the caller to fall back.
    ///
    /// This is the single source of truth for layout-aware cursor selection,
    /// shared by the column scan ([`Self::query_columns_native`] / the SQL
    /// provider) and the aggregate path ([`Self::aggregate_native`]). New
    /// streaming consumers should build on this rather than re-deciding the
    /// cursor by run count.
    pub fn scan_cursor(
        &self,
        snapshot: Snapshot,
        projection: Vec<(u16, TypeId)>,
        conditions: &[crate::query::Condition],
    ) -> Result<Option<Box<dyn crate::cursor::Cursor>>> {
        if self.ttl.is_some() {
            return Ok(None);
        }
        // A deferred bulk load leaves the live indexes unbuilt; resolving
        // conditions against them would return silently-empty survivor sets.
        // Signal "can't serve" so the caller falls back to a `&mut` path that
        // runs `ensure_indexes_complete`. (Condition-free scans don't touch
        // the indexes and stay served.)
        if !conditions.is_empty() && !self.indexes_complete {
            return Ok(None);
        }
        if self.run_refs.len() == 1 {
            Ok(self
                .native_page_cursor(snapshot, projection, conditions)?
                .map(|c| Box::new(c) as Box<dyn crate::cursor::Cursor>))
        } else {
            Ok(self
                .native_multi_run_cursor(snapshot, projection, conditions)?
                .map(|c| Box::new(c) as Box<dyn crate::cursor::Cursor>))
        }
    }

    /// Native vectorized aggregate over a (possibly filtered) column, in one
    /// pass over the typed buffers — no `Value`, no Arrow batch. Layout-agnostic:
    /// survivors stream through [`Self::scan_cursor`] (single- or multi-run,
    /// overlay-folded), so the same path serves every sorted-run layout.
    ///
    /// `column` is `None` for `COUNT(*)`. Order of attempts:
    /// 1. Single clean run + no `WHERE` ⇒ `MIN`/`MAX`/`COUNT(col)` straight from
    ///    page `min`/`max`/`null_count` (no decode).
    /// 2. `COUNT(*)` ⇒ survivor cardinality from the cursor's page plans.
    /// 3. Otherwise accumulate the projected column over the cursor.
    ///
    /// Returns `Ok(None)` (caller scans) when no native path applies: an
    /// overlay-only table with no sorted run, or a non-numeric column.
    pub fn aggregate_native(
        &self,
        snapshot: Snapshot,
        column: Option<u16>,
        conditions: &[crate::query::Condition],
        agg: NativeAgg,
    ) -> Result<Option<NativeAggResult>> {
        self.aggregate_native_inner(snapshot, column, conditions, agg, None)
    }

    pub fn aggregate_native_with_control(
        &self,
        snapshot: Snapshot,
        column: Option<u16>,
        conditions: &[crate::query::Condition],
        agg: NativeAgg,
        control: &crate::ExecutionControl,
    ) -> Result<Option<NativeAggResult>> {
        self.aggregate_native_inner(snapshot, column, conditions, agg, Some(control))
    }

    fn aggregate_native_inner(
        &self,
        snapshot: Snapshot,
        column: Option<u16>,
        conditions: &[crate::query::Condition],
        agg: NativeAgg,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<Option<NativeAggResult>> {
        execution_checkpoint(control, 0)?;
        if self.ttl.is_some() {
            return Ok(None);
        }
        // 1. Single clean run + no WHERE ⇒ MIN/MAX/COUNT(col) from page stats.
        if self.run_refs.len() == 1 && conditions.is_empty() {
            if let Some(res) = self.aggregate_from_stats(snapshot, column, agg)? {
                return Ok(Some(res));
            }
        }
        // 2. COUNT(*) ⇒ survivor count from the cursor's page plans, no decode.
        if matches!(agg, NativeAgg::Count) && column.is_none() {
            return Ok(self
                .scan_cursor(snapshot, Vec::new(), conditions)?
                .map(|c| NativeAggResult::Count(c.remaining_rows() as u64)));
        }
        // 3. Accumulate the projected column. COUNT(col) excludes nulls — the
        //    accumulator's count is the non-null count, which `pack_*` returns.
        let cid = match column {
            Some(c) => c,
            None => return Ok(None),
        };
        let ty = self.column_type(cid);
        let Some(mut cursor) = self.scan_cursor(snapshot, vec![(cid, ty.clone())], conditions)?
        else {
            return Ok(None);
        };
        execution_checkpoint(control, 0)?;
        match ty {
            TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                let (count, sum, mn, mx) = accumulate_int(cursor.as_mut(), control)?;
                Ok(Some(pack_int(agg, count, sum, mn, mx)))
            }
            TypeId::Float64 => {
                let (count, sum, mn, mx) = accumulate_float(cursor.as_mut(), control)?;
                Ok(Some(pack_float(agg, count, sum, mn, mx)))
            }
            _ => Ok(None),
        }
    }

    /// Phase 7.1 metadata fast path: answer an unfiltered `MIN`/`MAX`/`COUNT(col)`
    /// straight from page `min`/`max`/`null_count` — no column decode. Returns
    /// `None` (caller decodes) for `COUNT(*)`/`SUM`/`AVG`, when exact stats are
    /// unavailable (multi-version run; [`Table::exact_column_stats`] gates this),
    /// or for a column whose stats omit `min`/`max` while it still holds values
    /// (e.g. an encrypted column) — returning `NULL` there would be a wrong
    /// answer, so we fall back to decoding.
    fn aggregate_from_stats(
        &self,
        snapshot: Snapshot,
        column: Option<u16>,
        agg: NativeAgg,
    ) -> Result<Option<NativeAggResult>> {
        let cid = match (agg, column) {
            (NativeAgg::Count | NativeAgg::Min | NativeAgg::Max, Some(c)) => c,
            _ => return Ok(None), // COUNT(*), SUM, AVG: not served from page stats
        };
        let Some(stats) = self.exact_column_stats(snapshot, &[cid])? else {
            return Ok(None);
        };
        let Some(cs) = stats.get(&cid) else {
            return Ok(None);
        };
        match agg {
            // COUNT(col) excludes NULLs: live rows minus the column's null count.
            NativeAgg::Count => Ok(Some(NativeAggResult::Count(
                self.live_count.saturating_sub(cs.null_count),
            ))),
            NativeAgg::Min | NativeAgg::Max => {
                let bound = if agg == NativeAgg::Min {
                    &cs.min
                } else {
                    &cs.max
                };
                match bound {
                    Some(Value::Int64(x)) => Ok(Some(NativeAggResult::Int(*x))),
                    Some(Value::Float64(x)) => Ok(Some(NativeAggResult::Float(*x))),
                    Some(_) => Ok(None), // unexpected stat type ⇒ decode
                    // No bound: a genuine SQL NULL only when the column is wholly
                    // null. Otherwise the stats are simply unavailable (encrypted),
                    // so decode for a correct answer.
                    None if cs.null_count >= self.live_count => Ok(Some(NativeAggResult::Null)),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// Phase 7.1c: exact `COUNT(DISTINCT col)` from the bitmap index's partition
    /// cardinality — the number of distinct indexed values — with no scan. Each
    /// distinct value is one bitmap key; under the insert-only invariant (empty
    /// overlay, single run, `live_count == row_count`) every key has at least one
    /// live row, so the key count is exact. `NULL` is excluded from
    /// `COUNT(DISTINCT)`, so a null key (from an explicit `Value::Null` put) is
    /// discounted. Returns `None` (caller scans) without a bitmap index on the
    /// column or when the invariant does not hold.
    pub fn count_distinct_from_bitmap(&mut self, column_id: u16) -> Result<Option<u64>> {
        if self.ttl.is_some() {
            return Ok(None);
        }
        if !(self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1) {
            return Ok(None);
        }
        // A deferred bulk load leaves the bitmap unbuilt; complete it before
        // trusting its key count (same lazy contract as `query`/`flush`).
        self.ensure_indexes_complete()?;
        let reader = self.open_reader(self.run_refs[0].run_id)?;
        if self.live_count != reader.row_count() as u64 {
            return Ok(None);
        }
        let Some(bm) = self.bitmap.get(&column_id) else {
            return Ok(None); // no bitmap index ⇒ let the caller scan
        };
        let mut distinct = bm.value_count() as u64;
        // A null key (explicit `Value::Null`) is indexed but excluded from
        // COUNT(DISTINCT). (Schema-evolution-absent columns are never indexed.)
        if !bm.get(&Value::Null.encode_key()).is_empty() {
            distinct = distinct.saturating_sub(1);
        }
        Ok(Some(distinct))
    }

    /// Incremental aggregate over the live table (Phase 8.3). For an append-only
    /// table, a warm cache entry (same `cache_key`) lets the result be refreshed
    /// by aggregating **only the newly inserted rows** (row-id watermark delta)
    /// and merging, instead of a full recompute. The caller supplies a stable
    /// `cache_key` (e.g. a hash of the SQL + projection); distinct queries must
    /// use distinct keys.
    ///
    /// Returns [`IncrementalAggResult`] with the merged state and whether the
    /// delta path was taken. A single `delete` (ever) disables the incremental
    /// path for the table, so correctness never relies on append-only behavior
    /// that deletes invalidate.
    pub fn aggregate_incremental(
        &mut self,
        cache_key: u64,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: NativeAgg,
    ) -> Result<IncrementalAggResult> {
        self.aggregate_incremental_inner(cache_key, conditions, column, agg, None)
    }

    pub fn aggregate_incremental_with_control(
        &mut self,
        cache_key: u64,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: NativeAgg,
        control: &crate::ExecutionControl,
    ) -> Result<IncrementalAggResult> {
        self.aggregate_incremental_inner(cache_key, conditions, column, agg, Some(control))
    }

    fn aggregate_incremental_inner(
        &mut self,
        cache_key: u64,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: NativeAgg,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<IncrementalAggResult> {
        execution_checkpoint(control, 0)?;
        let snap = self.snapshot();
        let cur_wm = self.allocator.current().0;
        let cur_epoch = snap.epoch.0;
        // The watermark equals the committed row count only when the memtable is
        // empty (every allocated row id is durably in a run). With pending
        // (uncommitted) writes the allocator is ahead of the visible set, so the
        // delta range would silently skip just-committed rows — disable the
        // incremental path entirely in that case. The mutable-run tier holding
        // un-spilled data also disables it (those rows aren't in a run yet).
        let incremental_ok = self.ttl.is_none()
            && !self.had_deletes
            && self.memtable.is_empty()
            && self.mutable_run.is_empty();

        // Incremental path: append-only, no pending writes, warm cache, advanced
        // epoch.
        if incremental_ok {
            if let Some(cached) = self.agg_cache.get(&cache_key).cloned() {
                if cached.epoch == cur_epoch {
                    return Ok(IncrementalAggResult {
                        state: cached.state,
                        incremental: true,
                        delta_rows: 0,
                    });
                }
                if cached.epoch < cur_epoch && cached.watermark <= cur_wm {
                    let delta_len = cur_wm.saturating_sub(cached.watermark) as usize;
                    let mut delta_rids = Vec::with_capacity(delta_len);
                    for (index, row_id) in (cached.watermark..cur_wm).enumerate() {
                        execution_checkpoint(control, index)?;
                        delta_rids.push(row_id);
                    }
                    let delta_rows = self.rows_for_rids(&delta_rids, snap)?;
                    execution_checkpoint(control, 0)?;
                    let index_sets = self.resolve_index_conditions(conditions, snap)?;
                    let delta_state = agg_state_from_rows(
                        &delta_rows,
                        conditions,
                        &index_sets,
                        column,
                        agg,
                        &self.schema,
                        control,
                    )?;
                    let merged = cached.state.merge(delta_state);
                    let delta_n = delta_rids.len() as u64;
                    Arc::make_mut(&mut self.agg_cache).insert(
                        cache_key,
                        CachedAgg {
                            state: merged.clone(),
                            watermark: cur_wm,
                            epoch: cur_epoch,
                        },
                    );
                    return Ok(IncrementalAggResult {
                        state: merged,
                        incremental: true,
                        delta_rows: delta_n,
                    });
                }
            }
        }

        // Cold path. For Count/Sum/Min/Max the fast vectorized cursor produces a
        // directly-seedable state; for Avg it returns only the mean (losing the
        // sum+count needed to merge a future delta), so Avg falls back to a
        // visible-rows scan that captures both.
        let cursor_ok =
            self.memtable.is_empty() && self.mutable_run.is_empty() && self.run_refs.len() == 1;
        let state = if cursor_ok && agg != NativeAgg::Avg {
            match self.aggregate_native_inner(snap, column, conditions, agg, control)? {
                Some(result) => {
                    AggState::from_native(result, agg, column.map(|c| self.column_type(c)))
                }
                None => self.agg_state_full_scan(conditions, column, agg, snap, control)?,
            }
        } else {
            self.agg_state_full_scan(conditions, column, agg, snap, control)?
        };
        // Seed only when the watermark is meaningful (no pending writes).
        if incremental_ok {
            Arc::make_mut(&mut self.agg_cache).insert(
                cache_key,
                CachedAgg {
                    state: state.clone(),
                    watermark: cur_wm,
                    epoch: cur_epoch,
                },
            );
        }
        Ok(IncrementalAggResult {
            state,
            incremental: false,
            delta_rows: 0,
        })
    }

    /// Full visible-rows scan → [`AggState`] (cold path; captures sum+count for
    /// correct Avg seeding).
    fn agg_state_full_scan(
        &self,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: NativeAgg,
        snap: Snapshot,
        control: Option<&crate::ExecutionControl>,
    ) -> Result<AggState> {
        execution_checkpoint(control, 0)?;
        let rows = self.visible_rows(snap)?;
        execution_checkpoint(control, 0)?;
        let index_sets = self.resolve_index_conditions(conditions, snap)?;
        agg_state_from_rows(
            &rows,
            conditions,
            &index_sets,
            column,
            agg,
            &self.schema,
            control,
        )
    }

    /// Resolve only the index-defined conditions (`Ann`/`SparseMatch`) to row-id
    /// sets for membership testing during row-wise aggregation.
    fn resolve_index_conditions(
        &self,
        conditions: &[crate::query::Condition],
        snapshot: Snapshot,
    ) -> Result<Vec<RowIdSet>> {
        use crate::query::Condition;
        let mut sets = Vec::new();
        for c in conditions {
            if matches!(
                c,
                Condition::Ann { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::MinHashSimilar { .. }
            ) {
                sets.push(self.resolve_condition(c, snapshot)?);
            }
        }
        Ok(sets)
    }

    fn column_type(&self, cid: u16) -> TypeId {
        self.schema
            .columns
            .iter()
            .find(|c| c.id == cid)
            .map(|c| c.ty.clone())
            .unwrap_or(TypeId::Bytes)
    }

    /// Approximate `COUNT`/`SUM`/`AVG` over a filtered set, computed from the
    /// in-memory reservoir sample (Phase 8.2). Returns a point estimate plus a
    /// normal-theory confidence interval at the supplied z-score (1.96 ≈ 95 %).
    ///
    /// The WHERE predicates are evaluated **exactly** on each sampled row (so
    /// LIKE/FM and equality/range contribute no index bias); `Ann`/`SparseMatch`
    /// are index-defined and resolved once to a row-id set that sampled rows are
    /// tested against. `Ok(None)` when there is no usable sample.
    pub fn approx_aggregate(
        &mut self,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: ApproxAgg,
        z: f64,
    ) -> Result<Option<ApproxResult>> {
        self.approx_aggregate_with_candidate_authorization(conditions, column, agg, z, None)
    }

    /// Security-aware approximate aggregate. RLS is evaluated only for the
    /// reservoir candidates, and column masks are applied before aggregation.
    pub fn approx_aggregate_with_candidate_authorization(
        &mut self,
        conditions: &[crate::query::Condition],
        column: Option<u16>,
        agg: ApproxAgg,
        z: f64,
        authorization: Option<&crate::security::CandidateAuthorization<'_>>,
    ) -> Result<Option<ApproxResult>> {
        use crate::query::Condition;
        self.ensure_reservoir_complete()?;
        let snapshot = self.snapshot();
        let n_pop = self.count();
        let sample_rids: Vec<u64> = self.reservoir.row_ids().to_vec();
        if sample_rids.is_empty() {
            return Ok(None);
        }
        // Materialize the live, non-deleted sampled rows.
        let live_sample = self.rows_for_rids(&sample_rids, snapshot)?;
        let s = live_sample.len();
        if s == 0 {
            return Ok(None);
        }
        let authorized = authorization
            .map(|authorization| {
                let candidates = live_sample.iter().map(|row| row.row_id).collect::<Vec<_>>();
                self.policy_allowed_candidate_ids(&candidates, snapshot, authorization, None)
            })
            .transpose()?;

        // Pre-resolve Ann/Sparse conditions (index-defined predicates) to row-id
        // sets; the per-row predicates below are evaluated exactly.
        let mut index_sets: Vec<RowIdSet> = Vec::new();
        for c in conditions {
            if matches!(
                c,
                Condition::Ann { .. }
                    | Condition::SparseMatch { .. }
                    | Condition::MinHashSimilar { .. }
            ) {
                index_sets.push(self.resolve_condition(c, snapshot)?);
            }
        }

        // For Sum/Avg, gather the numeric column value of each passing row.
        let cid = match (agg, column) {
            (ApproxAgg::Count, _) => None,
            (_, Some(c)) => Some(c),
            _ => return Ok(None),
        };
        let mut passing_vals: Vec<f64> = Vec::with_capacity(s);
        for r in &live_sample {
            if authorized
                .as_ref()
                .is_some_and(|authorized| !authorized.contains(&r.row_id))
            {
                continue;
            }
            // Exact per-row predicate evaluation.
            if !conditions
                .iter()
                .all(|c| condition_matches_row(c, r, &self.schema))
            {
                continue;
            }
            // Ann/Sparse membership.
            if !index_sets.iter().all(|set| set.contains(r.row_id.0)) {
                continue;
            }
            if let Some(cid) = cid {
                let mut cells = r
                    .columns
                    .get(&cid)
                    .cloned()
                    .map(|value| vec![(cid, value)])
                    .unwrap_or_default();
                if let Some(authorization) = authorization {
                    authorization.security.apply_masks_to_cells(
                        authorization.table,
                        &mut cells,
                        authorization.principal,
                    );
                }
                if let Some(v) = as_f64(cells.first().map(|(_, value)| value)) {
                    passing_vals.push(v);
                } // nulls ⇒ excluded (matching SQL AVG/SUM null semantics)
            } else {
                passing_vals.push(0.0); // placeholder for COUNT
            }
        }
        let m = passing_vals.len();

        let (point, half) = match agg {
            ApproxAgg::Count => {
                // Proportion estimate scaled to the population.
                let p = m as f64 / s as f64;
                let point = n_pop as f64 * p;
                let var = if s > 1 {
                    n_pop as f64 * n_pop as f64 * p * (1.0 - p) / s as f64
                        * (1.0 - s as f64 / n_pop as f64).max(0.0)
                } else {
                    0.0
                };
                (point, z * var.sqrt())
            }
            ApproxAgg::Sum => {
                // Horvitz–Thompson: each sampled row represents n_pop/s rows.
                let y: Vec<f64> = live_sample
                    .iter()
                    .map(|r| {
                        let passes_row = authorized
                            .as_ref()
                            .is_none_or(|authorized| authorized.contains(&r.row_id))
                            && conditions
                                .iter()
                                .all(|c| condition_matches_row(c, r, &self.schema))
                            && index_sets.iter().all(|set| set.contains(r.row_id.0));
                        if passes_row {
                            cid.and_then(|cid| {
                                let mut cells = r
                                    .columns
                                    .get(&cid)
                                    .cloned()
                                    .map(|value| vec![(cid, value)])
                                    .unwrap_or_default();
                                if let Some(authorization) = authorization {
                                    authorization.security.apply_masks_to_cells(
                                        authorization.table,
                                        &mut cells,
                                        authorization.principal,
                                    );
                                }
                                as_f64(cells.first().map(|(_, value)| value))
                            })
                            .unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    })
                    .collect();
                let mean_y = y.iter().sum::<f64>() / s as f64;
                let point = n_pop as f64 * mean_y;
                let var = if s > 1 {
                    let ss: f64 = y.iter().map(|v| (v - mean_y).powi(2)).sum();
                    let var_y = ss / (s - 1) as f64;
                    n_pop as f64 * n_pop as f64 * var_y / s as f64
                        * (1.0 - s as f64 / n_pop as f64).max(0.0)
                } else {
                    0.0
                };
                (point, z * var.sqrt())
            }
            ApproxAgg::Avg => {
                if m == 0 {
                    return Ok(Some(ApproxResult {
                        point: 0.0,
                        ci_low: 0.0,
                        ci_high: 0.0,
                        n_population: n_pop,
                        n_sample_live: s,
                        n_passing: 0,
                    }));
                }
                let mean = passing_vals.iter().sum::<f64>() / m as f64;
                let half = if m > 1 {
                    let ss: f64 = passing_vals.iter().map(|v| (v - mean).powi(2)).sum();
                    let sd = (ss / (m - 1) as f64).sqrt();
                    let fpc = (1.0 - s as f64 / n_pop as f64).max(0.0);
                    z * sd / (m as f64).sqrt() * fpc.sqrt()
                } else {
                    0.0
                };
                (mean, half)
            }
        };

        Ok(Some(ApproxResult {
            point,
            ci_low: point - half,
            ci_high: point + half,
            n_population: n_pop,
            n_sample_live: s,
            n_passing: m,
        }))
    }

    /// Exact per-column statistics for the analytical aggregate fast path
    /// (Phase 7.1: `MIN`/`MAX`/`COUNT(col)` from page stats). Returns `None`
    /// unless the table is effectively insert-only at `snapshot` — empty
    /// memtable, a single sorted run, and `live_count == run.row_count()` — so
    /// the run's page `min`/`max`/`null_count` are exact (no tombstoned or
    /// superseded versions skew them). Under deletes/updates the caller falls
    /// back to scanning.
    pub fn exact_column_stats(
        &self,
        _snapshot: Snapshot,
        projection: &[u16],
    ) -> Result<Option<HashMap<u16, ColumnStat>>> {
        if self.ttl.is_some()
            || !(self.memtable.is_empty()
                && self.mutable_run.is_empty()
                && self.run_refs.len() == 1)
        {
            return Ok(None);
        }
        let reader = self.open_reader(self.run_refs[0].run_id)?;
        if self.live_count != reader.row_count() as u64 {
            return Ok(None);
        }
        let mut out = HashMap::new();
        for &cid in projection {
            let cdef = match self.schema.columns.iter().find(|c| c.id == cid) {
                Some(c) => c,
                None => continue,
            };
            // Absent column (schema evolution) ⇒ all rows null.
            let Some(stats) = reader.column_page_stats(cid) else {
                out.insert(
                    cid,
                    ColumnStat {
                        min: None,
                        max: None,
                        null_count: self.live_count,
                    },
                );
                continue;
            };
            let stat = match cdef.ty {
                TypeId::Int64 | TypeId::TimestampNanos | TypeId::Date32 => {
                    agg_int(stats, crate::sorted_run::be_i64).map(|(mn, mx, n)| ColumnStat {
                        min: mn.map(Value::Int64),
                        max: mx.map(Value::Int64),
                        null_count: n,
                    })
                }
                TypeId::Float64 => {
                    agg_float(stats, crate::sorted_run::be_f64).map(|(mn, mx, n)| ColumnStat {
                        min: mn.map(Value::Float64),
                        max: mx.map(Value::Float64),
                        null_count: n,
                    })
                }
                _ => None,
            };
            if let Some(s) = stat {
                out.insert(cid, s);
            }
        }
        Ok(Some(out))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub(crate) fn set_catalog_name(&mut self, name: String) {
        self.name = name;
    }

    pub(crate) fn prepare_alter_column(
        &mut self,
        column_name: &str,
        change: &AlterColumn,
    ) -> Result<(ColumnDef, Option<Schema>)> {
        if !self.pending_rows.is_empty() || !self.pending_dels.is_empty() {
            return Err(MongrelError::InvalidArgument(
                "ALTER COLUMN requires committing staged writes first".into(),
            ));
        }
        let old = self
            .schema
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .cloned()
            .ok_or_else(|| MongrelError::Schema(format!("unknown column {column_name}")))?;
        let mut next = old.clone();

        if let Some(name) = &change.name {
            let trimmed = name.trim();
            if trimmed.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "ALTER COLUMN name must not be empty".into(),
                ));
            }
            if trimmed != old.name && self.schema.columns.iter().any(|c| c.name == trimmed) {
                return Err(MongrelError::Schema(format!(
                    "column {trimmed} already exists"
                )));
            }
            next.name = trimmed.to_string();
        }

        if let Some(ty) = &change.ty {
            next.ty = ty.clone();
        }
        if let Some(flags) = change.flags {
            validate_alter_column_flags(old.flags, flags)?;
            next.flags = flags;
        }

        if let Some(default_change) = &change.default_value {
            next.default_value = default_change.clone();
        }

        validate_alter_column_type(&self.schema, &old, &next, self.has_stored_versions())?;
        if old.flags.contains(ColumnFlags::NULLABLE)
            && !next.flags.contains(ColumnFlags::NULLABLE)
            && self.column_has_nulls(old.id)?
        {
            return Err(MongrelError::InvalidArgument(format!(
                "column '{}' contains NULL values",
                old.name
            )));
        }
        if next == old {
            return Ok((next, None));
        }
        let mut schema = self.schema.clone();
        let index = schema
            .columns
            .iter()
            .position(|column| column.id == next.id)
            .ok_or_else(|| MongrelError::Schema(format!("unknown column {}", next.id)))?;
        schema.columns[index] = next.clone();
        schema.schema_id = schema
            .schema_id
            .checked_add(1)
            .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
        schema.validate_auto_increment()?;
        schema.validate_defaults()?;
        Ok((next, Some(schema)))
    }

    pub(crate) fn apply_altered_schema_prepared(&mut self, schema: Schema) {
        self.schema = schema;
        self.auto_inc = resolve_auto_inc(&self.schema);
        self.column_keys = build_column_keys(self.kek.as_deref(), &self.schema);
        self.clear_result_cache();
        let _ = std::fs::remove_dir_all(self.dir.join("_shadow"));
    }

    pub(crate) fn checkpoint_altered_schema(&mut self) -> Result<()> {
        checkpoint_current_schema(self)
    }

    pub fn alter_column(&mut self, column_name: &str, change: AlterColumn) -> Result<ColumnDef> {
        self.ensure_writable()?;
        let previous_schema = self.schema.clone();
        let (column, schema) = self.prepare_alter_column(column_name, &change)?;
        if let Some(schema) = schema {
            self.apply_altered_schema_prepared(schema);
            self.checkpoint_standalone_schema_change(previous_schema)?;
        }
        Ok(column)
    }

    fn column_has_nulls(&mut self, column_id: u16) -> Result<bool> {
        if self.live_count == 0 {
            return Ok(false);
        }
        let snap = self.snapshot();
        let columns = self.visible_columns_native(snap, Some(&[column_id]))?;
        Ok(columns
            .first()
            .map(|(_, col)| col.null_count(col.len()) != 0)
            .unwrap_or(true))
    }

    fn has_stored_versions(&self) -> bool {
        !self.memtable.is_empty()
            || !self.mutable_run.is_empty()
            || self.run_refs.iter().any(|r| r.row_count > 0)
            || !self.retiring.is_empty()
    }

    /// Add a column to the schema (schema evolution). Existing runs simply read
    /// back as null for the new column until re-written. Persists the new schema
    /// and manifest. The caller supplies the full [`ColumnFlags`] so migrations
    /// can add `PRIMARY KEY` / `AUTO_INCREMENT` columns correctly.
    pub fn add_column(
        &mut self,
        name: &str,
        ty: TypeId,
        flags: ColumnFlags,
        default_value: Option<crate::schema::DefaultExpr>,
    ) -> Result<u16> {
        self.add_column_with_id(name, ty, flags, default_value, None)
    }

    pub fn add_column_with_id(
        &mut self,
        name: &str,
        ty: TypeId,
        flags: ColumnFlags,
        default_value: Option<crate::schema::DefaultExpr>,
        requested_id: Option<u16>,
    ) -> Result<u16> {
        self.ensure_writable()?;
        if self.schema.columns.iter().any(|c| c.name == name) {
            return Err(MongrelError::Schema(format!(
                "column {name} already exists"
            )));
        }
        let id = if let Some(id) = requested_id.filter(|id| *id != 0) {
            if self.schema.columns.iter().any(|c| c.id == id) {
                return Err(MongrelError::Schema(format!(
                    "column id {id} already exists"
                )));
            }
            id
        } else {
            self.schema
                .columns
                .iter()
                .map(|c| c.id)
                .max()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| MongrelError::Schema("column id space exhausted".into()))?
        };
        let previous_schema = self.schema.clone();
        let mut next_schema = previous_schema.clone();
        next_schema.columns.push(ColumnDef {
            id,
            name: name.to_string(),
            ty,
            flags,
            default_value,
        });
        next_schema.schema_id = next_schema
            .schema_id
            .checked_add(1)
            .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
        next_schema.validate_auto_increment()?;
        next_schema.validate_defaults()?;
        self.apply_altered_schema_prepared(next_schema);
        self.checkpoint_standalone_schema_change(previous_schema)?;
        Ok(id)
    }

    /// Declare a `LearnedRange` (PGM) index on an existing numeric column and
    /// build it immediately from the current sorted run (Phase 13.3). After
    /// this, `Condition::Range` / `Condition::RangeF64` on that column resolve
    /// survivors sub-linearly (O(log segments + log ε)) instead of scanning the
    /// full column.
    ///
    /// Requires exactly one sorted run (call after `flush`). The index is
    /// rebuilt automatically on subsequent flushes.
    pub fn add_learned_range_index(&mut self, column_name: &str) -> Result<()> {
        self.ensure_writable()?;
        let cid = self
            .schema
            .columns
            .iter()
            .find(|c| c.name == column_name)
            .map(|c| c.id)
            .ok_or_else(|| MongrelError::Schema(format!("unknown column {column_name}")))?;
        let ty = self
            .schema
            .columns
            .iter()
            .find(|c| c.id == cid)
            .map(|c| c.ty.clone())
            .unwrap_or(TypeId::Int64);
        if !matches!(
            ty,
            TypeId::Int64 | TypeId::Float64 | TypeId::TimestampNanos | TypeId::Date32
        ) {
            return Err(MongrelError::Schema(format!(
                "LearnedRange requires a numeric column; {column_name} is {ty:?}"
            )));
        }
        if self
            .schema
            .indexes
            .iter()
            .any(|i| i.column_id == cid && i.kind == IndexKind::LearnedRange)
        {
            return Ok(()); // already declared
        }
        let previous_schema = self.schema.clone();
        let previous_learned_range = Arc::clone(&self.learned_range);
        let mut next_schema = previous_schema.clone();
        next_schema.indexes.push(IndexDef {
            name: format!("{}_learned_range", column_name),
            column_id: cid,
            kind: IndexKind::LearnedRange,
            predicate: None,
            options: Default::default(),
        });
        next_schema.schema_id = next_schema
            .schema_id
            .checked_add(1)
            .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
        self.apply_altered_schema_prepared(next_schema);
        if let Err(error) = self.build_learned_ranges() {
            self.apply_altered_schema_prepared(previous_schema);
            self.learned_range = previous_learned_range;
            return Err(error);
        }
        if let Err(error) = self.checkpoint_standalone_schema_change(previous_schema) {
            if !matches!(
                &error,
                MongrelError::DurableCommit { .. } | MongrelError::CommitOutcomeUnknown { .. }
            ) {
                self.learned_range = previous_learned_range;
            }
            return Err(error);
        }
        Ok(())
    }

    fn checkpoint_standalone_schema_change(&mut self, previous_schema: Schema) -> Result<()> {
        let mut schema_published = false;
        let schema_result = match self._root_guard.as_deref() {
            Some(root) => write_schema_durable_with_after(root, &self.schema, || {
                schema_published = true;
            }),
            None => write_schema_with_after(&self.dir, &self.schema, || {
                schema_published = true;
            }),
        };
        if schema_result.is_err() && !schema_published {
            self.apply_altered_schema_prepared(previous_schema);
            return schema_result;
        }

        let manifest_result = self.persist_manifest(self.current_epoch());
        match (schema_result, manifest_result) {
            (_, Ok(())) => Ok(()),
            (Ok(()), Err(error)) => {
                self.poison_after_maintenance_publish_failure();
                Err(MongrelError::DurableCommit {
                    epoch: self.current_epoch().0,
                    message: format!(
                        "schema is durable but matching manifest publication failed: {error}"
                    ),
                })
            }
            (Err(schema_error), Err(manifest_error)) => {
                self.poison_after_maintenance_publish_failure();
                Err(MongrelError::CommitOutcomeUnknown {
                    epoch: self.current_epoch().0,
                    message: format!(
                        "schema publication sync failed ({schema_error}); matching manifest publication also failed ({manifest_error})"
                    ),
                })
            }
        }
    }

    /// Tuning knob for the WAL auto-sync threshold. A no-op on a mounted table
    /// (the shared WAL's durability is governed by the group-commit coordinator).
    pub fn set_sync_byte_threshold(&mut self, threshold: u64) {
        self.sync_byte_threshold = threshold;
        if let WalSink::Private(w) = &mut self.wal {
            w.set_sync_byte_threshold(threshold);
        }
    }

    /// Flush all live page-cache entries to the persistent `_cache/` backing
    /// directory (best-effort). Useful before a clean shutdown so hot pages
    /// survive restart.
    pub fn page_cache_flush(&self) {
        self.page_cache.flush_to_disk();
    }

    /// Number of entries currently in the shared page cache (diagnostic).
    pub fn page_cache_len(&self) -> usize {
        self.page_cache.len()
    }

    /// Number of entries currently in the shared decoded-page cache (Phase
    /// 15.4 diagnostic).
    pub fn decoded_cache_len(&self) -> usize {
        self.decoded_cache.len()
    }

    /// Drain the live memtable (prototype/testing helper used by the flush path
    /// demos). Prefer [`Table::flush`] for the durable path.
    pub fn drain_memtable_sorted(&mut self) -> Vec<Row> {
        self.memtable.drain_sorted()
    }

    pub(crate) fn run_path(&self, run_id: u64) -> PathBuf {
        self.runs_dir().join(format!("r-{run_id}.sr"))
    }

    pub(crate) fn create_run_file(&self, run_id: u64) -> Result<Option<std::fs::File>> {
        match self.runs_root.as_deref() {
            Some(root) => Ok(Some(root.create_regular_new(format!("r-{run_id}.sr"))?)),
            None => Ok(None),
        }
    }

    pub(crate) fn create_run_entry(&self, name: &Path) -> Result<Option<std::fs::File>> {
        match self.runs_root.as_deref() {
            Some(root) => Ok(Some(root.create_regular_new(name)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn remove_run_entry(&self, name: &Path) -> Result<()> {
        match self.runs_root.as_deref() {
            Some(root) => match root.remove_file(name) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            },
            None => match std::fs::remove_file(self.runs_dir().join(name)) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            },
        }
    }

    pub(crate) fn publish_run_entry(&self, source: &Path, destination: &Path) -> Result<()> {
        match self.runs_root.as_deref() {
            Some(root) => root
                .rename_file_new(source, destination)
                .map_err(Into::into),
            None => crate::durable_file::rename(
                &self.runs_dir().join(source),
                &self.runs_dir().join(destination),
            )
            .map_err(Into::into),
        }
    }

    pub(crate) fn active_run_ids(&self) -> impl Iterator<Item = u128> + '_ {
        self.run_refs.iter().map(|run| run.run_id)
    }

    pub(crate) fn table_dir(&self) -> &Path {
        &self.dir
    }

    pub(crate) fn schema_ref(&self) -> &crate::schema::Schema {
        &self.schema
    }

    pub(crate) fn alloc_run_id(&mut self) -> Result<u64> {
        let id = self.next_run_id;
        self.next_run_id = self
            .next_run_id
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("run-id namespace exhausted".into()))?;
        Ok(id)
    }

    pub(crate) fn link_run(&mut self, run_ref: crate::manifest::RunRef) {
        self.run_refs.push(run_ref);
    }

    /// Link a spilled run found during shared-WAL recovery (spec §8.5).
    /// **Idempotent**: if the run is already in the manifest (the publish phase
    /// persisted it before the crash, or this is a clean reopen with the
    /// `TxnCommit` still in the WAL) this is a no-op returning `false`, so the
    /// caller never double-links or double-counts. Otherwise — a crash *after*
    /// the commit fsync but *before* publish persisted the manifest — the run is
    /// Enqueue a compaction-superseded run for retention-gated deletion (spec
    /// §6.4). The file stays on disk until [`Self::reap_retiring`] removes it
    /// once `min_active_snapshot` has advanced past `retire_epoch`.
    pub(crate) fn retire_run(&mut self, run_id: u128, retire_epoch: u64) {
        self.retiring.push(crate::manifest::RetiredRun {
            run_id,
            retire_epoch,
        });
    }

    /// Physically delete retired run files whose `retire_epoch` no pinned reader
    /// can still need (`min_active >= retire_epoch`), drop them from the queue,
    /// and persist the manifest if anything changed. Returns the count reaped.
    pub(crate) fn reap_retiring(
        &mut self,
        min_active: Epoch,
        backup_pinned: &std::collections::HashSet<u128>,
    ) -> Result<usize> {
        if self.retiring.is_empty() {
            return Ok(0);
        }
        let mut reaped = 0;
        let mut kept: Vec<crate::manifest::RetiredRun> = Vec::new();
        // Delete-then-persist is crash-idempotent: if we crash after unlinking
        // some files but before persisting, the manifest still lists them in
        // `retiring`; the next `reap_retiring` re-issues `remove_file` (the
        // error is ignored) and `check()` excludes `retiring` ids from orphan
        // detection, so the lingering entries are harmless until then.
        for r in std::mem::take(&mut self.retiring) {
            if min_active.0 >= r.retire_epoch && !backup_pinned.contains(&r.run_id) {
                let _ = self.remove_run_entry(Path::new(&format!("r-{}.sr", r.run_id)));
                reaped += 1;
            } else {
                kept.push(r);
            }
        }
        self.retiring = kept;
        if reaped > 0 {
            self.persist_manifest(self.current_epoch())?;
        }
        Ok(reaped)
    }

    pub(crate) fn has_reapable_retiring(
        &self,
        min_active: Epoch,
        backup_pinned: &std::collections::HashSet<u128>,
    ) -> bool {
        self.retiring
            .iter()
            .any(|run| min_active.0 >= run.retire_epoch && !backup_pinned.contains(&run.run_id))
    }

    pub(crate) fn recover_spilled_run(&mut self, run_ref: crate::manifest::RunRef) -> bool {
        if self.run_refs.iter().any(|r| r.run_id == run_ref.run_id) {
            return false;
        }
        self.live_count = self.live_count.saturating_add(run_ref.row_count);
        self.run_refs.push(run_ref);
        self.indexes_complete = false;
        true
    }

    pub(crate) fn kek_ref(&self) -> Option<&Arc<Kek>> {
        self.kek.as_ref()
    }

    pub(crate) fn open_reader(&self, run_id: u128) -> Result<RunReader> {
        let mut reader = match self.runs_root.as_deref() {
            Some(root) => RunReader::open_file_with_cache(
                root.open_regular(format!("r-{run_id}.sr"))?,
                self.schema.clone(),
                self.kek.clone(),
                Some(self.page_cache.clone()),
                Some(self.decoded_cache.clone()),
                self.table_id,
                Some(&self.verified_runs),
                None,
            )?,
            None => RunReader::open_with_cache(
                self.dir.join(RUNS_DIR).join(format!("r-{run_id}.sr")),
                self.schema.clone(),
                self.kek.clone(),
                Some(self.page_cache.clone()),
                Some(self.decoded_cache.clone()),
                self.table_id,
                Some(&self.verified_runs),
            )?,
        };
        // Overlay the real commit epoch for uniform-epoch (large-txn spill) runs:
        // their stored `_epoch` is a placeholder; the manifest RunRef carries the
        // assigned epoch. A no-op for ordinary runs.
        if let Some(rr) = self.run_refs.iter().find(|r| r.run_id == run_id) {
            reader.set_uniform_epoch(Epoch(rr.epoch_created));
        }
        Ok(reader)
    }

    pub(crate) fn run_refs(&self) -> &[RunRef] {
        &self.run_refs
    }

    pub(crate) fn retiring_run_ids(&self) -> impl Iterator<Item = u128> + '_ {
        self.retiring.iter().map(|run| run.run_id)
    }

    pub(crate) fn runs_dir(&self) -> PathBuf {
        self.runs_root
            .as_deref()
            .and_then(|root| root.io_path().ok())
            .unwrap_or_else(|| self.dir.join(RUNS_DIR))
    }

    pub(crate) fn wal_dir(&self) -> PathBuf {
        self.dir.join(WAL_DIR)
    }

    pub(crate) fn set_run_refs(&mut self, refs: Vec<RunRef>) {
        self.run_refs = refs;
    }

    pub(crate) fn compaction_zstd_level(&self) -> i32 {
        self.compaction_zstd_level
    }

    pub(crate) fn kek(&self) -> Option<Arc<Kek>> {
        self.kek.clone()
    }

    /// The index-checkpoint DEK (KEK-derived) for encrypted tables; `None` for
    /// plaintext tables. The checkpoint embeds index keys / PGM segment values
    /// derived from user data, so an encrypted table must encrypt it at rest.
    #[cfg(feature = "encryption")]
    fn idx_dek(&self) -> Option<Zeroizing<[u8; DEK_LEN]>> {
        self.kek.as_ref().map(|k| k.derive_idx_key())
    }

    #[cfg(not(feature = "encryption"))]
    fn idx_dek(&self) -> Option<Zeroizing<[u8; DEK_LEN]>> {
        None
    }

    /// Manifest (and other DB-wide metadata) meta DEK, derived from the KEK so
    /// the on-disk manifest is encrypted + authenticated at rest for encrypted
    /// tables. `None` for plaintext.
    #[cfg(feature = "encryption")]
    fn manifest_meta_dek(&self) -> Option<[u8; DEK_LEN]> {
        self.kek.as_ref().map(|k| *k.derive_meta_key())
    }

    #[cfg(not(feature = "encryption"))]
    fn manifest_meta_dek(&self) -> Option<[u8; DEK_LEN]> {
        None
    }

    /// `(column_id, scheme)` for every ENCRYPTED_INDEXABLE column — passed to
    /// the run writer so each run's descriptor records the column keys.
    pub(crate) fn indexable_column_specs(&self) -> Vec<(u16, u8)> {
        self.column_keys
            .iter()
            .map(|(&id, &(_, scheme))| (id, scheme))
            .collect()
    }

    /// Tokenize a value for an ENCRYPTED_INDEXABLE column (HMAC-eq or OPE-range,
    /// per the column's scheme). Returns `None` for plaintext columns. Indexes
    /// over such columns store tokens, and queries tokenize literals the same
    /// way — so lookups never decrypt the stored (encrypted) page payloads.
    #[cfg(feature = "encryption")]
    fn tokenize_value(&self, column_id: u16, v: &Value) -> Option<Value> {
        self.tokenize_value_enc(column_id, v)
    }

    #[cfg(feature = "encryption")]
    fn tokenize_value_enc(&self, column_id: u16, v: &Value) -> Option<Value> {
        use crate::encryption::{hmac_token, ope_token_f64, ope_token_i64, SCHEME_HMAC_EQ};
        let (key, scheme) = self.column_keys.get(&column_id)?;
        let token: Vec<u8> = match (*scheme, v) {
            (SCHEME_HMAC_EQ, _) => hmac_token(key, &v.encode_key()).to_vec(),
            (_, Value::Int64(x)) => ope_token_i64(key, *x).to_vec(),
            (_, Value::Float64(x)) => ope_token_f64(key, *x).to_vec(),
            _ => hmac_token(key, &v.encode_key()).to_vec(),
        };
        Some(Value::Bytes(token))
    }

    /// Encoded index key for a `Value`, tokenized for HMAC-eq columns.
    fn index_lookup_key(&self, column_id: u16, v: &Value) -> Vec<u8> {
        self.index_lookup_key_bytes(column_id, &v.encode_key())
    }

    /// Tokenize an already-encoded lookup key (equality queries pass the
    /// encoded search value; HMAC-eq columns wrap it under the column key).
    fn index_lookup_key_bytes(&self, column_id: u16, encoded: &[u8]) -> Vec<u8> {
        #[cfg(feature = "encryption")]
        {
            use crate::encryption::{hmac_token, SCHEME_HMAC_EQ};
            if let Some((key, scheme)) = self.column_keys.get(&column_id) {
                if *scheme == SCHEME_HMAC_EQ {
                    return hmac_token(key, encoded).to_vec();
                }
            }
        }
        let _ = column_id;
        encoded.to_vec()
    }
}

fn native_int64_strictly_increasing(col: &columnar::NativeColumn, n: usize) -> bool {
    let columnar::NativeColumn::Int64 { data, validity } = col else {
        return false;
    };
    if data.len() < n || !columnar::all_non_null(validity, n) {
        return false;
    }
    data.iter()
        .take(n)
        .zip(data.iter().skip(1))
        .all(|(a, b)| a < b)
}

/// Exact aggregate of a column's page stats into a min/max/null_count triple
/// (Phase 7.1). Only meaningful when the owning table is insert-only, which
/// [`Table::exact_column_stats`] gates on.
#[derive(Debug, Clone)]
pub struct ColumnStat {
    pub min: Option<Value>,
    pub max: Option<Value>,
    pub null_count: u64,
}

/// A supported native aggregate (Phase 7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeAgg {
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

/// The typed result of a [`NativeAgg`] over a column.
#[derive(Debug, Clone, PartialEq)]
pub enum NativeAggResult {
    Count(u64),
    Int(i64),
    Float(f64),
    /// No non-null inputs (SUM/MIN/MAX/AVG over zero rows ⇒ SQL NULL).
    Null,
}

/// A supported approximate aggregate over the reservoir sample (Phase 8.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApproxAgg {
    Count,
    Sum,
    Avg,
}

/// Point estimate with a normal-theory confidence interval from the reservoir
/// sample (Phase 8.2). `ci_low`/`ci_high` bracket `point` at the requested
/// z-score; the interval has zero width when the sample equals the whole table.
#[derive(Debug, Clone)]
pub struct ApproxResult {
    /// Point estimate of the aggregate.
    pub point: f64,
    /// Lower bound (`point − z·SE`).
    pub ci_low: f64,
    /// Upper bound (`point + z·SE`).
    pub ci_high: f64,
    /// Live population size (the table's `count()`).
    pub n_population: u64,
    /// Live rows in the sample (`≤` reservoir capacity).
    pub n_sample_live: usize,
    /// Sampled rows passing the WHERE predicate.
    pub n_passing: usize,
}

/// A mergeable running aggregate state (Phase 8.3). Two states over disjoint
/// row sets `merge` into the state over their union, so a cached analytical
/// aggregate can be updated by merging in only the delta (newly inserted rows)
/// instead of a full recompute.
#[derive(Debug, Clone, PartialEq)]
pub enum AggState {
    /// `COUNT(*)` or `COUNT(col)` over `n` matching rows.
    Count(u64),
    /// Int64 `SUM`: running `i128` sum + non-null count.
    SumI {
        sum: i128,
        count: u64,
    },
    /// Float64 `SUM`: running `f64` sum + non-null count.
    SumF {
        sum: f64,
        count: u64,
    },
    /// Int64 `AVG`: running `i128` sum + non-null count (avg = sum/count).
    AvgI {
        sum: i128,
        count: u64,
    },
    /// Float64 `AVG`: running `f64` sum + non-null count.
    AvgF {
        sum: f64,
        count: u64,
    },
    /// Int64 `MIN`/`MAX`.
    MinI(i64),
    MaxI(i64),
    /// Float64 `MIN`/`MAX`.
    MinF(f64),
    MaxF(f64),
    /// No matching rows observed yet.
    Empty,
}

impl AggState {
    /// Combine two states over disjoint row sets into the state over the union.
    pub fn merge(self, other: AggState) -> AggState {
        use AggState::*;
        match (self, other) {
            (Empty, x) | (x, Empty) => x,
            (Count(a), Count(b)) => Count(a + b),
            (SumI { sum: sa, count: ca }, SumI { sum: sb, count: cb }) => SumI {
                sum: sa + sb,
                count: ca + cb,
            },
            (SumF { sum: sa, count: ca }, SumF { sum: sb, count: cb }) => SumF {
                sum: sa + sb,
                count: ca + cb,
            },
            (AvgI { sum: sa, count: ca }, AvgI { sum: sb, count: cb }) => AvgI {
                sum: sa + sb,
                count: ca + cb,
            },
            (AvgF { sum: sa, count: ca }, AvgF { sum: sb, count: cb }) => AvgF {
                sum: sa + sb,
                count: ca + cb,
            },
            (MinI(a), MinI(b)) => MinI(a.min(b)),
            (MaxI(a), MaxI(b)) => MaxI(a.max(b)),
            (MinF(a), MinF(b)) => MinF(a.min(b)),
            (MaxF(a), MaxF(b)) => MaxF(a.max(b)),
            _ => Empty, // mismatched kinds — shouldn't happen (same query)
        }
    }

    /// The scalar point value (`f64`), or `None` when there were no inputs.
    pub fn point(&self) -> Option<f64> {
        match self {
            AggState::Count(n) => Some(*n as f64),
            AggState::SumI { sum, .. } => Some(*sum as f64),
            AggState::SumF { sum, .. } => Some(*sum),
            AggState::AvgI { sum, count } if *count > 0 => Some(*sum as f64 / *count as f64),
            AggState::AvgF { sum, count } if *count > 0 => Some(*sum / *count as f64),
            AggState::MinI(n) => Some(*n as f64),
            AggState::MaxI(n) => Some(*n as f64),
            AggState::MinF(n) => Some(*n),
            AggState::MaxF(n) => Some(*n),
            AggState::AvgI { .. } | AggState::AvgF { .. } | AggState::Empty => None,
        }
    }

    /// Convert a vectorized [`NativeAggResult`] (from the cursor path) into a
    /// mergeable [`AggState`], so the incremental cache can be seeded from the
    /// fast cold path. `ty` is the column's type (`None` for COUNT(*)).
    pub fn from_native(result: NativeAggResult, agg: NativeAgg, ty: Option<TypeId>) -> Self {
        let is_float = matches!(ty, Some(TypeId::Float64));
        match (agg, result) {
            (NativeAgg::Count, NativeAggResult::Count(n)) => AggState::Count(n),
            (NativeAgg::Sum, NativeAggResult::Int(x)) => AggState::SumI {
                sum: x as i128,
                count: 1, // count unknown from NativeAggResult; use sentinel
            },
            (NativeAgg::Sum, NativeAggResult::Float(x)) => AggState::SumF { sum: x, count: 1 },
            (NativeAgg::Avg, NativeAggResult::Float(x)) => AggState::AvgF { sum: x, count: 1 },
            (NativeAgg::Min, NativeAggResult::Int(x)) => AggState::MinI(x),
            (NativeAgg::Max, NativeAggResult::Int(x)) => AggState::MaxI(x),
            (NativeAgg::Min, NativeAggResult::Float(x)) => AggState::MinF(x),
            (NativeAgg::Max, NativeAggResult::Float(x)) => AggState::MaxF(x),
            (NativeAgg::Count, _) => AggState::Empty,
            (_, NativeAggResult::Null) => AggState::Empty,
            _ => {
                let _ = is_float;
                AggState::Empty
            }
        }
    }
}

/// A cached incremental aggregate (Phase 8.3): the mergeable state, the row-id
/// watermark it covers (rows `[0, watermark)`), and the snapshot epoch.
#[derive(Debug, Clone)]
pub struct CachedAgg {
    pub state: AggState,
    pub watermark: u64,
    pub epoch: u64,
}

/// Outcome of [`Table::aggregate_incremental`].
#[derive(Debug, Clone)]
pub struct IncrementalAggResult {
    /// The aggregate state covering all rows at the current epoch.
    pub state: AggState,
    /// `true` when produced by merging only the delta (new rows); `false` when
    /// a full recompute was required (cold cache, deletes, or same epoch).
    pub incremental: bool,
    /// Rows processed in the delta pass (`0` for a full recompute).
    pub delta_rows: u64,
}

/// Compute a mergeable [`AggState`] over `rows` that pass every per-row
/// `conditions` conjunct (and whose row id is in every pre-resolved
/// `index_sets`). Shared by the cold (full) and warm (delta) incremental paths.
fn agg_state_from_rows(
    rows: &[Row],
    conditions: &[crate::query::Condition],
    index_sets: &[RowIdSet],
    column: Option<u16>,
    agg: NativeAgg,
    schema: &Schema,
    control: Option<&crate::ExecutionControl>,
) -> Result<AggState> {
    let mut count: u64 = 0;
    let mut sum_i: i128 = 0;
    let mut sum_f: f64 = 0.0;
    let mut mn_i: i64 = i64::MAX;
    let mut mx_i: i64 = i64::MIN;
    let mut mn_f: f64 = f64::INFINITY;
    let mut mx_f: f64 = f64::NEG_INFINITY;
    let mut saw_int = false;
    let mut saw_float = false;
    for (index, r) in rows.iter().enumerate() {
        execution_checkpoint(control, index)?;
        if !conditions
            .iter()
            .all(|c| condition_matches_row(c, r, schema))
        {
            continue;
        }
        if !index_sets.iter().all(|s| s.contains(r.row_id.0)) {
            continue;
        }
        match agg {
            NativeAgg::Count => match column {
                // COUNT(*) counts every passing row.
                None => count += 1,
                // COUNT(col) excludes NULLs — explicit `Value::Null` and a column
                // absent from the row (schema evolution) are both NULL.
                Some(cid) => match r.columns.get(&cid) {
                    None | Some(Value::Null) => {}
                    Some(_) => count += 1,
                },
            },
            _ => match column.and_then(|cid| r.columns.get(&cid)) {
                Some(Value::Int64(n)) => {
                    count += 1;
                    sum_i += *n as i128;
                    mn_i = mn_i.min(*n);
                    mx_i = mx_i.max(*n);
                    saw_int = true;
                }
                Some(Value::Float64(f)) => {
                    count += 1;
                    sum_f += f;
                    mn_f = mn_f.min(*f);
                    mx_f = mx_f.max(*f);
                    saw_float = true;
                }
                _ => {}
            },
        }
    }
    Ok(match agg {
        NativeAgg::Count => {
            if count == 0 {
                AggState::Empty
            } else {
                AggState::Count(count)
            }
        }
        NativeAgg::Sum => {
            if count == 0 {
                AggState::Empty
            } else if saw_int {
                AggState::SumI { sum: sum_i, count }
            } else {
                AggState::SumF { sum: sum_f, count }
            }
        }
        NativeAgg::Avg => {
            if count == 0 {
                AggState::Empty
            } else if saw_int {
                AggState::AvgI { sum: sum_i, count }
            } else {
                AggState::AvgF { sum: sum_f, count }
            }
        }
        NativeAgg::Min => {
            if !saw_int && !saw_float {
                AggState::Empty
            } else if saw_int {
                AggState::MinI(mn_i)
            } else {
                AggState::MinF(mn_f)
            }
        }
        NativeAgg::Max => {
            if !saw_int && !saw_float {
                AggState::Empty
            } else if saw_int {
                AggState::MaxI(mx_i)
            } else {
                AggState::MaxF(mx_f)
            }
        }
    })
}

/// Evaluate an index-served [`Condition`] exactly against a materialized row.
/// `Ann`/`SparseMatch` (index-defined) always pass here; callers test those via a
/// pre-resolved row-id set.
fn condition_matches_row(c: &crate::query::Condition, row: &Row, schema: &Schema) -> bool {
    use crate::query::Condition;
    match c {
        Condition::Pk(key) => match schema.primary_key() {
            Some(pk) => row
                .columns
                .get(&pk.id)
                .map(|v| v.encode_key() == *key)
                .unwrap_or(false),
            None => false,
        },
        Condition::BitmapEq { column_id, value } => row
            .columns
            .get(column_id)
            .map(|v| v.encode_key() == *value)
            .unwrap_or(false),
        Condition::BitmapIn { column_id, values } => {
            let key = row.columns.get(column_id).map(|v| v.encode_key());
            match key {
                Some(k) => values.contains(&k),
                None => false,
            }
        }
        Condition::BytesPrefix { column_id, prefix } => row
            .columns
            .get(column_id)
            .map(|v| v.encode_key().starts_with(prefix))
            .unwrap_or(false),
        Condition::Range { column_id, lo, hi } => match row.columns.get(column_id) {
            Some(Value::Int64(n)) => *n >= *lo && *n <= *hi,
            _ => false,
        },
        Condition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => match row.columns.get(column_id) {
            Some(Value::Float64(n)) => {
                let lo_ok = if *lo_inclusive { *n >= *lo } else { *n > *lo };
                let hi_ok = if *hi_inclusive { *n <= *hi } else { *n < *hi };
                lo_ok && hi_ok
            }
            _ => false,
        },
        Condition::FmContains { column_id, pattern } => match row.columns.get(column_id) {
            Some(Value::Bytes(b)) => {
                !pattern.is_empty() && b.windows(pattern.len()).any(|w| w == &pattern[..])
            }
            _ => false,
        },
        Condition::FmContainsAll {
            column_id,
            patterns,
        } => match row.columns.get(column_id) {
            Some(Value::Bytes(b)) => patterns
                .iter()
                .all(|pat| !pat.is_empty() && b.windows(pat.len()).any(|w| w == &pat[..])),
            _ => false,
        },
        Condition::Ann { .. }
        | Condition::SparseMatch { .. }
        | Condition::MinHashSimilar { .. } => true,
        Condition::IsNull { column_id } => {
            matches!(row.columns.get(column_id), Some(Value::Null) | None)
        }
        Condition::IsNotNull { column_id } => {
            !matches!(row.columns.get(column_id), Some(Value::Null) | None)
        }
    }
}

/// Coerce a cell to `f64` for Sum/Avg (Int64/Float64 only).
fn as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Int64(n)) => Some(*n as f64),
        Some(Value::Float64(f)) => Some(*f),
        _ => None,
    }
}

/// One-pass vectorized accumulation of `(non-null count, sum, min, max)` over an
/// Int64 column streamed through `cursor`. The inner loop over a contiguous
/// `&[i64]` autovectorizes (SIMD) for the all-non-null prefix.
fn accumulate_int(
    cursor: &mut dyn crate::cursor::Cursor,
    control: Option<&crate::ExecutionControl>,
) -> Result<(u64, i128, i64, i64)> {
    let mut count: u64 = 0;
    let mut sum: i128 = 0;
    let mut mn: i64 = i64::MAX;
    let mut mx: i64 = i64::MIN;
    while let Some(cols) = cursor.next_batch()? {
        execution_checkpoint(control, 0)?;
        if let Some(crate::columnar::NativeColumn::Int64 { data, validity }) = cols.first() {
            if crate::columnar::all_non_null(validity, data.len()) {
                // All-non-null: vectorized sum/min/max with no per-element branch.
                count += data.len() as u64;
                for (chunk_index, chunk) in data.chunks(1024).enumerate() {
                    execution_checkpoint(control, chunk_index * 1024)?;
                    sum += chunk.iter().map(|&v| v as i128).sum::<i128>();
                    mn = mn.min(*chunk.iter().min().unwrap_or(&mn));
                    mx = mx.max(*chunk.iter().max().unwrap_or(&mx));
                }
            } else {
                for (i, &v) in data.iter().enumerate() {
                    execution_checkpoint(control, i)?;
                    if crate::columnar::validity_bit(validity, i) {
                        count += 1;
                        sum += v as i128;
                        mn = mn.min(v);
                        mx = mx.max(v);
                    }
                }
            }
        }
    }
    Ok((count, sum, mn, mx))
}

/// f64 analogue of [`accumulate_int`].
fn accumulate_float(
    cursor: &mut dyn crate::cursor::Cursor,
    control: Option<&crate::ExecutionControl>,
) -> Result<(u64, f64, f64, f64)> {
    let mut count: u64 = 0;
    let mut sum: f64 = 0.0;
    let mut mn: f64 = f64::INFINITY;
    let mut mx: f64 = f64::NEG_INFINITY;
    while let Some(cols) = cursor.next_batch()? {
        execution_checkpoint(control, 0)?;
        if let Some(crate::columnar::NativeColumn::Float64 { data, validity }) = cols.first() {
            if crate::columnar::all_non_null(validity, data.len()) {
                count += data.len() as u64;
                for (chunk_index, chunk) in data.chunks(1024).enumerate() {
                    execution_checkpoint(control, chunk_index * 1024)?;
                    sum += chunk.iter().sum::<f64>();
                    mn = mn.min(chunk.iter().copied().fold(f64::INFINITY, f64::min));
                    mx = mx.max(chunk.iter().copied().fold(f64::NEG_INFINITY, f64::max));
                }
            } else {
                for (i, &v) in data.iter().enumerate() {
                    execution_checkpoint(control, i)?;
                    if crate::columnar::validity_bit(validity, i) {
                        count += 1;
                        sum += v;
                        mn = mn.min(v);
                        mx = mx.max(v);
                    }
                }
            }
        }
    }
    Ok((count, sum, mn, mx))
}

#[inline]
fn execution_checkpoint(control: Option<&crate::ExecutionControl>, index: usize) -> Result<()> {
    if index.is_multiple_of(256) {
        control
            .map(crate::ExecutionControl::checkpoint)
            .transpose()?;
    }
    Ok(())
}

fn pack_int(agg: NativeAgg, count: u64, sum: i128, mn: i64, mx: i64) -> NativeAggResult {
    if count == 0 && !matches!(agg, NativeAgg::Count) {
        return NativeAggResult::Null;
    }
    match agg {
        NativeAgg::Count => NativeAggResult::Count(count),
        // i64 overflow on Sum ⇒ SQL NULL (DataFusion errors on overflow; null is
        // a safe, non-misleading fallback rather than a saturated wrong value).
        NativeAgg::Sum => match sum.try_into() {
            Ok(v) => NativeAggResult::Int(v),
            Err(_) => NativeAggResult::Null,
        },
        NativeAgg::Min => NativeAggResult::Int(mn),
        NativeAgg::Max => NativeAggResult::Int(mx),
        NativeAgg::Avg => NativeAggResult::Float((sum as f64) / (count as f64)),
    }
}

fn pack_float(agg: NativeAgg, count: u64, sum: f64, mn: f64, mx: f64) -> NativeAggResult {
    if count == 0 && !matches!(agg, NativeAgg::Count) {
        return NativeAggResult::Null;
    }
    match agg {
        NativeAgg::Count => NativeAggResult::Count(count),
        NativeAgg::Sum => NativeAggResult::Float(sum),
        NativeAgg::Min => NativeAggResult::Float(mn),
        NativeAgg::Max => NativeAggResult::Float(mx),
        NativeAgg::Avg => NativeAggResult::Float(sum / (count as f64)),
    }
}

/// Aggregate per-page `min`/`max`/`null_count` into a column-wide i64 triple.
/// Returns `None` if no page contributes a non-null min/max (all-null column).
fn agg_int(
    stats: &[crate::page::PageStat],
    decode: fn(Option<&[u8]>) -> Option<i64>,
) -> Option<(Option<i64>, Option<i64>, u64)> {
    let (mut mn, mut mx, mut nulls) = (i64::MAX, i64::MIN, 0u64);
    let mut any = false;
    for s in stats {
        if let Some(v) = decode(s.min.as_deref()) {
            mn = mn.min(v);
            any = true;
        }
        if let Some(v) = decode(s.max.as_deref()) {
            mx = mx.max(v);
            any = true;
        }
        nulls += s.null_count;
    }
    any.then_some((Some(mn), Some(mx), nulls))
}

/// f64 analogue of [`agg_int`] (compares as f64, not as bit patterns).
fn agg_float(
    stats: &[crate::page::PageStat],
    decode: fn(Option<&[u8]>) -> Option<f64>,
) -> Option<(Option<f64>, Option<f64>, u64)> {
    let (mut mn, mut mx, mut nulls) = (f64::INFINITY, f64::NEG_INFINITY, 0u64);
    let mut any = false;
    for s in stats {
        if let Some(v) = decode(s.min.as_deref()) {
            mn = mn.min(v);
            any = true;
        }
        if let Some(v) = decode(s.max.as_deref()) {
            mx = mx.max(v);
            any = true;
        }
        nulls += s.null_count;
    }
    any.then_some((Some(mn), Some(mx), nulls))
}

/// The four maintained secondary-index maps, keyed by column id.
type SecondaryIndexes = (
    HashMap<u16, BitmapIndex>,
    HashMap<u16, AnnIndex>,
    HashMap<u16, FmIndex>,
    HashMap<u16, SparseIndex>,
    HashMap<u16, MinHashIndex>,
);

fn empty_indexes(schema: &Schema) -> SecondaryIndexes {
    let mut bitmap = HashMap::new();
    let mut ann = HashMap::new();
    let mut fm = HashMap::new();
    let mut sparse = HashMap::new();
    let mut minhash = HashMap::new();
    for idef in &schema.indexes {
        match idef.kind {
            IndexKind::Bitmap => {
                bitmap.insert(idef.column_id, BitmapIndex::new());
            }
            IndexKind::Ann => {
                let dim = schema
                    .columns
                    .iter()
                    .find(|c| c.id == idef.column_id)
                    .and_then(|c| match c.ty {
                        TypeId::Embedding { dim } => Some(dim as usize),
                        _ => None,
                    })
                    .unwrap_or(0);
                let options = idef.options.ann.clone().unwrap_or_default();
                ann.insert(
                    idef.column_id,
                    AnnIndex::with_options(
                        dim,
                        options.m,
                        options.ef_construction,
                        options.ef_search,
                    ),
                );
            }
            IndexKind::FmIndex => {
                fm.insert(idef.column_id, FmIndex::new());
            }
            IndexKind::Sparse => {
                sparse.insert(idef.column_id, SparseIndex::new());
            }
            IndexKind::MinHash => {
                let options = idef.options.minhash.clone().unwrap_or_default();
                minhash.insert(
                    idef.column_id,
                    MinHashIndex::with_options(options.permutations, options.bands),
                );
            }
            _ => {}
        }
    }
    (bitmap, ann, fm, sparse, minhash)
}

const ALTER_COLUMN_PROTECTED_FLAGS: u32 = ColumnFlags::PRIMARY_KEY
    | ColumnFlags::AUTO_INCREMENT
    | ColumnFlags::ENCRYPTED
    | ColumnFlags::ENCRYPTED_INDEXABLE
    | ColumnFlags::EMBEDDING_BINARY_QUANTIZED;

fn validate_alter_column_flags(old: ColumnFlags, new: ColumnFlags) -> Result<()> {
    if (old.bits() ^ new.bits()) & ALTER_COLUMN_PROTECTED_FLAGS != 0 {
        return Err(MongrelError::Schema(
            "ALTER COLUMN may only change NULLABLE; primary key, auto-increment, encryption, and embedding flags are immutable".into(),
        ));
    }
    Ok(())
}

fn validate_alter_column_type(
    schema: &Schema,
    old: &ColumnDef,
    next: &ColumnDef,
    has_stored_versions: bool,
) -> Result<()> {
    if old.ty == next.ty {
        return Ok(());
    }
    if schema.indexes.iter().any(|i| i.column_id == old.id) {
        return Err(MongrelError::Schema(format!(
            "ALTER COLUMN TYPE is not supported for indexed column '{}'",
            old.name
        )));
    }
    if !has_stored_versions || storage_compatible_type_change(old.ty.clone(), next.ty.clone()) {
        return Ok(());
    }
    Err(MongrelError::Schema(format!(
        "ALTER COLUMN TYPE from {:?} to {:?} requires an empty column or a representation-compatible type",
        old.ty, next.ty
    )))
}

fn storage_compatible_type_change(old: TypeId, new: TypeId) -> bool {
    matches!(
        (old, new),
        (TypeId::Int64, TypeId::TimestampNanos) | (TypeId::TimestampNanos, TypeId::Int64)
    )
}

/// True when every row carries an `Int64` PK value and the sequence is
/// strictly increasing — no intra-batch duplicate is possible. The row-major
/// mirror of `native_int64_strictly_increasing` (the `bulk_pk_winner_indices`
/// fast path), used by `apply_put_rows_inner` to skip upsert probing for
/// append-style batches.
fn rows_pk_strictly_increasing(rows: &[Row], pk_id: u16) -> bool {
    let mut prev: Option<i64> = None;
    for r in rows {
        match r.columns.get(&pk_id) {
            Some(Value::Int64(v)) => {
                if prev.is_some_and(|p| p >= *v) {
                    return false;
                }
                prev = Some(*v);
            }
            _ => return false,
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn index_into(
    schema: &Schema,
    row: &Row,
    hot: &mut HotIndex,
    bitmap: &mut HashMap<u16, BitmapIndex>,
    ann: &mut HashMap<u16, AnnIndex>,
    fm: &mut HashMap<u16, FmIndex>,
    sparse: &mut HashMap<u16, SparseIndex>,
    minhash: &mut HashMap<u16, MinHashIndex>,
) {
    for idef in &schema.indexes {
        let Some(val) = row.columns.get(&idef.column_id) else {
            continue;
        };
        match idef.kind {
            IndexKind::Bitmap => {
                if let Some(b) = bitmap.get_mut(&idef.column_id) {
                    b.insert(val.encode_key(), row.row_id);
                }
            }
            IndexKind::Ann => {
                if let (Some(a), Value::Embedding(v)) = (ann.get_mut(&idef.column_id), val) {
                    a.insert_validated(v, row.row_id);
                }
            }
            IndexKind::FmIndex => {
                if let (Some(f), Value::Bytes(b)) = (fm.get_mut(&idef.column_id), val) {
                    f.insert(b.clone(), row.row_id);
                }
            }
            IndexKind::Sparse => {
                if let (Some(s), Value::Bytes(b)) = (sparse.get_mut(&idef.column_id), val) {
                    // A sparse vector is stored as a bincode'd `Vec<(u32, f32)>`
                    // in a Bytes column (SPLADE weights in, retrieval out).
                    if let Ok(terms) = bincode::deserialize::<Vec<(u32, f32)>>(b) {
                        s.insert(&terms, row.row_id);
                    }
                }
            }
            IndexKind::MinHash => {
                if let (Some(mh), Value::Bytes(b)) = (minhash.get_mut(&idef.column_id), val) {
                    // The set is a JSON array (the Kit's `set_similarity` shape);
                    // tokenize + hash its members into the MinHash signature.
                    let tokens = crate::index::token_hashes_from_bytes(b);
                    mh.insert(&tokens, row.row_id);
                }
            }
            _ => {}
        }
    }
    if let Some(pk_col) = schema.primary_key() {
        if let Some(pk_val) = row.columns.get(&pk_col.id) {
            hot.insert(pk_val.encode_key(), row.row_id);
        }
    }
}

/// Index a row into a single specific index (used for partial indexes where
/// only matching indexes should receive the row).
#[allow(clippy::too_many_arguments)]
fn index_into_single(
    idef: &IndexDef,
    _schema: &Schema,
    row: &Row,
    _hot: &mut HotIndex,
    bitmap: &mut HashMap<u16, BitmapIndex>,
    ann: &mut HashMap<u16, AnnIndex>,
    fm: &mut HashMap<u16, FmIndex>,
    sparse: &mut HashMap<u16, SparseIndex>,
    minhash: &mut HashMap<u16, MinHashIndex>,
) {
    let Some(val) = row.columns.get(&idef.column_id) else {
        return;
    };
    match idef.kind {
        IndexKind::Bitmap => {
            if let Some(b) = bitmap.get_mut(&idef.column_id) {
                b.insert(val.encode_key(), row.row_id);
            }
        }
        IndexKind::Ann => {
            if let (Some(a), Value::Embedding(v)) = (ann.get_mut(&idef.column_id), val) {
                a.insert_validated(v, row.row_id);
            }
        }
        IndexKind::FmIndex => {
            if let (Some(f), Value::Bytes(b)) = (fm.get_mut(&idef.column_id), val) {
                f.insert(b.clone(), row.row_id);
            }
        }
        IndexKind::Sparse => {
            if let (Some(s), Value::Bytes(b)) = (sparse.get_mut(&idef.column_id), val) {
                if let Ok(terms) = bincode::deserialize::<Vec<(u32, f32)>>(b) {
                    s.insert(&terms, row.row_id);
                }
            }
        }
        IndexKind::MinHash => {
            if let (Some(mh), Value::Bytes(b)) = (minhash.get_mut(&idef.column_id), val) {
                let tokens = crate::index::token_hashes_from_bytes(b);
                mh.insert(&tokens, row.row_id);
            }
        }
        _ => {}
    }
}

/// Evaluate a partial-index predicate against a row. Supports the most common
/// patterns: `"column IS NOT NULL"` and `"column IS NULL"`. More complex
/// expressions require a full SQL evaluator in core (future work); the
/// predicate string is stored verbatim and this function provides a pragmatic
/// subset. Returns `true` if the row should be indexed.
fn eval_partial_predicate(
    pred: &str,
    columns_map: &HashMap<u16, &Value>,
    name_to_id: &HashMap<&str, u16>,
) -> bool {
    let lower = pred.trim().to_ascii_lowercase();
    // Pattern: "column_name IS NOT NULL"
    if let Some(rest) = lower.strip_suffix(" is not null") {
        let col_name = rest.trim();
        if let Some(col_id) = name_to_id.get(col_name) {
            return columns_map
                .get(col_id)
                .is_some_and(|v| !matches!(v, Value::Null));
        }
    }
    // Pattern: "column_name IS NULL"
    if let Some(rest) = lower.strip_suffix(" is null") {
        let col_name = rest.trim();
        if let Some(col_id) = name_to_id.get(col_name) {
            return columns_map
                .get(col_id)
                .is_none_or(|v| matches!(v, Value::Null));
        }
    }
    // Unknown predicate syntax: index the row (conservative — better to
    // over-index than to miss rows).
    true
}

/// Per-element index key for the typed bulk-index path (Phase 14.2): mirrors
/// `index_into` on a `tokenized_for_indexes(row)` — encodes the element the way
/// [`Value::encode_key`] would, then applies the column's
/// `ENCRYPTED_INDEXABLE` tokenization (HMAC-eq / OPE) so bitmap/HOT keys match
/// what the incremental path stores. Returns `None` for null slots.
#[allow(dead_code)]
fn bulk_index_key(
    column_keys: &HashMap<u16, ([u8; 32], u8)>,
    column_id: u16,
    ty: TypeId,
    col: &columnar::NativeColumn,
    i: usize,
) -> Option<Vec<u8>> {
    let encoded = columnar::encode_key_native(ty, col, i)?;
    #[cfg(feature = "encryption")]
    {
        use crate::encryption::{hmac_token, ope_token_f64, ope_token_i64, SCHEME_HMAC_EQ};
        if let Some((key, scheme)) = column_keys.get(&column_id) {
            return Some(match (*scheme, col) {
                (SCHEME_HMAC_EQ, _) => hmac_token(key, &encoded).to_vec(),
                (_, columnar::NativeColumn::Int64 { data, .. }) => {
                    ope_token_i64(key, data[i]).to_vec()
                }
                (_, columnar::NativeColumn::Float64 { data, .. }) => {
                    ope_token_f64(key, data[i]).to_vec()
                }
                _ => hmac_token(key, &encoded).to_vec(),
            });
        }
    }
    #[cfg(not(feature = "encryption"))]
    {
        let _ = (column_id, column_keys, col);
    }
    Some(encoded)
}

pub(crate) fn write_schema(dir: &Path, schema: &Schema) -> Result<()> {
    write_schema_with_after(dir, schema, || {})
}

pub(crate) fn write_schema_durable(
    root: &crate::durable_file::DurableRoot,
    schema: &Schema,
) -> Result<()> {
    write_schema_durable_with_after(root, schema, || {})
}

fn write_schema_with_after<F>(dir: &Path, schema: &Schema, after_publish: F) -> Result<()>
where
    F: FnOnce(),
{
    let json = serde_json::to_string_pretty(schema)
        .map_err(|e| MongrelError::Schema(format!("encode schema: {e}")))?;
    crate::durable_file::write_atomic_with_after(
        &dir.join(SCHEMA_FILENAME),
        json.as_bytes(),
        after_publish,
    )?;
    Ok(())
}

fn write_schema_durable_with_after<F>(
    root: &crate::durable_file::DurableRoot,
    schema: &Schema,
    after_publish: F,
) -> Result<()>
where
    F: FnOnce(),
{
    let json = serde_json::to_string_pretty(schema)
        .map_err(|error| MongrelError::Schema(format!("encode schema: {error}")))?;
    root.write_atomic_with_after(SCHEMA_FILENAME, json.as_bytes(), after_publish)?;
    Ok(())
}

fn checkpoint_current_schema(table: &mut Table) -> Result<()> {
    let mut schema_published = false;
    let schema_result = match table._root_guard.as_deref() {
        Some(root) => write_schema_durable_with_after(root, &table.schema, || {
            schema_published = true;
        }),
        None => write_schema_with_after(&table.dir, &table.schema, || {
            schema_published = true;
        }),
    };
    if schema_result.is_err() && !schema_published {
        return schema_result;
    }
    match table.persist_manifest(table.current_epoch()) {
        Ok(()) => Ok(()),
        Err(manifest_error) => Err(match schema_result {
            Ok(()) => manifest_error,
            Err(schema_error) => MongrelError::Other(format!(
                "schema publication sync failed ({schema_error}); matching manifest publication also failed ({manifest_error})"
            )),
        }),
    }
}

fn read_schema(dir: &Path) -> Result<Schema> {
    let file = crate::durable_file::open_regular_nofollow(&dir.join(SCHEMA_FILENAME))?;
    read_schema_file(file)
}

fn read_schema_file(file: std::fs::File) -> Result<Schema> {
    const MAX_SCHEMA_BYTES: u64 = 16 * 1024 * 1024;
    use std::io::Read;

    let length = file.metadata()?.len();
    if length > MAX_SCHEMA_BYTES {
        return Err(MongrelError::ResourceLimitExceeded {
            resource: "schema bytes",
            requested: usize::try_from(length).unwrap_or(usize::MAX),
            limit: MAX_SCHEMA_BYTES as usize,
        });
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_SCHEMA_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(MongrelError::Schema(
            "schema length changed while reading".into(),
        ));
    }
    serde_json::from_slice(&bytes).map_err(|e| MongrelError::Schema(format!("decode schema: {e}")))
}

fn preflight_standalone_open(
    dir: &Path,
    runs_root: Option<&crate::durable_file::DurableRoot>,
    idx_root: Option<&crate::durable_file::DurableRoot>,
    manifest: &Manifest,
    schema: &Schema,
    records: &[crate::wal::Record],
    kek: Option<Arc<Kek>>,
) -> Result<()> {
    crate::wal::validate_shared_transaction_framing(records)?;
    if manifest.schema_id > schema.schema_id
        || manifest.flushed_epoch > manifest.current_epoch
        || manifest.global_idx_epoch > manifest.current_epoch
        || manifest.next_row_id == u64::MAX
        || manifest.auto_inc_next < 0
        || manifest.auto_inc_next == i64::MAX
        || (schema.auto_increment_column().is_none() && manifest.auto_inc_next != 0)
    {
        return Err(MongrelError::InvalidArgument(
            "manifest counters or schema identity are invalid".into(),
        ));
    }
    let mut run_ids = HashSet::new();
    let mut maximum_row_id = None::<u64>;
    for run in &manifest.runs {
        if run.run_id >= u64::MAX as u128
            || !run_ids.insert(run.run_id)
            || run.epoch_created > manifest.current_epoch
        {
            return Err(MongrelError::InvalidArgument(
                "manifest contains an invalid or duplicate active run".into(),
            ));
        }
        let mut reader = match runs_root {
            Some(root) => RunReader::open_file(
                root.open_regular(format!("r-{}.sr", run.run_id as u64))?,
                schema.clone(),
                kek.clone(),
            )?,
            None => RunReader::open(
                dir.join(RUNS_DIR)
                    .join(format!("r-{}.sr", run.run_id as u64)),
                schema.clone(),
                kek.clone(),
            )?,
        };
        let header = reader.header();
        if header.run_id != run.run_id
            || header.level != run.level
            || header.row_count != run.row_count
            || !header.is_uniform_epoch() && header.epoch_created != run.epoch_created
            || header.is_uniform_epoch() && header.epoch_created != 0
            || header.schema_id > schema.schema_id
        {
            return Err(MongrelError::InvalidArgument(format!(
                "run {} differs from its manifest",
                run.run_id
            )));
        }
        if header.row_count != 0 {
            maximum_row_id = Some(
                maximum_row_id.map_or(header.max_row_id, |value| value.max(header.max_row_id)),
            );
        }
        reader.validate_all_pages()?;
    }
    if maximum_row_id.is_some_and(|maximum| manifest.next_row_id <= maximum) {
        return Err(MongrelError::InvalidArgument(
            "manifest next_row_id does not advance beyond persisted rows".into(),
        ));
    }
    for run in &manifest.retiring {
        if run.run_id >= u64::MAX as u128
            || run.retire_epoch > manifest.current_epoch
            || !run_ids.insert(run.run_id)
        {
            return Err(MongrelError::InvalidArgument(
                "manifest contains an invalid or duplicate retired run".into(),
            ));
        }
    }
    #[cfg(feature = "encryption")]
    let idx_dek = kek.as_ref().map(|key| key.derive_idx_key());
    #[cfg(not(feature = "encryption"))]
    let idx_dek: Option<Zeroizing<[u8; DEK_LEN]>> = None;
    match idx_root {
        Some(root) => {
            global_idx::read_root(root, manifest.table_id, schema, idx_dek.as_deref())?;
        }
        None => {
            global_idx::read(dir, manifest.table_id, schema, idx_dek.as_deref())?;
        }
    }

    let committed = records
        .iter()
        .filter_map(|record| match record.op {
            Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();
    for record in records {
        let Some(&_commit_epoch) = committed.get(&record.txn_id) else {
            continue;
        };
        match &record.op {
            Op::Put { table_id, rows } => {
                if *table_id != manifest.table_id {
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!(
                            "private WAL record references table {table_id}, expected {}",
                            manifest.table_id
                        ),
                    });
                }
                let rows: Vec<Row> =
                    bincode::deserialize(rows).map_err(|error| MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!("committed Put payload could not be decoded: {error}"),
                    })?;
                for row in rows {
                    if row.deleted || row.row_id.0 == u64::MAX {
                        return Err(MongrelError::CorruptWal {
                            offset: record.seq.0,
                            reason: "committed Put contains an invalid row identity".into(),
                        });
                    }
                    let cells = row.columns.into_iter().collect::<Vec<_>>();
                    schema
                        .validate_values(&cells)
                        .map_err(|error| MongrelError::CorruptWal {
                            offset: record.seq.0,
                            reason: format!("committed Put violates table schema: {error}"),
                        })?;
                    if schema.auto_increment_column().is_some_and(|column| {
                        matches!(
                            cells.iter().find(|(id, _)| *id == column.id),
                            Some((_, Value::Int64(value))) if *value == i64::MAX
                        )
                    }) {
                        return Err(MongrelError::CorruptWal {
                            offset: record.seq.0,
                            reason: "committed Put exhausts AUTO_INCREMENT".into(),
                        });
                    }
                }
            }
            Op::Delete { table_id, .. } | Op::TruncateTable { table_id }
                if *table_id != manifest.table_id =>
            {
                return Err(MongrelError::CorruptWal {
                    offset: record.seq.0,
                    reason: format!(
                        "private WAL record references table {table_id}, expected {}",
                        manifest.table_id
                    ),
                });
            }
            Op::TxnCommit { added_runs, .. } if !added_runs.is_empty() => {
                return Err(MongrelError::CorruptWal {
                    offset: record.seq.0,
                    reason: "private WAL contains shared spilled-run metadata".into(),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

fn next_wal_segment(wal_dir: &Path) -> Result<PathBuf> {
    Ok(wal_dir.join(format!("seg-{:06}.wal", next_wal_number(wal_dir)?)))
}

fn wal_segment_number(path: &Path) -> Option<u64> {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .and_then(|stem| stem.strip_prefix("seg-"))
        .and_then(|number| number.parse().ok())
}

fn latest_wal_segment(wal_dir: &Path) -> Result<Option<PathBuf>> {
    let n = list_wal_numbers(wal_dir)?;
    Ok(n.map(|max| wal_dir.join(format!("seg-{max:06}.wal"))))
}

fn next_wal_number(wal_dir: &Path) -> Result<u32> {
    list_wal_numbers(wal_dir)?
        .map(|maximum| {
            maximum
                .checked_add(1)
                .ok_or_else(|| MongrelError::Full("WAL segment namespace exhausted".into()))
        })
        .unwrap_or(Ok(0))
}

fn list_wal_numbers(wal_dir: &Path) -> Result<Option<u32>> {
    let mut max_n = None;
    let entries = match std::fs::read_dir(wal_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    for entry in entries {
        let entry = entry?;
        let fname = entry.file_name();
        let Some(s) = fname.to_str() else {
            continue;
        };
        let Some(stripped) = s.strip_prefix("seg-") else {
            continue;
        };
        let Some(number) = stripped.strip_suffix(".wal") else {
            return Err(MongrelError::CorruptWal {
                offset: 0,
                reason: format!("malformed WAL segment name {s:?}"),
            });
        };
        let n = number
            .parse::<u32>()
            .map_err(|_| MongrelError::CorruptWal {
                offset: 0,
                reason: format!("malformed WAL segment name {s:?}"),
            })?;
        if s != format!("seg-{n:06}.wal") || !entry.file_type()?.is_file() {
            return Err(MongrelError::CorruptWal {
                offset: n as u64,
                reason: format!("noncanonical or nonregular WAL segment {s:?}"),
            });
        }
        max_n = Some(max_n.map(|m: u32| m.max(n)).unwrap_or(n));
    }
    Ok(max_n)
}
