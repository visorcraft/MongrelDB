//! In-memory write buffer (the "memtable").
//!
//! Phase 11.2 wires the buffered [`crate::be_tree::BeTree`] (a Bε-tree over the
//! composite `(RowId, Epoch)` version key) in as the live memtable, replacing
//! the prototype skip list. A Bε-tree buffers many pending mutations per
//! internal node and flushes them to one child in bulk, so write amplification
//! approaches O(1) — the update-amplification win the design calls for. The
//! composite key keeps multiple versions of a logical row coexisting. Product
//! visibility prefers HLC via [`crate::epoch::Snapshot::observes_row`] /
//! [`crate::epoch::Snapshot::version_is_newer`] when versions carry `commit_ts`
//! (P0.5-T3); epoch-only APIs remain for dual-model legacy call sites.

use crate::be_tree::BeTree;
use crate::epoch::Epoch;
use crate::rowid::RowId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// A cell value in the in-memory path. The flush path re-encodes these into
/// columnar pages; it is intentionally simple for the prototype.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(f64),
    Bytes(Vec<u8>),
    Embedding(Vec<f32>),
    /// Unscaled decimal value (i128). The column's `TypeId::Decimal128`
    /// carries the precision/scale for formatting.
    Decimal(i128),
    /// SQL INTERVAL value: months, days, nanoseconds.
    Interval {
        months: i64,
        days: i32,
        nanos: i64,
    },
    /// RFC 4122 UUID (16 bytes, big-endian for sort order).
    Uuid([u8; 16]),
    /// JSON value stored as a UTF-8 byte sequence.
    Json(Vec<u8>),
    /// Generated embedding with durable source and model provenance.
    ///
    /// Kept last so existing bincode enum discriminants remain stable.
    GeneratedEmbedding(Box<crate::embedding::GeneratedEmbeddingValue>),
}

impl Value {
    pub fn as_embedding(&self) -> Option<&[f32]> {
        match self {
            Self::Embedding(values) => Some(values),
            Self::GeneratedEmbedding(value) => Some(&value.vector),
            _ => None,
        }
    }

    pub fn generated_embedding_metadata(
        &self,
    ) -> Option<&crate::embedding::GeneratedEmbeddingMetadata> {
        match self {
            Self::GeneratedEmbedding(value) => Some(&value.metadata),
            _ => None,
        }
    }

    /// Lexicographically-comparable byte encoding for index keys (PK HOT,
    /// bitmaps). Big-endian for integers so byte order matches value order.
    pub fn encode_key(&self) -> Vec<u8> {
        match self {
            Value::Null => Vec::new(),
            Value::Bool(b) => vec![*b as u8],
            Value::Int64(n) => n.to_be_bytes().to_vec(),
            Value::Float64(f) => f.to_bits().to_be_bytes().to_vec(),
            Value::Bytes(b) => b.clone(),
            Value::Embedding(v) => {
                let mut out = Vec::with_capacity(v.len() * 4);
                for x in v {
                    out.extend_from_slice(&x.to_bits().to_be_bytes());
                }
                out
            }
            Value::GeneratedEmbedding(value) => {
                let mut out = Vec::with_capacity(value.vector.len() * 4);
                for x in &value.vector {
                    out.extend_from_slice(&x.to_bits().to_be_bytes());
                }
                out
            }
            Value::Decimal(d) => d.to_be_bytes().to_vec(),
            Value::Interval {
                months,
                days,
                nanos,
            } => {
                let mut out = Vec::with_capacity(20);
                out.extend_from_slice(&months.to_be_bytes());
                out.extend_from_slice(&days.to_be_bytes());
                out.extend_from_slice(&nanos.to_be_bytes());
                out
            }
            Value::Uuid(b) => b.to_vec(),
            Value::Json(b) => b.clone(),
        }
    }

    pub(crate) fn estimated_bytes(&self) -> u64 {
        match self {
            Value::Null => 1,
            Value::Bool(_) => 1,
            Value::Int64(_) | Value::Float64(_) => 8,
            Value::Bytes(bytes) | Value::Json(bytes) => 16 + bytes.len() as u64,
            Value::Embedding(values) => 16 + (values.len() as u64) * 4,
            Value::GeneratedEmbedding(value) => {
                16 + (value.vector.len() as u64) * 4
                    + value.metadata.provider_id.len() as u64
                    + value.metadata.model_id.len() as u64
                    + value.metadata.model_version.len() as u64
                    + value.metadata.preprocessing_version.len() as u64
                    + 48
            }
            Value::Decimal(_) | Value::Uuid(_) => 16,
            Value::Interval { .. } => 20,
        }
    }
}

/// One logical row held in the memtable. A `deleted` row is a tombstone.
///
/// Field order of the **bincode WAL `Put` payload** is fixed as
/// `(row_id, committed_epoch, columns, deleted)` — the 0.63.1 layout.
/// [`Self::commit_ts`] is in-memory only (`#[serde(skip)]`); durable HLC for
/// WAL recovery is `Op::CommitTimestamp`, and sorted runs use the
/// `SYS_COMMIT_TS` system column (with its own legacy-compatible path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub row_id: RowId,
    pub committed_epoch: Epoch,
    pub columns: HashMap<u16, Value>,
    pub deleted: bool,
    /// Optional HLC stamp (P0.5 dual-model). Not encoded in WAL `Put` bincode
    /// payloads — see struct-level docs. Kept last so call sites and future
    /// wire evolution treat the 0.63.1 fields as the stable prefix.
    #[serde(skip)]
    pub commit_ts: Option<mongreldb_types::hlc::HlcTimestamp>,
}

impl Row {
    pub fn new(row_id: RowId, committed_epoch: Epoch) -> Self {
        Self {
            row_id,
            committed_epoch,
            columns: HashMap::new(),
            deleted: false,
            commit_ts: None,
        }
    }

    pub fn new_with_hlc(
        row_id: RowId,
        committed_epoch: Epoch,
        commit_ts: mongreldb_types::hlc::HlcTimestamp,
    ) -> Self {
        Self {
            row_id,
            committed_epoch,
            columns: HashMap::new(),
            deleted: false,
            commit_ts: Some(commit_ts),
        }
    }

    pub fn with_column(mut self, column_id: u16, value: Value) -> Self {
        self.columns.insert(column_id, value);
        self
    }

    /// Rough byte estimate for flush-threshold decisions.
    pub fn estimated_bytes(&self) -> u64 {
        self.columns
            .values()
            .fold(32, |bytes, value| bytes + value.estimated_bytes())
    }
}

/// Bε-tree-backed memtable, ordered by `(RowId, Epoch)`. A drop-in replacement
/// for the prototype skip list: the same MVCC semantics with lower write
/// amplification (buffered messages flush to children in bulk).
#[derive(Clone)]
struct MemtableSegment {
    tree: BeTree,
    byte_size: u64,
}

/// Structurally shared committed overlays plus one small mutable write delta.
#[derive(Clone)]
pub struct Memtable {
    frozen: Arc<Vec<Arc<MemtableSegment>>>,
    active: MemtableSegment,
    byte_size: u64,
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            frozen: Arc::new(Vec::new()),
            active: MemtableSegment {
                tree: BeTree::new(),
                byte_size: 0,
            },
            byte_size: 0,
        }
    }

    /// Append a row version (keyed by `(row_id, committed_epoch)`). Versions are
    /// never overwritten; the newest visible one wins at read time.
    pub fn upsert(&mut self, row: Row) {
        let bytes = row.estimated_bytes();
        self.byte_size = self.byte_size.saturating_add(bytes);
        self.active.byte_size = self.active.byte_size.saturating_add(bytes);
        self.active.tree.insert_row(row);
    }

    /// Append a tombstone version for `row_id` at `epoch`. The tombstone copies
    /// the columns from the newest live version so that engine-level HOT cleanup
    /// can recover the primary-key value during WAL replay.
    pub fn tombstone(&mut self, row_id: RowId, epoch: Epoch) {
        let mut columns = HashMap::new();
        if let Some(live) = self.get(row_id, Epoch(epoch.0.saturating_sub(1))) {
            columns = live.columns;
        }
        let row = Row {
            row_id,
            committed_epoch: epoch,
            columns,
            deleted: true,
            commit_ts: None,
        };
        self.upsert(row);
    }

    /// Read the row at `row_id` visible to `snapshot`: the newest version with
    /// `epoch <= snapshot`. Returns `None` if that version is a tombstone (or no
    /// such version exists).
    pub fn get(&self, row_id: RowId, snapshot_epoch: Epoch) -> Option<Row> {
        self.get_version(row_id, snapshot_epoch)
            .and_then(|(_, row)| (!row.deleted).then_some(row))
    }

    /// Newest version of `row_id` with `epoch <= snapshot`, **including
    /// tombstones** (as a `Row` with `deleted=true`). Legacy epoch-only entry
    /// point; prefer [`Self::get_version_at`] when the caller holds a full
    /// [`crate::epoch::Snapshot`].
    pub fn get_version(&self, row_id: RowId, snapshot_epoch: Epoch) -> Option<(Epoch, Row)> {
        self.get_version_at(row_id, crate::epoch::Snapshot::at(snapshot_epoch))
    }

    /// Newest version of `row_id` visible under `snapshot` (including
    /// tombstones). Uses HLC authority when stamps are present (P0.5-T3).
    ///
    /// Seeks each segment's composite-key range for `row_id` so dual-model
    /// mixes (stamped + unstamped) and HLC/epoch order inversions stay correct
    /// without materializing every version in the memtable.
    pub fn get_version_at(
        &self,
        row_id: RowId,
        snapshot: crate::epoch::Snapshot,
    ) -> Option<(Epoch, Row)> {
        if !snapshot.uses_hlc_authority() {
            let mut best = self.active.tree.get_version(row_id, snapshot.epoch);
            for segment in self.frozen.iter().rev() {
                let Some(candidate) = segment.tree.get_version(row_id, snapshot.epoch) else {
                    continue;
                };
                if best.as_ref().is_none_or(|(epoch, _)| candidate.0 > *epoch) {
                    best = Some(candidate);
                }
            }
            return best;
        }

        let mut best: Option<Row> = None;
        for segment in self
            .frozen
            .iter()
            .map(|segment| &segment.tree)
            .chain(std::iter::once(&self.active.tree))
        {
            segment.visit_versions(row_id, |row| {
                if !snapshot.observes_row(row.committed_epoch, row.commit_ts) {
                    return;
                }
                if best.as_ref().is_none_or(|current| {
                    crate::epoch::Snapshot::version_is_newer(
                        row.committed_epoch,
                        row.commit_ts,
                        current.committed_epoch,
                        current.commit_ts,
                    )
                }) {
                    best = Some(row);
                }
            });
        }
        best.map(|row| (row.committed_epoch, row))
    }

    /// Number of stored versions.
    pub fn len(&self) -> usize {
        self.active.tree.mutations()
            + self
                .frozen
                .iter()
                .map(|segment| segment.tree.mutations())
                .sum::<usize>()
    }

    pub fn is_empty(&self) -> bool {
        self.active.tree.is_empty() && self.frozen.is_empty()
    }

    pub fn approx_bytes(&self) -> u64 {
        self.byte_size
    }

    /// Visible rows at `snapshot`, deduplicated to the newest version per
    /// `RowId` (tombstones drop their row). Returned in ascending `RowId` order.
    pub fn visible_rows(&self, snapshot_epoch: Epoch) -> Vec<Row> {
        self.visible_versions(snapshot_epoch)
            .into_iter()
            .filter(|r| !r.deleted)
            .collect()
    }

    /// Newest visible version per `RowId` at `snapshot`, **including
    /// tombstones** (as `Row`s with `deleted=true`). Used by the engine to merge
    /// versions across the memtable and sorted runs.
    pub fn visible_versions(&self, snapshot_epoch: Epoch) -> Vec<Row> {
        self.visible_versions_at(crate::epoch::Snapshot::at(snapshot_epoch))
    }

    pub fn visible_versions_at(&self, snapshot: crate::epoch::Snapshot) -> Vec<Row> {
        let mut by_row: BTreeMap<RowId, Row> = BTreeMap::new();
        for segment in self
            .frozen
            .iter()
            .map(|segment| &segment.tree)
            .chain(std::iter::once(&self.active.tree))
        {
            for row in segment.versions() {
                if !snapshot.observes_version(row.committed_epoch, row.commit_ts) {
                    continue;
                }
                by_row
                    .entry(row.row_id)
                    .and_modify(|existing| {
                        if crate::epoch::Snapshot::version_is_newer(
                            row.committed_epoch,
                            row.commit_ts,
                            existing.committed_epoch,
                            existing.commit_ts,
                        ) {
                            *existing = row.clone();
                        }
                    })
                    .or_insert(row);
            }
        }
        by_row.into_values().collect()
    }

    /// Freeze the current write delta so future clones share it by `Arc`.
    pub(crate) fn seal(&mut self) {
        if self.active.tree.is_empty() {
            return;
        }
        let active = std::mem::replace(
            &mut self.active,
            MemtableSegment {
                tree: BeTree::new(),
                byte_size: 0,
            },
        );
        Arc::make_mut(&mut self.frozen).push(Arc::new(active));
        if self.frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
            self.consolidate();
        }
    }

    fn consolidate(&mut self) {
        let mut tree = BeTree::new();
        for row in self
            .frozen
            .iter()
            .flat_map(|segment| segment.tree.versions())
        {
            tree.insert_row(row);
        }
        self.frozen = Arc::new(vec![Arc::new(MemtableSegment {
            tree,
            byte_size: self.byte_size,
        })]);
    }

    #[cfg(test)]
    pub(crate) fn frozen_layer_count(&self) -> usize {
        self.frozen.len()
    }

    /// Drain all versions (for a memtable-to-run flush). Returns them in
    /// ascending `(RowId, Epoch)` order.
    pub fn drain_sorted(&mut self) -> Vec<Row> {
        let mut out = self
            .frozen
            .iter()
            .flat_map(|segment| segment.tree.versions())
            .chain(self.active.tree.versions())
            .collect::<Vec<_>>();
        out.sort_by_key(|row| (row.row_id, row.committed_epoch));
        self.frozen = Arc::new(Vec::new());
        self.active = MemtableSegment {
            tree: BeTree::new(),
            byte_size: 0,
        };
        self.byte_size = 0;
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: u64, epoch: u64) -> Row {
        Row::new(RowId(id), Epoch(epoch)).with_column(1, Value::Int64(id as i64 * 10))
    }

    #[test]
    fn upsert_get_and_visibility() {
        let mut m = Memtable::new();
        m.upsert(row(1, 5));
        assert_eq!(m.len(), 1);
        assert!(m.get(RowId(1), Epoch(5)).is_some());
        assert!(m.get(RowId(1), Epoch(4)).is_none()); // not yet visible
        assert!(m.get(RowId(2), Epoch(9)).is_none()); // missing
    }

    #[test]
    fn tombstone_supersedes_at_its_epoch() {
        let mut m = Memtable::new();
        m.upsert(row(1, 1));
        // Before the tombstone: the live version is visible.
        assert!(m.get(RowId(1), Epoch(1)).is_some());
        m.tombstone(RowId(1), Epoch(2));
        // At/after the tombstone: hidden.
        assert!(m.get(RowId(1), Epoch(2)).is_none());
        assert!(m.get(RowId(1), Epoch(9)).is_none());
        // A snapshot before the tombstone still sees the live version.
        assert!(m.get(RowId(1), Epoch(1)).is_some());
    }

    #[test]
    fn sealed_generations_share_rows_and_consolidate() {
        let mut writer = Memtable::new();
        for id in 0..crate::MAX_READ_GENERATION_LAYERS as u64 + 2 {
            writer.upsert(row(id, id + 1));
            writer.seal();
        }
        assert!(writer.frozen_layer_count() < crate::MAX_READ_GENERATION_LAYERS);
        let generation = writer.clone();
        writer.upsert(row(99, 99));
        assert!(generation.get(RowId(99), Epoch(99)).is_none());
        assert!(writer.get(RowId(99), Epoch(99)).is_some());
    }

    #[test]
    fn hlc_visibility_is_authoritative_when_stamped() {
        use mongreldb_types::hlc::HlcTimestamp;
        let mut m = Memtable::new();
        let early = HlcTimestamp {
            physical_micros: 100,
            logical: 0,
            node_tiebreaker: 1,
        };
        let late = HlcTimestamp {
            physical_micros: 200,
            logical: 0,
            node_tiebreaker: 1,
        };
        let mut r1 = Row::new_with_hlc(RowId(1), Epoch(1), early);
        r1.columns.insert(1, Value::Int64(1));
        let mut r2 = Row::new_with_hlc(RowId(1), Epoch(2), late);
        r2.columns.insert(1, Value::Int64(2));
        m.upsert(r1);
        m.upsert(r2);
        let snap = crate::epoch::Snapshot::at_hlc(Epoch(99), early);
        let versions = m.visible_versions_at(snap);
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].columns.get(&1), Some(&Value::Int64(1)));
        let snap2 = crate::epoch::Snapshot::at_hlc(Epoch(1), late);
        assert_eq!(
            m.visible_versions_at(snap2)[0].columns.get(&1),
            Some(&Value::Int64(2))
        );
    }

    #[test]
    fn snapshot_hlc_hides_later_commit_ts_even_if_epoch_higher() {
        use mongreldb_types::hlc::HlcTimestamp;
        let mut m = Memtable::new();
        let early = HlcTimestamp {
            physical_micros: 100,
            logical: 0,
            node_tiebreaker: 1,
        };
        let late = HlcTimestamp {
            physical_micros: 200,
            logical: 0,
            node_tiebreaker: 1,
        };
        // Epoch(1) with late HLC would win under epoch-only rules when snap
        // epoch is 99 — HLC authority must hide it under an early pin.
        let mut late_row = Row::new_with_hlc(RowId(1), Epoch(1), late);
        late_row.columns.insert(1, Value::Int64(99));
        let mut early_row = Row::new_with_hlc(RowId(1), Epoch(50), early);
        early_row.columns.insert(1, Value::Int64(1));
        m.upsert(late_row);
        m.upsert(early_row);
        let snap = crate::epoch::Snapshot::at_hlc(Epoch(99), early);
        let versions = m.visible_versions_at(snap);
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].columns.get(&1), Some(&Value::Int64(1)));
        assert_eq!(versions[0].commit_ts, Some(early));
    }

    #[test]
    fn epoch_only_snapshot_sees_hlc_stamped_rows_by_epoch() {
        use mongreldb_types::hlc::HlcTimestamp;
        let mut m = Memtable::new();
        let ts = HlcTimestamp {
            physical_micros: 50,
            logical: 0,
            node_tiebreaker: 1,
        };
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(1), ts).with_column(1, Value::Int64(1)));
        m.upsert(Row::new(RowId(2), Epoch(1)).with_column(1, Value::Int64(2)));
        let legacy = crate::epoch::Snapshot::at(Epoch(99));
        let versions = m.visible_versions_at(legacy);
        assert_eq!(
            versions.len(),
            2,
            "dual-model: epoch pin sees HLC rows by epoch"
        );
        assert!(m.get_version_at(RowId(1), legacy).is_some());
        assert!(m.get_version_at(RowId(2), legacy).is_some());
        let future = crate::epoch::Snapshot::at(Epoch(0));
        assert!(m.get_version_at(RowId(1), future).is_none());
    }

    #[test]
    fn get_version_at_prefers_hlc_over_epoch_order() {
        use mongreldb_types::hlc::HlcTimestamp;
        let mut m = Memtable::new();
        let early = HlcTimestamp {
            physical_micros: 100,
            logical: 0,
            node_tiebreaker: 1,
        };
        let late = HlcTimestamp {
            physical_micros: 200,
            logical: 0,
            node_tiebreaker: 1,
        };
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(1), late).with_column(1, Value::Int64(99)));
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(50), early).with_column(1, Value::Int64(1)));
        let snap = crate::epoch::Snapshot::at_hlc(Epoch(99), early);
        let (_, row) = m.get_version_at(RowId(1), snap).expect("visible");
        assert_eq!(row.columns.get(&1), Some(&Value::Int64(1)));
        assert_eq!(row.commit_ts, Some(early));
    }

    /// WAL `Put` payloads must keep the 0.63.1 bincode layout
    /// `(row_id, committed_epoch, columns, deleted)`. `commit_ts` is
    /// in-memory only (`#[serde(skip)]`) so a 0.63.1-shaped blob still opens.
    #[test]
    fn wal_put_row_bincode_matches_0_63_1_layout() {
        use mongreldb_types::hlc::HlcTimestamp;
        use serde::{Deserialize, Serialize};

        #[derive(Serialize, Deserialize)]
        struct LegacyRow {
            row_id: RowId,
            committed_epoch: Epoch,
            columns: HashMap<u16, Value>,
            deleted: bool,
        }

        let legacy = LegacyRow {
            row_id: RowId(7),
            committed_epoch: Epoch(3),
            columns: [(1, Value::Int64(42))].into_iter().collect(),
            deleted: false,
        };
        let bytes = bincode::serialize(&legacy).expect("legacy encode");

        let decoded: Row = bincode::deserialize(&bytes).expect("0.63.1 payload must decode");
        assert_eq!(decoded.row_id, RowId(7));
        assert_eq!(decoded.committed_epoch, Epoch(3));
        assert_eq!(decoded.columns.get(&1), Some(&Value::Int64(42)));
        assert!(!decoded.deleted);
        assert!(decoded.commit_ts.is_none());

        // Round-trip through Row: commit_ts is not on the wire.
        let stamped = HlcTimestamp {
            physical_micros: 1_700_000_000_000,
            logical: 2,
            node_tiebreaker: 9,
        };
        let mut live = Row::new_with_hlc(RowId(7), Epoch(3), stamped);
        live.columns.insert(1, Value::Int64(42));
        let wire = bincode::serialize(&live).expect("row encode");
        assert_eq!(
            wire, bytes,
            "WAL Put encoding must match the 0.63.1 four-field layout"
        );
        let again: Row = bincode::deserialize(&wire).expect("row decode");
        assert!(
            again.commit_ts.is_none(),
            "commit_ts is restored from Op::CommitTimestamp, not the Put blob"
        );
    }

    #[test]
    fn drain_sorted_is_ascending_and_empties() {
        let mut m = Memtable::new();
        m.upsert(row(3, 1));
        m.upsert(row(1, 1));
        m.upsert(row(2, 1));
        let out = m.drain_sorted();
        let ids: Vec<u64> = out.iter().map(|r| r.row_id.0).collect();
        assert_eq!(ids, vec![1, 2, 3]);
        assert!(m.is_empty());
        assert_eq!(m.approx_bytes(), 0);
    }

    #[test]
    fn visible_rows_dedups_to_newest_version() {
        let mut m = Memtable::new();
        m.upsert(row(1, 1));
        m.upsert(row(2, 9)); // future relative to snapshot 5
        m.upsert(row(3, 1));
        m.upsert(row(1, 3)); // newer version of row 1
        let ids: Vec<u64> = m
            .visible_rows(Epoch(5))
            .iter()
            .map(|r| r.row_id.0)
            .collect();
        assert_eq!(ids, vec![1, 3]);
    }
}
