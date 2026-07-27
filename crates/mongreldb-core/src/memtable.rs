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

use crate::be_tree::{BeTree, BeTreeVersionCursor, BeTreeVersionCursorStats};
use crate::epoch::{Epoch, Snapshot};
use crate::rowid::RowId;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Same-`RowId` group members gathered between cooperative cancellation
/// checkpoints inside the memtable merge cursor (REM-C §7.11).
const CURSOR_GROUP_CHECKPOINT_INTERVAL: usize = 256;

/// Min-heap key used to merge memtable leaf streams in ascending
/// `(RowId, Epoch)` order. We carry the version's epoch too so that the
/// dedup pass at emit time has it without re-matching on `Cow`.
struct MemHead<'a> {
    rid: RowId,
    epoch: Epoch,
    index: u32,
    row: Cow<'a, Row>,
}

impl PartialEq for MemHead<'_> {
    fn eq(&self, other: &Self) -> bool {
        (self.rid, self.epoch, self.index) == (other.rid, other.epoch, other.index)
    }
}
impl Eq for MemHead<'_> {}
impl PartialOrd for MemHead<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for MemHead<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        (other.rid, other.epoch, other.index).cmp(&(self.rid, self.epoch, self.index))
    }
}

/// K-way merge over the per-segment lazy [`BeTreeVersionCursor`] streams
/// (REM-C §7.12). Holds one heap head per segment, pops the lowest `RowId`,
/// gathers every head sharing that `RowId`, and emits the newest visible
/// version exactly once per `RowId`. Candidates arrive oldest-first
/// (ascending epoch; exact-key ties in physical write order — older segment
/// index first, within a segment the tree's own write order) and the fold
/// resolves ties with `epoch::version_supersedes`, so the later physical
/// write wins an exact-stamp tie. No segment ever materializes more than
/// its current head plus the current same-row group.
pub struct MemtableVisibleVersionCursor<'a> {
    segments: Vec<BeTreeVersionCursor<'a>>,
    heap: BinaryHeap<MemHead<'a>>,
    snapshot: Snapshot,
    /// Heap heads are pulled from each segment lazily on the first advance
    /// so that construction is O(segments) and a cancelled control is
    /// observed before any version streams.
    primed: bool,
    finished: bool,
    /// Number of source versions examined during the last advance — the
    /// dedup pass that picks the newest visible version per RowId.
    pub(crate) last_examined: usize,
    /// Peak `last_examined` across all calls so far.
    pub(crate) peak_examined: usize,
}

impl<'a> MemtableVisibleVersionCursor<'a> {
    /// Controlled advance: checkpoints the supplied control while gathering
    /// large same-`RowId` groups (§7.11) and threads it into the per-segment
    /// Bε-tree cursors.
    #[doc(hidden)] // streaming-cursor plumbing; used by the engine and tests
    pub fn next_controlled(
        &mut self,
        control: &crate::ExecutionControl,
    ) -> crate::Result<Option<(RowId, Epoch, Cow<'a, Row>)>> {
        self.advance_impl(Some(control))
    }

    /// Aggregate Bε-tree cursor statistics across every segment source
    /// (REM-C §7.6 test instrumentation).
    #[doc(hidden)] // test instrumentation; not a stable public API
    pub fn be_tree_cursor_stats(&self) -> BeTreeVersionCursorStats {
        let mut total = BeTreeVersionCursorStats::default();
        for segment in &self.segments {
            let stats = segment.stats();
            total.active_frames += stats.active_frames;
            total.peak_active_frames = total.peak_active_frames.max(stats.peak_active_frames);
            total.buffered_messages_owned += stats.buffered_messages_owned;
            total.peak_buffered_messages_owned = total
                .peak_buffered_messages_owned
                .max(stats.peak_buffered_messages_owned);
            total.total_versions_precollected += stats.total_versions_precollected;
            total.versions_examined += stats.versions_examined;
            total.checkpoints += stats.checkpoints;
        }
        total
    }

    fn segment_next(
        &mut self,
        index: usize,
        control: Option<&crate::ExecutionControl>,
    ) -> crate::Result<Option<Cow<'a, Row>>> {
        match control {
            Some(control) => self.segments[index].next_controlled(control),
            None => Ok(self.segments[index].next()),
        }
    }

    fn push_head(&mut self, index: usize, row: Cow<'a, Row>) {
        self.heap.push(MemHead {
            rid: row.row_id,
            epoch: row.committed_epoch,
            index: index as u32,
            row,
        });
    }

    fn prime(&mut self, control: Option<&crate::ExecutionControl>) -> crate::Result<()> {
        for index in 0..self.segments.len() {
            if let Some(row) = self.segment_next(index, control)? {
                self.push_head(index, row);
            }
        }
        Ok(())
    }

    fn advance_impl(
        &mut self,
        control: Option<&crate::ExecutionControl>,
    ) -> crate::Result<Option<(RowId, Epoch, Cow<'a, Row>)>> {
        if self.finished {
            return Ok(None);
        }
        if !self.primed {
            self.prime(control)?;
            self.primed = true;
        }
        self.last_examined = 0;
        loop {
            let Some(MemHead {
                rid,
                epoch,
                index,
                row,
            }) = self.heap.pop()
            else {
                self.finished = true;
                return Ok(None);
            };
            self.last_examined += 1;
            // Advance the source the popped head came from.
            if let Some(next) = self.segment_next(index as usize, control)? {
                self.push_head(index as usize, next);
            }
            if !self.snapshot.observes_version(epoch, row.commit_ts) {
                continue;
            }
            // Gather every remaining head that shares this rid; pick the
            // newest visible version among them. Cancellation is observed
            // inside large same-row groups (§7.11).
            let mut best = Some(row);
            let mut gathered = 0usize;
            while self.heap.peek().is_some_and(|h| h.rid == rid) {
                gathered += 1;
                if gathered.is_multiple_of(CURSOR_GROUP_CHECKPOINT_INTERVAL) {
                    if let Some(control) = control {
                        control.checkpoint()?;
                    }
                }
                let head = self.heap.pop().unwrap();
                self.last_examined += 1;
                if let Some(next) = self.segment_next(head.index as usize, control)? {
                    self.push_head(head.index as usize, next);
                }
                if self
                    .snapshot
                    .observes_version(head.epoch, head.row.commit_ts)
                    && best.as_ref().is_none_or(|current| {
                        // Candidates arrive oldest-first (ascending epoch,
                        // ties broken by physical write order); an exact
                        // stamp tie goes to the later write.
                        crate::epoch::version_supersedes(
                            head.epoch,
                            head.row.commit_ts,
                            current.committed_epoch,
                            current.commit_ts,
                        )
                    })
                {
                    best = Some(head.row);
                }
            }
            let Some(best) = best else {
                continue;
            };
            let row_id = best.row_id;
            let epoch = best.committed_epoch;
            self.peak_examined = self.peak_examined.max(self.last_examined);
            return Ok(Some((row_id, epoch, best)));
        }
    }
}

impl<'a> Iterator for MemtableVisibleVersionCursor<'a> {
    type Item = (RowId, Epoch, Cow<'a, Row>);

    fn next(&mut self) -> Option<Self::Item> {
        match self.advance_impl(None) {
            Ok(item) => item,
            Err(_) => unreachable!("the no-cancellation path cannot fail"),
        }
    }
}

impl<'a> MemtableVisibleVersionCursor<'a> {
    /// Cursor is exhausted when the heap is empty.
    pub fn is_exhausted(&self) -> bool {
        self.finished
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
            // Newest segment first (active, then frozen in reverse): on an
            // exact stamp tie the physically newer segment wins — within one
            // segment ties are already resolved by the tree itself.
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
                // Candidates arrive in physical write order (oldest first);
                // the later write wins an exact stamp tie.
                if best.as_ref().is_none_or(|current| {
                    crate::epoch::version_supersedes(
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
        self.newest_visible_map(snapshot).into_values().collect()
    }

    /// Newest visible version per `RowId` as an ordered map (ascending RowId).
    /// Callers that need to stream into a controlled merge without a second
    /// full `Vec` should drain this map in batches rather than collecting.
    pub(crate) fn newest_visible_map(
        &self,
        snapshot: crate::epoch::Snapshot,
    ) -> BTreeMap<RowId, Row> {
        let mut by_row: BTreeMap<RowId, Row> = BTreeMap::new();
        // Oldest segment first, and within a segment oldest write first, so
        // `version_supersedes` can hand an exact stamp tie to the later
        // physical write (newer segment / newer entry wins).
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
                        if crate::epoch::version_supersedes(
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
        by_row
    }

    pub fn newest_visible_iter<'a>(
        &'a self,
        snapshot: &Snapshot,
    ) -> MemtableVisibleVersionCursor<'a> {
        let segments: Vec<BeTreeVersionCursor<'a>> = self
            .frozen
            .iter()
            .map(|segment| segment.tree.leaf_versions_iter())
            .chain(std::iter::once(self.active.tree.leaf_versions_iter()))
            .collect();
        MemtableVisibleVersionCursor {
            segments,
            heap: BinaryHeap::new(),
            snapshot: *snapshot,
            primed: false,
            finished: false,
            last_examined: 0,
            peak_examined: 0,
        }
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

    #[test]
    fn newest_visible_map_prefers_active_on_equal_version() {
        let mut m = Memtable::new();
        let mut deleted = row(1, 2);
        deleted.deleted = true;
        m.upsert(deleted);
        m.seal();
        m.upsert(row(1, 2));

        let versions = m.visible_versions_at(Snapshot::at(Epoch(2)));
        assert_eq!(versions.len(), 1);
        assert!(!versions[0].deleted);
    }

    #[test]
    fn newest_visible_iter_empty_memtable_yields_nothing() {
        let m = Memtable::new();
        assert!(m
            .newest_visible_iter(&Snapshot::at(Epoch(9)))
            .next()
            .is_none());
    }

    #[test]
    fn newest_visible_iter_single_insert_yields_one() {
        let mut m = Memtable::new();
        m.upsert(row(1, 3));
        let values: Vec<_> = m
            .newest_visible_iter(&Snapshot::at(Epoch(3)))
            .map(|(id, epoch, _)| (id, epoch))
            .collect();
        assert_eq!(values, vec![(RowId(1), Epoch(3))]);
    }

    #[test]
    fn newest_visible_iter_newer_epoch_wins() {
        let mut m = Memtable::new();
        m.upsert(row(1, 1));
        m.upsert(row(1, 2));
        let values: Vec<_> = m
            .newest_visible_iter(&Snapshot::at(Epoch(2)))
            .map(|(_, epoch, _)| epoch)
            .collect();
        assert_eq!(values, vec![Epoch(2)]);
    }

    #[test]
    fn newest_visible_iter_tombstone_suppresses_older_live_version() {
        // Mirrors MutableRunVisibleVersionCursor: the cursor yields the
        // tombstone Row itself (so the caller can classify it as a Tombstone
        // fallback or a StaleRowId), but the pre-tombstone live version is
        // suppressed when the tombstone is in scope of the calling snapshot.
        let mut m = Memtable::new();
        m.upsert(row(1, 1));
        m.tombstone(RowId(1), Epoch(2));
        let snap = Snapshot::at(Epoch(2));
        let got: Vec<(u64, u64, bool)> = m
            .newest_visible_iter(&snap)
            .map(|(rid, epoch, row)| (rid.0, epoch.0, row.deleted))
            .collect();
        assert_eq!(got, vec![(1, 2, true)], "tombstone is the newest");
        // Pre-tombstone snapshot still sees the live version.
        let snap_early = Snapshot::at(Epoch(1));
        let got_early: Vec<(u64, u64, bool)> = m
            .newest_visible_iter(&snap_early)
            .map(|(rid, epoch, row)| (rid.0, epoch.0, row.deleted))
            .collect();
        assert_eq!(got_early, vec![(1, 1, false)]);
    }

    /// Regression for the BeTree root-buffer-not-iterated bug (iss10).
    ///
    /// Before the fix, the Bε-tree version stream (now
    /// [`crate::be_tree::BeTreeVersionCursor`]) walked only the
    /// consolidated leaves of the Bε-tree — silently skipping messages that
    /// were still sitting in an internal-node buffer pending flush. A scan
    /// over a live memtable that has triggered at least one split therefore
    /// returned a subset of the inserted rows (typically the leaf-resident
    /// ones, missing every row still buffered at the root).
    ///
    /// This test inserts 1,000 rows without flushing. The first
    /// `LEAF_CAP = 32` rows go directly into a leaf; subsequent splits and
    /// buffer flushes leave a meaningful fraction of the rows sitting in
    /// internal-node buffers. The streaming cursor must yield all 1,000
    /// distinct `(RowId, Epoch)` pairs.
    #[test]
    fn newest_visible_iter_includes_root_buffer_rows() {
        const N: u64 = 1_000;
        let mut m = Memtable::new();
        // Each row gets a fresh RowId and its own (epoch-bumped) version; the
        // memtable has no flush path in this scope so the rows have to be
        // reachable via the active BeTree.
        for i in 0..N {
            let mut r = Row::new(RowId(i), Epoch(i + 1));
            r.columns.insert(1, Value::Int64(i as i64 * 10));
            m.upsert(r);
        }
        assert_eq!(m.len(), N as usize);

        // Snapshot high enough that every version is visible.
        let snap = Snapshot::at(Epoch(N + 10));
        let got: Vec<(u64, u64, i64)> = m
            .newest_visible_iter(&snap)
            .map(|(rid, epoch, row)| (rid.0, epoch.0, int_of_value(&row)))
            .collect();

        // Count: every distinct RowId must be visible exactly once.
        assert_eq!(
            got.len(),
            N as usize,
            "buffered rows must be visible to a streaming scan (got {})",
            got.len()
        );
        let mut seen_row_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (rid, _epoch, _v) in &got {
            assert!(
                seen_row_ids.insert(*rid),
                "duplicate RowId {rid} in streaming scan output"
            );
        }
        // Set equality: every input RowId was emitted, no extras.
        let expected_ids: std::collections::HashSet<u64> = (0..N).collect();
        let got_ids: std::collections::HashSet<u64> = got.iter().map(|(rid, _, _)| *rid).collect();
        assert_eq!(got_ids, expected_ids, "must yield every input RowId");

        // Spot-check: epoch and column bytes for a buffered row (high RowId
        // is almost certainly still buffered, not yet flushed to a leaf).
        let (_, epoch_raw, v) = got
            .iter()
            .find(|(rid, _, _)| *rid == N - 1)
            .copied()
            .expect("highest RowId present");
        assert_eq!(epoch_raw, N);
        assert_eq!(v, (N as i64 - 1) * 10);
    }

    /// Same shape as `newest_visible_iter_includes_root_buffer_rows`, but
    /// drives the cursor against one row with many versions — exercising the
    /// case where the same `RowId` coexists in both a leaf and the root
    /// buffer (dedup picks the newest visible, which must come from the
    /// buffer when the buffered version is the latest).
    #[test]
    fn newest_visible_iter_buffered_versions_dedup_against_leaf_resident() {
        // Seed the leaf via a few inserts.
        let mut m = Memtable::new();
        for i in 0..16u64 {
            let mut r = Row::new(RowId(7), Epoch(i + 1));
            r.columns.insert(1, Value::Int64(i as i64));
            m.upsert(r);
        }
        // Then drive the tree through several splits by inserting many more
        // rows so the buffered message for the same RowId has to coexist with
        // the leaf-resident version (different epochs for the same RowId in
        // two locations).
        for i in 0u64..4_000 {
            let mut r = Row::new(RowId(1000 + i), Epoch(i + 100));
            r.columns.insert(1, Value::Int64(i as i64));
            m.upsert(r);
        }
        // One more version of row 7, distinctly newer than the leaf-resident
        // ones — guaranteed to land in some internal-node buffer.
        let mut latest_seven = Row::new(RowId(7), Epoch(20_000));
        latest_seven.columns.insert(1, Value::Int64(999));
        m.upsert(latest_seven);

        let snap = Snapshot::at(Epoch(20_001));
        let seven = m
            .newest_visible_iter(&snap)
            .find(|(rid, _epoch, _row)| *rid == RowId(7))
            .expect("row 7 visible");
        assert_eq!(seven.1, Epoch(20_000), "buffered newest wins");
        // The full cursor must yield every distinct RowId (latest version):
        // 4,000 background rows + 1 distinct version of row 7.
        let total: u64 = m
            .newest_visible_iter(&snap)
            .map(|(rid, _epoch, _row)| rid.0)
            .fold(0u64, |acc, _| acc + 1);
        assert_eq!(total, 4_001, "buffer + leaf coverage");
    }

    fn int_of_value(row: &Row) -> i64 {
        match row.columns.get(&1) {
            Some(Value::Int64(x)) => *x,
            other => panic!("expected Int64 column, got {other:?}"),
        }
    }

    /// Build a memtable whose active tree has row 7 flushed into a leaf and
    /// a root buffer that still has free capacity: 33 inserts split the root
    /// leaf into an internal node, and 10 more inserts sit in its buffer
    /// (BUFFER_CAP is 16), so one further mutation is guaranteed to stay
    /// buffered at the root rather than flush to a leaf.
    fn memtable_with_leaf_resident_seven() -> Memtable {
        let mut m = Memtable::new();
        for i in 0..33u64 {
            m.upsert(row(i, 1));
        }
        for i in 100..110u64 {
            m.upsert(row(i, 1));
        }
        m
    }

    /// REM-C §7.13: one `RowId` resident in both a leaf and an internal-node
    /// buffer emits exactly once, with the newest (buffered) version.
    #[test]
    fn same_rowid_in_buffer_and_leaf_dedups_once() {
        let mut m = memtable_with_leaf_resident_seven();
        m.upsert(row(7, 2)); // buffered at the root; newer than the leaf copy
        let got: Vec<(u64, u64, i64)> = m
            .newest_visible_iter(&Snapshot::at(Epoch(3)))
            .filter(|(rid, _, _)| *rid == RowId(7))
            .map(|(rid, epoch, row)| (rid.0, epoch.0, int_of_value(&row)))
            .collect();
        assert_eq!(got, vec![(7, 2, 70)], "row 7 must emit exactly once");
    }

    /// REM-C §7.13: one `RowId` spanning several frozen segments plus the
    /// active segment emits exactly once, with the newest version.
    #[test]
    fn same_rowid_across_frozen_segments_dedups_once() {
        let mut m = Memtable::new();
        m.upsert(row(7, 1));
        m.seal();
        m.upsert(row(7, 2));
        m.seal();
        m.upsert(row(7, 3));
        let got: Vec<(u64, u64)> = m
            .newest_visible_iter(&Snapshot::at(Epoch(10)))
            .map(|(rid, epoch, _)| (rid.0, epoch.0))
            .collect();
        assert_eq!(got, vec![(7, 3)]);
    }

    /// REM-C §7.13: a tombstone still sitting in an internal-node buffer
    /// suppresses the older leaf-resident live row.
    #[test]
    fn buffered_tombstone_suppresses_leaf_live_row() {
        let mut m = memtable_with_leaf_resident_seven();
        m.tombstone(RowId(7), Epoch(2)); // buffered at the root
        let at_tombstone: Vec<(u64, bool)> = m
            .newest_visible_iter(&Snapshot::at(Epoch(3)))
            .filter(|(rid, _, _)| *rid == RowId(7))
            .map(|(_, epoch, row)| (epoch.0, row.deleted))
            .collect();
        assert_eq!(
            at_tombstone,
            vec![(2, true)],
            "buffered tombstone is the newest visible version"
        );
        let before: Vec<(u64, bool)> = m
            .newest_visible_iter(&Snapshot::at(Epoch(1)))
            .filter(|(rid, _, _)| *rid == RowId(7))
            .map(|(_, epoch, row)| (epoch.0, row.deleted))
            .collect();
        assert_eq!(before, vec![(1, false)]);
    }

    /// REM-C §7.13: HLC/epoch order inversion inside a single segment — the
    /// cursor must pick the higher HLC even at a lower epoch.
    #[test]
    fn hlc_inversion_inside_one_segment() {
        use mongreldb_types::hlc::HlcTimestamp;
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
        let mut m = Memtable::new();
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(50), early).with_column(1, Value::Int64(1)));
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(1), late).with_column(1, Value::Int64(99)));
        let snap = Snapshot::at_hlc(Epoch(99), late);
        let got: Vec<(u64, i64)> = m
            .newest_visible_iter(&snap)
            .map(|(_, epoch, row)| (epoch.0, int_of_value(&row)))
            .collect();
        assert_eq!(got, vec![(1, 99)], "higher HLC wins over higher epoch");
    }

    /// REM-C §7.13: the same inversion across a frozen segment and the
    /// active segment.
    #[test]
    fn hlc_inversion_across_frozen_segments() {
        use mongreldb_types::hlc::HlcTimestamp;
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
        let mut m = Memtable::new();
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(50), early).with_column(1, Value::Int64(1)));
        m.seal();
        m.upsert(Row::new_with_hlc(RowId(1), Epoch(1), late).with_column(1, Value::Int64(99)));
        let snap = Snapshot::at_hlc(Epoch(99), late);
        let got: Vec<(u64, i64)> = m
            .newest_visible_iter(&snap)
            .map(|(_, epoch, row)| (epoch.0, int_of_value(&row)))
            .collect();
        assert_eq!(got, vec![(1, 99)], "higher HLC wins across segments");
    }

    /// REM-C §7.13: cancellation is observed while gathering a large
    /// same-`RowId` version group, not only between groups.
    #[test]
    fn cancellation_during_large_same_row_group() {
        let mut m = Memtable::new();
        m.upsert(row(1, 1));
        for v in 0..10_000u64 {
            m.upsert(row(7, v + 1));
        }
        let snap = Snapshot::at(Epoch(20_000));
        let mut cursor = m.newest_visible_iter(&snap);
        let control = crate::ExecutionControl::new(None);
        let (rid, ..) = cursor
            .next_controlled(&control)
            .expect("first advance")
            .expect("row 1");
        assert_eq!(rid, RowId(1));
        // Row 7 has a 10,000-version group; the cancelled control must stop
        // the gather at its 256-version checkpoint.
        control.cancel(crate::CancellationReason::ClientRequest);
        let err = cursor
            .next_controlled(&control)
            .expect_err("cancelled control must stop the gather");
        assert!(matches!(err, crate::MongrelError::Cancelled));
    }
}
