//! In-memory write buffer (the "memtable").
//!
//! Phase 11.2 wires the buffered [`crate::be_tree::BeTree`] (a Bε-tree over the
//! composite `(RowId, Epoch)` version key) in as the live memtable, replacing
//! the prototype skip list. A Bε-tree buffers many pending mutations per
//! internal node and flushes them to one child in bulk, so write amplification
//! approaches O(1) — the update-amplification win the design calls for. The
//! composite key keeps multiple versions of a logical row coexisting so a
//! snapshot read returns the newest version with `committed_epoch <= snapshot`,
//! which is what makes MVCC correct within the live memtable.

use crate::be_tree::BeTree;
use crate::epoch::Epoch;
use crate::rowid::RowId;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

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
}

impl Value {
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
        }
    }
}

/// One logical row held in the memtable. A `deleted` row is a tombstone.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub row_id: RowId,
    pub committed_epoch: Epoch,
    pub columns: HashMap<u16, Value>,
    pub deleted: bool,
}

impl Row {
    pub fn new(row_id: RowId, committed_epoch: Epoch) -> Self {
        Self {
            row_id,
            committed_epoch,
            columns: HashMap::new(),
            deleted: false,
        }
    }

    pub fn with_column(mut self, column_id: u16, value: Value) -> Self {
        self.columns.insert(column_id, value);
        self
    }

    /// Rough byte estimate for flush-threshold decisions.
    pub fn estimated_bytes(&self) -> u64 {
        let mut n = 32; // header overhead
        for v in self.columns.values() {
            n += match v {
                Value::Null => 1,
                Value::Bool(_) => 1,
                Value::Int64(_) => 8,
                Value::Float64(_) => 8,
                Value::Bytes(b) => 16 + b.len() as u64,
                Value::Embedding(v) => 16 + (v.len() as u64) * 4,
            };
        }
        n
    }
}

/// Bε-tree-backed memtable, ordered by `(RowId, Epoch)`. A drop-in replacement
/// for the prototype skip list: the same MVCC semantics with lower write
/// amplification (buffered messages flush to children in bulk).
pub struct Memtable {
    tree: BeTree,
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
            tree: BeTree::new(),
            byte_size: 0,
        }
    }

    /// Append a row version (keyed by `(row_id, committed_epoch)`). Versions are
    /// never overwritten; the newest visible one wins at read time.
    pub fn upsert(&mut self, row: Row) {
        self.byte_size += row.estimated_bytes();
        self.tree.insert_row(row);
    }

    /// Append a tombstone version for `row_id` at `epoch`.
    pub fn tombstone(&mut self, row_id: RowId, epoch: Epoch) {
        let row = Row {
            row_id,
            committed_epoch: epoch,
            columns: HashMap::new(),
            deleted: true,
        };
        self.upsert(row);
    }

    /// Read the row at `row_id` visible to `snapshot`: the newest version with
    /// `epoch <= snapshot`. Returns `None` if that version is a tombstone (or no
    /// such version exists).
    pub fn get(&self, row_id: RowId, snapshot_epoch: Epoch) -> Option<Row> {
        self.tree.get_visible(row_id, snapshot_epoch)
    }

    /// Newest version of `row_id` with `epoch <= snapshot`, **including
    /// tombstones** (as a `Row` with `deleted=true`). Used by the engine to
    /// merge versions across the memtable and sorted runs.
    pub fn get_version(&self, row_id: RowId, snapshot_epoch: Epoch) -> Option<(Epoch, Row)> {
        self.tree.get_version(row_id, snapshot_epoch)
    }

    /// Number of stored versions.
    pub fn len(&self) -> usize {
        self.tree.mutations()
    }

    pub fn is_empty(&self) -> bool {
        self.tree.is_empty()
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
        let mut by_row: BTreeMap<RowId, Row> = BTreeMap::new();
        for row in self.tree.versions() {
            if row.committed_epoch <= snapshot_epoch {
                by_row
                    .entry(row.row_id)
                    .and_modify(|e| {
                        if row.committed_epoch > e.committed_epoch {
                            *e = row.clone();
                        }
                    })
                    .or_insert(row);
            }
        }
        by_row.into_values().collect()
    }

    /// Drain all versions (for a memtable-to-run flush). Returns them in
    /// ascending `(RowId, Epoch)` order.
    pub fn drain_sorted(&mut self) -> Vec<Row> {
        let out = std::mem::take(&mut self.tree).into_sorted_rows();
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
