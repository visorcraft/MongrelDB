//! Buffered Bε-tree (B-epsilon-tree) over composite `(RowId, Epoch)` keys —
//! the Phase 1 memtable target.
//!
//! Keyed by `(RowId, Epoch)`, so every version of a logical row coexists (an
//! update inserts a new key; the old version is untouched until compaction). A
//! Bε-tree buffers many pending mutations per internal node; when a buffer
//! fills, its messages flush to one child in bulk, giving write amplification
//! approaching O(1). Reads consult every buffer along the root→leaf path and
//! return the newest version with `epoch <= snapshot`.
//!
//! This is a drop-in MVCC alternative to the skip-list [`crate::Memtable`]; the
//! engine ships the skip-list today because it is simpler, while this structure
//! wins on update amplification at scale.

use crate::epoch::Epoch;
use crate::memtable::Row;
use crate::rowid::RowId;
use std::collections::HashMap;

/// Max children per internal node (`B`).
const FANOUT: usize = 8;
/// Messages buffered per internal node before flushing down to children.
const BUFFER_CAP: usize = 16;
/// Max rows per leaf before it splits.
const LEAF_CAP: usize = 32;

/// Composite version key: `(row_id, epoch)`.
type VKey = (RowId, Epoch);

/// A pending mutation pending application to the leaves below a node.
#[derive(Debug, Clone)]
enum Message {
    Upsert(Row),
    Tombstone { row_id: RowId, epoch: Epoch },
}

impl Message {
    fn key(&self) -> VKey {
        match self {
            Message::Upsert(r) => (r.row_id, r.committed_epoch),
            Message::Tombstone { row_id, epoch } => (*row_id, *epoch),
        }
    }

    fn to_row(&self) -> (Epoch, Row) {
        match self {
            Message::Upsert(r) => (r.committed_epoch, r.clone()),
            Message::Tombstone { row_id, epoch } => (
                *epoch,
                Row {
                    row_id: *row_id,
                    committed_epoch: *epoch,
                    columns: HashMap::new(),
                    deleted: true,
                    commit_ts: None,
                },
            ),
        }
    }
}

#[derive(Clone)]
enum Node {
    Leaf {
        rows: Vec<Row>,
    },
    Internal {
        keys: Vec<VKey>,
        children: Vec<Node>,
        buffer: Vec<Message>,
    },
}

impl Node {
    fn empty_leaf() -> Self {
        Node::Leaf { rows: Vec::new() }
    }
}

struct Split {
    key: VKey,
    node: Node,
}

/// Buffered Bε-tree over `(RowId, Epoch)` → [`Row`].
#[derive(Clone)]
pub struct BeTree {
    root: Node,
    mutations: usize,
}

impl Default for BeTree {
    fn default() -> Self {
        Self::new()
    }
}

impl BeTree {
    pub fn new() -> Self {
        Self {
            root: Node::empty_leaf(),
            mutations: 0,
        }
    }

    /// Number of mutations buffered.
    pub fn mutations(&self) -> usize {
        self.mutations
    }

    pub fn is_empty(&self) -> bool {
        self.mutations == 0
    }

    /// Insert a row version (keyed by its own `(row_id, committed_epoch)`).
    pub fn insert_row(&mut self, row: Row) {
        self.insert(Message::Upsert(row));
    }

    /// Insert a tombstone at `(row_id, epoch)`.
    pub fn delete(&mut self, row_id: RowId, epoch: Epoch) {
        self.insert(Message::Tombstone { row_id, epoch });
    }

    fn insert(&mut self, msg: Message) {
        self.mutations += 1;
        match &mut self.root {
            Node::Leaf { rows } => Self::leaf_apply(rows, msg),
            Node::Internal { buffer, .. } => buffer.push(msg),
        }
        if let Some(split) = Self::maintain(&mut self.root) {
            let left = std::mem::replace(&mut self.root, Node::empty_leaf());
            self.root = Node::Internal {
                keys: vec![split.key],
                children: vec![left, split.node],
                buffer: Vec::new(),
            };
        }
    }

    /// Newest version of `row_id` with `epoch <= snapshot`, including tombstones
    /// (returned as a `Row` with `deleted=true`). `None` if no such version.
    pub fn get(&self, row_id: RowId, snapshot: Epoch) -> Option<Row> {
        self.get_version(row_id, snapshot).map(|(_, r)| r)
    }

    /// Same as [`Self::get`] but also returns the version's epoch — the shape
    /// the engine's MVCC merge needs to pick the newest version across the
    /// memtable, the mutable-run tier, and sorted runs.
    pub fn get_version(&self, row_id: RowId, snapshot: Epoch) -> Option<(Epoch, Row)> {
        let mut best: Option<(Epoch, Row)> = None;
        Self::collect(&self.root, row_id, snapshot, &mut best);
        best
    }

    /// Visible (non-deleted) row at `row_id` for `snapshot`.
    pub fn get_visible(&self, row_id: RowId, snapshot: Epoch) -> Option<Row> {
        let r = self.get(row_id, snapshot)?;
        if r.deleted {
            None
        } else {
            Some(r)
        }
    }

    /// Every buffered version (non-consuming), in no defined order — leaves
    /// plus every internal-node buffer. Used by the memtable adapter to dedup
    /// the newest visible version per `RowId` for a full visible-rows scan.
    pub fn versions(&self) -> Vec<Row> {
        let mut out = Vec::with_capacity(self.mutations);
        Self::collect_all_versions(&self.root, &mut out);
        out
    }

    /// Consume the tree, flushing all buffers to leaves, returning every version
    /// in ascending `(RowId, Epoch)` order.
    pub fn into_sorted_rows(mut self) -> Vec<Row> {
        Self::flush_all(&mut self.root);
        Self::collect_leaves(&self.root)
    }

    // ---- internals -----------------------------------------------------

    fn maintain(node: &mut Node) -> Option<Split> {
        match node {
            Node::Leaf { rows } => {
                if rows.len() > LEAF_CAP {
                    Some(Self::split_leaf(rows))
                } else {
                    None
                }
            }
            Node::Internal {
                keys,
                children,
                buffer,
            } => {
                if buffer.len() > BUFFER_CAP {
                    let drained = std::mem::take(buffer);
                    for msg in drained {
                        let i = Self::child_index(keys, msg.key());
                        Self::push_into_child(&mut children[i], msg);
                    }
                    let mut i = 0;
                    while i < children.len() {
                        if let Some(split) = Self::maintain(&mut children[i]) {
                            keys.insert(i, split.key);
                            children.insert(i + 1, split.node);
                            i += 1;
                        }
                        i += 1;
                    }
                }
                if children.len() > FANOUT {
                    Some(Self::split_internal(keys, children))
                } else {
                    None
                }
            }
        }
    }

    fn leaf_apply(rows: &mut Vec<Row>, msg: Message) {
        // Composite keys are unique ⇒ always an insert (never an overwrite).
        let key = msg.key();
        let row = match msg {
            Message::Upsert(r) => r,
            Message::Tombstone { row_id, epoch } => Row {
                row_id,
                committed_epoch: epoch,
                columns: HashMap::new(),
                deleted: true,
                commit_ts: None,
            },
        };
        let i = rows.partition_point(|r| (r.row_id, r.committed_epoch) < key);
        rows.insert(i, row);
    }

    fn push_into_child(child: &mut Node, msg: Message) {
        match child {
            Node::Leaf { rows } => Self::leaf_apply(rows, msg),
            Node::Internal { buffer, .. } => buffer.push(msg),
        }
    }

    fn child_index(keys: &[VKey], key: VKey) -> usize {
        keys.partition_point(|k| *k <= key)
    }

    fn split_leaf(rows: &mut Vec<Row>) -> Split {
        let mid = rows.len() / 2;
        let right = rows.split_off(mid);
        let key = (right[0].row_id, right[0].committed_epoch);
        Split {
            key,
            node: Node::Leaf { rows: right },
        }
    }

    fn split_internal(keys: &mut Vec<VKey>, children: &mut Vec<Node>) -> Split {
        let m = keys.len() / 2;
        let promoted = keys[m];
        let right_keys = keys.split_off(m + 1);
        keys.pop();
        let right_children = children.split_off(m + 1);
        Split {
            key: promoted,
            node: Node::Internal {
                keys: right_keys,
                children: right_children,
                buffer: Vec::new(),
            },
        }
    }

    fn consider(best: &mut Option<(Epoch, Row)>, epoch: Epoch, row: Row) {
        match best {
            Some((be, _)) if *be >= epoch => {}
            _ => *best = Some((epoch, row)),
        }
    }

    fn collect(node: &Node, row_id: RowId, snapshot: Epoch, best: &mut Option<(Epoch, Row)>) {
        match node {
            Node::Leaf { rows } => {
                // Versions of `row_id` are contiguous; scan the slice whose
                // (row_id, epoch) <= (row_id, snapshot) and row_id matches.
                let upper =
                    rows.partition_point(|r| (r.row_id, r.committed_epoch) <= (row_id, snapshot));
                let mut i = upper;
                while i > 0 {
                    let i2 = i - 1;
                    if rows[i2].row_id != row_id {
                        break;
                    }
                    let r = &rows[i2];
                    if r.committed_epoch <= snapshot {
                        Self::consider(best, r.committed_epoch, r.clone());
                    }
                    i = i2;
                }
            }
            Node::Internal {
                keys,
                children,
                buffer,
            } => {
                for msg in buffer.iter() {
                    let (rid, e) = msg.key();
                    if rid == row_id && e <= snapshot {
                        let (epoch, row) = msg.to_row();
                        Self::consider(best, epoch, row);
                    }
                }
                let i = Self::child_index(keys, (row_id, snapshot));
                Self::collect(&children[i], row_id, snapshot, best);
            }
        }
    }

    fn flush_all(node: &mut Node) {
        match node {
            Node::Leaf { .. } => {}
            Node::Internal {
                keys,
                children,
                buffer,
            } => {
                let drained = std::mem::take(buffer);
                for msg in drained {
                    let i = Self::child_index(keys, msg.key());
                    Self::push_into_child(&mut children[i], msg);
                }
                for c in children.iter_mut() {
                    Self::flush_all(c);
                }
            }
        }
    }

    fn collect_leaves(node: &Node) -> Vec<Row> {
        match node {
            Node::Leaf { rows } => rows.clone(),
            Node::Internal { children, .. } => {
                children.iter().flat_map(Self::collect_leaves).collect()
            }
        }
    }

    fn collect_all_versions(node: &Node, out: &mut Vec<Row>) {
        match node {
            Node::Leaf { rows } => out.extend(rows.iter().cloned()),
            Node::Internal {
                children, buffer, ..
            } => {
                for msg in buffer {
                    out.push(msg.to_row().1);
                }
                for c in children {
                    Self::collect_all_versions(c, out);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Value;

    fn val_row(id: u64, epoch: u64, v: i64) -> Row {
        Row::new(RowId(id), Epoch(epoch)).with_column(1, Value::Int64(v))
    }

    #[test]
    fn point_lookups_round_trip() {
        let mut t = BeTree::new();
        for i in 0..50u64 {
            t.insert_row(val_row(i, i, i as i64 * 10));
        }
        for i in 0..50u64 {
            let r = t.get_visible(RowId(i), Epoch(100)).expect("row present");
            assert_eq!(r.row_id, RowId(i));
            assert!(matches!(r.columns.get(&1), Some(Value::Int64(v)) if *v == i as i64 * 10));
        }
        assert!(t.get_visible(RowId(500), Epoch(100)).is_none());
    }

    #[test]
    fn many_inserts_force_depth_growth() {
        let mut t = BeTree::new();
        let n = 5_000u64;
        for i in 0..n {
            t.insert_row(val_row(i, i, i as i64));
        }
        for i in 0..n {
            assert!(
                t.get_visible(RowId(i), Epoch(n + 1)).is_some(),
                "missing {i}"
            );
        }
        assert_eq!(t.into_sorted_rows().len(), n as usize);
    }

    #[test]
    fn multiple_versions_of_same_row_coexist_with_mvcc() {
        // The whole point of the composite key: an update keeps the old version.
        let mut t = BeTree::new();
        t.insert_row(val_row(7, 1, 100));
        t.insert_row(val_row(7, 5, 200));
        // Old snapshot sees the old value; new snapshot sees the new value.
        let old = t.get_visible(RowId(7), Epoch(2)).unwrap();
        assert!(matches!(old.columns.get(&1), Some(Value::Int64(v)) if *v == 100));
        let new = t.get_visible(RowId(7), Epoch(10)).unwrap();
        assert!(matches!(new.columns.get(&1), Some(Value::Int64(v)) if *v == 200));
    }

    #[test]
    fn tombstone_hides_row_at_and_after_epoch_but_not_before() {
        let mut t = BeTree::new();
        t.insert_row(val_row(3, 1, 42));
        assert!(t.get_visible(RowId(3), Epoch(1)).is_some());
        t.delete(RowId(3), Epoch(4));
        assert!(t.get_visible(RowId(3), Epoch(4)).is_none());
        assert!(t.get_visible(RowId(3), Epoch(9)).is_none());
        // Still visible to a snapshot before the tombstone.
        assert!(t.get_visible(RowId(3), Epoch(3)).is_some());
    }

    #[test]
    fn into_sorted_rows_is_keyed_by_row_then_epoch() {
        let mut t = BeTree::new();
        t.insert_row(val_row(30, 1, 1));
        t.insert_row(val_row(10, 1, 1));
        t.insert_row(val_row(30, 5, 2)); // newer version of row 30
        t.delete(RowId(10), Epoch(2));
        let rows = t.into_sorted_rows();
        let keys: Vec<(u64, u64)> = rows
            .iter()
            .map(|r| (r.row_id.0, r.committed_epoch.0))
            .collect();
        assert_eq!(keys, vec![(10, 1), (10, 2), (30, 1), (30, 5)]);
        assert!(
            rows.iter()
                .find(|r| r.row_id == RowId(10) && r.committed_epoch == Epoch(2))
                .unwrap()
                .deleted
        );
    }

    /// Regression for the Phase 11 review's CRITICAL claim: when one row
    /// accumulates enough versions to span multiple leaf splits (and internal
    /// buffering), a `(row_id, snapshot)` point lookup must still return the
    /// newest visible version. This forces many splits and exercises the descent
    /// across child boundaries for a single high-churn row.
    #[test]
    fn many_versions_of_one_row_stay_lookupable_across_splits() {
        let mut t = BeTree::new();
        const N: u64 = 600;
        // Interleave other rows so the composite-key space has many separators;
        // the high-churn row is 7777, with a version at every epoch 0..N.
        for e in 0..N {
            t.insert_row(val_row(7777, e, e as i64 * 2));
            // A few distinct sibling rows to vary the key space and force
            // splits at separator keys that are NOT row 7777.
            t.insert_row(val_row(e, e, 0));
        }
        assert_eq!(t.mutations(), 2 * N as usize);
        // Every snapshot epoch must see exactly epoch `s` as the newest version
        // of row 7777 (versions are dense 0..N).
        for s in 0..N {
            let r = t
                .get_version(RowId(7777), Epoch(s))
                .expect("missing version")
                .0;
            assert_eq!(r, Epoch(s), "snapshot {s} saw wrong newest version");
        }
        // Before the first version: nothing.
        assert!(t.get_version(RowId(7777), Epoch(0)).is_some()); // epoch 0 exists
                                                                 // The sibling rows are all visible at their own epoch.
        for e in 0..N {
            assert!(t.get_visible(RowId(e), Epoch(e)).is_some(), "sibling {e}");
        }

        // Tombstones mixed in across splits: delete row 7777 at a late epoch,
        // then confirm snapshots before/after the tombstone see the right thing.
        t.delete(RowId(7777), Epoch(N + 5));
        assert!(
            t.get_visible(RowId(7777), Epoch(N)).is_some(),
            "before tombstone"
        );
        assert!(
            t.get_visible(RowId(7777), Epoch(N + 5)).is_none(),
            "at tombstone"
        );
        assert!(
            t.get_visible(RowId(7777), Epoch(N + 99)).is_none(),
            "after tombstone"
        );
    }
}
