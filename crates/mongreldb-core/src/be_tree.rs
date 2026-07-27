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
use std::borrow::Cow;
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
pub(crate) enum Message {
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
pub(crate) enum Node {
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

/// Versions examined between cooperative cancellation checkpoints inside the
/// lazy cursor (REM-C §7.11).
const CURSOR_CHECKPOINT_INTERVAL: usize = 256;

/// Test-visible counters for the lazy Bε-tree version cursor (REM-C §7.6).
///
/// Each cursor node keeps its own local counts; [`BeTreeVersionCursor::stats`]
/// aggregates the live cursor chain (subtrees that finish fold their counts
/// into the parent before being dropped, so the aggregate is complete).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)] // test instrumentation; not a stable public API
pub struct BeTreeVersionCursorStats {
    /// Live cursor frames (one per node on the current root→leaf path).
    pub active_frames: usize,
    /// Peak of `active_frames` over the cursor's lifetime.
    pub peak_active_frames: usize,
    /// Buffered messages currently owned by live frames. Bounded by tree
    /// height × node buffer capacity — never by total version count.
    pub buffered_messages_owned: usize,
    /// Peak of `buffered_messages_owned` over the cursor's lifetime.
    pub peak_buffered_messages_owned: usize,
    /// Versions copied into a tree-wide collection before streaming. The
    /// lazy cursor never precollects, so this is always zero; it exists to
    /// fail loudly if a full materialization ever comes back.
    pub total_versions_precollected: usize,
    /// Versions emitted by this cursor node (aggregated: plus descendants).
    pub versions_examined: usize,
    /// `ExecutionControl::checkpoint` calls made by this cursor chain.
    pub checkpoints: usize,
}

/// One buffered message prepared for the lazy merge at a single internal
/// node: the message row plus its insertion sequence within the node buffer,
/// which stabilizes the equal-key tie order (see [`BeTreeVersionCursor`]).
struct BufferedCursorItem<'a> {
    key: VKey,
    sequence: usize,
    row: Cow<'a, Row>,
}

fn message_as_cow(message: &Message) -> Cow<'_, Row> {
    match message {
        Message::Upsert(row) => Cow::Borrowed(row),
        Message::Tombstone { row_id, epoch } => Cow::Owned(Row {
            row_id: *row_id,
            committed_epoch: *epoch,
            columns: HashMap::new(),
            deleted: true,
            commit_ts: None,
        }),
    }
}

enum NodeCursorFrame<'a> {
    Leaf { rows: std::slice::Iter<'a, Row> },
    Internal(Box<InternalCursorFrame<'a>>),
}

struct InternalCursorFrame<'a> {
    children: &'a [Node],
    child_index: usize,
    /// The current child's stream produced its last version. The matching
    /// buffer group may still hold items and keeps draining until the
    /// frame advances to the next child.
    child_exhausted: bool,
    buffer_groups: Vec<Vec<BufferedCursorItem<'a>>>,
    current_child: Option<Box<BeTreeVersionCursor<'a>>>,
    current_buffer: std::vec::IntoIter<BufferedCursorItem<'a>>,
    pending_child: Option<Cow<'a, Row>>,
    pending_buffer: Option<Cow<'a, Row>>,
}

/// Lazy ordered cursor over every version reachable from a [`BeTree`] root
/// (REM-C §7.7–§7.11). Yields `Cow<'a, Row>` in ascending `(RowId, Epoch)`
/// order without ever collecting all versions: each internal node merges its
/// own bounded, per-child sorted buffer group with the live child cursor, and
/// child key ranges are ordered and non-overlapping, so processing children
/// left to right yields a globally ordered stream. Cursor-owned memory is
/// bounded by tree depth × node buffer capacity.
///
/// ## Equal-key determinism (§7.10)
///
/// Equal `(RowId, Epoch)` keys are emitted in **physical write order**
/// (oldest first): within a leaf, rows sit in insert-after-equal order; an
/// internal node's child subtree (older, already-flushed writes) drains
/// before its buffered messages (newer writes); ties inside one node buffer
/// resolve in insertion order (`sequence`). Read folds walk this stream
/// oldest-first and resolve exact-stamp ties to the *later* physical write
/// via `epoch::version_supersedes`, so a same-span create+delete stays dead
/// and a same-span delete+re-put stays live. The rule never depends on heap
/// addresses or timing, so repeated scans over the same tree yield identical
/// streams.
///
/// ## Cooperative cancellation (§7.11)
///
/// [`Self::next_controlled`] checkpoints the supplied `ExecutionControl`
/// before descending into a new child (which sorts that node's buffer
/// groups) and every [`CURSOR_CHECKPOINT_INTERVAL`] versions emitted per
/// frame. The plain [`Iterator`] impl runs the same traversal without a
/// control for callers that cannot cancel.
pub struct BeTreeVersionCursor<'a> {
    node: NodeCursorFrame<'a>,
    stats: BeTreeVersionCursorStats,
}

impl<'a> BeTreeVersionCursor<'a> {
    fn new(node: &'a Node) -> Self {
        match node {
            Node::Leaf { rows } => Self {
                node: NodeCursorFrame::Leaf { rows: rows.iter() },
                stats: BeTreeVersionCursorStats {
                    active_frames: 1,
                    peak_active_frames: 1,
                    ..BeTreeVersionCursorStats::default()
                },
            },
            Node::Internal {
                keys,
                children,
                buffer,
            } => {
                // Assign each buffered message to the one child that owns its
                // key, then sort only that bounded per-child subset (§7.9).
                let mut groups: Vec<Vec<BufferedCursorItem<'a>>> =
                    (0..children.len()).map(|_| Vec::new()).collect();
                for (sequence, message) in buffer.iter().enumerate() {
                    let child = BeTree::child_index(keys, message.key());
                    groups[child].push(BufferedCursorItem {
                        key: message.key(),
                        sequence,
                        row: message_as_cow(message),
                    });
                }
                for group in &mut groups {
                    group.sort_by_key(|item| (item.key, item.sequence));
                }
                let owned: usize = groups.iter().map(Vec::len).sum();
                Self {
                    node: NodeCursorFrame::Internal(Box::new(InternalCursorFrame {
                        children,
                        child_index: 0,
                        child_exhausted: false,
                        buffer_groups: groups,
                        current_child: None,
                        current_buffer: Vec::new().into_iter(),
                        pending_child: None,
                        pending_buffer: None,
                    })),
                    stats: BeTreeVersionCursorStats {
                        active_frames: 1,
                        peak_active_frames: 1,
                        buffered_messages_owned: owned,
                        peak_buffered_messages_owned: owned,
                        ..BeTreeVersionCursorStats::default()
                    },
                }
            }
        }
    }

    /// Next version in ascending `(RowId, Epoch)` order, observing
    /// cooperative cancellation (§7.11).
    #[doc(hidden)] // streaming-cursor plumbing; used by the memtable adapter and tests
    pub fn next_controlled(
        &mut self,
        control: &crate::ExecutionControl,
    ) -> crate::Result<Option<Cow<'a, Row>>> {
        self.advance(Some(control))
    }

    /// Aggregated cursor statistics: local counts plus every live descendant,
    /// with finished subtrees already folded into their ancestors.
    #[doc(hidden)] // test instrumentation; not a stable public API
    pub fn stats(&self) -> BeTreeVersionCursorStats {
        let mut out = self.stats;
        let (frames, buffered) = self.footprint();
        out.active_frames = frames;
        out.buffered_messages_owned = buffered;
        if let NodeCursorFrame::Internal(frame) = &self.node {
            if let Some(child) = &frame.current_child {
                let child = child.stats();
                out.versions_examined += child.versions_examined;
                out.checkpoints += child.checkpoints;
                out.total_versions_precollected += child.total_versions_precollected;
            }
        }
        out
    }

    fn advance(
        &mut self,
        control: Option<&crate::ExecutionControl>,
    ) -> crate::Result<Option<Cow<'a, Row>>> {
        match &mut self.node {
            NodeCursorFrame::Leaf { rows } => {
                let Some(row) = rows.next() else {
                    return Ok(None);
                };
                Self::note_version(&mut self.stats, control)?;
                Ok(Some(Cow::Borrowed(row)))
            }
            NodeCursorFrame::Internal(frame) => {
                Self::advance_internal_frame(frame, &mut self.stats, control)
            }
        }
    }

    fn advance_internal_frame(
        frame: &mut InternalCursorFrame<'a>,
        stats: &mut BeTreeVersionCursorStats,
        control: Option<&crate::ExecutionControl>,
    ) -> crate::Result<Option<Cow<'a, Row>>> {
        let InternalCursorFrame {
            children,
            child_index,
            child_exhausted,
            buffer_groups,
            current_child,
            current_buffer,
            pending_child,
            pending_buffer,
        } = frame;
        loop {
            if current_child.is_none() && !*child_exhausted {
                if *child_index >= children.len() {
                    return Ok(None);
                }
                // Checkpoint before descending into the new child — the
                // descent sorts that node's buffer groups (§7.11).
                Self::checkpoint(stats, control)?;
                *current_buffer = std::mem::take(&mut buffer_groups[*child_index]).into_iter();
                *current_child = Some(Box::new(BeTreeVersionCursor::new(&children[*child_index])));
                Self::refresh_peaks(buffer_groups, current_buffer, current_child, stats);
                continue;
            }
            if pending_child.is_none() && !*child_exhausted {
                let child = current_child.as_mut().expect("live child cursor");
                match child.advance(control)? {
                    Some(row) => *pending_child = Some(row),
                    None => {
                        Self::absorb_child(stats, child);
                        *current_child = None;
                        *child_exhausted = true;
                    }
                }
                Self::refresh_peaks(buffer_groups, current_buffer, current_child, stats);
                continue;
            }
            if pending_buffer.is_none() {
                if let Some(item) = current_buffer.next() {
                    *pending_buffer = Some(item.row);
                }
            }
            let take_buffer = match (pending_child.is_some(), pending_buffer.is_some()) {
                (false, false) => {
                    // This child range is fully drained; move to the next.
                    *child_index += 1;
                    *child_exhausted = false;
                    continue;
                }
                (true, false) => false,
                (false, true) => true,
                (true, true) => {
                    let child_key = pending_child
                        .as_ref()
                        .map(|row| (row.row_id, row.committed_epoch));
                    let buffer_key = pending_buffer
                        .as_ref()
                        .map(|row| (row.row_id, row.committed_epoch));
                    // Exact-key ties emit the child (older, already-flushed
                    // write) first; the buffered message (newer write) comes
                    // last so `version_supersedes` folds hand the tie to the
                    // later physical write.
                    buffer_key < child_key
                }
            };
            Self::note_version(stats, control)?;
            return Ok(if take_buffer {
                pending_buffer.take()
            } else {
                pending_child.take()
            });
        }
    }

    /// Live `(frames, owned buffered messages)` of the subtree rooted here.
    fn footprint(&self) -> (usize, usize) {
        match &self.node {
            NodeCursorFrame::Leaf { .. } => (1, 0),
            NodeCursorFrame::Internal(frame) => {
                let own: usize = frame.buffer_groups.iter().map(Vec::len).sum::<usize>()
                    + frame.current_buffer.len();
                let (child_frames, child_buffered) = frame
                    .current_child
                    .as_ref()
                    .map_or((0, 0), |child| child.footprint());
                (1 + child_frames, own + child_buffered)
            }
        }
    }

    /// Refresh `active`/`peak` footprint counters after the child cursor was
    /// created, advanced, or dropped. Because a parent refreshes after every
    /// child operation, the root's peaks are the true global peaks.
    fn refresh_peaks(
        buffer_groups: &[Vec<BufferedCursorItem<'a>>],
        current_buffer: &std::vec::IntoIter<BufferedCursorItem<'a>>,
        current_child: &Option<Box<BeTreeVersionCursor<'a>>>,
        stats: &mut BeTreeVersionCursorStats,
    ) {
        let own_buffered: usize =
            buffer_groups.iter().map(Vec::len).sum::<usize>() + current_buffer.len();
        let (child_frames, child_buffered, child_peak_frames, child_peak_buffered) =
            match current_child {
                Some(child) => {
                    let (frames, buffered) = child.footprint();
                    (
                        frames,
                        buffered,
                        child.stats.peak_active_frames,
                        child.stats.peak_buffered_messages_owned,
                    )
                }
                None => (0, 0, 0, 0),
            };
        stats.active_frames = 1 + child_frames;
        stats.buffered_messages_owned = own_buffered + child_buffered;
        stats.peak_active_frames = stats
            .peak_active_frames
            .max(stats.active_frames)
            .max(1 + child_peak_frames);
        stats.peak_buffered_messages_owned = stats
            .peak_buffered_messages_owned
            .max(stats.buffered_messages_owned)
            .max(own_buffered + child_peak_buffered);
    }

    /// Fold a finished child subtree's counts into the parent so the
    /// aggregate stays complete after the child is dropped.
    fn absorb_child(stats: &mut BeTreeVersionCursorStats, child: &BeTreeVersionCursor<'a>) {
        let child = child.stats();
        stats.versions_examined += child.versions_examined;
        stats.checkpoints += child.checkpoints;
        stats.total_versions_precollected += child.total_versions_precollected;
        // Footprint peaks were already folded by refresh_peaks while the
        // child was live.
    }

    fn checkpoint(
        stats: &mut BeTreeVersionCursorStats,
        control: Option<&crate::ExecutionControl>,
    ) -> crate::Result<()> {
        if let Some(control) = control {
            control.checkpoint()?;
            stats.checkpoints += 1;
        }
        Ok(())
    }

    fn note_version(
        stats: &mut BeTreeVersionCursorStats,
        control: Option<&crate::ExecutionControl>,
    ) -> crate::Result<()> {
        stats.versions_examined += 1;
        if stats
            .versions_examined
            .is_multiple_of(CURSOR_CHECKPOINT_INTERVAL)
        {
            Self::checkpoint(stats, control)?;
        }
        Ok(())
    }
}

impl<'a> Iterator for BeTreeVersionCursor<'a> {
    type Item = Cow<'a, Row>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.advance(None) {
            Ok(item) => item,
            Err(_) => unreachable!("the no-cancellation path cannot fail"),
        }
    }
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

    /// Visit every buffered version of `row_id`, in the same relative order as
    /// [`Self::versions`], without materializing the whole tree or descending
    /// into unrelated key ranges.
    ///
    /// A logical row can span several leaves because the physical key includes
    /// its epoch, so the range walk covers every child intersecting
    /// `(row_id, Epoch::ZERO)..=(row_id, Epoch(u64::MAX))`.
    pub(crate) fn visit_versions(&self, row_id: RowId, mut visit: impl FnMut(Row)) {
        Self::visit_row_versions(&self.root, row_id, &mut visit);
    }

    /// Lazy ascending stream over every version in the tree (REM-C). The
    /// cursor holds no tree-wide collection; see [`BeTreeVersionCursor`].
    #[doc(hidden)] // streaming-cursor plumbing; used by the memtable adapter and tests
    pub fn leaf_versions_iter(&self) -> BeTreeVersionCursor<'_> {
        BeTreeVersionCursor::new(&self.root)
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
        // Insert *after* any equal `(row_id, epoch)` keys: a same-span
        // create+delete produces two entries with the same composite key,
        // and their physical write order (live first, tombstone last) must
        // survive — the read folds resolve exact-stamp ties to the later
        // write (`epoch::version_supersedes`). For unique keys this is
        // identical to the previous `< key` partition point.
        let i = rows.partition_point(|r| (r.row_id, r.committed_epoch) <= key);
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
                // Newest buffered message first: with `consider`'s
                // first-on-tie rule this makes the *later physical write*
                // win an exact (row_id, epoch) tie — the tombstone for a
                // same-span create+delete, the live row for a same-span
                // delete+re-put. Buffer contents are physically newer than
                // anything already flushed into the child below.
                for msg in buffer.iter().rev() {
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

    fn visit_row_versions(node: &Node, row_id: RowId, visit: &mut impl FnMut(Row)) {
        match node {
            Node::Leaf { rows } => {
                let start = rows.partition_point(|row| row.row_id < row_id);
                let end = rows.partition_point(|row| row.row_id <= row_id);
                for row in &rows[start..end] {
                    visit(row.clone());
                }
            }
            Node::Internal {
                keys,
                children,
                buffer,
            } => {
                // Children (older, already-flushed writes) before the buffer
                // (newer writes), matching `collect_all_versions`: the whole
                // traversal yields equal `(row_id, epoch)` keys in physical
                // write order so `version_supersedes` folds hand the tie to
                // the later write.
                let first = Self::child_index(keys, (row_id, Epoch::ZERO));
                let last = Self::child_index(keys, (row_id, Epoch(u64::MAX)));
                for child in &children[first..=last] {
                    Self::visit_row_versions(child, row_id, visit);
                }
                for message in buffer {
                    if message.key().0 == row_id {
                        visit(message.to_row().1);
                    }
                }
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
                // Children (older, already-flushed writes) before the buffer
                // (newer, not yet flushed): keeps the whole traversal in
                // physical write order for equal `(row_id, epoch)` keys, so
                // stable re-sorts and write-order-sensitive consumers (run
                // writer, `version_supersedes` folds) see the tombstone of a
                // same-span create+delete after the live version.
                for c in children {
                    Self::collect_all_versions(c, out);
                }
                for msg in buffer {
                    out.push(msg.to_row().1);
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
        let expected = t
            .versions()
            .into_iter()
            .filter(|row| row.row_id == RowId(7777))
            .map(|row| row.committed_epoch)
            .collect::<Vec<_>>();
        let mut visited = Vec::new();
        t.visit_versions(RowId(7777), |row| {
            assert_eq!(row.row_id, RowId(7777));
            visited.push(row.committed_epoch);
        });
        assert_eq!(visited, expected);
        visited.sort_unstable();
        assert_eq!(visited, (0..N).map(Epoch).collect::<Vec<_>>());

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

    /// REM-C §7.13: an out-of-order insert sequence that leaves messages
    /// sitting in internal-node buffers must still stream in strictly
    /// ascending `(RowId, Epoch)` order.
    #[test]
    fn out_of_order_internal_buffers_emit_ascending() {
        let mut t = BeTree::new();
        const N: u64 = 20_000;
        // 7_919 is coprime with 20_000, so the multiply is a bijection:
        // unique RowIds in a scrambled insert order.
        for i in 0..N {
            let rid = (i * 7_919) % N;
            t.insert_row(val_row(rid, 1, rid as i64));
        }
        // Late tombstones: many remain in internal-node buffers.
        for rid in (0..N).step_by(97) {
            t.delete(RowId(rid), Epoch(2));
        }
        let expected = t.mutations();
        let mut cursor = t.leaf_versions_iter();
        let mut prev: Option<(u64, u64)> = None;
        let mut count = 0usize;
        for row in cursor.by_ref() {
            let key = (row.row_id.0, row.committed_epoch.0);
            if let Some(p) = prev {
                assert!(p < key, "stream must ascend strictly: {p:?} then {key:?}");
            }
            prev = Some(key);
            count += 1;
        }
        assert_eq!(count, expected, "every version exactly once");
        let stats = cursor.stats();
        assert!(
            stats.peak_buffered_messages_owned > 0,
            "fixture must exercise internal-node buffers"
        );
        assert!(
            stats.peak_active_frames >= 2,
            "fixture must grow internal levels"
        );
        assert_eq!(stats.total_versions_precollected, 0);
    }

    /// REM-C §7.10: exact `(RowId, Epoch)` ties emit in physical write order
    /// (oldest first) — leaf rows before the node's buffered messages, buffer
    /// contents in insertion order — and never by heap addresses. Read folds
    /// resolve the tie to the *last* emitted version (`version_supersedes`).
    #[test]
    fn equal_key_tie_is_deterministic() {
        let live_leaf = val_row(7, 5, 42);
        let buffered_upsert = val_row(7, 5, 43);
        let tree = BeTree {
            root: Node::Internal {
                keys: vec![(RowId(50), Epoch(1))],
                children: vec![
                    Node::Leaf {
                        rows: vec![live_leaf],
                    },
                    Node::Leaf {
                        rows: vec![val_row(60, 1, 1)],
                    },
                ],
                buffer: vec![
                    Message::Upsert(buffered_upsert),
                    Message::Tombstone {
                        row_id: RowId(7),
                        epoch: Epoch(5),
                    },
                ],
            },
            mutations: 4,
        };
        let collect = || {
            tree.leaf_versions_iter()
                .map(|row| (row.row_id.0, row.committed_epoch.0, row.deleted))
                .collect::<Vec<_>>()
        };
        let first = collect();
        let second = collect();
        assert_eq!(first, second, "tie order must not depend on iteration run");
        assert_eq!(
            first,
            vec![
                (7, 5, false), // leaf-resident live row (oldest write)
                (7, 5, false), // buffered upsert (sequence 0)
                (7, 5, true),  // buffered tombstone (sequence 1, newest write)
                (60, 1, false),
            ],
            "child rows precede buffered messages at equal keys; buffer in insertion order"
        );
    }

    /// REM-C §7.6/§7.7: the first version is available after examining only a
    /// bounded number of versions — no full-tree walk or sort stands between
    /// construction and the first row.
    #[test]
    fn lazy_cursor_first_row_is_bounded() {
        let mut t = BeTree::new();
        const N: u64 = 100_000;
        for i in 0..N {
            t.insert_row(val_row(i, 1, i as i64));
        }
        let mut cursor = t.leaf_versions_iter();
        let control = crate::ExecutionControl::new(None);
        let first = cursor
            .next_controlled(&control)
            .expect("controlled advance")
            .expect("non-empty tree");
        assert_eq!(first.row_id, RowId(0));
        let stats = cursor.stats();
        assert_eq!(stats.total_versions_precollected, 0);
        assert!(
            stats.versions_examined <= 2 * CURSOR_CHECKPOINT_INTERVAL,
            "first row must not wait for a tree scan: {}",
            stats.versions_examined
        );
        assert!(
            stats.peak_active_frames <= 64,
            "frames bounded by tree height: {}",
            stats.peak_active_frames
        );
        assert!(
            stats.peak_buffered_messages_owned <= 64 * (BUFFER_CAP + 1),
            "buffered messages bounded by height × BUFFER_CAP: {}",
            stats.peak_buffered_messages_owned
        );
    }

    /// REM-C §7.11: a cancelled control is observed during tree traversal,
    /// before any version streams.
    #[test]
    fn cancellation_is_observed_during_tree_traversal() {
        let mut t = BeTree::new();
        for i in 0..50_000u64 {
            t.insert_row(val_row(i, 1, i as i64));
        }
        let mut cursor = t.leaf_versions_iter();
        let control = crate::ExecutionControl::new(None);
        control.cancel(crate::CancellationReason::ClientRequest);
        let err = cursor
            .next_controlled(&control)
            .expect_err("cancelled control must stop the cursor");
        assert!(matches!(err, crate::MongrelError::Cancelled));
        assert_eq!(
            cursor.stats().versions_examined,
            0,
            "cancellation lands before the first version streams"
        );
    }
}
