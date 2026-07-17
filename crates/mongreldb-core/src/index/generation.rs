//! S1C-002: atomically-published index generations.
//!
//! Every index family in [`crate::index`] already uses the same physical
//! layout (S1C-003): an immutable base plus zero or more immutable frozen
//! deltas behind an `Arc<Vec<Arc<_>>>`, and one small active mutable delta
//! (see each family's `seal()`/`consolidate()`). This module formalizes that
//! layout as *generations*: after the writer seals its active deltas, the
//! per-column indexes are captured into an [`IndexGeneration`] — one
//! [`IndexFamilyGeneration`] per public index family — that is published
//! atomically (an `ArcSwap` swap in [`crate::engine::Table`]) so readers pin
//! an `Arc` and never coordinate with writers.
//!
//! Capturing is cheap by construction: a sealed index clone shares every
//! frozen layer `Arc` with the writer and copies only the (empty) active
//! delta plus the map shell. No write clones the complete index set merely
//! because readers exist; writers keep mutating only their small active
//! delta.
//!
//! `applied_through` is the engine's commit-epoch watermark ([`Epoch`]): the
//! generation reflects every commit whose epoch is `<= applied_through`. (The
//! spec's `HlcTimestamp` maps onto this watermark at the commit-log layer —
//! see `CommitReceipt::commit_ts`; the engine's visibility currency is the
//! epoch.)

use crate::epoch::Epoch;
use crate::index::{AnnIndex, BitmapIndex, ColumnLearnedRange, FmIndex, MinHashIndex, SparseIndex};
use std::collections::HashMap;
use std::sync::Arc;

/// One index family's published generation: the per-column index views,
/// structurally shared with the writer's frozen layers, plus the highest
/// commit epoch applied into them.
#[derive(Clone)]
pub struct IndexFamilyGeneration<T> {
    indexes: Arc<HashMap<u16, T>>,
    applied_through: Epoch,
}

impl<T> Default for IndexFamilyGeneration<T> {
    fn default() -> Self {
        Self::empty(Epoch(0))
    }
}

impl<T> IndexFamilyGeneration<T> {
    /// An empty generation (no per-column indexes yet).
    pub fn empty(applied_through: Epoch) -> Self {
        Self {
            indexes: Arc::new(HashMap::new()),
            applied_through,
        }
    }

    /// Capture by cloning the per-column map. Callers seal first, so each
    /// cloned index shares its frozen layers and carries an empty active
    /// delta — the clone is O(#columns), never O(#rows).
    pub(crate) fn capture(indexes: &HashMap<u16, T>, applied_through: Epoch) -> Self
    where
        T: Clone,
    {
        Self {
            indexes: Arc::new(indexes.clone()),
            applied_through,
        }
    }

    /// Capture by sharing an already-`Arc`-wrapped map (the learned-range
    /// family is rebuilt wholesale into a fresh `Arc`, so sharing is stable).
    pub(crate) fn share(indexes: Arc<HashMap<u16, T>>, applied_through: Epoch) -> Self {
        Self {
            indexes,
            applied_through,
        }
    }

    /// The index for `column_id`, if that column has one in this family.
    pub fn get(&self, column_id: u16) -> Option<&T> {
        self.indexes.get(&column_id)
    }

    /// Number of indexed columns in this family.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Highest commit epoch applied into these indexes.
    pub fn applied_through(&self) -> Epoch {
        self.applied_through
    }

    /// Iterate `(column_id, index)` pairs.
    pub fn iter(&self) -> impl Iterator<Item = (u16, &T)> + '_ {
        self.indexes
            .iter()
            .map(|(column_id, index)| (*column_id, index))
    }

    /// Column ids with an index in this family (unsorted map order).
    pub fn column_ids(&self) -> impl Iterator<Item = u16> + '_ {
        self.indexes.keys().copied()
    }
}

/// The six public index families, captured and published as one atomic
/// generation (S1C-002). Readers pin an `Arc<IndexGeneration>`; writers
/// publish a replacement with a single `ArcSwap` store.
#[derive(Clone, Default)]
pub struct IndexGeneration {
    bitmap: IndexFamilyGeneration<BitmapIndex>,
    range: IndexFamilyGeneration<ColumnLearnedRange>,
    fm: IndexFamilyGeneration<FmIndex>,
    ann: IndexFamilyGeneration<AnnIndex>,
    sparse: IndexFamilyGeneration<SparseIndex>,
    minhash: IndexFamilyGeneration<MinHashIndex>,
    applied_through: Epoch,
}

impl IndexGeneration {
    /// Capture one generation from the writer's (freshly sealed) per-family
    /// index maps at the `applied_through` watermark.
    pub(crate) fn capture(
        bitmap: &HashMap<u16, BitmapIndex>,
        range: &Arc<HashMap<u16, ColumnLearnedRange>>,
        fm: &HashMap<u16, FmIndex>,
        ann: &HashMap<u16, AnnIndex>,
        sparse: &HashMap<u16, SparseIndex>,
        minhash: &HashMap<u16, MinHashIndex>,
        applied_through: Epoch,
    ) -> Self {
        Self {
            bitmap: IndexFamilyGeneration::capture(bitmap, applied_through),
            range: IndexFamilyGeneration::share(Arc::clone(range), applied_through),
            fm: IndexFamilyGeneration::capture(fm, applied_through),
            ann: IndexFamilyGeneration::capture(ann, applied_through),
            sparse: IndexFamilyGeneration::capture(sparse, applied_through),
            minhash: IndexFamilyGeneration::capture(minhash, applied_through),
            applied_through,
        }
    }

    pub fn bitmap(&self) -> &IndexFamilyGeneration<BitmapIndex> {
        &self.bitmap
    }

    pub fn range(&self) -> &IndexFamilyGeneration<ColumnLearnedRange> {
        &self.range
    }

    pub fn fm(&self) -> &IndexFamilyGeneration<FmIndex> {
        &self.fm
    }

    pub fn ann(&self) -> &IndexFamilyGeneration<AnnIndex> {
        &self.ann
    }

    pub fn sparse(&self) -> &IndexFamilyGeneration<SparseIndex> {
        &self.sparse
    }

    pub fn minhash(&self) -> &IndexFamilyGeneration<MinHashIndex> {
        &self.minhash
    }

    /// Highest commit epoch applied into this generation. Every family is
    /// captured at the same watermark, so this equals each family's
    /// `applied_through`.
    pub fn applied_through(&self) -> Epoch {
        self.applied_through
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RowId;

    #[test]
    fn captured_generation_shares_frozen_layers_with_writer() {
        let mut writer = BitmapIndex::new();
        writer.insert(b"red".to_vec(), RowId(1));
        writer.seal();
        let mut map = HashMap::new();
        map.insert(7u16, writer.clone());
        let generation = IndexFamilyGeneration::capture(&map, Epoch(5));

        // Writes after the capture go to the writer's fresh active delta and
        // are invisible through the pinned generation.
        writer.insert(b"blue".to_vec(), RowId(2));
        let pinned = generation.get(7).expect("column 7 indexed");
        assert!(pinned.get(b"red").contains(1));
        assert!(pinned.get(b"blue").is_empty());
        assert!(writer.get(b"blue").contains(2));
        assert_eq!(generation.applied_through(), Epoch(5));
        assert_eq!(generation.len(), 1);
        assert_eq!(generation.column_ids().collect::<Vec<_>>(), vec![7]);
    }

    #[test]
    fn index_generation_capture_covers_all_families() {
        let mut bitmap = HashMap::new();
        bitmap.insert(1u16, BitmapIndex::new());
        let mut ann = HashMap::new();
        ann.insert(2u16, AnnIndex::new(8));
        let generation = IndexGeneration::capture(
            &bitmap,
            &Arc::new(HashMap::new()),
            &HashMap::new(),
            &ann,
            &HashMap::new(),
            &HashMap::new(),
            Epoch(11),
        );
        assert_eq!(generation.applied_through(), Epoch(11));
        assert_eq!(generation.bitmap().applied_through(), Epoch(11));
        assert!(generation.bitmap().get(1).is_some());
        assert!(generation.ann().get(2).is_some());
        assert!(generation.fm().is_empty());
        assert!(generation.range().is_empty());
        assert!(generation.sparse().is_empty());
        assert!(generation.minhash().is_empty());
    }
}
