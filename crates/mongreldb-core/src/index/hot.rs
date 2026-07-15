//! Height-Optimized Trie — the planned in-memory primary-key index.
//!
//! HOT has fewer pointer chases and better cache behavior than a plain ART. The
//! real trie compaction is a Phase-2 deliverable; this module ships a correct
//! `BTreeMap`-backed stand-in with the same surface so the engine can run end
//! to end today.

use crate::rowid::RowId;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

/// Primary-key index: `key bytes → RowId`.
#[derive(Clone)]
struct HotSegment {
    inserted: BTreeMap<Vec<u8>, RowId>,
    removed: HashSet<Vec<u8>>,
}

#[derive(Clone)]
pub struct HotIndex {
    frozen: Arc<Vec<Arc<HotSegment>>>,
    active: HotSegment,
}

impl Default for HotIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl HotIndex {
    pub fn new() -> Self {
        Self {
            frozen: Arc::new(Vec::new()),
            active: HotSegment {
                inserted: BTreeMap::new(),
                removed: HashSet::new(),
            },
        }
    }

    /// Map (or re-map) `key` to `row_id`. Last writer wins.
    pub fn insert(&mut self, key: Vec<u8>, row_id: RowId) {
        self.active.removed.remove(&key);
        self.active.inserted.insert(key, row_id);
    }

    pub fn get(&self, key: &[u8]) -> Option<RowId> {
        if let Some(row_id) = self.active.inserted.get(key) {
            return Some(*row_id);
        }
        if self.active.removed.contains(key) {
            return None;
        }
        for segment in self.frozen.iter().rev() {
            if let Some(row_id) = segment.inserted.get(key) {
                return Some(*row_id);
            }
            if segment.removed.contains(key) {
                return None;
            }
        }
        None
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<RowId> {
        let previous = self.get(key);
        self.active.inserted.remove(key);
        self.active.removed.insert(key.to_vec());
        previous
    }

    pub fn len(&self) -> usize {
        self.entries().len()
    }

    pub fn is_empty(&self) -> bool {
        self.active.inserted.is_empty() && self.active.removed.is_empty() && self.frozen.is_empty()
    }

    /// Snapshot the `(key, row_id)` pairs for checkpointing to `_idx/global.idx`.
    pub fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        let mut entries = BTreeMap::new();
        for segment in self
            .frozen
            .iter()
            .map(Arc::as_ref)
            .chain(std::iter::once(&self.active))
        {
            for key in &segment.removed {
                entries.remove(key);
            }
            entries.extend(
                segment
                    .inserted
                    .iter()
                    .map(|(key, row_id)| (key.clone(), *row_id)),
            );
        }
        entries.into_iter().collect()
    }

    /// Rebuild from a snapshot produced by [`HotIndex::entries`] (itself
    /// already ascending — a `BTreeMap` iterates in key order). `.collect()`
    /// drives `BTreeMap`'s bulk-build `FromIterator`, which is dramatically
    /// faster than the equivalent one-at-a-time `insert()` loop for a large,
    /// already-sorted checkpoint (the common case: this is on the
    /// `Table::open` hot path, reloading a persisted `_idx/global.idx`).
    pub fn from_entries(entries: Vec<(Vec<u8>, RowId)>) -> Self {
        Self {
            frozen: Arc::new(Vec::new()),
            active: HotSegment {
                inserted: entries.into_iter().collect(),
                removed: HashSet::new(),
            },
        }
    }

    pub(crate) fn seal(&mut self) {
        if self.active.inserted.is_empty() && self.active.removed.is_empty() {
            return;
        }
        let active = std::mem::replace(
            &mut self.active,
            HotSegment {
                inserted: BTreeMap::new(),
                removed: HashSet::new(),
            },
        );
        Arc::make_mut(&mut self.frozen).push(Arc::new(active));
    }

    pub(crate) fn frozen_layer_count(&self) -> usize {
        self.frozen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_get() {
        let mut h = HotIndex::new();
        h.insert(b"alice".to_vec(), RowId(1));
        h.insert(b"bob".to_vec(), RowId(2));
        assert_eq!(h.get(b"alice"), Some(RowId(1)));
        assert_eq!(h.get(b"bob"), Some(RowId(2)));
        assert_eq!(h.get(b"carol"), None);
        h.insert(b"alice".to_vec(), RowId(9));
        assert_eq!(h.get(b"alice"), Some(RowId(9)));
    }
}
