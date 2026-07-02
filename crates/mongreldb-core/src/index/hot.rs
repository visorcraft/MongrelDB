//! Height-Optimized Trie — the planned in-memory primary-key index.
//!
//! HOT has fewer pointer chases and better cache behavior than a plain ART. The
//! real trie compaction is a Phase-2 deliverable; this module ships a correct
//! `BTreeMap`-backed stand-in with the same surface so the engine can run end
//! to end today.

use crate::rowid::RowId;
use std::collections::BTreeMap;

/// Primary-key index: `key bytes → RowId`.
pub struct HotIndex {
    inner: BTreeMap<Vec<u8>, RowId>,
}

impl Default for HotIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl HotIndex {
    pub fn new() -> Self {
        Self {
            inner: BTreeMap::new(),
        }
    }

    /// Map (or re-map) `key` to `row_id`. Last writer wins.
    pub fn insert(&mut self, key: Vec<u8>, row_id: RowId) {
        self.inner.insert(key, row_id);
    }

    pub fn get(&self, key: &[u8]) -> Option<RowId> {
        self.inner.get(key).copied()
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<RowId> {
        self.inner.remove(key)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Snapshot the `(key, row_id)` pairs for checkpointing to `_idx/global.idx`.
    pub fn entries(&self) -> Vec<(Vec<u8>, RowId)> {
        self.inner.iter().map(|(k, v)| (k.clone(), *v)).collect()
    }

    /// Rebuild from a snapshot produced by [`HotIndex::entries`] (itself
    /// already ascending — a `BTreeMap` iterates in key order). `.collect()`
    /// drives `BTreeMap`'s bulk-build `FromIterator`, which is dramatically
    /// faster than the equivalent one-at-a-time `insert()` loop for a large,
    /// already-sorted checkpoint (the common case: this is on the
    /// `Table::open` hot path, reloading a persisted `_idx/global.idx`).
    pub fn from_entries(entries: Vec<(Vec<u8>, RowId)>) -> Self {
        Self {
            inner: entries.into_iter().collect(),
        }
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
