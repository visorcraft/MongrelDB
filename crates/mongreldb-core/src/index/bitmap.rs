//! Roaring-bitmap secondary index — `value bytes → row-id set`.
//!
//! Best for low-cardinality columns (equality, IN, GROUP BY). Multiple indexes
//! intersect with cheap SIMD bitmap ops in the shared [`RowId`] space.

use crate::rowid::RowId;
use roaring::RoaringBitmap;
use std::collections::HashMap;
use std::sync::Arc;

type BitmapLayer = HashMap<Vec<u8>, RoaringBitmap>;

/// `value → row-id set`. Values are type-aware encoded bytes (lexicographically
/// comparable), matching the encoding used for page min/max.
#[derive(Clone)]
pub struct BitmapIndex {
    frozen: Arc<Vec<Arc<BitmapLayer>>>,
    active: BitmapLayer,
}

impl Default for BitmapIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl BitmapIndex {
    pub fn new() -> Self {
        Self {
            frozen: Arc::new(Vec::new()),
            active: HashMap::new(),
        }
    }

    pub fn insert(&mut self, value: Vec<u8>, row_id: RowId) {
        // Roaring bitmaps address u32. The Phase-3 upgrade shards bitmaps by
        // the high 32 bits to cover the full u64 row-id space; until then we
        // require row ids < 2^32.
        let id32 = u32::try_from(row_id.0)
            .expect("bitmap index supports row_id < 2^32; shard-by-high-bits is a Phase-3 upgrade");
        self.active.entry(value).or_default().insert(id32);
    }

    /// The row-id set for `value` (empty if absent).
    pub fn get(&self, value: &[u8]) -> RoaringBitmap {
        let mut rows = self.active.get(value).cloned().unwrap_or_default();
        for layer in self.frozen.iter() {
            if let Some(layer_rows) = layer.get(value) {
                rows |= layer_rows;
            }
        }
        rows
    }

    /// Intersection of several sets — the workhorse of multi-condition queries.
    pub fn intersect(sets: &[RoaringBitmap]) -> RoaringBitmap {
        match sets {
            [] => RoaringBitmap::new(),
            [first, rest @ ..] => {
                let mut acc = first.clone();
                for s in rest {
                    acc &= s;
                }
                acc
            }
        }
    }

    pub fn value_count(&self) -> usize {
        self.keys().len()
    }

    /// All distinct values (keys) in this index — Phase 17.2 broadcast join.
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut keys = std::collections::HashSet::new();
        keys.extend(self.active.keys().cloned());
        for layer in self.frozen.iter() {
            keys.extend(layer.keys().cloned());
        }
        keys.into_iter().collect()
    }

    /// Snapshot `(value_bytes → serialized RoaringBitmap)` pairs for
    /// checkpointing to `_idx/global.idx`.
    pub fn entries(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.keys()
            .into_iter()
            .map(|k| {
                let v = self.get(&k);
                let mut bytes = Vec::new();
                v.serialize_into(&mut bytes)
                    .expect("roaring serialize is infallible for Vec");
                (k, bytes)
            })
            .collect()
    }

    /// Rebuild from a snapshot produced by [`BitmapIndex::entries`].
    pub fn from_entries(
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> std::result::Result<Self, &'static str> {
        let mut active = HashMap::new();
        for (k, bytes) in entries {
            let bm = RoaringBitmap::deserialize_from(&bytes[..]).map_err(|_| "bad bitmap bytes")?;
            active.insert(k, bm);
        }
        Ok(Self {
            frozen: Arc::new(Vec::new()),
            active,
        })
    }

    pub(crate) fn seal(&mut self) {
        if self.active.is_empty() {
            return;
        }
        let active = std::mem::take(&mut self.active);
        Arc::make_mut(&mut self.frozen).push(Arc::new(active));
        if self.frozen.len() >= crate::MAX_READ_GENERATION_LAYERS {
            self.consolidate();
        }
    }

    fn consolidate(&mut self) {
        let mut merged = HashMap::<Vec<u8>, RoaringBitmap>::new();
        for layer in self.frozen.iter() {
            for (key, rows) in layer.iter() {
                *merged.entry(key.clone()).or_default() |= rows;
            }
        }
        self.frozen = Arc::new(vec![Arc::new(merged)]);
    }

    #[cfg(test)]
    pub(crate) fn frozen_layer_count(&self) -> usize {
        self.frozen.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_and_intersect() {
        let mut color = BitmapIndex::new();
        color.insert(b"red".to_vec(), RowId(1));
        color.insert(b"red".to_vec(), RowId(3));
        color.insert(b"blue".to_vec(), RowId(3));

        let mut region = BitmapIndex::new();
        region.insert(b"us".to_vec(), RowId(1));
        region.insert(b"us".to_vec(), RowId(3));
        region.insert(b"eu".to_vec(), RowId(2));

        let red = color.get(b"red");
        let us = region.get(b"us");
        let both = BitmapIndex::intersect(&[red, us]);
        let ids: Vec<u32> = both.iter().collect();
        assert_eq!(ids, vec![1, 3]);
    }

    #[test]
    fn sealed_generations_share_bitmaps_and_consolidate() {
        let mut writer = BitmapIndex::new();
        for id in 0..crate::MAX_READ_GENERATION_LAYERS as u64 + 2 {
            writer.insert(b"all".to_vec(), RowId(id));
            writer.seal();
        }
        assert!(writer.frozen_layer_count() < crate::MAX_READ_GENERATION_LAYERS);
        let generation = writer.clone();
        writer.insert(b"new".to_vec(), RowId(99));
        assert!(generation.get(b"new").is_empty());
        assert!(writer.get(b"new").contains(99));
    }
}
