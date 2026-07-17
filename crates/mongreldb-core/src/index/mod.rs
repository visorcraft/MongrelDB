//! Indexes. All share the [`crate::rowid::RowId`] space so multi-condition
//! queries intersect with SIMD bitmap ops.
//!
//! The primary-key path is the in-memory [`hot::HotIndex`] (PK → RowId) backed
//! by the on-disk learned [`pgm::LearnedIndex`] (RowId → page offset). Secondary
//! indexes are chosen per column via [`crate::schema::IndexKind`].

pub mod ann;
pub mod bitmap;
pub mod fm_index;
pub mod generation;
pub mod hnsw;
pub mod hot;
pub mod learned_range;
pub mod minhash;
pub mod pgm;
pub mod sparse;

pub use ann::AnnIndex;
pub use bitmap::BitmapIndex;
pub use fm_index::FmIndex;
pub use generation::{IndexFamilyGeneration, IndexGeneration};
pub use hot::HotIndex;
pub use learned_range::{ColumnLearnedRange, ColumnLearnedRangeSnapshot};
pub use minhash::{
    minhash_member_hash_v1, minhash_token_hash, token_hashes_from_bytes, MinHashIndex,
};
pub use pgm::LearnedIndex;
pub use sparse::SparseIndex;
