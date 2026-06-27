//! MongrelDB core — a log-structured columnar store with sub-ms writes, learned
//! indexes over a shared row-id space, page-level native encryption, an
//! MVCC-tagged content-addressed cache, and an AI-native access layer.
//!
//! See `DBPLAN.md` at the repository root for the full design and on-disk
//! layouts. Phase 0 (this crate) delivers the WAL + memtable + Bε-tree write
//! path plus the container formats and index scaffolds.

#![allow(clippy::module_inception)]

pub mod be_tree;
pub mod cache;
pub mod columnar;
pub mod compaction;
pub mod cursor;
pub mod encryption;
pub mod engine;
pub mod epoch;
pub mod error;
pub mod gc;
pub mod global_idx;
pub mod index;
pub mod manifest;
pub mod memtable;
pub mod mutable_run;
pub mod page;
pub mod pma;
pub mod query;
pub mod reservoir;
pub mod retention;
pub mod rowid;
pub mod schema;
pub mod sorted_run;
pub mod tsv;
pub mod wal;

pub use be_tree::BeTree;
pub use cache::PageCache;
pub use columnar::{decode_column, encode_column};
pub use cursor::{Cursor, MultiRunCursor, NativePageCursor};
pub use encryption::{Cipher, PlaintextCipher};
pub use engine::{
    AggState, ApproxAgg, ApproxResult, CachedAgg, ColumnStat, IncrementalAggResult, NativeAgg,
    NativeAggResult, Table,
};
pub use epoch::{Epoch, EpochAuthority, EpochClock, Snapshot};
pub use error::{MongrelError, Result};
pub use gc::{CheckReport, DoctorReport, GcReport};
pub use index::{
    AnnIndex, BitmapIndex, ColumnLearnedRange, FmIndex, HotIndex, LearnedIndex, SparseIndex,
};
pub use memtable::{Memtable, Row, Value};
pub use mutable_run::MutableRun;
pub use page::{CachedPage, Encoding, PageStat};
pub use query::{Condition, Query};
pub use reservoir::Reservoir;
pub use retention::{OwnedSnapshotGuard, SnapshotGuard, SnapshotRegistry};
pub use rowid::{RowId, RowIdAllocator};
pub use schema::{ColumnDef, ColumnFlags, IndexDef, IndexKind, Schema, TypeId};
pub use sorted_run::{
    read_column_dir, read_header, write_run, write_run_with, ColumnPayload, RunHeader, RunReader,
    RunSpec, RunWriter,
};
pub use wal::{Op, Record, Wal, WalReader};

#[cfg(feature = "encryption")]
pub use encryption::{AesCipher, ColumnKeyDescriptor, EncryptionDescriptor, Kek};
