//! Phase 15.5 — Arrow IPC read-cache shadow.
//!
//! A derived, disposable, zero-copy decode of a sorted run's columnar data,
//! stored as Arrow IPC alongside the `.sr` file. On a scan of a clean single-run
//! table, the shadow provides `RecordBatch`es directly from the mmap'd IPC file
//! — no `NativeColumn` allocation, no per-element conversion.
//!
//! **Lifecycle:** the shadow is keyed by run-id. It is written lazily on the
//! first full scan and deleted when the run is removed (compaction/GC). The
//! `.sr` run stays the source of truth; a missing/stale shadow falls through to
//! the normal decode path.
//!
//! **MVCC:** the shadow is a raw decode of the run, not a visibility filter.
//! For clean runs (`RUN_FLAG_CLEAN`), all rows are visible → the shadow IS the
//! final result. For versioned runs, the caller must still apply visibility on
//! top (the shadow path is only used for clean runs).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::record_batch::RecordBatch;

const SHADOW_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Manages Arrow IPC shadow files (`<table_dir>/_shadow/r-<run_id>.arrow`).
pub struct ArrowShadow {
    dir: PathBuf,
}

impl ArrowShadow {
    pub fn new(table_dir: &Path) -> Self {
        let dir = table_dir.join("_shadow");
        let _ = std::fs::create_dir_all(&dir);
        Self { dir }
    }

    fn shadow_path(&self, run_id: u128) -> PathBuf {
        self.dir.join(format!("r-{run_id}.arrow"))
    }

    /// Try reading the shadow for `run_id`. Returns `None` on miss or error
    /// (the normal decode path is the fallback).
    pub fn try_read(&self, run_id: u128) -> Option<RecordBatch> {
        let path = self.shadow_path(run_id);
        if std::fs::metadata(&path).ok()?.len() > SHADOW_MAX_BYTES {
            return None;
        }
        let file = std::fs::File::open(&path).ok()?;
        let reader = FileReader::try_new(file, None).ok()?;
        let batches: Vec<RecordBatch> = reader
            .into_iter()
            .collect::<arrow::error::Result<Vec<_>>>()
            .ok()?;
        if batches.is_empty() {
            return None;
        }
        if batches.len() == 1 {
            return Some(batches.into_iter().next().unwrap());
        }
        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, &batches).ok()
    }

    /// Write `batch` as the shadow for `run_id` (atomic: write tmp + rename).
    /// Best-effort: silently ignores errors.
    pub fn write(&self, run_id: u128, batch: &RecordBatch) {
        let path = self.shadow_path(run_id);
        let tmp = path.with_extension("tmp");
        let write = || -> std::io::Result<()> {
            let file = std::fs::File::create(&tmp)?;
            let mut writer = FileWriter::try_new(file, batch.schema().as_ref())
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok(())
        };
        if write().is_ok() {
            let _ = std::fs::rename(&tmp, &path);
        } else {
            let _ = std::fs::remove_file(&tmp);
        }
    }

    /// Delete shadows whose run-id is not in `live_run_ids` (lazy GC).
    pub fn sweep(&self, live_run_ids: &HashSet<u128>) {
        let Ok(entries) = std::fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
                let _ = std::fs::remove_file(&path);
                continue;
            }
            if let Some(id_str) = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|s| s.strip_prefix("r-"))
                .and_then(|s| s.strip_suffix(".arrow"))
            {
                if let Ok(id) = id_str.parse::<u128>() {
                    if !live_run_ids.contains(&id) {
                        let _ = std::fs::remove_file(&path);
                    }
                }
            }
        }
    }
}
