//! Lazy, page-aware native-column cursor for streaming scans (Phase 6.2).
//!
//! [`NativePageCursor`] is the streaming source backing the SQL scan's
//! single-run fast path. It is built up front under the DB lock, where MVCC
//! visibility and predicate survivor resolution happen once; the cursor then
//! owns the run reader and lazily decodes **only the projected columns of pages
//! that contain survivors**, one page (batch) per [`NativePageCursor::next_batch`].
//!
//! Pages with no surviving rows are never decoded (page skipping), and projected
//! columns are decoded only as the consumer pulls (late materialization — a
//! `LIMIT` satisfied early stops paying the decode cost of later pages). The
//! cursor never uses page `min/max` for MVCC visibility; visibility comes from
//! the system columns (`RowId`/`Epoch`/deleted) resolved at build time.

use std::collections::HashSet;

use crate::columnar::{decode_page_native, NativeColumn};
use crate::error::Result;
use crate::schema::TypeId;
use crate::sorted_run::{RunReader, SYS_ROW_ID};

/// A forward streaming scan cursor over typed native columns. Implemented by
/// the single-run [`NativePageCursor`] (page-plan fast path) and the multi-run
/// [`MultiRunCursor`] (k-way merge by `RowId` across N runs — Phase 16.1). The
/// SQL scan holds a `Box<dyn Cursor>` so both layouts stream lazily instead of
/// materializing every row up front.
pub trait Cursor: Send {
    /// Decode the next batch of survivor rows as projected native columns, in
    /// ascending `RowId` order. `None` when the stream is exhausted.
    fn next_batch(&mut self) -> Result<Option<Vec<NativeColumn>>>;
    /// Exact count of surviving rows still to be yielded (without decoding).
    fn remaining_rows(&self) -> usize;
    /// The projected column types, in output order.
    fn projection_types(&self) -> Vec<TypeId>;
}

/// One page's worth of within-page survivor positions to decode.
#[derive(Clone)]
pub(crate) struct PagePlan {
    /// Page sequence number (0-based) across the run's PAX pages.
    pub(crate) seq: usize,
    /// Within-page row positions that survive MVCC + the predicate.
    pub(crate) positions: Vec<usize>,
}

/// A forward cursor over a single sorted run that yields the projected columns
/// of surviving rows, page by page. Built by [`crate::engine::Table`].
///
/// All MVCC visibility and predicate resolution is settled at construction
/// (the `PagePlan`s); [`Self::next_batch`] is pure lazy column decode + gather.
pub struct NativePageCursor {
    reader: RunReader,
    projection: Vec<(u16, TypeId)>,
    plans: Vec<PagePlan>,
    next: usize,
    /// Phase 13.1: pre-materialized columns from the memtable / mutable-run
    /// overlay, yielded as a single final batch after all page plans are
    /// drained. `None` when there is no overlay (clean single-run layout).
    overlay: Option<Vec<NativeColumn>>,
    /// Row count of the overlay batch (tracked separately so `remaining_rows`
    /// works even when `projection` is empty — the COUNT(*) path).
    overlay_rows: usize,
}

impl NativePageCursor {
    /// Build a cursor over `reader` with an optional overlay batch (Phase 13.1).
    /// The overlay — pre-materialized columns from the memtable / mutable-run
    /// tier — is yielded as a single final batch after all page plans. `None`
    /// for a clean single-run layout (no overlay).
    pub(crate) fn new_with_overlay(
        reader: RunReader,
        projection: Vec<(u16, TypeId)>,
        plans: Vec<PagePlan>,
        overlay: Option<Vec<NativeColumn>>,
    ) -> Self {
        let overlay_rows = overlay
            .as_ref()
            .map(|cols| cols.first().map(|c| c.len()).unwrap_or(0))
            .unwrap_or(0);
        Self {
            reader,
            projection,
            plans,
            next: 0,
            overlay,
            overlay_rows,
        }
    }

    /// The projected column types, in output order.
    pub fn projection_types(&self) -> Vec<TypeId> {
        self.projection.iter().map(|(_, t)| *t).collect()
    }

    /// Total surviving rows still to be yielded across all remaining plans plus
    /// the overlay (the scan's exact output row count, without decoding pages).
    pub fn remaining_rows(&self) -> usize {
        let pages: usize = self.plans[self.next..]
            .iter()
            .map(|p| p.positions.len())
            .sum();
        pages + self.overlay_rows
    }

    /// Decode the next surviving page's projected columns, gathered to that
    /// page's survivor positions. Returns `None` when no pages remain. The
    /// overlay batch (if any) is yielded as the final batch.
    pub fn next_batch(&mut self) -> Result<Option<Vec<NativeColumn>>> {
        while self.next < self.plans.len() {
            let plan = self.plans[self.next].clone();
            self.next += 1;
            if plan.positions.is_empty() {
                continue;
            }
            let nrows = self
                .reader
                .page_row_counts(SYS_ROW_ID)?
                .get(plan.seq)
                .copied()
                .unwrap_or(0);
            let mut cols = Vec::with_capacity(self.projection.len());
            for (cid, ty) in &self.projection {
                // Schema evolution: a column added via `add_column` after this
                // run was written is absent here, so decode all-null at the
                // survivor positions (mirroring RunReader::column_native).
                let col = if self.reader.has_column(*cid) {
                    let page = self.reader.read_page(*cid, plan.seq)?;
                    let decoded = decode_page_native(*ty, &page, nrows)?;
                    decoded.gather(&plan.positions)
                } else {
                    crate::columnar::null_native(*ty, plan.positions.len())
                };
                cols.push(col);
            }
            return Ok(Some(cols));
        }
        // Phase 13.1: yield the pre-materialized overlay batch (memtable /
        // mutable-run tier) as the final batch, then clear it. When the
        // projection is empty (COUNT(*) path) but the overlay has rows, emit
        // an empty-column batch carrying just the row count.
        if self.overlay_rows > 0 {
            self.overlay_rows = 0;
            if let Some(cols) = self.overlay.take() {
                return Ok(Some(cols));
            }
            // Empty projection: fabricate a zero-column batch with the right
            // row count so the caller's `RecordBatch` infers the count.
            return Ok(Some(Vec::new()));
        }
        Ok(None)
    }
}

impl Cursor for NativePageCursor {
    fn next_batch(&mut self) -> Result<Option<Vec<NativeColumn>>> {
        NativePageCursor::next_batch(self)
    }
    fn remaining_rows(&self) -> usize {
        NativePageCursor::remaining_rows(self)
    }
    fn projection_types(&self) -> Vec<TypeId> {
        NativePageCursor::projection_types(self)
    }
}

/// Number of survivor rows materialized per `next_batch` on the multi-run path.
/// Matches the encoded 65 536-row page size so a batch typically spans at most
/// a handful of pages across runs.
const MERGE_BATCH_ROWS: usize = 65_536;

/// One run's contribution to a [`MultiRunCursor`]: the run's owned survivors —
/// rows whose newest MVCC-visible version lives in *this* run (not shadowed by
/// the overlay or a newer run) that also satisfy the predicate — plus a lazily
/// decoded cache of the current page's projected columns.
pub(crate) struct RunStream {
    reader: RunReader,
    /// Owned survivors as `(row_id, page_seq, within_page_pos)`, ascending by
    /// `row_id` (runs are sorted by `RowId`, so this is also position order).
    survivors: Vec<(u64, usize, usize)>,
    head: usize,
    page_row_counts: Vec<usize>,
    /// Page seq currently decoded into `cur_cols` (`None` before the first decode).
    cur_page: Option<usize>,
    cur_cols: Vec<NativeColumn>,
}

impl RunStream {
    pub(crate) fn new(
        reader: RunReader,
        survivors: Vec<(u64, usize, usize)>,
        page_row_counts: Vec<usize>,
    ) -> Self {
        Self {
            reader,
            survivors,
            head: 0,
            page_row_counts,
            cur_page: None,
            cur_cols: Vec::new(),
        }
    }
}

/// A forward cursor over **multiple** sorted runs that yields the projected
/// columns of surviving rows via a k-way merge by `RowId` (Phase 16.1).
///
/// Cross-run MVCC resolution (newest visible version per `RowId`) and predicate
/// survivor resolution are settled at construction using only the cheap system
/// columns; `next_batch` then lazily decodes the projected data columns of just
/// the pages that own survivors, each page at most once. This generalizes the
/// single-run [`NativePageCursor`] to arbitrary run counts so multi-run tables
/// stream instead of fully materializing.
pub struct MultiRunCursor {
    streams: Vec<RunStream>,
    projection: Vec<(u16, TypeId)>,
    /// Min-merge heap of `(row_id, stream_index)` over each stream's next survivor.
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(u64, usize)>>,
    remaining: usize,
    overlay: Option<Vec<NativeColumn>>,
    overlay_rows: usize,
    overlay_done: bool,
}

impl MultiRunCursor {
    pub(crate) fn new(
        streams: Vec<RunStream>,
        projection: Vec<(u16, TypeId)>,
        heap: std::collections::BinaryHeap<std::cmp::Reverse<(u64, usize)>>,
        remaining: usize,
        overlay: Option<Vec<NativeColumn>>,
    ) -> Self {
        let overlay_rows = overlay
            .as_ref()
            .map(|cols| cols.first().map(|c| c.len()).unwrap_or(0))
            .unwrap_or(0);
        Self {
            streams,
            projection,
            heap,
            remaining,
            overlay,
            overlay_rows,
            overlay_done: false,
        }
    }

    fn decode_page(&mut self, sidx: usize, page_seq: usize) -> Result<()> {
        let ncols = self.projection.len();
        let stream = &mut self.streams[sidx];
        let nrows = stream.page_row_counts.get(page_seq).copied().unwrap_or(0);
        let mut cols = Vec::with_capacity(ncols);
        for (cid, ty) in &self.projection {
            let col = if stream.reader.has_column(*cid) {
                let page = stream.reader.read_page(*cid, page_seq)?;
                decode_page_native(*ty, &page, nrows)?
            } else {
                crate::columnar::null_native(*ty, nrows)
            };
            cols.push(col);
        }
        stream.cur_page = Some(page_seq);
        stream.cur_cols = cols;
        Ok(())
    }
}

impl Cursor for MultiRunCursor {
    fn projection_types(&self) -> Vec<TypeId> {
        self.projection.iter().map(|(_, t)| *t).collect()
    }

    fn remaining_rows(&self) -> usize {
        self.remaining + self.overlay_rows
    }

    fn next_batch(&mut self) -> Result<Option<Vec<NativeColumn>>> {
        // Phase 1 — k-way merge: pop survivors in ascending RowId order into
        // per-(stream, page) segments. No data-column decode here; only the
        // precomputed (page_seq, pos) is used.
        if !self.heap.is_empty() {
            let mut segments: Vec<(usize, usize, Vec<usize>)> = Vec::new();
            let mut count = 0usize;
            while count < MERGE_BATCH_ROWS {
                let Some(std::cmp::Reverse((_, sidx))) = self.heap.pop() else {
                    break;
                };
                let stream = &mut self.streams[sidx];
                if stream.head >= stream.survivors.len() {
                    continue;
                }
                let (_rid, page_seq, pos) = stream.survivors[stream.head];
                stream.head += 1;
                if let Some(last) = segments.last_mut() {
                    if last.0 == sidx && last.1 == page_seq {
                        last.2.push(pos);
                    } else {
                        segments.push((sidx, page_seq, vec![pos]));
                    }
                } else {
                    segments.push((sidx, page_seq, vec![pos]));
                }
                count += 1;
                self.remaining -= 1;
                if stream.head < stream.survivors.len() {
                    let next_rid = stream.survivors[stream.head].0;
                    self.heap.push(std::cmp::Reverse((next_rid, sidx)));
                }
            }

            // Phase 2 — gather: decode each segment's page (lazily, cached per
            // stream; segments are in ascending page order within a stream, so
            // each page decodes at most once) and gather its positions.
            let ncols = self.projection.len();
            if ncols == 0 {
                // COUNT(*) carries only a row count via a zero-column batch.
                return Ok(Some(Vec::new()));
            }
            let mut pieces: Vec<Vec<NativeColumn>> = vec![Vec::new(); ncols];
            for (sidx, page_seq, positions) in &segments {
                if self.streams[*sidx].cur_page != Some(*page_seq) {
                    self.decode_page(*sidx, *page_seq)?;
                }
                let cur_cols = &self.streams[*sidx].cur_cols;
                for j in 0..ncols {
                    pieces[j].push(cur_cols[j].gather(positions));
                }
            }
            let out: Vec<NativeColumn> = (0..ncols)
                .map(|j| NativeColumn::concat(&pieces[j]))
                .collect();
            return Ok(Some(out));
        }

        // Overlay (memtable / mutable-run tier) as the final batch.
        if !self.overlay_done && self.overlay_rows > 0 {
            self.overlay_done = true;
            self.overlay_rows = 0;
            if let Some(cols) = self.overlay.take() {
                return Ok(Some(cols));
            }
            return Ok(Some(Vec::new()));
        }
        Ok(None)
    }
}

/// Map each visible, survivor row position to its page and within-page offset,
/// dropping pages that end up with no survivors.
///
/// * `visible_positions` / `rids` — MVCC-visible rows and their `RowId`s
///   (from [`RunReader::visible_positions_with_rids`]).
/// * `page_row_counts` — PAX page row counts (from [`RunReader::page_row_counts`]).
/// * `survivors` — `None` for an unfiltered full scan, or the predicate-resolved
///   `RowId` set to intersect with the visible rows.
///
/// Pure indexing/arithmetic — no page bytes are read — so it is cheap to call
/// up front. Plans come out in ascending page order with within-page positions
/// ascending.
pub(crate) fn build_page_plans(
    visible_positions: &[usize],
    rids: &[i64],
    page_row_counts: &[usize],
    survivors: Option<&HashSet<u64>>,
) -> Vec<PagePlan> {
    debug_assert_eq!(visible_positions.len(), rids.len());
    // Cumulative page start offsets.
    let mut starts = Vec::with_capacity(page_row_counts.len());
    let mut acc = 0usize;
    for &r in page_row_counts {
        starts.push(acc);
        acc += r;
    }
    let mut by_page: std::collections::BTreeMap<usize, Vec<usize>> =
        std::collections::BTreeMap::new();

    let n = visible_positions.len();
    // `rids` is sorted ascending (runs are written `(RowId, Epoch)`-ordered and
    // visible positions are emitted in rid-ascending order). For a *selective*
    // predicate (few survivors of many visible rows) it is ~k·log n cheaper to
    // iterate the small survivor set and binary-search `rids` than to walk all
    // visible positions doing O(1) HashSet contains — this is the inverse of the
    // pre-16.3c loop and matches `query_columns_native`'s pattern. The factor 32
    // bounds log2(n) for run sizes up to ~4 B rows, so `k·32 < n` ⟺ `k·log n < n`.
    let selective = match survivors {
        Some(set) if n > 0 => (set.len() as u64).saturating_mul(32) < n as u64,
        _ => false,
    };
    if selective {
        let set = survivors.unwrap();
        for &s in set {
            let Ok(i) = rids.binary_search(&(s as i64)) else {
                continue; // survivor lives in the overlay, not this run
            };
            let global = visible_positions[i];
            let page_seq = match starts.partition_point(|&st| st <= global) {
                0 => continue,
                p => p - 1,
            };
            by_page
                .entry(page_seq)
                .or_default()
                .push(global - starts[page_seq]);
        }
    } else {
        for (i, &global) in visible_positions.iter().enumerate() {
            if let Some(set) = survivors {
                if !set.contains(&(rids[i] as u64)) {
                    continue;
                }
            }
            // Pages are contiguous; find the last page whose start <= global.
            let page_seq = match starts.partition_point(|&s| s <= global) {
                0 => continue,
                p => p - 1,
            };
            let within = global - starts[page_seq];
            by_page.entry(page_seq).or_default().push(within);
        }
    }
    by_page
        .into_iter()
        .map(|(seq, mut positions)| {
            positions.sort_unstable();
            PagePlan { seq, positions }
        })
        .collect()
}
