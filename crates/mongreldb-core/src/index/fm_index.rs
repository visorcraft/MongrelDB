//! Succinct FM-index over a text column — arbitrary substring search in
//! `O(m log σ)` with a representation ~the size of the text.
//!
//! Built from the concatenated documents (each followed by a reserved separator
//! byte, with a unique terminator), via: suffix array → BWT → `C` array → a
//! **wavelet matrix** over the BWT for `Occ(c, i) = rank`. Count uses backward
//! search; locate walks `LF` links to sampled suffix-array positions. An agent
//! composes `fm_contains(s) ∩ semsearch(e, k)` then re-ranks.

use crate::rowid::RowId;
use std::collections::HashSet;

const SA_SAMPLE_RATE: usize = 4;
/// Bits per rank block — the within-block scan is `O(BITS/64) = O(1)` words, so
/// `rank` is `O(1)` (was `O(i/64)`). 512 is the standard wavelet-tree block.
const RANK_BLOCK_BITS: usize = 512;
const RANK_BLOCK_WORDS: usize = RANK_BLOCK_BITS / 64;

/// A wavelet tree over byte symbols with `rank` (count of `c` in `[0, i)`).
struct WaveletTree {
    root: WtNode,
}

enum WtNode {
    Leaf,
    Inner {
        lo: u32,
        hi: u32,
        bits: Vec<u64>,
        len: usize,
        /// Cumulative one-count at each `RANK_BLOCK_BITS` boundary: `rank1(i)`
        /// = `rank_prefix[i / RANK_BLOCK_BITS]` + popcount of the ≤8 words inside
        /// the block up to `i`. Makes `rank` O(1) instead of O(i/64).
        rank_prefix: Vec<usize>,
        left: Box<WtNode>,
        right: Box<WtNode>,
    },
}

impl WaveletTree {
    fn build(symbols: &[u8]) -> Self {
        Self {
            root: build_node(symbols, 0, 256),
        }
    }

    /// Count of symbol `c` in `[0, i)`.
    fn rank(&self, c: u8, i: usize) -> usize {
        rank_node(&self.root, c as u32, i)
    }
}

fn build_node(symbols: &[u8], lo: u32, hi: u32) -> WtNode {
    if lo + 1 == hi {
        return WtNode::Leaf; // single-symbol range: all positions are this symbol
    }
    let mid = (lo + hi) / 2;
    let mut bits = vec![0u64; symbols.len().div_ceil(64)];
    let mut left_syms = Vec::new();
    let mut right_syms = Vec::new();
    for (i, &s) in symbols.iter().enumerate() {
        if (s as u32) < mid {
            left_syms.push(s);
        } else {
            right_syms.push(s);
            bits[i / 64] |= 1u64 << (i % 64);
        }
    }
    let rank_prefix = build_rank_prefix(&bits, symbols.len());
    WtNode::Inner {
        lo,
        hi,
        bits,
        len: symbols.len(),
        rank_prefix,
        left: Box::new(build_node(&left_syms, lo, mid)),
        right: Box::new(build_node(&right_syms, mid, hi)),
    }
}

/// Prefix popcount at every `RANK_BLOCK_BITS` boundary, for O(1) `rank1`.
fn build_rank_prefix(bits: &[u64], len: usize) -> Vec<usize> {
    let n_blocks = len.div_ceil(RANK_BLOCK_BITS) + 1;
    let mut prefix = Vec::with_capacity(n_blocks);
    prefix.push(0);
    let mut acc = 0usize;
    let n_words = bits.len();
    let total_blocks = len.div_ceil(RANK_BLOCK_BITS);
    for b in 0..total_blocks {
        let w_start = b * RANK_BLOCK_WORDS;
        let w_end = (w_start + RANK_BLOCK_WORDS).min(n_words);
        for w in &bits[w_start..w_end] {
            acc += w.count_ones() as usize;
        }
        prefix.push(acc);
    }
    prefix
}

/// `rank1` in `O(1)`: block prefix + popcount of the ≤`RANK_BLOCK_WORDS` words
/// inside the block up to `i`.
fn rank1(bits: &[u64], len: usize, rank_prefix: &[usize], i: usize) -> usize {
    let i = i.min(len);
    let block = i / RANK_BLOCK_BITS;
    let base = rank_prefix[block];
    let w_start = block * RANK_BLOCK_WORDS;
    let full = i / 64;
    let mut ones = base;
    for w in &bits[w_start..full.min(w_start + RANK_BLOCK_WORDS)] {
        ones += w.count_ones() as usize;
    }
    if i % 64 != 0 && full < bits.len() && (full >= w_start && full < w_start + RANK_BLOCK_WORDS) {
        ones += (bits[full] & ((1u64 << (i % 64)) - 1)).count_ones() as usize;
    }
    ones
}

fn rank_node(node: &WtNode, c: u32, i: usize) -> usize {
    match node {
        WtNode::Leaf => i,
        WtNode::Inner {
            lo,
            hi,
            bits,
            len,
            rank_prefix,
            left,
            right,
        } => {
            let mid = (lo + hi) / 2;
            let ones = rank1(bits, *len, rank_prefix, i);
            if c < mid {
                rank_node(left, c, i - ones)
            } else {
                rank_node(right, c, ones)
            }
        }
    }
}

/// The derived (rebuilt-on-demand) FM-index structures. Kept separate from the
/// source `docs` so [`FmIndex::insert`] can simply append + invalidate instead
/// of rebuilding the suffix array / BWT / wavelet tree on every single insert
/// (Phase 11.3: amortized incremental updates — the build runs lazily on the
/// first query after a batch of inserts, not once per row).
struct Built {
    doc_start: Vec<usize>,
    bwt: Vec<u8>,
    c: [usize; 256],
    wave: WaveletTree,
    sa_sample: Vec<Option<usize>>,
    n: usize,
}

pub struct FmIndex {
    docs: Vec<(Vec<u8>, RowId)>,
    /// `None` (dirty) when inserts have landed since the last build; rebuilt
    /// lazily by [`Self::ensure_built`]. The query methods take `&self`, so this
    /// uses a thread-safe interior-mutability cell — a `Mutex` (not `RefCell`)
    /// so that `FmIndex` (and by extension `Table`) is `Sync` and a `&Table`
    /// can be shared across read threads.
    built: parking_lot::Mutex<Option<Built>>,
}

impl Default for FmIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl FmIndex {
    pub fn new() -> Self {
        Self {
            docs: Vec::new(),
            built: parking_lot::Mutex::new(None),
        }
    }

    /// Insert a document. **Does not rebuild** — the index is marked dirty and
    /// rebuilt once on the next query, so a batch of inserts pays a single
    /// `O(text log text)` build instead of one per row.
    pub fn insert(&mut self, text: Vec<u8>, row_id: RowId) {
        self.docs.push((text, row_id));
        *self.built.lock() = None;
    }

    pub fn doc_count(&self) -> usize {
        self.docs.len()
    }

    /// Snapshot the source `(text, row_id)` documents for checkpointing to
    /// `_idx/global.idx`. The BWT/wavelet-tree/SA are derived, so they are
    /// rebuilt (deterministically) on load via [`FmIndex::from_docs`].
    pub fn docs(&self) -> Vec<(Vec<u8>, RowId)> {
        self.docs.clone()
    }

    /// Rebuild from a snapshot produced by [`FmIndex::docs`] (also used on
    /// reopen). The build is deferred to the first query — `from_docs` just
    /// installs the source docs.
    pub fn from_docs(docs: Vec<(Vec<u8>, RowId)>) -> Self {
        let mut idx = Self::new();
        idx.docs = docs;
        idx
    }

    /// Rebuild the derived structures now if inserts have dirtied the index.
    fn ensure_built(&self) {
        if self.built.lock().is_some() {
            return;
        }
        let b = self.build_inner();
        *self.built.lock() = Some(b);
    }

    /// Force a rebuild now (e.g. before serializing if a caller wants the
    /// structures warm). No-op when already built.
    pub fn flush_build(&self) {
        self.ensure_built();
    }

    fn build_inner(&self) -> Built {
        // Reserve two byte values not present in any document as separator and
        // terminator (terminator must be < separator and < all real symbols for
        // the suffix-array/BWT ordering to behave).
        let used: HashSet<u8> = self
            .docs
            .iter()
            .flat_map(|(t, _)| t.iter().copied())
            .collect();
        let mut reserved = (0u16..256).filter(|b| !used.contains(&(*b as u8)));
        let term = reserved.next().unwrap_or(0) as u8; // smallest unused ⇒ acts as '$'
        let sep = reserved.next().unwrap_or(1) as u8;

        let mut text = Vec::new();
        let mut doc_start = Vec::with_capacity(self.docs.len());
        for (t, _) in &self.docs {
            doc_start.push(text.len());
            text.extend_from_slice(t);
            text.push(sep);
        }
        text.push(term);
        let n = text.len();

        // Suffix array by sorting suffix offsets (naive; fine for prototype sizes).
        let mut sa: Vec<usize> = (0..n).collect();
        sa.sort_by(|&a, &b| text[a..].cmp(&text[b..]));

        // BWT: char preceding each suffix; terminator position → terminator.
        let mut bwt = Vec::with_capacity(n);
        for &suff in &sa {
            if suff == 0 {
                bwt.push(term);
            } else {
                bwt.push(text[suff - 1]);
            }
        }

        // C[c] = number of symbols strictly less than c in text.
        let mut freq = [0usize; 256];
        for &b in &text {
            freq[b as usize] += 1;
        }
        let mut c = [0usize; 256];
        let mut acc = 0;
        for s in 0..256 {
            c[s] = acc;
            acc += freq[s];
        }

        // Wavelet matrix over BWT, and sampled SA.
        let wave = WaveletTree::build(&bwt);
        let sa_sample: Vec<Option<usize>> = (0..n)
            .map(|i| {
                if i % SA_SAMPLE_RATE == 0 {
                    Some(sa[i])
                } else {
                    None
                }
            })
            .collect();

        Built {
            doc_start,
            bwt,
            c,
            wave,
            sa_sample,
            n,
        }
    }

    /// Backward search; returns `(lo, hi)` BWT-row range (count = `hi - lo`).
    fn backward(&self, pattern: &[u8]) -> (usize, usize) {
        self.ensure_built();
        let b = self.built.lock();
        let b = b.as_ref().expect("fm built");
        let mut lo = 0usize;
        let mut hi = b.n;
        for &c in pattern.iter().rev() {
            let occ_lo = b.wave.rank(c, lo);
            let occ_hi = b.wave.rank(c, hi);
            lo = b.c[c as usize] + occ_lo;
            hi = b.c[c as usize] + occ_hi;
            if lo >= hi {
                return (lo, lo);
            }
        }
        (lo, hi)
    }

    fn locate_range(&self, lo: usize, hi: usize) -> Vec<RowId> {
        self.ensure_built();
        let b = self.built.lock();
        let b = b.as_ref().expect("fm built");
        let mut hits: HashSet<u64> = HashSet::new();
        let mut out = Vec::new();
        for row in lo..hi {
            let tpos = Self::locate_row(b, row);
            // Map text position → document.
            let d = match b.doc_start.partition_point(|&o| o <= tpos) {
                0 => 0,
                k => k - 1,
            };
            if let Some((_, rid)) = self.docs.get(d) {
                if hits.insert(rid.0) {
                    out.push(*rid);
                }
            }
        }
        out
    }

    /// Walk sampled `LF` links from `row` to a sampled suffix-array position.
    fn locate_row(b: &Built, row: usize) -> usize {
        let mut r = row;
        let mut steps = 0usize;
        loop {
            if let Some(Some(pos)) = b.sa_sample.get(r) {
                return (*pos + steps) % b.n;
            }
            let c = b.bwt[r];
            r = b.c[c as usize] + b.wave.rank(c, r);
            steps += 1;
            if steps > b.n {
                return b.n; // safety; should not happen
            }
        }
    }

    /// Count documents containing `pattern` as a substring.
    pub fn count(&self, pattern: &[u8]) -> usize {
        if pattern.is_empty() {
            return self.docs.len();
        }
        let (lo, hi) = self.backward(pattern);
        if lo >= hi {
            return 0;
        }
        self.locate_range(lo, hi).len()
    }

    /// Row ids of documents containing `pattern` (deduplicated).
    pub fn locate(&self, pattern: &[u8]) -> Vec<RowId> {
        if pattern.is_empty() {
            return self.docs.iter().map(|(_, r)| *r).collect();
        }
        let (lo, hi) = self.backward(pattern);
        if lo >= hi {
            return Vec::new();
        }
        self.locate_range(lo, hi)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substring_count_and_locate() {
        let mut idx = FmIndex::new();
        idx.insert(b"the quick brown fox".to_vec(), RowId(1));
        idx.insert(b"the lazy dog".to_vec(), RowId(2));
        idx.insert(b"fox in socks".to_vec(), RowId(3));
        idx.insert(b"box of frogs".to_vec(), RowId(4));

        let mut fox = idx.locate(b"fox");
        fox.sort_by_key(|r| r.0);
        assert_eq!(fox, vec![RowId(1), RowId(3)]);
        assert_eq!(idx.count(b"the"), 2);
        // "ox" appears in fox, fox, box (socks has no "ox") → 3 docs.
        assert_eq!(idx.count(b"ox"), 3);
        assert_eq!(idx.count(b""), 4);
        assert_eq!(idx.count(b"missing"), 0);
    }

    #[test]
    fn matches_overlap_and_repeats() {
        let mut idx = FmIndex::new();
        idx.insert(b"aaaa".to_vec(), RowId(9)); // three "aa" occurrences, one doc
        assert_eq!(idx.count(b"aa"), 1);
        assert_eq!(idx.locate(b"aa"), vec![RowId(9)]);
    }

    #[test]
    fn lazy_inserts_rebuild_once_per_query_batch() {
        // A batch of inserts must NOT rebuild per row; the first query rebuilds
        // once and subsequent queries reuse the cached build until the next
        // insert. This is the Phase 11.3 amortized incremental update.
        let mut idx = FmIndex::new();
        for i in 0..200u64 {
            // No panic, no per-insert rebuild cost observable here — correctness
            // is that all 200 docs are queryable after the batch.
            idx.insert(format!("doc number {i}").into_bytes(), RowId(i));
        }
        assert_eq!(idx.doc_count(), 200);
        // First query triggers exactly one build.
        assert_eq!(idx.count(b"doc number"), 200);
        // Cross-check the substring "1" against a brute-force scan of the docs.
        let expected_ones = idx
            .docs()
            .iter()
            .filter(|(t, _)| t.windows(1).any(|w| w == b"1"))
            .count();
        let mut hits = idx.locate(b"1");
        hits.sort_by_key(|r| r.0);
        assert_eq!(hits.len(), expected_ones);
        // An empty pattern matches every document.
        assert_eq!(idx.count(b""), 200);
    }

    #[test]
    fn rank_is_correct_on_a_large_symbol_set() {
        // Exercises the O(1) blocked rank over many symbols: build the wavelet
        // tree directly over a long pseudo-random byte sequence and check rank
        // against a brute-force reference at several offsets.
        let n = 20_000usize;
        let symbols: Vec<u8> = (0..n as u32)
            .map(|i| (i.wrapping_mul(2654435761) >> 16) as u8)
            .collect();
        let wave = WaveletTree::build(&symbols);
        for &c in &[0u8, 1, 17, 64, 128, 200, 255] {
            let mut brute = 0usize;
            for (i, &s) in symbols.iter().enumerate() {
                assert_eq!(
                    wave.rank(c, i),
                    brute,
                    "rank({c}, {i}) mismatch at brute={brute}"
                );
                if s == c {
                    brute += 1;
                }
            }
            assert_eq!(wave.rank(c, n), brute, "rank at tail");
        }
    }
}
