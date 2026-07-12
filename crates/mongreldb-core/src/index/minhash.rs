//! MinHash / LSH set-similarity index (`IndexKind::MinHash`).
//!
//! Serves the set-similarity / dedup-join primitive sub-linearly. A column
//! declared with this index holds a set as a JSON array (the Kit's
//! `set_similarity` representation); at index time we tokenize each row's set,
//! hash the members to 64-bit token hashes, and reduce them to a fixed-width
//! MinHash **signature** (an unbiased estimator of Jaccard similarity). Rows are
//! bucketed by **LSH bands** so a query only has to score candidates that share
//! a band bucket, not the whole table.
//!
//! Results are *approximate* (LSH recall < 100%): the index returns a candidate
//! set ranked by estimated Jaccard. Callers that need exact top-k re-verify the
//! candidates against the stored sets (see the Kit's `set_similarity`).

use crate::rowid::RowId;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// Number of hash permutations in a signature (estimator resolution).
const NUM_PERM: usize = 128;
/// Number of LSH bands. `NUM_PERM / NUM_BANDS` rows per band. With 128/32 the
/// candidate threshold (P≈0.5) sits near Jaccard ≈ 0.42.
const NUM_BANDS: usize = 32;
const ROWS_PER_BAND: usize = NUM_PERM / NUM_BANDS;

/// Stable v1 hash for a string set member.
pub fn minhash_token_hash(token: &str) -> u64 {
    minhash_member_hash_v1(&serde_json::Value::String(token.into())).unwrap()
}

/// Stable, typed XXH3-64 hash contract for public raw-member queries and
/// persisted MinHash-derived index state.
pub fn minhash_member_hash_v1(member: &serde_json::Value) -> Result<u64, &'static str> {
    let mut canonical = Vec::new();
    match member {
        serde_json::Value::String(value) => {
            canonical.push(0x01);
            canonical.extend_from_slice(value.as_bytes());
        }
        serde_json::Value::Number(value) => {
            canonical.push(0x02);
            canonical.extend_from_slice(value.to_string().as_bytes());
        }
        serde_json::Value::Bool(value) => {
            canonical.extend_from_slice(&[0x03, u8::from(*value)]);
        }
        _ => return Err("set member must be a string, number, or boolean"),
    }
    Ok(xxhash_rust::xxh3::xxh3_64_with_seed(&canonical, 0))
}

/// Tokenize a set-valued column cell (a JSON array, or a JSON string holding
/// one — matching the Kit's `set_similarity` storage) into a deduplicated set
/// of token hashes. Non-array / unparseable cells yield the empty set.
pub fn token_hashes_from_bytes(bytes: &[u8]) -> Vec<u64> {
    let arr = match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(serde_json::Value::Array(a)) => a,
        Ok(serde_json::Value::String(s)) => match serde_json::from_str::<serde_json::Value>(&s) {
            Ok(serde_json::Value::Array(a)) => a,
            _ => return Vec::new(),
        },
        _ => return Vec::new(),
    };
    let mut set = HashSet::new();
    for member in arr {
        if let Ok(hash) = minhash_member_hash_v1(&member) {
            set.insert(hash);
        }
    }
    set.into_iter().collect()
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The `NUM_PERM` `(a, b)` permutation coefficients — fixed for all time so
/// signatures are stable across processes and across a rebuild-from-runs.
fn coeffs() -> &'static [(u64, u64); NUM_PERM] {
    static COEFFS: OnceLock<[(u64, u64); NUM_PERM]> = OnceLock::new();
    COEFFS.get_or_init(|| {
        let mut c = [(0u64, 0u64); NUM_PERM];
        for (i, slot) in c.iter_mut().enumerate() {
            let a = splitmix64(0xA5A5_0000 ^ (i as u64).wrapping_mul(2)) | 1;
            let b = splitmix64(0x5A5A_0000 ^ ((i as u64).wrapping_mul(2) + 1));
            *slot = (a, b);
        }
        c
    })
}

/// MinHash signature (`NUM_PERM` u32 mins) of a set of token hashes. `None` for
/// the empty set.
fn signature(token_hashes: &[u64]) -> Option<Vec<u32>> {
    if token_hashes.is_empty() {
        return None;
    }
    let coeffs = coeffs();
    let mut sig = vec![u32::MAX; NUM_PERM];
    for &h in token_hashes {
        for (i, &(a, b)) in coeffs.iter().enumerate() {
            let p = a.wrapping_mul(h).wrapping_add(b);
            let v = (p >> 32) as u32;
            if v < sig[i] {
                sig[i] = v;
            }
        }
    }
    Some(sig)
}

/// LSH bucket key for band `b` of a signature.
fn band_key(b: usize, sig: &[u32]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (b as u64).hash(&mut h);
    let lo = b * ROWS_PER_BAND;
    sig[lo..lo + ROWS_PER_BAND].hash(&mut h);
    h.finish()
}

#[derive(Default)]
pub struct MinHashIndex {
    /// Per-row signatures, in insertion order.
    sigs: Vec<(RowId, Vec<u32>)>,
    /// LSH band bucket → indices into `sigs`. Derived from `sigs`; rebuilt on
    /// restore rather than checkpointed.
    buckets: HashMap<u64, Vec<u32>>,
}

impl MinHashIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index a row's set (as token hashes). Empty sets are skipped.
    pub fn insert(&mut self, token_hashes: &[u64], row_id: RowId) {
        let Some(sig) = signature(token_hashes) else {
            return;
        };
        let idx = self.sigs.len() as u32;
        for b in 0..NUM_BANDS {
            self.buckets.entry(band_key(b, &sig)).or_default().push(idx);
        }
        self.sigs.push((row_id, sig));
    }

    /// Candidate row ids for a query set, ranked by estimated Jaccard (highest
    /// first), truncated to `k`. Candidates are the rows sharing ≥1 LSH band
    /// bucket with the query — a sub-linear subset of the table.
    pub fn search(&self, query_token_hashes: &[u64], k: usize) -> Vec<(RowId, f32)> {
        let Some(qsig) = signature(query_token_hashes) else {
            return Vec::new();
        };
        let mut candidates: HashSet<u32> = HashSet::new();
        for b in 0..NUM_BANDS {
            if let Some(v) = self.buckets.get(&band_key(b, &qsig)) {
                candidates.extend(v.iter().copied());
            }
        }
        let mut scored: Vec<(RowId, f32)> = candidates
            .into_iter()
            .map(|idx| {
                let (rid, sig) = &self.sigs[idx as usize];
                let matches = sig.iter().zip(&qsig).filter(|(a, b)| a == b).count();
                (*rid, matches as f32 / NUM_PERM as f32)
            })
            .collect();
        scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(k);
        scored
    }

    pub fn is_empty(&self) -> bool {
        self.sigs.is_empty()
    }

    /// Snapshot the signatures for checkpointing (buckets are derived).
    pub fn entries(&self) -> Vec<(RowId, Vec<u32>)> {
        self.sigs.clone()
    }

    /// Rebuild from a snapshot produced by [`MinHashIndex::entries`].
    pub fn from_entries(entries: Vec<(RowId, Vec<u32>)>) -> Self {
        let mut idx = Self {
            sigs: Vec::with_capacity(entries.len()),
            buckets: HashMap::new(),
        };
        for (rid, sig) in entries {
            let i = idx.sigs.len() as u32;
            for b in 0..NUM_BANDS {
                idx.buckets.entry(band_key(b, &sig)).or_default().push(i);
            }
            idx.sigs.push((rid, sig));
        }
        idx
    }
}

/// Checkpoint payload type (kept explicit for the global-index serde).
pub type MinHashEntries = Vec<(RowId, Vec<u32>)>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_typed_hash_vectors() {
        assert_eq!(minhash_token_hash("1"), 4_601_219_942_126_179_299);
        assert_eq!(
            minhash_member_hash_v1(&serde_json::json!(1)).unwrap(),
            6_001_628_596_940_409_521
        );
        assert_eq!(
            minhash_member_hash_v1(&serde_json::json!(true)).unwrap(),
            16_169_524_375_275_942_869
        );
        assert_ne!(
            minhash_token_hash("1"),
            minhash_member_hash_v1(&serde_json::json!(1)).unwrap()
        );
        assert_ne!(
            minhash_token_hash("true"),
            minhash_member_hash_v1(&serde_json::json!(true)).unwrap()
        );
    }

    fn set(tokens: &[&str]) -> Vec<u64> {
        tokens.iter().map(|t| minhash_token_hash(t)).collect()
    }

    #[test]
    fn similar_sets_are_candidates_and_rank_by_jaccard() {
        let mut idx = MinHashIndex::new();
        idx.insert(&set(&["a", "b", "c", "d"]), RowId(1)); // identical to query
        idx.insert(&set(&["a", "b", "c", "e"]), RowId(2)); // 3/5 overlap
        idx.insert(&set(&["x", "y", "z", "w"]), RowId(3)); // disjoint
                                                           // A near-identical big set that shares no *band* is still fine to miss;
                                                           // the identical one must always be found.
        let hits = idx.search(&set(&["a", "b", "c", "d"]), 10);
        let ids: Vec<u64> = hits.iter().map(|(r, _)| r.0).collect();
        assert!(ids.contains(&1), "identical set must be a candidate");
        // The identical set ranks first with estimate ~1.0.
        assert_eq!(hits[0].0, RowId(1));
        assert!(hits[0].1 > 0.95);
        // The disjoint set should not outrank the overlapping ones if present.
        assert!(!ids.contains(&3) || hits.last().unwrap().0 == RowId(3));
    }

    #[test]
    fn checkpoint_roundtrip_preserves_search() {
        let mut idx = MinHashIndex::new();
        idx.insert(&set(&["a", "b", "c", "d"]), RowId(1));
        idx.insert(&set(&["a", "b", "c", "e"]), RowId(2));
        let restored = MinHashIndex::from_entries(idx.entries());
        let a = idx.search(&set(&["a", "b", "c", "d"]), 5);
        let b = restored.search(&set(&["a", "b", "c", "d"]), 5);
        assert_eq!(a, b);
    }

    #[test]
    fn tokenizes_json_array_bytes() {
        let direct = token_hashes_from_bytes(br#"["a","b","c"]"#);
        assert_eq!(direct.len(), 3);
        // A JSON string holding an array is also accepted.
        let quoted = token_hashes_from_bytes(br#""[\"a\",\"b\",\"c\"]""#);
        assert_eq!(quoted.len(), 3);
        // Order-independent: same set → same hashes.
        let mut a = direct.clone();
        let mut b = token_hashes_from_bytes(br#"["c","b","a"]"#);
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b);
    }
}
