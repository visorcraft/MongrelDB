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

/// Number of hash permutations in a signature (estimator resolution).
const NUM_PERM: usize = 128;
/// Number of LSH bands. `NUM_PERM / NUM_BANDS` rows per band. With 128/32 the
/// candidate threshold (P≈0.5) sits near Jaccard ≈ 0.3826.
const NUM_BANDS: usize = 32;

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

fn coefficient(i: usize) -> (u64, u64) {
    let a = splitmix64(0xA5A5_0000 ^ (i as u64).wrapping_mul(2)) | 1;
    let b = splitmix64(0x5A5A_0000 ^ ((i as u64).wrapping_mul(2) + 1));
    (a, b)
}

/// MinHash signature (`NUM_PERM` u32 mins) of a set of token hashes. `None` for
/// the empty set.
fn signature(token_hashes: &[u64], permutations: usize) -> Option<Vec<u32>> {
    if token_hashes.is_empty() {
        return None;
    }
    let mut sig = vec![u32::MAX; permutations];
    for &h in token_hashes {
        for (i, slot) in sig.iter_mut().enumerate() {
            let (a, b) = coefficient(i);
            let p = a.wrapping_mul(h).wrapping_add(b);
            let v = (p >> 32) as u32;
            if v < *slot {
                *slot = v;
            }
        }
    }
    Some(sig)
}

/// LSH bucket key for band `b` of a signature.
fn band_key(b: usize, sig: &[u32], rows_per_band: usize) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    (b as u64).hash(&mut h);
    let lo = b * rows_per_band;
    sig[lo..lo + rows_per_band].hash(&mut h);
    h.finish()
}

pub struct MinHashIndex {
    permutations: usize,
    bands: usize,
    /// Per-row signatures, in insertion order.
    sigs: Vec<(RowId, Vec<u32>)>,
    /// LSH band bucket → indices into `sigs`. Derived from `sigs`; rebuilt on
    /// restore rather than checkpointed.
    buckets: HashMap<u64, Vec<u32>>,
}

impl MinHashIndex {
    pub fn new() -> Self {
        Self::with_options(NUM_PERM, NUM_BANDS)
    }

    pub fn with_options(permutations: usize, bands: usize) -> Self {
        assert!(permutations > 0 && bands > 0 && permutations % bands == 0);
        Self {
            permutations,
            bands,
            sigs: Vec::new(),
            buckets: HashMap::new(),
        }
    }

    /// Index a row's set (as token hashes). Empty sets are skipped.
    pub fn insert(&mut self, token_hashes: &[u64], row_id: RowId) {
        let Some(sig) = signature(token_hashes, self.permutations) else {
            return;
        };
        let idx = self.sigs.len() as u32;
        for b in 0..self.bands {
            self.buckets
                .entry(band_key(b, &sig, self.permutations / self.bands))
                .or_default()
                .push(idx);
        }
        self.sigs.push((row_id, sig));
    }

    /// Candidate row ids for a query set, ranked by estimated Jaccard (highest
    /// first), truncated to `k`. Candidates are the rows sharing ≥1 LSH band
    /// bucket with the query — a sub-linear subset of the table.
    pub fn search(&self, query_token_hashes: &[u64], k: usize) -> Vec<(RowId, f32)> {
        self.search_filtered(query_token_hashes, k, |_| true)
    }

    pub fn search_filtered(
        &self,
        query_token_hashes: &[u64],
        k: usize,
        allowed: impl Fn(RowId) -> bool,
    ) -> Vec<(RowId, f32)> {
        let Some(qsig) = signature(query_token_hashes, self.permutations) else {
            return Vec::new();
        };
        let mut candidates: HashSet<u32> = HashSet::new();
        for b in 0..self.bands {
            if let Some(v) = self
                .buckets
                .get(&band_key(b, &qsig, self.permutations / self.bands))
            {
                candidates.extend(v.iter().copied());
            }
        }
        let mut scored: Vec<(RowId, f32)> = candidates
            .into_iter()
            .filter_map(|idx| {
                let (rid, sig) = &self.sigs[idx as usize];
                if !allowed(*rid) {
                    return None;
                }
                let matches = sig.iter().zip(&qsig).filter(|(a, b)| a == b).count();
                Some((*rid, matches as f32 / self.permutations as f32))
            })
            .collect();
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.truncate(k);
        scored
    }

    pub fn search_with_context(
        &self,
        query_token_hashes: &[u64],
        k: usize,
        context: Option<&crate::query::AiExecutionContext>,
    ) -> crate::Result<Vec<(RowId, f32)>> {
        let Some(qsig) = signature(query_token_hashes, self.permutations) else {
            return Ok(Vec::new());
        };
        let mut candidates: HashSet<u32> = HashSet::new();
        for b in 0..self.bands {
            if let Some(context) = context {
                context.consume(1)?;
            }
            if let Some(indices) =
                self.buckets
                    .get(&band_key(b, &qsig, self.permutations / self.bands))
            {
                candidates.extend(indices.iter().copied());
            }
        }
        let mut scored = Vec::with_capacity(candidates.len().min(k));
        for chunk in candidates.into_iter().collect::<Vec<_>>().chunks(256) {
            if let Some(context) = context {
                context.consume(chunk.len())?;
            }
            for idx in chunk {
                let (rid, sig) = &self.sigs[*idx as usize];
                let matches = sig.iter().zip(&qsig).filter(|(a, b)| a == b).count();
                scored.push((*rid, matches as f32 / self.permutations as f32));
            }
        }
        scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        scored.truncate(k);
        Ok(scored)
    }

    pub fn candidate_row_ids(&self, query_token_hashes: &[u64]) -> Vec<RowId> {
        let Some(signature) = signature(query_token_hashes, self.permutations) else {
            return Vec::new();
        };
        let mut candidates = HashSet::new();
        for band in 0..self.bands {
            if let Some(indices) =
                self.buckets
                    .get(&band_key(band, &signature, self.permutations / self.bands))
            {
                candidates.extend(indices.iter().map(|index| self.sigs[*index as usize].0));
            }
        }
        candidates.into_iter().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.sigs.is_empty()
    }

    pub fn options(&self) -> (usize, usize) {
        (self.permutations, self.bands)
    }

    /// Snapshot the signatures for checkpointing (buckets are derived).
    pub fn entries(&self) -> Vec<(RowId, Vec<u32>)> {
        self.sigs.clone()
    }

    /// Rebuild from a snapshot produced by [`MinHashIndex::entries`].
    pub fn from_entries(entries: Vec<(RowId, Vec<u32>)>) -> Self {
        Self::from_entries_with_options(entries, NUM_PERM, NUM_BANDS)
    }

    pub fn from_entries_with_options(
        entries: Vec<(RowId, Vec<u32>)>,
        permutations: usize,
        bands: usize,
    ) -> Self {
        let mut idx = Self {
            permutations,
            bands,
            sigs: Vec::with_capacity(entries.len()),
            buckets: HashMap::new(),
        };
        for (rid, sig) in entries {
            let i = idx.sigs.len() as u32;
            for b in 0..bands {
                idx.buckets
                    .entry(band_key(b, &sig, permutations / bands))
                    .or_default()
                    .push(i);
            }
            idx.sigs.push((rid, sig));
        }
        idx
    }

    pub fn snapshot(&self) -> MinHashSnapshot {
        MinHashSnapshot {
            permutations: self.permutations,
            bands: self.bands,
            entries: self.entries(),
        }
    }

    pub fn from_snapshot(snapshot: MinHashSnapshot) -> Self {
        Self::from_entries_with_options(snapshot.entries, snapshot.permutations, snapshot.bands)
    }
}

impl Default for MinHashIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MinHashSnapshot {
    pub permutations: usize,
    pub bands: usize,
    pub entries: MinHashEntries,
}

/// Checkpoint payload type (kept explicit for the global-index serde).
pub type MinHashEntries = Vec<(RowId, Vec<u32>)>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_typed_hash_vectors() {
        let fixtures: Vec<serde_json::Value> =
            serde_json::from_str(include_str!("../../../../docs/ai/minhash-v1-golden.json"))
                .unwrap();
        for fixture in fixtures {
            let expected = fixture["expected"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap();
            assert_eq!(
                minhash_member_hash_v1(&fixture["member"]).unwrap(),
                expected
            );
        }
        assert_ne!(
            minhash_token_hash("1"),
            minhash_member_hash_v1(&serde_json::json!(1)).unwrap()
        );
        assert_ne!(
            minhash_token_hash("true"),
            minhash_member_hash_v1(&serde_json::json!(true)).unwrap()
        );
    }

    #[test]
    fn custom_options_survive_snapshot() {
        let mut index = MinHashIndex::with_options(64, 16);
        let query = set(&["a", "b", "c", "d"]);
        index.insert(&query, RowId(7));
        let restored = MinHashIndex::from_snapshot(index.snapshot());
        assert_eq!(restored.options(), (64, 16));
        assert_eq!(restored.search(&query, 1)[0].0, RowId(7));
        assert_eq!(restored.search(&query, 1)[0].1, 1.0);
    }

    #[test]
    fn exact_verification_cannot_recover_an_lsh_miss() {
        let base = set(&["a", "b", "c", "d"]);
        let mut index = MinHashIndex::with_options(1, 1);
        index.insert(&base, RowId(1));
        let missed = (0..100)
            .map(|candidate| set(&["a", "b", "c", &format!("x{candidate}")]))
            .find(|query| index.search(query, 1).is_empty())
            .expect("one-permutation LSH must miss a near set in this fixture");
        assert_eq!(
            base.iter().filter(|token| missed.contains(token)).count(),
            3
        );
        assert!(index.search(&missed, 1).is_empty());
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
