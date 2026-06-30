//! Tool-call-native query surface.
//!
//! A [`Query`] is a conjunction of [`Condition`]s. Each condition resolves to a
//! set of row ids in the shared [`crate::rowid::RowId`] space (PK exact, bitmap
//! equality, ANN semantic, FM substring, or a column range). [`crate::Table`]
//! intersects the sets and materializes the survivors — letting an agent express
//! `semsearch ∩ fm_contains ∩ cat_in`, which no SQL FTS pipeline can.

/// One predicate over the row-id space.
#[derive(Debug, Clone)]
pub enum Condition {
    /// Primary-key exact match (encoded key bytes).
    Pk(Vec<u8>),
    /// Low-cardinality equality via the roaring bitmap index.
    BitmapEq { column_id: u16, value: Vec<u8> },
    /// Multi-value equality via the roaring bitmap index (Phase 13.5). Resolves
    /// to the **union** of `bitmap[col].get(v)` for each value — the index-
    /// accelerated equivalent of `col IN (v1, v2, …)` or a semi-join's runtime
    /// value set.
    BitmapIn {
        column_id: u16,
        values: Vec<Vec<u8>>,
    },
    /// Semantic search via the binary-quantized ANN index.
    Ann {
        column_id: u16,
        query: Vec<f32>,
        k: usize,
    },
    /// Arbitrary substring via the FM index (no tokenization).
    FmContains { column_id: u16, pattern: Vec<u8> },
    /// Multi-segment FM intersection for `LIKE '%seg1%seg2%...'` (Priority 12).
    /// Resolves to the **intersection** of FM lookups for each segment — a much
    /// tighter superset than the single longest segment. DataFusion still
    /// re-applies the real wildcard semantics (`Inexact` pushdown).
    FmContainsAll {
        column_id: u16,
        patterns: Vec<Vec<u8>>,
    },
    /// Inclusive integer range (served by scanning the int column, later by the
    /// learned PGM index / page-index pruning). Exclusive bounds (`>`,`<`) are
    /// expressed exactly via ±1 in the translator.
    Range { column_id: u16, lo: i64, hi: i64 },
    /// Floating-point range with per-bound inclusivity (exact for `>`/`<`/`>=`/
    /// `<=`/`BETWEEN`), served the same way as [`Condition::Range`].
    RangeF64 {
        column_id: u16,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    },
    /// SPLADE-style sparse retrieval: top-k row ids by sparse dot product over
    /// shared tokens. `query` is a sparse vector `(token id → weight)`.
    SparseMatch {
        column_id: u16,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    /// Rows where `column_id` is NULL. Resolved by decoding the column and
    /// collecting null positions — a column scan, but no row materialization.
    /// Page-stat aware: pages with `null_count == 0` are skipped.
    IsNull { column_id: u16 },
    /// Rows where `column_id` is NOT NULL. The complement of [`Self::IsNull`].
    /// Page-stat aware: pages with `null_count == row_count` are skipped.
    IsNotNull { column_id: u16 },
}

/// A conjunctive query. Empty ⇒ all rows.
#[derive(Debug, Default, Clone)]
pub struct Query {
    pub conditions: Vec<Condition>,
}

impl Query {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn and(mut self, c: Condition) -> Self {
        self.conditions.push(c);
        self
    }
    pub fn pk(key: Vec<u8>) -> Self {
        Self::new().and(Condition::Pk(key))
    }
}

/// Canonical 64-bit cache key for a conjunctive native query + optional
/// projection at `epoch` (Phase 19.1 / 19.6). Conditions are commutative (they
/// are ANDed), so each condition is hashed into its own 64-bit digest, the
/// digests are sorted, then folded together — two queries with the same
/// semantics in a different order produce the same key. Within a condition,
/// `BitmapIn` values are deduped+sorted and the `SparseMatch` query is sorted
/// by token id. `epoch` is folded in so a `commit()` (which bumps it) orphans
/// every prior entry without an explicit sweep.
pub fn canonical_query_key(
    conditions: &[Condition],
    projection: Option<&[u16]>,
    epoch: u64,
) -> u64 {
    let fold = |seed: u64, b: u64| -> u64 { seed.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(b) };
    let mut acc = fold(0xA5A5_A5A5_A5A5_A5A5, epoch);
    // Order-independent: per-condition digests, sorted, then folded.
    let mut digests: Vec<u64> = conditions.iter().map(hash_condition).collect();
    digests.sort_unstable();
    let n = digests.len() as u64;
    acc = fold(acc, n);
    for d in digests {
        acc = fold(acc, d);
    }
    // Projection: sorted column ids (None ⇒ "all columns", distinct from any
    // explicit projection incl. one listing every column, by intent).
    match projection {
        Some(p) => {
            let mut p = p.to_vec();
            p.sort_unstable();
            p.dedup();
            acc = fold(acc, 0x5E);
            acc = fold(acc, p.len() as u64);
            for id in p {
                acc = fold(acc, id as u64);
            }
        }
        None => {
            acc = fold(acc, 0xA5);
        }
    }
    acc
}

/// Hash a single condition into a 64-bit digest (order-independent w.r.t. its
/// siblings; see [`canonical_query_key`]). Floats are hashed via `to_bits` for
/// determinism.
fn hash_condition(c: &Condition) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match c {
        Condition::Pk(k) => {
            0u8.hash(&mut h);
            k.hash(&mut h);
        }
        Condition::BitmapEq { column_id, value } => {
            1u8.hash(&mut h);
            column_id.hash(&mut h);
            value.hash(&mut h);
        }
        Condition::BitmapIn { column_id, values } => {
            2u8.hash(&mut h);
            column_id.hash(&mut h);
            let mut v: Vec<&Vec<u8>> = values.iter().collect();
            v.sort();
            v.dedup();
            v.len().hash(&mut h);
            for b in v {
                b.hash(&mut h);
            }
        }
        Condition::Ann {
            column_id,
            query,
            k,
        } => {
            3u8.hash(&mut h);
            column_id.hash(&mut h);
            k.hash(&mut h);
            for f in query {
                f.to_bits().hash(&mut h);
            }
        }
        Condition::FmContains { column_id, pattern } => {
            4u8.hash(&mut h);
            column_id.hash(&mut h);
            pattern.hash(&mut h);
        }
        Condition::FmContainsAll {
            column_id,
            patterns,
        } => {
            10u8.hash(&mut h);
            column_id.hash(&mut h);
            let mut sorted: Vec<&[u8]> = patterns.iter().map(|p| p.as_slice()).collect();
            sorted.sort();
            sorted.len().hash(&mut h);
            for p in sorted {
                p.hash(&mut h);
            }
        }
        Condition::Range { column_id, lo, hi } => {
            5u8.hash(&mut h);
            column_id.hash(&mut h);
            lo.hash(&mut h);
            hi.hash(&mut h);
        }
        Condition::RangeF64 {
            column_id,
            lo,
            lo_inclusive,
            hi,
            hi_inclusive,
        } => {
            6u8.hash(&mut h);
            column_id.hash(&mut h);
            lo.to_bits().hash(&mut h);
            lo_inclusive.hash(&mut h);
            hi.to_bits().hash(&mut h);
            hi_inclusive.hash(&mut h);
        }
        Condition::SparseMatch {
            column_id,
            query,
            k,
        } => {
            7u8.hash(&mut h);
            column_id.hash(&mut h);
            k.hash(&mut h);
            let mut q: Vec<(u32, u32)> = query.iter().map(|(t, w)| (*t, w.to_bits())).collect();
            q.sort_by_key(|(t, _)| *t);
            for (t, wb) in q {
                t.hash(&mut h);
                wb.hash(&mut h);
            }
        }
        Condition::IsNull { column_id } => {
            8u8.hash(&mut h);
            column_id.hash(&mut h);
        }
        Condition::IsNotNull { column_id } => {
            9u8.hash(&mut h);
            column_id.hash(&mut h);
        }
    }
    h.finish()
}

/// Extract the column IDs referenced by a slice of conditions (Phase 19.1
/// hardening (c)). `Pk` references no user column (it's a row-id lookup) so it
/// contributes nothing. Used for conservative column-based cache invalidation:
/// a commit touching any of these columns may change the result.
pub fn condition_columns(conditions: &[Condition]) -> Vec<u16> {
    let mut cols: Vec<u16> = conditions
        .iter()
        .filter_map(|c| match c {
            Condition::Pk(_) => None,
            Condition::BitmapEq { column_id, .. }
            | Condition::BitmapIn { column_id, .. }
            | Condition::Ann { column_id, .. }
            | Condition::FmContains { column_id, .. }
            | Condition::FmContainsAll { column_id, .. }
            | Condition::Range { column_id, .. }
            | Condition::RangeF64 { column_id, .. }
            | Condition::SparseMatch { column_id, .. }
            | Condition::IsNull { column_id }
            | Condition::IsNotNull { column_id } => Some(*column_id),
        })
        .collect();
    cols.sort_unstable();
    cols.dedup();
    cols
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_chains() {
        let q = Query::pk(b"k".to_vec()).and(Condition::Range {
            column_id: 1,
            lo: 0,
            hi: 10,
        });
        assert_eq!(q.conditions.len(), 2);
    }

    /// Phase 19.6: order-independent canonicalization — the same conditions in a
    /// different order, and a `BitmapIn` with shuffled/duplicate values, all
    /// produce the same key.
    #[test]
    fn canonical_key_is_order_independent() {
        let e = 7u64;
        let a = Query::new()
            .and(Condition::Range {
                column_id: 1,
                lo: 0,
                hi: 10,
            })
            .and(Condition::BitmapEq {
                column_id: 2,
                value: b"x".to_vec(),
            });
        let b = Query::new()
            .and(Condition::BitmapEq {
                column_id: 2,
                value: b"x".to_vec(),
            })
            .and(Condition::Range {
                column_id: 1,
                lo: 0,
                hi: 10,
            });
        assert_eq!(
            canonical_query_key(&a.conditions, None, e),
            canonical_query_key(&b.conditions, None, e),
            "condition order must not affect the key"
        );

        // BitmapIn dedup + sort.
        let ordered = Condition::BitmapIn {
            column_id: 3,
            values: vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        };
        let shuffled = Condition::BitmapIn {
            column_id: 3,
            values: vec![b"c".to_vec(), b"a".to_vec(), b"a".to_vec(), b"b".to_vec()],
        };
        assert_eq!(
            canonical_query_key(std::slice::from_ref(&ordered), None, e),
            canonical_query_key(&[shuffled], None, e),
            "BitmapIn values must dedup+sort"
        );

        // Epoch changes the key (invalidation).
        assert_ne!(
            canonical_query_key(&a.conditions, None, e),
            canonical_query_key(&a.conditions, None, e + 1),
            "epoch must fold into the key"
        );

        // Projection None vs explicit differs (by intent).
        let proj = vec![1u16, 2];
        assert_ne!(
            canonical_query_key(&a.conditions, None, e),
            canonical_query_key(&a.conditions, Some(&proj), e),
            "None projection must differ from an explicit projection"
        );
        // Projection order-independence.
        let proj_rev = vec![2u16, 1];
        assert_eq!(
            canonical_query_key(&a.conditions, Some(&proj), e),
            canonical_query_key(&a.conditions, Some(&proj_rev), e),
        );
    }
}
