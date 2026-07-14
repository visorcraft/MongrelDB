use serde::{Deserialize, Serialize};

/// A stable, dense row identifier shared by *every* index in a table.
///
/// Row IDs are allocated monotonically and **never reused**. Deletes record a
/// tombstone at the row id; updates allocate a *new* row id and tombstone the
/// old one. All indexes (primary HOT, learned PGM, secondary bitmaps, ANN,
/// FM-index) resolve to or from `RowId`, so multi-condition queries intersect
/// in a single id space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowId(pub u64);

impl RowId {
    pub const MIN: RowId = RowId(0);
    pub const NULL_SORT_KEY: u16 = 0xFFFF;

    #[inline]
    pub fn next(self) -> RowId {
        RowId(self.0.wrapping_add(1))
    }
}

impl std::fmt::Display for RowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowId({})", self.0)
    }
}

impl From<u64> for RowId {
    fn from(v: u64) -> Self {
        RowId(v)
    }
}

/// Monotonic allocator for [`RowId`]s.
#[derive(Debug, Default, Clone)]
pub struct RowIdAllocator {
    next: u64,
}

impl RowIdAllocator {
    pub fn new(start: u64) -> Self {
        Self { next: start }
    }

    /// Allocate a single new row id.
    #[inline]
    pub fn alloc(&mut self) -> RowId {
        let id = self.next;
        self.next += 1;
        RowId(id)
    }

    /// Allocate a contiguous range of `n` row ids, returning the inclusive start.
    pub fn alloc_range(&mut self, n: u64) -> RowId {
        let start = self.next;
        self.next = self.next.saturating_add(n);
        RowId(start)
    }

    #[inline]
    pub fn current(&self) -> RowId {
        RowId(self.next)
    }

    /// Advance the allocator past `id` if it is ahead. Used during recovery.
    pub fn advance_to(&mut self, id: RowId) {
        if id.0 >= self.next {
            self.next = id.0 + 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_monotonically() {
        let mut a = RowIdAllocator::default();
        assert_eq!(a.alloc(), RowId(0));
        assert_eq!(a.alloc(), RowId(1));
        let start = a.alloc_range(3);
        assert_eq!(start, RowId(2));
        assert_eq!(a.current(), RowId(5));
        assert_eq!(a.alloc(), RowId(5));
    }

    #[test]
    fn advance_to_moves_head() {
        let mut a = RowIdAllocator::default();
        a.advance_to(RowId(100));
        assert_eq!(a.alloc(), RowId(101));
    }
}
