use roaring::RoaringBitmap;

#[derive(Clone, Debug)]
pub(crate) enum RowIdSet {
    Empty,
    One(u64),
    Roaring(RoaringBitmap),
    Sorted(Vec<u64>),
}

impl RowIdSet {
    pub(crate) fn empty() -> Self {
        Self::Empty
    }

    pub(crate) fn one(row_id: u64) -> Self {
        Self::One(row_id)
    }

    pub(crate) fn from_roaring(bitmap: RoaringBitmap) -> Self {
        match bitmap.len() {
            0 => Self::Empty,
            1 => Self::One(bitmap.iter().next().unwrap() as u64),
            _ => Self::Roaring(bitmap),
        }
    }

    pub(crate) fn from_unsorted(mut row_ids: Vec<u64>) -> Self {
        row_ids.sort_unstable();
        Self::from_sorted(row_ids)
    }

    pub(crate) fn from_sorted(mut row_ids: Vec<u64>) -> Self {
        row_ids.dedup();
        match row_ids.len() {
            0 => Self::Empty,
            1 => Self::One(row_ids[0]),
            _ => Self::Sorted(row_ids),
        }
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Roaring(bitmap) => bitmap.len() as usize,
            Self::Sorted(row_ids) => row_ids.len(),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn contains(&self, row_id: u64) -> bool {
        match self {
            Self::Empty => false,
            Self::One(id) => *id == row_id,
            Self::Roaring(bitmap) => u32::try_from(row_id)
                .ok()
                .is_some_and(|id| bitmap.contains(id)),
            Self::Sorted(row_ids) => row_ids.binary_search(&row_id).is_ok(),
        }
    }

    pub(crate) fn insert(&mut self, row_id: u64) {
        match self {
            Self::Empty => *self = Self::One(row_id),
            Self::One(id) if *id == row_id => {}
            Self::One(id) => {
                let mut row_ids = [*id, row_id];
                row_ids.sort_unstable();
                *self = Self::Sorted(row_ids.to_vec());
            }
            Self::Roaring(bitmap) => match u32::try_from(row_id) {
                Ok(id) => {
                    bitmap.insert(id);
                }
                Err(_) => {
                    let mut row_ids = bitmap.iter().map(|id| id as u64).collect::<Vec<_>>();
                    row_ids.push(row_id);
                    *self = Self::from_unsorted(row_ids);
                }
            },
            Self::Sorted(row_ids) => match row_ids.binary_search(&row_id) {
                Ok(_) => {}
                Err(pos) => row_ids.insert(pos, row_id),
            },
        }
    }

    pub(crate) fn remove(&mut self, row_id: u64) {
        match self {
            Self::Empty => {}
            Self::One(id) => {
                if *id == row_id {
                    *self = Self::Empty;
                }
            }
            Self::Roaring(bitmap) => {
                if let Ok(id) = u32::try_from(row_id) {
                    bitmap.remove(id);
                    if bitmap.is_empty() {
                        *self = Self::Empty;
                    } else if bitmap.len() == 1 {
                        *self = Self::One(bitmap.iter().next().unwrap() as u64);
                    }
                }
            }
            Self::Sorted(row_ids) => {
                if let Ok(pos) = row_ids.binary_search(&row_id) {
                    row_ids.remove(pos);
                    match row_ids.len() {
                        0 => *self = Self::Empty,
                        1 => *self = Self::One(row_ids[0]),
                        _ => {}
                    }
                }
            }
        }
    }

    pub(crate) fn remove_many<I>(&mut self, row_ids: I)
    where
        I: IntoIterator<Item = u64>,
    {
        for row_id in row_ids {
            self.remove(row_id);
            if self.is_empty() {
                break;
            }
        }
    }

    pub(crate) fn to_sorted_vec(&self) -> Vec<u64> {
        match self {
            Self::Empty => Vec::new(),
            Self::One(id) => vec![*id],
            Self::Roaring(bitmap) => bitmap.iter().map(|id| id as u64).collect(),
            Self::Sorted(row_ids) => row_ids.clone(),
        }
    }

    pub(crate) fn into_sorted_vec(self) -> Vec<u64> {
        match self {
            Self::Empty => Vec::new(),
            Self::One(id) => vec![id],
            Self::Roaring(bitmap) => bitmap.iter().map(|id| id as u64).collect(),
            Self::Sorted(row_ids) => row_ids,
        }
    }

    pub(crate) fn to_roaring_lossy(&self) -> RoaringBitmap {
        self.to_sorted_vec()
            .into_iter()
            .filter_map(|id| u32::try_from(id).ok())
            .collect()
    }

    pub(crate) fn intersect_many(mut sets: Vec<Self>) -> Self {
        if sets.iter().any(Self::is_empty) {
            return Self::Empty;
        }
        if sets.is_empty() {
            return Self::Empty;
        }
        if sets.iter().all(|s| matches!(s, Self::Roaring(_))) {
            let mut iter = sets.into_iter();
            let Self::Roaring(mut acc) = iter.next().unwrap() else {
                unreachable!();
            };
            for set in iter {
                let Self::Roaring(bitmap) = set else {
                    unreachable!();
                };
                acc &= &bitmap;
                if acc.is_empty() {
                    return Self::Empty;
                }
            }
            return Self::from_roaring(acc);
        }
        let first_idx = sets
            .iter()
            .enumerate()
            .min_by_key(|(_, set)| set.len())
            .map(|(idx, _)| idx)
            .unwrap_or(0);
        let first = sets.swap_remove(first_idx);
        let mut row_ids = first.into_sorted_vec();
        for set in &sets {
            row_ids.retain(|id| set.contains(*id));
            if row_ids.is_empty() {
                return Self::Empty;
            }
        }
        Self::from_sorted(row_ids)
    }
}
