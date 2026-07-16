use mongreldb_core::CancellationReason;
use mongreldb_query::QueryId;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

const MAX_METADATA_BYTES: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct PreCancelKey {
    query_id: QueryId,
    owner: String,
    session_id: Option<String>,
}

impl PreCancelKey {
    fn new(query_id: QueryId, owner: &str, session_id: Option<&str>) -> Result<Self, InsertError> {
        if owner.len() > MAX_METADATA_BYTES
            || session_id.is_some_and(|value| value.len() > MAX_METADATA_BYTES)
        {
            return Err(InsertError::MetadataTooLarge);
        }
        Ok(Self {
            query_id,
            owner: owner.to_owned(),
            session_id: session_id.map(str::to_owned),
        })
    }

    fn approximate_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.owner.len())
            .saturating_add(self.session_id.as_ref().map_or(0, String::len))
    }
}

#[derive(Debug)]
struct Entry {
    expires_at: Instant,
    approximate_bytes: usize,
    reason: CancellationReason,
}

#[derive(Debug)]
struct OwnerRate {
    window_started: Instant,
    requests: usize,
    approximate_bytes: usize,
}

#[derive(Debug, Default)]
struct StoreState {
    entries: HashMap<PreCancelKey, Entry>,
    by_query_id: HashMap<QueryId, usize>,
    owner_rates: HashMap<String, OwnerRate>,
    approximate_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistrationLookup {
    NoReservation,
    Matching(CancellationReason),
    ReservedByAnotherIdentity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertError {
    Full,
    OwnerLimit,
    RateLimited,
    MetadataTooLarge,
}

#[derive(Debug)]
pub(crate) struct PreCancelStore {
    state: Mutex<StoreState>,
    ttl: Duration,
    max_entries: usize,
    max_bytes: usize,
    max_entries_per_owner: usize,
    rate_window: Duration,
    max_requests_per_owner: usize,
}

impl PreCancelStore {
    pub(crate) fn new(
        ttl: Duration,
        max_entries: usize,
        max_bytes: usize,
        max_entries_per_owner: usize,
        rate_window: Duration,
        max_requests_per_owner: usize,
    ) -> Self {
        Self {
            state: Mutex::new(StoreState::default()),
            ttl,
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
            max_entries_per_owner: max_entries_per_owner.max(1),
            rate_window,
            max_requests_per_owner: max_requests_per_owner.max(1),
        }
    }

    pub(crate) fn insert(
        &self,
        query_id: QueryId,
        owner: &str,
        session_id: Option<&str>,
        reason: CancellationReason,
    ) -> Result<(), InsertError> {
        let key = PreCancelKey::new(query_id, owner, session_id)?;
        let expires_at = Instant::now()
            .checked_add(self.ttl)
            .ok_or(InsertError::Full)?;
        let mut state = self.lock();
        self.prune_locked(&mut state);
        self.record_request(&mut state, owner)?;
        if state.entries.contains_key(&key) {
            return Ok(());
        }
        if state
            .entries
            .keys()
            .filter(|existing| existing.owner == owner)
            .count()
            >= self.max_entries_per_owner
        {
            return Err(InsertError::OwnerLimit);
        }
        let approximate_bytes = key
            .approximate_bytes()
            .saturating_add(std::mem::size_of::<Entry>());
        if state.entries.len() >= self.max_entries
            || state.approximate_bytes.saturating_add(approximate_bytes) > self.max_bytes
        {
            return Err(InsertError::Full);
        }
        state.approximate_bytes = state.approximate_bytes.saturating_add(approximate_bytes);
        if !state.by_query_id.contains_key(&query_id) {
            state.approximate_bytes = state
                .approximate_bytes
                .saturating_add(std::mem::size_of::<(QueryId, usize)>());
        }
        *state.by_query_id.entry(query_id).or_default() += 1;
        state.entries.insert(
            key,
            Entry {
                expires_at,
                approximate_bytes,
                reason,
            },
        );
        Ok(())
    }

    pub(crate) fn take(
        &self,
        query_id: QueryId,
        owner: &str,
        session_id: Option<&str>,
    ) -> Option<CancellationReason> {
        let Ok(key) = PreCancelKey::new(query_id, owner, session_id) else {
            return None;
        };
        let mut state = self.lock();
        self.prune_locked(&mut state);
        if let Some(entry) = state.entries.remove(&key) {
            state.approximate_bytes = state
                .approximate_bytes
                .saturating_sub(entry.approximate_bytes);
            remove_query_id(&mut state, query_id);
            Some(entry.reason)
        } else {
            None
        }
    }

    pub(crate) fn reason(
        &self,
        query_id: QueryId,
        owner: &str,
        session_id: Option<&str>,
    ) -> Option<CancellationReason> {
        let Ok(key) = PreCancelKey::new(query_id, owner, session_id) else {
            return None;
        };
        let mut state = self.lock();
        self.prune_locked(&mut state);
        state.entries.get(&key).map(|entry| entry.reason)
    }

    pub(crate) fn reason_for_query(&self, query_id: QueryId) -> Option<CancellationReason> {
        let mut state = self.lock();
        self.prune_locked(&mut state);
        state
            .entries
            .iter()
            .find_map(|(key, entry)| (key.query_id == query_id).then_some(entry.reason))
    }

    pub(crate) fn lookup_for_registration(
        &self,
        query_id: QueryId,
        owner: &str,
        session_id: Option<&str>,
    ) -> RegistrationLookup {
        let Ok(key) = PreCancelKey::new(query_id, owner, session_id) else {
            return RegistrationLookup::NoReservation;
        };
        let mut state = self.lock();
        self.prune_locked(&mut state);
        if let Some(entry) = state.entries.get(&key) {
            RegistrationLookup::Matching(entry.reason)
        } else if state.by_query_id.contains_key(&query_id) {
            RegistrationLookup::ReservedByAnotherIdentity
        } else {
            RegistrationLookup::NoReservation
        }
    }

    pub(crate) fn reason_for_query_in_session(
        &self,
        query_id: QueryId,
        session_id: &str,
    ) -> Option<CancellationReason> {
        let mut state = self.lock();
        self.prune_locked(&mut state);
        state.entries.iter().find_map(|(key, entry)| {
            (key.query_id == query_id && key.session_id.as_deref() == Some(session_id))
                .then_some(entry.reason)
        })
    }

    pub(crate) fn len(&self) -> usize {
        let mut state = self.lock();
        self.prune_locked(&mut state);
        state.entries.len()
    }

    pub(crate) fn approximate_bytes(&self) -> usize {
        let mut state = self.lock();
        self.prune_locked(&mut state);
        state.approximate_bytes
    }

    fn lock(&self) -> MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn record_request(&self, state: &mut StoreState, owner: &str) -> Result<(), InsertError> {
        if let Some(rate) = state.owner_rates.get_mut(owner) {
            if rate.requests >= self.max_requests_per_owner {
                return Err(InsertError::RateLimited);
            }
            rate.requests += 1;
            return Ok(());
        }
        if state.owner_rates.len() >= self.max_entries {
            return Err(InsertError::Full);
        }
        let approximate_bytes = std::mem::size_of::<OwnerRate>()
            .saturating_add(std::mem::size_of::<String>())
            .saturating_add(owner.len());
        if state.approximate_bytes.saturating_add(approximate_bytes) > self.max_bytes {
            return Err(InsertError::Full);
        }
        state.approximate_bytes = state.approximate_bytes.saturating_add(approximate_bytes);
        state.owner_rates.insert(
            owner.to_owned(),
            OwnerRate {
                window_started: Instant::now(),
                requests: 1,
                approximate_bytes,
            },
        );
        Ok(())
    }

    fn prune_locked(&self, state: &mut StoreState) {
        let now = Instant::now();
        let expired = state
            .entries
            .iter()
            .filter_map(|(key, entry)| (entry.expires_at <= now).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in expired {
            if let Some(entry) = state.entries.remove(&key) {
                state.approximate_bytes = state
                    .approximate_bytes
                    .saturating_sub(entry.approximate_bytes);
                remove_query_id(state, key.query_id);
            }
        }
        state.owner_rates.retain(|_, rate| {
            let keep = now.saturating_duration_since(rate.window_started) < self.rate_window;
            if !keep {
                state.approximate_bytes = state
                    .approximate_bytes
                    .saturating_sub(rate.approximate_bytes);
            }
            keep
        });
    }
}

fn remove_query_id(state: &mut StoreState, query_id: QueryId) {
    if let Some(count) = state.by_query_id.get_mut(&query_id) {
        *count -= 1;
        if *count == 0 {
            state.by_query_id.remove(&query_id);
            state.approximate_bytes = state
                .approximate_bytes
                .saturating_sub(std::mem::size_of::<(QueryId, usize)>());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u8) -> QueryId {
        format!("{value:032x}").parse().unwrap()
    }

    #[test]
    fn entries_are_owner_and_session_bound_and_expire() {
        let store = PreCancelStore::new(
            Duration::from_millis(50),
            8,
            4096,
            8,
            Duration::from_millis(50),
            8,
        );
        store
            .insert(
                id(1),
                "alice",
                Some("one"),
                CancellationReason::ClientRequest,
            )
            .unwrap();
        assert_eq!(
            store.reason(id(1), "alice", Some("one")),
            Some(CancellationReason::ClientRequest)
        );
        assert_eq!(store.take(id(1), "bob", Some("one")), None);
        assert_eq!(store.take(id(1), "alice", Some("two")), None);
        assert_eq!(
            store.reason_for_query(id(1)),
            Some(CancellationReason::ClientRequest)
        );
        std::thread::sleep(Duration::from_millis(75));
        assert_eq!(store.take(id(1), "alice", Some("one")), None);
        assert_eq!(store.len(), 0);
        assert_eq!(store.approximate_bytes(), 0);
    }

    #[test]
    fn registration_lookup_tracks_all_identities_and_expiry() {
        let store = PreCancelStore::new(
            Duration::from_millis(25),
            8,
            4096,
            8,
            Duration::from_secs(1),
            8,
        );
        store
            .insert(id(1), "alice", Some("a"), CancellationReason::ClientRequest)
            .unwrap();
        store
            .insert(id(1), "alice", Some("a"), CancellationReason::ClientRequest)
            .unwrap();
        store
            .insert(id(1), "bob", Some("b"), CancellationReason::Deadline)
            .unwrap();

        assert_eq!(
            store.lookup_for_registration(id(1), "alice", Some("a")),
            RegistrationLookup::Matching(CancellationReason::ClientRequest)
        );
        assert_eq!(
            store.lookup_for_registration(id(1), "alice", Some("b")),
            RegistrationLookup::ReservedByAnotherIdentity
        );
        assert_eq!(
            store.lookup_for_registration(id(2), "alice", Some("a")),
            RegistrationLookup::NoReservation
        );
        assert_eq!(
            store.take(id(1), "alice", Some("a")),
            Some(CancellationReason::ClientRequest)
        );
        assert_eq!(
            store.lookup_for_registration(id(1), "alice", Some("a")),
            RegistrationLookup::ReservedByAnotherIdentity
        );
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(
            store.lookup_for_registration(id(1), "bob", Some("b")),
            RegistrationLookup::NoReservation
        );
        assert_eq!(store.approximate_bytes(), 0);
    }

    #[test]
    fn administrative_lookup_keeps_requested_session_isolation() {
        let store = PreCancelStore::new(
            Duration::from_secs(60),
            8,
            4096,
            8,
            Duration::from_secs(1),
            8,
        );
        store
            .insert(
                id(1),
                "alice",
                Some("session-a"),
                CancellationReason::ClientRequest,
            )
            .unwrap();
        store
            .insert(
                id(1),
                "bob",
                Some("session-b"),
                CancellationReason::Deadline,
            )
            .unwrap();

        assert_eq!(
            store.reason_for_query_in_session(id(1), "session-a"),
            Some(CancellationReason::ClientRequest)
        );
        assert_eq!(
            store.reason_for_query_in_session(id(1), "session-b"),
            Some(CancellationReason::Deadline)
        );
        assert_eq!(store.reason_for_query_in_session(id(1), "session-c"), None);
    }

    #[test]
    fn entries_have_global_byte_and_per_owner_caps() {
        let store = PreCancelStore::new(
            Duration::from_secs(60),
            2,
            4096,
            1,
            Duration::from_secs(1),
            8,
        );
        store
            .insert(id(1), "alice", None, CancellationReason::ClientRequest)
            .unwrap();
        assert_eq!(
            store.insert(id(2), "alice", None, CancellationReason::ClientRequest),
            Err(InsertError::OwnerLimit)
        );
        store
            .insert(id(2), "bob", None, CancellationReason::ClientRequest)
            .unwrap();
        assert_eq!(
            store.insert(id(3), "carol", None, CancellationReason::ClientRequest),
            Err(InsertError::Full)
        );

        let tiny = PreCancelStore::new(Duration::from_secs(60), 8, 1, 8, Duration::from_secs(1), 8);
        assert_eq!(
            tiny.insert(id(1), "alice", None, CancellationReason::ClientRequest),
            Err(InsertError::Full)
        );
    }

    #[test]
    fn repeated_requests_are_rate_limited_per_owner_and_reset() {
        let store = PreCancelStore::new(
            Duration::from_secs(60),
            8,
            4096,
            8,
            Duration::from_millis(50),
            2,
        );
        for _ in 0..2 {
            store
                .insert(id(1), "alice", None, CancellationReason::ClientRequest)
                .unwrap();
        }
        assert_eq!(
            store.insert(id(1), "alice", None, CancellationReason::ClientRequest),
            Err(InsertError::RateLimited)
        );
        store
            .insert(id(2), "bob", None, CancellationReason::ClientRequest)
            .unwrap();
        std::thread::sleep(Duration::from_millis(75));
        store
            .insert(id(1), "alice", None, CancellationReason::ClientRequest)
            .unwrap();
    }

    #[test]
    fn unrepresentable_ttl_fails_closed_without_panicking() {
        let store = PreCancelStore::new(Duration::MAX, 8, 4096, 8, Duration::from_secs(1), 8);
        assert_eq!(
            store.insert(id(1), "alice", None, CancellationReason::ClientRequest),
            Err(InsertError::Full)
        );
        assert_eq!(store.len(), 0);
    }
}
