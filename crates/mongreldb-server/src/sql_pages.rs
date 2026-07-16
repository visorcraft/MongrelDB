use hmac::{Hmac, Mac};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const CURSOR_VERSION: &str = "sp1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct SqlPageLimits {
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
    pub(crate) tokens: usize,
}

#[derive(Clone)]
pub(crate) struct RetainedSqlResult {
    id: String,
    owner: String,
    expires_at: Instant,
    expires_at_ms: u64,
    rows: Arc<Vec<Value>>,
    projection: Arc<Vec<String>>,
    limits: SqlPageLimits,
    approximate_bytes: usize,
    binding: SqlPageBinding,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SqlPageBinding {
    pub(crate) security_version: u64,
    pub(crate) catalog_epoch: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct SqlPage {
    pub(crate) rows: Vec<Value>,
    pub(crate) next_cursor: Option<String>,
    pub(crate) offset: usize,
    pub(crate) row_count: usize,
    pub(crate) total_rows: usize,
    pub(crate) byte_count: usize,
    pub(crate) estimated_tokens: usize,
    pub(crate) limits: SqlPageLimits,
    pub(crate) projection: Vec<String>,
    pub(crate) expires_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InsertError {
    Full,
    OwnerLimit,
    EntropyUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CursorError {
    Invalid,
    Expired,
    NotFound,
    PageLimit,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageError {
    RowExceedsLimits,
    OffsetInvalid,
    Cancelled,
}

#[derive(Default)]
struct StoreState {
    entries: HashMap<String, RetainedSqlResult>,
    approximate_bytes: usize,
}

pub(crate) struct SqlPageStore {
    state: Mutex<StoreState>,
    ttl: Duration,
    max_entries: usize,
    max_bytes: usize,
    max_entries_per_owner: usize,
}

impl SqlPageStore {
    pub(crate) fn new(
        ttl: Duration,
        max_entries: usize,
        max_bytes: usize,
        max_entries_per_owner: usize,
    ) -> Self {
        Self {
            state: Mutex::new(StoreState::default()),
            ttl,
            max_entries: max_entries.max(1),
            max_bytes: max_bytes.max(1),
            max_entries_per_owner: max_entries_per_owner.max(1),
        }
    }

    pub(crate) fn insert(
        &self,
        owner: &str,
        rows: Vec<Value>,
        projection: Vec<String>,
        limits: SqlPageLimits,
        approximate_bytes: usize,
        binding: SqlPageBinding,
    ) -> Result<RetainedSqlResult, InsertError> {
        let mut state = self.lock();
        prune(&mut state);
        if state.entries.len() >= self.max_entries {
            return Err(InsertError::Full);
        }
        if state
            .entries
            .values()
            .filter(|entry| entry.owner == owner)
            .count()
            >= self.max_entries_per_owner
        {
            return Err(InsertError::OwnerLimit);
        }
        if state.approximate_bytes.saturating_add(approximate_bytes) > self.max_bytes {
            return Err(InsertError::Full);
        }
        let expires_at = Instant::now()
            .checked_add(self.ttl)
            .ok_or(InsertError::Full)?;
        let mut unique_id = None;
        for _ in 0..128 {
            let mut id = [0u8; 16];
            mongreldb_core::encryption::fill_random(&mut id)
                .map_err(|_| InsertError::EntropyUnavailable)?;
            let id = hex(&id);
            if !state.entries.contains_key(&id) {
                unique_id = Some(id);
                break;
            }
        }
        let id = unique_id.ok_or(InsertError::EntropyUnavailable)?;
        let result = RetainedSqlResult {
            id,
            owner: owner.to_owned(),
            expires_at,
            expires_at_ms: now_ms().saturating_add(duration_ms(self.ttl)),
            rows: Arc::new(rows),
            projection: Arc::new(projection),
            limits,
            approximate_bytes,
            binding,
        };
        state.approximate_bytes = state.approximate_bytes.saturating_add(approximate_bytes);
        state.entries.insert(result.id.clone(), result.clone());
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn continue_page(
        &self,
        cursor: &str,
        owner: &str,
        key: &[u8; 32],
        binding: SqlPageBinding,
    ) -> Result<SqlPage, CursorError> {
        self.continue_page_inner(cursor, owner, key, binding, None)
    }

    pub(crate) fn continue_page_with_control(
        &self,
        cursor: &str,
        owner: &str,
        key: &[u8; 32],
        binding: SqlPageBinding,
        query: &mongreldb_query::RegisteredSqlQuery,
    ) -> Result<SqlPage, CursorError> {
        self.continue_page_inner(cursor, owner, key, binding, Some(query))
    }

    fn continue_page_inner(
        &self,
        cursor: &str,
        owner: &str,
        key: &[u8; 32],
        binding: SqlPageBinding,
        query: Option<&mongreldb_query::RegisteredSqlQuery>,
    ) -> Result<SqlPage, CursorError> {
        checkpoint(query)?;
        let cursor = parse_cursor(cursor, owner, key)?;
        checkpoint(query)?;
        let mut state = self.lock();
        if let Some(expired) = state.entries.get(&cursor.result_id).cloned() {
            if expired.expires_at <= Instant::now() {
                state.entries.remove(&cursor.result_id);
                state.approximate_bytes = state
                    .approximate_bytes
                    .saturating_sub(expired.approximate_bytes);
                return Err(CursorError::Expired);
            }
        }
        prune(&mut state);
        let Some(result) = state.entries.get(&cursor.result_id).cloned() else {
            return Err(CursorError::NotFound);
        };
        if result.expires_at_ms != cursor.expires_at_ms {
            return Err(CursorError::Invalid);
        }
        if result.owner != owner {
            return Err(CursorError::NotFound);
        }
        if result.binding != binding {
            if let Some(removed) = state.entries.remove(&result.id) {
                state.approximate_bytes = state
                    .approximate_bytes
                    .saturating_sub(removed.approximate_bytes);
            }
            return Err(CursorError::Expired);
        }
        drop(state);
        checkpoint(query)?;
        match render_page(&result, cursor.offset, key, query) {
            Ok(page) => Ok(page),
            Err(PageError::OffsetInvalid) => Err(CursorError::Invalid),
            Err(PageError::Cancelled) => Err(CursorError::Cancelled),
            Err(PageError::RowExceedsLimits) => {
                self.discard(&result);
                Err(CursorError::PageLimit)
            }
        }
    }

    pub(crate) fn first_page(
        result: &RetainedSqlResult,
        key: &[u8; 32],
    ) -> Result<SqlPage, PageError> {
        render_page(result, 0, key, None)
    }

    pub(crate) fn discard(&self, result: &RetainedSqlResult) {
        let mut state = self.lock();
        if let Some(removed) = state.entries.remove(&result.id) {
            state.approximate_bytes = state
                .approximate_bytes
                .saturating_sub(removed.approximate_bytes);
        }
    }

    pub(crate) fn max_retained_bytes(&self) -> usize {
        self.max_bytes
    }

    fn lock(&self) -> MutexGuard<'_, StoreState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

fn render_page(
    result: &RetainedSqlResult,
    offset: usize,
    key: &[u8; 32],
    query: Option<&mongreldb_query::RegisteredSqlQuery>,
) -> Result<SqlPage, PageError> {
    if offset > result.rows.len() {
        return Err(PageError::OffsetInvalid);
    }
    let mut rows = Vec::new();
    let mut byte_count = 2usize;
    for row in result.rows.iter().skip(offset).take(result.limits.rows) {
        if query.is_some_and(|query| query.checkpoint().is_err()) {
            return Err(PageError::Cancelled);
        }
        let row_bytes = serde_json::to_vec(row).map_err(|_| PageError::RowExceedsLimits)?;
        let next_bytes = byte_count
            .saturating_add(usize::from(!rows.is_empty()))
            .saturating_add(row_bytes.len());
        let next_tokens = estimate_tokens(next_bytes);
        if next_bytes > result.limits.bytes || next_tokens > result.limits.tokens {
            break;
        }
        byte_count = next_bytes;
        rows.push(row.clone());
    }
    if rows.is_empty() && offset < result.rows.len() {
        return Err(PageError::RowExceedsLimits);
    }
    let next_offset = offset.saturating_add(rows.len());
    let next_cursor =
        (next_offset < result.rows.len()).then(|| format_cursor(result, next_offset, key));
    Ok(SqlPage {
        row_count: rows.len(),
        total_rows: result.rows.len(),
        estimated_tokens: estimate_tokens(byte_count),
        rows,
        next_cursor,
        offset,
        byte_count,
        limits: result.limits.clone(),
        projection: result.projection.as_ref().clone(),
        expires_at_ms: result.expires_at_ms,
    })
}

fn checkpoint(query: Option<&mongreldb_query::RegisteredSqlQuery>) -> Result<(), CursorError> {
    query
        .map(mongreldb_query::RegisteredSqlQuery::checkpoint)
        .transpose()
        .map(|_| ())
        .map_err(|_| CursorError::Cancelled)
}

#[derive(Debug)]
struct ParsedCursor {
    result_id: String,
    offset: usize,
    expires_at_ms: u64,
}

fn format_cursor(result: &RetainedSqlResult, offset: usize, key: &[u8; 32]) -> String {
    let owner_hash = hex(&Sha256::digest(result.owner.as_bytes()));
    let payload = format!(
        "{CURSOR_VERSION}:{}:{offset}:{}:{owner_hash}",
        result.id, result.expires_at_ms
    );
    format!("{payload}:{}", sign(&payload, key))
}

fn parse_cursor(value: &str, owner: &str, key: &[u8; 32]) -> Result<ParsedCursor, CursorError> {
    let mut parts = value.rsplitn(2, ':');
    let tag = parts.next().ok_or(CursorError::Invalid)?;
    let payload = parts.next().ok_or(CursorError::Invalid)?;
    let tag = unhex::<32>(tag).ok_or(CursorError::Invalid)?;
    let mut mac = Hmac::<Sha256>::new_from_slice(key).map_err(|_| CursorError::Invalid)?;
    mac.update(payload.as_bytes());
    mac.verify_slice(&tag).map_err(|_| CursorError::Invalid)?;
    let parts: Vec<_> = payload.split(':').collect();
    if parts.len() != 5 || parts[0] != CURSOR_VERSION || unhex::<16>(parts[1]).is_none() {
        return Err(CursorError::Invalid);
    }
    let offset = parts[2].parse().map_err(|_| CursorError::Invalid)?;
    let expires_at_ms: u64 = parts[3].parse().map_err(|_| CursorError::Invalid)?;
    let expected_owner: [u8; 32] = Sha256::digest(owner.as_bytes()).into();
    if unhex::<32>(parts[4]) != Some(expected_owner) {
        return Err(CursorError::NotFound);
    }
    Ok(ParsedCursor {
        result_id: parts[1].to_owned(),
        offset,
        expires_at_ms,
    })
}

fn prune(state: &mut StoreState) {
    let now = Instant::now();
    state.entries.retain(|_, entry| {
        let keep = entry.expires_at > now;
        if !keep {
            state.approximate_bytes = state
                .approximate_bytes
                .saturating_sub(entry.approximate_bytes);
        }
        keep
    });
}

fn sign(payload: &str, key: &[u8; 32]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts 32-byte keys");
    mac.update(payload.as_bytes());
    hex(&mac.finalize().into_bytes())
}

fn estimate_tokens(bytes: usize) -> usize {
    bytes.saturating_add(3) / 4
}

pub(crate) fn accounted_bytes<E>(
    serialized_bytes: usize,
    rows: &[Value],
    projection: &[String],
    mut checkpoint: impl FnMut() -> Result<(), E>,
) -> Result<usize, E> {
    const ENTRY_OVERHEAD: usize = 512;
    let mut nodes = 0usize;
    let mut bytes = ENTRY_OVERHEAD
        .saturating_add(serialized_bytes)
        .saturating_add(rows.len().saturating_mul(std::mem::size_of::<Value>()))
        .saturating_add(
            projection
                .iter()
                .map(|name| std::mem::size_of::<String>().saturating_add(name.capacity()))
                .fold(0usize, usize::saturating_add),
        );
    for row in rows {
        bytes = bytes.saturating_add(value_heap_bytes(row, &mut nodes, &mut checkpoint)?);
    }
    checkpoint()?;
    Ok(bytes)
}

fn value_heap_bytes<E>(
    value: &Value,
    nodes: &mut usize,
    checkpoint: &mut impl FnMut() -> Result<(), E>,
) -> Result<usize, E> {
    const OBJECT_ENTRY_OVERHEAD: usize = 128;
    *nodes = nodes.saturating_add(1);
    if *nodes & 255 == 0 {
        checkpoint()?;
    }
    match value {
        Value::String(value) => Ok(value.capacity()),
        Value::Array(values) => {
            let mut bytes = values
                .capacity()
                .saturating_mul(std::mem::size_of::<Value>());
            for value in values {
                bytes = bytes.saturating_add(value_heap_bytes(value, nodes, checkpoint)?);
            }
            Ok(bytes)
        }
        Value::Object(values) => {
            let mut bytes = 0usize;
            for (key, value) in values {
                bytes = bytes
                    .saturating_add(OBJECT_ENTRY_OVERHEAD)
                    .saturating_add(key.capacity())
                    .saturating_add(value_heap_bytes(value, nodes, checkpoint)?);
            }
            Ok(bytes)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => Ok(0),
    }
}

fn hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn unhex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2 {
        return None;
    }
    let mut output = [0u8; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(output)
}

fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    const BINDING: SqlPageBinding = SqlPageBinding {
        security_version: 7,
        catalog_epoch: 11,
    };

    fn store(ttl: Duration) -> SqlPageStore {
        SqlPageStore::new(ttl, 8, 4096, 4)
    }

    fn rows() -> Vec<Value> {
        vec![
            serde_json::json!({"id": 1}),
            serde_json::json!({"id": 2}),
            serde_json::json!({"id": 3}),
        ]
    }

    #[test]
    fn retained_capacity_counts_value_tree_overhead() {
        let store = SqlPageStore::new(Duration::from_secs(60), 8, 1024, 4);
        let rows = vec![serde_json::json!({}); 4];
        let projection: Vec<_> = (0..128).map(|index| format!("c{index}")).collect();
        let bytes = accounted_bytes(13, &rows, &projection, || Ok::<(), ()>(())).unwrap();
        assert!(matches!(
            store.insert(
                "alice",
                rows,
                projection,
                SqlPageLimits {
                    rows: 4,
                    bytes: 1024,
                    tokens: 256,
                },
                bytes,
                BINDING,
            ),
            Err(InsertError::Full)
        ));
    }

    #[test]
    fn retained_capacity_counts_nested_value_nodes() {
        let store = SqlPageStore::new(Duration::from_secs(60), 8, 4096, 4);
        let rows = vec![serde_json::json!({"nested": vec![Value::Null; 256]})];
        let projection = vec!["nested".to_owned()];
        let serialized = serde_json::to_vec(&rows).unwrap().len();
        let bytes = accounted_bytes(serialized, &rows, &projection, || Ok::<(), ()>(())).unwrap();
        assert!(bytes > 4096);
        assert!(matches!(
            store.insert(
                "alice",
                rows,
                projection,
                SqlPageLimits {
                    rows: 1,
                    bytes: 4096,
                    tokens: 1024,
                },
                bytes,
                BINDING,
            ),
            Err(InsertError::Full)
        ));
    }

    #[test]
    fn cursor_is_owner_bound_tamper_evident_and_stable() {
        let store = store(Duration::from_secs(60));
        let result = store
            .insert(
                "alice",
                rows(),
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            )
            .unwrap();
        let key = [7; 32];
        let first = SqlPageStore::first_page(&result, &key).unwrap();
        assert_eq!(first.rows, vec![serde_json::json!({"id": 1})]);
        let cursor = first.next_cursor.unwrap();
        assert_eq!(
            store
                .continue_page(&cursor, "alice", &key, BINDING)
                .unwrap()
                .rows,
            vec![serde_json::json!({"id": 2})]
        );
        assert_eq!(
            store
                .continue_page(&cursor, "bob", &key, BINDING)
                .unwrap_err(),
            CursorError::NotFound
        );
        let mut tampered = cursor.into_bytes();
        tampered[5] = if tampered[5] == b'0' { b'1' } else { b'0' };
        assert_eq!(
            store
                .continue_page(
                    &String::from_utf8(tampered).unwrap(),
                    "alice",
                    &key,
                    BINDING,
                )
                .unwrap_err(),
            CursorError::Invalid
        );
    }

    #[test]
    fn page_rejects_a_row_larger_than_its_byte_or_token_budget() {
        let store = store(Duration::from_secs(60));
        let result = store
            .insert(
                "alice",
                vec![serde_json::json!({"large": "value"})],
                vec!["large".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 8,
                    tokens: 2,
                },
                20,
                BINDING,
            )
            .unwrap();
        assert_eq!(
            SqlPageStore::first_page(&result, &[1; 32]).unwrap_err(),
            PageError::RowExceedsLimits
        );
    }

    #[test]
    fn expired_cursor_is_rejected() {
        let store = store(Duration::from_millis(20));
        let result = store
            .insert(
                "alice",
                rows(),
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            )
            .unwrap();
        let key = [7; 32];
        let cursor = SqlPageStore::first_page(&result, &key)
            .unwrap()
            .next_cursor
            .unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(
            store
                .continue_page(&cursor, "alice", &key, BINDING)
                .unwrap_err(),
            CursorError::Expired
        );
    }

    #[test]
    fn wall_clock_expiry_does_not_override_monotonic_ttl() {
        let store = store(Duration::from_secs(60));
        let mut result = store
            .insert(
                "alice",
                rows(),
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            )
            .unwrap();
        result.expires_at_ms = 0;
        store
            .lock()
            .entries
            .get_mut(&result.id)
            .unwrap()
            .expires_at_ms = 0;
        let key = [7; 32];
        let cursor = SqlPageStore::first_page(&result, &key)
            .unwrap()
            .next_cursor
            .unwrap();
        assert!(store.continue_page(&cursor, "alice", &key, BINDING).is_ok());
    }

    #[test]
    fn unrepresentable_ttl_fails_closed_without_panicking() {
        let store = store(Duration::MAX);
        assert!(matches!(
            store.insert(
                "alice",
                rows(),
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            ),
            Err(InsertError::Full)
        ));
    }

    #[test]
    fn changed_security_or_catalog_generation_expires_cursor() {
        let store = store(Duration::from_secs(60));
        let result = store
            .insert(
                "alice",
                rows(),
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            )
            .unwrap();
        let key = [7; 32];
        let cursor = SqlPageStore::first_page(&result, &key)
            .unwrap()
            .next_cursor
            .unwrap();
        assert_eq!(
            store
                .continue_page(
                    &cursor,
                    "alice",
                    &key,
                    SqlPageBinding {
                        security_version: BINDING.security_version + 1,
                        ..BINDING
                    },
                )
                .unwrap_err(),
            CursorError::Expired
        );
        assert_eq!(
            store
                .continue_page(&cursor, "alice", &key, BINDING)
                .unwrap_err(),
            CursorError::NotFound
        );
    }

    #[test]
    fn terminal_continuation_replays_until_expiry() {
        let store = SqlPageStore::new(Duration::from_secs(60), 1, 30, 1);
        let result = store
            .insert(
                "alice",
                vec![serde_json::json!({"id": 1}), serde_json::json!({"id": 2})],
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            )
            .unwrap();
        let key = [7; 32];
        let cursor = SqlPageStore::first_page(&result, &key)
            .unwrap()
            .next_cursor
            .unwrap();
        let terminal = store
            .continue_page(&cursor, "alice", &key, BINDING)
            .unwrap();
        assert!(terminal.next_cursor.is_none());
        let replay = store
            .continue_page(&cursor, "alice", &key, BINDING)
            .unwrap();
        assert_eq!(replay.rows, terminal.rows);
        assert_eq!(replay.offset, terminal.offset);
        assert_eq!(replay.next_cursor, terminal.next_cursor);
        assert!(matches!(
            store.insert(
                "alice",
                vec![serde_json::json!({"id": 3})],
                vec!["id".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 1024,
                    tokens: 256,
                },
                30,
                BINDING,
            ),
            Err(InsertError::Full)
        ));
    }

    #[test]
    fn oversized_later_row_discards_retained_result() {
        let store = SqlPageStore::new(Duration::from_secs(60), 1, 4096, 1);
        let result = store
            .insert(
                "alice",
                vec![
                    serde_json::json!({"value": "ok"}),
                    serde_json::json!({"value": "far too large"}),
                ],
                vec!["value".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 16,
                    tokens: 16,
                },
                64,
                BINDING,
            )
            .unwrap();
        let key = [7; 32];
        let cursor = SqlPageStore::first_page(&result, &key)
            .unwrap()
            .next_cursor
            .unwrap();
        assert_eq!(
            store
                .continue_page(&cursor, "alice", &key, BINDING)
                .unwrap_err(),
            CursorError::PageLimit
        );
        assert!(store
            .insert(
                "alice",
                vec![serde_json::json!({"value": "new"})],
                vec!["value".into()],
                SqlPageLimits {
                    rows: 1,
                    bytes: 64,
                    tokens: 16,
                },
                32,
                BINDING,
            )
            .is_ok());
    }
}
