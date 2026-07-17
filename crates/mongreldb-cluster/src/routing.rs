//! Gateway routing and retry policy (spec section 11.7, Stage 2G).
//!
//! A gateway keeps, per consensus group, a small routing cache — leader hint,
//! term, metadata version, endpoint list — and consults the stable error
//! taxonomy (spec section 9.7, [`ErrorCategory::retry_class`]) to decide what
//! to do with a failed attempt. This module is pure logic: no networking and
//! no wall clock. Time enters only as caller-supplied logical [`Duration`]
//! timestamps, and backoff jitter is seeded, so every decision and every
//! backoff schedule is deterministic and testable. The transport binding
//! lands with the RPC wave; until then the caller resolves [`RetryTarget`]s
//! and performs metadata refreshes itself.
//!
//! The non-negotiable rule (spec section 11.7):
//!
//! > Never automatically replay an ambiguous write without a durable
//! > idempotency key.
//!
//! A write is retried only when it carries a durable idempotency key (the
//! Stage 1 idempotency ledger then replays the original receipt or conflicts
//! on the same key) or when the failure carries an unambiguous not-proposed
//! status. Idempotent reads retry freely, across endpoints when a replica is
//! unavailable.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

use mongreldb_types::errors::{ErrorCategory, RetryClass};
use mongreldb_types::ids::{MetadataVersion, NodeId, RaftGroupId, TabletId};

/// Identifies the routed unit a cache entry describes.
///
/// Stage 2 routes one logical database per Raft group; Stage 3 will route per
/// tablet. Both are consensus groups with at most one effective leader per
/// term (spec section 4.2), so the routing state below is identical for
/// either key.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub enum GroupKey {
    /// One Raft consensus group (Stage 2: the single database group).
    RaftGroup(RaftGroupId),
    /// One tablet's consensus group (Stage 3).
    Tablet(TabletId),
}

/// How to reach one replica of a group.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Endpoint {
    /// The replica's node identity.
    pub node_id: NodeId,
    /// Transport address (opaque to this module; resolved by the RPC wave).
    pub address: String,
}

/// A `(term, leader)` pair carried by a response, used to track leadership.
///
/// A hint is only ever trusted when its term is at least the term the cache
/// already holds: terms never move backward and there is at most one
/// effective leader per term (spec section 4.2), so a strictly older hint is
/// always stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LeaderHint {
    /// The consensus term in which `leader` leads.
    pub term: u64,
    /// The node believed to be the leader in `term`.
    pub leader: NodeId,
}

/// The routing state held for one group (spec section 11.7): leader hint,
/// term, metadata version, and endpoint list.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RoutingEntry {
    /// The highest-term leader hint observed so far, if any.
    pub leader_hint: Option<LeaderHint>,
    /// The highest consensus term observed for the group (`0` = unknown).
    pub term: u64,
    /// The control-plane metadata version this entry was built from.
    pub metadata_version: MetadataVersion,
    /// How to reach the group's replicas.
    pub endpoints: Vec<Endpoint>,
}

/// The gateway's per-group routing cache (spec section 11.7).
///
/// A plain [`std::sync::RwLock`] guards a small map: reads clone a compact
/// entry out under a shared read lock, so concurrent gateway threads never
/// serialize against each other, and writers only touch one entry at a time.
#[derive(Debug, Default)]
pub struct RoutingCache {
    entries: RwLock<HashMap<GroupKey, RoutingEntry>>,
}

impl RoutingCache {
    /// An empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or fully replaces the entry for `key`.
    ///
    /// Prefer [`Self::refresh`] for metadata-driven installs: it ignores a
    /// refresh that is older than what the cache already holds and merges
    /// leader hints by term instead of blindly overwriting.
    pub fn upsert(&self, key: GroupKey, entry: RoutingEntry) {
        self.entries
            .write()
            .expect("routing cache lock poisoned")
            .insert(key, entry);
    }

    /// Cheap concurrent read: clones the entry out under a read lock.
    pub fn get(&self, key: GroupKey) -> Option<RoutingEntry> {
        self.entries
            .read()
            .expect("routing cache lock poisoned")
            .get(&key)
            .cloned()
    }

    /// Drops the entry for `key` (the group is gone or the entry is known
    /// to be bad). Returns whether an entry was present.
    pub fn invalidate(&self, key: GroupKey) -> bool {
        self.entries
            .write()
            .expect("routing cache lock poisoned")
            .remove(&key)
            .is_some()
    }

    /// Whether the cached entry is stale relative to `current`: the control
    /// plane has published a newer metadata version than the entry was built
    /// from. A missing entry is always stale.
    pub fn is_stale(&self, key: GroupKey, current: MetadataVersion) -> bool {
        self.entries
            .read()
            .expect("routing cache lock poisoned")
            .get(&key)
            .is_none_or(|entry| entry.metadata_version < current)
    }

    /// Drops every entry built from metadata older than `current`. Returns
    /// how many entries were invalidated.
    pub fn invalidate_below(&self, current: MetadataVersion) -> usize {
        let mut entries = self.entries.write().expect("routing cache lock poisoned");
        let before = entries.len();
        entries.retain(|_, entry| entry.metadata_version >= current);
        before - entries.len()
    }

    /// Installs refreshed routing metadata for `key`.
    ///
    /// A refresh built from an older metadata version than the cached entry
    /// is ignored (returns `false`); an equal or newer version replaces the
    /// endpoint list and metadata version. Leader hints are merged by term
    /// rather than replaced: the highest-term hint wins, so a refresh that
    /// carries no hint (or an older one) never erases fresher leadership
    /// knowledge.
    pub fn refresh(
        &self,
        key: GroupKey,
        metadata_version: MetadataVersion,
        endpoints: Vec<Endpoint>,
        leader_hint: Option<LeaderHint>,
    ) -> bool {
        let mut entries = self.entries.write().expect("routing cache lock poisoned");
        match entries.get_mut(&key) {
            Some(entry) if entry.metadata_version > metadata_version => false,
            Some(entry) => {
                let leader_hint = merge_hints(entry.leader_hint, leader_hint);
                let term = leader_hint
                    .map_or(entry.term, |hint| hint.term)
                    .max(entry.term);
                *entry = RoutingEntry {
                    leader_hint,
                    term,
                    metadata_version,
                    endpoints,
                };
                true
            }
            None => {
                let term = leader_hint.map_or(0, |hint| hint.term);
                entries.insert(
                    key,
                    RoutingEntry {
                        leader_hint,
                        term,
                        metadata_version,
                        endpoints,
                    },
                );
                true
            }
        }
    }

    /// Leader-hint tracking (spec section 11.7): records a `(term, leader)`
    /// pair carried by a response, but only when its term is strictly newer
    /// than the term the cache already holds. Stale hints — and conflicting
    /// hints in the current term, which cannot name the effective leader
    /// (spec section 4.2) — are ignored. Returns whether the cache changed.
    ///
    /// A hint for a group with no entry is not actionable (there is no
    /// endpoint list to reach the leader through) and is dropped.
    pub fn apply_leader_hint(&self, key: GroupKey, hint: LeaderHint) -> bool {
        let mut entries = self.entries.write().expect("routing cache lock poisoned");
        match entries.get_mut(&key) {
            Some(entry) if hint.term > entry.term => {
                entry.term = hint.term;
                entry.leader_hint = Some(hint);
                true
            }
            _ => false,
        }
    }

    /// Number of cached groups (diagnostics).
    pub fn len(&self) -> usize {
        self.entries
            .read()
            .expect("routing cache lock poisoned")
            .len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Keeps the highest-term of two optional leader hints.
fn merge_hints(current: Option<LeaderHint>, offered: Option<LeaderHint>) -> Option<LeaderHint> {
    match (current, offered) {
        (Some(current), Some(offered)) => Some(if offered.term > current.term {
            offered
        } else {
            current
        }),
        (current @ Some(_), None) => current,
        (None, offered) => offered,
    }
}

/// One operation the gateway is routing, with its retry budget.
///
/// `deadline` is an absolute logical timestamp in the same clock domain as
/// the `now` argument passed to [`RetryPolicy::decide`]; queue wait and
/// backoff count toward it (spec section 4.7). `max_attempts` counts the
/// initial attempt, so `max_attempts == 1` disables retries entirely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationDescriptor {
    /// Whether re-executing the operation is safe from the caller's
    /// perspective. Spec section 11.7 retries *idempotent* reads freely; a
    /// read explicitly marked non-idempotent is surfaced rather than guessed
    /// at.
    pub idempotent: bool,
    /// The durable idempotency key deduplicating write replays through the
    /// Stage 1 idempotency ledger. Replayed verbatim on every retry — never
    /// regenerated.
    pub idempotency_key: Option<String>,
    /// Whether the operation mutates no state.
    pub read_only: bool,
    /// Absolute logical deadline; no retry is granted that starts or ends
    /// past it.
    pub deadline: Duration,
    /// Total attempt budget including the initial attempt.
    pub max_attempts: u32,
}

/// A failed attempt, classified through the stable taxonomy (spec 9.7).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// The stable error category of the failure.
    pub category: ErrorCategory,
    /// The `(term, leader)` hint carried by a `NotLeader` response, if any.
    pub leader_hint: Option<LeaderHint>,
    /// The server positively stated the request was never proposed, making a
    /// write replay unambiguous (spec section 11.7).
    pub not_proposed: bool,
}

impl Failure {
    /// A failure with only its category.
    pub fn new(category: ErrorCategory) -> Self {
        Self {
            category,
            leader_hint: None,
            not_proposed: false,
        }
    }

    /// Attaches the leader hint carried by the response.
    pub fn with_leader_hint(mut self, hint: LeaderHint) -> Self {
        self.leader_hint = Some(hint);
        self
    }

    /// Marks that the server positively reported the request as never
    /// proposed, so replaying it is unambiguous.
    pub fn not_proposed(mut self) -> Self {
        self.not_proposed = true;
        self
    }
}

/// Per-operation retry progress, threaded through every [`RetryPolicy::decide`]
/// call of one operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetryState {
    /// Retries granted so far (the initial attempt is not counted).
    pub attempts: u32,
    /// Metadata refreshes granted so far.
    pub metadata_refreshes: u32,
    /// Whether the one-shot refresh for a schema/cluster version mismatch
    /// has already been spent (spec section 11.7: refresh once, then
    /// surface).
    pub version_mismatch_refreshed: bool,
}

/// Where a granted retry should be sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryTarget {
    /// A specific leader, named by a fresh hint.
    Leader(NodeId),
    /// Any known endpoint of the group; idempotent reads may fail over
    /// across replicas when one is unavailable.
    AnyEndpoint,
    /// Whatever the routing cache holds at send time: the leader hint if one
    /// is known, otherwise any endpoint.
    CachedRoute,
}

/// What the gateway should do with a failed attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryAction {
    /// Retry the operation after `delay` (`Duration::ZERO` = immediately),
    /// replaying the request byte-for-byte — including the original durable
    /// idempotency key, echoed here verbatim and never regenerated.
    Retry {
        /// Where to send the retry.
        target: RetryTarget,
        /// How long to wait first.
        delay: Duration,
        /// The 1-based ordinal of this retry.
        attempt: u32,
        /// The operation's durable idempotency key, unchanged.
        idempotency_key: Option<String>,
    },
    /// Refresh routing (and, for schema mismatches, schema) metadata for the
    /// group, then retry the operation with the same idempotency key.
    RefreshMetadata {
        /// The 1-based ordinal of this retry.
        attempt: u32,
        /// The operation's durable idempotency key, unchanged.
        idempotency_key: Option<String>,
    },
    /// Do not retry; surface `category` to the caller.
    ///
    /// Categories whose [`RetryClass`] is `RetryTransaction` surface here
    /// too: restarting the whole transaction from a fresh snapshot is the
    /// session layer's job — a routing-level replay cannot help.
    Surface {
        /// The category to surface to the caller.
        category: ErrorCategory,
    },
}

/// The gateway retry policy engine (spec section 11.7).
///
/// The backoff schedule is deterministic: a pure function of
/// [`Self::jitter_seed`] and the retry index, with no wall-clock randomness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Backoff before the first retry; doubles per retry.
    pub initial_backoff: Duration,
    /// Cap for any single backoff.
    pub max_backoff: Duration,
    /// Seed for the deterministic jitter. Gateway instances may draw this
    /// from the CSPRNG at construction; tests pin it.
    pub jitter_seed: u64,
}

/// The default jitter seed, fixed so a default-constructed policy is fully
/// deterministic.
pub const DEFAULT_JITTER_SEED: u64 = 0x6D6F_6E67_7265_6C00;

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            jitter_seed: DEFAULT_JITTER_SEED,
        }
    }
}

impl RetryPolicy {
    /// The deterministic backoff before retry `retry_index` (0-based).
    ///
    /// The base doubles per index from [`Self::initial_backoff`], capped at
    /// [`Self::max_backoff`]; equal jitter then picks a value in
    /// `[base / 2, base]` via a seeded splitmix64 — same seed and index,
    /// same delay, always.
    pub fn backoff_delay(&self, retry_index: u32) -> Duration {
        let initial = micros_of(self.initial_backoff).max(1);
        let maximum = micros_of(self.max_backoff).max(initial);
        let doublings = retry_index.min(31);
        let base = initial.saturating_mul(1u64 << doublings).min(maximum);
        let half = base / 2;
        let jitter = splitmix64(self.jitter_seed ^ u64::from(retry_index)) % (half + 1);
        Duration::from_micros(half + jitter)
    }

    /// Decides the next action for a failed attempt (spec section 11.7).
    ///
    /// `key` identifies the routed group, `op` the operation and its budget,
    /// `state` the retry progress so far (updated in place when a retry is
    /// granted), `failure` what happened, `cache` the routing cache, and
    /// `now` the current logical timestamp in the deadline's clock domain.
    ///
    /// Decision order:
    ///
    /// 1. Fresh `(term, leader)` hints are recorded in the cache first —
    ///    they remain useful for later operations even when this one
    ///    surfaces.
    /// 2. A spent deadline surfaces [`ErrorCategory::DeadlineExceeded`];
    ///    nothing retries past it.
    /// 3. The write guard: a non-read operation is replayed only with a
    ///    durable idempotency key or an unambiguous not-proposed status —
    ///    an ambiguous write is never automatically replayed.
    /// 4. The category's [`ErrorCategory::retry_class`] drives the rest:
    ///    `NotLeader` uses a fresh returned hint (refreshing metadata when
    ///    the hint is absent, stale, or conflicts), `StaleMetadata` refreshes
    ///    and retries, availability categories retry with bounded backoff,
    ///    schema/cluster version mismatches refresh once and then surface,
    ///    and terminal categories surface immediately.
    pub fn decide(
        &self,
        key: GroupKey,
        op: &OperationDescriptor,
        state: &mut RetryState,
        failure: &Failure,
        cache: &RoutingCache,
        now: Duration,
    ) -> RetryAction {
        // Leader-hint tracking: a response carrying a strictly newer term
        // updates the cache even when this operation itself will not be
        // retried (stale hints are ignored inside `apply_leader_hint`).
        if let Some(hint) = failure.leader_hint {
            cache.apply_leader_hint(key, hint);
        }

        // Deadlines are never retried past (spec section 4.7).
        if now >= op.deadline {
            return RetryAction::Surface {
                category: ErrorCategory::DeadlineExceeded,
            };
        }

        // The attempt budget counts the initial attempt, so one retry is
        // granted only while another attempt remains.
        if state.attempts + 1 >= op.max_attempts {
            return RetryAction::Surface {
                category: failure.category,
            };
        }

        // The write guard (spec section 11.7): idempotent reads replay
        // freely; writes replay only with a durable idempotency key or an
        // unambiguous not-proposed status. NEVER automatically replay an
        // ambiguous write.
        let replayable = if op.read_only {
            op.idempotent
        } else {
            op.idempotency_key.is_some() || failure.not_proposed
        };
        let class = failure.category.retry_class();
        let class_retries = matches!(
            class,
            RetryClass::IdempotencyKeyRequired
                | RetryClass::AfterMetadataRefresh
                | RetryClass::AfterBackoff
        );
        if class_retries && !replayable {
            return RetryAction::Surface {
                category: failure.category,
            };
        }

        match class {
            RetryClass::Never => match failure.category {
                // A stale cache can produce a spurious version mismatch while
                // a rolling upgrade is in flight (ADR-0010): refresh once,
                // then surface if the mismatch is real.
                ErrorCategory::ClusterVersionMismatch => {
                    self.version_mismatch_refresh(op, state, failure.category)
                }
                _ => RetryAction::Surface {
                    category: failure.category,
                },
            },
            RetryClass::IdempotencyKeyRequired => {
                // CommitOutcomeUnknown is ambiguous; reaching here means the
                // write guard above already proved the replay safe (durable
                // key or unambiguous not-proposed status).
                self.grant_retry(op, state, RetryTarget::CachedRoute, Duration::ZERO)
            }
            RetryClass::AfterMetadataRefresh => match failure.category {
                ErrorCategory::NotLeader => {
                    // Use the returned leader hint when it is fresh — after
                    // the tracking update above, a fresh (or agreeing) hint
                    // is exactly the one the cache now holds. Otherwise the
                    // cached route is stale: refresh metadata and retry.
                    let cached = cache.get(key);
                    let usable_hint = failure.leader_hint.filter(|hint| {
                        cached.as_ref().is_some_and(|entry| {
                            !entry.endpoints.is_empty()
                                && entry.leader_hint.is_some_and(|held| held == *hint)
                        })
                    });
                    match usable_hint {
                        Some(hint) => self.grant_retry(
                            op,
                            state,
                            RetryTarget::Leader(hint.leader),
                            Duration::ZERO,
                        ),
                        None => self.grant_refresh(op, state),
                    }
                }
                // Retry against a refreshed schema once; a repeated mismatch
                // is real and surfaces (spec section 11.7).
                ErrorCategory::SchemaVersionMismatch => {
                    self.version_mismatch_refresh(op, state, failure.category)
                }
                // StaleMetadata, LeaderUnknown, TabletMoved, TabletSplitting:
                // refresh routing metadata, then retry.
                _ => self.grant_refresh(op, state),
            },
            RetryClass::AfterBackoff => {
                let delay = self.backoff_delay(state.attempts);
                // A backoff that would end past the deadline never starts.
                if now.saturating_add(delay) > op.deadline {
                    return RetryAction::Surface {
                        category: ErrorCategory::DeadlineExceeded,
                    };
                }
                // An unavailable replica fails idempotent reads over to
                // another endpoint; everything else waits and re-resolves
                // through the cache.
                let target =
                    if op.read_only && failure.category == ErrorCategory::ReplicaUnavailable {
                        RetryTarget::AnyEndpoint
                    } else {
                        RetryTarget::CachedRoute
                    };
                self.grant_retry(op, state, target, delay)
            }
            // Restarting the whole transaction is the session layer's job.
            RetryClass::RetryTransaction => RetryAction::Surface {
                category: failure.category,
            },
        }
    }

    /// Grants a backoff-free retry, charging one attempt.
    fn grant_retry(
        &self,
        op: &OperationDescriptor,
        state: &mut RetryState,
        target: RetryTarget,
        delay: Duration,
    ) -> RetryAction {
        state.attempts += 1;
        RetryAction::Retry {
            target,
            delay,
            attempt: state.attempts,
            idempotency_key: op.idempotency_key.clone(),
        }
    }

    /// Grants a metadata refresh followed by a retry, charging one attempt
    /// and one refresh.
    fn grant_refresh(&self, op: &OperationDescriptor, state: &mut RetryState) -> RetryAction {
        state.attempts += 1;
        state.metadata_refreshes += 1;
        RetryAction::RefreshMetadata {
            attempt: state.attempts,
            idempotency_key: op.idempotency_key.clone(),
        }
    }

    /// Schema/cluster version mismatches refresh metadata once, then surface
    /// (spec section 11.7).
    fn version_mismatch_refresh(
        &self,
        op: &OperationDescriptor,
        state: &mut RetryState,
        category: ErrorCategory,
    ) -> RetryAction {
        if state.version_mismatch_refreshed {
            return RetryAction::Surface { category };
        }
        state.version_mismatch_refreshed = true;
        self.grant_refresh(op, state)
    }
}

/// Whole-microseconds view of a [`Duration`], saturated at `u64::MAX`.
fn micros_of(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// splitmix64 finalizer: a small deterministic mixer for seeded jitter.
fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn group_key() -> GroupKey {
        GroupKey::RaftGroup(RaftGroupId::from_bytes([7; 16]))
    }

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn hint(term: u64, leader: u8) -> LeaderHint {
        LeaderHint {
            term,
            leader: node(leader),
        }
    }

    fn three_endpoints() -> Vec<Endpoint> {
        (1..=3u8)
            .map(|byte| Endpoint {
                node_id: node(byte),
                address: format!("node{byte}.test:7443"),
            })
            .collect()
    }

    fn entry(term: u64, leader: u8, version: u64) -> RoutingEntry {
        RoutingEntry {
            leader_hint: Some(hint(term, leader)),
            term,
            metadata_version: MetadataVersion::new(version),
            endpoints: three_endpoints(),
        }
    }

    fn cache_with(entry: RoutingEntry) -> RoutingCache {
        let cache = RoutingCache::new();
        cache.upsert(group_key(), entry);
        cache
    }

    fn policy() -> RetryPolicy {
        RetryPolicy {
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_secs(1),
            jitter_seed: 7,
        }
    }

    fn read_op() -> OperationDescriptor {
        OperationDescriptor {
            idempotent: true,
            idempotency_key: None,
            read_only: true,
            deadline: Duration::from_secs(30),
            max_attempts: 4,
        }
    }

    fn keyed_write_op() -> OperationDescriptor {
        OperationDescriptor {
            idempotent: false,
            idempotency_key: Some("txn-key-42".to_owned()),
            read_only: false,
            deadline: Duration::from_secs(30),
            max_attempts: 4,
        }
    }

    fn plain_write_op() -> OperationDescriptor {
        OperationDescriptor {
            idempotency_key: None,
            ..keyed_write_op()
        }
    }

    // --- RoutingCache -----------------------------------------------------

    #[test]
    fn upsert_get_and_invalidate_round_trip() {
        let cache = cache_with(entry(3, 1, 9));
        assert_eq!(cache.len(), 1);
        assert!(!cache.is_empty());
        assert_eq!(cache.get(group_key()), Some(entry(3, 1, 9)));
        assert!(cache.invalidate(group_key()));
        assert!(cache.get(group_key()).is_none());
        assert!(
            !cache.invalidate(group_key()),
            "second invalidate is a no-op"
        );
        assert!(cache.is_empty());
    }

    #[test]
    fn staleness_and_invalidation_follow_metadata_version() {
        let cache = RoutingCache::new();
        let old_key = group_key();
        let new_key = GroupKey::Tablet(TabletId::from_bytes([9; 16]));
        cache.upsert(old_key, entry(1, 1, 1));
        cache.upsert(new_key, entry(1, 1, 3));

        assert!(cache.is_stale(old_key, MetadataVersion::new(2)));
        assert!(
            !cache.is_stale(new_key, MetadataVersion::new(2)),
            "entry at v3 is fresh against control-plane v2"
        );
        assert!(
            cache.is_stale(
                GroupKey::Tablet(TabletId::from_bytes([4; 16])),
                MetadataVersion::new(1)
            ),
            "a missing entry is always stale"
        );

        assert_eq!(cache.invalidate_below(MetadataVersion::new(2)), 1);
        assert!(cache.get(old_key).is_none());
        assert!(cache.get(new_key).is_some());
    }

    #[test]
    fn refresh_installs_newer_metadata_and_ignores_older() {
        let cache = cache_with(entry(5, 1, 3));

        // An older refresh is ignored entirely.
        assert!(!cache.refresh(
            group_key(),
            MetadataVersion::new(2),
            vec![],
            Some(hint(9, 2)),
        ));
        assert_eq!(cache.get(group_key()), Some(entry(5, 1, 3)));

        // A newer refresh replaces endpoints/version but keeps the
        // higher-term hint over the older one it carries.
        let new_endpoints = vec![Endpoint {
            node_id: node(4),
            address: "node4.test:7443".to_owned(),
        }];
        assert!(cache.refresh(
            group_key(),
            MetadataVersion::new(4),
            new_endpoints.clone(),
            Some(hint(4, 2)),
        ));
        let installed = cache.get(group_key()).expect("entry present");
        assert_eq!(installed.metadata_version, MetadataVersion::new(4));
        assert_eq!(installed.endpoints, new_endpoints);
        assert_eq!(
            installed.leader_hint,
            Some(hint(5, 1)),
            "higher-term hint wins"
        );
        assert_eq!(installed.term, 5);

        // A refresh carrying a newer hint installs it and advances the term.
        assert!(cache.refresh(
            group_key(),
            MetadataVersion::new(5),
            new_endpoints,
            Some(hint(6, 3))
        ));
        let installed = cache.get(group_key()).expect("entry present");
        assert_eq!(installed.leader_hint, Some(hint(6, 3)));
        assert_eq!(installed.term, 6);
    }

    #[test]
    fn apply_leader_hint_requires_a_strictly_newer_term() {
        let cache = cache_with(entry(5, 1, 3));

        assert!(
            !cache.apply_leader_hint(group_key(), hint(4, 2)),
            "older term ignored"
        );
        assert!(
            !cache.apply_leader_hint(group_key(), hint(5, 2)),
            "conflicting leader in the same term ignored"
        );
        assert!(
            !cache.apply_leader_hint(group_key(), hint(5, 1)),
            "equal term is never an update"
        );
        assert_eq!(cache.get(group_key()), Some(entry(5, 1, 3)));

        assert!(cache.apply_leader_hint(group_key(), hint(6, 2)));
        let updated = cache.get(group_key()).expect("entry present");
        assert_eq!(updated.term, 6);
        assert_eq!(updated.leader_hint, Some(hint(6, 2)));

        // A hint for an unknown group is dropped: with no endpoint list it is
        // not actionable.
        let unknown = GroupKey::RaftGroup(RaftGroupId::from_bytes([8; 16]));
        assert!(!cache.apply_leader_hint(unknown, hint(9, 2)));
        assert!(cache.get(unknown).is_none());
    }

    #[test]
    fn concurrent_reads_and_hint_updates_do_not_deadlock() {
        let cache = Arc::new(cache_with(entry(1, 1, 1)));
        let mut handles = Vec::new();
        for _ in 0..4 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for _ in 0..500 {
                    let _ = cache.get(group_key());
                    cache.apply_leader_hint(group_key(), hint(2, 2));
                }
            }));
        }
        for handle in handles {
            handle.join().expect("reader thread panicked");
        }
        assert_eq!(
            cache.get(group_key()).and_then(|entry| entry.leader_hint),
            Some(hint(2, 2))
        );
    }

    // --- NotLeader / metadata refresh --------------------------------------

    #[test]
    fn not_leader_uses_fresh_hint_and_updates_cache() {
        let cache = cache_with(entry(3, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::NotLeader).with_leader_hint(hint(4, 2));

        let action = policy().decide(
            group_key(),
            &read_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert_eq!(
            action,
            RetryAction::Retry {
                target: RetryTarget::Leader(node(2)),
                delay: Duration::ZERO,
                attempt: 1,
                idempotency_key: None,
            }
        );
        let updated = cache.get(group_key()).expect("entry present");
        assert_eq!(updated.term, 4, "fresh hint installed");
        assert_eq!(updated.leader_hint, Some(hint(4, 2)));
    }

    #[test]
    fn not_leader_with_stale_hint_refreshes_metadata() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::NotLeader).with_leader_hint(hint(4, 2));

        let action = policy().decide(
            group_key(),
            &read_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert_eq!(
            action,
            RetryAction::RefreshMetadata {
                attempt: 1,
                idempotency_key: None,
            }
        );
        assert_eq!(
            cache.get(group_key()),
            Some(entry(5, 1, 1)),
            "stale hint leaves the cache untouched"
        );
        assert_eq!(state.metadata_refreshes, 1);
    }

    #[test]
    fn not_leader_without_hint_refreshes_metadata() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::NotLeader);

        let action = policy().decide(
            group_key(),
            &read_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert!(matches!(action, RetryAction::RefreshMetadata { .. }));
    }

    #[test]
    fn stale_metadata_refreshes_then_retries() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::StaleMetadata);

        let action = policy().decide(
            group_key(),
            &read_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert!(matches!(
            action,
            RetryAction::RefreshMetadata { attempt: 1, .. }
        ));
        assert_eq!(state.metadata_refreshes, 1);
    }

    #[test]
    fn version_mismatches_refresh_once_then_surface() {
        for category in [
            ErrorCategory::SchemaVersionMismatch,
            ErrorCategory::ClusterVersionMismatch,
        ] {
            let cache = cache_with(entry(5, 1, 1));
            let mut state = RetryState::default();
            let failure = Failure::new(category);

            let first = policy().decide(
                group_key(),
                &read_op(),
                &mut state,
                &failure,
                &cache,
                Duration::ZERO,
            );
            assert!(
                matches!(first, RetryAction::RefreshMetadata { attempt: 1, .. }),
                "{category}: first mismatch refreshes once"
            );

            let second = policy().decide(
                group_key(),
                &read_op(),
                &mut state,
                &failure,
                &cache,
                Duration::ZERO,
            );
            assert_eq!(
                second,
                RetryAction::Surface { category },
                "{category}: a repeated mismatch is real and surfaces"
            );
        }
    }

    // --- The write guard ----------------------------------------------------

    #[test]
    fn write_without_key_is_never_replayed_on_commit_outcome_unknown() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::CommitOutcomeUnknown);

        let action = policy().decide(
            group_key(),
            &plain_write_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::CommitOutcomeUnknown,
            },
            "an ambiguous write is never automatically replayed"
        );
        assert_eq!(state.attempts, 0, "no retry was granted");
    }

    #[test]
    fn write_without_key_needs_unambiguous_not_proposed_status() {
        let cache = cache_with(entry(3, 1, 1));

        // NotLeader without a not-proposed assertion: surfaces, but the fresh
        // hint still lands in the cache for the next operation.
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::NotLeader).with_leader_hint(hint(4, 2));
        let action = policy().decide(
            group_key(),
            &plain_write_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );
        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::NotLeader,
            }
        );
        assert_eq!(
            cache.get(group_key()).and_then(|entry| entry.leader_hint),
            Some(hint(4, 2)),
            "hint tracking applies even when the operation surfaces"
        );

        // The same failure with an unambiguous not-proposed status retries.
        let mut state = RetryState::default();
        let failure = failure.not_proposed();
        let action = policy().decide(
            group_key(),
            &plain_write_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );
        assert!(matches!(
            action,
            RetryAction::Retry {
                target: RetryTarget::Leader(_),
                ..
            }
        ));
    }

    #[test]
    fn write_with_key_retries_commit_outcome_unknown_with_the_same_key() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::CommitOutcomeUnknown);
        let op = keyed_write_op();

        for expected_attempt in 1..=2 {
            let action = policy().decide(
                group_key(),
                &op,
                &mut state,
                &failure,
                &cache,
                Duration::ZERO,
            );
            assert_eq!(
                action,
                RetryAction::Retry {
                    target: RetryTarget::CachedRoute,
                    delay: Duration::ZERO,
                    attempt: expected_attempt,
                    idempotency_key: Some("txn-key-42".to_owned()),
                },
                "the durable key is replayed verbatim, never regenerated"
            );
        }
    }

    #[test]
    fn non_idempotent_read_is_not_replayed() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let op = OperationDescriptor {
            idempotent: false,
            ..read_op()
        };
        let failure = Failure::new(ErrorCategory::ReplicaUnavailable);

        let action = policy().decide(
            group_key(),
            &op,
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::ReplicaUnavailable,
            }
        );
    }

    // --- Reads across endpoints / backoff -----------------------------------

    #[test]
    fn idempotent_read_fails_over_across_endpoints() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::ReplicaUnavailable);

        let action = policy().decide(
            group_key(),
            &read_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        match action {
            RetryAction::Retry {
                target,
                delay,
                attempt,
                ..
            } => {
                assert_eq!(
                    target,
                    RetryTarget::AnyEndpoint,
                    "reads may try another replica"
                );
                assert_eq!(attempt, 1);
                assert_eq!(
                    delay,
                    policy().backoff_delay(0),
                    "bounded deterministic backoff"
                );
            }
            other => panic!("expected retry, got {other:?}"),
        }
    }

    #[test]
    fn quorum_unavailable_retries_with_bounded_backoff_then_surfaces() {
        let cache = cache_with(entry(5, 1, 1));
        let op = OperationDescriptor {
            max_attempts: 3,
            ..keyed_write_op()
        };
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::QuorumUnavailable);
        let policy = policy();

        for index in 0..2u32 {
            let action = policy.decide(
                group_key(),
                &op,
                &mut state,
                &failure,
                &cache,
                Duration::ZERO,
            );
            match action {
                RetryAction::Retry {
                    target,
                    delay,
                    attempt,
                    idempotency_key,
                } => {
                    assert_eq!(target, RetryTarget::CachedRoute);
                    assert_eq!(attempt, index + 1);
                    assert_eq!(delay, policy.backoff_delay(index));
                    assert!(delay <= policy.max_backoff, "backoff stays bounded");
                    assert_eq!(idempotency_key, Some("txn-key-42".to_owned()));
                }
                other => panic!("expected bounded backoff retry, got {other:?}"),
            }
        }

        let action = policy.decide(
            group_key(),
            &op,
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );
        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::QuorumUnavailable,
            },
            "quorum loss surfaces once the attempt budget is spent"
        );
    }

    #[test]
    fn transaction_conflicts_surface_for_session_level_restart() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::TransactionConflict);

        let action = policy().decide(
            group_key(),
            &keyed_write_op(),
            &mut state,
            &failure,
            &cache,
            Duration::ZERO,
        );

        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::TransactionConflict,
            },
            "RetryTransaction classes surface: the session layer restarts the whole transaction"
        );
    }

    // --- Budgets: attempts and deadline -------------------------------------

    #[test]
    fn attempt_cap_is_enforced() {
        let cache = cache_with(entry(5, 1, 1));
        let op = OperationDescriptor {
            max_attempts: 3,
            ..read_op()
        };
        let mut state = RetryState::default();
        let failure = Failure::new(ErrorCategory::QuorumUnavailable);
        let policy = policy();

        assert!(matches!(
            policy.decide(
                group_key(),
                &op,
                &mut state,
                &failure,
                &cache,
                Duration::ZERO
            ),
            RetryAction::Retry { attempt: 1, .. }
        ));
        assert!(matches!(
            policy.decide(
                group_key(),
                &op,
                &mut state,
                &failure,
                &cache,
                Duration::ZERO
            ),
            RetryAction::Retry { attempt: 2, .. }
        ));
        assert_eq!(
            policy.decide(
                group_key(),
                &op,
                &mut state,
                &failure,
                &cache,
                Duration::ZERO
            ),
            RetryAction::Surface {
                category: ErrorCategory::QuorumUnavailable,
            },
            "the third failure exhausts max_attempts = 3 (initial + 2 retries)"
        );
    }

    #[test]
    fn spent_deadline_surfaces_deadline_exceeded() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let op = OperationDescriptor {
            deadline: Duration::from_secs(5),
            ..read_op()
        };
        let failure = Failure::new(ErrorCategory::ReplicaUnavailable);

        let action = policy().decide(
            group_key(),
            &op,
            &mut state,
            &failure,
            &cache,
            Duration::from_secs(5),
        );

        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::DeadlineExceeded,
            }
        );
    }

    #[test]
    fn backoff_that_would_cross_the_deadline_never_starts() {
        let cache = cache_with(entry(5, 1, 1));
        let mut state = RetryState::default();
        let op = OperationDescriptor {
            deadline: Duration::from_millis(44),
            ..read_op()
        };
        // First backoff is at least initial_backoff / 2 = 5ms; now (40ms)
        // plus any delay >= 5ms crosses the 44ms deadline.
        let failure = Failure::new(ErrorCategory::QuorumUnavailable);

        let action = policy().decide(
            group_key(),
            &op,
            &mut state,
            &failure,
            &cache,
            Duration::from_millis(40),
        );

        assert_eq!(
            action,
            RetryAction::Surface {
                category: ErrorCategory::DeadlineExceeded,
            },
            "queue wait and backoff count toward the deadline"
        );
    }

    // --- Backoff schedule ----------------------------------------------------

    #[test]
    fn backoff_schedule_is_deterministic_and_bounded() {
        let policy = policy();
        let schedule: Vec<Duration> = (0..8).map(|index| policy.backoff_delay(index)).collect();
        let replayed: Vec<Duration> = (0..8).map(|index| policy.backoff_delay(index)).collect();
        assert_eq!(schedule, replayed, "same seed and index, same delay");

        let other_seed = RetryPolicy {
            jitter_seed: 8,
            ..policy
        };
        let other: Vec<Duration> = (0..8)
            .map(|index| other_seed.backoff_delay(index))
            .collect();
        assert_ne!(schedule, other, "the seed drives the jitter");

        // Equal jitter keeps every delay within [base / 2, base] of the
        // doubling, capped base.
        for (index, delay) in schedule.iter().enumerate() {
            let base = policy
                .initial_backoff
                .saturating_mul(1u32 << index)
                .min(policy.max_backoff);
            let lower = base / 2;
            assert!(
                *delay >= lower && *delay <= base,
                "index {index}: {delay:?} within [{lower:?}, {base:?}]"
            );
        }

        // High indices saturate at the cap, never above.
        let capped = policy.backoff_delay(30);
        assert!(capped <= policy.max_backoff);
        assert!(capped >= policy.max_backoff / 2);
    }
}
