//! Tablet identity, partitioning, local layout, and tablet routing (spec
//! sections 12.1-12.4, Stages 3B/3C/3D).
//!
//! Stage 3B (spec section 12.2) lands the partition model: [`Partitioning`]
//! (hash, range, tenant, time-range) with partition-key extraction through
//! the order-preserving [`RowKeyEncoder`], the slot mapping
//! [`Partitioning::route`], the per-table metadata shape
//! [`TablePartitioningRecord`] (whose [`PartitioningOrigin`] keeps automatic
//! defaults visible in schema metadata), and the colocation declaration
//! [`ColocatedWith`].
//!
//! Stage 3C (spec section 12.3) lands the tablet descriptor family of spec
//! section 12.1 ([`TabletDescriptor`], [`ReplicaDescriptor`], [`TabletState`]
//! with its transition graph enforced in code, [`PartitionBounds`] over typed
//! [`Key`] bytes) and the on-node layout [`TabletLayout`]:
//!
//! ```text
//! node-data/
//!   tablets/<tablet-id>/{state,runs,indexes,temp}   + tablet.json
//!   groups/<raft-group-id>/{raft,snapshots}
//! ```
//!
//! The per-tablet metadata file is versioned, checksummed, and written
//! atomically with the same idiom as the node identity in [`crate::node`];
//! loading fails closed on a missing, corrupt, unknown-version, or
//! wrong-tablet file. One tablet storage core is owned by one node process:
//! [`TabletOwnershipRegistry`] is the process-local half of that rule (the
//! storage core's file lease remains the cross-process half), mirroring the
//! open-reservation concept of the Stage 1 shared-core registry (S1A-002).
//!
//! Stage 3D (spec section 12.4) lands pure tablet-selection helpers:
//! [`find_tablet_for_key`] routes point reads/writes directly,
//! [`tablets_overlapping`] fans range queries out to every overlapping
//! tablet, and [`check_generation`] classifies a stale request generation as
//! the typed [`RoutingError`] the gateway maps onto `TabletMoved`,
//! `TabletSplitting`, or `StaleMetadata` before refreshing and retrying
//! through [`crate::routing`].
//!
//! Stages 3E/3F (spec sections 12.5-12.6) live in [`crate::split`] and
//! [`crate::merge`]; this file carries their descriptor-side helpers —
//! [`PartitionBounds::split_at`], [`PartitionBounds::meets_start_of`],
//! [`PartitionBounds::union_adjacent`],
//! [`TabletDescriptor::published_transition`] (the one-publication/one-tick
//! generation rules are documented on [`TabletDescriptor`]), and
//! [`TabletLayout::teardown`] for source-replica removal. The enforced
//! [`TabletState`] graph needs no new edges: child publication rides
//! `Creating -> Active`, source retirement `Splitting`/`Merging ->
//! `Retiring -> Retired`, and abort paths ride the existing
//! `Splitting`/`Merging -> Active` and `Creating -> Retired` edges.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs;
use std::ops;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use mongreldb_types::errors::ErrorCategory;
use mongreldb_types::ids::{NodeId, RaftGroupId, TableId, TabletId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::node::ClusterError;

// ---------------------------------------------------------------------------
// Column identifiers
// ---------------------------------------------------------------------------

/// Column identifier inside one table.
///
/// Mirrors the storage core's `u16` column ids (spec section 12.2 declares
/// partition keys over `ColumnId`); the cluster crate deliberately does not
/// depend on the core, so the newtype is declared here and must track it.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
#[repr(transparent)]
pub struct ColumnId(pub u16);

impl ColumnId {
    /// Wraps a raw column id.
    pub const fn new(id: u16) -> Self {
        Self(id)
    }

    /// The raw column id.
    pub const fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ColumnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// Typed key bytes
// ---------------------------------------------------------------------------

/// Typed key bytes produced by [`RowKeyEncoder`].
///
/// The encoding is order-preserving: the lexicographic byte order of two
/// [`Key`]s matches the typed order of the values they encode, which is what
/// lets [`PartitionBounds`] range over bytes while partitioning ranges over
/// typed values.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key(Vec<u8>);

impl Key {
    /// Wraps raw encoded bytes without copying.
    pub const fn from_bytes(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Borrows the encoded bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the key, returning the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for Key {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl fmt::Display for Key {
    /// Lowercase hexadecimal of the encoded bytes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex_encode(&self.0))
    }
}

impl Serialize for Key {
    /// Human-readable serializers (e.g. JSON) receive lowercase hex; binary
    /// serializers receive the raw byte string.
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&hex_encode(&self.0))
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct KeyVisitor;

        impl<'v> serde::de::Visitor<'v> for KeyVisitor {
            type Value = Key;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a hex string or byte sequence")
            }

            fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Key, E> {
                hex_decode(text)
                    .map(Key)
                    .map_err(|detail| E::custom(format!("invalid key hex: {detail}")))
            }

            fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Key, E> {
                Ok(Key(bytes.to_vec()))
            }

            fn visit_byte_buf<E: serde::de::Error>(self, bytes: Vec<u8>) -> Result<Key, E> {
                Ok(Key(bytes))
            }

            fn visit_seq<A: serde::de::SeqAccess<'v>>(self, mut seq: A) -> Result<Key, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(byte) = seq.next_element::<u8>()? {
                    bytes.push(byte);
                }
                Ok(Key(bytes))
            }
        }

        if deserializer.is_human_readable() {
            deserializer.deserialize_str(KeyVisitor)
        } else {
            deserializer.deserialize_bytes(KeyVisitor)
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("odd number of hex digits".to_owned());
    }
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 2);
    for pair in bytes.chunks_exact(2) {
        let digit = |byte: u8| -> Result<u8, String> {
            (byte as char)
                .to_digit(16)
                .map(|value| value as u8)
                .ok_or_else(|| format!("invalid hex character `{}`", byte as char))
        };
        out.push((digit(pair[0])? << 4) | digit(pair[1])?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Partition bounds
// ---------------------------------------------------------------------------

/// One endpoint of a partition range.
///
/// Mirrors [`std::ops::Bound`] with a serde-friendly shape: the standard
/// enum carries no `Serialize`/`Deserialize`, and tablet descriptors must
/// persist their bounds (spec section 12.1). Declaration order is frozen;
/// variants are never reused (spec section 4.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Bound<T> {
    /// No endpoint; the range extends without limit on this side.
    Unbounded,
    /// The endpoint value is part of the range.
    Included(T),
    /// The endpoint value is not part of the range.
    Excluded(T),
}

impl<T: Clone> Bound<T> {
    /// Converts from the standard-library bound.
    pub fn from_std(bound: ops::Bound<T>) -> Self {
        match bound {
            ops::Bound::Unbounded => Self::Unbounded,
            ops::Bound::Included(value) => Self::Included(value),
            ops::Bound::Excluded(value) => Self::Excluded(value),
        }
    }
}

/// The key range one tablet covers: `low` to `high` over typed [`Key`] bytes
/// (spec section 12.1).
///
/// Tablets partition the whole key space, so well-formed bounds never leave
/// gaps or overlaps between routable tablets of one table;
/// [`Self::overlaps`] and [`Self::is_adjacent_to`] are the predicates split,
/// merge, and range routing are built on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionBounds {
    /// First key of the tablet (`Unbounded` for the first tablet of a table).
    pub low: Bound<Key>,
    /// End key of the tablet (`Unbounded` for the last tablet of a table).
    pub high: Bound<Key>,
}

impl PartitionBounds {
    /// Builds bounds, rejecting empty or inverted ranges.
    pub fn new(low: Bound<Key>, high: Bound<Key>) -> Result<Self, TabletError> {
        let bounds = Self { low, high };
        bounds.validate()?;
        Ok(bounds)
    }

    /// The whole key space.
    pub fn unbounded() -> Self {
        Self {
            low: Bound::Unbounded,
            high: Bound::Unbounded,
        }
    }

    /// The bounds are non-empty: `low` is strictly below `high`, except that
    /// the single-point range `[k, k]` (both endpoints included) is allowed.
    pub fn validate(&self) -> Result<(), TabletError> {
        if let (Some(low), Some(high)) = (bound_key(&self.low), bound_key(&self.high)) {
            if low > high {
                return Err(TabletError::InvalidBounds(format!(
                    "low endpoint {low} is above high endpoint {high}"
                )));
            }
            let single_point =
                matches!(self.low, Bound::Included(_)) && matches!(self.high, Bound::Included(_));
            if low == high && !single_point {
                return Err(TabletError::InvalidBounds(format!(
                    "empty range at endpoint {low}"
                )));
            }
        }
        Ok(())
    }

    /// Whether `key` lies inside the bounds.
    pub fn contains(&self, key: &Key) -> bool {
        let above_low = match &self.low {
            Bound::Unbounded => true,
            Bound::Included(low) => key >= low,
            Bound::Excluded(low) => key > low,
        };
        let below_high = match &self.high {
            Bound::Unbounded => true,
            Bound::Included(high) => key <= high,
            Bound::Excluded(high) => key < high,
        };
        above_low && below_high
    }

    /// Whether the two ranges share at least one key.
    pub fn overlaps(&self, other: &Self) -> bool {
        !ends_before(&self.high, &other.low) && !ends_before(&other.high, &self.low)
    }

    /// Whether the two ranges touch with no gap and no overlap: the touching
    /// endpoints name the same key and exactly one of them includes it (for
    /// example `[a, c)` and `[c, b)` are adjacent; `[a, c]` and `[c, b)`
    /// overlap at `c`; `[a, c)` and `(c, b]` leave `c` uncovered).
    pub fn is_adjacent_to(&self, other: &Self) -> bool {
        meets(&self.high, &other.low) || meets(&other.high, &self.low)
    }

    /// Whether `self` ends exactly where `other` begins — the lower half of
    /// an adjacent pair (spec section 12.6 merge ordering).
    pub fn meets_start_of(&self, other: &Self) -> bool {
        meets(&self.high, &other.low)
    }

    /// Splits the range at `key` into the lower half `[low, key)` and the
    /// upper half `[key, high)` (spec section 12.5 step 2). The halves meet at
    /// `key` ([`Self::meets_start_of`]), so they partition the original range
    /// with no gap and no overlap.
    ///
    /// Returns `None` — never a partial split — when `key` is not contained
    /// in the range or would leave either half empty (for example a split at
    /// an included `low` endpoint).
    pub fn split_at(&self, key: &Key) -> Option<(Self, Self)> {
        if !self.contains(key) {
            return None;
        }
        let lower = Self::new(self.low.clone(), Bound::Excluded(key.clone())).ok()?;
        let upper = Self::new(Bound::Included(key.clone()), self.high.clone()).ok()?;
        Some((lower, upper))
    }

    /// The combined bounds of two adjacent ranges: the lower range's `low`
    /// and the upper range's `high` (spec section 12.6). Returns `None` when
    /// the ranges are not adjacent (overlapping or disjoint) — adjacent valid
    /// ranges always combine into a valid range.
    pub fn union_adjacent(&self, other: &Self) -> Option<Self> {
        let (lower, upper) = if self.meets_start_of(other) {
            (self, other)
        } else if other.meets_start_of(self) {
            (other, self)
        } else {
            return None;
        };
        Self::new(lower.low.clone(), upper.high.clone()).ok()
    }
}

/// The endpoint key of a bound, if bounded.
fn bound_key(bound: &Bound<Key>) -> Option<&Key> {
    match bound {
        Bound::Unbounded => None,
        Bound::Included(key) | Bound::Excluded(key) => Some(key),
    }
}

/// Whether a range ending at `high` ends strictly before a range starting at
/// `low` begins. Touching endpoints count as ending before only when at least
/// one side excludes the shared key.
fn ends_before(high: &Bound<Key>, low: &Bound<Key>) -> bool {
    let (Some(high_key), Some(low_key)) = (bound_key(high), bound_key(low)) else {
        return false;
    };
    if high_key != low_key {
        return high_key < low_key;
    }
    // Same endpoint key: the ranges share it only when both sides include it.
    !(matches!(high, Bound::Included(_)) && matches!(low, Bound::Included(_)))
}

/// Whether `high` and `low` meet: same key, exactly one side included.
fn meets(high: &Bound<Key>, low: &Bound<Key>) -> bool {
    match (high, low) {
        (Bound::Included(high), Bound::Excluded(low))
        | (Bound::Excluded(high), Bound::Included(low)) => high == low,
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Tablet descriptors (spec section 12.1)
// ---------------------------------------------------------------------------

/// Lifecycle state of a tablet (spec sections 12.1, 12.5, 12.6). Declaration
/// order is frozen; variants are never reused (spec section 4.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TabletState {
    /// Created as learners and catching up; never routed to (spec section
    /// 12.5: do not expose child tablets before catch-up).
    Creating,
    /// Serving traffic.
    Active,
    /// Split in flight: the source remains the routable owner until the
    /// children are published atomically (spec section 12.5).
    Splitting,
    /// Merge in flight: the sources remain routable until the hidden
    /// replacement is published atomically (spec section 12.6).
    Merging,
    /// Retired from routing by an atomic publication; retained until no
    /// old-generation requests or pins remain (spec section 12.5 step 10).
    Retiring,
    /// Fully retired; replicas may be removed. Terminal: tablet identifiers
    /// are never reused (spec section 7).
    Retired,
}

impl TabletState {
    /// Whether the transition `self -> next` is permitted.
    ///
    /// The graph (enforced here, applied at quorum by the meta group):
    ///
    /// ```text
    /// Creating  -> Active | Retired          (catch-up done / creation aborted)
    /// Active    -> Splitting | Merging | Retiring
    /// Splitting -> Active | Retiring         (split aborted / children published)
    /// Merging   -> Active | Retiring         (merge aborted / replacement published)
    /// Retiring  -> Retired                   (no old-generation pins remain)
    /// Retired   -> (terminal)
    /// ```
    pub fn can_transition_to(self, next: Self) -> bool {
        use TabletState::{Active, Creating, Merging, Retired, Retiring, Splitting};
        matches!(
            (self, next),
            (Creating, Active)
                | (Creating, Retired)
                | (Active, Splitting)
                | (Active, Merging)
                | (Active, Retiring)
                | (Splitting, Active)
                | (Splitting, Retiring)
                | (Merging, Active)
                | (Merging, Retiring)
                | (Retiring, Retired)
        )
    }

    /// Whether requests may be routed to this tablet: the serving owner
    /// (`Active`) and the sources of an in-flight split or merge, which stay
    /// authoritative until the atomic publication flips them to
    /// [`TabletState::Retiring`]. `Creating` tablets are never exposed (spec
    /// section 12.5).
    pub fn is_routable(self) -> bool {
        matches!(self, Self::Active | Self::Splitting | Self::Merging)
    }
}

impl fmt::Display for TabletState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Creating => "Creating",
            Self::Active => "Active",
            Self::Splitting => "Splitting",
            Self::Merging => "Merging",
            Self::Retiring => "Retiring",
            Self::Retired => "Retired",
        };
        f.write_str(name)
    }
}

/// Role of one replica within its tablet's Raft group (spec section 12.1).
/// Declaration order is frozen; variants are never reused (spec section
/// 4.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ReplicaRole {
    /// Voting member of the group; counts toward quorum.
    Voter,
    /// Non-voting member receiving the log; promoted to voter during the
    /// movement protocol (spec section 12.7).
    Learner,
    /// Non-voting specialized read replica (spec §13.5, Stage 4E).
    ReadReplica,
    /// Non-voting AI-optimized read replica (larger ANN indexes).
    AiReadReplica,
    /// Non-voting analytics replica (columnar projections, longer history).
    AnalyticsReplica,
    /// Non-voting backup replica (snapshot retention).
    BackupReplica,
}

impl ReplicaRole {
    /// Whether this role counts toward Raft quorum (only [`Self::Voter`]).
    pub const fn counts_toward_quorum(self) -> bool {
        matches!(self, Self::Voter)
    }

    /// Whether the replica applies the committed log (all roles do).
    pub const fn applies_log(self) -> bool {
        true
    }

    /// Whether lag-based routing may select this replica for reads.
    pub const fn serves_reads(self) -> bool {
        !matches!(self, Self::BackupReplica)
    }
}

impl fmt::Display for ReplicaRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Voter => "Voter",
            Self::Learner => "Learner",
            Self::ReadReplica => "ReadReplica",
            Self::AiReadReplica => "AiReadReplica",
            Self::AnalyticsReplica => "AnalyticsReplica",
            Self::BackupReplica => "BackupReplica",
        };
        f.write_str(name)
    }
}

/// One replica of a tablet (spec section 12.1).
///
/// `raft_node_id` is the replica's identifier inside the tablet's Raft
/// group: the `RaftNodeId` (`u64`) of `mongreldb-consensus`, allocated by the
/// meta control plane and never reused within one group. The mapping between
/// the cluster-wide [`NodeId`] and the per-group `raft_node_id` is replicated
/// meta-group state (spec section 12.1); this descriptor carries both so
/// routers and placement never re-resolve it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaDescriptor {
    /// Cluster-wide identity of the node holding the replica.
    pub node_id: NodeId,
    /// Voter or learner.
    pub role: ReplicaRole,
    /// Openraft node id within the tablet's Raft group (see above).
    pub raft_node_id: u64,
}

/// One independently replicated data partition (spec section 12.1).
///
/// `generation` advances at every atomic descriptor publication (split,
/// merge, move); every request carries the generation it was routed with,
/// and a mismatch is classified by [`check_generation`].
///
/// Publication generation rules (spec sections 12.5-12.6; the protocols live
/// in [`crate::split`] and [`crate::merge`]):
///
/// - Split: the source is marked [`TabletState::Splitting`] at `g + 1` (`g` =
///   the pre-split generation); the children are created `Creating` at
///   `g + 1` and are never routed to; the atomic routing publication assigns
///   one new generation `p = (source generation at publication) + 1`, taking
///   the children `Creating -> Active` and the source `Splitting -> Retiring`
///   together. A request holding `g` against the splitting source is
///   classified [`RoutingError::TabletSplit`]; after publication any
///   generation below `p` against the retiring source is
///   [`RoutingError::TabletMoved`].
/// - Merge: each source is marked [`TabletState::Merging`] at its own
///   `g + 1`; the publication generation is
///   `p = max(sources' generations at publication) + 1`, assigned to the
///   replacement (`Creating -> Active`) and to both sources
///   (`Merging -> Retiring`) in one atomic command.
/// - Source removal publishes `Retiring -> Retired` at `p + 1` before the
///   descriptor is deleted.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TabletDescriptor {
    /// The tablet's identity.
    pub tablet_id: TabletId,
    /// Table the tablet belongs to.
    pub table_id: TableId,
    /// Logical database owning the table (meta-resolved). When zero on a
    /// legacy descriptor, the runtime resolves via meta
    /// `table_id → database_id` before falling back to a deterministic
    /// raft-group-derived id for pre-metadata tablets only.
    #[serde(default = "crate::tablet::zero_database_id")]
    pub database_id: mongreldb_types::ids::DatabaseId,
    /// Raft group replicating the tablet.
    pub raft_group_id: RaftGroupId,
    /// Key range the tablet covers.
    pub partition: PartitionBounds,
    /// All replicas (voters and learners), on distinct nodes.
    pub replicas: Vec<ReplicaDescriptor>,
    /// Last known leader, when the meta plane has observed one.
    pub leader_hint: Option<NodeId>,
    /// Descriptor generation; bumped by every atomic publication.
    pub generation: u64,
    /// Lifecycle state; transitions go through [`Self::try_transition`].
    pub state: TabletState,
}

fn zero_database_id() -> mongreldb_types::ids::DatabaseId {
    mongreldb_types::ids::DatabaseId::ZERO
}

impl TabletDescriptor {
    /// Structural validation: reserved identifiers, well-formed bounds,
    /// distinct replica nodes and Raft ids, a leader hint that names a
    /// replica, and at least one voter outside [`TabletState::Creating`].
    pub fn validate(&self) -> Result<(), TabletError> {
        if self.tablet_id == TabletId::ZERO {
            return Err(TabletError::InvalidDescriptor(
                "reserved all-zero tablet id".to_owned(),
            ));
        }
        if self.raft_group_id == RaftGroupId::ZERO {
            return Err(TabletError::InvalidDescriptor(
                "reserved all-zero raft group id".to_owned(),
            ));
        }
        self.partition.validate()?;
        if self.replicas.is_empty() {
            return Err(TabletError::InvalidDescriptor(
                "tablet has no replicas".to_owned(),
            ));
        }
        for (index, replica) in self.replicas.iter().enumerate() {
            if self.replicas[..index]
                .iter()
                .any(|prior| prior.node_id == replica.node_id)
            {
                return Err(TabletError::InvalidDescriptor(format!(
                    "node {} holds more than one replica",
                    replica.node_id
                )));
            }
            if self.replicas[..index]
                .iter()
                .any(|prior| prior.raft_node_id == replica.raft_node_id)
            {
                return Err(TabletError::InvalidDescriptor(format!(
                    "raft node id {} is used by more than one replica",
                    replica.raft_node_id
                )));
            }
        }
        if let Some(leader) = self.leader_hint {
            if !self
                .replicas
                .iter()
                .any(|replica| replica.node_id == leader)
            {
                return Err(TabletError::InvalidDescriptor(format!(
                    "leader hint {leader} is not a replica of the tablet"
                )));
            }
        }
        if self.state != TabletState::Creating && self.voter_count() == 0 {
            return Err(TabletError::InvalidDescriptor(format!(
                "tablet in state {} has no voters",
                self.state
            )));
        }
        Ok(())
    }

    /// The voter replicas.
    pub fn voters(&self) -> impl Iterator<Item = &ReplicaDescriptor> {
        self.replicas
            .iter()
            .filter(|replica| replica.role == ReplicaRole::Voter)
    }

    /// The learner replicas.
    pub fn learners(&self) -> impl Iterator<Item = &ReplicaDescriptor> {
        self.replicas
            .iter()
            .filter(|replica| replica.role == ReplicaRole::Learner)
    }

    /// Number of voting replicas.
    pub fn voter_count(&self) -> usize {
        self.voters().count()
    }

    /// The replica on `node`, if any.
    pub fn replica_on(&self, node: NodeId) -> Option<&ReplicaDescriptor> {
        self.replicas.iter().find(|replica| replica.node_id == node)
    }

    /// Transitions to `next`, enforcing the [`TabletState`] graph. Generation
    /// bumps are the meta group's business — this changes only the state.
    pub fn try_transition(&mut self, next: TabletState) -> Result<(), TabletError> {
        if !self.state.can_transition_to(next) {
            return Err(TabletError::InvalidStateTransition {
                tablet: self.tablet_id,
                from: self.state,
                to: next,
            });
        }
        self.state = next;
        Ok(())
    }

    /// A copy of the descriptor transitioned to `next` with the generation
    /// bumped by one: the shape of one atomic descriptor publication (see the
    /// publication generation rules on [`TabletDescriptor`]). The descriptor
    /// itself is unchanged, so the caller stages the published form and the
    /// meta plane applies it last-writer-wins.
    pub fn published_transition(&self, next: TabletState) -> Result<Self, TabletError> {
        let mut published = self.clone();
        published.try_transition(next)?;
        published.generation =
            published
                .generation
                .checked_add(1)
                .ok_or(TabletError::InvalidDescriptor(
                    "descriptor generation overflows u64".to_owned(),
                ))?;
        Ok(published)
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// The one error type of the tablet surface: descriptors, layout, and
/// ownership.
#[derive(Debug, thiserror::Error)]
pub enum TabletError {
    /// A durable metadata file failed the node.rs idiom's checks (unknown
    /// version, corrupt payload, I/O). Always fail closed.
    #[error("tablet metadata operation failed: {0}")]
    Metadata(#[from] ClusterError),
    /// No `tablet.json` exists where a tablet was expected.
    #[error("tablet metadata is missing at {0}")]
    MissingMetadata(PathBuf),
    /// The persisted metadata names a different tablet or Raft group than the
    /// directory it lives in.
    #[error(
        "tablet directory {path} holds metadata for tablet {found} / group {found_group}, \
         expected tablet {expected} / group {expected_group}"
    )]
    TabletMismatch {
        /// The tablet directory.
        path: PathBuf,
        /// Tablet id implied by the directory name.
        expected: TabletId,
        /// Tablet id the metadata names.
        found: TabletId,
        /// Raft group id implied by the layout.
        expected_group: RaftGroupId,
        /// Raft group id the metadata names.
        found_group: RaftGroupId,
    },
    /// `create` found existing, different metadata; never silently replaced.
    #[error("tablet directory {0} already holds different tablet metadata")]
    MetadataConflict(PathBuf),
    /// The requested [`TabletState`] transition is not in the graph.
    #[error("invalid tablet state transition for tablet {tablet}: {from} -> {to}")]
    InvalidStateTransition {
        /// The tablet whose state was to change.
        tablet: TabletId,
        /// Current state.
        from: TabletState,
        /// Requested target state.
        to: TabletState,
    },
    /// Empty or inverted partition bounds.
    #[error("invalid partition bounds: {0}")]
    InvalidBounds(String),
    /// A [`TabletDescriptor`] failed structural validation.
    #[error("invalid tablet descriptor: {0}")]
    InvalidDescriptor(String),
    /// Partitioning failed.
    #[error("partitioning failed: {0}")]
    Partition(#[from] PartitionError),
    /// Another tablet storage core in this process already owns the tablet
    /// directory (spec section 4.1; mirrors S1A-002).
    #[error("tablet storage core at {path} is already owned in this process by tablet {tablet}")]
    AlreadyOwned {
        /// Tablet holding the reservation.
        tablet: TabletId,
        /// The contested tablet directory.
        path: PathBuf,
    },
}

/// Partition-key extraction or slot routing failed (spec section 12.2).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PartitionError {
    /// The [`Partitioning`] declaration itself is malformed.
    #[error("invalid partitioning: {0}")]
    InvalidPartitioning(String),
    /// A declared partition-key column was absent from the supplied values.
    #[error("partition key column {column} is missing from the supplied values")]
    MissingPartitionColumn {
        /// The column that was looked up.
        column: ColumnId,
    },
    /// A partition-key column carried a value of the wrong type.
    #[error("partition key column {column} is {found}, expected {expected}")]
    PartitionColumnType {
        /// The column whose value failed the type check.
        column: ColumnId,
        /// The required value kind.
        expected: &'static str,
        /// The supplied value kind.
        found: &'static str,
    },
    /// An encoded [`Key`] could not be decoded back into components.
    #[error("malformed encoded key: {0}")]
    MalformedKey(String),
    /// A time-range timestamp before the Unix epoch maps to a negative slot.
    #[error("time-range partition slot is negative for pre-epoch timestamp {micros} micros")]
    NegativeSlot {
        /// The offending timestamp, in microseconds since the Unix epoch.
        micros: i64,
    },
}

/// A request arrived with a tablet generation that no longer matches (spec
/// section 12.4). The gateway refreshes metadata and retries safe operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RoutingError {
    /// The tablet moved (or retired); refresh routing metadata.
    #[error(
        "tablet {tablet_id} moved: request used generation {used_generation}, \
         current generation is {current_generation}"
    )]
    TabletMoved {
        /// The tablet the request targeted.
        tablet_id: TabletId,
        /// Generation the request was routed with.
        used_generation: u64,
        /// Generation the replica now holds.
        current_generation: u64,
    },
    /// The tablet is mid-split; retry once the split publishes.
    #[error(
        "tablet {tablet_id} is splitting: request used generation {used_generation}, \
         current generation is {current_generation}"
    )]
    TabletSplit {
        /// The tablet the request targeted.
        tablet_id: TabletId,
        /// Generation the request was routed with.
        used_generation: u64,
        /// Generation the replica now holds.
        current_generation: u64,
    },
    /// The request's routing metadata is stale for any other reason.
    #[error(
        "stale tablet metadata for tablet {tablet_id}: request used generation \
         {used_generation}, current generation is {current_generation}"
    )]
    StaleMetadata {
        /// The tablet the request targeted.
        tablet_id: TabletId,
        /// Generation the request was routed with.
        used_generation: u64,
        /// Generation the replica now holds.
        current_generation: u64,
    },
}

impl RoutingError {
    /// The stable error category the gateway maps this onto (spec sections
    /// 9.7, 12.4); all three refresh routing metadata and retry safe
    /// operations.
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::TabletMoved { .. } => ErrorCategory::TabletMoved,
            Self::TabletSplit { .. } => ErrorCategory::TabletSplitting,
            Self::StaleMetadata { .. } => ErrorCategory::StaleMetadata,
        }
    }
}

// ---------------------------------------------------------------------------
// Row key encoding (spec section 12.2)
// ---------------------------------------------------------------------------

/// One typed key component fed to [`RowKeyEncoder`].
///
/// The supported type tags are deliberately lean — they cover the partition
/// keys of spec section 12.2 (numeric, textual, timestamp). Extension rule
/// (spec section 4.10): a new value kind is a new variant with a fresh tag
/// byte appended at the end of the tag space; tags are never reused or
/// reordered, because keys persist in tablet bounds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum KeyValue {
    /// SQL NULL; sorts before every other value.
    Null,
    /// Boolean; `false` sorts before `true`.
    Bool(bool),
    /// Signed 64-bit integer.
    Int(i64),
    /// Microseconds since the Unix epoch (the time-range partition type).
    TimestampMicros(i64),
    /// UTF-8 text.
    Text(String),
    /// Opaque bytes.
    Bytes(Vec<u8>),
}

impl KeyValue {
    /// Stable name of the value kind, for typed errors.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Bool(_) => "bool",
            Self::Int(_) => "int",
            Self::TimestampMicros(_) => "timestamp-micros",
            Self::Text(_) => "text",
            Self::Bytes(_) => "bytes",
        }
    }
}

// Type tags; declaration order defines the cross-type sort order and is
// frozen (spec section 4.10).
const TAG_NULL: u8 = 0x01;
const TAG_BOOL: u8 = 0x02;
const TAG_INT: u8 = 0x03;
const TAG_TIMESTAMP_MICROS: u8 = 0x04;
const TAG_TEXT: u8 = 0x05;
const TAG_BYTES: u8 = 0x06;

/// Order-preserving encoder for typed key components.
///
/// Encoding rules, component by component (one tag byte, then the payload):
///
/// - `Null`: tag only.
/// - `Bool`: tag, then `0x00`/`0x01`.
/// - `Int` and `TimestampMicros`: tag, then 8 big-endian bytes of the value
///   with the sign bit flipped, so byte order matches signed numeric order.
/// - `Text` and `Bytes`: tag, then the payload with every `0x00` byte
///   escaped as `0x00 0xFF`, terminated by `0x00 0x00` — prefix-free and
///   order-preserving.
///
/// Components concatenate in declared column order, so a composite key sorts
/// by its first column, then its second, and so on.
pub struct RowKeyEncoder;

impl RowKeyEncoder {
    /// Encodes a full key from its typed components.
    pub fn encode_key(values: &[KeyValue]) -> Key {
        let mut out = Vec::new();
        for value in values {
            Self::encode_component(&mut out, value);
        }
        Key::from_bytes(out)
    }

    /// Appends one component to `out`.
    pub fn encode_component(out: &mut Vec<u8>, value: &KeyValue) {
        match value {
            KeyValue::Null => out.push(TAG_NULL),
            KeyValue::Bool(bit) => {
                out.push(TAG_BOOL);
                out.push(u8::from(*bit));
            }
            KeyValue::Int(int) => {
                out.push(TAG_INT);
                out.extend_from_slice(&orderable_i64(*int).to_be_bytes());
            }
            KeyValue::TimestampMicros(micros) => {
                out.push(TAG_TIMESTAMP_MICROS);
                out.extend_from_slice(&orderable_i64(*micros).to_be_bytes());
            }
            KeyValue::Text(text) => {
                out.push(TAG_TEXT);
                encode_escaped(out, text.as_bytes());
            }
            KeyValue::Bytes(bytes) => {
                out.push(TAG_BYTES);
                encode_escaped(out, bytes);
            }
        }
    }

    /// Decodes a key back into its components; the inverse of
    /// [`Self::encode_key`]. Malformed input fails closed.
    pub fn decode_components(key: &Key) -> Result<Vec<KeyValue>, PartitionError> {
        let bytes = key.as_bytes();
        let mut values = Vec::new();
        let mut cursor = 0;
        while cursor < bytes.len() {
            let tag = bytes[cursor];
            cursor += 1;
            let value = match tag {
                TAG_NULL => KeyValue::Null,
                TAG_BOOL => {
                    let byte = take(bytes, &mut cursor, 1)?[0];
                    match byte {
                        0 => KeyValue::Bool(false),
                        1 => KeyValue::Bool(true),
                        other => {
                            return Err(PartitionError::MalformedKey(format!(
                                "invalid bool payload {other}"
                            )))
                        }
                    }
                }
                TAG_INT => {
                    let raw = take(bytes, &mut cursor, 8)?;
                    KeyValue::Int(unorderable_i64(raw))
                }
                TAG_TIMESTAMP_MICROS => {
                    let raw = take(bytes, &mut cursor, 8)?;
                    KeyValue::TimestampMicros(unorderable_i64(raw))
                }
                TAG_TEXT => {
                    let payload = decode_escaped(bytes, &mut cursor)?;
                    let text = String::from_utf8(payload)
                        .map_err(|error| PartitionError::MalformedKey(error.to_string()))?;
                    KeyValue::Text(text)
                }
                TAG_BYTES => KeyValue::Bytes(decode_escaped(bytes, &mut cursor)?),
                other => {
                    return Err(PartitionError::MalformedKey(format!(
                        "unknown type tag 0x{other:02x}"
                    )))
                }
            };
            values.push(value);
        }
        Ok(values)
    }

    /// FNV-1a 64-bit over `bytes` — the same mixer the storage core uses to
    /// derive row ids from primary keys (`engine.rs`): offset basis
    /// `0xcbf29ce484222325`, prime `0x100000001b3`.
    pub fn fnv1a64(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash
    }
}

/// Maps an `i64` onto `u64` preserving order (sign-bit flip).
fn orderable_i64(value: i64) -> u64 {
    (value as u64) ^ (1 << 63)
}

/// Inverse of [`orderable_i64`], from 8 big-endian bytes.
fn unorderable_i64(raw: &[u8]) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(raw);
    (u64::from_be_bytes(bytes) ^ (1 << 63)) as i64
}

/// Takes `count` bytes from `bytes` at `cursor`, advancing it.
fn take<'a>(bytes: &'a [u8], cursor: &mut usize, count: usize) -> Result<&'a [u8], PartitionError> {
    let end = cursor
        .checked_add(count)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| PartitionError::MalformedKey("truncated payload".to_owned()))?;
    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

/// Appends `payload` with `0x00` escaped as `0x00 0xFF`, plus the `0x00 0x00`
/// terminator.
fn encode_escaped(out: &mut Vec<u8>, payload: &[u8]) {
    for byte in payload {
        out.push(*byte);
        if *byte == 0x00 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

/// Reads one escaped payload up to and past its `0x00 0x00` terminator.
fn decode_escaped(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>, PartitionError> {
    let mut payload = Vec::new();
    loop {
        let byte = *bytes
            .get(*cursor)
            .ok_or_else(|| PartitionError::MalformedKey("unterminated payload".to_owned()))?;
        *cursor += 1;
        if byte != 0x00 {
            payload.push(byte);
            continue;
        }
        let escape = *bytes
            .get(*cursor)
            .ok_or_else(|| PartitionError::MalformedKey("unterminated payload".to_owned()))?;
        *cursor += 1;
        match escape {
            0x00 => return Ok(payload),
            0xFF => payload.push(0x00),
            other => {
                return Err(PartitionError::MalformedKey(format!(
                    "invalid escape byte 0x{other:02x}"
                )))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Partitioning (spec section 12.2)
// ---------------------------------------------------------------------------

/// Fixed-width time interval for time-range partitioning, in microseconds.
///
/// Calendar intervals (week, month) are deliberately out of scope: they are
/// timezone- and calendar-dependent, while partition boundaries must be a
/// pure function of the timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeInterval {
    micros: u64,
}

impl TimeInterval {
    /// An interval of `micros` microseconds.
    pub fn micros(micros: u64) -> Result<Self, TabletError> {
        if micros == 0 {
            return Err(TabletError::InvalidDescriptor(
                "time interval must be positive".to_owned(),
            ));
        }
        Ok(Self { micros })
    }

    /// An interval of `hours` hours.
    pub fn hours(hours: u64) -> Result<Self, TabletError> {
        Self::micros(
            hours
                .checked_mul(3_600_000_000)
                .ok_or(TabletError::InvalidDescriptor(
                    "time interval overflows u64 micros".to_owned(),
                ))?,
        )
    }

    /// An interval of `days` days.
    pub fn days(days: u64) -> Result<Self, TabletError> {
        Self::micros(
            days.checked_mul(86_400_000_000)
                .ok_or(TabletError::InvalidDescriptor(
                    "time interval overflows u64 micros".to_owned(),
                ))?,
        )
    }

    /// The interval width in microseconds.
    pub const fn as_micros(self) -> u64 {
        self.micros
    }
}

/// How one table's rows map onto tablets (spec section 12.2). Declaration
/// order is frozen; variants are never reused (spec section 4.10).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Partitioning {
    /// Hash of the declared columns into a fixed bucket space. Tablets own
    /// contiguous runs of buckets (see [`Partitioning::routed_key`]).
    Hash {
        /// Partition-key columns, in hash order.
        columns: Vec<ColumnId>,
        /// Bucket count; positive.
        buckets: u32,
    },
    /// Lexicographic ranges over the declared columns, split at `splits`.
    /// `splits.len() + 1` partitions cover the key space: partition `i` owns
    /// `[splits[i - 1], splits[i])`, unbounded at the edges.
    Range {
        /// Partition-key columns, in sort order.
        columns: Vec<ColumnId>,
        /// Ordered, strictly increasing split points.
        splits: Vec<Key>,
    },
    /// One bucket space per tenant: the tenant column hashes into
    /// `buckets_per_tenant` buckets, so every tenant's rows spread evenly and
    /// tenants can be isolated onto dedicated tablets (spec section 12.5).
    Tenant {
        /// Column carrying the tenant identifier.
        tenant_column: ColumnId,
        /// Buckets each tenant hashes into; positive.
        buckets_per_tenant: u32,
    },
    /// Time-bucketed ranges: partition `i` owns
    /// `[i * interval, (i + 1) * interval)` of the timestamp column.
    TimeRange {
        /// Column carrying the row timestamp.
        timestamp_column: ColumnId,
        /// Fixed interval width.
        interval: TimeInterval,
    },
}

impl Partitioning {
    /// Validates the declaration: non-empty column lists, positive bucket
    /// counts, strictly increasing splits, positive interval.
    pub fn validate(&self) -> Result<(), PartitionError> {
        match self {
            Self::Hash { columns, buckets } => {
                if columns.is_empty() {
                    return Err(PartitionError::InvalidPartitioning(
                        "hash partitioning needs at least one column".to_owned(),
                    ));
                }
                if *buckets == 0 {
                    return Err(PartitionError::InvalidPartitioning(
                        "hash partitioning needs at least one bucket".to_owned(),
                    ));
                }
            }
            Self::Range { columns, splits } => {
                if columns.is_empty() {
                    return Err(PartitionError::InvalidPartitioning(
                        "range partitioning needs at least one column".to_owned(),
                    ));
                }
                if splits.windows(2).any(|window| window[0] >= window[1]) {
                    return Err(PartitionError::InvalidPartitioning(
                        "range splits must be strictly increasing".to_owned(),
                    ));
                }
            }
            Self::Tenant {
                buckets_per_tenant, ..
            } => {
                if *buckets_per_tenant == 0 {
                    return Err(PartitionError::InvalidPartitioning(
                        "tenant partitioning needs at least one bucket per tenant".to_owned(),
                    ));
                }
            }
            Self::TimeRange { interval, .. } => {
                if interval.as_micros() == 0 {
                    return Err(PartitionError::InvalidPartitioning(
                        "time-range interval must be positive".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// The declared partition-key columns, in key order (spec section 12.2:
    /// every table has a declared partition key).
    pub fn partition_columns(&self) -> Vec<ColumnId> {
        match self {
            Self::Hash { columns, .. } | Self::Range { columns, .. } => columns.clone(),
            Self::Tenant { tenant_column, .. } => vec![*tenant_column],
            Self::TimeRange {
                timestamp_column, ..
            } => vec![*timestamp_column],
        }
    }

    /// Extracts the partition key from a row's column values: the declared
    /// partition-key columns, in declared order, encoded by
    /// [`RowKeyEncoder`]. A missing declared column fails closed.
    ///
    /// The fast path of spec section 12.2 — the primary key includes the
    /// partition key — means the caller can usually supply the primary-key
    /// components directly; this function does not care where the values came
    /// from.
    pub fn partition_key(
        &self,
        values: &BTreeMap<ColumnId, KeyValue>,
    ) -> Result<Key, PartitionError> {
        self.validate()?;
        let columns = self.partition_columns();
        let mut components = Vec::with_capacity(columns.len());
        for column in columns {
            let value = values
                .get(&column)
                .ok_or(PartitionError::MissingPartitionColumn { column })?;
            if let Self::TimeRange { .. } = self {
                if !matches!(value, KeyValue::TimestampMicros(_)) {
                    return Err(PartitionError::PartitionColumnType {
                        column,
                        expected: "timestamp-micros",
                        found: value.type_name(),
                    });
                }
            }
            components.push(value.clone());
        }
        Ok(RowKeyEncoder::encode_key(&components))
    }

    /// Maps an extracted partition key onto its partition slot (spec section
    /// 12.2):
    ///
    /// - `Hash`: `fnv1a64(key) % buckets`.
    /// - `Range`: index of the first split above the key, `0..=splits.len()`.
    /// - `Tenant`: `fnv1a64(tenant key) % buckets_per_tenant`; the slot is
    ///   per tenant, so the full partition address is `(tenant, slot)` — see
    ///   [`Self::routed_key`].
    /// - `TimeRange`: `timestamp / interval` (epoch-floored); pre-epoch
    ///   timestamps fail closed with [`PartitionError::NegativeSlot`].
    pub fn route(&self, partition_key: &Key) -> Result<u64, PartitionError> {
        self.validate()?;
        match self {
            Self::Hash { buckets, .. } => {
                Ok(RowKeyEncoder::fnv1a64(partition_key.as_bytes()) % u64::from(*buckets))
            }
            Self::Range { splits, .. } => {
                Ok(splits.partition_point(|split| split <= partition_key) as u64)
            }
            Self::Tenant {
                buckets_per_tenant, ..
            } => {
                Ok(RowKeyEncoder::fnv1a64(partition_key.as_bytes())
                    % u64::from(*buckets_per_tenant))
            }
            Self::TimeRange { interval, .. } => {
                let mut components = RowKeyEncoder::decode_components(partition_key)?;
                if components.len() != 1 {
                    return Err(PartitionError::MalformedKey(format!(
                        "time-range partition key has {} components, expected 1",
                        components.len()
                    )));
                }
                let Some(KeyValue::TimestampMicros(micros)) = components.pop() else {
                    return Err(PartitionError::MalformedKey(
                        "time-range partition key does not start with a timestamp".to_owned(),
                    ));
                };
                let slot =
                    micros.div_euclid(i64::try_from(interval.as_micros()).map_err(|_| {
                        PartitionError::InvalidPartitioning(
                            "time-range interval exceeds i64 micros".to_owned(),
                        )
                    })?);
                u64::try_from(slot).map_err(|_| PartitionError::NegativeSlot { micros })
            }
        }
    }

    /// The canonical routing address of a partition key — the byte string
    /// whose ranges the tablet [`PartitionBounds`] of this table are
    /// allocated over:
    ///
    /// - `Range` and `TimeRange`: the partition key itself; bounds range
    ///   directly over encoded key bytes.
    /// - `Hash`: the 8 big-endian bytes of the bucket; the meta plane
    ///   allocates tablet bounds as contiguous bucket runs, e.g. buckets
    ///   `[lo, hi)` via [`hash_slot_bounds`].
    /// - `Tenant`: the tenant key encoding followed by the 8 big-endian
    ///   bytes of the bucket, so bounds range per tenant over the composite.
    pub fn routed_key(&self, partition_key: &Key) -> Result<Key, PartitionError> {
        match self {
            Self::Range { .. } | Self::TimeRange { .. } => Ok(partition_key.clone()),
            Self::Hash { .. } => Ok(Key::from_bytes(
                self.route(partition_key)?.to_be_bytes().to_vec(),
            )),
            Self::Tenant { .. } => {
                let mut bytes = partition_key.as_bytes().to_vec();
                bytes.extend_from_slice(&self.route(partition_key)?.to_be_bytes());
                Ok(Key::from_bytes(bytes))
            }
        }
    }

    /// The bounds of range partition `index` (`0..=splits.len()`); `None`
    /// for out-of-range indexes and non-range partitioning.
    pub fn range_bounds(&self, index: u64) -> Option<PartitionBounds> {
        let Self::Range { splits, .. } = self else {
            return None;
        };
        let index = usize::try_from(index).ok()?;
        if index > splits.len() {
            return None;
        }
        let low = index.checked_sub(1).map_or(Bound::Unbounded, |previous| {
            Bound::Included(splits[previous].clone())
        });
        let high = splits
            .get(index)
            .map_or(Bound::Unbounded, |split| Bound::Excluded(split.clone()));
        Some(PartitionBounds { low, high })
    }
}

/// Tablet bounds covering hash buckets `[low_slot, high_slot_exclusive)` in
/// the canonical [`Partitioning::routed_key`] space of a hash-partitioned
/// table.
pub fn hash_slot_bounds(low_slot: u64, high_slot_exclusive: u64) -> PartitionBounds {
    PartitionBounds {
        low: Bound::Included(Key::from_bytes(low_slot.to_be_bytes().to_vec())),
        high: Bound::Excluded(Key::from_bytes(high_slot_exclusive.to_be_bytes().to_vec())),
    }
}

/// Why a table's [`Partitioning`] is what it is (spec section 12.2: automatic
/// defaults must be visible in schema metadata). Declaration order is frozen;
/// variants are never reused (spec section 4.10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitioningOrigin {
    /// Declared explicitly by `CREATE TABLE`.
    Declared,
    /// Derived automatically (hash of the primary key); recorded so the
    /// default is visible in schema metadata rather than implicit.
    AutomaticDefault,
}

/// Colocation declaration (spec section 12.2: related tables may declare
/// colocation). A table colocated with another shares its partition layout,
/// so equal partition keys land on the same tablets and local joins stay
/// local. Colocation requires the two tables' partitionings to agree on the
/// partition-key types; the meta group enforces that at declaration time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct ColocatedWith(pub TableId);

/// The per-table partitioning metadata record (spec section 12.2). One such
/// record lives in the replicated schema metadata of every table; the
/// [`PartitioningOrigin`] keeps automatic defaults visible.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TablePartitioningRecord {
    /// The table this record describes.
    pub table_id: TableId,
    /// How the table's rows map onto tablets.
    pub partitioning: Partitioning,
    /// Whether the partitioning was declared or derived by default.
    pub origin: PartitioningOrigin,
    /// Colocation declaration, when the table is colocated with another.
    pub colocated_with: Option<ColocatedWith>,
}

impl TablePartitioningRecord {
    /// The automatic default (spec section 12.2): hash partitioning over the
    /// primary-key columns, recorded with [`PartitioningOrigin::AutomaticDefault`]
    /// so the default is visible in schema metadata.
    pub fn automatic_default(
        table_id: TableId,
        primary_key_columns: Vec<ColumnId>,
        buckets: u32,
    ) -> Self {
        Self {
            table_id,
            partitioning: Partitioning::Hash {
                columns: primary_key_columns,
                buckets,
            },
            origin: PartitioningOrigin::AutomaticDefault,
            colocated_with: None,
        }
    }

    /// Validates the record: usable table id, well-formed partitioning, no
    /// self-colocation.
    pub fn validate(&self) -> Result<(), TabletError> {
        if self.table_id == TableId::ZERO {
            return Err(TabletError::InvalidDescriptor(
                "reserved zero table id".to_owned(),
            ));
        }
        self.partitioning.validate()?;
        if self
            .colocated_with
            .is_some_and(|target| target.0 == self.table_id)
        {
            return Err(TabletError::InvalidDescriptor(
                "a table cannot be colocated with itself".to_owned(),
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tablet routing (spec section 12.4)
// ---------------------------------------------------------------------------

/// Routes a point read/write directly: the routable tablet of `table_id`
/// whose bounds contain `routed_key` (see [`Partitioning::routed_key`]).
///
/// A well-formed meta plane yields exactly one match; against a stale cache
/// copy a miss means "refresh metadata", which is why the caller resolves a
/// miss to a metadata refresh, not an error.
pub fn find_tablet_for_key<'a>(
    tablets: &'a [TabletDescriptor],
    table_id: TableId,
    routed_key: &Key,
) -> Option<&'a TabletDescriptor> {
    tablets.iter().find(|tablet| {
        tablet.table_id == table_id
            && tablet.state.is_routable()
            && tablet.partition.contains(routed_key)
    })
}

/// Routes a range query to every overlapping tablet (spec section 12.4),
/// ordered deterministically by low endpoint, then tablet id.
pub fn tablets_overlapping<'a>(
    tablets: &'a [TabletDescriptor],
    table_id: TableId,
    range: &PartitionBounds,
) -> Vec<&'a TabletDescriptor> {
    let mut matched: Vec<&TabletDescriptor> = tablets
        .iter()
        .filter(|tablet| {
            tablet.table_id == table_id
                && tablet.state.is_routable()
                && tablet.partition.overlaps(range)
        })
        .collect();
    matched.sort_by(|left, right| {
        cmp_low_bounds(&left.partition.low, &right.partition.low)
            .then_with(|| left.tablet_id.cmp(&right.tablet_id))
    });
    matched
}

/// Low-endpoint order for routing output: `Unbounded` sorts first, and at
/// equal keys an included endpoint sorts before an excluded one.
fn cmp_low_bounds(left: &Bound<Key>, right: &Bound<Key>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (Bound::Included(left), Bound::Included(right))
        | (Bound::Excluded(left), Bound::Excluded(right)) => left.cmp(right),
        (Bound::Included(left), Bound::Excluded(right)) => left.cmp(right).then(Ordering::Less),
        (Bound::Excluded(left), Bound::Included(right)) => left.cmp(right).then(Ordering::Greater),
    }
}

/// Verifies the generation a request was routed with against the descriptor
/// the receiving replica holds (spec section 12.4: every request includes
/// the tablet generation it used).
///
/// A match passes. A mismatch is classified from the tablet's current state:
/// a source in [`TabletState::Splitting`] reports [`RoutingError::TabletSplit`],
/// a retired or retiring tablet reports [`RoutingError::TabletMoved`], and
/// anything else — including a request newer than the replica's descriptor,
/// which means the replica is behind — reports
/// [`RoutingError::StaleMetadata`]. The gateway refreshes metadata and
/// retries safe operations on all three.
pub fn check_generation(
    descriptor: &TabletDescriptor,
    used_generation: u64,
) -> Result<(), RoutingError> {
    if used_generation == descriptor.generation {
        return Ok(());
    }
    let tablet_id = descriptor.tablet_id;
    let current_generation = descriptor.generation;
    // Review N2: a request generation *newer* than the replica's descriptor
    // means the replica is behind (StaleMetadata), even while Splitting —
    // only an *older* request against a splitting source is TabletSplit.
    Err(match descriptor.state {
        TabletState::Splitting if used_generation < current_generation => {
            RoutingError::TabletSplit {
                tablet_id,
                used_generation,
                current_generation,
            }
        }
        TabletState::Retiring | TabletState::Retired => RoutingError::TabletMoved {
            tablet_id,
            used_generation,
            current_generation,
        },
        _ => RoutingError::StaleMetadata {
            tablet_id,
            used_generation,
            current_generation,
        },
    })
}

// ---------------------------------------------------------------------------
// Tablet local storage (spec section 12.3)
// ---------------------------------------------------------------------------

/// Name of the per-node tablet directory root under the node data dir.
pub const TABLETS_DIR: &str = "tablets";
/// Name of the per-node Raft-group directory root under the node data dir.
pub const GROUPS_DIR: &str = "groups";
/// Subdirectories of one tablet directory.
pub const TABLET_STATE_DIR: &str = "state";
/// Sorted-run subdirectory of one tablet directory.
pub const TABLET_RUNS_DIR: &str = "runs";
/// Local index-generation subdirectory of one tablet directory.
pub const TABLET_INDEXES_DIR: &str = "indexes";
/// Temporary/spill subdirectory of one tablet directory.
pub const TABLET_TEMP_DIR: &str = "temp";
/// Raft log subdirectory of one group directory.
pub const GROUP_RAFT_DIR: &str = "raft";
/// Snapshot subdirectory of one group directory.
///
/// Historical name kept for layout docs; the consensus state machine stores
/// snapshots under `groups/<id>/raft/snapshot` (review **N3**). Prefer
/// [`TabletLayout::raft_snapshot_dir`].
pub const GROUP_SNAPSHOTS_DIR: &str = "snapshots";
/// Name of the versioned, checksummed tablet metadata file.
pub const TABLET_META_FILENAME: &str = "tablet.json";
/// The tablet-metadata format version this build writes.
pub const TABLET_META_FORMAT_VERSION: u32 = 1;
/// The oldest tablet-metadata format version this build accepts.
pub const MIN_SUPPORTED_TABLET_META_FORMAT_VERSION: u32 = 1;

/// On-node directory layout of one tablet replica and its Raft group (spec
/// section 12.3):
///
/// ```text
/// node-data/
///   tablets/<tablet-id>/{state,runs,indexes,temp}   + tablet.json
///   groups/<raft-group-id>/{raft,snapshots}
/// ```
///
/// The applied MVCC state, sorted runs, local index generations, and
/// compaction state live under the tablet directory; the Raft log and
/// snapshots live under the group directory, so a group can outlive any
/// single applied-state rebuild (spec section 4.4: runs and indexes are
/// applied state and may be rebuilt).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TabletLayout {
    node_data: PathBuf,
    tablet_id: TabletId,
    raft_group_id: RaftGroupId,
}

impl TabletLayout {
    /// The layout of `tablet_id`/`raft_group_id` under `node_data`.
    pub fn new(
        node_data: impl Into<PathBuf>,
        tablet_id: TabletId,
        raft_group_id: RaftGroupId,
    ) -> Self {
        Self {
            node_data: node_data.into(),
            tablet_id,
            raft_group_id,
        }
    }

    /// The tablet this layout belongs to.
    pub fn tablet_id(&self) -> TabletId {
        self.tablet_id
    }

    /// The Raft group this layout belongs to.
    pub fn raft_group_id(&self) -> RaftGroupId {
        self.raft_group_id
    }

    /// The node data root the layout lives under.
    pub fn node_data(&self) -> &Path {
        &self.node_data
    }

    /// `node-data/tablets/<tablet-id>`.
    pub fn tablet_dir(&self) -> PathBuf {
        self.node_data
            .join(TABLETS_DIR)
            .join(self.tablet_id.to_hex())
    }

    /// `tablets/<tablet-id>/state` (applied MVCC + compaction state).
    pub fn state_dir(&self) -> PathBuf {
        self.tablet_dir().join(TABLET_STATE_DIR)
    }

    /// `tablets/<tablet-id>/runs` (sorted runs).
    pub fn runs_dir(&self) -> PathBuf {
        self.tablet_dir().join(TABLET_RUNS_DIR)
    }

    /// `tablets/<tablet-id>/indexes` (local index generations).
    pub fn indexes_dir(&self) -> PathBuf {
        self.tablet_dir().join(TABLET_INDEXES_DIR)
    }

    /// `tablets/<tablet-id>/temp` (spill scratch space).
    pub fn temp_dir(&self) -> PathBuf {
        self.tablet_dir().join(TABLET_TEMP_DIR)
    }

    /// `node-data/groups/<raft-group-id>`.
    pub fn group_dir(&self) -> PathBuf {
        self.node_data
            .join(GROUPS_DIR)
            .join(self.raft_group_id.to_hex())
    }

    /// `groups/<raft-group-id>/raft` (Raft log).
    pub fn raft_dir(&self) -> PathBuf {
        self.group_dir().join(GROUP_RAFT_DIR)
    }

    /// Legacy layout path `groups/<raft-group-id>/snapshots` (unused by the
    /// consensus SM). Prefer [`Self::raft_snapshot_dir`] (review **N3**).
    pub fn snapshots_dir(&self) -> PathBuf {
        self.group_dir().join(GROUP_SNAPSHOTS_DIR)
    }

    /// `groups/<raft-group-id>/raft/snapshot` — the path the consensus state
    /// machine actually writes (review **N3** reconciliation).
    pub fn raft_snapshot_dir(&self) -> PathBuf {
        self.raft_dir().join("snapshot")
    }

    /// `tablets/<tablet-id>/tablet.json`.
    pub fn metadata_path(&self) -> PathBuf {
        self.tablet_dir().join(TABLET_META_FILENAME)
    }

    /// Every directory spec section 12.3 requires, tablet dirs first.
    fn required_dirs(&self) -> [PathBuf; 6] {
        [
            self.state_dir(),
            self.runs_dir(),
            self.indexes_dir(),
            self.temp_dir(),
            self.raft_dir(),
            self.snapshots_dir(),
        ]
    }

    /// Creates the directory tree and persists the initial tablet metadata.
    ///
    /// The metadata file is created atomically and only if absent (the same
    /// hard-link publish idiom as the node identity); a concurrent or
    /// repeated `create` that finds identical metadata succeeds, while
    /// different existing metadata fails closed with
    /// [`TabletError::MetadataConflict`] — a tablet directory is never
    /// silently repurposed.
    pub fn create(&self, descriptor: &TabletDescriptor) -> Result<(), TabletError> {
        self.check_descriptor_identity(descriptor)?;
        descriptor.validate()?;
        for dir in self.required_dirs() {
            fs::create_dir_all(&dir).map_err(ClusterError::Io)?;
        }
        let file = TabletMetaFile::envelope(descriptor)?;
        let bytes = crate::node::encode_json(TABLET_META_FILENAME, &file)?;
        match crate::node::create_meta_file(&self.tablet_dir(), TABLET_META_FILENAME, &bytes)
            .map_err(ClusterError::Io)?
        {
            true => Ok(()),
            // Lost a create race or re-run: accept identical metadata only.
            false => match self.load_metadata()? {
                existing if existing == *descriptor => Ok(()),
                _ => Err(TabletError::MetadataConflict(self.tablet_dir())),
            },
        }
    }

    /// Atomically replaces the persisted metadata (used by the meta-driven
    /// apply path when the descriptor's generation advances). The descriptor
    /// is validated and must name this layout's tablet and group.
    pub fn store_metadata(&self, descriptor: &TabletDescriptor) -> Result<(), TabletError> {
        self.check_descriptor_identity(descriptor)?;
        descriptor.validate()?;
        let file = TabletMetaFile::envelope(descriptor)?;
        let bytes = crate::node::encode_json(TABLET_META_FILENAME, &file)?;
        crate::node::write_meta_atomic(&self.tablet_dir(), TABLET_META_FILENAME, &bytes)
            .map_err(ClusterError::Io)?;
        Ok(())
    }

    /// Loads and verifies the persisted tablet metadata. Missing, corrupt,
    /// unknown-version, wrong-checksum, or wrong-tablet files all fail closed
    /// (spec sections 4.10, 12.3).
    pub fn load_metadata(&self) -> Result<TabletDescriptor, TabletError> {
        let Some(bytes) = crate::node::read_meta_file(&self.metadata_path())? else {
            return Err(TabletError::MissingMetadata(self.metadata_path()));
        };
        let file: TabletMetaFile = crate::node::decode_json(TABLET_META_FILENAME, &bytes)?;
        if file.format_version < MIN_SUPPORTED_TABLET_META_FORMAT_VERSION
            || file.format_version > TABLET_META_FORMAT_VERSION
        {
            return Err(ClusterError::UnsupportedFormatVersion {
                file: TABLET_META_FILENAME,
                found: file.format_version,
                min: MIN_SUPPORTED_TABLET_META_FORMAT_VERSION,
                max: TABLET_META_FORMAT_VERSION,
            }
            .into());
        }
        if file.checksum != tablet_checksum(&file.tablet)? {
            return Err(ClusterError::CorruptMetadata {
                file: TABLET_META_FILENAME,
                detail: "checksum mismatch".to_owned(),
            }
            .into());
        }
        self.check_descriptor_identity(&file.tablet)?;
        file.tablet.validate()?;
        Ok(file.tablet)
    }

    /// Opens the layout for use: verifies the persisted metadata and the
    /// presence of every required directory, failing closed on any missing
    /// or corrupt piece (spec section 12.3).
    pub fn validate(&self) -> Result<TabletDescriptor, TabletError> {
        let descriptor = self.load_metadata()?;
        for dir in self.required_dirs() {
            if !dir.is_dir() {
                return Err(ClusterError::CorruptMetadata {
                    file: TABLET_META_FILENAME,
                    detail: format!("required directory {} is missing", dir.display()),
                }
                .into());
            }
        }
        Ok(descriptor)
    }

    /// The descriptor must name exactly this layout's tablet and group.
    fn check_descriptor_identity(&self, descriptor: &TabletDescriptor) -> Result<(), TabletError> {
        if descriptor.tablet_id != self.tablet_id || descriptor.raft_group_id != self.raft_group_id
        {
            return Err(TabletError::TabletMismatch {
                path: self.tablet_dir(),
                expected: self.tablet_id,
                found: descriptor.tablet_id,
                expected_group: self.raft_group_id,
                found_group: descriptor.raft_group_id,
            });
        }
        Ok(())
    }

    /// Removes the local replica: the tablet directory and the Raft-group
    /// directory (spec section 12.5 step 11; the consensus membership removal
    /// is the runtime's half). Idempotent — a partially torn-down or absent
    /// directory is fine — but never destructive across identity: a
    /// `tablet.json` that names a different tablet or group fails closed with
    /// [`TabletError::TabletMismatch`] instead of deleting foreign state.
    pub fn teardown(&self) -> Result<(), TabletError> {
        if self.tablet_dir().is_dir() {
            match self.load_metadata() {
                Ok(_) | Err(TabletError::MissingMetadata(_)) => {}
                // Corrupt or foreign metadata blocks automatic teardown.
                Err(error) => return Err(error),
            }
            fs::remove_dir_all(self.tablet_dir()).map_err(ClusterError::Io)?;
        }
        if self.group_dir().is_dir() {
            fs::remove_dir_all(self.group_dir()).map_err(ClusterError::Io)?;
        }
        Ok(())
    }
}

/// The durable tablet metadata envelope: versioned and checksummed, so a
/// torn or tampered file fails closed instead of opening a wrong tablet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TabletMetaFile {
    /// Durable format version; see [`TABLET_META_FORMAT_VERSION`].
    format_version: u32,
    /// Lowercase-hex SHA-256 of the canonical JSON encoding of `tablet`.
    checksum: String,
    /// The persisted descriptor.
    tablet: TabletDescriptor,
}

impl TabletMetaFile {
    fn envelope(tablet: &TabletDescriptor) -> Result<Self, TabletError> {
        Ok(Self {
            format_version: TABLET_META_FORMAT_VERSION,
            checksum: tablet_checksum(tablet)?,
            tablet: tablet.clone(),
        })
    }
}

/// SHA-256 of the canonical (compact JSON) encoding of the descriptor.
fn tablet_checksum(tablet: &TabletDescriptor) -> Result<String, ClusterError> {
    let bytes = serde_json::to_vec(tablet).map_err(|error| ClusterError::CorruptMetadata {
        file: TABLET_META_FILENAME,
        detail: format!("encode: {error}"),
    })?;
    Ok(hex_encode(&Sha256::digest(&bytes)))
}

/// Scan `<node_data>/tablets/*/tablet.json` and load every valid descriptor.
///
/// Used by the §15 `SHOW TABLETS` / `SHOW REPLICAS` admin surface when a
/// tablet runtime has persisted local metadata. Missing tablets dir is an
/// empty list (standalone or not yet hosting tablets); corrupt individual
/// files are skipped and counted in the returned issues.
pub fn list_tablets_on_disk(
    node_data: impl AsRef<Path>,
) -> Result<(Vec<TabletDescriptor>, Vec<String>), ClusterError> {
    let root = node_data.as_ref().join(TABLETS_DIR);
    if !root.is_dir() {
        return Ok((Vec::new(), Vec::new()));
    }
    let mut tablets = Vec::new();
    let mut issues = Vec::new();
    let entries = fs::read_dir(&root).map_err(ClusterError::Io)?;
    for entry in entries {
        let entry = entry.map_err(ClusterError::Io)?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let meta = path.join(TABLET_META_FILENAME);
        if !meta.is_file() {
            continue;
        }
        // Load without TabletLayout identity check (group id unknown until
        // decode). Verify format/checksum via the same envelope path.
        match load_tablet_meta_file(&meta) {
            Ok(descriptor) => tablets.push(descriptor),
            Err(error) => issues.push(format!("{}: {error}", meta.display())),
        }
    }
    tablets.sort_by_key(|t| t.tablet_id);
    Ok((tablets, issues))
}

fn load_tablet_meta_file(path: &Path) -> Result<TabletDescriptor, ClusterError> {
    let Some(bytes) = crate::node::read_meta_file(path)? else {
        return Err(ClusterError::CorruptMetadata {
            file: TABLET_META_FILENAME,
            detail: format!("missing {}", path.display()),
        });
    };
    let file: TabletMetaFile = crate::node::decode_json(TABLET_META_FILENAME, &bytes)?;
    if file.format_version < MIN_SUPPORTED_TABLET_META_FORMAT_VERSION
        || file.format_version > TABLET_META_FORMAT_VERSION
    {
        return Err(ClusterError::UnsupportedFormatVersion {
            file: TABLET_META_FILENAME,
            found: file.format_version,
            min: MIN_SUPPORTED_TABLET_META_FORMAT_VERSION,
            max: TABLET_META_FORMAT_VERSION,
        });
    }
    if file.checksum != tablet_checksum(&file.tablet)? {
        return Err(ClusterError::CorruptMetadata {
            file: TABLET_META_FILENAME,
            detail: "checksum mismatch".to_owned(),
        });
    }
    file.tablet
        .validate()
        .map_err(|e| ClusterError::CorruptMetadata {
            file: TABLET_META_FILENAME,
            detail: e.to_string(),
        })?;
    Ok(file.tablet)
}

/// Process-local registry of owned tablet storage cores (spec section 12.3:
/// one tablet storage core is owned by one node process).
///
/// This is the in-process half of the rule, mirroring the open-reservation
/// concept of the Stage 1 shared-core registry (S1A-002): while a
/// [`TabletOwnershipGuard`] is alive, a second reservation of the same
/// canonical tablet directory fails closed with
/// [`TabletError::AlreadyOwned`]. The cross-process half stays with the
/// storage core's file lease (`_meta/.lock`), exactly as in single-node
/// mode; this registry never touches the file system beyond canonicalizing
/// the tablet directory.
#[derive(Debug, Default)]
pub struct TabletOwnershipRegistry {
    reservations: Mutex<HashMap<PathBuf, TabletId>>,
}

impl TabletOwnershipRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// The process-global registry.
    pub fn global() -> &'static Self {
        static REGISTRY: OnceLock<TabletOwnershipRegistry> = OnceLock::new();
        REGISTRY.get_or_init(Self::new)
    }

    /// Reserves `layout`'s tablet directory for this process. The directory
    /// must already exist (create the layout first); the reservation keys on
    /// the canonical path so aliased paths cannot double-open a tablet.
    pub fn try_reserve(
        &self,
        layout: &TabletLayout,
    ) -> Result<TabletOwnershipGuard<'_>, TabletError> {
        let path = layout
            .tablet_dir()
            .canonicalize()
            .map_err(ClusterError::Io)?;
        let mut reservations = self
            .reservations
            .lock()
            .expect("tablet ownership registry lock poisoned");
        if let Some(holder) = reservations.get(&path) {
            return Err(TabletError::AlreadyOwned {
                tablet: *holder,
                path,
            });
        }
        reservations.insert(path.clone(), layout.tablet_id());
        Ok(TabletOwnershipGuard {
            registry: self,
            path,
            tablet_id: layout.tablet_id(),
        })
    }

    /// Number of live reservations (diagnostics).
    pub fn len(&self) -> usize {
        self.reservations
            .lock()
            .expect("tablet ownership registry lock poisoned")
            .len()
    }

    /// Whether no tablet storage core is reserved.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// RAII reservation of one tablet directory; dropping releases the
/// reservation so the tablet may be reopened.
#[derive(Debug)]
pub struct TabletOwnershipGuard<'a> {
    registry: &'a TabletOwnershipRegistry,
    path: PathBuf,
    tablet_id: TabletId,
}

impl TabletOwnershipGuard<'_> {
    /// The tablet whose storage core this guard owns.
    pub fn tablet_id(&self) -> TabletId {
        self.tablet_id
    }

    /// The canonical reserved path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TabletOwnershipGuard<'_> {
    fn drop(&mut self) {
        let mut reservations = self
            .registry
            .reservations
            .lock()
            .expect("tablet ownership registry lock poisoned");
        if reservations.get(&self.path) == Some(&self.tablet_id) {
            reservations.remove(&self.path);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn node(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    fn tablet_id(byte: u8) -> TabletId {
        TabletId::from_bytes([byte; 16])
    }

    fn group_id(byte: u8) -> RaftGroupId {
        RaftGroupId::from_bytes([byte; 16])
    }

    fn key(bytes: &[u8]) -> Key {
        Key::from_bytes(bytes.to_vec())
    }

    fn text_key(text: &str) -> Key {
        RowKeyEncoder::encode_key(&[KeyValue::Text(text.to_owned())])
    }

    fn int_key(value: i64) -> Key {
        RowKeyEncoder::encode_key(&[KeyValue::Int(value)])
    }

    fn bounds(low: Bound<Key>, high: Bound<Key>) -> PartitionBounds {
        PartitionBounds::new(low, high).unwrap()
    }

    fn descriptor(state: TabletState) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: tablet_id(9),
            table_id: TableId::new(3),
            database_id: mongreldb_types::ids::DatabaseId::ZERO,
            raft_group_id: group_id(7),
            partition: bounds(
                Bound::Included(text_key("a")),
                Bound::Excluded(text_key("m")),
            ),
            replicas: vec![
                ReplicaDescriptor {
                    node_id: node(1),
                    role: ReplicaRole::Voter,
                    raft_node_id: 11,
                },
                ReplicaDescriptor {
                    node_id: node(2),
                    role: ReplicaRole::Voter,
                    raft_node_id: 12,
                },
                ReplicaDescriptor {
                    node_id: node(3),
                    role: ReplicaRole::Learner,
                    raft_node_id: 13,
                },
            ],
            leader_hint: Some(node(1)),
            generation: 7,
            state,
        }
    }

    // -- descriptor serde --------------------------------------------------

    #[test]
    fn descriptor_round_trips_serde_in_every_state() {
        for state in [
            TabletState::Creating,
            TabletState::Active,
            TabletState::Splitting,
            TabletState::Merging,
            TabletState::Retiring,
            TabletState::Retired,
        ] {
            let descriptor = descriptor(state);
            let json = serde_json::to_vec(&descriptor).unwrap();
            let back: TabletDescriptor = serde_json::from_slice(&json).unwrap();
            assert_eq!(back, descriptor);
        }
    }

    #[test]
    fn descriptor_rejects_unknown_fields() {
        let descriptor = descriptor(TabletState::Active);
        let mut value: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&descriptor).unwrap()).unwrap();
        value["unexpected"] = serde_json::json!(1);
        assert!(serde_json::from_value::<TabletDescriptor>(value).is_err());
    }

    // -- descriptor validation ---------------------------------------------

    #[test]
    fn descriptor_validation_catches_structural_violations() {
        let mut zero_tablet = descriptor(TabletState::Active);
        zero_tablet.tablet_id = TabletId::ZERO;
        assert!(matches!(
            zero_tablet.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));

        let mut duplicate_nodes = descriptor(TabletState::Active);
        duplicate_nodes.replicas[1].node_id = node(1);
        assert!(matches!(
            duplicate_nodes.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));

        let mut duplicate_raft_ids = descriptor(TabletState::Active);
        duplicate_raft_ids.replicas[1].raft_node_id = 11;
        assert!(matches!(
            duplicate_raft_ids.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));

        let mut foreign_leader = descriptor(TabletState::Active);
        foreign_leader.leader_hint = Some(node(8));
        assert!(matches!(
            foreign_leader.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));

        // A non-creating tablet needs at least one voter.
        let mut learner_only = descriptor(TabletState::Active);
        learner_only.replicas.iter_mut().for_each(|replica| {
            replica.role = ReplicaRole::Learner;
        });
        assert!(matches!(
            learner_only.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));
        // In Creating, an all-learner replica set is the norm (spec 12.5).
        let mut creating = learner_only.clone();
        creating.state = TabletState::Creating;
        creating.validate().unwrap();

        descriptor(TabletState::Active).validate().unwrap();
    }

    // -- tablet state transition graph --------------------------------------

    #[test]
    fn transition_graph_allows_exactly_the_documented_edges() {
        use TabletState::{Active, Creating, Merging, Retired, Retiring, Splitting};
        let allowed = [
            (Creating, Active),
            (Creating, Retired),
            (Active, Splitting),
            (Active, Merging),
            (Active, Retiring),
            (Splitting, Active),
            (Splitting, Retiring),
            (Merging, Active),
            (Merging, Retiring),
            (Retiring, Retired),
        ];
        let states = [Creating, Active, Splitting, Merging, Retiring, Retired];
        for from in states {
            for to in states {
                assert_eq!(
                    from.can_transition_to(to),
                    allowed.contains(&(from, to)),
                    "unexpected graph edge {from} -> {to}"
                );
            }
        }
    }

    #[test]
    fn try_transition_enforces_the_graph() {
        let mut tablet = descriptor(TabletState::Active);
        let error = tablet.try_transition(TabletState::Creating).unwrap_err();
        assert!(matches!(
            error,
            TabletError::InvalidStateTransition { tablet, from, to }
                if tablet == tablet_id(9)
                    && from == TabletState::Active
                    && to == TabletState::Creating
        ));
        tablet.try_transition(TabletState::Splitting).unwrap();
        tablet.try_transition(TabletState::Retiring).unwrap();
        tablet.try_transition(TabletState::Retired).unwrap();
        // Retired is terminal.
        assert!(tablet.try_transition(TabletState::Active).is_err());
    }

    #[test]
    fn published_transition_stages_an_atomic_publication() {
        let tablet = descriptor(TabletState::Active);
        let splitting = tablet.published_transition(TabletState::Splitting).unwrap();
        // The staged copy advanced; the original is untouched.
        assert_eq!(splitting.state, TabletState::Splitting);
        assert_eq!(splitting.generation, 8);
        assert_eq!(tablet.state, TabletState::Active);
        assert_eq!(tablet.generation, 7);
        splitting.validate().unwrap();

        // The graph still applies: Active -> Creating is not an edge.
        assert!(matches!(
            tablet.published_transition(TabletState::Creating),
            Err(TabletError::InvalidStateTransition { .. })
        ));
    }

    #[test]
    fn routability_matches_the_split_merge_protocol() {
        assert!(TabletState::Active.is_routable());
        assert!(TabletState::Splitting.is_routable());
        assert!(TabletState::Merging.is_routable());
        assert!(!TabletState::Creating.is_routable());
        assert!(!TabletState::Retiring.is_routable());
        assert!(!TabletState::Retired.is_routable());
    }

    // -- partition bounds ---------------------------------------------------

    #[test]
    fn bounds_containment_respects_inclusion() {
        let range = bounds(Bound::Included(key(b"b")), Bound::Excluded(key(b"f")));
        assert!(!range.contains(&key(b"a")));
        assert!(range.contains(&key(b"b")));
        assert!(range.contains(&key(b"e")));
        assert!(!range.contains(&key(b"f")));
        assert!(!range.contains(&key(b"z")));

        let open = bounds(Bound::Excluded(key(b"b")), Bound::Included(key(b"f")));
        assert!(!open.contains(&key(b"b")));
        assert!(open.contains(&key(b"f")));

        let everything = PartitionBounds::unbounded();
        assert!(everything.contains(&key(b"")));
        assert!(everything.contains(&key(b"zzzz")));
    }

    #[test]
    fn bounds_overlap_and_adjacency_matrix() {
        let lower = bounds(Bound::Included(key(b"a")), Bound::Excluded(key(b"c")));
        let upper = bounds(Bound::Included(key(b"c")), Bound::Excluded(key(b"e")));
        // [a, c) and [c, e): adjacent, no overlap.
        assert!(!lower.overlaps(&upper));
        assert!(lower.is_adjacent_to(&upper));
        assert!(upper.is_adjacent_to(&lower));

        // [a, c] and [c, e): overlap at c, not adjacent.
        let lower_closed = bounds(Bound::Included(key(b"a")), Bound::Included(key(b"c")));
        assert!(lower_closed.overlaps(&upper));
        assert!(!lower_closed.is_adjacent_to(&upper));

        // [a, c) and (c, e]: c uncovered: neither overlap nor adjacent.
        let upper_open = bounds(Bound::Excluded(key(b"c")), Bound::Included(key(b"e")));
        assert!(!lower.overlaps(&upper_open));
        assert!(!lower.is_adjacent_to(&upper_open));

        // Nested ranges overlap; unbounded overlaps everything.
        let nested = bounds(Bound::Included(key(b"a0")), Bound::Excluded(key(b"b")));
        assert!(lower.overlaps(&nested));
        assert!(PartitionBounds::unbounded().overlaps(&lower));
        assert!(!PartitionBounds::unbounded().is_adjacent_to(&lower));

        // Disjoint ranges with a gap.
        let far = bounds(Bound::Included(key(b"x")), Bound::Unbounded);
        assert!(!lower.overlaps(&far));
        assert!(!lower.is_adjacent_to(&far));
    }

    #[test]
    fn bounds_validation_rejects_empty_and_inverted_ranges() {
        assert!(matches!(
            PartitionBounds::new(Bound::Included(key(b"m")), Bound::Excluded(key(b"a"))),
            Err(TabletError::InvalidBounds(_))
        ));
        // [k, k) is empty.
        assert!(matches!(
            PartitionBounds::new(Bound::Included(key(b"k")), Bound::Excluded(key(b"k"))),
            Err(TabletError::InvalidBounds(_))
        ));
        // (k, k] and (k, k) are empty.
        assert!(
            PartitionBounds::new(Bound::Excluded(key(b"k")), Bound::Included(key(b"k"))).is_err()
        );
        // [k, k] is the single point k.
        let point = bounds(Bound::Included(key(b"k")), Bound::Included(key(b"k")));
        assert!(point.contains(&key(b"k")));
        assert!(!point.contains(&key(b"j")));
    }

    #[test]
    fn split_at_partitions_the_range_with_no_gap_or_overlap() {
        let range = bounds(Bound::Included(key(b"b")), Bound::Excluded(key(b"f")));
        let (lower, upper) = range.split_at(&key(b"d")).unwrap();
        assert_eq!(
            lower,
            bounds(Bound::Included(key(b"b")), Bound::Excluded(key(b"d")))
        );
        assert_eq!(
            upper,
            bounds(Bound::Included(key(b"d")), Bound::Excluded(key(b"f")))
        );
        // The halves meet at the split key: adjacent, never overlapping.
        assert!(lower.meets_start_of(&upper));
        assert!(lower.is_adjacent_to(&upper));
        assert!(!lower.overlaps(&upper));
        for candidate in [b"a", b"b", b"c", b"d", b"e", b"f", b"g"] {
            let candidate = key(candidate);
            assert_eq!(
                range.contains(&candidate),
                lower.contains(&candidate) || upper.contains(&candidate),
                "coverage mismatch at {candidate}"
            );
            assert!(
                !(lower.contains(&candidate) && upper.contains(&candidate)),
                "double coverage at {candidate}"
            );
        }

        // Unbounded sides split fine; the split key itself lands in the upper half.
        let whole = PartitionBounds::unbounded();
        let (low_half, high_half) = whole.split_at(&key(b"k")).unwrap();
        assert!(matches!(low_half.low, Bound::Unbounded));
        assert!(matches!(high_half.high, Bound::Unbounded));
        assert!(!low_half.contains(&key(b"k")));
        assert!(high_half.contains(&key(b"k")));

        // Keys outside the range, or at the very edge, never split.
        assert!(range.split_at(&key(b"a")).is_none());
        assert!(range.split_at(&key(b"f")).is_none());
        assert!(range.split_at(&key(b"b")).is_none()); // would empty the lower half
                                                       // A single-point range cannot split.
        let point = bounds(Bound::Included(key(b"b")), Bound::Included(key(b"b")));
        assert!(point.split_at(&key(b"b")).is_none());
        // An included high endpoint may split: the upper half is the point.
        let closed = bounds(Bound::Included(key(b"b")), Bound::Included(key(b"f")));
        let (_, upper) = closed.split_at(&key(b"f")).unwrap();
        assert_eq!(
            upper,
            bounds(Bound::Included(key(b"f")), Bound::Included(key(b"f")))
        );
    }

    #[test]
    fn union_adjacent_combines_only_adjacent_ranges() {
        let lower = bounds(Bound::Included(key(b"a")), Bound::Excluded(key(b"c")));
        let upper = bounds(Bound::Included(key(b"c")), Bound::Excluded(key(b"e")));
        let combined = bounds(Bound::Included(key(b"a")), Bound::Excluded(key(b"e")));
        // Order-independent.
        assert_eq!(lower.union_adjacent(&upper).unwrap(), combined);
        assert_eq!(upper.union_adjacent(&lower).unwrap(), combined);

        // Overlap, gaps, and unbounded meets never combine.
        let overlapping = bounds(Bound::Included(key(b"a")), Bound::Included(key(b"c")));
        assert!(lower.union_adjacent(&overlapping).is_none());
        let gapped = bounds(Bound::Excluded(key(b"c")), Bound::Excluded(key(b"e")));
        assert!(lower.union_adjacent(&gapped).is_none());
        assert!(PartitionBounds::unbounded()
            .union_adjacent(&lower)
            .is_none());

        // The merged chain covers exactly the union.
        let left = bounds(Bound::Unbounded, Bound::Excluded(key(b"c")));
        assert_eq!(
            left.union_adjacent(&upper).unwrap(),
            bounds(Bound::Unbounded, Bound::Excluded(key(b"e")))
        );
    }

    // -- row key encoder -----------------------------------------------------

    #[test]
    fn encoded_keys_preserve_typed_order() {
        let ints: Vec<Key> = [i64::MIN, -7, -1, 0, 1, 42, i64::MAX]
            .into_iter()
            .map(int_key)
            .collect();
        let mut shuffled = ints.clone();
        shuffled.reverse();
        shuffled.sort();
        assert_eq!(shuffled, ints);

        // Text is bytewise, prefix-free: "a" < "a\0" < "aa" < "b".
        let mut texts = [
            text_key("aa"),
            text_key("a"),
            RowKeyEncoder::encode_key(&[KeyValue::Text("a\0".to_owned())]),
            text_key("b"),
        ];
        texts.sort();
        assert_eq!(texts[0], text_key("a"));
        assert_eq!(texts[2], text_key("aa"));

        // Cross-type order is the tag order.
        let values = [
            KeyValue::Null,
            KeyValue::Bool(true),
            KeyValue::Int(1),
            KeyValue::TimestampMicros(1),
            KeyValue::Text("x".to_owned()),
            KeyValue::Bytes(vec![0xFF]),
        ];
        let mut cross: Vec<Key> = values
            .iter()
            .map(|value| RowKeyEncoder::encode_key(std::slice::from_ref(value)))
            .collect();
        let sorted = cross.clone();
        cross.reverse();
        cross.sort();
        assert_eq!(cross, sorted);
    }

    #[test]
    fn encoded_keys_decode_back_to_their_components() {
        let values = vec![
            KeyValue::Null,
            KeyValue::Bool(true),
            KeyValue::Int(-42),
            KeyValue::TimestampMicros(1_700_000_000_000_000),
            KeyValue::Text("tenant\0-42".to_owned()),
            KeyValue::Bytes(vec![0x00, 0x01, 0xFF]),
        ];
        let encoded = RowKeyEncoder::encode_key(&values);
        assert_eq!(RowKeyEncoder::decode_components(&encoded).unwrap(), values);

        // Malformed input fails closed.
        assert!(matches!(
            RowKeyEncoder::decode_components(&key(&[0x7F])),
            Err(PartitionError::MalformedKey(_))
        ));
        assert!(matches!(
            RowKeyEncoder::decode_components(&key(&[TAG_TEXT, b'a'])),
            Err(PartitionError::MalformedKey(_))
        ));
    }

    // -- partitioning: validation and extraction ------------------------------

    #[test]
    fn partitioning_validation_fails_closed_on_bad_declarations() {
        let no_columns = Partitioning::Hash {
            columns: vec![],
            buckets: 16,
        };
        assert!(matches!(
            no_columns.validate(),
            Err(PartitionError::InvalidPartitioning(_))
        ));
        let no_buckets = Partitioning::Hash {
            columns: vec![ColumnId(1)],
            buckets: 0,
        };
        assert!(no_buckets.validate().is_err());
        let unsorted = Partitioning::Range {
            columns: vec![ColumnId(1)],
            splits: vec![int_key(20), int_key(10)],
        };
        assert!(unsorted.validate().is_err());
        let no_tenant_buckets = Partitioning::Tenant {
            tenant_column: ColumnId(1),
            buckets_per_tenant: 0,
        };
        assert!(no_tenant_buckets.validate().is_err());
        assert!(TimeInterval::micros(0).is_err());
        assert!(TimeInterval::days(1).is_ok());
    }

    #[test]
    fn partition_key_extracts_declared_columns_in_declared_order() {
        let partitioning = Partitioning::Hash {
            columns: vec![ColumnId(5), ColumnId(2)],
            buckets: 16,
        };
        let mut values = BTreeMap::new();
        values.insert(ColumnId(2), KeyValue::Int(2));
        values.insert(ColumnId(5), KeyValue::Text("tenant".to_owned()));
        values.insert(ColumnId(9), KeyValue::Bool(true)); // not a partition column
        let key = partitioning.partition_key(&values).unwrap();
        // Declared order (5, 2), not column-id order (2, 5).
        assert_eq!(
            RowKeyEncoder::decode_components(&key).unwrap(),
            vec![KeyValue::Text("tenant".to_owned()), KeyValue::Int(2)]
        );

        // A missing declared column fails closed.
        let mut incomplete = BTreeMap::new();
        incomplete.insert(ColumnId(2), KeyValue::Int(2));
        assert_eq!(
            partitioning.partition_key(&incomplete).unwrap_err(),
            PartitionError::MissingPartitionColumn {
                column: ColumnId(5)
            }
        );
    }

    // -- partitioning: route() --------------------------------------------------

    #[test]
    fn hash_route_is_deterministic_and_bounded() {
        let partitioning = Partitioning::Hash {
            columns: vec![ColumnId(1)],
            buckets: 16,
        };
        let mut values = BTreeMap::new();
        values.insert(ColumnId(1), KeyValue::Int(42));
        let key = partitioning.partition_key(&values).unwrap();
        let slot = partitioning.route(&key).unwrap();
        assert_eq!(slot, partitioning.route(&key).unwrap());
        assert!(slot < 16);

        // Distinct keys spread over distinct buckets.
        let buckets: std::collections::BTreeSet<u64> = (0..100)
            .map(|i| {
                let key = RowKeyEncoder::encode_key(&[KeyValue::Int(i)]);
                partitioning.route(&key).unwrap()
            })
            .collect();
        assert!(buckets.len() > 8, "poor spread: {buckets:?}");
    }

    #[test]
    fn range_route_indexes_the_split_points() {
        let partitioning = Partitioning::Range {
            columns: vec![ColumnId(1)],
            splits: vec![int_key(10), int_key(20)],
        };
        let route = |value: i64| partitioning.route(&int_key(value)).unwrap();
        assert_eq!(route(i64::MIN), 0);
        assert_eq!(route(9), 0);
        assert_eq!(route(10), 1); // a split starts its own partition
        assert_eq!(route(19), 1);
        assert_eq!(route(20), 2);
        assert_eq!(route(i64::MAX), 2);

        // range_bounds reconstructs the covering partition chain.
        let zero = partitioning.range_bounds(0).unwrap();
        let one = partitioning.range_bounds(1).unwrap();
        let two = partitioning.range_bounds(2).unwrap();
        assert!(partitioning.range_bounds(3).is_none());
        assert!(zero.is_adjacent_to(&one));
        assert!(one.is_adjacent_to(&two));
        assert!(!zero.is_adjacent_to(&two));
        assert!(zero.contains(&int_key(9)));
        assert!(!zero.contains(&int_key(10)));
        assert!(two.contains(&int_key(20)));
        assert!(matches!(zero.low, Bound::Unbounded));
        assert!(matches!(two.high, Bound::Unbounded));
    }

    #[test]
    fn tenant_route_is_per_tenant_deterministic_and_bounded() {
        let partitioning = Partitioning::Tenant {
            tenant_column: ColumnId(1),
            buckets_per_tenant: 8,
        };
        let key_for = |tenant: &str| {
            let mut values = BTreeMap::new();
            values.insert(ColumnId(1), KeyValue::Text(tenant.to_owned()));
            partitioning.partition_key(&values).unwrap()
        };
        let acme = key_for("acme");
        let slot = partitioning.route(&acme).unwrap();
        assert_eq!(slot, partitioning.route(&acme).unwrap());
        assert!(slot < 8);
        // The slot is per tenant; the routed key disambiguates (spec 12.2).
        let routed = partitioning.routed_key(&acme).unwrap();
        let mut expected = acme.as_bytes().to_vec();
        expected.extend_from_slice(&slot.to_be_bytes());
        assert_eq!(routed, key(&expected));

        let initech = key_for("initech");
        assert_eq!(
            partitioning.route(&initech).unwrap(),
            partitioning.route(&key_for("initech")).unwrap()
        );
    }

    #[test]
    fn time_range_route_buckets_by_interval() {
        let interval = TimeInterval::hours(1).unwrap();
        let partitioning = Partitioning::TimeRange {
            timestamp_column: ColumnId(3),
            interval,
        };
        let key_for = |micros: i64| {
            let mut values = BTreeMap::new();
            values.insert(ColumnId(3), KeyValue::TimestampMicros(micros));
            partitioning.partition_key(&values).unwrap()
        };
        let width = i64::try_from(interval.as_micros()).unwrap();
        assert_eq!(partitioning.route(&key_for(0)).unwrap(), 0);
        assert_eq!(partitioning.route(&key_for(width - 1)).unwrap(), 0);
        assert_eq!(partitioning.route(&key_for(width)).unwrap(), 1);
        assert_eq!(partitioning.route(&key_for(5 * width / 2)).unwrap(), 2);

        // Pre-epoch timestamps fail closed.
        assert!(matches!(
            partitioning.route(&key_for(-1)),
            Err(PartitionError::NegativeSlot { micros: -1 })
        ));

        // The timestamp column must carry a timestamp.
        let mut wrong_type = BTreeMap::new();
        wrong_type.insert(ColumnId(3), KeyValue::Int(0));
        assert_eq!(
            partitioning.partition_key(&wrong_type).unwrap_err(),
            PartitionError::PartitionColumnType {
                column: ColumnId(3),
                expected: "timestamp-micros",
                found: "int",
            }
        );
    }

    // -- table partitioning records and colocation -----------------------------

    #[test]
    fn table_partitioning_record_round_trips_with_colocation() {
        let record = TablePartitioningRecord {
            table_id: TableId::new(4),
            partitioning: Partitioning::Range {
                columns: vec![ColumnId(1)],
                splits: vec![int_key(100)],
            },
            origin: PartitioningOrigin::Declared,
            colocated_with: Some(ColocatedWith(TableId::new(2))),
        };
        record.validate().unwrap();
        let json = serde_json::to_vec(&record).unwrap();
        let back: TablePartitioningRecord = serde_json::from_slice(&json).unwrap();
        assert_eq!(back, record);
    }

    #[test]
    fn automatic_defaults_are_visible_in_schema_metadata() {
        let record =
            TablePartitioningRecord::automatic_default(TableId::new(4), vec![ColumnId(1)], 64);
        assert_eq!(record.origin, PartitioningOrigin::AutomaticDefault);
        assert_eq!(record.partitioning.partition_columns(), vec![ColumnId(1)]);
        assert_eq!(record.colocated_with, None);
        record.validate().unwrap();

        // The default is serialized, not implicit.
        let json = serde_json::to_value(&record).unwrap();
        assert_eq!(json["origin"], serde_json::json!("AutomaticDefault"));
    }

    #[test]
    fn record_validation_rejects_zero_table_and_self_colocation() {
        let mut zero =
            TablePartitioningRecord::automatic_default(TableId::ZERO, vec![ColumnId(1)], 4);
        assert!(matches!(
            zero.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));
        zero.table_id = TableId::new(4);
        zero.colocated_with = Some(ColocatedWith(TableId::new(4)));
        assert!(matches!(
            zero.validate(),
            Err(TabletError::InvalidDescriptor(_))
        ));
    }

    // -- tablet routing (spec 12.4) ---------------------------------------------

    fn routed_hash_tablet(byte: u8, state: TabletState, slots: (u64, u64)) -> TabletDescriptor {
        let mut tablet = descriptor(state);
        tablet.tablet_id = tablet_id(byte);
        tablet.table_id = TableId::new(5);
        tablet.partition = hash_slot_bounds(slots.0, slots.1);
        tablet
    }

    #[test]
    fn point_reads_and_writes_route_directly() {
        let partitioning = Partitioning::Hash {
            columns: vec![ColumnId(1)],
            buckets: 4,
        };
        let tablets = vec![
            routed_hash_tablet(1, TabletState::Active, (0, 2)),
            routed_hash_tablet(2, TabletState::Active, (2, 4)),
        ];
        for value in 0..50 {
            let key = RowKeyEncoder::encode_key(&[KeyValue::Int(value)]);
            let routed = partitioning.routed_key(&key).unwrap();
            let slot = partitioning.route(&key).unwrap();
            let expected = tablet_id(if slot < 2 { 1 } else { 2 });
            assert_eq!(
                find_tablet_for_key(&tablets, TableId::new(5), &routed).map(|t| t.tablet_id),
                Some(expected),
                "value {value} routed wrong"
            );
            // Other tables and unroutable states never match.
            assert!(find_tablet_for_key(&tablets, TableId::new(6), &routed).is_none());
        }
        // A Creating child tablet is never exposed before catch-up.
        let children = vec![
            routed_hash_tablet(1, TabletState::Active, (0, 2)),
            routed_hash_tablet(3, TabletState::Creating, (0, 1)),
        ];
        let routed = partitioning
            .routed_key(&RowKeyEncoder::encode_key(&[KeyValue::Int(1)]))
            .unwrap();
        let slot = partitioning
            .route(&RowKeyEncoder::encode_key(&[KeyValue::Int(1)]))
            .unwrap();
        if slot < 2 {
            assert_eq!(
                find_tablet_for_key(&children, TableId::new(5), &routed).map(|t| t.tablet_id),
                Some(tablet_id(1))
            );
        }
    }

    #[test]
    fn range_queries_route_to_all_overlapping_tablets_in_order() {
        let t = |byte: u8, low: Bound<Key>, high: Bound<Key>| {
            let mut tablet = descriptor(TabletState::Active);
            tablet.tablet_id = tablet_id(byte);
            tablet.table_id = TableId::new(5);
            tablet.partition = bounds(low, high);
            tablet
        };
        let tablets = vec![
            t(3, Bound::Included(text_key("m")), Bound::Unbounded),
            t(1, Bound::Unbounded, Bound::Excluded(text_key("e"))),
            t(
                2,
                Bound::Included(text_key("e")),
                Bound::Excluded(text_key("m")),
            ),
            {
                let mut retired = t(4, Bound::Unbounded, Bound::Excluded(text_key("e")));
                retired.state = TabletState::Retired;
                retired
            },
        ];
        // Point-narrow range inside the middle tablet.
        let narrow = bounds(
            Bound::Included(text_key("f")),
            Bound::Excluded(text_key("g")),
        );
        assert_eq!(
            tablets_overlapping(&tablets, TableId::new(5), &narrow)
                .iter()
                .map(|tablet| tablet.tablet_id)
                .collect::<Vec<_>>(),
            vec![tablet_id(2)]
        );
        // Full scan: all routable tablets, sorted by low endpoint, retired excluded.
        assert_eq!(
            tablets_overlapping(&tablets, TableId::new(5), &PartitionBounds::unbounded())
                .iter()
                .map(|tablet| tablet.tablet_id)
                .collect::<Vec<_>>(),
            vec![tablet_id(1), tablet_id(2), tablet_id(3)]
        );
    }

    #[test]
    fn stale_generations_classify_per_tablet_state() {
        let tablet = descriptor(TabletState::Active);
        assert!(check_generation(&tablet, 7).is_ok());

        let mut splitting = tablet.clone();
        splitting.state = TabletState::Splitting;
        let error = check_generation(&splitting, 6).unwrap_err();
        assert_eq!(
            error,
            RoutingError::TabletSplit {
                tablet_id: tablet_id(9),
                used_generation: 6,
                current_generation: 7,
            }
        );
        assert_eq!(error.category(), ErrorCategory::TabletSplitting);

        let mut retiring = tablet.clone();
        retiring.state = TabletState::Retiring;
        let error = check_generation(&retiring, 6).unwrap_err();
        assert!(matches!(error, RoutingError::TabletMoved { .. }));
        assert_eq!(error.category(), ErrorCategory::TabletMoved);

        // Any other mismatch — including a request newer than the replica —
        // is plain stale metadata.
        let error = check_generation(&tablet, 6).unwrap_err();
        assert!(matches!(error, RoutingError::StaleMetadata { .. }));
        assert_eq!(error.category(), ErrorCategory::StaleMetadata);
        let error = check_generation(&tablet, 8).unwrap_err();
        assert!(matches!(error, RoutingError::StaleMetadata { .. }));

        // Review N2: newer generation while Splitting is StaleMetadata
        // (replica behind), not TabletSplit.
        let error = check_generation(&splitting, 9).unwrap_err();
        assert!(
            matches!(error, RoutingError::StaleMetadata { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn list_tablets_on_disk_reads_real_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let desc = descriptor(TabletState::Active);
        let layout = TabletLayout::new(dir.path(), desc.tablet_id, desc.raft_group_id);
        layout.create(&desc).unwrap();
        let (listed, issues) = list_tablets_on_disk(dir.path()).unwrap();
        assert!(issues.is_empty());
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].tablet_id, desc.tablet_id);
        assert_eq!(listed[0].replicas.len(), desc.replicas.len());
    }

    // -- tablet layout (spec 12.3) ---------------------------------------------

    fn layout_fixture(root: &Path) -> (TabletLayout, TabletDescriptor) {
        let descriptor = descriptor(TabletState::Active);
        let layout = TabletLayout::new(root, descriptor.tablet_id, descriptor.raft_group_id);
        (layout, descriptor)
    }

    #[test]
    fn layout_create_makes_the_spec_directory_tree_and_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();

        for path in [
            layout.state_dir(),
            layout.runs_dir(),
            layout.indexes_dir(),
            layout.temp_dir(),
            layout.raft_dir(),
            layout.snapshots_dir(),
        ] {
            assert!(path.is_dir(), "missing {}", path.display());
        }
        assert!(layout.metadata_path().is_file());
        // The spec tree: node-data/tablets/<tablet-id>, groups/<group-id>.
        assert_eq!(
            layout.tablet_dir(),
            dir.path()
                .join(TABLETS_DIR)
                .join(descriptor.tablet_id.to_hex())
        );
        assert_eq!(
            layout.group_dir(),
            dir.path()
                .join(GROUPS_DIR)
                .join(descriptor.raft_group_id.to_hex())
        );

        // The persisted metadata verifies and round-trips.
        assert_eq!(layout.validate().unwrap(), descriptor);
        assert_eq!(layout.load_metadata().unwrap(), descriptor);
    }

    #[test]
    fn layout_create_is_idempotent_but_never_repurposes_a_directory() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();
        // Identical re-create succeeds (crash-recovered create).
        layout.create(&descriptor).unwrap();
        // Different metadata fails closed.
        let mut other = descriptor.clone();
        other.generation = 8;
        assert!(matches!(
            layout.create(&other),
            Err(TabletError::MetadataConflict(_))
        ));
    }

    #[test]
    fn layout_store_metadata_advances_the_descriptor_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();
        let mut advanced = descriptor.clone();
        advanced.generation = 8;
        advanced.state = TabletState::Splitting;
        layout.store_metadata(&advanced).unwrap();
        assert_eq!(layout.load_metadata().unwrap(), advanced);

        // A descriptor for another tablet/group is refused.
        let mut foreign = advanced.clone();
        foreign.tablet_id = tablet_id(8);
        assert!(matches!(
            layout.store_metadata(&foreign),
            Err(TabletError::TabletMismatch { .. })
        ));
    }

    #[test]
    fn layout_open_fails_closed_on_missing_or_corrupt_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());

        // Nothing created: metadata is missing.
        assert!(matches!(
            layout.load_metadata(),
            Err(TabletError::MissingMetadata(_))
        ));
        layout.create(&descriptor).unwrap();

        // Garbage bytes.
        std::fs::write(layout.metadata_path(), b"{ not json").unwrap();
        assert!(matches!(
            layout.load_metadata(),
            Err(TabletError::Metadata(ClusterError::CorruptMetadata { .. }))
        ));

        // Unknown format version.
        layout.store_metadata(&descriptor).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(layout.metadata_path()).unwrap()).unwrap();
        value["format_version"] = serde_json::json!(99);
        std::fs::write(layout.metadata_path(), serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            layout.load_metadata(),
            Err(TabletError::Metadata(
                ClusterError::UnsupportedFormatVersion { found: 99, .. }
            ))
        ));
    }

    #[test]
    fn layout_open_fails_closed_on_checksum_or_identity_drift() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();

        // Tamper with the payload: the checksum no longer matches.
        let path = layout.metadata_path();
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        value["tablet"]["generation"] = serde_json::json!(8);
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(
            layout.load_metadata(),
            Err(TabletError::Metadata(ClusterError::CorruptMetadata { .. }))
        ));

        // A foreign tablet's (self-consistent) metadata in this directory is
        // an identity error, never a silent open.
        let other_dir = tempfile::tempdir().unwrap();
        let mut foreign = descriptor.clone();
        foreign.tablet_id = tablet_id(8);
        let foreign_layout =
            TabletLayout::new(other_dir.path(), foreign.tablet_id, foreign.raft_group_id);
        foreign_layout.create(&foreign).unwrap();
        std::fs::write(
            &path,
            std::fs::read(foreign_layout.metadata_path()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            layout.load_metadata(),
            Err(TabletError::TabletMismatch { .. })
        ));
    }

    #[test]
    fn layout_validate_requires_the_full_directory_tree() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();
        layout.validate().unwrap();

        std::fs::remove_dir(layout.runs_dir()).unwrap();
        assert!(matches!(
            layout.validate(),
            Err(TabletError::Metadata(ClusterError::CorruptMetadata { .. }))
        ));
    }

    #[test]
    fn teardown_removes_the_replica_idempotently_but_never_foreign_state() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();
        assert!(layout.tablet_dir().is_dir() && layout.group_dir().is_dir());

        layout.teardown().unwrap();
        assert!(!layout.tablet_dir().exists());
        assert!(!layout.group_dir().exists());
        // Tearing down an absent replica is fine (crash-resumed removal).
        layout.teardown().unwrap();

        // A directory holding foreign metadata is never deleted.
        let other_dir = tempfile::tempdir().unwrap();
        let mut foreign = descriptor.clone();
        foreign.tablet_id = tablet_id(8);
        let foreign_layout =
            TabletLayout::new(other_dir.path(), foreign.tablet_id, foreign.raft_group_id);
        foreign_layout.create(&foreign).unwrap();
        layout.create(&descriptor).unwrap();
        std::fs::write(
            layout.metadata_path(),
            std::fs::read(foreign_layout.metadata_path()).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            layout.teardown(),
            Err(TabletError::TabletMismatch { .. })
        ));
        assert!(layout.tablet_dir().is_dir(), "foreign state was deleted");
    }

    // -- process-local ownership (spec 4.1 / 12.3) -------------------------------

    #[test]
    fn one_tablet_storage_core_is_owned_by_one_process() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();

        let registry = TabletOwnershipRegistry::new();
        let guard = registry.try_reserve(&layout).unwrap();
        assert_eq!(guard.tablet_id(), descriptor.tablet_id);
        assert_eq!(registry.len(), 1);

        // A second open of the same tablet fails closed — even by the owner.
        let error = registry.try_reserve(&layout).unwrap_err();
        assert!(matches!(
            error,
            TabletError::AlreadyOwned { tablet, .. } if tablet == descriptor.tablet_id
        ));

        // A different tablet directory reserves independently.
        let mut distinct = descriptor.clone();
        distinct.tablet_id = tablet_id(10);
        let distinct_layout =
            TabletLayout::new(dir.path(), distinct.tablet_id, distinct.raft_group_id);
        distinct_layout.create(&distinct).unwrap();
        let _distinct_guard = registry.try_reserve(&distinct_layout).unwrap();
        assert_eq!(registry.len(), 2);

        // Dropping the guard releases the reservation.
        drop(guard);
        assert_eq!(registry.len(), 1);
        let _reacquired = registry.try_reserve(&layout).unwrap();
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn ownership_reservation_keys_on_the_canonical_path() {
        let dir = tempfile::tempdir().unwrap();
        let (layout, descriptor) = layout_fixture(dir.path());
        layout.create(&descriptor).unwrap();

        let registry = TabletOwnershipRegistry::new();
        let _guard = registry.try_reserve(&layout).unwrap();
        // The same tablet reached through an aliased path (`/./`) is the
        // same reservation.
        let aliased_root = dir.path().join(".");
        let aliased =
            TabletLayout::new(aliased_root, descriptor.tablet_id, descriptor.raft_group_id);
        assert!(matches!(
            registry.try_reserve(&aliased),
            Err(TabletError::AlreadyOwned { .. })
        ));
    }
}
