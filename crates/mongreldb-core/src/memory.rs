//! Global memory governor (spec section 10.5, S1E-003).
//!
//! Implemented in the Stage 1E wave: one node-level [`MemoryGovernor`] owns the
//! budgets of S1E-003 — page cache, decoded cache, query execution, result
//! buffering, AI candidates, compaction, replication, backup, and network
//! buffers — so no subsystem independently allocates beyond its reservation
//! (spec §13.2). Subsystems reserve through
//! [`try_reserve`](MemoryGovernor::try_reserve) and hold the returned
//! [`Reservation`] RAII guard; dropping the guard releases the bytes.
//!
//! ## Pressure and escalation
//!
//! [`pressure`](MemoryGovernor::pressure) is the fraction of the configured
//! maximum in use (`0.0..=1.0`). As it rises the governor escalates in the
//! exact order of S1E-003, each level with its own threshold and hysteresis so
//! the level does not flap around a boundary:
//!
//! 1. [`RejectLowPriority`](EscalationLevel::RejectLowPriority) — new
//!    low-priority work is rejected in `try_reserve`.
//! 2. [`EvictCaches`](EscalationLevel::EvictCaches) — the governor drives
//!    registered reclaimable caches via [`evict_reclaimable`](MemoryGovernor::evict_reclaimable).
//! 3. [`SpillOperators`](EscalationLevel::SpillOperators) — eligible query
//!    operators spill working memory to disk: [`spill_trigger`](MemoryGovernor::spill_trigger)
//!    exposes the signal and [`request_spill_grant`](MemoryGovernor::request_spill_grant)
//!    issues the S1E-004 [`SpillGrant`] (the disk side is
//!    [`crate::spill::SpillManager`]).
//! 4. [`ThrottleMaintenance`](EscalationLevel::ThrottleMaintenance) —
//!    maintenance work is throttled (§13.1: maintenance yields to foreground).
//! 5. At every level the reserved floor holds: replication and network-buffer
//!    (control-plane/replication protocol, §13.1) memory is never fully
//!    starved — non-reserved classes can never consume the last
//!    `reserved_floor_bytes`.
//!
//! The reservation fast path is lock-free (a pair of atomic adds with exact
//! rollback on rejection) and performs no allocation; the [`Reservation`]
//! guard is two words plus one `Arc` refcount bump.

use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use serde::{Deserialize, Serialize};

/// The memory pools of S1E-003: the budgets one node-level governor owns.
///
/// The variant set is exactly the spec's, in spec order; [`index`](Self::index)
/// matches that order and is the layout of
/// [`GovernorConfig::class_budgets`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MemoryClass {
    /// Raw (on-disk form) page cache.
    PageCache,
    /// Decoded (post-decompress/decrypt) page cache.
    DecodedCache,
    /// Query execution operators (hash tables, sort runs in memory).
    QueryExecution,
    /// Result materialization and buffering.
    ResultBuffering,
    /// AI candidate sets (ANN/full-text retrieval buffers).
    AiCandidates,
    /// Compaction and index maintenance.
    Compaction,
    /// Replication log shipping and follow-apply buffers. Reserved.
    Replication,
    /// Backup/export buffers.
    Backup,
    /// Network receive/send buffers; carries the control-plane and
    /// replication protocols. Reserved.
    NetworkBuffers,
}

impl MemoryClass {
    /// Number of classes (array layout of the governor's counters/budgets).
    pub const COUNT: usize = 9;

    /// Every class, in spec order (the [`index`](Self::index) layout).
    pub const ALL: [MemoryClass; MemoryClass::COUNT] = [
        MemoryClass::PageCache,
        MemoryClass::DecodedCache,
        MemoryClass::QueryExecution,
        MemoryClass::ResultBuffering,
        MemoryClass::AiCandidates,
        MemoryClass::Compaction,
        MemoryClass::Replication,
        MemoryClass::Backup,
        MemoryClass::NetworkBuffers,
    ];

    /// Stable index in `0..COUNT` (spec order).
    pub fn index(self) -> usize {
        Self::ALL
            .iter()
            .position(|c| *c == self)
            .expect("ALL is total")
    }

    /// Stable lowercase name.
    pub fn name(self) -> &'static str {
        match self {
            MemoryClass::PageCache => "page_cache",
            MemoryClass::DecodedCache => "decoded_cache",
            MemoryClass::QueryExecution => "query_execution",
            MemoryClass::ResultBuffering => "result_buffering",
            MemoryClass::AiCandidates => "ai_candidates",
            MemoryClass::Compaction => "compaction",
            MemoryClass::Replication => "replication",
            MemoryClass::Backup => "backup",
            MemoryClass::NetworkBuffers => "network_buffers",
        }
    }

    /// Classes whose memory must never be fully starved (S1E-003 step 5,
    /// §13.1 reserved control/replication capacity): the replication pool, and
    /// the network buffers that carry the control-plane and replication
    /// protocols. Only these may draw down the reserved floor.
    pub fn is_reserved(self) -> bool {
        matches!(self, MemoryClass::Replication | MemoryClass::NetworkBuffers)
    }

    /// Deferrable classes rejected first under pressure (S1E-003 step 1,
    /// §13.1 maintenance yields to foreground work).
    pub fn is_low_priority(self) -> bool {
        matches!(self, MemoryClass::Compaction | MemoryClass::Backup)
    }

    /// Classes whose memory is reclaimable on demand (S1E-003 step 2): the
    /// caches can evict entries and hand bytes back.
    pub fn is_reclaimable_cache(self) -> bool {
        matches!(self, MemoryClass::PageCache | MemoryClass::DecodedCache)
    }

    /// Classes whose operator working memory the S1E-004 spill manager can
    /// move to disk under escalation step 3: query execution (hash tables,
    /// sort runs) and result materialization. The caches are reclaimed
    /// instead (step 2), AI candidate sets are recomputable, and the reserved
    /// pools are never spilled.
    pub fn is_spill_eligible(self) -> bool {
        matches!(
            self,
            MemoryClass::QueryExecution | MemoryClass::ResultBuffering
        )
    }
}

impl fmt::Display for MemoryClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// Errors of governor configuration and reservation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MemoryError {
    /// The [`GovernorConfig`] failed validation.
    #[error("invalid memory governor configuration: {0}")]
    InvalidConfig(&'static str),
    /// The reservation did not fit within the class budget, the node maximum,
    /// or (for non-reserved classes) the reserved floor.
    #[error(
        "memory reservation of {requested} bytes for {class} rejected: {available} bytes available"
    )]
    Exhausted {
        /// Requesting pool.
        class: MemoryClass,
        /// Requested bytes.
        requested: u64,
        /// Bytes the requester could still have been granted.
        available: u64,
    },
    /// Low-priority work rejected under pressure (S1E-003 escalation step 1).
    #[error("low-priority memory reservation for {class} rejected: memory pressure (S1E-003 escalation step 1)")]
    LowPriorityRejected {
        /// Requesting pool.
        class: MemoryClass,
    },
}

/// The pressure thresholds at which each escalation level activates, plus the
/// hysteresis band that keeps the level from flapping around a boundary.
///
/// Fractions of the configured maximum, in `(0, 1]`, strictly increasing in
/// the S1E-003 escalation order.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EscalationThresholds {
    /// Step 1: reject new low-priority work.
    pub reject_low_priority: f64,
    /// Step 2: evict reclaimable caches.
    pub evict_caches: f64,
    /// Step 3: spill eligible query operators (S1E-004 hook point).
    pub spill_operators: f64,
    /// Step 4: throttle maintenance.
    pub throttle_maintenance: f64,
    /// De-escalation band: a level drops only once pressure falls below its
    /// activation threshold minus this hysteresis.
    pub hysteresis: f64,
}

impl Default for EscalationThresholds {
    fn default() -> Self {
        Self {
            reject_low_priority: 0.70,
            evict_caches: 0.80,
            spill_operators: 0.90,
            throttle_maintenance: 0.95,
            hysteresis: 0.05,
        }
    }
}

impl EscalationThresholds {
    fn validate(&self) -> Result<(), MemoryError> {
        let in_band = |v: f64| v > 0.0 && v <= 1.0;
        if !in_band(self.reject_low_priority)
            || !in_band(self.evict_caches)
            || !in_band(self.spill_operators)
            || !in_band(self.throttle_maintenance)
        {
            return Err(MemoryError::InvalidConfig(
                "escalation thresholds must be in (0, 1]",
            ));
        }
        if !(self.reject_low_priority < self.evict_caches
            && self.evict_caches < self.spill_operators
            && self.spill_operators < self.throttle_maintenance)
        {
            return Err(MemoryError::InvalidConfig(
                "escalation thresholds must be strictly increasing in S1E-003 order",
            ));
        }
        if !(0.0..self.reject_low_priority).contains(&self.hysteresis) {
            return Err(MemoryError::InvalidConfig(
                "hysteresis must be in [0, reject_low_priority)",
            ));
        }
        Ok(())
    }
}

/// Node-level governor configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GovernorConfig {
    /// Configured maximum bytes the node may reserve across all classes.
    pub max_bytes: u64,
    /// Bytes of `max_bytes` only reserved classes (replication, network
    /// buffers) may consume (S1E-003 step 5).
    pub reserved_floor_bytes: u64,
    /// Per-class budgets in [`MemoryClass::index`] order. Defaults to
    /// `max_bytes` per class (bounded by the node total only).
    pub class_budgets: [u64; MemoryClass::COUNT],
    /// Escalation thresholds.
    pub thresholds: EscalationThresholds,
}

impl GovernorConfig {
    /// A config with the default reserved floor (`max_bytes / 8`), per-class
    /// budgets bounded only by the node total, and default thresholds.
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            reserved_floor_bytes: max_bytes / 8,
            class_budgets: [max_bytes; MemoryClass::COUNT],
            thresholds: EscalationThresholds::default(),
        }
    }

    /// Overrides one class budget.
    pub fn with_class_budget(mut self, class: MemoryClass, bytes: u64) -> Self {
        self.class_budgets[class.index()] = bytes;
        self
    }

    /// Overrides the reserved floor.
    pub fn with_reserved_floor(mut self, bytes: u64) -> Self {
        self.reserved_floor_bytes = bytes;
        self
    }

    /// Overrides the escalation thresholds.
    pub fn with_thresholds(mut self, thresholds: EscalationThresholds) -> Self {
        self.thresholds = thresholds;
        self
    }

    /// Checks the configuration invariants.
    pub fn validate(&self) -> Result<(), MemoryError> {
        if self.max_bytes == 0 {
            return Err(MemoryError::InvalidConfig("max_bytes must be nonzero"));
        }
        if self.reserved_floor_bytes > self.max_bytes {
            return Err(MemoryError::InvalidConfig(
                "reserved_floor_bytes must not exceed max_bytes",
            ));
        }
        if self.class_budgets.iter().any(|b| *b > self.max_bytes) {
            return Err(MemoryError::InvalidConfig(
                "class budgets must not exceed max_bytes",
            ));
        }
        self.thresholds.validate()
    }
}

/// The governor's escalation level under memory pressure, in the exact order
/// of S1E-003 (a higher level implies every lower level's action stays
/// active). Ordering is derived, so levels compare by escalation severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum EscalationLevel {
    /// No pressure response.
    None = 0,
    /// Step 1: reject new low-priority work.
    RejectLowPriority = 1,
    /// Step 2: evict reclaimable caches.
    EvictCaches = 2,
    /// Step 3: spill eligible query operators (S1E-004 hook point).
    SpillOperators = 3,
    /// Step 4: throttle maintenance.
    ThrottleMaintenance = 4,
}

impl EscalationLevel {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => EscalationLevel::None,
            1 => EscalationLevel::RejectLowPriority,
            2 => EscalationLevel::EvictCaches,
            3 => EscalationLevel::SpillOperators,
            _ => EscalationLevel::ThrottleMaintenance,
        }
    }

    /// The next more severe level, if any.
    fn up(self) -> Option<Self> {
        match self {
            EscalationLevel::None => Some(EscalationLevel::RejectLowPriority),
            EscalationLevel::RejectLowPriority => Some(EscalationLevel::EvictCaches),
            EscalationLevel::EvictCaches => Some(EscalationLevel::SpillOperators),
            EscalationLevel::SpillOperators => Some(EscalationLevel::ThrottleMaintenance),
            EscalationLevel::ThrottleMaintenance => None,
        }
    }

    /// The next less severe level.
    fn down(self) -> Self {
        match self {
            EscalationLevel::None => EscalationLevel::None,
            EscalationLevel::RejectLowPriority => EscalationLevel::None,
            EscalationLevel::EvictCaches => EscalationLevel::RejectLowPriority,
            EscalationLevel::SpillOperators => EscalationLevel::EvictCaches,
            EscalationLevel::ThrottleMaintenance => EscalationLevel::SpillOperators,
        }
    }

    /// The pressure at which this level activates.
    fn threshold(self, t: &EscalationThresholds) -> f64 {
        match self {
            EscalationLevel::None => 0.0,
            EscalationLevel::RejectLowPriority => t.reject_low_priority,
            EscalationLevel::EvictCaches => t.evict_caches,
            EscalationLevel::SpillOperators => t.spill_operators,
            EscalationLevel::ThrottleMaintenance => t.throttle_maintenance,
        }
    }

    /// Stable name.
    pub fn name(self) -> &'static str {
        match self {
            EscalationLevel::None => "none",
            EscalationLevel::RejectLowPriority => "reject_low_priority",
            EscalationLevel::EvictCaches => "evict_caches",
            EscalationLevel::SpillOperators => "spill_operators",
            EscalationLevel::ThrottleMaintenance => "throttle_maintenance",
        }
    }
}

impl fmt::Display for EscalationLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A subsystem whose memory the governor can reclaim under pressure (S1E-003
/// step 2). Implemented by the page caches (`crate::cache`); the governor
/// holds implementors weakly, so a dropped cache simply stops being driven.
pub trait Reclaimable: Send + Sync {
    /// Evict at least `budget` bytes of reclaimable entries; returns the bytes
    /// actually freed (fewer when less was reclaimable).
    fn evict_reclaimable(&self, budget: u64) -> u64;
    /// Bytes currently reclaimable.
    fn reclaimable_bytes(&self) -> u64;
}

struct Inner {
    config: GovernorConfig,
    class_used: [AtomicU64; MemoryClass::COUNT],
    total_used: AtomicU64,
    escalation: AtomicU8,
    reservations_granted: AtomicU64,
    reservations_rejected: AtomicU64,
    low_priority_rejected: AtomicU64,
    spill_triggers: AtomicU64,
    /// Reclaimable subsystems (cold path only — registration and pressure
    /// relief; never touched by the reservation fast path).
    reclaimers: parking_lot::Mutex<Vec<Weak<dyn Reclaimable>>>,
}

/// A point-in-time snapshot of governor state (telemetry and tests).
#[derive(Debug, Clone)]
pub struct GovernorStats {
    /// Configured node maximum.
    pub max_bytes: u64,
    /// Configured reserved floor.
    pub reserved_floor_bytes: u64,
    /// Total reserved bytes across all classes.
    pub total_used: u64,
    /// Reserved bytes per class, in [`MemoryClass::index`] order.
    pub class_used: [u64; MemoryClass::COUNT],
    /// `total_used / max_bytes`, clamped to `0.0..=1.0`.
    pub pressure: f64,
    /// Current escalation level.
    pub escalation: EscalationLevel,
    /// Cumulative granted reservations.
    pub reservations_granted: u64,
    /// Cumulative rejected reservations.
    pub reservations_rejected: u64,
    /// Cumulative rejections under escalation step 1.
    pub low_priority_rejected: u64,
    /// Cumulative entries into the spill level (S1E-004 hook signal count).
    pub spill_triggers: u64,
}

impl GovernorStats {
    /// Reserved bytes of one class.
    pub fn usage_for(&self, class: MemoryClass) -> u64 {
        self.class_used[class.index()]
    }
}

/// The node-level memory governor (S1E-003). Cheap to clone (one `Arc`);
/// thread-safe; the reservation fast path is lock-free and allocation-free.
pub struct MemoryGovernor {
    inner: Arc<Inner>,
}

impl Clone for MemoryGovernor {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for MemoryGovernor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryGovernor")
            .field("max_bytes", &self.inner.config.max_bytes)
            .field("total_used", &self.inner.total_used.load(Ordering::Relaxed))
            .field("pressure", &self.pressure())
            .field("escalation", &self.escalation())
            .finish()
    }
}

impl MemoryGovernor {
    /// A governor over `config` (validated).
    pub fn new(config: GovernorConfig) -> Result<Self, MemoryError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                class_used: std::array::from_fn(|_| AtomicU64::new(0)),
                total_used: AtomicU64::new(0),
                escalation: AtomicU8::new(EscalationLevel::None as u8),
                reservations_granted: AtomicU64::new(0),
                reservations_rejected: AtomicU64::new(0),
                low_priority_rejected: AtomicU64::new(0),
                spill_triggers: AtomicU64::new(0),
                reclaimers: parking_lot::Mutex::new(Vec::new()),
            }),
        })
    }

    /// A governor with the default configuration for `max_bytes`.
    pub fn with_max_bytes(max_bytes: u64) -> Result<Self, MemoryError> {
        Self::new(GovernorConfig::new(max_bytes))
    }

    /// The governor's configuration.
    pub fn config(&self) -> &GovernorConfig {
        &self.inner.config
    }

    /// Configured node maximum in bytes.
    pub fn max_bytes(&self) -> u64 {
        self.inner.config.max_bytes
    }

    /// Configured reserved floor in bytes.
    pub fn reserved_floor_bytes(&self) -> u64 {
        self.inner.config.reserved_floor_bytes
    }

    /// Configured budget of one class.
    pub fn class_budget(&self, class: MemoryClass) -> u64 {
        self.inner.config.class_budgets[class.index()]
    }

    /// Currently reserved bytes of one class.
    pub fn usage(&self, class: MemoryClass) -> u64 {
        self.inner.class_used[class.index()].load(Ordering::Relaxed)
    }

    /// Currently reserved bytes across all classes.
    pub fn total_used(&self) -> u64 {
        self.inner.total_used.load(Ordering::Relaxed)
    }

    /// Fraction of the configured maximum in use, clamped to `0.0..=1.0`.
    pub fn pressure(&self) -> f64 {
        let max = self.inner.config.max_bytes;
        if max == 0 {
            return 0.0;
        }
        (self.total_used() as f64 / max as f64).clamp(0.0, 1.0)
    }

    /// The current escalation level (with hysteresis applied).
    pub fn escalation(&self) -> EscalationLevel {
        EscalationLevel::from_u8(self.inner.escalation.load(Ordering::Relaxed))
    }

    /// Step 1 active: new low-priority work is being rejected.
    pub fn should_reject_low_priority_work(&self) -> bool {
        self.escalation() >= EscalationLevel::RejectLowPriority
    }

    /// Step 2 active: reclaimable caches should be evicted
    /// ([`evict_reclaimable`](Self::evict_reclaimable) drives them).
    pub fn should_evict_caches(&self) -> bool {
        self.escalation() >= EscalationLevel::EvictCaches
    }

    /// Step 3 active: eligible query operators should spill. Exposed to the
    /// S1E-004 spill path: [`request_spill_grant`](Self::request_spill_grant)
    /// turns the signal into a typed grant; the disk side is
    /// [`crate::spill::SpillManager`].
    pub fn spill_trigger(&self) -> bool {
        self.escalation() >= EscalationLevel::SpillOperators
    }

    /// Step 4 active: maintenance work should be throttled (§13.1).
    pub fn should_throttle_maintenance(&self) -> bool {
        self.escalation() >= EscalationLevel::ThrottleMaintenance
    }

    /// Reserves `bytes` of `class`, returning an RAII guard that releases the
    /// bytes on drop. Zero-byte reservations always succeed.
    ///
    /// Admission rules, in order:
    ///
    /// 1. Under escalation step 1, low-priority classes are rejected
    ///    ([`MemoryError::LowPriorityRejected`]).
    /// 2. The request must fit the class budget and the node limit — the
    ///    configured maximum for reserved classes, the maximum minus the
    ///    reserved floor for every other class, so replication/control memory
    ///    is never fully starved (S1E-003 step 5).
    ///
    /// The fast path is two atomic adds with exact rollback on rejection; no
    /// locks, no allocation.
    pub fn try_reserve(&self, bytes: u64, class: MemoryClass) -> Result<Reservation, MemoryError> {
        self.inner.try_add(class, bytes)?;
        Ok(Reservation {
            governor: self.clone(),
            class,
            bytes,
        })
    }

    /// Registers a reclaimable subsystem (held weakly) for the governor to
    /// drive under escalation step 2. Cold path; not for hot-path use.
    pub fn register_reclaimable<R: Reclaimable + 'static>(&self, reclaimable: &Arc<R>) {
        self.inner
            .reclaimers
            .lock()
            .push(Arc::downgrade(reclaimable) as Weak<dyn Reclaimable>);
    }

    /// Drives registered reclaimable subsystems until at least `budget` bytes
    /// have been freed (or nothing more is reclaimable), returning the bytes
    /// actually freed. The entry point of escalation step 2. Cold path.
    pub fn evict_reclaimable(&self, budget: u64) -> u64 {
        // Clone the registry so re-entrant registration from a callback
        // cannot deadlock on the lock; weak refs prune dropped subsystems.
        let reclaimers: Vec<Arc<dyn Reclaimable>> = {
            let mut registry = self.inner.reclaimers.lock();
            registry.retain(|weak| weak.strong_count() > 0);
            registry.iter().filter_map(Weak::upgrade).collect()
        };
        let mut freed = 0u64;
        for reclaimer in reclaimers {
            if freed >= budget {
                break;
            }
            freed = freed.saturating_add(reclaimer.evict_reclaimable(budget - freed));
        }
        freed
    }

    /// Bytes currently reclaimable from registered subsystems.
    pub fn reclaimable_bytes(&self) -> u64 {
        let reclaimers: Vec<Arc<dyn Reclaimable>> = {
            let registry = self.inner.reclaimers.lock();
            registry.iter().filter_map(Weak::upgrade).collect()
        };
        reclaimers.iter().map(|r| r.reclaimable_bytes()).sum()
    }

    /// Step 3 entry point (S1E-004): while the spill trigger is active, an
    /// operator holding a [`Reservation`] of a spill-eligible class
    /// ([`MemoryClass::is_spill_eligible`]) may ask to move up to `bytes` of
    /// its working set to disk through [`crate::spill::SpillManager`]. On
    /// success the reservation shrinks immediately — the governor accounts
    /// the memory as freed — and the returned [`SpillGrant`] records the
    /// spilled amount; writing the bytes to spill files is charged against
    /// the query's temporary-disk budget, and reading them back re-reserves
    /// through [`try_reserve`](Self::try_reserve). Returns `None` when the
    /// trigger is inactive, the class is not spill-eligible, or the
    /// reservation holds no bytes.
    pub fn request_spill_grant(
        &self,
        reservation: &mut Reservation,
        bytes: u64,
    ) -> Option<SpillGrant> {
        if !self.spill_trigger() || !reservation.class().is_spill_eligible() {
            return None;
        }
        let bytes = bytes.min(reservation.bytes());
        if bytes == 0 {
            return None;
        }
        reservation
            .resize(reservation.bytes() - bytes)
            .expect("shrinking a reservation always succeeds");
        Some(SpillGrant {
            class: reservation.class(),
            bytes,
        })
    }

    /// A point-in-time snapshot of governor state.
    pub fn stats(&self) -> GovernorStats {
        GovernorStats {
            max_bytes: self.inner.config.max_bytes,
            reserved_floor_bytes: self.inner.config.reserved_floor_bytes,
            total_used: self.total_used(),
            class_used: std::array::from_fn(|i| self.inner.class_used[i].load(Ordering::Relaxed)),
            pressure: self.pressure(),
            escalation: self.escalation(),
            reservations_granted: self.inner.reservations_granted.load(Ordering::Relaxed),
            reservations_rejected: self.inner.reservations_rejected.load(Ordering::Relaxed),
            low_priority_rejected: self.inner.low_priority_rejected.load(Ordering::Relaxed),
            spill_triggers: self.inner.spill_triggers.load(Ordering::Relaxed),
        }
    }

    /// Recomputes the escalation level from current pressure with hysteresis:
    /// escalation is immediate; de-escalation of a level happens only below
    /// its threshold minus the hysteresis band.
    fn recompute_escalation(&self) {
        let pressure = self.pressure();
        let thresholds = &self.inner.config.thresholds;
        let mut level = self.escalation();
        while let Some(next) = level.up() {
            if pressure >= next.threshold(thresholds) {
                level = next;
            } else {
                break;
            }
        }
        while level != EscalationLevel::None
            && pressure < level.threshold(thresholds) - thresholds.hysteresis
        {
            level = level.down();
        }
        let previous =
            EscalationLevel::from_u8(self.inner.escalation.swap(level as u8, Ordering::Relaxed));
        if previous < EscalationLevel::SpillOperators && level >= EscalationLevel::SpillOperators {
            self.inner.spill_triggers.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Inner {
    /// Admission core shared by `try_reserve` and `Reservation::resize`
    /// growth. Exact accounting: add first, validate the post-add totals,
    /// roll back on rejection — so a granted set never exceeds its limits.
    fn try_add(self: &Arc<Self>, class: MemoryClass, bytes: u64) -> Result<(), MemoryError> {
        if bytes == 0 {
            return Ok(());
        }
        let governor = MemoryGovernor {
            inner: Arc::clone(self),
        };
        // A request larger than the node maximum can never be granted; reject
        // it before the atomic adds so an absurd size cannot wrap the
        // counters.
        if bytes > self.config.max_bytes {
            self.reservations_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(MemoryError::Exhausted {
                class,
                requested: bytes,
                available: 0,
            });
        }
        if class.is_low_priority() && governor.escalation() >= EscalationLevel::RejectLowPriority {
            self.low_priority_rejected.fetch_add(1, Ordering::Relaxed);
            self.reservations_rejected.fetch_add(1, Ordering::Relaxed);
            return Err(MemoryError::LowPriorityRejected { class });
        }
        let index = class.index();
        let new_class_used = self.class_used[index].fetch_add(bytes, Ordering::Relaxed) + bytes;
        let new_total_used = self.total_used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        let limit = if class.is_reserved() {
            self.config.max_bytes
        } else {
            self.config.max_bytes - self.config.reserved_floor_bytes
        };
        if new_class_used <= self.config.class_budgets[index] && new_total_used <= limit {
            self.reservations_granted.fetch_add(1, Ordering::Relaxed);
            governor.recompute_escalation();
            Ok(())
        } else {
            self.class_used[index].fetch_sub(bytes, Ordering::Relaxed);
            self.total_used.fetch_sub(bytes, Ordering::Relaxed);
            self.reservations_rejected.fetch_add(1, Ordering::Relaxed);
            Err(MemoryError::Exhausted {
                class,
                requested: bytes,
                available: limit.saturating_sub(new_total_used - bytes),
            })
        }
    }

    /// Releases bytes of a class and recomputes escalation (release can
    /// de-escalate once the hysteresis band is cleared).
    fn release(self: &Arc<Self>, class: MemoryClass, bytes: u64) {
        if bytes == 0 {
            return;
        }
        self.class_used[class.index()].fetch_sub(bytes, Ordering::Relaxed);
        self.total_used.fetch_sub(bytes, Ordering::Relaxed);
        MemoryGovernor {
            inner: Arc::clone(self),
        }
        .recompute_escalation();
    }
}

/// RAII memory reservation: releases its bytes back to the governor on drop.
///
/// Two words plus one `Arc` refcount — no allocation. `Send`/`Sync`, so a
/// reservation can move across threads and live inside async tasks. Leaking a
/// reservation (`mem::forget`) leaks its accounting but is memory-safe.
#[must_use = "a reservation releases its bytes on drop"]
pub struct Reservation {
    governor: MemoryGovernor,
    class: MemoryClass,
    bytes: u64,
}

impl Reservation {
    /// The class this reservation charged.
    pub fn class(&self) -> MemoryClass {
        self.class
    }

    /// The bytes currently held.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Resizes the reservation. Shrinking always succeeds; growth goes
    /// through the same admission rules as
    /// [`try_reserve`](MemoryGovernor::try_reserve) and leaves the reservation
    /// unchanged on failure.
    pub fn resize(&mut self, new_bytes: u64) -> Result<(), MemoryError> {
        if new_bytes == self.bytes {
            return Ok(());
        }
        if new_bytes < self.bytes {
            let delta = self.bytes - new_bytes;
            self.governor.inner.release(self.class, delta);
            self.bytes = new_bytes;
            Ok(())
        } else {
            self.governor
                .inner
                .try_add(self.class, new_bytes - self.bytes)?;
            self.bytes = new_bytes;
            Ok(())
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.governor.inner.release(self.class, self.bytes);
    }
}

impl fmt::Debug for Reservation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reservation")
            .field("class", &self.class)
            .field("bytes", &self.bytes)
            .finish()
    }
}

/// Proof that the governor authorized a spill under escalation step 3
/// (S1E-004): an eligible operator moved `bytes` of working memory out of its
/// reservation into spill files. A pure token — the memory accounting already
/// happened in [`MemoryGovernor::request_spill_grant`]; the disk side is
/// [`crate::spill::SpillManager`]'s per-query budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpillGrant {
    class: MemoryClass,
    bytes: u64,
}

impl SpillGrant {
    /// The memory class the spilled bytes came from.
    pub fn class(self) -> MemoryClass {
        self.class
    }

    /// Bytes the operator spilled (already released from its reservation).
    pub fn bytes(self) -> u64 {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn governor(max: u64, floor: u64) -> MemoryGovernor {
        MemoryGovernor::new(GovernorConfig::new(max).with_reserved_floor(floor)).unwrap()
    }

    #[test]
    fn memory_class_set_matches_spec() {
        // S1E-003: exactly these nine pools, in spec order.
        assert_eq!(MemoryClass::ALL.len(), 9);
        assert_eq!(MemoryClass::COUNT, 9);
        let names: Vec<_> = MemoryClass::ALL.iter().map(|c| c.name()).collect();
        assert_eq!(
            names,
            vec![
                "page_cache",
                "decoded_cache",
                "query_execution",
                "result_buffering",
                "ai_candidates",
                "compaction",
                "replication",
                "backup",
                "network_buffers"
            ]
        );
        for (i, class) in MemoryClass::ALL.iter().enumerate() {
            assert_eq!(class.index(), i);
        }
    }

    #[test]
    fn class_classification() {
        for class in MemoryClass::ALL {
            assert_eq!(
                class.is_reserved(),
                matches!(
                    class,
                    MemoryClass::Replication | MemoryClass::NetworkBuffers
                ),
                "reserved: {class}"
            );
            assert_eq!(
                class.is_low_priority(),
                matches!(class, MemoryClass::Compaction | MemoryClass::Backup),
                "low priority: {class}"
            );
            assert_eq!(
                class.is_reclaimable_cache(),
                matches!(class, MemoryClass::PageCache | MemoryClass::DecodedCache),
                "reclaimable: {class}"
            );
        }
    }

    #[test]
    fn reservation_accounting_and_raii_release() {
        let governor = governor(1024, 128);
        assert_eq!(governor.total_used(), 0);
        assert_eq!(governor.pressure(), 0.0);

        let a = governor
            .try_reserve(100, MemoryClass::QueryExecution)
            .unwrap();
        let b = governor.try_reserve(60, MemoryClass::PageCache).unwrap();
        assert_eq!(governor.usage(MemoryClass::QueryExecution), 100);
        assert_eq!(governor.usage(MemoryClass::PageCache), 60);
        assert_eq!(governor.total_used(), 160);
        assert!((governor.pressure() - 160.0 / 1024.0).abs() < 1e-12);
        assert_eq!(a.bytes(), 100);
        assert_eq!(a.class(), MemoryClass::QueryExecution);

        drop(a);
        assert_eq!(governor.usage(MemoryClass::QueryExecution), 0);
        assert_eq!(governor.total_used(), 60);

        // Explicit clone/drop of the guard still accounts exactly once.
        let stats_before = governor.stats();
        drop(b);
        assert_eq!(governor.total_used(), 0);
        assert_eq!(governor.pressure(), 0.0);
        assert_eq!(
            governor.stats().reservations_granted,
            stats_before.reservations_granted
        );
    }

    #[test]
    fn resize_grows_and_shrinks_with_admission_rules() {
        let governor = governor(1000, 100);
        let mut r = governor
            .try_reserve(100, MemoryClass::QueryExecution)
            .unwrap();
        r.resize(900).unwrap();
        assert_eq!(r.bytes(), 900);
        assert_eq!(governor.total_used(), 900);
        // Growth past the non-reserved limit (max - floor) fails and leaves
        // the reservation unchanged.
        assert!(r.resize(901).is_err());
        assert_eq!(r.bytes(), 900);
        assert_eq!(governor.total_used(), 900);
        // Shrinking always succeeds.
        r.resize(50).unwrap();
        assert_eq!(governor.total_used(), 50);
        r.resize(0).unwrap();
        assert_eq!(governor.total_used(), 0);
        assert!(r.resize(0).is_ok());
    }

    #[test]
    fn per_class_budget_is_enforced() {
        let governor = MemoryGovernor::new(
            GovernorConfig::new(1000).with_class_budget(MemoryClass::AiCandidates, 100),
        )
        .unwrap();
        let _held = governor
            .try_reserve(100, MemoryClass::AiCandidates)
            .unwrap();
        let rejected = governor.try_reserve(1, MemoryClass::AiCandidates);
        assert!(matches!(rejected, Err(MemoryError::Exhausted { .. })));
        // Another class is unaffected (bounded by the node total only).
        assert!(governor
            .try_reserve(500, MemoryClass::QueryExecution)
            .is_ok());
    }

    #[test]
    fn zero_byte_reservations_always_succeed() {
        let governor = governor(100, 10);
        let _full = governor.try_reserve(100, MemoryClass::Replication).unwrap();
        // Node is at the maximum; zero-byte grants still succeed.
        for class in MemoryClass::ALL {
            assert!(
                governor.try_reserve(0, class).is_ok(),
                "zero bytes: {class}"
            );
        }
    }

    #[test]
    fn oversized_requests_are_rejected_without_touching_counters() {
        let governor = governor(100, 10);
        for class in MemoryClass::ALL {
            let rejected = governor.try_reserve(u64::MAX, class);
            assert!(
                matches!(rejected, Err(MemoryError::Exhausted { .. })),
                "oversized: {class}"
            );
        }
        assert_eq!(governor.total_used(), 0);
        for class in MemoryClass::ALL {
            assert_eq!(governor.usage(class), 0, "{class}");
        }
        assert_eq!(governor.stats().reservations_rejected, 9);
        assert_eq!(governor.stats().reservations_granted, 0);
        // The governor is undamaged afterwards.
        assert!(governor.try_reserve(50, MemoryClass::Replication).is_ok());
    }

    #[test]
    fn reserved_floor_never_starved_under_adversarial_pressure() {
        let governor = governor(1000, 100);
        // Adversarial foreground pressure: fill every byte a non-reserved
        // class may take (max - floor).
        let mut held = Vec::new();
        for _ in 0..9 {
            held.push(
                governor
                    .try_reserve(100, MemoryClass::QueryExecution)
                    .unwrap(),
            );
        }
        // The next non-reserved byte would eat the floor: rejected. At 90%
        // pressure escalation step 1 is also active, so low-priority classes
        // are rejected as low-priority work; the rest as exhausted.
        for class in MemoryClass::ALL {
            if class.is_reserved() {
                continue;
            }
            let rejected = governor.try_reserve(1, class);
            if class.is_low_priority() {
                assert!(
                    matches!(rejected, Err(MemoryError::LowPriorityRejected { .. })),
                    "low-priority {class} rejected under step 1"
                );
            } else {
                assert!(
                    matches!(rejected, Err(MemoryError::Exhausted { .. })),
                    "non-reserved {class} must stop at the floor"
                );
            }
        }
        // Replication and network buffers (control plane) still reserve up to
        // the full maximum: never fully starved (S1E-003 step 5).
        held.push(governor.try_reserve(60, MemoryClass::Replication).unwrap());
        held.push(
            governor
                .try_reserve(40, MemoryClass::NetworkBuffers)
                .unwrap(),
        );
        assert_eq!(governor.total_used(), 1000);
        // But reserved classes cannot exceed the node maximum either.
        assert!(governor.try_reserve(1, MemoryClass::Replication).is_err());
        assert!(governor
            .try_reserve(1, MemoryClass::NetworkBuffers)
            .is_err());
        // Releasing foreground pressure re-opens the shared space.
        held.clear();
        assert_eq!(governor.total_used(), 0);
        assert!(governor
            .try_reserve(900, MemoryClass::QueryExecution)
            .is_ok());
    }

    #[test]
    fn escalation_levels_activate_in_exact_spec_order() {
        // Thresholds 0.70 / 0.80 / 0.90 / 0.95 on a 1000-byte governor (floor
        // zero, so the non-reserved cap does not interfere).
        let governor = governor(1000, 0);
        let mut held = Vec::new();
        let reserve = |bytes: u64, held: &mut Vec<Reservation>| {
            held.push(
                governor
                    .try_reserve(bytes, MemoryClass::QueryExecution)
                    .unwrap(),
            );
        };

        assert_eq!(governor.escalation(), EscalationLevel::None);
        reserve(600, &mut held); // 0.60
        assert_eq!(governor.escalation(), EscalationLevel::None);
        assert!(!governor.should_reject_low_priority_work());

        reserve(100, &mut held); // 0.70 — step 1
        assert_eq!(governor.escalation(), EscalationLevel::RejectLowPriority);
        assert!(governor.should_reject_low_priority_work());
        assert!(!governor.should_evict_caches());

        reserve(100, &mut held); // 0.80 — step 2
        assert_eq!(governor.escalation(), EscalationLevel::EvictCaches);
        assert!(governor.should_evict_caches());
        assert!(!governor.spill_trigger());

        reserve(100, &mut held); // 0.90 — step 3 (spill hook fires once)
        assert_eq!(governor.escalation(), EscalationLevel::SpillOperators);
        assert!(governor.spill_trigger());
        assert!(!governor.should_throttle_maintenance());
        assert_eq!(governor.stats().spill_triggers, 1);

        reserve(50, &mut held); // 0.95 — step 4
        assert_eq!(governor.escalation(), EscalationLevel::ThrottleMaintenance);
        assert!(governor.should_throttle_maintenance());
        assert_eq!(governor.stats().spill_triggers, 1);

        // The derived ordering matches the spec's textual order 1 → 4.
        assert!(EscalationLevel::RejectLowPriority < EscalationLevel::EvictCaches);
        assert!(EscalationLevel::EvictCaches < EscalationLevel::SpillOperators);
        assert!(EscalationLevel::SpillOperators < EscalationLevel::ThrottleMaintenance);
    }

    #[test]
    fn hysteresis_prevents_flapping() {
        let governor = governor(1000, 0);
        let mut r = governor
            .try_reserve(700, MemoryClass::QueryExecution)
            .unwrap();
        assert_eq!(governor.escalation(), EscalationLevel::RejectLowPriority);
        // Release to just inside the hysteresis band (0.66 > 0.70 - 0.05):
        // the level holds.
        r.resize(660).unwrap();
        assert_eq!(governor.escalation(), EscalationLevel::RejectLowPriority);
        // Below the band (0.64 < 0.65): de-escalates.
        r.resize(640).unwrap();
        assert_eq!(governor.escalation(), EscalationLevel::None);
        // Mid-level band: escalate to EvictCaches (0.80), release to 0.76
        // (> 0.75) — holds; release to 0.74 — drops to step 1 (0.74 >= 0.70).
        r.resize(800).unwrap();
        assert_eq!(governor.escalation(), EscalationLevel::EvictCaches);
        r.resize(760).unwrap();
        assert_eq!(governor.escalation(), EscalationLevel::EvictCaches);
        r.resize(740).unwrap();
        assert_eq!(governor.escalation(), EscalationLevel::RejectLowPriority);
    }

    #[test]
    fn spill_eligible_classes_are_the_query_pools() {
        for class in MemoryClass::ALL {
            assert_eq!(
                class.is_spill_eligible(),
                matches!(
                    class,
                    MemoryClass::QueryExecution | MemoryClass::ResultBuffering
                ),
                "spill-eligible: {class}"
            );
        }
    }

    #[test]
    fn spill_grants_require_the_trigger_and_an_eligible_class() {
        let governor = governor(1000, 0);
        let mut query = governor
            .try_reserve(100, MemoryClass::QueryExecution)
            .unwrap();
        // No pressure: the trigger is inactive, no grant is issued.
        assert!(!governor.spill_trigger());
        assert_eq!(governor.request_spill_grant(&mut query, 50), None);
        assert_eq!(query.bytes(), 100);

        // Drive pressure to step 3 (0.90) with an ineligible class's memory.
        let mut cache = governor.try_reserve(800, MemoryClass::PageCache).unwrap();
        assert!(governor.spill_trigger());
        // Ineligible classes are never granted, even under the trigger.
        assert_eq!(governor.request_spill_grant(&mut cache, 50), None);
        assert_eq!(cache.bytes(), 800);
        let mut replication = governor.try_reserve(90, MemoryClass::Replication).unwrap();
        assert_eq!(governor.request_spill_grant(&mut replication, 50), None);
        // The eligible query reservation is granted.
        let grant = governor.request_spill_grant(&mut query, 60).unwrap();
        assert_eq!(
            grant,
            SpillGrant {
                class: MemoryClass::QueryExecution,
                bytes: 60
            }
        );
        assert_eq!(grant.class(), MemoryClass::QueryExecution);
        assert_eq!(grant.bytes(), 60);
    }

    #[test]
    fn spill_grant_shrinks_the_reservation_and_clamps_to_held_bytes() {
        let governor = governor(1000, 0);
        let mut r = governor
            .try_reserve(900, MemoryClass::QueryExecution)
            .unwrap();
        assert!(governor.spill_trigger());
        assert_eq!(governor.total_used(), 900);

        let grant = governor.request_spill_grant(&mut r, 400).unwrap();
        assert_eq!(grant.bytes(), 400);
        assert_eq!(r.bytes(), 500);
        // The governor accounts the spilled memory as freed immediately — and
        // the relief drops the node back below the spill threshold.
        assert_eq!(governor.total_used(), 500);
        assert!(!governor.spill_trigger());
        assert_eq!(governor.request_spill_grant(&mut r, 100), None);

        // Fresh pressure re-arms the trigger; requests clamp to held bytes.
        let _pressure = governor
            .try_reserve(400, MemoryClass::QueryExecution)
            .unwrap();
        assert!(governor.spill_trigger());
        let grant = governor.request_spill_grant(&mut r, u64::MAX).unwrap();
        assert_eq!(grant.bytes(), 500);
        assert_eq!(r.bytes(), 0);
        assert_eq!(governor.total_used(), 400);
        // An empty reservation earns no grant, even under the trigger.
        let _more_pressure = governor
            .try_reserve(500, MemoryClass::QueryExecution)
            .unwrap();
        assert!(governor.spill_trigger());
        assert_eq!(governor.request_spill_grant(&mut r, 1), None);

        // Reading spilled data back re-reserves through the normal admission
        // path (and can be rejected under pressure, like any reservation).
        drop(_more_pressure);
        drop(_pressure);
        assert!(governor
            .try_reserve(500, MemoryClass::QueryExecution)
            .is_ok());
    }

    #[test]
    fn low_priority_work_is_rejected_first_under_pressure() {
        let governor = governor(1000, 100);
        let _pressure = governor
            .try_reserve(700, MemoryClass::QueryExecution)
            .unwrap();
        assert!(governor.should_reject_low_priority_work());
        // Step 1: new low-priority work rejected even though bytes remain.
        for class in [MemoryClass::Compaction, MemoryClass::Backup] {
            let rejected = governor.try_reserve(1, class);
            assert!(
                matches!(rejected, Err(MemoryError::LowPriorityRejected { .. })),
                "{class} rejected under step 1"
            );
        }
        assert_eq!(governor.stats().low_priority_rejected, 2);
        // Foreground work is still admitted within its limits.
        assert!(governor
            .try_reserve(100, MemoryClass::QueryExecution)
            .is_ok());
        drop(_pressure);
        // Below the hysteresis band, low-priority work is admitted again.
        assert!(governor.try_reserve(100, MemoryClass::Compaction).is_ok());
    }

    struct StubReclaimer {
        bytes: AtomicU64,
    }

    impl Reclaimable for StubReclaimer {
        fn evict_reclaimable(&self, budget: u64) -> u64 {
            let freed = self.bytes.load(Ordering::Relaxed).min(budget);
            self.bytes.fetch_sub(freed, Ordering::Relaxed);
            freed
        }

        fn reclaimable_bytes(&self) -> u64 {
            self.bytes.load(Ordering::Relaxed)
        }
    }

    #[test]
    fn evict_reclaimable_drives_registered_subsystems() {
        let governor = governor(1000, 100);
        let a = Arc::new(StubReclaimer {
            bytes: AtomicU64::new(60),
        });
        let b = Arc::new(StubReclaimer {
            bytes: AtomicU64::new(100),
        });
        governor.register_reclaimable(&a);
        governor.register_reclaimable(&b);
        assert_eq!(governor.reclaimable_bytes(), 160);

        // Step 2: a 100-byte relief frees 60 from the first, 40 from the second.
        assert_eq!(governor.evict_reclaimable(100), 100);
        assert_eq!(a.reclaimable_bytes(), 0);
        assert_eq!(b.reclaimable_bytes(), 60);
        // Less reclaimable than requested: frees what exists.
        assert_eq!(governor.evict_reclaimable(1000), 60);
        // A dropped subsystem is pruned, not driven.
        drop(a);
        drop(b);
        assert_eq!(governor.reclaimable_bytes(), 0);
        assert_eq!(governor.evict_reclaimable(10), 0);
        assert!(governor.inner.reclaimers.lock().is_empty());
    }

    #[test]
    fn config_validation() {
        assert!(matches!(
            MemoryGovernor::new(GovernorConfig::new(0)),
            Err(MemoryError::InvalidConfig(_))
        ));
        assert!(matches!(
            MemoryGovernor::new(GovernorConfig::new(100).with_reserved_floor(101)),
            Err(MemoryError::InvalidConfig(_))
        ));
        assert!(matches!(
            MemoryGovernor::new(
                GovernorConfig::new(100).with_class_budget(MemoryClass::Backup, 101)
            ),
            Err(MemoryError::InvalidConfig(_))
        ));
        // Not strictly increasing in S1E-003 order.
        let not_increasing = EscalationThresholds {
            reject_low_priority: 0.70,
            evict_caches: 0.70,
            spill_operators: 0.90,
            throttle_maintenance: 0.95,
            hysteresis: 0.05,
        };
        assert!(matches!(
            MemoryGovernor::new(GovernorConfig::new(100).with_thresholds(not_increasing)),
            Err(MemoryError::InvalidConfig(_))
        ));
        // Hysteresis band crossing below zero.
        let band_too_wide = EscalationThresholds {
            hysteresis: 0.70,
            ..EscalationThresholds::default()
        };
        assert!(matches!(
            MemoryGovernor::new(GovernorConfig::new(100).with_thresholds(band_too_wide)),
            Err(MemoryError::InvalidConfig(_))
        ));
        // Threshold out of the (0, 1] band.
        let out_of_band = EscalationThresholds {
            throttle_maintenance: 1.5,
            ..EscalationThresholds::default()
        };
        assert!(matches!(
            MemoryGovernor::new(GovernorConfig::new(100).with_thresholds(out_of_band)),
            Err(MemoryError::InvalidConfig(_))
        ));
        assert!(MemoryGovernor::new(GovernorConfig::new(100)).is_ok());
    }

    #[test]
    fn concurrent_reservations_never_exceed_limits_and_release_exactly() {
        let governor = governor(1 << 16, 1 << 12);
        let mut granted_total = 0u64;
        // Single-threaded: granted bytes never exceed max - floor for a
        // non-reserved class.
        let mut held = Vec::new();
        while let Ok(r) = governor.try_reserve(64, MemoryClass::QueryExecution) {
            granted_total += 64;
            held.push(r);
        }
        assert!(granted_total <= (1 << 16) - (1 << 12));
        held.clear();
        assert_eq!(governor.total_used(), 0);

        // Multi-threaded hammering: accounting returns exactly to zero, and a
        // final grant of the full non-reserved limit succeeds afterwards.
        let governor = Arc::new(governor);
        let mut threads = Vec::new();
        for _ in 0..4 {
            let governor = Arc::clone(&governor);
            threads.push(std::thread::spawn(move || {
                for _ in 0..1000 {
                    if let Ok(r) = governor.try_reserve(128, MemoryClass::QueryExecution) {
                        std::thread::yield_now();
                        drop(r);
                    }
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(governor.total_used(), 0);
        for class in MemoryClass::ALL {
            assert_eq!(governor.usage(class), 0, "{class}");
        }
        let full = governor
            .try_reserve(((1 << 16) - (1 << 12)) as u64, MemoryClass::QueryExecution)
            .unwrap();
        assert_eq!(governor.total_used(), (1 << 16) - (1 << 12));
        drop(full);
        assert_eq!(governor.total_used(), 0);
    }

    #[test]
    fn stats_snapshot_is_consistent() {
        let governor = governor(2048, 256);
        let _a = governor.try_reserve(512, MemoryClass::PageCache).unwrap();
        let _b = governor.try_reserve(128, MemoryClass::Replication).unwrap();
        let _ = governor.try_reserve(1 << 20, MemoryClass::Backup); // rejected
        let stats = governor.stats();
        assert_eq!(stats.max_bytes, 2048);
        assert_eq!(stats.reserved_floor_bytes, 256);
        assert_eq!(stats.total_used, 640);
        assert_eq!(stats.usage_for(MemoryClass::PageCache), 512);
        assert_eq!(stats.usage_for(MemoryClass::Replication), 128);
        assert!((stats.pressure - 640.0 / 2048.0).abs() < 1e-12);
        assert_eq!(stats.escalation, governor.escalation());
        assert_eq!(stats.reservations_granted, 2);
        assert_eq!(stats.reservations_rejected, 1);
    }

    #[test]
    fn config_serde_round_trip() {
        let config = GovernorConfig::new(1 << 20)
            .with_reserved_floor(1 << 16)
            .with_class_budget(MemoryClass::AiCandidates, 1 << 17);
        let json = serde_json::to_string(&config).unwrap();
        let back: GovernorConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, config);
        assert!(back.validate().is_ok());
    }
}
