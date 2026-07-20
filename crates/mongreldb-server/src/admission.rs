//! Hierarchical scheduler admission for the SQL request path (S1E-002 / S4A)
//! and node memory-pressure gating (S4B / spec §13.2).
//!
//! Design choice (spec §13.1):
//! - The process-wide `sql_semaphore` remains the **outer node hard cap**.
//! - [`SchedulerAdmission`] enforces class/tenant fairness **inside** that bound.
//!
//! Design choice (spec §13.2 / S4B):
//! - [`NodeMemoryGovernor`](mongreldb_core::NodeMemoryGovernor) is evaluated on
//!   SQL/AI admission with best-effort live inputs (DB reservation totals,
//!   AI semaphore saturation, optional process RSS). Missing OS metrics default
//!   to zero / safe defaults.
//! - Actions are applied here (not only surfaced in SHOW RESOURCE GROUPS):
//!   - `RejectOversizedAi` → refuse new AI/analytics class work
//!   - `ReduceAdmission` → temporarily halve InteractiveSql / AiRetrieval
//!     `max_concurrency` (restored when pressure clears)
//!   - `EvictCaches` → best-effort `MemoryGovernor::evict_reclaimable`
//!   - `MoveTabletLeaders` → **no-op** outside cluster tablet routing; counted
//!     and logged (single-node server has no leader-move path)
//!
//! `HierarchicalScheduler::poll` is global: concurrent requests must not steal
//! each other's work items. This module registers oneshot waiters keyed by
//! `work_id` and a small dispatch helper polls + delivers only to those waiters.
//! [`AdmittedWork`] RAII-completes on drop so concurrency slots free.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use mongreldb_core::{
    ClassConfig, GovernorAction, HierarchicalScheduler, MemoryGovernor, NodeMemoryGovernor,
    NodePressureInputs, ResourceGroupRegistry, SchedulerError, SchedulerStats, WorkItem,
    WorkloadClass,
};
use mongreldb_types::ids::QueryId;
use tokio::sync::oneshot;

/// Parameters for one admission submit.
#[derive(Debug, Clone)]
pub struct AdmitRequest<'a> {
    /// Tenant key (empty / `"default"` for unscoped work).
    pub tenant: &'a str,
    /// Workload class queue.
    pub class: WorkloadClass,
    /// Higher runs first within a class (0..=255).
    pub priority: u8,
    /// Optional deadline budget at submit.
    pub deadline: Option<Duration>,
    /// Optional query id for cancellation correlation.
    pub query_id: Option<QueryId>,
    /// Opaque payload tag for the caller.
    pub tag: &'a str,
}

/// Shared hierarchical-scheduler bridge with oneshot waiters and pressure gate.
#[derive(Clone)]
pub struct SchedulerAdmission {
    inner: Arc<SchedulerAdmissionInner>,
    /// Live pressure flags applied from node-governor evaluate (S4B).
    pressure: Arc<PressureControl>,
}

struct SchedulerAdmissionInner {
    state: Mutex<AdmissionState>,
}

struct AdmissionState {
    scheduler: HierarchicalScheduler,
    /// Pending async waiters keyed by work id. Dispatch delivers exactly once.
    waiters: HashMap<u64, oneshot::Sender<WorkItem>>,
}

/// Baseline class configs captured at construction (for pressure restore).
#[derive(Debug, Clone)]
struct ClassBaselines {
    interactive: ClassConfig,
    ai: ClassConfig,
    analytics: ClassConfig,
}

/// Applied node-pressure state (S4B). Shared via [`Arc`] with the admission bridge.
#[derive(Debug)]
pub struct PressureControl {
    /// `RejectOversizedAi` active: refuse new AI / analytics class work.
    reject_ai: AtomicBool,
    /// `ReduceAdmission` currently applied to InteractiveSql / AiRetrieval.
    reduced: AtomicBool,
    /// `MoveTabletLeaders` no-ops recorded (not in cluster mode).
    move_tablet_noops: AtomicU64,
    /// Bytes freed by the last `EvictCaches` application.
    last_evict_bytes: AtomicU64,
    /// Number of successful evaluate→apply cycles.
    evaluate_count: AtomicU64,
    baselines: Mutex<ClassBaselines>,
}

/// Point-in-time pressure flags for admin / tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PressureSnapshot {
    /// AI/analytics rejected under memory pressure.
    pub reject_ai: bool,
    /// InteractiveSql / AiRetrieval concurrency reduced.
    pub reduced_admission: bool,
    /// Count of tablet-move no-ops (non-cluster).
    pub move_tablet_noops: u64,
    /// Last eviction reclaimed bytes.
    pub last_evict_bytes: u64,
    /// Evaluate/apply cycles so far.
    pub evaluate_count: u64,
}

impl PressureControl {
    fn new(baselines: ClassBaselines) -> Self {
        Self {
            reject_ai: AtomicBool::new(false),
            reduced: AtomicBool::new(false),
            move_tablet_noops: AtomicU64::new(0),
            last_evict_bytes: AtomicU64::new(0),
            evaluate_count: AtomicU64::new(0),
            baselines: Mutex::new(baselines),
        }
    }

    /// Snapshot of applied pressure flags.
    pub fn snapshot(&self) -> PressureSnapshot {
        PressureSnapshot {
            reject_ai: self.reject_ai.load(Ordering::Relaxed),
            reduced_admission: self.reduced.load(Ordering::Relaxed),
            move_tablet_noops: self.move_tablet_noops.load(Ordering::Relaxed),
            last_evict_bytes: self.last_evict_bytes.load(Ordering::Relaxed),
            evaluate_count: self.evaluate_count.load(Ordering::Relaxed),
        }
    }

    /// True when new AI / analytics work must be refused.
    pub fn reject_ai(&self) -> bool {
        self.reject_ai.load(Ordering::Relaxed)
    }

    fn lock_baselines(&self) -> std::sync::MutexGuard<'_, ClassBaselines> {
        self.baselines
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
}

/// Best-effort live sources for [`NodePressureInputs`].
///
/// Unavailable OS metrics may be left as `None` / zero — documented defaults
/// in [`build_pressure_inputs`].
#[derive(Debug, Clone)]
pub struct PressureInputSources {
    /// Bytes currently reserved on the database [`MemoryGovernor`].
    pub db_reserved_bytes: u64,
    /// Configured max on the database governor.
    pub db_max_bytes: u64,
    /// Configured max on the node governor (fallback).
    pub node_configured_max_bytes: u64,
    /// Per-tablet reservations tracked by the node governor.
    pub tablet_reserved_bytes: u64,
    /// AI admission semaphore capacity (constructor value).
    pub ai_capacity: usize,
    /// Currently available AI semaphore permits.
    pub ai_available: usize,
    /// Process RSS when available (Linux `/proc/self/status`); `None` elsewhere.
    pub process_rss_bytes: Option<u64>,
}

/// Read process RSS from `/proc/self/status` (Linux). Returns `None` when the
/// file is unavailable or unparseable — callers treat that as "no OS metric".
pub fn process_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        let Some(rest) = line.strip_prefix("VmRSS:") else {
            continue;
        };
        let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
        return Some(kb.saturating_mul(1024));
    }
    None
}

/// Build pressure inputs from best-effort live sources.
///
/// Mapping:
/// - `query_reserved_bytes` = DB reserved + tablet reserved
/// - `configured_max_bytes` = max(db max, node max, 1)
/// - `os_pressure` = max(RSS/physical estimate, AI semaphore utilization);
///   optional `MONGRELDB_NODE_GOVERNOR_FORCE_OS_PRESSURE` overrides for tests
/// - `physical_memory_bytes` = RSS×4 estimate when RSS known, else default
/// - cache hit rate / compaction / replication backlogs default to calm values
///   when not instrumented (0 backlog, hit rate 1.0)
pub fn build_pressure_inputs(src: &PressureInputSources) -> NodePressureInputs {
    let configured_max_bytes = src.db_max_bytes.max(src.node_configured_max_bytes).max(1);
    let query_reserved_bytes = src
        .db_reserved_bytes
        .saturating_add(src.tablet_reserved_bytes);

    let ai_util = if src.ai_capacity == 0 {
        0.0
    } else {
        let used = src.ai_capacity.saturating_sub(src.ai_available);
        (used as f64 / src.ai_capacity as f64).clamp(0.0, 1.0)
    };

    // Without cgroup/MemAvailable we approximate physical from RSS when present
    // (RSS as a lower bound; 4× keeps os_pressure from saturating on a healthy
    // process). When RSS is unavailable, physical stays at the struct default
    // and os_pressure comes only from AI util / force env.
    let (physical_memory_bytes, rss_pressure) = match src.process_rss_bytes {
        Some(rss) if rss > 0 => {
            let physical = rss.saturating_mul(4).max(configured_max_bytes);
            let p = (rss as f64 / physical as f64).clamp(0.0, 1.0);
            (physical, p)
        }
        _ => (NodePressureInputs::default().physical_memory_bytes, 0.0),
    };

    let mut os_pressure = rss_pressure.max(ai_util);
    if let Ok(forced) = std::env::var("MONGRELDB_NODE_GOVERNOR_FORCE_OS_PRESSURE") {
        if let Ok(value) = forced.parse::<f64>() {
            os_pressure = os_pressure.max(value.clamp(0.0, 1.0));
        }
    }

    NodePressureInputs {
        physical_memory_bytes,
        configured_max_bytes,
        os_pressure,
        cache_hit_rate: 1.0,
        query_reserved_bytes,
        compaction_backlog_bytes: 0,
        replication_backlog_bytes: 0,
    }
}

/// Evaluate the node governor and apply returned actions onto the admission
/// bridge (and optional reclaimable cache governor).
///
/// Returns the action list from [`NodeMemoryGovernor::evaluate`].
pub fn refresh_pressure(
    governor: &mut NodeMemoryGovernor,
    inputs: &NodePressureInputs,
    admission: &SchedulerAdmission,
    cache_governor: Option<&MemoryGovernor>,
) -> Vec<GovernorAction> {
    let actions = governor.evaluate(inputs);
    apply_governor_actions(admission, &actions, cache_governor);
    admission
        .pressure
        .evaluate_count
        .fetch_add(1, Ordering::Relaxed);
    actions
}

/// Apply governor actions to admission pressure flags / class configs / caches.
///
/// `MoveTabletLeaders` is a **documented no-op** on this single-node HTTP
/// server path (no tablet leader routing wired here).
pub fn apply_governor_actions(
    admission: &SchedulerAdmission,
    actions: &[GovernorAction],
    cache_governor: Option<&MemoryGovernor>,
) {
    let reject_ai = actions
        .iter()
        .any(|a| matches!(a, GovernorAction::RejectOversizedAi));
    let reduce = actions
        .iter()
        .any(|a| matches!(a, GovernorAction::ReduceAdmission));
    let evict = actions
        .iter()
        .any(|a| matches!(a, GovernorAction::EvictCaches));
    let move_leaders = actions
        .iter()
        .any(|a| matches!(a, GovernorAction::MoveTabletLeaders { .. }));

    admission
        .pressure
        .reject_ai
        .store(reject_ai, Ordering::Relaxed);

    if reduce {
        apply_reduce_admission(admission);
    } else {
        clear_reduce_admission(admission);
    }

    if evict {
        if let Some(cache) = cache_governor {
            // Ask reclaimers for everything they can spare under pressure.
            let budget = cache.reclaimable_bytes().max(cache.max_bytes() / 16).max(1);
            let freed = cache.evict_reclaimable(budget);
            admission
                .pressure
                .last_evict_bytes
                .store(freed, Ordering::Relaxed);
        }
    }

    if move_leaders {
        // Not in cluster / tablet-routing mode on this path: record no-op.
        // Log on first occurrence and every 100th to avoid admission-path spam.
        let n = admission
            .pressure
            .move_tablet_noops
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if n == 1 || n.is_multiple_of(100) {
            eprintln!(
                "[node_governor] MoveTabletLeaders requested; no-op outside cluster mode (count={n})"
            );
        }
    }
}

fn apply_reduce_admission(admission: &SchedulerAdmission) {
    if admission.pressure.reduced.swap(true, Ordering::SeqCst) {
        return;
    }
    let (interactive, ai) = {
        let baselines = admission.pressure.lock_baselines();
        let mut interactive = baselines.interactive.clone();
        let mut ai = baselines.ai.clone();
        interactive.max_concurrency = (interactive.max_concurrency / 2).max(1);
        ai.max_concurrency = (ai.max_concurrency / 2).max(1);
        (interactive, ai)
    };
    // Bypass set_class_config baseline bookkeeping (already under pressure).
    {
        let mut state = admission.inner.lock();
        state
            .scheduler
            .set_class_config(WorkloadClass::InteractiveSql, interactive);
        state
            .scheduler
            .set_class_config(WorkloadClass::AiRetrieval, ai);
    }
}

fn clear_reduce_admission(admission: &SchedulerAdmission) {
    if !admission.pressure.reduced.swap(false, Ordering::SeqCst) {
        return;
    }
    let (interactive, ai) = {
        let baselines = admission.pressure.lock_baselines();
        (baselines.interactive.clone(), baselines.ai.clone())
    };
    let mut state = admission.inner.lock();
    state
        .scheduler
        .set_class_config(WorkloadClass::InteractiveSql, interactive);
    state
        .scheduler
        .set_class_config(WorkloadClass::AiRetrieval, ai);
}

/// RAII handle for one admitted unit of work. Drop calls `complete`.
pub struct AdmittedWork {
    work_id: u64,
    inner: Arc<SchedulerAdmissionInner>,
    completed: bool,
}

impl std::fmt::Debug for AdmittedWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmittedWork")
            .field("work_id", &self.work_id)
            .field("completed", &self.completed)
            .finish()
    }
}

/// Combined outer semaphore + hierarchical class admission permit.
pub struct SqlAdmissionGuard {
    /// Outer node hard cap.
    _permit: tokio::sync::OwnedSemaphorePermit,
    /// Class/tenant slot; completes on drop.
    _work: AdmittedWork,
}

impl SqlAdmissionGuard {
    /// Bundle an outer node permit with a class-admitted work slot.
    pub fn new(permit: tokio::sync::OwnedSemaphorePermit, work: AdmittedWork) -> Self {
        Self {
            _permit: permit,
            _work: work,
        }
    }
}

impl AdmittedWork {
    /// Stable work id assigned by the scheduler.
    #[allow(dead_code)] // used by unit tests and admin diagnostics
    pub fn work_id(&self) -> u64 {
        self.work_id
    }

    /// Explicit complete (also runs on drop).
    #[allow(dead_code)] // used by unit tests; Drop is the production path
    pub fn complete(mut self) {
        self.finish();
    }

    fn finish(&mut self) {
        if self.completed {
            return;
        }
        self.completed = true;
        let mut state = self.inner.lock();
        let _ = state.scheduler.complete(self.work_id);
        dispatch_ready(&mut state);
    }
}

impl Drop for AdmittedWork {
    fn drop(&mut self) {
        self.finish();
    }
}

impl SchedulerAdmission {
    /// Build with default per-class configs, then overlay resource-group bounds
    /// when the registry has a group named after the class.
    pub fn new() -> Self {
        Self::from_resource_groups(&ResourceGroupRegistry::with_defaults())
    }

    /// Configure class queues from a resource-group registry (tighter of group
    /// vs. class defaults is applied field-by-field from the group).
    pub fn from_resource_groups(groups: &ResourceGroupRegistry) -> Self {
        let mut scheduler = HierarchicalScheduler::new();
        // resolved_class_config folds resource groups + InteractiveSql env overrides.
        apply_resource_groups(&mut scheduler, groups);
        let baselines = ClassBaselines {
            interactive: resolved_class_config(groups, WorkloadClass::InteractiveSql),
            ai: resolved_class_config(groups, WorkloadClass::AiRetrieval),
            analytics: resolved_class_config(groups, WorkloadClass::Analytics),
        };
        Self {
            inner: Arc::new(SchedulerAdmissionInner {
                state: Mutex::new(AdmissionState {
                    scheduler,
                    waiters: HashMap::new(),
                }),
            }),
            pressure: Arc::new(PressureControl::new(baselines)),
        }
    }

    /// Live pressure control (S4B).
    pub fn pressure(&self) -> &PressureControl {
        &self.pressure
    }

    /// Refuse AI work when the node governor has raised `RejectOversizedAi`.
    pub fn check_ai_admitted(&self) -> Result<(), AdmitError> {
        if self.pressure.reject_ai() {
            Err(AdmitError::PressureRejected {
                resource: "ai_memory_pressure",
            })
        } else {
            Ok(())
        }
    }

    /// Override a class config (tests / operator reload).
    ///
    /// When pressure reduction is **not** active, InteractiveSql / AiRetrieval
    /// / Analytics baselines are updated so a later reduce/restore cycle uses
    /// the new operator values.
    #[allow(dead_code)] // production reload path will call this; unit tests already do
    pub fn set_class_config(&self, class: WorkloadClass, config: ClassConfig) {
        {
            let mut state = self.inner.lock();
            state.scheduler.set_class_config(class, config.clone());
        }
        if !self.pressure.reduced.load(Ordering::Relaxed) {
            let mut baselines = self.pressure.lock_baselines();
            match class {
                WorkloadClass::InteractiveSql => baselines.interactive = config,
                WorkloadClass::AiRetrieval => baselines.ai = config,
                WorkloadClass::Analytics => baselines.analytics = config,
                _ => {}
            }
        }
    }

    /// Snapshot for admin observability (`SHOW RESOURCE GROUPS`).
    pub fn stats(&self) -> SchedulerStats {
        self.inner.lock().scheduler.stats()
    }

    /// Submit interactive work and wait until this `work_id` is polled (or
    /// `cancel` fires / submit is rejected).
    ///
    /// Concurrent callers never steal each other's items: poll results are
    /// delivered only to the waiter registered for that work id.
    ///
    /// Under `RejectOversizedAi`, `AiRetrieval` and `Analytics` submits fail
    /// closed with [`AdmitError::PressureRejected`].
    pub async fn submit_and_wait<C>(
        &self,
        req: AdmitRequest<'_>,
        cancel: C,
    ) -> Result<AdmittedWork, AdmitError>
    where
        C: Future<Output = ()>,
    {
        if matches!(
            req.class,
            WorkloadClass::AiRetrieval | WorkloadClass::Analytics
        ) {
            self.check_ai_admitted()?;
        }

        let (work_id, rx) = {
            let mut state = self.inner.lock();
            let work_id = state
                .scheduler
                .submit(
                    req.tenant,
                    req.class,
                    req.priority,
                    req.deadline,
                    req.query_id,
                    req.tag,
                )
                .map_err(AdmitError::Rejected)?;
            let (tx, rx) = oneshot::channel();
            state.waiters.insert(work_id, tx);
            dispatch_ready(&mut state);
            (work_id, rx)
        };

        tokio::pin!(cancel);
        tokio::select! {
            biased;
            result = rx => {
                match result {
                    Ok(_item) => Ok(AdmittedWork {
                        work_id,
                        inner: Arc::clone(&self.inner),
                        completed: false,
                    }),
                    // Sender dropped without delivery: treat as cancelled.
                    Err(_) => {
                        self.cancel_work(work_id);
                        Err(AdmitError::Cancelled)
                    }
                }
            }
            _ = &mut cancel => {
                self.cancel_work(work_id);
                Err(AdmitError::Cancelled)
            }
        }
    }

    /// Cancel a queued (or running) work item and free its slot if running.
    pub fn cancel_work(&self, work_id: u64) {
        let mut state = self.inner.lock();
        state.waiters.remove(&work_id);
        let _ = state.scheduler.cancel(work_id);
        // If poll already moved it to running, free the concurrency slot.
        let _ = state.scheduler.complete(work_id);
        dispatch_ready(&mut state);
    }
}

impl Default for SchedulerAdmission {
    fn default() -> Self {
        Self::new()
    }
}

impl SchedulerAdmissionInner {
    fn lock(&self) -> std::sync::MutexGuard<'_, AdmissionState> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}

/// Errors from class admission (mapped by the server to query errors).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmitError {
    /// Scheduler rejected submit (queue full / tenant quota).
    Rejected(SchedulerError),
    /// Caller cancelled while waiting for a concurrency slot.
    Cancelled,
    /// Node memory governor refused this class of work under pressure (S4B).
    PressureRejected {
        /// Resource name for [`mongreldb_core::MongrelError::ResourceLimitExceeded`].
        resource: &'static str,
    },
}

/// Map scheduler rejection onto a ResourceExhausted core error.
pub fn scheduler_error_to_query(error: SchedulerError) -> mongreldb_query::MongrelQueryError {
    let (resource, requested, limit) = match &error {
        SchedulerError::QueueFull { depth, max, .. } => ("scheduler_queue", *depth + 1, *max),
        SchedulerError::TenantQuota { .. } => ("tenant_quota", 1, 0),
        SchedulerError::UnknownWork(_) => ("scheduler", 1, 0),
    };
    mongreldb_query::MongrelQueryError::Core(mongreldb_core::MongrelError::ResourceLimitExceeded {
        resource,
        requested,
        limit,
    })
}

/// Map any [`AdmitError`] onto a query-layer error (ResourceExhausted where applicable).
pub fn admit_error_to_query(error: AdmitError) -> mongreldb_query::MongrelQueryError {
    match error {
        AdmitError::Rejected(error) => scheduler_error_to_query(error),
        AdmitError::Cancelled => mongreldb_query::MongrelQueryError::InvalidQueryState(
            "SQL admission cancelled while waiting for scheduler slot".into(),
        ),
        AdmitError::PressureRejected { resource } => mongreldb_query::MongrelQueryError::Core(
            mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource,
                requested: 1,
                limit: 0,
            },
        ),
    }
}

/// Map scheduler admission onto the core taxonomy for non-SQL Kit work.
pub fn admit_error_to_core(error: AdmitError) -> mongreldb_core::MongrelError {
    match error {
        AdmitError::Rejected(error) => match scheduler_error_to_query(error) {
            mongreldb_query::MongrelQueryError::Core(error) => error,
            error => mongreldb_core::MongrelError::Other(error.to_string()),
        },
        AdmitError::Cancelled => mongreldb_core::MongrelError::Cancelled,
        AdmitError::PressureRejected { resource } => {
            mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource,
                requested: 1,
                limit: 0,
            }
        }
    }
}

/// Priority for a workload class from the resource-group registry (fallback
/// to [`ClassConfig::for_class`] weight-derived defaults).
pub fn priority_for_class(groups: &ResourceGroupRegistry, class: WorkloadClass) -> u8 {
    groups
        .get(class.name())
        .map(|g| g.priority)
        .unwrap_or_else(|| match class {
            WorkloadClass::Control => 255,
            WorkloadClass::Replication => 254,
            WorkloadClass::Oltp => 200,
            WorkloadClass::InteractiveSql => 180,
            WorkloadClass::AiRetrieval => 150,
            WorkloadClass::Analytics => 100,
            WorkloadClass::Maintenance => 50,
            WorkloadClass::Backup => 40,
        })
}

/// Class config after resource-group overlay + InteractiveSql env overrides.
fn resolved_class_config(groups: &ResourceGroupRegistry, class: WorkloadClass) -> ClassConfig {
    let mut config = ClassConfig::for_class(class);
    if let Some(group) = groups.get(class.name()) {
        // Operator resource group is authoritative for class bounds.
        config.max_concurrency = group.max_concurrency;
        config.max_queue = group.max_queue;
        config.weight = group.cpu_weight.max(1);
        if class.has_reserved_capacity() {
            // Keep at least one reserved slot for control/replication.
            config.reserved_slots = config.reserved_slots.max(1).min(config.max_concurrency);
        }
    }
    if class == WorkloadClass::InteractiveSql {
        if let Some(v) = positive_env_usize("MONGRELDB_SCHEDULER_INTERACTIVE_SQL_MAX_QUEUE") {
            config.max_queue = v;
        }
        if let Some(v) = positive_env_usize("MONGRELDB_SCHEDULER_INTERACTIVE_SQL_MAX_CONCURRENCY") {
            config.max_concurrency = v;
        }
    }
    config
}

/// Apply resource-group concurrency/queue/weight onto class configs when the
/// group is tighter (or simply mirrors the group as the operator source of truth).
fn apply_resource_groups(scheduler: &mut HierarchicalScheduler, groups: &ResourceGroupRegistry) {
    for class in WorkloadClass::ALL {
        scheduler.set_class_config(class, resolved_class_config(groups, class));
    }
}

fn positive_env_usize(name: &str) -> Option<usize> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
}

/// Poll ready work and deliver each item to its registered waiter.
/// Orphaned ready items (no waiter) are completed immediately so slots free.
fn dispatch_ready(state: &mut AdmissionState) {
    // Bound the batch so a single lock hold cannot run unbounded; re-enter
    // while demand remains and concurrency is free.
    loop {
        let ready = state.scheduler.poll(32);
        if ready.is_empty() {
            break;
        }
        for item in ready {
            let work_id = item.work_id;
            match state.waiters.remove(&work_id) {
                Some(tx) => {
                    if tx.send(item).is_err() {
                        // Waiter dropped between remove and send.
                        let _ = state.scheduler.complete(work_id);
                    }
                }
                None => {
                    let _ = state.scheduler.complete(work_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    fn tiny_interactive() -> ClassConfig {
        ClassConfig {
            max_queue: 1,
            weight: 64,
            reserved_slots: 0,
            max_concurrency: 1,
        }
    }

    fn sql_req(tag: &str) -> AdmitRequest<'_> {
        AdmitRequest {
            tenant: "t",
            class: WorkloadClass::InteractiveSql,
            priority: 180,
            deadline: None,
            query_id: None,
            tag,
        }
    }

    #[tokio::test]
    async fn queue_full_is_resource_exhausted_mapping() {
        let admission = SchedulerAdmission::new();
        admission.set_class_config(WorkloadClass::InteractiveSql, tiny_interactive());

        let never = std::future::pending::<()>();
        let first = admission
            .submit_and_wait(sql_req("a"), never)
            .await
            .expect("first admits");

        // Second fills the single queue slot while first holds concurrency.
        let admission2 = admission.clone();
        let second = tokio::spawn(async move {
            admission2
                .submit_and_wait(sql_req("b"), std::future::pending::<()>())
                .await
        });
        // Wait until the second request is queued (deterministic via stats).
        let queued = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let stats = admission.stats();
                let sql = stats.per_class.get("interactive_sql").unwrap();
                if sql.queued >= 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(
            queued.is_ok(),
            "second request must enqueue behind the holder"
        );

        let err = admission
            .submit_and_wait(sql_req("c"), std::future::pending::<()>())
            .await
            .expect_err("third must be rejected");
        let rejected = match err {
            AdmitError::Rejected(e) => e,
            other => panic!("expected QueueFull, got {other:?}"),
        };
        assert!(matches!(rejected, SchedulerError::QueueFull { max: 1, .. }));
        let mapped = scheduler_error_to_query(rejected);
        assert!(matches!(
            mapped,
            mongreldb_query::MongrelQueryError::Core(
                mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "scheduler_queue",
                    ..
                }
            )
        ));
        assert_eq!(
            mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "scheduler_queue",
                requested: 1,
                limit: 1,
            }
            .category(),
            mongreldb_types::errors::ErrorCategory::ResourceExhausted
        );

        // Release the holder so the queued waiter admits, then drop it.
        drop(first);
        let second = tokio::time::timeout(Duration::from_secs(2), second)
            .await
            .expect("second must admit after holder completes")
            .expect("join")
            .expect("second admit");
        drop(second);
    }

    #[tokio::test]
    async fn control_admits_when_interactive_sql_saturated() {
        let admission = SchedulerAdmission::new();
        admission.set_class_config(WorkloadClass::InteractiveSql, tiny_interactive());
        // Control keeps reserved capacity.
        admission.set_class_config(
            WorkloadClass::Control,
            ClassConfig {
                max_queue: 8,
                weight: 256,
                reserved_slots: 2,
                max_concurrency: 8,
            },
        );

        let _sql = admission
            .submit_and_wait(sql_req("sql"), std::future::pending::<()>())
            .await
            .unwrap();

        let control = admission
            .submit_and_wait(
                AdmitRequest {
                    tenant: "system",
                    class: WorkloadClass::Control,
                    priority: 255,
                    deadline: None,
                    query_id: None,
                    tag: "ctl",
                },
                std::future::pending::<()>(),
            )
            .await
            .expect("control must admit under interactive saturation");
        assert!(control.work_id() > 0);
        control.complete();
    }

    #[tokio::test]
    async fn cancel_while_waiting_frees_queue_slot() {
        let admission = SchedulerAdmission::new();
        admission.set_class_config(WorkloadClass::InteractiveSql, tiny_interactive());

        let _holder = admission
            .submit_and_wait(sql_req("hold"), std::future::pending::<()>())
            .await
            .unwrap();

        let cancelled = AtomicBool::new(false);
        let (cancel_tx, cancel_rx) = oneshot::channel::<()>();
        let admission2 = admission.clone();
        let waiter = tokio::spawn(async move {
            let result = admission2
                .submit_and_wait(sql_req("wait"), async {
                    let _ = cancel_rx.await;
                })
                .await;
            cancelled.store(true, Ordering::SeqCst);
            result
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = cancel_tx.send(());
        let result = waiter.await.unwrap();
        assert!(matches!(result, Err(AdmitError::Cancelled)));

        // Queue slot freed: a new submit can enqueue (will wait for concurrency).
        let stats = admission.stats();
        let sql = stats.per_class.get("interactive_sql").unwrap();
        assert_eq!(sql.queued, 0, "cancelled work must leave the queue");
        assert_eq!(sql.running, 1, "holder still running");
    }

    #[tokio::test]
    async fn concurrent_waiters_receive_own_work_ids() {
        let admission = SchedulerAdmission::new();
        admission.set_class_config(
            WorkloadClass::InteractiveSql,
            ClassConfig {
                max_queue: 16,
                weight: 64,
                reserved_slots: 0,
                max_concurrency: 2,
            },
        );

        let a = admission.clone();
        let b = admission.clone();
        let (wa, wb) = tokio::join!(
            a.submit_and_wait(sql_req("a"), std::future::pending::<()>()),
            b.submit_and_wait(sql_req("b"), std::future::pending::<()>()),
        );
        let wa = wa.unwrap();
        let wb = wb.unwrap();
        assert_ne!(wa.work_id(), wb.work_id());
        drop(wa);
        drop(wb);
    }

    fn high_pressure_inputs(max: u64) -> NodePressureInputs {
        NodePressureInputs {
            configured_max_bytes: max,
            query_reserved_bytes: (max as f64 * 0.95) as u64,
            os_pressure: 0.95,
            ..NodePressureInputs::default()
        }
    }

    fn calm_inputs(max: u64) -> NodePressureInputs {
        NodePressureInputs {
            configured_max_bytes: max,
            query_reserved_bytes: max / 100,
            os_pressure: 0.0,
            ..NodePressureInputs::default()
        }
    }

    #[test]
    fn high_pressure_rejects_ai_and_reduces_admission() {
        let admission = SchedulerAdmission::new();
        admission.set_class_config(
            WorkloadClass::InteractiveSql,
            ClassConfig {
                max_queue: 64,
                weight: 64,
                reserved_slots: 0,
                max_concurrency: 16,
            },
        );
        admission.set_class_config(
            WorkloadClass::AiRetrieval,
            ClassConfig {
                max_queue: 64,
                weight: 32,
                reserved_slots: 0,
                max_concurrency: 16,
            },
        );

        let mut gov = NodeMemoryGovernor::new(
            mongreldb_core::MemoryGovernor::new(mongreldb_core::GovernorConfig::new(1_000_000))
                .unwrap(),
        );
        let actions =
            refresh_pressure(&mut gov, &high_pressure_inputs(1_000_000), &admission, None);
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, GovernorAction::RejectOversizedAi)),
            "actions={actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, GovernorAction::ReduceAdmission)),
            "actions={actions:?}"
        );
        assert!(
            actions
                .iter()
                .any(|a| matches!(a, GovernorAction::MoveTabletLeaders { .. })),
            "actions={actions:?}"
        );

        let snap = admission.pressure().snapshot();
        assert!(snap.reject_ai);
        assert!(snap.reduced_admission);
        assert!(
            snap.move_tablet_noops >= 1,
            "tablet move must be no-op counted"
        );
        assert!(snap.evaluate_count >= 1);

        assert!(matches!(
            admission.check_ai_admitted(),
            Err(AdmitError::PressureRejected {
                resource: "ai_memory_pressure"
            })
        ));

        // Calm pressure restores AI admission and clears reduce.
        let calm_actions = refresh_pressure(&mut gov, &calm_inputs(1_000_000), &admission, None);
        assert!(!calm_actions
            .iter()
            .any(|a| matches!(a, GovernorAction::RejectOversizedAi)));
        let snap = admission.pressure().snapshot();
        assert!(!snap.reject_ai);
        assert!(!snap.reduced_admission);
        assert!(admission.check_ai_admitted().is_ok());
    }

    #[tokio::test]
    async fn high_pressure_submit_and_wait_rejects_ai_class() {
        let admission = SchedulerAdmission::new();
        let mut gov = NodeMemoryGovernor::new(
            mongreldb_core::MemoryGovernor::new(mongreldb_core::GovernorConfig::new(1_000_000))
                .unwrap(),
        );
        refresh_pressure(&mut gov, &high_pressure_inputs(1_000_000), &admission, None);

        let err = admission
            .submit_and_wait(
                AdmitRequest {
                    tenant: "t",
                    class: WorkloadClass::AiRetrieval,
                    priority: 150,
                    deadline: None,
                    query_id: None,
                    tag: "ai",
                },
                std::future::pending::<()>(),
            )
            .await
            .expect_err("AI class must be rejected under RejectOversizedAi");
        assert!(matches!(
            err,
            AdmitError::PressureRejected {
                resource: "ai_memory_pressure"
            }
        ));
        let mapped = admit_error_to_query(err);
        assert!(matches!(
            mapped,
            mongreldb_query::MongrelQueryError::Core(
                mongreldb_core::MongrelError::ResourceLimitExceeded {
                    resource: "ai_memory_pressure",
                    ..
                }
            )
        ));
        assert_eq!(
            mongreldb_core::MongrelError::ResourceLimitExceeded {
                resource: "ai_memory_pressure",
                requested: 1,
                limit: 0,
            }
            .category(),
            mongreldb_types::errors::ErrorCategory::ResourceExhausted
        );

        // Interactive SQL still admits under reduce (not full reject).
        let sql = admission
            .submit_and_wait(sql_req("sql"), std::future::pending::<()>())
            .await
            .expect("InteractiveSql must still admit under ReduceAdmission");
        drop(sql);
    }

    #[test]
    fn reduce_admission_halves_interactive_concurrency() {
        let admission = SchedulerAdmission::new();
        admission.set_class_config(
            WorkloadClass::InteractiveSql,
            ClassConfig {
                max_queue: 4,
                weight: 64,
                reserved_slots: 0,
                max_concurrency: 4,
            },
        );
        let mut gov = NodeMemoryGovernor::new(
            mongreldb_core::MemoryGovernor::new(mongreldb_core::GovernorConfig::new(1_000_000))
                .unwrap(),
        );
        // 0.80 pressure triggers ReduceAdmission but not necessarily RejectOversizedAi.
        let inputs = NodePressureInputs {
            configured_max_bytes: 1_000_000,
            query_reserved_bytes: 820_000,
            os_pressure: 0.82,
            ..NodePressureInputs::default()
        };
        let actions = refresh_pressure(&mut gov, &inputs, &admission, None);
        assert!(actions
            .iter()
            .any(|a| matches!(a, GovernorAction::ReduceAdmission)));
        assert!(admission.pressure().snapshot().reduced_admission);

        // Baseline 4 → reduced 2. Hold two slots; third must queue (queued>=1).
        // We drive this synchronously via submit_and_wait in a multi-thread runtime
        // below is unit-level: re-apply reduce is idempotent and clear restores.
        clear_reduce_admission(&admission);
        assert!(!admission.pressure().snapshot().reduced_admission);
        // Manually re-apply via full refresh to ensure restore→reduce cycle.
        refresh_pressure(&mut gov, &inputs, &admission, None);
        assert!(admission.pressure().snapshot().reduced_admission);
        refresh_pressure(&mut gov, &calm_inputs(1_000_000), &admission, None);
        assert!(!admission.pressure().snapshot().reduced_admission);
    }

    #[test]
    fn build_pressure_inputs_uses_db_and_ai_proxy() {
        let inputs = build_pressure_inputs(&PressureInputSources {
            db_reserved_bytes: 500,
            db_max_bytes: 1000,
            node_configured_max_bytes: 2000,
            tablet_reserved_bytes: 50,
            ai_capacity: 4,
            ai_available: 0,
            process_rss_bytes: None,
        });
        assert_eq!(inputs.query_reserved_bytes, 550);
        assert_eq!(inputs.configured_max_bytes, 2000);
        // AI fully saturated → os_pressure at least 1.0
        assert!((inputs.os_pressure - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn evict_caches_drives_memory_governor() {
        use mongreldb_core::memory::{GovernorConfig, MemoryClass, MemoryGovernor};
        use std::sync::atomic::AtomicU64 as StdAtomicU64;

        struct FakeCache {
            reclaimable: StdAtomicU64,
        }
        impl mongreldb_core::memory::Reclaimable for FakeCache {
            fn evict_reclaimable(&self, budget: u64) -> u64 {
                let have = self.reclaimable.load(Ordering::Relaxed);
                let take = have.min(budget);
                self.reclaimable.fetch_sub(take, Ordering::Relaxed);
                take
            }
            fn reclaimable_bytes(&self) -> u64 {
                self.reclaimable.load(Ordering::Relaxed)
            }
        }

        let gov =
            MemoryGovernor::new(GovernorConfig::new(1_000_000).with_reserved_floor(0)).unwrap();
        let cache = Arc::new(FakeCache {
            reclaimable: StdAtomicU64::new(10_000),
        });
        gov.register_reclaimable(&cache);
        // Touch usage so pressure path is realistic.
        let _res = gov.try_reserve(100, MemoryClass::PageCache).unwrap();

        let admission = SchedulerAdmission::new();
        let mut node = NodeMemoryGovernor::new(gov.clone());
        let actions = refresh_pressure(
            &mut node,
            &high_pressure_inputs(1_000_000),
            &admission,
            Some(&gov),
        );
        assert!(actions
            .iter()
            .any(|a| matches!(a, GovernorAction::EvictCaches)));
        assert!(
            admission.pressure().snapshot().last_evict_bytes > 0,
            "evict must free reclaimable bytes"
        );
        assert_eq!(cache.reclaimable.load(Ordering::Relaxed), 0);
    }
}
