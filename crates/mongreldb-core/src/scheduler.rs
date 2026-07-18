//! Hierarchical scheduler (spec section 13.1, Stage 4A).
//!
//! Queues: control, replication, OLTP, interactive SQL, AI retrieval,
//! analytics, maintenance, backup. Weighted fair scheduling with per-tenant
//! quotas; control and replication have reserved capacity and are never
//! fully starved. Deadline and priority propagate to work items (tablet
//! fragments inherit them at the gateway).
//!
//! Pure in-process admission: no threads are spawned here. Callers
//! `submit` work and `poll` ready items; a cancelled item is dropped from
//! the queue before dispatch.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap, HashMap};
use std::time::Duration;

use mongreldb_types::ids::QueryId;

use crate::resource::WorkloadClass;

/// Errors of the hierarchical scheduler.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchedulerError {
    /// Tenant quota exhausted for the class.
    #[error("tenant {tenant:?} quota exhausted for class {class}")]
    TenantQuota {
        /// Tenant key.
        tenant: String,
        /// Workload class.
        class: String,
    },
    /// Class queue is full.
    #[error("queue for class {class} is full ({depth}/{max})")]
    QueueFull {
        /// Workload class.
        class: String,
        /// Current depth.
        depth: usize,
        /// Configured max.
        max: usize,
    },
    /// Unknown work id.
    #[error("unknown work id {0}")]
    UnknownWork(u64),
}

/// Per-class queue configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassConfig {
    /// Maximum queued items.
    pub max_queue: usize,
    /// Scheduling weight (relative share; must be nonzero).
    pub weight: u32,
    /// Reserved concurrency slots (control/replication).
    pub reserved_slots: usize,
    /// Maximum concurrent running items for this class.
    pub max_concurrency: usize,
}

impl ClassConfig {
    /// Default config for a workload class (mirrors resource-group defaults).
    pub fn for_class(class: WorkloadClass) -> Self {
        match class {
            WorkloadClass::Control => Self {
                max_queue: 64,
                weight: 256,
                reserved_slots: 2,
                max_concurrency: 8,
            },
            WorkloadClass::Replication => Self {
                max_queue: 64,
                weight: 256,
                reserved_slots: 2,
                max_concurrency: 8,
            },
            WorkloadClass::Oltp => Self {
                max_queue: 256,
                weight: 128,
                reserved_slots: 0,
                max_concurrency: 64,
            },
            WorkloadClass::InteractiveSql => Self {
                max_queue: 64,
                weight: 64,
                reserved_slots: 0,
                max_concurrency: 16,
            },
            WorkloadClass::AiRetrieval => Self {
                max_queue: 64,
                weight: 32,
                reserved_slots: 0,
                max_concurrency: 16,
            },
            WorkloadClass::Analytics => Self {
                max_queue: 32,
                weight: 16,
                reserved_slots: 0,
                max_concurrency: 8,
            },
            WorkloadClass::Maintenance => Self {
                max_queue: 32,
                weight: 8,
                reserved_slots: 0,
                max_concurrency: 4,
            },
            WorkloadClass::Backup => Self {
                max_queue: 16,
                weight: 8,
                reserved_slots: 0,
                max_concurrency: 2,
            },
        }
    }
}

/// Per-tenant quota across classes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantQuota {
    /// Maximum concurrently running items for this tenant (all classes).
    pub max_running: usize,
    /// Maximum queued items for this tenant.
    pub max_queued: usize,
    /// Optional per-class caps (class name → max running).
    pub per_class_running: BTreeMap<String, usize>,
}

impl Default for TenantQuota {
    fn default() -> Self {
        Self {
            max_running: 32,
            max_queued: 128,
            per_class_running: BTreeMap::new(),
        }
    }
}

/// One unit of schedulable work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkItem {
    /// Stable work id assigned by the scheduler.
    pub work_id: u64,
    /// Optional query id for cancellation fan-out.
    pub query_id: Option<QueryId>,
    /// Tenant key (empty string = default).
    pub tenant: String,
    /// Workload class.
    pub class: WorkloadClass,
    /// Higher runs first within a class (0..=255).
    pub priority: u8,
    /// Deadline budget remaining at submit (propagated to fragments).
    pub deadline: Option<Duration>,
    /// Opaque payload tag for the caller.
    pub tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HeapEntry {
    priority: u8,
    /// Lower seq = older = fairer among equal priority.
    seq: u64,
    work_id: u64,
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is max-heap: higher priority first, then lower seq.
        self.priority
            .cmp(&other.priority)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

#[derive(Debug, Default)]
struct ClassQueue {
    heap: BinaryHeap<HeapEntry>,
    running: usize,
}

#[derive(Debug, Default)]
struct TenantState {
    running: usize,
    queued: usize,
    per_class_running: BTreeMap<String, usize>,
}

/// Per-class stats snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct ClassStats {
    /// Queued items.
    pub queued: usize,
    /// Running items.
    pub running: usize,
}

/// Scheduler observability snapshot.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SchedulerStats {
    /// Per-class stats keyed by class name.
    pub per_class: BTreeMap<String, ClassStats>,
    /// Number of tenants with state.
    pub tenants: usize,
}

/// Hierarchical weighted-fair scheduler (spec §13.1).
#[derive(Debug)]
pub struct HierarchicalScheduler {
    configs: BTreeMap<WorkloadClass, ClassConfig>,
    queues: BTreeMap<WorkloadClass, ClassQueue>,
    items: HashMap<u64, WorkItem>,
    /// Virtual finish times for weighted fair sharing (class → vtime).
    vtime: BTreeMap<WorkloadClass, u64>,
    tenants: HashMap<String, TenantState>,
    quotas: HashMap<String, TenantQuota>,
    default_quota: TenantQuota,
    next_id: u64,
    next_seq: u64,
    cancelled: HashMap<u64, ()>,
    /// Running work metadata (work_id → tenant, class).
    running_meta: HashMap<u64, (String, WorkloadClass)>,
}

impl Default for HierarchicalScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl HierarchicalScheduler {
    /// Build with default per-class configs.
    pub fn new() -> Self {
        let mut configs = BTreeMap::new();
        let mut queues = BTreeMap::new();
        let mut vtime = BTreeMap::new();
        for class in WorkloadClass::ALL {
            configs.insert(class, ClassConfig::for_class(class));
            queues.insert(class, ClassQueue::default());
            vtime.insert(class, 0);
        }
        Self {
            configs,
            queues,
            items: HashMap::new(),
            vtime,
            tenants: HashMap::new(),
            quotas: HashMap::new(),
            default_quota: TenantQuota::default(),
            next_id: 1,
            next_seq: 1,
            cancelled: HashMap::new(),
            running_meta: HashMap::new(),
        }
    }

    /// Override a class config.
    pub fn set_class_config(&mut self, class: WorkloadClass, config: ClassConfig) {
        self.configs.insert(class, config);
    }

    /// Set or replace a tenant quota.
    pub fn set_tenant_quota(&mut self, tenant: impl Into<String>, quota: TenantQuota) {
        self.quotas.insert(tenant.into(), quota);
    }

    /// Submit work. Returns the assigned work id.
    pub fn submit(
        &mut self,
        tenant: impl Into<String>,
        class: WorkloadClass,
        priority: u8,
        deadline: Option<Duration>,
        query_id: Option<QueryId>,
        tag: impl Into<String>,
    ) -> Result<u64, SchedulerError> {
        let tenant = tenant.into();
        let config = self.configs.get(&class).expect("all classes configured");
        let queue = self.queues.get(&class).expect("all classes queued");
        if queue.heap.len() >= config.max_queue {
            return Err(SchedulerError::QueueFull {
                class: class.name().into(),
                depth: queue.heap.len(),
                max: config.max_queue,
            });
        }

        let quota = self
            .quotas
            .get(&tenant)
            .cloned()
            .unwrap_or_else(|| self.default_quota.clone());
        let tenant_state = self.tenants.entry(tenant.clone()).or_default();
        if tenant_state.queued >= quota.max_queued {
            return Err(SchedulerError::TenantQuota {
                tenant: tenant.clone(),
                class: class.name().into(),
            });
        }

        let work_id = self.next_id;
        self.next_id += 1;
        let seq = self.next_seq;
        self.next_seq += 1;

        let item = WorkItem {
            work_id,
            query_id,
            tenant: tenant.clone(),
            class,
            priority,
            deadline,
            tag: tag.into(),
        };
        self.items.insert(work_id, item);
        self.queues
            .get_mut(&class)
            .expect("queue")
            .heap
            .push(HeapEntry {
                priority,
                seq,
                work_id,
            });
        self.tenants.entry(tenant).or_default().queued += 1;
        Ok(work_id)
    }

    /// Cancel queued work (or mark so a concurrent poll drops it).
    pub fn cancel(&mut self, work_id: u64) -> Result<(), SchedulerError> {
        if let Some(item) = self.items.remove(&work_id) {
            if let Some(t) = self.tenants.get_mut(&item.tenant) {
                t.queued = t.queued.saturating_sub(1);
            }
            self.cancelled.insert(work_id, ());
            return Ok(());
        }
        if self.running_meta.contains_key(&work_id) {
            self.cancelled.insert(work_id, ());
            return Ok(());
        }
        Err(SchedulerError::UnknownWork(work_id))
    }

    /// Poll up to `limit` ready work items under fairness + reserved rules.
    pub fn poll(&mut self, limit: usize) -> Vec<WorkItem> {
        let mut ready = Vec::new();
        for _ in 0..limit {
            let Some(class) = self.pick_class() else {
                break;
            };
            let Some(item) = self.pop_class(class) else {
                // Class had demand but tenant caps blocked; try others once.
                continue;
            };
            if self.cancelled.remove(&item.work_id).is_some() {
                continue;
            }
            if let Some(t) = self.tenants.get_mut(&item.tenant) {
                t.queued = t.queued.saturating_sub(1);
                t.running += 1;
                *t.per_class_running
                    .entry(item.class.name().into())
                    .or_default() += 1;
            }
            if let Some(q) = self.queues.get_mut(&item.class) {
                q.running += 1;
            }
            let weight = self
                .configs
                .get(&item.class)
                .map(|c| c.weight.max(1))
                .unwrap_or(1);
            let vt = self.vtime.entry(item.class).or_default();
            *vt = vt.saturating_add(1_000u64 / u64::from(weight));
            ready.push(item);
        }
        ready
    }

    /// Mark work finished (frees concurrency).
    pub fn complete(&mut self, work_id: u64) -> Result<(), SchedulerError> {
        let Some((tenant, class)) = self.running_meta.remove(&work_id) else {
            return Err(SchedulerError::UnknownWork(work_id));
        };
        self.cancelled.remove(&work_id);
        if let Some(q) = self.queues.get_mut(&class) {
            q.running = q.running.saturating_sub(1);
        }
        if let Some(t) = self.tenants.get_mut(&tenant) {
            t.running = t.running.saturating_sub(1);
            if let Some(c) = t.per_class_running.get_mut(class.name()) {
                *c = c.saturating_sub(1);
            }
        }
        Ok(())
    }

    /// Snapshot for tests/observability.
    pub fn stats(&self) -> SchedulerStats {
        let mut per_class = BTreeMap::new();
        for class in WorkloadClass::ALL {
            let q = self.queues.get(&class);
            per_class.insert(
                class.name().to_string(),
                ClassStats {
                    queued: q.map(|q| q.heap.len()).unwrap_or(0),
                    running: q.map(|q| q.running).unwrap_or(0),
                },
            );
        }
        SchedulerStats {
            per_class,
            tenants: self.tenants.len(),
        }
    }

    /// Pick the next class: reserved classes first if they have demand and
    /// free reserved slots; else weighted fair among classes with demand
    /// and free concurrency.
    fn pick_class(&self) -> Option<WorkloadClass> {
        for class in [WorkloadClass::Control, WorkloadClass::Replication] {
            let config = self.configs.get(&class)?;
            let queue = self.queues.get(&class)?;
            let reserved = config.reserved_slots.max(1);
            if !queue.heap.is_empty()
                && queue.running < reserved
                && queue.running < config.max_concurrency
            {
                return Some(class);
            }
        }
        let mut best: Option<(WorkloadClass, u64)> = None;
        for class in WorkloadClass::ALL {
            let config = match self.configs.get(&class) {
                Some(c) => c,
                None => continue,
            };
            let queue = match self.queues.get(&class) {
                Some(q) => q,
                None => continue,
            };
            if queue.heap.is_empty() || queue.running >= config.max_concurrency {
                continue;
            }
            let vt = *self.vtime.get(&class).unwrap_or(&0);
            match best {
                None => best = Some((class, vt)),
                Some((_, best_vt)) if vt < best_vt => best = Some((class, vt)),
                Some((best_class, best_vt)) if vt == best_vt && class < best_class => {
                    best = Some((class, vt));
                }
                _ => {}
            }
        }
        best.map(|(c, _)| c)
    }

    fn pop_class(&mut self, class: WorkloadClass) -> Option<WorkItem> {
        // Peek-loop: pop cancelled, re-queue tenant-saturated.
        let mut deferred = Vec::new();
        let result = loop {
            let entry = {
                let queue = self.queues.get_mut(&class)?;
                queue.heap.pop()
            };
            let Some(entry) = entry else {
                break None;
            };
            if self.cancelled.remove(&entry.work_id).is_some() {
                let _ = self.items.remove(&entry.work_id);
                continue;
            }
            let Some(item) = self.items.remove(&entry.work_id) else {
                continue;
            };
            let quota = self
                .quotas
                .get(&item.tenant)
                .cloned()
                .unwrap_or_else(|| self.default_quota.clone());
            let tenant_state = self.tenants.entry(item.tenant.clone()).or_default();
            let over_running = tenant_state.running >= quota.max_running;
            let over_class = quota
                .per_class_running
                .get(class.name())
                .is_some_and(|cap| {
                    *tenant_state
                        .per_class_running
                        .get(class.name())
                        .unwrap_or(&0)
                        >= *cap
                });
            if over_running || over_class {
                deferred.push((entry, item));
                continue;
            }
            self.running_meta
                .insert(item.work_id, (item.tenant.clone(), item.class));
            break Some(item);
        };
        // Put deferred back.
        if let Some(queue) = self.queues.get_mut(&class) {
            for (entry, item) in deferred {
                self.items.insert(item.work_id, item);
                queue.heap.push(entry);
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_not_starved_under_ai_flood() {
        let mut sched = HierarchicalScheduler::new();
        // Flood AI.
        for i in 0..32 {
            sched
                .submit(
                    "t1",
                    WorkloadClass::AiRetrieval,
                    100,
                    None,
                    None,
                    format!("ai-{i}"),
                )
                .unwrap();
        }
        // One control item.
        let control_id = sched
            .submit("system", WorkloadClass::Control, 255, None, None, "ctl")
            .unwrap();

        let mut saw_control = false;
        for _ in 0..40 {
            let batch = sched.poll(1);
            if batch.is_empty() {
                break;
            }
            for item in batch {
                if item.work_id == control_id {
                    saw_control = true;
                }
                sched.complete(item.work_id).unwrap();
            }
            if saw_control {
                break;
            }
        }
        assert!(saw_control, "control must run despite AI flood");
    }

    #[test]
    fn tenant_quota_blocks_adversary() {
        let mut sched = HierarchicalScheduler::new();
        sched.set_tenant_quota(
            "noisy",
            TenantQuota {
                max_running: 1,
                max_queued: 2,
                per_class_running: BTreeMap::new(),
            },
        );
        sched
            .submit("noisy", WorkloadClass::Analytics, 50, None, None, "a")
            .unwrap();
        sched
            .submit("noisy", WorkloadClass::Analytics, 50, None, None, "b")
            .unwrap();
        let err = sched
            .submit("noisy", WorkloadClass::Analytics, 50, None, None, "c")
            .unwrap_err();
        assert!(matches!(err, SchedulerError::TenantQuota { .. }));

        // Other tenant still admitted.
        sched
            .submit("quiet", WorkloadClass::Oltp, 200, None, None, "ok")
            .unwrap();
    }

    #[test]
    fn cancel_drops_before_dispatch() {
        let mut sched = HierarchicalScheduler::new();
        let id = sched
            .submit("t", WorkloadClass::Oltp, 10, None, None, "x")
            .unwrap();
        sched.cancel(id).unwrap();
        let batch = sched.poll(10);
        assert!(batch.is_empty());
    }

    #[test]
    fn priority_orders_within_class() {
        let mut sched = HierarchicalScheduler::new();
        let low = sched
            .submit("t", WorkloadClass::Oltp, 1, None, None, "low")
            .unwrap();
        let high = sched
            .submit("t", WorkloadClass::Oltp, 200, None, None, "high")
            .unwrap();
        let batch = sched.poll(1);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].work_id, high);
        assert_ne!(batch[0].work_id, low);
    }
}
