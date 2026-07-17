//! Scenario runner: schedule driving, fault orchestration, the event
//! log, and failure artifacts (spec section 9.5, FND-005).
//!
//! A [`Scenario`] wires a [`Runtime`], a [`Network`], and one
//! [`VirtualDisk`] per node together and drives the schedule to
//! completion. While any task is ready, a seeded pick chooses the next
//! step; when nothing can run, the clock advances on idle to the next
//! timer or message delivery. A scenario that can neither run nor
//! advance while tasks remain is reported as a deadlock; a step budget
//! guards against livelock.
//!
//! Every failure (deadlock, step limit, or a caught task panic)
//! persists the seed and the full event log as JSON so CI can archive
//! the exact repro — see [`failure_dir`].

use crate::clock::{Micros, SkewSchedule};
use crate::disk::VirtualDisk;
use crate::network::{DeliveryOutcome, DropReason, LinkConfig, Network, NetworkStats, NodeId};
use crate::rng::Seed;
use crate::runtime::{Runtime, TaskBody, TaskId};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::{env, fs, io};

/// One recorded step of a simulation run, in execution order. Two runs
/// of the same scenario with the same seed produce identical sequences.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    /// A task was spawned for a node process.
    TaskSpawned {
        /// New task id.
        task: TaskId,
        /// Owning node.
        node: NodeId,
        /// Process name.
        name: String,
    },
    /// A task finished.
    TaskDone {
        /// Finished task id.
        task: TaskId,
        /// Owning node.
        node: NodeId,
        /// Process name.
        name: String,
    },
    /// A message entered the flight queue.
    MessageSent {
        /// Sender.
        from: NodeId,
        /// Destination.
        to: NodeId,
        /// Sequence number.
        seq: u64,
        /// Scheduled delivery time.
        deliver_at: Micros,
    },
    /// A message never reached its destination.
    MessageDropped {
        /// Sender.
        from: NodeId,
        /// Destination.
        to: NodeId,
        /// Sequence number.
        seq: u64,
        /// Why it was dropped.
        reason: DropReason,
    },
    /// A send scheduled an extra copy.
    MessageDuplicated {
        /// Sender.
        from: NodeId,
        /// Destination.
        to: NodeId,
        /// Sequence number of the original copy.
        seq: u64,
    },
    /// A send undercut a previously scheduled send on the same link.
    MessageReordered {
        /// Sender.
        from: NodeId,
        /// Destination.
        to: NodeId,
        /// Sequence number.
        seq: u64,
    },
    /// A message reached a destination inbox.
    MessageDelivered {
        /// Sender.
        from: NodeId,
        /// Destination.
        to: NodeId,
        /// Sequence number.
        seq: u64,
    },
    /// A write landed in a file's pending stage.
    DiskWrite {
        /// Owning node.
        node: NodeId,
        /// File path.
        path: String,
        /// Bytes written.
        len: usize,
    },
    /// A write was truncated (torn write).
    DiskTornWrite {
        /// Owning node.
        node: NodeId,
        /// File path.
        path: String,
        /// Bytes requested.
        requested: usize,
        /// Bytes actually written.
        written: usize,
    },
    /// An injected write failure fired.
    DiskWriteFailed {
        /// Owning node.
        node: NodeId,
        /// File path.
        path: String,
    },
    /// Pending bytes became durable.
    DiskFsync {
        /// Owning node.
        node: NodeId,
        /// File path.
        path: String,
        /// Total durable bytes after the sync.
        durable_len: usize,
    },
    /// An injected fsync failure fired.
    DiskFsyncFailed {
        /// Owning node.
        node: NodeId,
        /// File path.
        path: String,
    },
    /// A node process crashed; volatile state was dropped.
    Crash {
        /// Crashed node.
        node: NodeId,
    },
    /// A crashed node process restarted.
    Restart {
        /// Restarted node.
        node: NodeId,
    },
    /// Connectivity between two node sets was cut.
    Partition {
        /// First side.
        group_a: Vec<NodeId>,
        /// Second side.
        group_b: Vec<NodeId>,
    },
    /// All partition cuts were removed.
    Healed,
    /// A node's skew schedule was replaced.
    SkewUpdated {
        /// Affected node.
        node: NodeId,
    },
    /// The virtual clock advanced on idle.
    ClockAdvanced {
        /// Previous time.
        from: Micros,
        /// New time.
        to: Micros,
    },
    /// An application-level record from `NodeContext::log`.
    Custom {
        /// Logging node.
        node: NodeId,
        /// Free-form message.
        message: String,
    },
}

/// Why a scenario run failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ScenarioError {
    /// No task can run, no message is in flight, no timer is pending,
    /// and unfinished tasks remain.
    #[error("deadlock detected at t={at_micros}: {waiting} task(s) waiting, none runnable")]
    DeadlockDetected {
        /// Virtual time of the detection.
        at_micros: Micros,
        /// Tasks still unfinished.
        waiting: usize,
    },
    /// The step budget ran out (probable livelock).
    #[error("step limit {limit} exceeded (possible livelock)")]
    StepLimitExceeded {
        /// The budget that was exceeded.
        limit: u64,
    },
    /// A task body panicked; the panic is re-raised after the failure
    /// artifact is persisted.
    #[error("task panicked: {0}")]
    TaskPanicked(String),
}

/// Summary of a completed run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunOutcome {
    /// The seed the run used.
    pub seed: Seed,
    /// Task steps executed.
    pub steps: u64,
    /// Final virtual time.
    pub sim_time_micros: Micros,
}

/// A registered node process: its name plus the factory that rebuilds
/// its task body on (re)start.
type ProcessFactory = (String, Box<dyn FnMut() -> TaskBody>);

/// A wired-up simulation: runtime, network, per-node disks, process
/// factories for restart, and the event log.
pub struct Scenario {
    seed: Seed,
    runtime: Runtime,
    network: Network,
    disks: BTreeMap<NodeId, VirtualDisk>,
    factories: BTreeMap<NodeId, ProcessFactory>,
    crashed: BTreeSet<NodeId>,
    events: Vec<Event>,
}

impl Scenario {
    /// A scenario with default link behavior and no nodes.
    pub fn new(seed: Seed) -> Self {
        Self {
            seed,
            runtime: Runtime::new(seed),
            network: Network::new(LinkConfig::default()),
            disks: BTreeMap::new(),
            factories: BTreeMap::new(),
            crashed: BTreeSet::new(),
            events: Vec::new(),
        }
    }

    /// The seed this scenario runs with.
    pub fn seed(&self) -> Seed {
        self.seed
    }

    /// Current virtual time.
    pub fn now(&self) -> Micros {
        self.runtime.clock().now()
    }

    /// The recorded event log, in execution order.
    pub fn events(&self) -> &[Event] {
        &self.events
    }

    /// Cumulative network counters.
    pub fn network_stats(&self) -> NetworkStats {
        self.network.stats()
    }

    /// A node's disk, if the node exists.
    pub fn disk(&self, node: NodeId) -> Option<&VirtualDisk> {
        self.disks.get(&node)
    }

    /// A node's disk for fault configuration (created on demand).
    pub fn disk_mut(&mut self, node: NodeId) -> &mut VirtualDisk {
        self.disks.entry(node).or_default()
    }

    /// Registers a node process. The factory produces the task body; it
    /// is called now and again on every restart, so anything captured by
    /// the returned closure is volatile state that a crash discards.
    /// Durable state belongs on the node's disk.
    pub fn add_node(
        &mut self,
        node: NodeId,
        name: &str,
        mut factory: impl FnMut() -> TaskBody + 'static,
    ) {
        self.disks.entry(node).or_default();
        let body = factory();
        let task = self.runtime.spawn(node, name, body);
        self.events.push(Event::TaskSpawned {
            task,
            node,
            name: name.to_string(),
        });
        self.factories
            .insert(node, (name.to_string(), Box::new(factory)));
    }

    /// Overrides one directed link's behavior.
    pub fn set_link_config(&mut self, from: NodeId, to: NodeId, config: LinkConfig) {
        self.network.set_link_config(from, to, config);
    }

    /// Installs a clock skew schedule for a node.
    pub fn set_skew(&mut self, node: NodeId, schedule: SkewSchedule) {
        self.runtime.clock_mut().set_skew(node, schedule);
        self.events.push(Event::SkewUpdated { node });
    }

    /// Cuts connectivity between two node sets until [`Scenario::heal`].
    pub fn partition(
        &mut self,
        group_a: impl IntoIterator<Item = NodeId>,
        group_b: impl IntoIterator<Item = NodeId>,
    ) {
        let group_a: Vec<NodeId> = group_a.into_iter().collect();
        let group_b: Vec<NodeId> = group_b.into_iter().collect();
        self.network
            .partition(group_a.iter().copied(), group_b.iter().copied());
        self.events.push(Event::Partition { group_a, group_b });
    }

    /// Restores full connectivity.
    pub fn heal(&mut self) {
        self.network.heal();
        self.events.push(Event::Healed);
    }

    /// Crashes a node: its tasks (volatile state) and inbox are dropped
    /// and its disk loses every un-fsynced byte. In-flight messages to
    /// the node are discarded on delivery.
    pub fn crash_node(&mut self, node: NodeId) {
        if !self.crashed.insert(node) {
            return;
        }
        self.runtime.remove_tasks_of(node);
        self.network.clear_inbox(node);
        if let Some(disk) = self.disks.get_mut(&node) {
            disk.crash();
        }
        self.events.push(Event::Crash { node });
    }

    /// Restarts a crashed node by re-invoking its process factory. Only
    /// nodes registered with [`Scenario::add_node`] can restart.
    pub fn restart_node(&mut self, node: NodeId) {
        if !self.crashed.remove(&node) {
            return;
        }
        self.disks.entry(node).or_default();
        let (name, factory) = self
            .factories
            .get_mut(&node)
            .expect("no process registered for node");
        let name = name.clone();
        let body = factory();
        let task = self.runtime.spawn(node, &name, body);
        self.events.push(Event::Restart { node });
        self.events.push(Event::TaskSpawned { task, node, name });
    }

    /// Drives the schedule to completion with no mid-run chaos.
    ///
    /// On failure the seed and event log are persisted to
    /// [`failure_dir`]; a task panic is re-raised after persisting.
    pub fn run(&mut self, max_steps: u64) -> Result<RunOutcome, ScenarioError> {
        self.run_with(max_steps, |_| {})
    }

    /// Drives the schedule to completion. `chaos` runs before every
    /// scheduling decision with full mutable access to the scenario, so
    /// tests can partition, heal, crash, and restart at chosen virtual
    /// times or in reaction to recorded events.
    pub fn run_with(
        &mut self,
        max_steps: u64,
        mut chaos: impl FnMut(&mut Scenario),
    ) -> Result<RunOutcome, ScenarioError> {
        let outcome = catch_unwind(AssertUnwindSafe(|| self.run_inner(max_steps, &mut chaos)));
        match outcome {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(error)) => {
                self.persist_failure(&error);
                Err(error)
            }
            Err(payload) => {
                self.persist_failure(&ScenarioError::TaskPanicked(panic_message(
                    payload.as_ref(),
                )));
                resume_unwind(payload)
            }
        }
    }

    fn run_inner(
        &mut self,
        max_steps: u64,
        chaos: &mut dyn FnMut(&mut Scenario),
    ) -> Result<RunOutcome, ScenarioError> {
        let mut steps = 0u64;
        loop {
            chaos(self);
            self.runtime.wake_due();

            if let Some(task) = self.runtime.pick_ready() {
                steps += 1;
                if steps > max_steps {
                    return Err(ScenarioError::StepLimitExceeded { limit: max_steps });
                }
                let node = self.runtime.task_node(task);
                let disk = self.disks.entry(node).or_default();
                self.runtime
                    .step(task, &mut self.network, disk, &mut self.events);
                continue;
            }

            let next = self
                .network
                .next_delivery()
                .into_iter()
                .chain(self.runtime.next_wake())
                .min();
            match next {
                Some(target) => {
                    let from = self.now();
                    self.runtime
                        .clock_mut()
                        .advance_to(target)
                        .expect("event times are monotonic");
                    self.events.push(Event::ClockAdvanced { from, to: target });
                    self.deliver_due();
                }
                None => {
                    if self.runtime.all_done() {
                        return Ok(RunOutcome {
                            seed: self.seed,
                            steps,
                            sim_time_micros: self.now(),
                        });
                    }
                    return Err(ScenarioError::DeadlockDetected {
                        at_micros: self.now(),
                        waiting: self.runtime.unfinished_count(),
                    });
                }
            }
        }
    }

    fn deliver_due(&mut self) {
        let now = self.now();
        let deliveries = self.network.deliver_due(now, &self.crashed);
        let mut woke = BTreeSet::new();
        for delivery in deliveries {
            match delivery.outcome {
                DeliveryOutcome::Delivered => {
                    self.events.push(Event::MessageDelivered {
                        from: delivery.from,
                        to: delivery.to,
                        seq: delivery.seq,
                    });
                    woke.insert(delivery.to);
                }
                DeliveryOutcome::Discarded(reason) => {
                    self.events.push(Event::MessageDropped {
                        from: delivery.from,
                        to: delivery.to,
                        seq: delivery.seq,
                        reason,
                    });
                }
            }
        }
        for node in woke {
            self.runtime.wake_receivers(node);
        }
    }

    fn persist_failure(&self, error: &ScenarioError) {
        let dir = failure_dir();
        if let Err(io_error) = persist_failure_report(&dir, self.seed, error, &self.events) {
            eprintln!(
                "mongreldb-sim: could not persist failure artifact to {}: {io_error}",
                dir.display()
            );
        }
    }
}

fn panic_message(payload: &dyn Any) -> String {
    if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Where failed-seed artifacts are written: `$MONGRELDB_SIM_FAILURES`,
/// defaulting to `target/sim-failures/`.
pub fn failure_dir() -> PathBuf {
    env::var("MONGRELDB_SIM_FAILURES")
        .map_or_else(|_| PathBuf::from("target/sim-failures"), PathBuf::from)
}

static FAILURE_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Writes a `{seed, error, events}` JSON artifact for a failed run and
/// returns its path.
pub fn persist_failure_report(
    dir: &Path,
    seed: Seed,
    error: &ScenarioError,
    events: &[Event],
) -> io::Result<PathBuf> {
    fs::create_dir_all(dir)?;
    let ordinal = FAILURE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!("sim-failure-{}-{ordinal}.json", seed.get()));
    let report = serde_json::json!({
        "seed": seed.get(),
        "error": error.to_string(),
        "event_count": events.len(),
        "events": events,
    });
    fs::write(&path, serde_json::to_string_pretty(&report)?)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::TaskState;

    const A: NodeId = NodeId(1);

    #[test]
    fn deadlock_is_detected() {
        let mut scenario = Scenario::new(Seed::new(1));
        scenario.add_node(A, "waiter", || Box::new(|_| TaskState::WaitForMessage));
        let error = scenario.run_inner(1_000, &mut |_| {}).unwrap_err();
        assert!(matches!(
            error,
            ScenarioError::DeadlockDetected { waiting: 1, .. }
        ));
    }

    #[test]
    fn step_limit_guards_livelock() {
        let mut scenario = Scenario::new(Seed::new(2));
        scenario.add_node(A, "spinner", || Box::new(|_| TaskState::Yield));
        let error = scenario.run_inner(100, &mut |_| {}).unwrap_err();
        assert_eq!(error, ScenarioError::StepLimitExceeded { limit: 100 });
    }

    #[test]
    fn happy_path_run_records_events() {
        let mut scenario = Scenario::new(Seed::new(3));
        scenario.add_node(A, "lonely", || {
            Box::new(|ctx| {
                ctx.log("hello");
                TaskState::Done
            })
        });
        let outcome = scenario.run(1_000).unwrap();
        assert_eq!(outcome.seed, Seed::new(3));
        assert_eq!(outcome.steps, 1);
        assert!(scenario
            .events()
            .iter()
            .any(|event| matches!(event, Event::Custom { message, .. } if message == "hello")));
    }

    #[test]
    fn persist_failure_report_writes_seed_file() {
        let dir =
            env::temp_dir().join(format!("mongreldb-sim-persist-test-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let error = ScenarioError::StepLimitExceeded { limit: 5 };
        let events = vec![Event::Healed];
        let path = persist_failure_report(&dir, Seed::new(77), &error, &events).unwrap();

        let written: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(written["seed"], 77);
        assert!(written["error"].as_str().unwrap().contains("step limit"));
        assert_eq!(written["events"], serde_json::json!(["Healed"]));
        let _ = fs::remove_dir_all(&dir);
    }
}
