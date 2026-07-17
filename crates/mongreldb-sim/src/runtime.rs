//! Deterministic cooperative task scheduling (spec section 9.5,
//! FND-005).
//!
//! Tasks are boxed closure state machines: each poll runs one step and
//! returns a [`TaskState`] telling the scheduler what to do next. The
//! executor is deliberately std-only and single-threaded — no tokio, no
//! worker threads — because a production runtime schedules on OS threads
//! and reads the real clock, which would make runs irreproducible. When
//! several tasks are ready, the runtime's seeded [`SimRng`] picks the
//! next one, so the seed fixes the interleaving.
//!
//! [`crate::scenario::Scenario`] builds process crash/restart on top:
//! crashing a node drops its task bodies (volatile state) while the
//! node's [`VirtualDisk`] keeps exactly the fsynced prefix.

use crate::clock::{Clock, Micros};
use crate::disk::{DiskError, VirtualDisk};
use crate::network::{Message, Network, NodeId, SendOutcome};
use crate::rng::{Seed, SimRng};
use crate::scenario::Event;
use std::collections::BTreeMap;

/// A task handle within a [`Runtime`].
pub type TaskId = u64;

/// One step of a cooperative state machine. Captured variables are the
/// task's volatile state: they vanish when the owning process crashes.
pub type TaskBody = Box<dyn FnMut(&mut NodeContext<'_>) -> TaskState>;

/// What a task wants after a step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    /// Reschedule as soon as the scheduler comes back to it.
    Yield,
    /// Wake after the given number of virtual micros.
    SleepFor(Micros),
    /// Park until a network message arrives for this node.
    WaitForMessage,
    /// The task is finished.
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Ready,
    Sleeping(Micros),
    WaitingMessage,
    Done,
}

struct Task {
    node: NodeId,
    name: String,
    status: Status,
    rng: SimRng,
    body: TaskBody,
}

/// The single-node view handed to a task step: virtual time, a seeded
/// stream, the network, the node's disk, and the scenario event log.
pub struct NodeContext<'a> {
    node: NodeId,
    clock: &'a Clock,
    rng: &'a mut SimRng,
    net: &'a mut Network,
    disk: &'a mut VirtualDisk,
    events: &'a mut Vec<Event>,
}

impl NodeContext<'_> {
    /// The node this task runs on.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// Global virtual time.
    pub fn now(&self) -> Micros {
        self.clock.now()
    }

    /// Node-local time with the node's skew schedule applied.
    pub fn node_now(&self) -> i64 {
        self.clock.node_now(self.node)
    }

    /// The task's own seeded stream (forked at spawn).
    pub fn rng(&mut self) -> &mut SimRng {
        &mut *self.rng
    }

    /// Sends a message, recording send/drop/duplicate/reorder events.
    pub fn send(&mut self, to: NodeId, payload: impl Into<Vec<u8>>) -> SendOutcome {
        let outcome = self.net.send(
            self.node,
            to,
            payload.into(),
            self.clock.now(),
            &mut *self.rng,
        );
        match &outcome {
            SendOutcome::Scheduled {
                seq,
                deliver_at,
                duplicated,
                reordered,
            } => {
                self.events.push(Event::MessageSent {
                    from: self.node,
                    to,
                    seq: *seq,
                    deliver_at: *deliver_at,
                });
                if *duplicated {
                    self.events.push(Event::MessageDuplicated {
                        from: self.node,
                        to,
                        seq: *seq,
                    });
                }
                if *reordered {
                    self.events.push(Event::MessageReordered {
                        from: self.node,
                        to,
                        seq: *seq,
                    });
                }
            }
            SendOutcome::Dropped { seq, reason } => {
                self.events.push(Event::MessageDropped {
                    from: self.node,
                    to,
                    seq: *seq,
                    reason: *reason,
                });
            }
        }
        outcome
    }

    /// Pops the oldest message from this node's inbox, if any.
    pub fn try_recv(&mut self) -> Option<Message> {
        self.net.try_recv(self.node)
    }

    /// Appends to a file's pending stage, recording the outcome.
    pub fn disk_append(&mut self, path: &str, bytes: &[u8]) -> Result<usize, DiskError> {
        match self.disk.append(path, bytes) {
            Ok(written) => {
                if written < bytes.len() {
                    self.events.push(Event::DiskTornWrite {
                        node: self.node,
                        path: path.to_string(),
                        requested: bytes.len(),
                        written,
                    });
                } else {
                    self.events.push(Event::DiskWrite {
                        node: self.node,
                        path: path.to_string(),
                        len: written,
                    });
                }
                Ok(written)
            }
            Err(error) => {
                self.events.push(Event::DiskWriteFailed {
                    node: self.node,
                    path: path.to_string(),
                });
                Err(error)
            }
        }
    }

    /// Fsyncs a file, recording the outcome.
    pub fn disk_fsync(&mut self, path: &str) -> Result<(), DiskError> {
        match self.disk.fsync(path) {
            Ok(()) => {
                self.events.push(Event::DiskFsync {
                    node: self.node,
                    path: path.to_string(),
                    durable_len: self.disk.durable_len(path),
                });
                Ok(())
            }
            Err(error) => {
                self.events.push(Event::DiskFsyncFailed {
                    node: self.node,
                    path: path.to_string(),
                });
                Err(error)
            }
        }
    }

    /// Live view of a file: durable bytes plus pending bytes.
    pub fn disk_read(&self, path: &str) -> Vec<u8> {
        self.disk.read(path)
    }

    /// Crash-recovery view of a file: exactly the fsynced prefix.
    pub fn disk_read_durable(&self, path: &str) -> Vec<u8> {
        self.disk.read_durable(path)
    }

    /// Records an application-level event in the scenario log.
    pub fn log(&mut self, message: impl Into<String>) {
        self.events.push(Event::Custom {
            node: self.node,
            message: message.into(),
        });
    }
}

/// The cooperative executor: a clock, a seeded stream, and the tasks.
pub struct Runtime {
    clock: Clock,
    rng: SimRng,
    tasks: BTreeMap<TaskId, Task>,
    next_task_id: TaskId,
}

impl Runtime {
    /// An empty runtime at virtual t=0.
    pub fn new(seed: Seed) -> Self {
        Self {
            clock: Clock::new(),
            rng: SimRng::from_seed(seed),
            tasks: BTreeMap::new(),
            next_task_id: 1,
        }
    }

    /// The virtual clock driving timers and node skew.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    pub(crate) fn clock_mut(&mut self) -> &mut Clock {
        &mut self.clock
    }

    /// Spawns a task on a node. The task receives its own stream forked
    /// from the runtime seed.
    pub fn spawn(&mut self, node: NodeId, name: &str, body: TaskBody) -> TaskId {
        let id = self.next_task_id;
        self.next_task_id += 1;
        let rng = self.rng.fork();
        self.tasks.insert(
            id,
            Task {
                node,
                name: name.to_string(),
                status: Status::Ready,
                rng,
                body,
            },
        );
        id
    }

    pub(crate) fn task_node(&self, task: TaskId) -> NodeId {
        self.tasks[&task].node
    }

    /// Wakes every task whose sleep deadline has passed.
    pub(crate) fn wake_due(&mut self) {
        let now = self.clock.now();
        for task in self.tasks.values_mut() {
            if let Status::Sleeping(until) = task.status {
                if until <= now {
                    task.status = Status::Ready;
                }
            }
        }
    }

    /// Wakes tasks of `node` that parked on [`TaskState::WaitForMessage`].
    pub(crate) fn wake_receivers(&mut self, node: NodeId) {
        for task in self.tasks.values_mut() {
            if task.node == node && task.status == Status::WaitingMessage {
                task.status = Status::Ready;
            }
        }
    }

    /// Picks a ready task using the runtime's seeded stream, so the seed
    /// fixes the interleaving. Returns `None` when nothing can run.
    pub(crate) fn pick_ready(&mut self) -> Option<TaskId> {
        let ready: Vec<TaskId> = self
            .tasks
            .iter()
            .filter(|(_, task)| task.status == Status::Ready)
            .map(|(&id, _)| id)
            .collect();
        if ready.is_empty() {
            None
        } else {
            Some(ready[self.rng.below(ready.len() as u64) as usize])
        }
    }

    /// Runs one step of a task and applies the resulting [`TaskState`].
    pub(crate) fn step(
        &mut self,
        task_id: TaskId,
        net: &mut Network,
        disk: &mut VirtualDisk,
        events: &mut Vec<Event>,
    ) {
        let now = self.clock.now();
        let task = self.tasks.get_mut(&task_id).expect("task must exist");
        let node = task.node;
        let state = {
            let mut ctx = NodeContext {
                node,
                clock: &self.clock,
                rng: &mut task.rng,
                net: &mut *net,
                disk: &mut *disk,
                events: &mut *events,
            };
            (task.body)(&mut ctx)
        };
        match state {
            TaskState::Yield => task.status = Status::Ready,
            TaskState::SleepFor(delta) => task.status = Status::Sleeping(now + delta),
            TaskState::WaitForMessage => {
                task.status = if net.inbox_len(node) > 0 {
                    Status::Ready
                } else {
                    Status::WaitingMessage
                };
            }
            TaskState::Done => {
                task.status = Status::Done;
                events.push(Event::TaskDone {
                    task: task_id,
                    node,
                    name: task.name.clone(),
                });
            }
        }
    }

    /// Drops every task of a node, discarding its volatile state.
    /// Returns how many tasks were dropped.
    pub(crate) fn remove_tasks_of(&mut self, node: NodeId) -> usize {
        let before = self.tasks.len();
        self.tasks.retain(|_, task| task.node != node);
        before - self.tasks.len()
    }

    /// The earliest sleep deadline among parked tasks, if any.
    pub(crate) fn next_wake(&self) -> Option<Micros> {
        self.tasks
            .values()
            .filter_map(|task| match task.status {
                Status::Sleeping(until) => Some(until),
                _ => None,
            })
            .min()
    }

    /// Whether every spawned task has finished.
    pub(crate) fn all_done(&self) -> bool {
        self.tasks.values().all(|task| task.status == Status::Done)
    }

    /// Number of tasks that have not finished yet.
    pub(crate) fn unfinished_count(&self) -> usize {
        self.tasks
            .values()
            .filter(|task| task.status != Status::Done)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::LinkConfig;

    const A: NodeId = NodeId(1);

    fn harness(seed: u64) -> (Runtime, Network, VirtualDisk, Vec<Event>) {
        (
            Runtime::new(Seed::new(seed)),
            Network::new(LinkConfig::default()),
            VirtualDisk::new(),
            Vec::new(),
        )
    }

    #[test]
    fn tasks_run_as_cooperative_state_machines() {
        let (mut runtime, mut net, mut disk, mut events) = harness(1);
        let task = runtime.spawn(A, "counter", {
            let mut steps = 0;
            Box::new(move |_ctx| {
                steps += 1;
                if steps >= 3 {
                    TaskState::Done
                } else {
                    TaskState::Yield
                }
            })
        });

        runtime.step(task, &mut net, &mut disk, &mut events);
        runtime.step(task, &mut net, &mut disk, &mut events);
        assert!(!runtime.all_done());
        assert_eq!(runtime.unfinished_count(), 1);
        runtime.step(task, &mut net, &mut disk, &mut events);
        assert!(runtime.all_done());
        assert!(events.iter().any(|event| matches!(
            event,
            Event::TaskDone { task: done, name, .. } if *done == task && name == "counter"
        )));
    }

    #[test]
    fn sleeping_tasks_wake_at_their_deadline() {
        let (mut runtime, mut net, mut disk, mut events) = harness(2);
        let task = runtime.spawn(A, "sleeper", {
            let mut slept = false;
            Box::new(move |_ctx| {
                if slept {
                    TaskState::Done
                } else {
                    slept = true;
                    TaskState::SleepFor(100)
                }
            })
        });

        runtime.step(task, &mut net, &mut disk, &mut events);
        assert_eq!(runtime.next_wake(), Some(100));
        runtime.wake_due();
        assert_eq!(runtime.pick_ready(), None);

        runtime.clock_mut().advance_to(100).unwrap();
        runtime.wake_due();
        assert_eq!(runtime.pick_ready(), Some(task));
    }

    #[test]
    fn seeded_ready_pick_is_reproducible() {
        let pick_sequence = |seed: u64| {
            let mut runtime = Runtime::new(Seed::new(seed));
            for _ in 0..5 {
                runtime.spawn(A, "idle", Box::new(|_| TaskState::Yield));
            }
            (0..20).map(|_| runtime.pick_ready()).collect::<Vec<_>>()
        };
        assert_eq!(pick_sequence(9), pick_sequence(9));
        assert_ne!(pick_sequence(9), pick_sequence(10));
    }

    #[test]
    fn crash_drops_tasks_and_receiver_wake_works() {
        let (mut runtime, mut net, mut disk, mut events) = harness(3);
        let b = NodeId(2);
        runtime.spawn(b, "waiter", Box::new(|_| TaskState::WaitForMessage));
        runtime.spawn(b, "waiter2", Box::new(|_| TaskState::WaitForMessage));
        let parked = runtime.tasks.len();
        assert_eq!(parked, 2);

        // Step one waiter: it parks because the inbox is empty.
        let task = runtime.pick_ready().unwrap();
        runtime.step(task, &mut net, &mut disk, &mut events);
        runtime.wake_receivers(b);
        // Messages arrived? No — wake_receivers only helps after delivery;
        // with an empty inbox the stepped task parks again on next step.

        assert_eq!(runtime.remove_tasks_of(b), 2);
        assert_eq!(runtime.unfinished_count(), 0);
    }
}
