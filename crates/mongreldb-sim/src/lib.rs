//! MongrelDB deterministic test simulator (spec section 9.5, FND-005).
//!
//! A seeded, reproducible environment for consensus and
//! distributed-transaction tests: a virtual clock with per-node skew
//! schedules, deterministic cooperative task scheduling, virtual network
//! links with delay/drop/duplication/reordering and partition/heal,
//! virtual durable disks with write/fsync/torn-write faults, and process
//! crash/restart that preserves exactly the fsynced prefix.
//!
//! # Determinism contract
//!
//! Same seed ⇒ identical event order and identical outcomes. The
//! contract rests on four rules the whole crate follows:
//!
//! - Every random decision flows through [`SimRng`], seeded once from
//!   the scenario [`Seed`]. Subsystems receive independent streams via
//!   [`SimRng::fork`] / [`SimRng::fork_label`], so drawing extra numbers
//!   in one subsystem never perturbs another.
//! - Time is virtual microseconds; the simulator never reads the wall
//!   clock. The clock advances only when no task can run
//!   ("advance-on-idle"), jumping straight to the next timer or message
//!   delivery.
//! - Scheduling is cooperative and single-threaded. When several tasks
//!   are ready, the runtime's seeded stream picks the next one, fixing
//!   the interleaving.
//! - Only `BTreeMap`/`BTreeSet` are iterated on behavior-affecting
//!   paths, so hash-randomized iteration order can never leak into a
//!   run.
//!
//! # Why no async runtime
//!
//! The executor is deliberately std-only: no tokio, no other runtime.
//! A production executor schedules on OS threads and reads the real
//! clock, which makes runs irreproducible. Here a task is a boxed
//! closure state machine ([`TaskBody`]) polled step by step by the
//! seeded cooperative executor in [`runtime`], so every interleaving
//! decision and every timer fires under simulator control.
//!
//! # Failure artifacts
//!
//! When a run fails (deadlock, step-limit exhaustion, or a task panic),
//! [`Scenario::run`] / [`Scenario::run_with`] persist the seed and the
//! full event log as JSON to `$MONGRELDB_SIM_FAILURES` (default
//! `target/sim-failures/`) so CI can archive the exact repro.

pub mod clock;
pub mod disk;
pub mod network;
pub mod rng;
pub mod runtime;
pub mod scenario;

pub use clock::{Clock, ClockError, Micros, SkewSchedule};
pub use disk::{DiskError, VirtualDisk};
pub use network::{
    Delivery, DeliveryOutcome, DropReason, LinkConfig, Message, Network, NetworkStats, NodeId,
    SendOutcome,
};
pub use rng::{Seed, SimRng};
pub use runtime::{NodeContext, Runtime, TaskBody, TaskId, TaskState};
pub use scenario::{
    failure_dir, persist_failure_report, Event, RunOutcome, Scenario, ScenarioError,
};
