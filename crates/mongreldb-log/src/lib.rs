//! MongrelDB commit-log abstraction and versioned command envelope.
//!
//! This crate defines the durable-write contract of the engine
//! (spec sections 6.2, 9.3, 9.4):
//!
//! - [`CommandEnvelope`]: the versioned, checksummed, canonically encoded
//!   form of every persisted cluster/transaction command.
//! - [`CommitLog`]: the single authority through which commands become
//!   committed. The storage apply path receives only committed commands.
//!
//! The standalone adapter (`StandaloneCommitLog`) lives in `mongreldb-core`
//! because it wraps the existing shared WAL; the replicated adapter lands
//! with the consensus crate in Stage 2.

pub mod commit_log;
pub mod envelope;
pub mod mem;

pub use commit_log::{
    CommitLog, CommitReceipt, CommittedEntry, DurabilityLevel, ExecutionControl, LogError,
    LogPosition, LogSnapshot,
};
pub use envelope::{
    CommandEnvelope, EnvelopeError, COMMAND_ENVELOPE_FORMAT_VERSION, MAX_COMMAND_PAYLOAD_BYTES,
    MIN_SUPPORTED_FORMAT_VERSION,
};
pub use mem::{InMemoryCommitLog, TimestampSource};
