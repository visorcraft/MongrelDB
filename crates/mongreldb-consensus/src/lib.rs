//! MongrelDB consensus adapter (spec section 6.5, Stage 2B).
//!
//! Wraps a mature Raft implementation (`openraft`, ADR-0004): MongrelDB
//! implements the storage/state-machine/network adapter, never the consensus
//! algorithm itself. One consensus group owns one committed log order with at
//! most one effective leader per term (spec section 4.2).

pub mod engine_sink;
pub mod error;
pub mod group;
pub mod identity;
pub mod network;
pub mod raft_log;
pub mod read;
pub mod state_machine;
pub mod storage;
