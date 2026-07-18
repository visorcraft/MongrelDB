//! MongrelDB cluster runtime (spec section 6.6, Stages 2-3).
//!
//! Owns the node runtime, meta control-plane group, tablet descriptors,
//! placement, split/merge, rebalancing, distributed transactions, cluster
//! backup, and rolling-upgrade coordination.

pub mod bootstrap;
pub mod ddl;
pub mod dist_txn;
pub mod global_constraints;
pub mod merge;
pub mod meta;
pub mod network;
pub mod node;
pub mod placement;
pub mod routing;
pub mod runtime;
pub mod split;
pub mod tablet;
