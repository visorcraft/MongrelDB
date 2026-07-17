//! `DatabaseHandle`: a lightweight caller-specific object referencing one
//! `DatabaseCore` (spec §10.1, S1A-001).
//!
//! Handles are issued by [`crate::manager::DatabaseManager::open_shared`].
//! Every handle carries its own [`HandleIdentity`] and [`HandleAccess`], while
//! recovery, WAL opening, open-generation advancement, and table mounting all
//! happened exactly once on the shared core. Dropping one handle never closes
//! storage; the core closes when the last reference drops or when
//! [`DatabaseHandle::shutdown`] drains it (S1A-004).
//!
//! The full storage API is available through `Deref<Target = Database>`: a
//! handle behaves exactly like a `Database` facade over the shared core, with
//! per-handle identity layered on top. The core itself never stores one
//! mutable "current principal" (spec §4.6) — identity lives here, on the
//! handle.

use std::sync::Arc;
use std::time::Duration;

use crate::core::{LifecycleState, OperationGuard};
use crate::database::{Database, DatabaseCore};
use crate::error::Result;

/// The per-caller identity bound to one handle (spec §10.1, S1A-001).
///
/// This is a label the core never consults for shared-table authorization in
/// Stage 1A (the embedded enforcement path reads the facade's auth state, and
/// shared cores reject auth-mode transitions); per-request enforcement over
/// handles arrives with Stage 1D sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleIdentity {
    /// No credentials were supplied for this handle.
    Credentialless,
    /// A user resolved from the database catalog. `user_id` +
    /// `created_version` pin the exact catalog generation so username reuse
    /// cannot revive a stale identity.
    CatalogUser {
        username: String,
        user_id: u64,
        created_version: u64,
    },
    /// A non-catalog principal (service account, daemon worker, test actor).
    ServicePrincipal { principal_id: [u8; 16] },
}

/// The identity requested when attaching a shared handle (spec §2.2).
///
/// Stage 1A supports credentialless and service-principal attaches. Catalog
/// users authenticate through an attached handle (Stage 1D session work adds
/// credentialed attaches).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpenIdentity {
    /// Attach without credentials. Rejected (fail closed) when the database
    /// catalog has `require_auth` enabled.
    Credentialless,
    /// Attach as a non-catalog service principal.
    ServicePrincipal { principal_id: [u8; 16] },
}

impl OpenIdentity {
    pub(crate) fn handle_identity(&self) -> HandleIdentity {
        match self {
            OpenIdentity::Credentialless => HandleIdentity::Credentialless,
            OpenIdentity::ServicePrincipal { principal_id } => HandleIdentity::ServicePrincipal {
                principal_id: *principal_id,
            },
        }
    }
}

/// Per-handle access restriction (spec §2.2: "each handle may have its own
/// principal and read-only restriction").
///
/// Stage 1A issues read-write handles; the read-only restriction is enforced
/// once per-handle admission lands with Stage 1D sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HandleAccess {
    read_only: bool,
}

impl HandleAccess {
    /// A read-write handle (the Stage 1A default).
    pub fn read_write() -> Self {
        Self { read_only: false }
    }

    /// A read-only handle.
    pub fn read_only() -> Self {
        Self { read_only: true }
    }

    /// Whether this handle is restricted to reads.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

/// A lightweight caller-specific handle onto one shared [`DatabaseCore`]
/// (spec §10.1, S1A-001).
///
/// Obtained from [`crate::manager::DatabaseManager::open_shared`]. Cloning or
/// dropping a handle has no storage side effects; storage closes when the
/// last core reference drops or [`Self::shutdown`] runs.
pub struct DatabaseHandle {
    /// The facade carrying this handle's auth context (principal-less for
    /// Stage 1A attaches) over the shared core. `Deref` exposes its full API.
    database: Database,
    identity: HandleIdentity,
    access: HandleAccess,
}

impl DatabaseHandle {
    pub(crate) fn new(database: Database, identity: HandleIdentity, access: HandleAccess) -> Self {
        Self {
            database,
            identity,
            access,
        }
    }

    /// The identity bound to this handle.
    pub fn identity(&self) -> &HandleIdentity {
        &self.identity
    }

    /// The access restriction bound to this handle.
    pub fn access(&self) -> HandleAccess {
        self.access
    }

    /// The shared storage core behind this handle. Two handles over the same
    /// root return `Arc`s to the *same* core.
    pub fn core(&self) -> Arc<DatabaseCore> {
        self.database.core()
    }

    /// The current lifecycle state of the shared core (S1A-004).
    pub fn lifecycle_state(&self) -> LifecycleState {
        self.database.lifecycle_state()
    }

    /// Admit one operation against the shared core (S1A-004). The RAII guard
    /// releases the operation slot on drop; new operations are rejected once
    /// the core leaves [`LifecycleState::Open`].
    pub fn operation_guard(&self) -> Result<OperationGuard> {
        self.database.operation_guard()
    }

    /// Shut the shared core down (spec §10.1, S1A-004): drain in-flight
    /// operations within `drain_deadline`, sync durable state, release the
    /// file lock, and mark the core `Closed`. Every handle — including this
    /// one — then rejects further operations. Dropping a handle never closes
    /// storage; only this method or the last core drop does.
    pub fn shutdown(&self, drain_deadline: Duration) -> Result<()> {
        self.database.core().shutdown(drain_deadline)
    }
}

impl std::ops::Deref for DatabaseHandle {
    type Target = Database;

    fn deref(&self) -> &Database {
        &self.database
    }
}

impl std::fmt::Debug for DatabaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseHandle")
            .field("identity", &self.identity)
            .field("access", &self.access)
            .field("database", &self.database)
            .finish()
    }
}
