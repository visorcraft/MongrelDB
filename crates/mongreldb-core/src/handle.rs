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
//! Handles expose only principal-aware operations. They never dereference to
//! the raw [`Database`] facade or return mutable table handles.

use std::sync::Arc;
use std::time::Duration;

use crate::core::{LifecycleState, OperationGuard};
use crate::database::Database;
use crate::error::{MongrelError, Result};

/// A password that zeroizes its allocation and never reveals itself through
/// `Debug` output.
pub struct SecretString(zeroize::Zeroizing<String>);

impl SecretString {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(zeroize::Zeroizing::new(secret.into()))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString([REDACTED])")
    }
}

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
/// Catalog credentials are verified against the live shared catalog before a
/// handle is issued. The password allocation is zeroized when attach returns.
#[derive(Debug)]
pub enum OpenIdentity {
    /// Attach without credentials. Rejected (fail closed) when the database
    /// catalog has `require_auth` enabled.
    Credentialless,
    /// Attach as a non-catalog service principal.
    ServicePrincipal { principal_id: [u8; 16] },
    /// Attach as a service principal restricted to these exact permissions.
    ScopedServicePrincipal {
        principal_id: [u8; 16],
        permissions: Vec<crate::auth::Permission>,
    },
    /// Authenticate one catalog user on the existing shared core.
    CatalogCredentials {
        username: String,
        password: SecretString,
    },
}

/// Per-handle access restriction (spec §2.2: "each handle may have its own
/// principal and read-only restriction").
///
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
    /// The facade carrying this handle's live authorization context.
    database: Database,
    identity: HandleIdentity,
    access: HandleAccess,
    service_permissions: Option<Vec<crate::auth::Permission>>,
}

impl DatabaseHandle {
    pub(crate) fn new(
        database: Database,
        identity: HandleIdentity,
        access: HandleAccess,
        service_permissions: Option<Vec<crate::auth::Permission>>,
    ) -> Self {
        Self {
            database,
            identity,
            access,
            service_permissions,
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

    /// Whether two handles reference the exact same process-local core.
    pub fn shares_core_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.database.core(), &other.database.core())
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

    fn authorize_write(&self, operation: &'static str) -> Result<()> {
        if self.access.is_read_only() {
            Err(MongrelError::ReadOnlyHandle { operation })
        } else {
            Ok(())
        }
    }

    fn authorize_permission(&self, permission: &crate::auth::Permission) -> Result<()> {
        if let Some(permissions) = &self.service_permissions {
            if !permissions
                .iter()
                .any(|granted| granted.satisfies(permission))
            {
                return Err(MongrelError::PermissionDenied {
                    required: permission.clone(),
                    principal: match &self.identity {
                        HandleIdentity::ServicePrincipal { principal_id } => {
                            format!("service:{principal_id:02x?}")
                        }
                        _ => "service".into(),
                    },
                });
            }
        }
        self.database.require(permission)
    }

    /// Execute an authorized native query with live roles, RLS, masks, and
    /// column grants.
    pub fn query(
        &self,
        table: &str,
        query: &crate::query::Query,
        projection: Option<&[u16]>,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.database
            .query_for_current_principal(table, query, projection)
    }

    /// Count rows visible to this handle after live authorization and RLS.
    pub fn count(&self, table: &str) -> Result<u64> {
        self.database
            .count_for(table, self.database.principal().as_ref())
    }

    /// Return all rows visible to this handle after RLS and masks.
    pub fn rows(&self, table: &str) -> Result<Vec<crate::memtable::Row>> {
        self.database
            .rows_for(table, self.database.principal().as_ref())
    }

    /// Create a table through this handle's live principal.
    pub fn create_table(&self, name: &str, schema: crate::schema::Schema) -> Result<u64> {
        self.authorize_write("create table")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.database.create_table(name, schema)
    }

    /// Atomically insert one row through this handle's live principal.
    pub fn put(
        &self,
        table: &str,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<Option<i64>> {
        self.authorize_write("put")?;
        self.database
            .transaction_for_current_principal(|transaction| transaction.put(table, cells))
    }

    /// Atomically delete one row through this handle's live principal.
    pub fn delete(&self, table: &str, row_id: crate::RowId) -> Result<()> {
        self.authorize_write("delete")?;
        self.database
            .transaction_for_current_principal(|transaction| transaction.delete(table, row_id))
    }

    /// Create a catalog user. Admin only.
    pub fn create_user(&self, username: &str, password: &str) -> Result<crate::auth::UserEntry> {
        self.authorize_write("create user")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.create_user(username, password)
    }

    /// Drop a catalog user. Admin only.
    pub fn drop_user(&self, username: &str) -> Result<()> {
        self.authorize_write("drop user")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.drop_user(username)
    }

    /// Create a role. Admin only.
    pub fn create_role(&self, role: &str) -> Result<crate::auth::RoleEntry> {
        self.authorize_write("create role")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.create_role(role)
    }

    /// Grant a role to a catalog user. Admin only.
    pub fn grant_role(&self, username: &str, role: &str) -> Result<()> {
        self.authorize_write("grant role")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.grant_role(username, role)
    }

    /// Revoke a role from a catalog user. Admin only.
    pub fn revoke_role(&self, username: &str, role: &str) -> Result<()> {
        self.authorize_write("revoke role")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.revoke_role(username, role)
    }

    /// Grant one permission to a role. Admin only.
    pub fn grant_permission(&self, role: &str, permission: crate::auth::Permission) -> Result<()> {
        self.authorize_write("grant permission")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.grant_permission(role, permission)
    }

    /// Revoke one permission from a role. Admin only.
    pub fn revoke_permission(&self, role: &str, permission: crate::auth::Permission) -> Result<()> {
        self.authorize_write("revoke permission")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.revoke_permission(role, permission)
    }

    /// Shut the shared core down (spec §10.1, S1A-004): drain in-flight
    /// operations within `drain_deadline`, sync durable state, release the
    /// file lock, and mark the core `Closed`. Every handle — including this
    /// one — then rejects further operations. Dropping a handle never closes
    /// storage; only this method or the last core drop does.
    pub fn shutdown(&self, drain_deadline: Duration) -> Result<()> {
        self.authorize_write("shutdown")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.core().shutdown(drain_deadline)
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
