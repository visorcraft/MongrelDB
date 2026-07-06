//! Shared auth state for per-table enforcement.
//!
//! The [`AuthState`] struct bundles the `require_auth` flag (read from the
//! catalog) and the cached principal into a single `Arc`-clonable handle.
//! This handle is cloned into every mounted [`crate::engine::Table`] so the
//! Table layer can enforce `Select`/`Insert`/`Update`/`Delete` permissions
//! without holding a reference back to `Database` (which would create a
//! reference cycle, since `Database` owns the `Arc<Mutex<Table>>` handles).
//!
//! The design is intentionally extensible: the [`TableAuthChecker`] trait
//! lets the daemon (or any future multi-tenant layer) provide its own
//! principal source — e.g. one that reads from per-request state — while the
//! embedded default checker reads the `Database`'s cached principal. See
//! `docs/15-credential-enforcement.md` §4.3.

use crate::auth::Principal;
use crate::error::{MongrelError, Result};
use parking_lot::RwLock;
use std::sync::Arc;

/// A cloneable snapshot of the auth state shared between `Database` and every
/// mounted `Table`. Cloning is cheap (one `Arc` bump).
///
/// The `require_auth` flag is read live from the catalog on every check, so
/// `enable_auth` / offline-disable are reflected immediately. The principal
/// is the open-time cached value; daemons that need per-request principals
/// should swap in their own [`TableAuthChecker`].
#[derive(Clone, Debug)]
pub struct AuthState {
    inner: Arc<AuthStateInner>,
}

#[derive(Debug)]
struct AuthStateInner {
    /// The require_auth flag, read live from the catalog. This is a `RwLock<bool>`
    /// mirror of `Catalog::require_auth` — kept here so the Table layer can
    /// read it without acquiring the full catalog lock on every operation.
    /// Updated by `Database` whenever the catalog's flag changes.
    require_auth: RwLock<bool>,
    /// The cached principal for this handle. `None` on credentialless
    /// databases.
    principal: RwLock<Option<Principal>>,
}

impl AuthState {
    /// Create a new auth state. `require_auth` is the initial catalog value;
    /// `principal` is the cached principal (if any).
    pub fn new(require_auth: bool, principal: Option<Principal>) -> Self {
        Self {
            inner: Arc::new(AuthStateInner {
                require_auth: RwLock::new(require_auth),
                principal: RwLock::new(principal),
            }),
        }
    }

    /// Create a credentialless auth state (no enforcement).
    pub fn disabled() -> Self {
        Self::new(false, None)
    }

    /// Whether enforcement is currently active.
    pub fn require_auth(&self) -> bool {
        *self.inner.require_auth.read()
    }

    /// Update the `require_auth` flag. Called by `Database` when the catalog
    /// flag changes (e.g. `enable_auth`).
    pub fn set_require_auth(&self, value: bool) {
        *self.inner.require_auth.write() = value;
    }

    /// A clone of the cached principal, if any.
    pub fn principal(&self) -> Option<Principal> {
        self.inner.principal.read().clone()
    }

    /// Replace the cached principal. Called by `Database::open_with_credentials`,
    /// `enable_auth`, and `refresh_principal`.
    pub fn set_principal(&self, principal: Option<Principal>) {
        *self.inner.principal.write() = principal;
    }

    /// Enforcement check: if `require_auth` is true, verify the cached
    /// principal satisfies `perm` for `table_name`. On credentialless
    /// databases, this is a no-op (`Ok(())`).
    ///
    /// This is the primary entry point called by `Table` and `Transaction`
    /// enforcement points.
    pub fn require(&self, table_name: &str, perm: RequiredPermission) -> Result<()> {
        if !self.require_auth() {
            return Ok(());
        }
        let guard = self.inner.principal.read();
        let p = guard.as_ref().ok_or(MongrelError::AuthRequired)?;
        let required = perm.into_permission(table_name);
        if p.has_permission(&required) {
            Ok(())
        } else {
            Err(MongrelError::PermissionDenied {
                required,
                principal: p.username.clone(),
            })
        }
    }
}

/// The permission "shape" needed for a table operation. This is separate
/// from [`crate::auth::Permission`] because at the call site we know the
/// operation kind (read/insert/update/delete) and the table name, but
/// constructing the full `Permission` requires allocating a `String` for the
/// table name. `RequiredPermission` is a lightweight enum that defers the
/// allocation to `into_permission`, which is only called when enforcement is
/// actually active (i.e. `require_auth` is true).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredPermission {
    /// `SELECT` on the table.
    Select,
    /// `INSERT` on the table.
    Insert,
    /// `UPDATE` on the table.
    Update,
    /// `DELETE` on the table.
    Delete,
}

impl RequiredPermission {
    /// Construct the full [`crate::auth::Permission`] for this kind, allocating
    /// the table-name `String`.
    pub fn into_permission(self, table_name: &str) -> crate::auth::Permission {
        use crate::auth::Permission;
        let table = table_name.to_string();
        match self {
            RequiredPermission::Select => Permission::Select { table },
            RequiredPermission::Insert => Permission::Insert { table },
            RequiredPermission::Update => Permission::Update { table },
            RequiredPermission::Delete => Permission::Delete { table },
        }
    }
}

/// Extensible auth-checker trait for the Table layer. The default
/// implementation (used by embedded/CLI) delegates to [`AuthState`]. The
/// daemon can provide its own implementation that reads the principal from
/// per-request state, enabling per-user enforcement on a shared `Database`.
///
/// This is deliberately a `Fn`-style trait (not `FnMut`) so it can be shared
/// across threads via `Arc<dyn TableAuthChecker>`.
pub trait TableAuthChecker: Send + Sync + std::fmt::Debug {
    /// Check whether the current principal has `perm` on `table_name`.
    /// Returns `Ok(())` if allowed, `Err(MongrelError::PermissionDenied)`
    /// (or `AuthRequired`) if not.
    fn check(&self, table_name: &str, perm: RequiredPermission) -> Result<()>;
}

/// The default auth checker — delegates to a shared [`AuthState`]. Used by
/// embedded/CLI/programmatic opens where the principal is the open-time
/// cached value.
#[derive(Debug, Clone)]
pub struct DefaultTableAuthChecker {
    state: AuthState,
}

impl DefaultTableAuthChecker {
    pub fn new(state: AuthState) -> Self {
        Self { state }
    }
}

impl TableAuthChecker for DefaultTableAuthChecker {
    fn check(&self, table_name: &str, perm: RequiredPermission) -> Result<()> {
        self.state.require(table_name, perm)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Permission;

    #[test]
    fn disabled_state_is_noop() {
        let state = AuthState::disabled();
        assert!(!state.require_auth());
        state.require("orders", RequiredPermission::Select).unwrap();
        state.require("orders", RequiredPermission::Insert).unwrap();
    }

    #[test]
    fn enabled_state_with_no_principal_is_auth_required() {
        let state = AuthState::new(true, None);
        assert!(state.require_auth());
        let err = state
            .require("orders", RequiredPermission::Select)
            .unwrap_err();
        assert!(matches!(err, MongrelError::AuthRequired));
    }

    #[test]
    fn enabled_state_denies_without_permission() {
        let principal = Principal {
            username: "alice".into(),
            is_admin: false,
            roles: vec![],
            permissions: vec![Permission::Select {
                table: "orders".into(),
            }],
        };
        let state = AuthState::new(true, Some(principal));
        // Select on orders → ok.
        state.require("orders", RequiredPermission::Select).unwrap();
        // Insert on orders → denied.
        let err = state
            .require("orders", RequiredPermission::Insert)
            .unwrap_err();
        assert!(matches!(err, MongrelError::PermissionDenied { .. }));
    }

    #[test]
    fn enabled_state_admin_bypasses() {
        let principal = Principal {
            username: "admin".into(),
            is_admin: true,
            roles: vec![],
            permissions: vec![],
        };
        let state = AuthState::new(true, Some(principal));
        state.require("orders", RequiredPermission::Select).unwrap();
        state.require("orders", RequiredPermission::Insert).unwrap();
        state.require("orders", RequiredPermission::Delete).unwrap();
    }

    #[test]
    fn set_require_auth_propagates_to_clones() {
        let state = AuthState::disabled();
        let clone = state.clone();
        assert!(!clone.require_auth());
        state.set_require_auth(true);
        assert!(clone.require_auth(), "clone sees the live flag");
    }

    #[test]
    fn default_checker_delegates_to_state() {
        let principal = Principal {
            username: "alice".into(),
            is_admin: false,
            roles: vec![],
            permissions: vec![Permission::Insert {
                table: "orders".into(),
            }],
        };
        let checker = DefaultTableAuthChecker::new(AuthState::new(true, Some(principal)));
        checker.check("orders", RequiredPermission::Insert).unwrap();
        let err = checker
            .check("orders", RequiredPermission::Select)
            .unwrap_err();
        assert!(matches!(err, MongrelError::PermissionDenied { .. }));
    }
}
