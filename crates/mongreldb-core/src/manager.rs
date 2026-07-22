//! Process-local shared-core registry: `DatabaseManager` (spec §10.1,
//! S1A-002/S1A-003).
//!
//! One process owns one storage core per durable root (spec §4.1). The
//! manager keys cores by their stable [`DatabaseFileIdentity`] — never by
//! path text — and hands out lightweight [`DatabaseHandle`]s that all
//! reference the same `Arc<DatabaseCore>`. Recovery, WAL opening,
//! open-generation advancement, and table mounting happen exactly once, on
//! the first `open_shared`; concurrent attaches rendezvous on an
//! [`OpenWaitCell`] instead of racing a second open.
//!
//! Exclusivity is enforced both ways (spec §2.6 applies to independent
//! writers only): a shared core holds the same `ExclusiveDatabaseLease` an
//! exclusive `Database::open` would take, so `Database::open` on the same
//! root is rejected while shared handles exist — and `open_shared` fails the
//! same way while an exclusive owner holds the root. Dropping one handle
//! never closes storage; the last drop closes it (the lease and the file lock
//! release with the core), and a stale `Weak` is re-initialized on the next
//! attach.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, Weak};

use parking_lot::{Condvar, Mutex};

use crate::core::DatabaseFileIdentity;
use crate::database::{Database, DatabaseCore};
use crate::error::Result;
use crate::handle::{DatabaseHandle, HandleAccess, OpenIdentity};

/// The process-local shared-core registry (spec §10.1, S1A-002).
pub struct DatabaseManager {
    entries: Mutex<HashMap<DatabaseFileIdentity, CoreEntry>>,
}

/// One registry slot per durable root (spec §10.1, S1A-002).
enum CoreEntry {
    /// Initialization is in progress on another thread; waiters block on the
    /// cell and re-check the map when it fires.
    Opening(OpenWaitCell),
    /// The core is initialized. The registry never keeps a core alive — the
    /// handles do; when the last one drops the `Weak` goes stale and the next
    /// attach re-initializes.
    Open(Weak<DatabaseCore>),
    /// `shutdown()` has begun on this core (S1A-004). Transient: the shutdown
    /// removes the entry once the file lock is released.
    Closing,
}

/// Rendezvous for concurrent `open_shared` calls: exactly one caller
/// initializes; the rest wait here and then re-read the registry.
#[derive(Clone)]
struct OpenWaitCell {
    inner: Arc<(Mutex<bool>, Condvar)>,
}

impl OpenWaitCell {
    fn new() -> Self {
        Self {
            inner: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn wait(&self) {
        let (lock, condvar) = &*self.inner;
        let mut ready = lock.lock();
        while !*ready {
            condvar.wait(&mut ready);
        }
    }

    fn fire(&self) {
        let (lock, condvar) = &*self.inner;
        *lock.lock() = true;
        condvar.notify_all();
    }
}

/// Removes a still-`Opening` entry and wakes its waiters if initialization
/// ends without installing a core (failure or panic), so attachers never
/// block forever.
struct OpeningGuard<'a> {
    manager: &'a DatabaseManager,
    identity: DatabaseFileIdentity,
    cell: OpenWaitCell,
    armed: bool,
}

impl OpeningGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for OpeningGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut entries = self.manager.entries.lock();
        if entries.get(&self.identity).is_some_and(
            |entry| matches!(entry, CoreEntry::Opening(cell) if cell.same_as(&self.cell)),
        ) {
            entries.remove(&self.identity);
        }
        drop(entries);
        self.cell.fire();
    }
}

impl OpenWaitCell {
    fn same_as(&self, other: &OpenWaitCell) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// Back-link installed on a manager-initialized core so `shutdown()` can move
/// the registry entry through `Closing` to removal (S1A-004).
pub(crate) struct CoreRegistration {
    pub identity: DatabaseFileIdentity,
    pub manager: &'static DatabaseManager,
}

impl DatabaseManager {
    /// The process-global registry (spec §10.1, S1A-002).
    pub fn global() -> &'static DatabaseManager {
        static MANAGER: OnceLock<DatabaseManager> = OnceLock::new();
        MANAGER.get_or_init(|| DatabaseManager {
            entries: Mutex::new(HashMap::new()),
        })
    }

    /// Attach to the one shared core for `root`, initializing it on first use
    /// (spec §10.1, S1A-002).
    ///
    /// Concurrent attaches initialize exactly once: the first caller runs the
    /// full open path (recovery, WAL opening, open-generation advancement,
    /// table mounting) while the rest wait, then every caller receives a
    /// handle over the same core. Fails with `DatabaseLocked` while an
    /// exclusive `Database` owner holds the root, and vice versa.
    pub fn open_shared(
        self: &'static DatabaseManager,
        root: impl AsRef<std::path::Path>,
        identity: OpenIdentity,
    ) -> Result<DatabaseHandle> {
        self.open_shared_with_access(root, identity, HandleAccess::read_write())
    }

    /// Attach with an explicit per-handle read-only or read-write restriction.
    pub fn open_shared_with_access(
        self: &'static DatabaseManager,
        root: impl AsRef<std::path::Path>,
        identity: OpenIdentity,
        access: HandleAccess,
    ) -> Result<DatabaseHandle> {
        let identity_key = DatabaseFileIdentity::for_path(root.as_ref())?;
        let core = loop {
            let mut entries = self.entries.lock();
            match entries.get(&identity_key) {
                Some(CoreEntry::Open(weak)) => {
                    if let Some(core) = weak.upgrade() {
                        if core.is_open() {
                            break core;
                        }
                        // A shutdown transitioned this core out of `Open`; the
                        // entry is on its way out. If the lock is still held
                        // the re-open below fails fast with `DatabaseLocked`.
                        entries.remove(&identity_key);
                    } else {
                        // Stale weak: the last handle dropped and storage
                        // closed with it. Re-initialize.
                        entries.remove(&identity_key);
                    }
                }
                Some(CoreEntry::Opening(cell)) => {
                    let cell = cell.clone();
                    drop(entries);
                    cell.wait();
                }
                Some(CoreEntry::Closing) => {
                    entries.remove(&identity_key);
                }
                None => {
                    let cell = OpenWaitCell::new();
                    entries.insert(identity_key.clone(), CoreEntry::Opening(cell.clone()));
                    drop(entries);
                    let mut guard = OpeningGuard {
                        manager: self,
                        identity: identity_key.clone(),
                        cell: cell.clone(),
                        armed: true,
                    };
                    // The one initialization for this core: recovery, WAL
                    // opening, open-generation advancement, table mounting.
                    let core = Database::open_for_shared_core(root.as_ref())?.into_core();
                    core.set_registry(CoreRegistration {
                        identity: identity_key.clone(),
                        manager: self,
                    })?;
                    self.entries
                        .lock()
                        .insert(identity_key.clone(), CoreEntry::Open(Arc::downgrade(&core)));
                    guard.disarm();
                    cell.fire();
                    break core;
                }
            }
        };
        let unbound = Database::from_core(Arc::clone(&core), None, true);
        let (handle_identity, principal) = match identity {
            OpenIdentity::Credentialless => {
                if unbound.require_auth_enabled() {
                    return Err(crate::error::MongrelError::AuthRequired);
                }
                (crate::handle::HandleIdentity::Credentialless, None)
            }
            OpenIdentity::ServiceCredentials { token_id, secret } => {
                // Authority comes only from the shared store after secret
                // verification (P0.1). Callers cannot supply permissions.
                let def = core
                    .service_principals
                    .authenticate(&token_id, secret.expose())?;
                let principal = service_principal_from_def(&def);
                (
                    crate::handle::HandleIdentity::ServicePrincipal {
                        token_id: def.token_id,
                        principal_id: def.principal_id,
                        creation_version: def.creation_version,
                    },
                    Some(principal),
                )
            }
            OpenIdentity::CatalogCredentials { username, password } => {
                if !unbound.require_auth_enabled() {
                    return Err(crate::error::MongrelError::AuthNotRequired);
                }
                let principal = unbound
                    .authenticate_principal(&username, password.expose())?
                    .ok_or_else(|| crate::error::MongrelError::InvalidCredentials {
                        username: username.clone(),
                    })?;
                let identity = crate::handle::HandleIdentity::CatalogUser {
                    username: principal.username.clone(),
                    user_id: principal.user_id,
                    created_version: principal.created_epoch,
                };
                (identity, Some(principal))
            }
        };
        let facade = Database::from_core(core, principal, true);
        Ok(DatabaseHandle::new(facade, handle_identity, access, None))
    }

    /// Number of registry entries (all states). Diagnostics and tests.
    pub fn registered_entries(&self) -> usize {
        self.entries.lock().len()
    }

    /// S1A-004 shutdown step: move the entry for `core` to `Closing` so new
    /// attaches do not rendezvous on a core that is going away.
    pub(crate) fn mark_closing(&self, identity: &DatabaseFileIdentity, core: &Arc<DatabaseCore>) {
        let mut entries = self.entries.lock();
        if entries.get(identity).is_some_and(
            |entry| matches!(entry, CoreEntry::Open(weak) if weak.as_ptr() == Arc::as_ptr(core)),
        ) {
            entries.insert(identity.clone(), CoreEntry::Closing);
        }
    }

    /// S1A-004 shutdown step: remove the entry for `core` once the file lock
    /// is released. The next attach re-initializes a fresh core.
    pub(crate) fn entry_closed(&self, identity: &DatabaseFileIdentity, core: &Arc<DatabaseCore>) {
        let mut entries = self.entries.lock();
        let owned = match entries.get(identity) {
            Some(CoreEntry::Closing) => true,
            Some(CoreEntry::Open(weak)) => weak.as_ptr() == Arc::as_ptr(core),
            _ => false,
        };
        if owned {
            entries.remove(identity);
        }
    }
}

fn service_principal_from_def(
    def: &crate::service_principal::ServicePrincipalDefinition,
) -> crate::auth::Principal {
    crate::auth::Principal {
        user_id: 0,
        created_epoch: def.creation_version,
        username: format!("service:{}", def.token_id),
        is_admin: false,
        roles: Vec::new(),
        permissions: def.permissions.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_cell_wakes_all_waiters() {
        let cell = OpenWaitCell::new();
        let waiter = {
            let cell = cell.clone();
            std::thread::spawn(move || cell.wait())
        };
        std::thread::sleep(std::time::Duration::from_millis(10));
        cell.fire();
        waiter.join().unwrap();
        // Fired cells return immediately.
        cell.wait();
    }

    #[test]
    fn global_is_a_singleton() {
        assert!(std::ptr::eq(
            DatabaseManager::global(),
            DatabaseManager::global()
        ));
    }
}
