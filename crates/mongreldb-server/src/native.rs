//! Production native RPC adapters over the same database, sessions, and SQL
//! query registry used by the HTTP server.

use std::collections::HashMap;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::ipc::writer::StreamWriter;
use futures::{Stream, StreamExt};
use mongreldb_core::{
    Database, JwksCache, JwtValidationConfig, MongrelError, Principal, ServiceToken,
    ServiceTokenRegistry,
};
use mongreldb_protocol::native;
use mongreldb_protocol::request::AuthenticatedIdentity;
use mongreldb_protocol::validate_native_context;
use mongreldb_query::{
    CancelOutcome, ManagedQueryBatches, MongrelQueryError, MongrelSession, QueryId,
    SqlQueryOptions, SqlQueryPhase, SqlQueryRegistry,
};
use mongreldb_types::errors::{ErrorCategory, RetryClass};
use mongreldb_types::ids::TransactionId;
use prost::Message;
use tonic::{Code, Request, Response, Status};

use crate::prepared;
use crate::sessions::{SessionEntry, SessionStore};

const AUTH_TOKEN_TTL: Duration = Duration::from_secs(300);
const MAX_AUTH_TOKENS: usize = 4_096;

enum NativeIdempotency {
    None,
    Execute(crate::sql_idempotency::SqlIdempotencyExecution),
    Replay(crate::sql_idempotency::SqlDurableReceipt),
}

#[derive(Clone)]
struct AuthGrant {
    principal: Option<Principal>,
    identity: AuthenticatedIdentity,
    expires_unix_micros: u64,
}

struct PendingScram {
    username: String,
    session: mongreldb_core::ScramServerSession,
    fake_user: bool,
    expires_unix_micros: u64,
}

#[derive(Clone)]
struct NativeOidc {
    validation: JwtValidationConfig,
    cache: Arc<JwksCache<crate::oidc::HttpsJwksProvider>>,
}

/// Native bearer authentication backed by hardened core verifiers.
#[derive(Clone, Default)]
pub struct NativeExternalAuth {
    service_tokens: Arc<RwLock<ServiceTokenRegistry>>,
    oidc: Option<NativeOidc>,
}

impl NativeExternalAuth {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn upsert_service_token(&self, token: ServiceToken) -> Result<(), MongrelError> {
        self.service_tokens
            .write()
            .map_err(|_| MongrelError::Other("service-token registry poisoned".into()))?
            .upsert(token);
        Ok(())
    }

    pub fn with_oidc(
        mut self,
        validation: JwtValidationConfig,
        provider: crate::oidc::HttpsJwksProvider,
    ) -> Self {
        self.oidc = Some(NativeOidc {
            validation,
            cache: Arc::new(JwksCache::new(provider)),
        });
        self
    }
}

/// All seven native services backed by the canonical server runtime.
#[derive(Clone)]
pub struct NativeRuntime {
    db: Arc<Database>,
    sessions: Arc<SessionStore>,
    query_registry: Arc<SqlQueryRegistry>,
    sql_idempotency: Option<Arc<crate::sql_idempotency::SqlIdempotencyStore>>,
    auth_grants: Arc<Mutex<HashMap<Vec<u8>, AuthGrant>>>,
    scram_exchanges: Arc<Mutex<HashMap<Vec<u8>, PendingScram>>>,
    external_auth: Option<NativeExternalAuth>,
}

impl NativeRuntime {
    pub fn new(
        db: Arc<Database>,
        sessions: Arc<SessionStore>,
        query_registry: Arc<SqlQueryRegistry>,
    ) -> Self {
        Self {
            db,
            sessions,
            query_registry,
            sql_idempotency: None,
            auth_grants: Arc::new(Mutex::new(HashMap::new())),
            scram_exchanges: Arc::new(Mutex::new(HashMap::new())),
            external_auth: None,
        }
    }

    pub(crate) fn with_sql_idempotency(
        mut self,
        store: Arc<crate::sql_idempotency::SqlIdempotencyStore>,
    ) -> Self {
        self.sql_idempotency = Some(store);
        self
    }

    pub fn with_external_auth(mut self, auth: NativeExternalAuth) -> Self {
        self.external_auth = Some(auth);
        self
    }

    fn issue_auth_grant(
        &self,
        principal: Option<Principal>,
        identity: AuthenticatedIdentity,
    ) -> Result<Vec<u8>, Status> {
        let token = mongreldb_types::ids::QueryId::new_random()
            .as_bytes()
            .to_vec();
        let now = now_unix_micros();
        let mut grants = self
            .auth_grants
            .lock()
            .map_err(|_| Status::internal("auth grant store poisoned"))?;
        grants.retain(|_, grant| grant.expires_unix_micros > now);
        if grants.len() >= MAX_AUTH_TOKENS {
            return Err(Status::resource_exhausted(
                "too many pending authentications",
            ));
        }
        grants.insert(
            token.clone(),
            AuthGrant {
                principal,
                identity,
                expires_unix_micros: now.saturating_add(AUTH_TOKEN_TTL.as_micros() as u64),
            },
        );
        Ok(token)
    }

    fn consume_auth_grant(&self, token: &[u8]) -> Result<AuthGrant, Status> {
        let grant = self
            .auth_grants
            .lock()
            .map_err(|_| Status::internal("auth grant store poisoned"))?
            .remove(token)
            .ok_or_else(|| Status::unauthenticated("invalid or expired auth token"))?;
        if grant.expires_unix_micros <= now_unix_micros() {
            return Err(Status::unauthenticated("invalid or expired auth token"));
        }
        if let Some(principal) = &grant.principal {
            let current = self
                .db
                .resolve_current_principal(principal)
                .ok_or_else(|| Status::unauthenticated("principal was revoked"))?;
            if current.user_id != principal.user_id
                || current.created_epoch != principal.created_epoch
            {
                return Err(Status::unauthenticated("principal was revoked"));
            }
        }
        Ok(grant)
    }

    fn session(
        &self,
        bytes: &[u8],
        required_scope: &str,
    ) -> Result<(String, Arc<SessionEntry>), Status> {
        let token = id_hex(bytes, "session id")?;
        let entry = self
            .sessions
            .get_by_token(&token)
            .ok_or_else(|| Status::not_found("session not found"))?;
        if let AuthenticatedIdentity::ExternalPrincipal { scopes, .. } =
            &entry.protocol_record().principal
        {
            if !scopes
                .iter()
                .any(|scope| scope == "*" || scope == required_scope)
            {
                return Err(Status::permission_denied(format!(
                    "service principal lacks {required_scope} scope"
                )));
            }
        }
        Ok((token, entry))
    }

    fn session_principal(&self, entry: &SessionEntry) -> Result<Option<Principal>, Status> {
        match entry.protocol_record().principal {
            AuthenticatedIdentity::Credentialless => Ok(None),
            AuthenticatedIdentity::CatalogUser {
                username,
                user_id,
                created_version,
            } => {
                let principal = self
                    .db
                    .resolve_principal(&username)
                    .ok_or_else(|| Status::unauthenticated("principal was revoked"))?;
                if principal.user_id != user_id || principal.created_epoch != created_version {
                    return Err(Status::unauthenticated("principal was revoked"));
                }
                Ok(Some(principal))
            }
            AuthenticatedIdentity::ServicePrincipal { .. } => Err(Status::permission_denied(
                "internal service principal cannot use native catalog",
            )),
            AuthenticatedIdentity::ExternalPrincipal {
                username,
                user_id,
                created_version,
                ..
            } => {
                let principal = self
                    .db
                    .resolve_principal(&username)
                    .ok_or_else(|| Status::unauthenticated("principal was revoked"))?;
                if principal.user_id != user_id || principal.created_epoch != created_version {
                    return Err(Status::unauthenticated("principal was revoked"));
                }
                Ok(Some(principal))
            }
        }
    }

    async fn execute_sql(
        &self,
        entry: Arc<SessionEntry>,
        session_id: String,
        query_id: QueryId,
        sql: String,
        context: Option<&native::RequestContext>,
    ) -> Result<(Vec<u8>, ManagedQueryBatches), Status> {
        let options = SqlQueryOptions {
            query_id: Some(query_id),
            timeout: request_timeout(context)?,
            owner: Some(entry.owner.clone()),
            session_id: Some(session_id),
            parent_control: None,
        };
        let session = entry.session();
        let query = session.register_query(options).map_err(query_status)?;
        let batches = session
            .run_with_query_for_serialization(&sql, query)
            .await
            .map_err(query_status)?;
        Ok((query_id.as_bytes().to_vec(), batches))
    }

    async fn begin_idempotency(
        &self,
        request: &native::ExecuteRequest,
        entry: &SessionEntry,
        session_id: &str,
        sql: &str,
    ) -> Result<NativeIdempotency, Status> {
        let key = request
            .context
            .as_ref()
            .map(|context| context.idempotency_key.as_str())
            .unwrap_or_default();
        if key.is_empty() {
            return Ok(NativeIdempotency::None);
        }
        if !matches!(
            request.command,
            Some(native::execute_request::Command::Sql(_))
        ) {
            return Err(Status::invalid_argument(
                "idempotency key requires one direct SQL write",
            ));
        }
        if mongreldb_query::classify_sql_idempotency(sql)
            != mongreldb_query::SqlIdempotencyClass::SingleWrite
        {
            return Err(Status::invalid_argument(
                "idempotency key requires one direct SQL write",
            ));
        }
        crate::sql_idempotency::SqlIdempotencyStore::validate_key(key)
            .map_err(Status::invalid_argument)?;
        if entry.session().staged_sql_operation_count().is_some() {
            return Err(Status::failed_precondition(
                "idempotency is unavailable inside an open transaction",
            ));
        }
        let store = self.sql_idempotency.as_ref().ok_or_else(|| {
            Status::unavailable("durable native SQL idempotency is not configured")
        })?;
        let parameters = request
            .parameters
            .iter()
            .flatten()
            .copied()
            .collect::<Vec<_>>();
        let binding = crate::sql_idempotency::SqlIdempotencyBinding {
            sql_fingerprint: mongreldb_query::normalized_sql_fingerprint(sql),
            parameter_hash: crate::sql_idempotency::hash(&parameters),
            request_semantics_hash: crate::sql_idempotency::hash(b"native-arrow-ipc-v1"),
            session_semantics_hash: crate::sql_idempotency::hash(session_id.as_bytes()),
            expires_after_ms: store.expires_after_ms(),
        };
        match store.begin(&entry.owner, key, binding).await {
            crate::sql_idempotency::BeginResult::Execute(execution) => {
                Ok(NativeIdempotency::Execute(execution))
            }
            crate::sql_idempotency::BeginResult::Replay { receipt, .. } => {
                Ok(NativeIdempotency::Replay(receipt))
            }
            crate::sql_idempotency::BeginResult::Mismatch => Err(structured_status(
                Code::AlreadyExists,
                ErrorCategory::TransactionConflict,
                "idempotency key was reused with different request semantics",
            )),
            crate::sql_idempotency::BeginResult::Indeterminate { .. } => Err(structured_status(
                Code::Unknown,
                ErrorCategory::CommitOutcomeUnknown,
                "idempotency outcome is indeterminate",
            )),
            crate::sql_idempotency::BeginResult::Full => Err(structured_status(
                Code::ResourceExhausted,
                ErrorCategory::ResourceExhausted,
                "idempotency store is full",
            )),
            crate::sql_idempotency::BeginResult::Unavailable(_reason) => Err(structured_status(
                Code::Unavailable,
                ErrorCategory::ReplicaUnavailable,
                "idempotency store is unavailable",
            )),
        }
    }

    fn finish_idempotency(
        &self,
        execution: crate::sql_idempotency::SqlIdempotencyExecution,
        query_id: QueryId,
    ) {
        let Some(status) = self.query_registry.status(query_id) else {
            return;
        };
        if let Some(mut receipt) = super::sql_terminal_idempotency_receipt(&status) {
            receipt.commit_receipt = crate::sql_idempotency::record_core_idempotency_commit(
                &self.db,
                execution.owner(),
                execution.key(),
                execution.binding(),
                execution.ttl(),
            );
            execution.commit(receipt);
        } else if super::can_abort_idempotency_intent(&status) {
            execution.abort();
        }
    }

    async fn resolve_sql(
        &self,
        entry: &SessionEntry,
        request: &native::ExecuteRequest,
    ) -> Result<String, Status> {
        use native::execute_request::Command;
        match &request.command {
            Some(Command::Sql(sql)) => Ok(sql.clone()),
            Some(Command::PreparedStatementId(id)) => {
                let (_, binding) = entry
                    .prepared_binding_by_id(*id)
                    .ok_or_else(|| Status::not_found("prepared statement not found"))?;
                if !prepared::CatalogState::capture(&self.db).is_compatible(&binding) {
                    return Err(structured_status(
                        Code::FailedPrecondition,
                        ErrorCategory::SchemaVersionMismatch,
                        "prepared statement schema changed",
                    ));
                }
                let values = request
                    .parameters
                    .iter()
                    .map(|encoded| {
                        bincode::deserialize::<mongreldb_protocol::request::ParameterValue>(encoded)
                            .map_err(|_| Status::invalid_argument("invalid prepared parameter"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let literals = values
                    .iter()
                    .map(parameter_literal)
                    .collect::<Result<Vec<_>, _>>()?;
                bind_numbered_parameters(&binding.sql, &literals)
            }
            Some(Command::AdminCommand(command)) => String::from_utf8(command.clone())
                .map_err(|_| Status::invalid_argument("admin command must be UTF-8 SQL")),
            None => Err(Status::invalid_argument("execute command is required")),
        }
    }
}

#[tonic::async_trait]
impl native::auth_service_server::AuthService for NativeRuntime {
    async fn authenticate(
        &self,
        request: Request<native::AuthenticateRequest>,
    ) -> Result<Response<native::AuthenticateResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (principal, identity, response_identity) = match request.credential {
            Some(native::authenticate_request::Credential::Password(password)) => {
                let username = password.username;
                let password = zeroize::Zeroizing::new(password.password);
                let principal = self
                    .db
                    .authenticate_principal(&username, password.as_str())
                    .map_err(core_status)?
                    .ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
                let identity = AuthenticatedIdentity::CatalogUser {
                    username: principal.username.clone(),
                    user_id: principal.user_id,
                    created_version: principal.created_epoch,
                };
                let response_identity = native_identity(Some(&principal));
                (Some(principal), identity, response_identity)
            }
            Some(native::authenticate_request::Credential::MysqlCachingSha2(credential)) => {
                if credential.nonce.len() != 20
                    || (!credential.proof.is_empty() && credential.proof.len() != 32)
                {
                    return Err(Status::invalid_argument(
                        "invalid caching_sha2_password proof",
                    ));
                }
                let principal = self
                    .db
                    .authenticate_mysql_caching_sha2_principal(
                        &credential.username,
                        &credential.nonce,
                        &credential.proof,
                    )
                    .ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
                let identity = AuthenticatedIdentity::CatalogUser {
                    username: principal.username.clone(),
                    user_id: principal.user_id,
                    created_version: principal.created_epoch,
                };
                let response_identity = native_identity(Some(&principal));
                (Some(principal), identity, response_identity)
            }
            None if !self.db.require_auth_enabled() => (
                None,
                AuthenticatedIdentity::Credentialless,
                native_identity(None),
            ),
            Some(native::authenticate_request::Credential::ServiceToken(credential)) => {
                let auth = self.external_auth.as_ref().ok_or_else(|| {
                    Status::unauthenticated("service-token authentication is not configured")
                })?;
                let secret = zeroize::Zeroizing::new(credential.secret);
                let token = auth
                    .service_tokens
                    .read()
                    .map_err(|_| Status::internal("service-token registry poisoned"))?
                    .authenticate(&credential.token_id, secret.as_str(), now_unix_seconds())
                    .cloned()
                    .ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
                let principal = self
                    .db
                    .resolve_principal(&token.principal)
                    .ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
                let identity = AuthenticatedIdentity::ExternalPrincipal {
                    provider: "service_token".into(),
                    subject: token.token_id.clone(),
                    username: principal.username.clone(),
                    user_id: principal.user_id,
                    created_version: principal.created_epoch,
                    scopes: token.scopes.clone(),
                };
                let response_identity =
                    native_external_identity(&principal, token.token_id, token.scopes);
                (Some(principal), identity, response_identity)
            }
            Some(native::authenticate_request::Credential::Oidc(credential)) => {
                let oidc = self
                    .external_auth
                    .as_ref()
                    .and_then(|auth| auth.oidc.clone())
                    .ok_or_else(|| {
                        Status::unauthenticated("OIDC authentication is not configured")
                    })?;
                let token = credential.compact_jws;
                let verified = tokio::task::spawn_blocking(move || {
                    oidc.cache
                        .verify(&token, &oidc.validation, now_unix_seconds())
                })
                .await
                .map_err(|_| Status::internal("OIDC verifier task failed"))?
                .map_err(|_| Status::unauthenticated("invalid credentials"))?;
                let principal = self
                    .db
                    .resolve_principal(&verified.principal)
                    .ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
                let identity = AuthenticatedIdentity::ExternalPrincipal {
                    provider: "oidc".into(),
                    subject: verified.principal.clone(),
                    username: principal.username.clone(),
                    user_id: principal.user_id,
                    created_version: principal.created_epoch,
                    scopes: verified.scopes.clone(),
                };
                let response_identity =
                    native_external_identity(&principal, verified.principal, verified.scopes);
                (Some(principal), identity, response_identity)
            }
            None => return Err(Status::unauthenticated("credentials are required")),
        };
        let auth_token = self.issue_auth_grant(principal.clone(), identity)?;
        Ok(Response::new(native::AuthenticateResponse {
            identity: Some(response_identity),
            auth_token,
        }))
    }

    async fn begin_scram(
        &self,
        request: Request<native::BeginScramRequest>,
    ) -> Result<Response<native::BeginScramResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        if !request
            .client_first_bare
            .starts_with(&format!("n={},", request.username))
        {
            return Err(Status::invalid_argument(
                "SCRAM username does not match client-first message",
            ));
        }
        let (verifier, fake_user) = match self.db.user_scram_verifier(&request.username) {
            Some(verifier) => (verifier, false),
            None => {
                let salt = mongreldb_types::ids::QueryId::new_random();
                (
                    mongreldb_core::ScramVerifier::from_password(
                        "invalid-user-password",
                        salt.as_bytes(),
                        mongreldb_core::security_hardening::SCRAM_SHA_256_MIN_ITERATIONS,
                    )
                    .map_err(|error| Status::internal(error.to_string()))?,
                    true,
                )
            }
        };
        let server_nonce = mongreldb_types::ids::QueryId::new_random().to_hex();
        let session = mongreldb_core::ScramServerSession::begin(
            verifier,
            request.client_first_bare,
            &request.client_nonce,
            &server_nonce,
            mongreldb_core::ScramChannelBindingPolicy::Disabled,
            Vec::new(),
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let server_first = session.server_first_message().to_owned();
        let exchange_id = mongreldb_types::ids::QueryId::new_random()
            .as_bytes()
            .to_vec();
        let now = now_unix_micros();
        let mut exchanges = self
            .scram_exchanges
            .lock()
            .map_err(|_| Status::internal("SCRAM exchange store poisoned"))?;
        exchanges.retain(|_, exchange| exchange.expires_unix_micros > now);
        if exchanges.len() >= MAX_AUTH_TOKENS {
            return Err(Status::resource_exhausted("too many SCRAM exchanges"));
        }
        exchanges.insert(
            exchange_id.clone(),
            PendingScram {
                username: request.username,
                session,
                fake_user,
                expires_unix_micros: now.saturating_add(AUTH_TOKEN_TTL.as_micros() as u64),
            },
        );
        Ok(Response::new(native::BeginScramResponse {
            exchange_id,
            server_first,
        }))
    }

    async fn finish_scram(
        &self,
        request: Request<native::FinishScramRequest>,
    ) -> Result<Response<native::FinishScramResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let exchange = self
            .scram_exchanges
            .lock()
            .map_err(|_| Status::internal("SCRAM exchange store poisoned"))?
            .remove(&request.exchange_id)
            .ok_or_else(|| Status::unauthenticated("invalid SCRAM exchange"))?;
        if exchange.expires_unix_micros <= now_unix_micros() {
            return Err(Status::unauthenticated("invalid SCRAM exchange"));
        }
        let server_final = exchange
            .session
            .finish(&request.client_final_without_proof, &request.client_proof)
            .map_err(|_| Status::unauthenticated("invalid credentials"))?;
        if exchange.fake_user {
            return Err(Status::unauthenticated("invalid credentials"));
        }
        let principal = self
            .db
            .resolve_principal(&exchange.username)
            .ok_or_else(|| Status::unauthenticated("invalid credentials"))?;
        let identity = AuthenticatedIdentity::CatalogUser {
            username: principal.username.clone(),
            user_id: principal.user_id,
            created_version: principal.created_epoch,
        };
        let auth_token = self.issue_auth_grant(Some(principal.clone()), identity)?;
        Ok(Response::new(native::FinishScramResponse {
            server_final,
            authentication: Some(native::AuthenticateResponse {
                identity: Some(native_identity(Some(&principal))),
                auth_token,
            }),
        }))
    }
}

#[tonic::async_trait]
impl native::session_service_server::SessionService for NativeRuntime {
    async fn open_session(
        &self,
        request: Request<native::OpenSessionRequest>,
    ) -> Result<Response<native::OpenSessionResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        if request.database_id != self.sessions.database_id().as_bytes() {
            return Err(Status::not_found("database not found"));
        }
        let grant = self.consume_auth_grant(&request.auth_token)?;
        let owner = grant.principal.as_ref().map_or_else(
            || "anonymous".into(),
            |principal| principal.username.clone(),
        );
        let session = MongrelSession::open_with_external_modules_as(
            Arc::clone(&self.db),
            std::iter::empty(),
            grant.principal,
        )
        .map_err(query_status)?
        .with_query_registry(Arc::clone(&self.query_registry));
        let token = self
            .sessions
            .create_with_identity(session, owner, grant.identity)
            .ok_or_else(|| Status::resource_exhausted("session limit reached"))?;
        Ok(Response::new(native::OpenSessionResponse {
            session_id: hex_id(&token)?,
        }))
    }

    async fn close_session(
        &self,
        request: Request<native::CloseSessionRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let token = id_hex(&request.session_id, "session id")?;
        if !self.sessions.close_by_token(&token) {
            return Err(Status::not_found("session not found"));
        }
        self.query_registry
            .cancel_session(&token, mongreldb_core::CancellationReason::SessionClosed);
        Ok(Response::new(native::Empty {}))
    }
}

#[tonic::async_trait]
impl native::query_service_server::QueryService for NativeRuntime {
    type ExecuteStreamStream =
        Pin<Box<dyn Stream<Item = Result<native::ArrowFrame, Status>> + Send + 'static>>;

    async fn prepare(
        &self,
        request: Request<native::PrepareRequest>,
    ) -> Result<Response<native::PrepareResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (_, entry) = self.session(&request.session_id, "query")?;
        let _guard = entry.lock.lock().await;
        let statement_id = entry.allocate_statement_id();
        let name = format!("native_{}", statement_id.get());
        entry
            .session()
            .run(&format!("PREPARE {name} AS {}", request.sql))
            .await
            .map_err(query_status)?;
        let catalog = prepared::CatalogState::capture(&self.db);
        entry.insert_prepared_binding(
            name,
            prepared::build_binding(statement_id, request.sql, Vec::new(), &catalog),
        );
        Ok(Response::new(native::PrepareResponse {
            statement_id: statement_id.get(),
            schema_version: catalog.catalog_version.get(),
        }))
    }

    async fn execute(
        &self,
        request: Request<native::ExecuteRequest>,
    ) -> Result<Response<native::ExecuteResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (session_id, entry) = self.session(&request.session_id, "query")?;
        let id = query_id(&request.query_id)?;
        let sql = self.resolve_sql(&entry, &request).await?;
        let idempotency = self
            .begin_idempotency(&request, &entry, &session_id, &sql)
            .await?;
        if let NativeIdempotency::Replay(receipt) = idempotency {
            return Ok(Response::new(replayed_response(&request.query_id, receipt)));
        }
        let execution = match idempotency {
            NativeIdempotency::Execute(execution) => Some(execution),
            NativeIdempotency::None => None,
            NativeIdempotency::Replay(_) => unreachable!(),
        };
        let (query_id, batches) = match self
            .execute_sql(entry, session_id, id, sql, request.context.as_ref())
            .await
        {
            Ok(result) => result,
            Err(error) => {
                if let Some(execution) = execution {
                    self.finish_idempotency(execution, id);
                }
                return Err(error);
            }
        };
        let frames = batches
            .batches()
            .iter()
            .enumerate()
            .map(|(sequence, batch)| encode_batch(batch, sequence as u64, false))
            .chain(std::iter::once(Ok(native::ArrowFrame {
                ipc: Vec::new(),
                sequence: batches.batches().len() as u64,
                end_of_stream: true,
            })))
            .collect::<Result<Vec<_>, Status>>();
        let frames = match frames {
            Ok(frames) => {
                batches.complete().map_err(query_status)?;
                frames
            }
            Err(error) => {
                batches.fail_serialization();
                if let Some(execution) = execution {
                    self.finish_idempotency(execution, id);
                }
                return Err(error);
            }
        };
        let status = self.query_registry.status(id);
        if let Some(execution) = execution {
            self.finish_idempotency(execution, id);
        }
        Ok(Response::new(native::ExecuteResponse {
            query_id,
            rows_affected: 0,
            frames,
            idempotency_replayed: false,
            committed: status
                .as_ref()
                .is_some_and(|status| status.durable_outcome.committed),
            commit_epoch: status
                .as_ref()
                .and_then(|status| status.durable_outcome.last_commit_epoch)
                .unwrap_or(0),
            original_query_id: id.as_bytes().to_vec(),
        }))
    }

    async fn execute_stream(
        &self,
        request: Request<native::ExecuteRequest>,
    ) -> Result<Response<Self::ExecuteStreamStream>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        if request
            .context
            .as_ref()
            .is_some_and(|context| !context.idempotency_key.is_empty())
        {
            return Err(Status::invalid_argument(
                "idempotent writes require buffered Execute",
            ));
        }
        let (session_id, entry) = self.session(&request.session_id, "query")?;
        let id = query_id(&request.query_id)?;
        let sql = self.resolve_sql(&entry, &request).await?;
        let stream = entry
            .session()
            .run_stream_with_options(
                &sql,
                SqlQueryOptions {
                    query_id: Some(id),
                    timeout: request_timeout(request.context.as_ref())?,
                    owner: Some(entry.owner.clone()),
                    session_id: Some(session_id),
                    parent_control: None,
                },
            )
            .await
            .map_err(query_status)?;
        let output = futures::stream::try_unfold(
            (stream, 0_u64, false),
            |(mut stream, sequence, done)| async move {
                if done {
                    return Ok(None);
                }
                match stream.next().await {
                    Some(Ok(batch)) => {
                        let frame = encode_batch(&batch, sequence, false)?;
                        Ok(Some((frame, (stream, sequence + 1, false))))
                    }
                    Some(Err(error)) => Err(stream_status(error)),
                    None => Ok(Some((
                        native::ArrowFrame {
                            ipc: Vec::new(),
                            sequence,
                            end_of_stream: true,
                        },
                        (stream, sequence + 1, true),
                    ))),
                }
            },
        );
        Ok(Response::new(Box::pin(output)))
    }

    async fn cancel_query(
        &self,
        request: Request<native::CancelQueryRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (session_id, _) = self.session(&request.session_id, "query")?;
        let query_id = query_id(&request.query_id)?;
        let status = self
            .query_registry
            .status(query_id)
            .ok_or_else(|| Status::not_found("query not found"))?;
        if status.session_id.as_deref() != Some(&session_id) {
            return Err(Status::not_found("query not found"));
        }
        match self.query_registry.cancel(query_id) {
            CancelOutcome::Accepted | CancelOutcome::AlreadyCancelling => {
                Ok(Response::new(native::Empty {}))
            }
            CancelOutcome::TooLate | CancelOutcome::AlreadyFinished => {
                Err(Status::failed_precondition("query already finished"))
            }
            CancelOutcome::NotFound => Err(Status::not_found("query not found")),
        }
    }

    async fn get_query_status(
        &self,
        request: Request<native::GetQueryStatusRequest>,
    ) -> Result<Response<native::QueryStatusResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (session_id, _) = self.session(&request.session_id, "query")?;
        let id = query_id(&request.query_id)?;
        let status = self
            .query_registry
            .status(id)
            .ok_or_else(|| Status::not_found("query not found"))?;
        if status.session_id.as_deref() != Some(&session_id) {
            return Err(Status::not_found("query not found"));
        }
        Ok(Response::new(native::QueryStatusResponse {
            query_id: id.as_bytes().to_vec(),
            phase: native_phase(status.phase) as i32,
            error: status.terminal_error.map(|error| native::ErrorDetail {
                category_code: 0,
                category: format!("{:?}", error.category),
                message: error.code,
                retryable: false,
                metadata: HashMap::new(),
            }),
        }))
    }
}

fn replayed_response(
    current_query_id: &[u8],
    receipt: crate::sql_idempotency::SqlDurableReceipt,
) -> native::ExecuteResponse {
    native::ExecuteResponse {
        query_id: current_query_id.to_vec(),
        rows_affected: 0,
        frames: Vec::new(),
        idempotency_replayed: true,
        committed: receipt.outcome.committed,
        commit_epoch: receipt.outcome.last_commit_epoch.unwrap_or(0),
        original_query_id: hex_id(&receipt.original_query_id).unwrap_or_default(),
    }
}

#[tonic::async_trait]
impl native::transaction_service_server::TransactionService for NativeRuntime {
    async fn begin(
        &self,
        request: Request<native::BeginTransactionRequest>,
    ) -> Result<Response<native::BeginTransactionResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (_, entry) = self.session(&request.session_id, "transaction")?;
        let sql = match native::IsolationLevel::try_from(request.isolation) {
            Ok(native::IsolationLevel::ReadCommitted) => {
                "BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            }
            Ok(native::IsolationLevel::Serializable) => {
                "BEGIN; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            }
            _ => "BEGIN",
        };
        entry.session().run(sql).await.map_err(query_status)?;
        Ok(Response::new(native::BeginTransactionResponse {
            transaction_id: TransactionId::new_random().as_bytes().to_vec(),
        }))
    }

    async fn commit(
        &self,
        request: Request<native::TransactionRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        transaction_sql(self, request.into_inner(), "COMMIT").await
    }

    async fn rollback(
        &self,
        request: Request<native::TransactionRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        transaction_sql(self, request.into_inner(), "ROLLBACK").await
    }
}

#[tonic::async_trait]
impl native::catalog_service_server::CatalogService for NativeRuntime {
    async fn get_schema(
        &self,
        request: Request<native::GetSchemaRequest>,
    ) -> Result<Response<native::GetSchemaResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        if request.database_id != self.sessions.database_id().as_bytes() {
            return Err(Status::not_found("database not found"));
        }
        let (_, entry) = self.session(&request.session_id, "catalog:read")?;
        let principal = self.session_principal(&entry)?;
        self.db
            .require_for(
                principal.as_ref(),
                &mongreldb_core::Permission::Select {
                    table: request.table.clone(),
                },
            )
            .map_err(core_status)?;
        let table = self.db.table(&request.table).map_err(core_status)?;
        let schema = table.lock().schema().clone();
        Ok(Response::new(native::GetSchemaResponse {
            table: request.table,
            schema_version: schema.schema_id,
            columns: schema
                .columns
                .into_iter()
                .map(|column| native::ColumnSchema {
                    name: column.name,
                    data_type: format!("{:?}", column.ty),
                    nullable: column.flags.contains(mongreldb_core::ColumnFlags::NULLABLE),
                })
                .collect(),
        }))
    }

    async fn create_table(
        &self,
        request: Request<native::CreateTableRequest>,
    ) -> Result<Response<native::CreateTableResponse>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (_, entry) = self.session(&request.session_id, "catalog:write")?;
        let _guard = entry.lock.lock().await;
        let principal = self.session_principal(&entry)?;
        self.db
            .require_for(principal.as_ref(), &mongreldb_core::Permission::Ddl)
            .map_err(core_status)?;
        let columns = request
            .columns
            .into_iter()
            .map(native_create_column)
            .collect::<Result<Vec<_>, _>>()?;
        let uniques = request
            .uniques
            .into_iter()
            .map(|constraint| {
                Ok(mongreldb_core::constraint::UniqueConstraint {
                    id: native_u16(constraint.id, "unique constraint id")?,
                    name: constraint.name,
                    columns: native_u16s(constraint.columns, "unique constraint column")?,
                })
            })
            .collect::<Result<Vec<_>, Status>>()?;
        let foreign_keys = request
            .foreign_keys
            .into_iter()
            .map(native_foreign_key)
            .collect::<Result<Vec<_>, _>>()?;
        let mut schema = mongreldb_core::Schema {
            schema_id: request.schema_id,
            columns,
            indexes: Vec::new(),
            colocation: Vec::new(),
            constraints: mongreldb_core::constraint::TableConstraints {
                uniques,
                foreign_keys,
                checks: Vec::new(),
            },
            clustered: false,
        };
        if let Ok(existing) = self.db.table(&request.table) {
            let existing = existing.lock().schema().clone();
            schema.schema_id = existing.schema_id;
            if serde_json::to_vec(&schema).map_err(|_| Status::internal("schema encode failed"))?
                != serde_json::to_vec(&existing)
                    .map_err(|_| Status::internal("schema encode failed"))?
            {
                return Err(Status::already_exists(
                    "table exists with a different schema",
                ));
            }
            entry
                .session()
                .refresh_database_table(&request.table)
                .map_err(query_status)?;
            return Ok(Response::new(native::CreateTableResponse {
                table_id: self.db.table_id(&request.table).map_err(core_status)?,
                schema_version: self.db.catalog_version(),
            }));
        }
        let table_id = self
            .db
            .create_table(&request.table, schema)
            .map_err(core_status)?;
        entry
            .session()
            .refresh_database_table(&request.table)
            .map_err(query_status)?;
        Ok(Response::new(native::CreateTableResponse {
            table_id,
            schema_version: self.db.catalog_version(),
        }))
    }
}

fn native_create_column(column: native::CreateColumn) -> Result<mongreldb_core::ColumnDef, Status> {
    let ty = match native::ColumnType::try_from(column.data_type)
        .map_err(|_| Status::invalid_argument("unknown native column type"))?
    {
        native::ColumnType::Bool => mongreldb_core::TypeId::Bool,
        native::ColumnType::Int8 => mongreldb_core::TypeId::Int8,
        native::ColumnType::Int16 => mongreldb_core::TypeId::Int16,
        native::ColumnType::Int32 => mongreldb_core::TypeId::Int32,
        native::ColumnType::Int64 => mongreldb_core::TypeId::Int64,
        native::ColumnType::Uint8 => mongreldb_core::TypeId::UInt8,
        native::ColumnType::Uint16 => mongreldb_core::TypeId::UInt16,
        native::ColumnType::Uint32 => mongreldb_core::TypeId::UInt32,
        native::ColumnType::Uint64 => mongreldb_core::TypeId::UInt64,
        native::ColumnType::Float32 => mongreldb_core::TypeId::Float32,
        native::ColumnType::Float64 => mongreldb_core::TypeId::Float64,
        native::ColumnType::TimestampNanos => mongreldb_core::TypeId::TimestampNanos,
        native::ColumnType::Date32 => mongreldb_core::TypeId::Date32,
        native::ColumnType::Date64 => mongreldb_core::TypeId::Date64,
        native::ColumnType::Time64 => mongreldb_core::TypeId::Time64,
        native::ColumnType::Bytes => mongreldb_core::TypeId::Bytes,
        native::ColumnType::Json => mongreldb_core::TypeId::Json,
        native::ColumnType::Decimal128 => mongreldb_core::TypeId::Decimal128 {
            precision: u8::try_from(column.decimal_precision)
                .map_err(|_| Status::invalid_argument("decimal precision exceeds u8"))?,
            scale: i8::try_from(column.decimal_scale)
                .map_err(|_| Status::invalid_argument("decimal scale exceeds i8"))?,
        },
        native::ColumnType::Unspecified => {
            return Err(Status::invalid_argument("native column type is required"))
        }
    };
    let mut flags = mongreldb_core::ColumnFlags::empty();
    if column.nullable {
        flags = flags.with(mongreldb_core::ColumnFlags::NULLABLE);
    }
    if column.primary_key {
        flags = flags.with(mongreldb_core::ColumnFlags::PRIMARY_KEY);
    }
    if column.auto_increment {
        flags = flags.with(mongreldb_core::ColumnFlags::AUTO_INCREMENT);
    }
    Ok(mongreldb_core::ColumnDef {
        id: native_u16(column.id, "column id")?,
        name: column.name,
        ty,
        flags,
        default_value: None,
        embedding_source: None,
    })
}

fn native_foreign_key(
    foreign_key: native::ForeignKey,
) -> Result<mongreldb_core::constraint::ForeignKey, Status> {
    let action = |value| -> Result<mongreldb_core::constraint::FkAction, Status> {
        match native::ForeignKeyAction::try_from(value)
            .map_err(|_| Status::invalid_argument("unknown foreign-key action"))?
        {
            native::ForeignKeyAction::Unspecified | native::ForeignKeyAction::Restrict => {
                Ok(mongreldb_core::constraint::FkAction::Restrict)
            }
            native::ForeignKeyAction::Cascade => Ok(mongreldb_core::constraint::FkAction::Cascade),
            native::ForeignKeyAction::SetNull => Ok(mongreldb_core::constraint::FkAction::SetNull),
        }
    };
    Ok(mongreldb_core::constraint::ForeignKey {
        id: native_u16(foreign_key.id, "foreign-key id")?,
        name: foreign_key.name,
        columns: native_u16s(foreign_key.columns, "foreign-key column")?,
        ref_table: foreign_key.referenced_table,
        ref_columns: native_u16s(foreign_key.referenced_columns, "referenced column")?,
        on_delete: action(foreign_key.on_delete)?,
        on_update: action(foreign_key.on_update)?,
    })
}

fn native_u16(value: u32, field: &str) -> Result<u16, Status> {
    u16::try_from(value).map_err(|_| Status::invalid_argument(format!("{field} exceeds u16")))
}

fn native_u16s(values: Vec<u32>, field: &str) -> Result<Vec<u16>, Status> {
    values
        .into_iter()
        .map(|value| native_u16(value, field))
        .collect()
}

#[tonic::async_trait]
impl native::admin_service_server::AdminService for NativeRuntime {
    async fn execute_admin(
        &self,
        request: Request<native::ExecuteAdminRequest>,
    ) -> Result<Response<native::Empty>, Status> {
        let request = request.into_inner();
        validate_native_context(request.context.as_ref())?;
        let (_, entry) = self.session(&request.session_id, "admin")?;
        let sql = String::from_utf8(request.command)
            .map_err(|_| Status::invalid_argument("admin command must be UTF-8 SQL"))?;
        entry.session().run(&sql).await.map_err(query_status)?;
        Ok(Response::new(native::Empty {}))
    }
}

#[tonic::async_trait]
impl native::health_service_server::HealthService for NativeRuntime {
    async fn status(
        &self,
        request: Request<native::HealthRequest>,
    ) -> Result<Response<native::HealthResponse>, Status> {
        validate_native_context(request.get_ref().context.as_ref())?;
        Ok(Response::new(native::HealthResponse {
            serving: self.db.lifecycle_state() == mongreldb_core::LifecycleState::Open,
            detail: "ready".into(),
        }))
    }
}

async fn transaction_sql(
    runtime: &NativeRuntime,
    request: native::TransactionRequest,
    sql: &str,
) -> Result<Response<native::Empty>, Status> {
    validate_native_context(request.context.as_ref())?;
    let (_, entry) = runtime.session(&request.session_id, "transaction")?;
    entry.session().run(sql).await.map_err(query_status)?;
    Ok(Response::new(native::Empty {}))
}

fn query_id(bytes: &[u8]) -> Result<QueryId, Status> {
    QueryId::from_str(&id_hex(bytes, "query id")?)
        .map_err(|_| Status::invalid_argument("query id must be 16 bytes"))
}

fn id_hex(bytes: &[u8], label: &str) -> Result<String, Status> {
    if bytes.len() != 16 {
        return Err(Status::invalid_argument(format!(
            "{label} must be 16 bytes"
        )));
    }
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn hex_id(text: &str) -> Result<Vec<u8>, Status> {
    if text.len() != 32 {
        return Err(Status::internal("invalid server session id"));
    }
    (0..16)
        .map(|index| {
            u8::from_str_radix(&text[index * 2..index * 2 + 2], 16)
                .map_err(|_| Status::internal("invalid server session id"))
        })
        .collect()
}

fn request_timeout(context: Option<&native::RequestContext>) -> Result<Option<Duration>, Status> {
    let deadline = context.map_or(0, |context| context.deadline_unix_micros);
    if deadline == 0 {
        return Ok(None);
    }
    let remaining = deadline.saturating_sub(now_unix_micros());
    if remaining == 0 {
        return Err(structured_status(
            Code::DeadlineExceeded,
            ErrorCategory::DeadlineExceeded,
            "request deadline exceeded",
        ));
    }
    Ok(Some(Duration::from_micros(remaining)))
}

fn now_unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u128::from(u64::MAX)) as u64
}

fn now_unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn native_identity(principal: Option<&Principal>) -> native::AuthenticatedIdentity {
    match principal {
        Some(principal) => native::AuthenticatedIdentity {
            principal_id: principal.user_id,
            principal_name: principal.username.clone(),
            roles: principal.roles.clone(),
            scopes: principal
                .permissions
                .iter()
                .map(|permission| format!("{permission:?}"))
                .collect(),
        },
        None => native::AuthenticatedIdentity {
            principal_id: 0,
            principal_name: "anonymous".into(),
            roles: Vec::new(),
            scopes: Vec::new(),
        },
    }
}

fn native_external_identity(
    principal: &Principal,
    label: String,
    scopes: Vec<String>,
) -> native::AuthenticatedIdentity {
    native::AuthenticatedIdentity {
        principal_id: principal.user_id,
        principal_name: label,
        roles: principal.roles.clone(),
        scopes,
    }
}

fn parameter_literal(
    value: &mongreldb_protocol::request::ParameterValue,
) -> Result<String, Status> {
    use mongreldb_protocol::request::ParameterValue;
    match value {
        ParameterValue::Null => Ok("NULL".into()),
        ParameterValue::Bool(value) => Ok(if *value { "TRUE" } else { "FALSE" }.into()),
        ParameterValue::Integer(value) => Ok(value.to_string()),
        ParameterValue::Float(value) if value.is_finite() => Ok(value.to_string()),
        ParameterValue::Float(_) => Err(Status::invalid_argument("non-finite float parameter")),
        ParameterValue::Text(value) => Ok(format!("'{}'", value.replace('\'', "''"))),
        ParameterValue::Bytes(value) => Ok(format!(
            "X'{}'",
            value
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        )),
    }
}

fn bind_numbered_parameters(sql: &str, literals: &[String]) -> Result<String, Status> {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Quote(char),
        LineComment,
        BlockComment,
    }

    let mut output = String::with_capacity(sql.len());
    let mut used = vec![false; literals.len()];
    let mut state = State::Normal;
    let mut chars = sql.chars().peekable();
    while let Some(character) = chars.next() {
        output.push(character);
        match state {
            State::Normal if matches!(character, '\'' | '"' | '`') => {
                state = State::Quote(character);
            }
            State::Normal if character == '-' && chars.peek() == Some(&'-') => {
                output.push(chars.next().expect("peeked"));
                state = State::LineComment;
            }
            State::Normal if character == '/' && chars.peek() == Some(&'*') => {
                output.push(chars.next().expect("peeked"));
                state = State::BlockComment;
            }
            State::Normal if character == '$' && chars.peek().is_some_and(char::is_ascii_digit) => {
                output.pop();
                let mut digits = String::new();
                while chars.peek().is_some_and(char::is_ascii_digit) {
                    digits.push(chars.next().expect("peeked"));
                }
                let index = digits
                    .parse::<usize>()
                    .ok()
                    .and_then(|index| index.checked_sub(1))
                    .filter(|index| *index < literals.len())
                    .ok_or_else(|| {
                        Status::invalid_argument("prepared parameter is out of range")
                    })?;
                used[index] = true;
                output.push_str(&literals[index]);
            }
            State::Quote(_) if character == '\\' => {
                if let Some(escaped) = chars.next() {
                    output.push(escaped);
                }
            }
            State::Quote(end) if character == end && chars.peek() == Some(&end) => {
                output.push(chars.next().expect("peeked"));
            }
            State::Quote(end) if character == end => state = State::Normal,
            State::LineComment if character == '\n' => state = State::Normal,
            State::BlockComment if character == '*' && chars.peek() == Some(&'/') => {
                output.push(chars.next().expect("peeked"));
                state = State::Normal;
            }
            _ => {}
        }
    }
    if used.iter().any(|used| !used) {
        return Err(Status::invalid_argument(
            "prepared parameter count does not match SQL placeholders",
        ));
    }
    Ok(output)
}

fn encode_batch(
    batch: &arrow::record_batch::RecordBatch,
    sequence: u64,
    end_of_stream: bool,
) -> Result<native::ArrowFrame, Status> {
    let mut ipc = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc, &batch.schema())
            .map_err(|error| Status::internal(error.to_string()))?;
        writer
            .write(batch)
            .and_then(|_| writer.finish())
            .map_err(|error| Status::internal(error.to_string()))?;
    }
    Ok(native::ArrowFrame {
        ipc,
        sequence,
        end_of_stream,
    })
}

fn core_status(error: MongrelError) -> Status {
    let category = error.category();
    let code = match category {
        ErrorCategory::Unauthenticated => Code::Unauthenticated,
        ErrorCategory::PermissionDenied => Code::PermissionDenied,
        ErrorCategory::DeadlineExceeded => Code::DeadlineExceeded,
        ErrorCategory::ResourceExhausted => Code::ResourceExhausted,
        ErrorCategory::StaleMetadata
        | ErrorCategory::SchemaVersionMismatch
        | ErrorCategory::ClusterVersionMismatch => Code::FailedPrecondition,
        ErrorCategory::TransactionConflict
        | ErrorCategory::SerializationFailure
        | ErrorCategory::Deadlock => Code::Aborted,
        _ => Code::Internal,
    };
    structured_status(code, category, &error.to_string())
}

fn query_status(error: MongrelQueryError) -> Status {
    query_status_ref(&error)
}

fn query_status_ref(error: &MongrelQueryError) -> Status {
    match error {
        MongrelQueryError::Core(error) => {
            let category = error.category();
            let code = match category {
                ErrorCategory::Unauthenticated => Code::Unauthenticated,
                ErrorCategory::PermissionDenied => Code::PermissionDenied,
                ErrorCategory::DeadlineExceeded => Code::DeadlineExceeded,
                ErrorCategory::ResourceExhausted => Code::ResourceExhausted,
                ErrorCategory::StaleMetadata
                | ErrorCategory::SchemaVersionMismatch
                | ErrorCategory::ClusterVersionMismatch => Code::FailedPrecondition,
                ErrorCategory::TransactionConflict
                | ErrorCategory::SerializationFailure
                | ErrorCategory::Deadlock => Code::Aborted,
                _ => Code::Internal,
            };
            structured_status(code, category, &error.to_string())
        }
        MongrelQueryError::DeadlineExceeded { .. } => structured_status(
            Code::DeadlineExceeded,
            ErrorCategory::DeadlineExceeded,
            &error.to_string(),
        ),
        MongrelQueryError::QueryCancelled { .. } => structured_status(
            Code::Cancelled,
            ErrorCategory::Cancelled,
            &error.to_string(),
        ),
        MongrelQueryError::QueryRegistryFull | MongrelQueryError::ResultLimitExceeded { .. } => {
            structured_status(
                Code::ResourceExhausted,
                ErrorCategory::ResourceExhausted,
                &error.to_string(),
            )
        }
        MongrelQueryError::TransactionAborted => structured_status(
            Code::Aborted,
            ErrorCategory::TransactionAborted,
            &error.to_string(),
        ),
        MongrelQueryError::OutcomeUnknown { .. } => structured_status(
            Code::Unknown,
            ErrorCategory::CommitOutcomeUnknown,
            &error.to_string(),
        ),
        error => Status::internal(error.to_string()),
    }
}

fn stream_status(error: impl std::error::Error + 'static) -> Status {
    let message = error.to_string();
    let mut source = Some(&error as &(dyn std::error::Error + 'static));
    while let Some(current) = source {
        if let Some(error) = current.downcast_ref::<MongrelQueryError>() {
            return query_status_ref(error);
        }
        source = current.source();
    }
    Status::internal(message)
}

fn structured_status(code: Code, category: ErrorCategory, message: &str) -> Status {
    let detail = native::ErrorDetail {
        category_code: category.code(),
        category: category.to_string(),
        message: message.into(),
        retryable: category.retry_class() != RetryClass::Never,
        metadata: HashMap::new(),
    };
    Status::with_details(code, message, detail.encode_to_vec().into())
}

fn native_phase(phase: SqlQueryPhase) -> native::QueryPhase {
    match phase {
        SqlQueryPhase::Queued => native::QueryPhase::Queued,
        SqlQueryPhase::Planning => native::QueryPhase::Planning,
        SqlQueryPhase::Executing
        | SqlQueryPhase::Streaming
        | SqlQueryPhase::CommitCritical
        | SqlQueryPhase::Cancelling => native::QueryPhase::Executing,
        SqlQueryPhase::Serializing => native::QueryPhase::Serializing,
        SqlQueryPhase::Completed => native::QueryPhase::Completed,
        SqlQueryPhase::Failed => native::QueryPhase::Failed,
        SqlQueryPhase::Cancelled => native::QueryPhase::Cancelled,
    }
}
