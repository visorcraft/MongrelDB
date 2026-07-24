//! mongreldb-server entry point.
//!
//! Supports `--daemon` mode (fork into background), graceful signal handling
//! (SIGINT/SIGTERM flush all tables then exit; SIGHUP reloads the mutable
//! configuration subset live, spec §10.7), a proper flag parser, and
//! subcommands for deterministic-stable snapshots:
//!
//!   mongreldb-server snapshot <db_dir>   — checkpoint to a stable byte image
//!   mongreldb-server restore  <db_dir>   — open + verify + checkpoint

#[cfg(feature = "cluster")]
use mongreldb_cluster::bootstrap::{
    cluster_init, cluster_join, node_drain, node_remove, removal_confirmation_token, InitRequest,
    JoinInvite, TrustConfig,
};
#[cfg(feature = "cluster")]
use mongreldb_cluster::node::{Locality, NodeCapacity, NodeIdentity};
use mongreldb_core::Database;
#[cfg(feature = "native-rpc")]
use mongreldb_core::ServiceToken;
#[cfg(feature = "remote-embedding")]
use mongreldb_core::{EmbeddingNormalization, EmbeddingProviderRegistry};
#[cfg(all(feature = "oidc", feature = "native-rpc"))]
use mongreldb_core::{JwtAlgorithm, JwtValidationConfig};
#[cfg(feature = "native-rpc")]
use mongreldb_protocol::native_transport::{
    NativeRpcServer, NativeRpcServerConfig, NativeRpcServices,
};
#[cfg(feature = "native-rpc")]
use mongreldb_server::native::NativeExternalAuth;
#[cfg(all(feature = "oidc", feature = "native-rpc"))]
use mongreldb_server::oidc::HttpsJwksProvider;
#[cfg(feature = "remote-embedding")]
use mongreldb_server::remote_embedding::{
    EnvironmentSecretResolver, RemoteEmbeddingConfig, RemoteEmbeddingProvider,
};
#[cfg(feature = "vault-kms")]
use mongreldb_server::vault_kms::{VaultTransitConfig, VaultTransitKeyManagementProvider};
use mongreldb_server::{
    build_app_with_storage, spawn_auto_compactor, spawn_session_reaper, ServerStorageRuntime,
    SessionStore,
};
#[cfg(feature = "cluster")]
use mongreldb_server::{cluster_admin, cluster_runtime, fragment_rpc};
#[cfg(feature = "cluster")]
use mongreldb_types::ids::{ClusterId, NodeId};
#[cfg(feature = "remote-embedding")]
use serde::Deserialize;
#[cfg(feature = "cluster")]
use serde_json::json;
#[cfg(feature = "cluster")]
use std::collections::BTreeMap;
#[cfg(any(feature = "native-rpc", feature = "remote-embedding"))]
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::net::SocketAddr;
#[cfg(feature = "cluster")]
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

/// Parsed command-line arguments.
struct Args {
    db_dir: String,
    port: u16,
    auth_token: Option<String>,
    user_auth: bool,
    max_connections: Option<usize>,
    max_sessions: usize,
    session_idle_timeout_secs: u64,
    #[cfg(feature = "native-rpc")]
    native_port: Option<u16>,
    #[cfg(feature = "native-rpc")]
    native_listen: Option<String>,
    #[cfg(feature = "native-rpc")]
    native_advertise: Option<String>,
    #[cfg(feature = "native-rpc")]
    tls_certificate: Option<String>,
    #[cfg(feature = "native-rpc")]
    tls_private_key: Option<String>,
    #[cfg(feature = "native-rpc")]
    tls_client_ca: Option<String>,
    #[cfg(feature = "native-rpc")]
    require_client_cert: bool,
    #[cfg(feature = "native-rpc")]
    service_token_file: Option<String>,
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    oidc_issuer: Option<String>,
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    oidc_audience: Option<String>,
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    oidc_allowed_hosts: BTreeSet<String>,
    passphrase: Option<String>,
    #[cfg(feature = "vault-kms")]
    vault_url: Option<String>,
    #[cfg(feature = "vault-kms")]
    vault_mount: Option<String>,
    #[cfg(feature = "vault-kms")]
    vault_key: Option<String>,
    #[cfg(feature = "vault-kms")]
    vault_ca_certificate: Option<String>,
    daemon: bool,
    pidfile: Option<String>,
    /// When set, start a live cluster [`NodeRuntime`] from this node-data dir.
    #[cfg(feature = "cluster")]
    cluster_node_data: Option<String>,
    /// Optional cluster RPC listen address (`host:port`).
    #[cfg(feature = "cluster")]
    cluster_rpc_listen: Option<String>,
    /// Repeatable JSON configuration for a named remote embedding provider.
    #[cfg(feature = "remote-embedding")]
    embedding_provider_files: Vec<String>,
}

const DEFAULT_PORT: u16 = 8453;

const USAGE: &str = "\
mongreldb-server — HTTP daemon for MongrelDB

USAGE:
    mongreldb-server <db_dir> [options]

ARGS:
    <db_dir>            Database directory (required, first positional arg)
    <port>              Optional second positional arg (numeric) for backward compat

OPTIONS:
    --port <port>               Listen port (default 8453)
    --auth-token <token>        Enable Bearer token authentication
    --auth-users                Enable Basic user authentication
    --max-connections <n>       Max concurrent connections
    --max-sessions <n>          Max live sessions for cross-request txns (default 256)
    --session-idle-timeout <s>  Idle session reaping timeout in seconds (default 300)
    --native-port <port>        Native gRPC listener port (binds 127.0.0.1:<port>)
    --native-listen <host:port> Native gRPC bind address (default host 127.0.0.1)
    --native-advertise <addr>   Advertised native endpoint (status / clients)
    --tls-cert <path>           Native listener PEM certificate
    --tls-key <path>            Native listener PEM private key
    --tls-client-ca <path>      Require native client certificates from this CA
    --service-tokens <path>     JSON array of Argon2id service-token records
    --oidc-issuer <url>         Native OIDC issuer (HTTPS)
    --oidc-audience <audience>  Native OIDC audience
    --oidc-allow-host <host>    Allowed OIDC/JWKS host (repeatable)
    --passphrase <passphrase>   Open an encrypted database
    --vault-url <https-url>     Encrypt with HashiCorp Vault Transit
    --vault-mount <mount>       Vault Transit mount name
    --vault-key <key>           Vault Transit key name
    --vault-ca-cert <path>      Additional Vault CA certificate
    --daemon                    Fork into the background (daemonize)
    --pidfile <path>            PID file path (default: <db_dir>/mongreldb.pid)
    --cluster-node-data <dir>   Enable cluster mode: start NodeRuntime from this
                                provisioned node-data directory (after
                                `cluster init` / `cluster join`)
    --cluster-rpc-listen <addr> Cluster raft RPC listen address (host:port;
                                default 127.0.0.1:17443 or env)
    --embedding-provider <path> Register a remote embedding provider from JSON
                                (repeatable; secrets are environment references)
    -h, --help                  Print this help message

SUBCOMMANDS (one-shot; they do not start the daemon):
    snapshot <db_dir>           Checkpoint to a stable byte image
    restore <db_dir>            Open + verify + checkpoint
    cluster init|join|status    Cluster bootstrap (spec section 11.1, S2A-002)
    node drain|remove           Cluster membership transitions

ENVIRONMENT:
    MONGRELDB_DB_USERNAME       Database-handle username (set with DB_PASSWORD)
    MONGRELDB_DB_PASSWORD       Database-handle password (set with DB_USERNAME)
    MONGRELDB_VAULT_TOKEN       Vault token, removed from environment at startup
    MONGRELDB_VAULT_NAMESPACE   Optional Vault Enterprise namespace
    MONGRELDB_CLUSTER_NODE_DATA Same as --cluster-node-data
    MONGRELDB_CLUSTER_RPC_LISTEN Same as --cluster-rpc-listen
    MONGRELDB_CLUSTER_PLAINTEXT_TEST=1
                                Test-only: plaintext cluster transport (NON-PRODUCTION;
                                refused in production builds without the
                                dangerous-test-transport feature)
    MONGRELDB_NATIVE_LISTEN     Native bind address (host:port); default host 127.0.0.1
    MONGRELDB_NATIVE_ADVERTISE  Advertised native endpoint
    MONGRELDB_NATIVE_PORT       Native port on 127.0.0.1 (legacy)
    MONGRELDB_NATIVE_TLS_CERT   Native TLS certificate PEM path
    MONGRELDB_NATIVE_TLS_KEY    Native TLS private key PEM path
    MONGRELDB_NATIVE_TLS_CA     Native client CA PEM (require client certs)
    MONGRELDB_NATIVE_REQUIRE_CLIENT_CERT=1
                                Require native client certificates

COMPILE-TIME FEATURES:
    The native RPC, cluster, OIDC, Vault KMS, and remote-embedding options are
    only available when the binary is built with the corresponding cargo
    features (native-rpc, cluster, oidc, vault-kms, remote-embedding).
";

struct DatabaseCredentials {
    username: String,
    password: Zeroizing<String>,
}

fn database_credentials_from_values(
    username: Option<OsString>,
    password: Option<OsString>,
) -> Result<Option<DatabaseCredentials>, String> {
    let (username, password) = match (username, password) {
        (None, None) => return Ok(None),
        (Some(username), Some(password)) => (username, password),
        _ => {
            return Err(
                "MONGRELDB_DB_USERNAME and MONGRELDB_DB_PASSWORD must be set together".into(),
            )
        }
    };
    let username = username
        .into_string()
        .map_err(|_| "MONGRELDB_DB_USERNAME must be valid UTF-8".to_string())?;
    let password = Zeroizing::new(
        password
            .into_string()
            .map_err(|_| "MONGRELDB_DB_PASSWORD must be valid UTF-8".to_string())?,
    );
    if username.is_empty() {
        return Err("MONGRELDB_DB_USERNAME must not be empty".into());
    }
    if password.is_empty() {
        return Err("MONGRELDB_DB_PASSWORD must not be empty".into());
    }
    Ok(Some(DatabaseCredentials { username, password }))
}

/// Read database-open credentials once, then remove them before daemonization
/// or worker threads can inherit the environment. The password remains in a
/// `Zeroizing<String>` only until the database open/create call returns.
fn take_database_credentials_from_env() -> Result<Option<DatabaseCredentials>, String> {
    let username = std::env::var_os("MONGRELDB_DB_USERNAME");
    let password = std::env::var_os("MONGRELDB_DB_PASSWORD");
    std::env::remove_var("MONGRELDB_DB_USERNAME");
    std::env::remove_var("MONGRELDB_DB_PASSWORD");
    database_credentials_from_values(username, password)
}

#[cfg(feature = "vault-kms")]
fn take_vault_environment(
    configured: bool,
) -> Result<(Option<Zeroizing<String>>, Option<String>), String> {
    let token = std::env::var_os("MONGRELDB_VAULT_TOKEN");
    let namespace = std::env::var_os("MONGRELDB_VAULT_NAMESPACE");
    std::env::remove_var("MONGRELDB_VAULT_TOKEN");
    std::env::remove_var("MONGRELDB_VAULT_NAMESPACE");
    if !configured {
        if token.is_some() || namespace.is_some() {
            return Err("Vault environment requires --vault-url".into());
        }
        return Ok((None, None));
    }
    let token = token
        .ok_or("MONGRELDB_VAULT_TOKEN is required with --vault-url")?
        .into_string()
        .map_err(|_| "MONGRELDB_VAULT_TOKEN must be valid UTF-8")?;
    if token.is_empty() {
        return Err("MONGRELDB_VAULT_TOKEN must not be empty".into());
    }
    let namespace = namespace
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "MONGRELDB_VAULT_NAMESPACE must be valid UTF-8".to_string())
        })
        .transpose()?;
    Ok((Some(Zeroizing::new(token)), namespace))
}

fn open_or_create_database(
    db_dir: &str,
    passphrase: Option<&str>,
    kms: Option<(&dyn mongreldb_core::KeyManagementProvider, &str)>,
    credentials: Option<DatabaseCredentials>,
) -> mongreldb_core::Result<Database> {
    let catalog_exists = std::path::Path::new(db_dir).join("CATALOG").exists();
    if let Some((provider, key_id)) = kms {
        return match (catalog_exists, credentials.as_ref()) {
            (true, Some(credentials)) => Database::open_with_kms_and_credentials(
                db_dir,
                provider,
                &credentials.username,
                credentials.password.as_str(),
            ),
            (false, Some(credentials)) => Database::create_with_kms_and_credentials(
                db_dir,
                provider,
                key_id,
                &credentials.username,
                credentials.password.as_str(),
            ),
            (true, None) => Database::open_with_kms(db_dir, provider),
            (false, None) => Database::create_with_kms(db_dir, provider, key_id),
        };
    }
    match (catalog_exists, passphrase, credentials.as_ref()) {
        (true, Some(passphrase), Some(credentials)) => Database::open_encrypted_with_credentials(
            db_dir,
            passphrase,
            &credentials.username,
            credentials.password.as_str(),
        ),
        (false, Some(passphrase), Some(credentials)) => {
            Database::create_encrypted_with_credentials(
                db_dir,
                passphrase,
                &credentials.username,
                credentials.password.as_str(),
            )
        }
        (true, None, Some(credentials)) => Database::open_with_credentials(
            db_dir,
            &credentials.username,
            credentials.password.as_str(),
        ),
        (false, None, Some(credentials)) => Database::create_with_credentials(
            db_dir,
            &credentials.username,
            credentials.password.as_str(),
        ),
        (true, Some(passphrase), None) => Database::open_encrypted(db_dir, passphrase),
        (false, Some(passphrase), None) => Database::create_encrypted(db_dir, passphrase),
        (true, None, None) => Database::open(db_dir),
        (false, None, None) => Database::create(db_dir),
    }
}

fn validate_http_auth_configuration(
    db: &Database,
    auth_token: Option<&str>,
    user_auth: bool,
) -> Result<(), String> {
    if db.require_auth_enabled() && auth_token.is_none() && !user_auth {
        return Err(
            "this database requires authentication; configure --auth-users or --auth-token".into(),
        );
    }
    Ok(())
}

/// Parse command-line arguments. Returns `Err(message)` on failure.
fn parse_args() -> Result<Args, String> {
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() < 2 {
        return Err(format!(
            "error: a database directory is required\n\n{USAGE}"
        ));
    }

    let mut db_dir: Option<String> = None;
    let mut port: Option<u16> = None;
    let mut auth_token: Option<String> = None;
    let mut user_auth = false;
    let mut max_connections: Option<usize> = None;
    let mut max_sessions: usize = 256;
    let mut session_idle_timeout_secs: u64 = 300;
    #[cfg(feature = "native-rpc")]
    let mut native_port = None;
    #[cfg(feature = "native-rpc")]
    let mut native_listen = None;
    #[cfg(feature = "native-rpc")]
    let mut native_advertise = None;
    #[cfg(feature = "native-rpc")]
    let mut tls_certificate = None;
    #[cfg(feature = "native-rpc")]
    let mut tls_private_key = None;
    #[cfg(feature = "native-rpc")]
    let mut tls_client_ca = None;
    #[cfg(feature = "native-rpc")]
    let mut require_client_cert = false;
    #[cfg(feature = "native-rpc")]
    let mut service_token_file = None;
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    let mut oidc_issuer = None;
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    let mut oidc_audience = None;
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    let mut oidc_allowed_hosts = BTreeSet::new();
    let mut passphrase: Option<String> = None;
    #[cfg(feature = "vault-kms")]
    let mut vault_url = None;
    #[cfg(feature = "vault-kms")]
    let mut vault_mount = None;
    #[cfg(feature = "vault-kms")]
    let mut vault_key = None;
    #[cfg(feature = "vault-kms")]
    let mut vault_ca_certificate = None;
    let mut daemon = false;
    let mut pidfile: Option<String> = None;
    #[cfg(feature = "cluster")]
    let mut cluster_node_data: Option<String> = None;
    #[cfg(feature = "cluster")]
    let mut cluster_rpc_listen: Option<String> = None;
    #[cfg(feature = "remote-embedding")]
    let mut embedding_provider_files = Vec::new();

    // Skip the program name (raw[0]).
    let mut i = 1;
    while i < raw.len() {
        let arg = &raw[i];
        match arg.as_str() {
            "-h" | "--help" => {
                print!("{USAGE}");
                std::process::exit(0);
            }
            "--port" => {
                let v = raw.get(i + 1).ok_or("--port requires a value")?;
                port = Some(
                    v.parse::<u16>()
                        .map_err(|_| format!("--port: invalid port '{v}'"))?,
                );
                i += 2;
            }
            "--auth-token" => {
                let v = raw.get(i + 1).ok_or("--auth-token requires a value")?;
                auth_token = Some(v.clone());
                i += 2;
            }
            "--auth-users" => {
                user_auth = true;
                i += 1;
            }
            "--max-connections" => {
                let v = raw.get(i + 1).ok_or("--max-connections requires a value")?;
                max_connections = Some(
                    v.parse::<usize>()
                        .map_err(|_| format!("--max-connections: invalid value '{v}'"))?,
                );
                i += 2;
            }
            "--max-sessions" => {
                let v = raw.get(i + 1).ok_or("--max-sessions requires a value")?;
                max_sessions = v
                    .parse::<usize>()
                    .map_err(|_| format!("--max-sessions: invalid value '{v}'"))?;
                i += 2;
            }
            "--session-idle-timeout" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--session-idle-timeout requires a value")?;
                session_idle_timeout_secs = v
                    .parse::<u64>()
                    .map_err(|_| format!("--session-idle-timeout: invalid value '{v}'"))?;
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--native-port" => {
                let value = raw.get(i + 1).ok_or("--native-port requires a value")?;
                native_port = Some(
                    value
                        .parse::<u16>()
                        .map_err(|_| format!("--native-port: invalid port '{value}'"))?,
                );
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--native-listen" => {
                native_listen = Some(
                    raw.get(i + 1)
                        .ok_or("--native-listen requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--native-advertise" => {
                native_advertise = Some(
                    raw.get(i + 1)
                        .ok_or("--native-advertise requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--tls-cert" => {
                tls_certificate =
                    Some(raw.get(i + 1).ok_or("--tls-cert requires a value")?.clone());
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--tls-key" => {
                tls_private_key = Some(raw.get(i + 1).ok_or("--tls-key requires a value")?.clone());
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--tls-client-ca" => {
                tls_client_ca = Some(
                    raw.get(i + 1)
                        .ok_or("--tls-client-ca requires a value")?
                        .clone(),
                );
                // Supplying a client CA implies client certificates are required.
                require_client_cert = true;
                i += 2;
            }
            #[cfg(feature = "native-rpc")]
            "--require-client-cert" => {
                require_client_cert = true;
                i += 1;
            }
            #[cfg(feature = "native-rpc")]
            "--service-tokens" => {
                service_token_file = Some(
                    raw.get(i + 1)
                        .ok_or("--service-tokens requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(all(feature = "oidc", feature = "native-rpc"))]
            "--oidc-issuer" => {
                oidc_issuer = Some(
                    raw.get(i + 1)
                        .ok_or("--oidc-issuer requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(all(feature = "oidc", feature = "native-rpc"))]
            "--oidc-audience" => {
                oidc_audience = Some(
                    raw.get(i + 1)
                        .ok_or("--oidc-audience requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(all(feature = "oidc", feature = "native-rpc"))]
            "--oidc-allow-host" => {
                oidc_allowed_hosts.insert(
                    raw.get(i + 1)
                        .ok_or("--oidc-allow-host requires a value")?
                        .clone(),
                );
                i += 2;
            }
            "--passphrase" => {
                let v = raw.get(i + 1).ok_or("--passphrase requires a value")?;
                passphrase = Some(v.clone());
                i += 2;
            }
            #[cfg(feature = "vault-kms")]
            "--vault-url" => {
                vault_url = Some(
                    raw.get(i + 1)
                        .ok_or("--vault-url requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(feature = "vault-kms")]
            "--vault-mount" => {
                vault_mount = Some(
                    raw.get(i + 1)
                        .ok_or("--vault-mount requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(feature = "vault-kms")]
            "--vault-key" => {
                vault_key = Some(
                    raw.get(i + 1)
                        .ok_or("--vault-key requires a value")?
                        .clone(),
                );
                i += 2;
            }
            #[cfg(feature = "vault-kms")]
            "--vault-ca-cert" => {
                vault_ca_certificate = Some(
                    raw.get(i + 1)
                        .ok_or("--vault-ca-cert requires a value")?
                        .clone(),
                );
                i += 2;
            }
            "--daemon" => {
                daemon = true;
                i += 1;
            }
            "--pidfile" => {
                let v = raw.get(i + 1).ok_or("--pidfile requires a value")?;
                pidfile = Some(v.clone());
                i += 2;
            }
            #[cfg(feature = "cluster")]
            "--cluster-node-data" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--cluster-node-data requires a value")?;
                cluster_node_data = Some(v.clone());
                i += 2;
            }
            #[cfg(feature = "cluster")]
            "--cluster-rpc-listen" => {
                let v = raw
                    .get(i + 1)
                    .ok_or("--cluster-rpc-listen requires a value")?;
                cluster_rpc_listen = Some(v.clone());
                i += 2;
            }
            #[cfg(feature = "remote-embedding")]
            "--embedding-provider" => {
                let value = raw
                    .get(i + 1)
                    .ok_or("--embedding-provider requires a value")?;
                embedding_provider_files.push(value.clone());
                i += 2;
            }
            // Flags for compile-time features that are not part of this build
            // fail closed with an explicit error instead of being swallowed as
            // a positional database directory.
            #[cfg(not(feature = "native-rpc"))]
            "--native-port"
            | "--native-listen"
            | "--native-advertise"
            | "--tls-cert"
            | "--tls-key"
            | "--tls-client-ca"
            | "--require-client-cert"
            | "--service-tokens" => {
                return Err(format!(
                    "{arg} requires the `native-rpc` feature (not compiled into this build)"
                ));
            }
            #[cfg(not(all(feature = "oidc", feature = "native-rpc")))]
            "--oidc-issuer" | "--oidc-audience" | "--oidc-allow-host" => {
                return Err(format!(
                    "{arg} requires a build with both the `oidc` and `native-rpc` features"
                ));
            }
            #[cfg(not(feature = "vault-kms"))]
            "--vault-url" | "--vault-mount" | "--vault-key" | "--vault-ca-cert" => {
                return Err(format!(
                    "{arg} requires the `vault-kms` feature (not compiled into this build)"
                ));
            }
            #[cfg(not(feature = "cluster"))]
            "--cluster-node-data" | "--cluster-rpc-listen" => {
                return Err(format!(
                    "{arg} requires the `cluster` feature (not compiled into this build)"
                ));
            }
            #[cfg(not(feature = "remote-embedding"))]
            "--embedding-provider" => {
                return Err(format!(
                    "{arg} requires the `remote-embedding` feature (not compiled into this build)"
                ));
            }
            // Positional: first is db_dir, second (if numeric) is port for backward compat.
            other => {
                if db_dir.is_none() {
                    db_dir = Some(other.to_string());
                } else if port.is_none() {
                    // Treat as backward-compat positional port only if numeric.
                    if let Ok(p) = other.parse::<u16>() {
                        port = Some(p);
                    } else {
                        return Err(format!("unexpected argument: {other}\n\n{USAGE}"));
                    }
                } else {
                    return Err(format!("unexpected argument: {other}\n\n{USAGE}"));
                }
                i += 1;
            }
        }
    }

    let db_dir = db_dir.ok_or_else(|| format!("a database directory is required\n\n{USAGE}"))?;
    let port = port.unwrap_or(DEFAULT_PORT);
    #[cfg(feature = "native-rpc")]
    {
        // Full native listen validation (loopback default, remote TLS requirement)
        // runs later via NativeListenInput so env knobs are included.
        let native_requested = native_port.is_some()
            || native_listen.is_some()
            || tls_certificate.is_some()
            || tls_private_key.is_some()
            || tls_client_ca.is_some()
            || require_client_cert
            || native_advertise.is_some();
        #[cfg(feature = "oidc")]
        let native_auth_requested =
            service_token_file.is_some() || oidc_issuer.is_some() || oidc_audience.is_some();
        #[cfg(not(feature = "oidc"))]
        let native_auth_requested = service_token_file.is_some();
        if native_auth_requested && !native_requested {
            return Err(
                "native authentication options require --native-port / --native-listen \
                 (or MONGRELDB_NATIVE_LISTEN)"
                    .into(),
            );
        }
    }
    #[cfg(all(feature = "oidc", feature = "native-rpc"))]
    {
        if oidc_issuer.is_some() != oidc_audience.is_some() {
            return Err("--oidc-issuer and --oidc-audience must be configured together".into());
        }
        if oidc_issuer.is_some() && oidc_allowed_hosts.is_empty() {
            return Err("--oidc-issuer requires at least one --oidc-allow-host".into());
        }
    }
    #[cfg(feature = "vault-kms")]
    {
        let vault_configured = vault_url.is_some() || vault_mount.is_some() || vault_key.is_some();
        if vault_configured
            && !(vault_url.is_some() && vault_mount.is_some() && vault_key.is_some())
        {
            return Err(
                "--vault-url, --vault-mount, and --vault-key must be configured together".into(),
            );
        }
        if vault_ca_certificate.is_some() && !vault_configured {
            return Err("--vault-ca-cert requires --vault-url".into());
        }
        if passphrase.is_some() && vault_configured {
            return Err("--passphrase and --vault-url are mutually exclusive".into());
        }
    }

    Ok(Args {
        db_dir,
        port,
        auth_token,
        user_auth,
        max_connections,
        max_sessions,
        session_idle_timeout_secs,
        #[cfg(feature = "native-rpc")]
        native_port,
        #[cfg(feature = "native-rpc")]
        native_listen,
        #[cfg(feature = "native-rpc")]
        native_advertise,
        #[cfg(feature = "native-rpc")]
        tls_certificate,
        #[cfg(feature = "native-rpc")]
        tls_private_key,
        #[cfg(feature = "native-rpc")]
        tls_client_ca,
        #[cfg(feature = "native-rpc")]
        require_client_cert,
        #[cfg(feature = "native-rpc")]
        service_token_file,
        #[cfg(all(feature = "oidc", feature = "native-rpc"))]
        oidc_issuer,
        #[cfg(all(feature = "oidc", feature = "native-rpc"))]
        oidc_audience,
        #[cfg(all(feature = "oidc", feature = "native-rpc"))]
        oidc_allowed_hosts,
        passphrase,
        #[cfg(feature = "vault-kms")]
        vault_url,
        #[cfg(feature = "vault-kms")]
        vault_mount,
        #[cfg(feature = "vault-kms")]
        vault_key,
        #[cfg(feature = "vault-kms")]
        vault_ca_certificate,
        daemon,
        pidfile,
        #[cfg(feature = "cluster")]
        cluster_node_data,
        #[cfg(feature = "cluster")]
        cluster_rpc_listen,
        #[cfg(feature = "remote-embedding")]
        embedding_provider_files,
    })
}

#[cfg(feature = "remote-embedding")]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteEmbeddingConfigFile {
    provider_id: String,
    model_id: String,
    model_version: String,
    preprocessing_version: String,
    dimension: u32,
    #[serde(default)]
    normalization: EmbeddingNormalization,
    endpoint: String,
    allowed_hosts: BTreeSet<String>,
    secret_reference: String,
    tenant: String,
    #[serde(default = "default_embedding_timeout_ms")]
    timeout_ms: u64,
    #[serde(default = "default_embedding_max_retries")]
    max_retries: usize,
    #[serde(default = "default_embedding_max_response_bytes")]
    max_response_bytes: usize,
}

#[cfg(feature = "remote-embedding")]
const fn default_embedding_timeout_ms() -> u64 {
    30_000
}

#[cfg(feature = "remote-embedding")]
const fn default_embedding_max_retries() -> usize {
    2
}

#[cfg(feature = "remote-embedding")]
const fn default_embedding_max_response_bytes() -> usize {
    16 * 1024 * 1024
}

#[cfg(feature = "remote-embedding")]
fn load_embedding_providers(
    paths: &[String],
    registry: &EmbeddingProviderRegistry,
) -> Result<(), String> {
    for path in paths {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("embedding-provider file {path}: {error}"))?;
        let config: RemoteEmbeddingConfigFile = serde_json::from_slice(&bytes)
            .map_err(|error| format!("embedding-provider file {path}: {error}"))?;
        let provider_id = config.provider_id.clone();
        let endpoint = reqwest::Url::parse(&config.endpoint)
            .map_err(|error| format!("embedding-provider file {path}: {error}"))?;
        let provider = RemoteEmbeddingProvider::new(
            RemoteEmbeddingConfig {
                provider_id,
                model_id: config.model_id,
                model_version: config.model_version,
                preprocessing_version: config.preprocessing_version,
                dimension: config.dimension,
                normalization: config.normalization,
                endpoint,
                allowed_hosts: config.allowed_hosts,
                secret_reference: config.secret_reference,
                tenant: config.tenant,
                timeout: std::time::Duration::from_millis(config.timeout_ms),
                max_retries: config.max_retries,
                max_response_bytes: config.max_response_bytes,
            },
            Arc::new(EnvironmentSecretResolver),
        )
        .map_err(|error| format!("embedding-provider file {path}: {error}"))?;
        registry
            .register_new(Arc::new(provider))
            .map_err(|error| format!("embedding-provider file {path}: {error}"))?;
    }
    Ok(())
}

/// Resolve the pidfile path: explicit `--pidfile`, else `<db_dir>/mongreldb.pid`.
fn resolve_pidfile(args: &Args) -> String {
    if let Some(ref p) = args.pidfile {
        p.clone()
    } else {
        let mut p = std::path::PathBuf::from(&args.db_dir);
        p.push("mongreldb.pid");
        p.to_string_lossy().into_owned()
    }
}

/// Fork into the background (classic double-fork-style daemonization using
/// `setsid`). The parent writes the child PID to `pidfile` and exits 0.
fn daemonize(pidfile: &str) -> Result<(), String> {
    use std::os::fd::AsRawFd;

    // SAFETY: `fork()` has no preconditions. We check the return value.
    let pid = unsafe { libc::fork() };
    if pid < 0 {
        return Err("fork() failed".to_string());
    }
    if pid > 0 {
        // Parent: write the child PID and exit immediately.
        if let Err(e) = std::fs::write(pidfile, format!("{pid}\n")) {
            eprintln!("warning: could not write pidfile {pidfile}: {e}");
        }
        std::process::exit(0);
    }

    // Child: become a new session leader to detach from the controlling terminal.
    // SAFETY: `setsid()` has no preconditions.
    if unsafe { libc::setsid() } < 0 {
        return Err("setsid() failed".to_string());
    }

    // Redirect stdin/stdout/stderr to /dev/null.
    let devnull = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
        .map_err(|e| format!("open /dev/null: {e}"))?;
    let fd = devnull.as_raw_fd();
    // SAFETY: `dup2` just duplicates an open fd onto the standard streams.
    unsafe {
        libc::dup2(fd, libc::STDIN_FILENO);
        libc::dup2(fd, libc::STDOUT_FILENO);
        libc::dup2(fd, libc::STDERR_FILENO);
    }
    // Keep `devnull` alive for the rest of the process by forgetting it.
    std::mem::forget(devnull);

    Ok(())
}

fn main() {
    // ── Subcommand dispatch ─────────────────────────────────────────────────
    //
    // `snapshot` and `restore` are one-shot maintenance subcommands that
    // operate on the database directory and exit (they do NOT start the HTTP
    // server). Everything else is the daemon mode.
    let raw: Vec<String> = std::env::args().collect();
    if raw.len() >= 2 {
        match raw[1].as_str() {
            "snapshot" => {
                let db_dir = raw.get(2).cloned().unwrap_or_else(|| {
                    eprintln!("usage: mongreldb-server snapshot <db_dir>");
                    std::process::exit(1);
                });
                cmd_snapshot(&db_dir);
                return;
            }
            "restore" => {
                let db_dir = raw.get(2).cloned().unwrap_or_else(|| {
                    eprintln!("usage: mongreldb-server restore <db_dir>");
                    std::process::exit(1);
                });
                cmd_restore(&db_dir);
                return;
            }
            // Cluster bootstrap + membership subcommands (spec §11.1, S2A-002).
            #[cfg(feature = "cluster")]
            "cluster" => {
                cmd_cluster(&raw[2..]);
                return;
            }
            #[cfg(feature = "cluster")]
            "node" => {
                cmd_node(&raw[2..]);
                return;
            }
            #[cfg(not(feature = "cluster"))]
            "cluster" | "node" => {
                eprintln!(
                    "error: `{}` subcommands require the `cluster` feature \
                     (not compiled into this build)",
                    raw[1]
                );
                std::process::exit(1);
            }
            _ => {}
        }
    }

    // ── Daemon mode ─────────────────────────────────────────────────────────
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };
    let database_credentials = match take_database_credentials_from_env() {
        Ok(credentials) => credentials,
        Err(error) => {
            eprintln!("database credentials: {error}");
            std::process::exit(1);
        }
    };
    #[cfg(feature = "vault-kms")]
    let (vault_token, vault_namespace) = match take_vault_environment(args.vault_url.is_some()) {
        Ok(environment) => environment,
        Err(error) => {
            eprintln!("Vault configuration: {error}");
            std::process::exit(1);
        }
    };
    #[cfg(feature = "vault-kms")]
    let vault_ca_certificate = args
        .vault_ca_certificate
        .as_ref()
        .map(std::fs::read)
        .transpose()
        .unwrap_or_else(|error| {
            eprintln!("failed to read Vault CA certificate: {error}");
            std::process::exit(1);
        });

    let pidfile = resolve_pidfile(&args);

    if args.daemon {
        if let Err(e) = daemonize(&pidfile) {
            eprintln!("daemonize failed: {e}");
            std::process::exit(1);
        }
    }

    #[cfg(feature = "vault-kms")]
    let vault_provider = args.vault_url.as_ref().map(|endpoint| {
        VaultTransitKeyManagementProvider::new(VaultTransitConfig {
            endpoint: endpoint.clone(),
            mount: args.vault_mount.clone().unwrap_or_default(),
            token: vault_token.expect("validated Vault configuration requires a token"),
            namespace: vault_namespace,
            timeout: std::time::Duration::from_secs(10),
            ca_certificate_pem: vault_ca_certificate,
        })
        .unwrap_or_else(|error| {
            eprintln!("failed to configure Vault KMS: {error}");
            std::process::exit(1);
        })
    });
    #[cfg(feature = "vault-kms")]
    let kms = vault_provider.as_ref().map(|provider| {
        (
            provider as &dyn mongreldb_core::KeyManagementProvider,
            args.vault_key.as_deref().unwrap_or_default(),
        )
    });
    #[cfg(not(feature = "vault-kms"))]
    let kms: Option<(&dyn mongreldb_core::KeyManagementProvider, &str)> = None;

    // P0.2: cluster mode must not open a peer standalone user database.
    // NodeRuntime owns tablet roots; public data is consensus-owned.
    #[cfg(feature = "cluster")]
    let cluster_configured =
        cluster_runtime::cluster_node_data_from_env(args.cluster_node_data.clone()).is_some();
    #[cfg(not(feature = "cluster"))]
    let cluster_configured = false;

    let standalone_db = if cluster_configured {
        None
    } else {
        // Credential ownership is moved into this call. Its password is
        // zeroized immediately after open/create returns, before workers start.
        let db = Arc::new(
            open_or_create_database(
                &args.db_dir,
                args.passphrase.as_deref(),
                kms,
                database_credentials,
            )
            .unwrap_or_else(|e| {
                eprintln!("failed to open or create {}: {e}", args.db_dir);
                std::process::exit(1);
            }),
        );
        if let Err(error) =
            validate_http_auth_configuration(&db, args.auth_token.as_deref(), args.user_auth)
        {
            eprintln!("failed to start: {error}");
            std::process::exit(1);
        }
        Some(db)
    };
    if cluster_configured {
        if args.user_auth && args.auth_token.is_none() {
            eprintln!(
                "cluster mode: --auth-users requires a standalone catalog which is not opened; \
                 configure --auth-token for control-plane admin, or omit user auth"
            );
            std::process::exit(1);
        }
        eprintln!(
            "cluster mode: not opening standalone user database at {} \
             (public data plane is NodeRuntime/tablet owned)",
            args.db_dir
        );
    }

    // Build Tokio only after the credential environment is cleared and any
    // password-owning `Zeroizing<String>` has been dropped by database open.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|error| {
            eprintln!("failed to start async runtime: {error}");
            if let Some(db) = &standalone_db {
                let _ = db.close();
            }
            std::process::exit(1);
        });
    runtime.block_on(run_server(args, pidfile, standalone_db));
}

#[cfg(feature = "native-rpc")]
fn load_native_external_auth(args: &Args) -> Result<Option<NativeExternalAuth>, String> {
    #[cfg(feature = "oidc")]
    if args.service_token_file.is_none() && args.oidc_issuer.is_none() {
        return Ok(None);
    }
    #[cfg(not(feature = "oidc"))]
    if args.service_token_file.is_none() {
        return Ok(None);
    }
    #[cfg_attr(not(feature = "oidc"), allow(unused_mut))]
    let mut auth = NativeExternalAuth::new();
    if let Some(path) = &args.service_token_file {
        let metadata = std::fs::metadata(path)
            .map_err(|error| format!("service-token file {path}: {error}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if metadata.permissions().mode() & 0o077 != 0 {
                return Err(format!(
                    "service-token file {path} must not be group/world accessible"
                ));
            }
        }
        let tokens: Vec<ServiceToken> = serde_json::from_slice(
            &std::fs::read(path).map_err(|error| format!("service-token file {path}: {error}"))?,
        )
        .map_err(|error| format!("service-token file {path}: {error}"))?;
        let mut ids = BTreeSet::new();
        for token in tokens {
            if !ids.insert(token.token_id.clone()) {
                return Err(format!(
                    "service-token file {path} contains duplicate token id {}",
                    token.token_id
                ));
            }
            auth.upsert_service_token(token)
                .map_err(|error| error.to_string())?;
        }
    }
    #[cfg(feature = "oidc")]
    if let (Some(issuer), Some(audience)) = (&args.oidc_issuer, &args.oidc_audience) {
        let provider = HttpsJwksProvider::new(
            args.oidc_allowed_hosts.clone(),
            std::time::Duration::from_secs(10),
            1024 * 1024,
        )
        .map_err(|error| error.to_string())?;
        auth = auth.with_oidc(
            JwtValidationConfig {
                issuer: issuer.clone(),
                audience: audience.clone(),
                skew_seconds: 60,
                allowed_algorithms: vec![JwtAlgorithm::Rs256, JwtAlgorithm::Es256],
                max_token_age_seconds: 3600,
                required_scopes: Vec::new(),
            },
            provider,
        );
    }
    Ok(Some(auth))
}

async fn run_server(args: Args, pidfile: String, standalone_db: Option<Arc<Database>>) {
    // Cross-request session store for interactive transactions. The reaper
    // shares this Arc so it sweeps the same map the handlers use.
    let sessions = Arc::new(SessionStore::new(
        args.max_sessions,
        std::time::Duration::from_secs(args.session_idle_timeout_secs),
    ));
    spawn_session_reaper(Arc::clone(&sessions));
    #[cfg(feature = "native-rpc")]
    let native_external_auth = load_native_external_auth(&args).unwrap_or_else(|error| {
        eprintln!("failed to configure native authentication: {error}");
        std::process::exit(1);
    });

    // Build the single authoritative storage runtime (P0.2).
    #[cfg(feature = "cluster")]
    let storage = match cluster_runtime::cluster_node_data_from_env(args.cluster_node_data.clone())
    {
        Some(node_data) => {
            if standalone_db.is_some() {
                // Defensive: main must not pass a standalone open in cluster mode.
                eprintln!(
                    "internal error: cluster mode refused dual-root standalone database open"
                );
                std::process::exit(1);
            }
            let options = cluster_runtime::ClusterRuntimeOptions::resolve(
                node_data,
                args.cluster_rpc_listen.clone(),
            );
            eprintln!(
                "cluster mode: starting NodeRuntime from {} (rpc listen {})",
                options.node_data.display(),
                options.rpc_listen
            );
            if let Err(error) = cluster_runtime::admit_plaintext_cluster_transport(
                options.plaintext_test,
                cluster_runtime::plaintext_cluster_transport_allowed(),
            ) {
                // P1.3-T4 / P1.3-X2: production binary refuses plaintext unless
                // this build explicitly admits the test escape hatch.
                eprintln!("{error}");
                std::process::exit(1);
            }
            if options.plaintext_test {
                eprintln!(
                    "WARNING: MONGRELDB_CLUSTER_PLAINTEXT_TEST=1 — plaintext \
                     cluster transport is for tests only (NON-PRODUCTION)"
                );
            }
            let handle = match cluster_runtime::ClusterRuntimeHandle::start(options).await {
                Ok(handle) => handle,
                Err(error) => {
                    eprintln!("failed to start cluster NodeRuntime: {error}");
                    std::process::exit(1);
                }
            };
            match handle.runtime_status_json().await {
                Ok(status) => eprintln!(
                    "cluster runtime live: node_id={} rpc={} meta={} tablets={}",
                    status["node_id"],
                    status["rpc_address"],
                    status["meta_present"],
                    status["tablet_count"]
                ),
                Err(error) => {
                    eprintln!("cluster runtime started but status failed: {error}")
                }
            }
            // P0.2 residual #9: install fragment + AI workers on the product path.
            let (fragment_endpoint, ai_endpoint) =
                match fragment_rpc::install_production_cluster_workers(&handle).await {
                    Ok(endpoints) => endpoints,
                    Err(error) => {
                        eprintln!("failed to install cluster fragment/AI workers: {error}");
                        let _ = handle.shutdown().await;
                        std::process::exit(1);
                    }
                };
            eprintln!("cluster workers installed: fragment service + AI service on internal RPC");
            ServerStorageRuntime::cluster_with_workers(handle, fragment_endpoint, ai_endpoint)
        }
        None => {
            let db = standalone_db.expect("standalone mode opens a local database");
            #[cfg(feature = "remote-embedding")]
            if let Err(error) =
                load_embedding_providers(&args.embedding_provider_files, db.embedding_providers())
            {
                eprintln!("failed to configure embedding providers: {error}");
                std::process::exit(1);
            }
            // §5.9: background cost-aware compaction (run-count trigger).
            spawn_auto_compactor(Arc::clone(&db));
            ServerStorageRuntime::standalone(db)
        }
    };

    // Standalone build (no `cluster` feature): the only storage runtime is a
    // local database.
    #[cfg(not(feature = "cluster"))]
    let storage = {
        let db = standalone_db.expect("standalone mode opens a local database");
        #[cfg(feature = "remote-embedding")]
        if let Err(error) =
            load_embedding_providers(&args.embedding_provider_files, db.embedding_providers())
        {
            eprintln!("failed to configure embedding providers: {error}");
            std::process::exit(1);
        }
        // §5.9: background cost-aware compaction (run-count trigger).
        spawn_auto_compactor(Arc::clone(&db));
        ServerStorageRuntime::standalone(db)
    };

    let standalone_for_native = storage.standalone_db().cloned();
    let (app, server_control) = build_app_with_storage(
        storage,
        std::iter::empty(),
        args.auth_token.clone(),
        args.max_connections,
        args.user_auth,
        Arc::clone(&sessions),
    );
    let (native_result_tx, mut native_result_rx) = tokio::sync::mpsc::channel(1);
    // Holds the sender so `native_result_rx.recv()` pends forever when no
    // native listener is configured (same as a listener that never exits).
    #[cfg_attr(not(feature = "native-rpc"), allow(unused_mut, unused_variables))]
    let mut native_result_guard = Some(native_result_tx);
    #[cfg_attr(not(feature = "native-rpc"), allow(unused_mut))]
    let mut native_shutdown: Option<tokio::sync::oneshot::Sender<()>> = None;
    #[cfg(feature = "native-rpc")]
    let mut native_task = None;
    #[cfg(feature = "native-rpc")]
    {
        let native_listen = mongreldb_server::NativeListenInput::from_cli_and_env(
            mongreldb_server::NativeListenInput {
                listen: args.native_listen.clone(),
                native_port: args.native_port,
                advertise: args.native_advertise.clone(),
                tls_cert: args.tls_certificate.clone(),
                tls_key: args.tls_private_key.clone(),
                tls_ca: args.tls_client_ca.clone(),
                require_client_cert: args.require_client_cert || args.tls_client_ca.is_some(),
            },
        )
        .resolve()
        .unwrap_or_else(|error| {
            eprintln!("native listener configuration error: {error}");
            std::process::exit(1);
        });
        if let Some(native_cfg) = native_listen {
            let Some(db) = standalone_for_native.clone() else {
                eprintln!(
                    "native RPC requires standalone storage; cluster mode does not open a \
                 peer user database (configure native RPC only in standalone mode)"
                );
                std::process::exit(1);
            };
            let read_pem = |path: &std::path::Path| {
                std::fs::read(path).unwrap_or_else(|error| {
                    eprintln!("failed to read native TLS file {}: {error}", path.display());
                    std::process::exit(1);
                })
            };
            let mut runtime = server_control.native_runtime(db, Arc::clone(&sessions));
            if let Some(auth) = native_external_auth {
                runtime = runtime.with_external_auth(auth);
            }
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
            native_shutdown = Some(shutdown_tx);
            let result_tx = native_result_guard.take().expect("native result sender");
            let address = native_cfg.listen;
            let client_ca_pem = if native_cfg.require_client_cert || native_cfg.tls_ca.is_some() {
                Some(read_pem(
                    native_cfg
                        .tls_ca
                        .as_deref()
                        .expect("require_client_cert validated with CA"),
                ))
            } else {
                None
            };
            let server = NativeRpcServer::new(NativeRpcServerConfig {
                address,
                certificate_pem: read_pem(&native_cfg.tls_cert),
                private_key_pem: read_pem(&native_cfg.tls_key),
                client_ca_pem,
                max_connections: args.max_connections.unwrap_or(1_024),
                max_concurrent_streams: 1_024,
                max_in_flight_per_connection: 1_024,
                request_timeout: std::time::Duration::from_secs(30),
                idle_timeout: std::time::Duration::from_secs(args.session_idle_timeout_secs),
                keepalive_interval: std::time::Duration::from_secs(30),
                keepalive_timeout: std::time::Duration::from_secs(10),
            });
            native_task = Some(tokio::spawn(async move {
                let result = server
                    .serve_with_shutdown(
                        NativeRpcServices {
                            auth: runtime.clone(),
                            session: runtime.clone(),
                            query: runtime.clone(),
                            transaction: runtime.clone(),
                            catalog: runtime.clone(),
                            admin: runtime.clone(),
                            health: runtime,
                        },
                        async move {
                            let _ = shutdown_rx.await;
                        },
                    )
                    .await;
                let _ = result_tx
                    .send(result.map_err(|error| error.to_string()))
                    .await;
            }));
            eprintln!("mongreldb native RPC listening on https://{address}");
            if let Some(advertise) = &native_cfg.advertise {
                eprintln!("mongreldb native RPC advertise: {advertise}");
            }
            if !mongreldb_server::is_loopback_addr(address.ip()) {
                eprintln!(
                    "native RPC bound on non-loopback address {address} (TLS required; \
                 certificate reload requires process restart)"
                );
            }
        }
    }

    if args.auth_token.is_some() {
        eprintln!("token authentication enabled (Authorization: Bearer <token>)");
    }
    if args.user_auth {
        eprintln!("user authentication enabled (Authorization: Basic <user:pass>)");
    }
    if let Some(max) = args.max_connections {
        eprintln!("connection limit: {max}");
    }
    eprintln!(
        "sessions: max {} (idle timeout {}s) \u{2014} cross-request txns via X-Session-ID",
        args.max_sessions, args.session_idle_timeout_secs
    );

    let addr = SocketAddr::from(([127, 0, 0, 1], args.port));
    eprintln!("mongreldb-server listening on http://{addr}");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to bind {addr}: {e}");
            std::process::exit(1);
        });

    // Graceful shutdown via tokio::select!. Race the server against SIGINT
    // (ctrl_c) and SIGTERM (unix). Whichever fires first wins; we then flush
    // all tables via `db.close()` before exiting. SIGHUP instead triggers a
    // live reload of the mutable configuration subset (§10.7) via a dedicated
    // task so the signal never terminates the serve loop.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        let mut sighup = signal(SignalKind::hangup()).expect("install SIGHUP handler");
        let reload_control = server_control.clone();
        tokio::spawn(async move {
            while sighup.recv().await.is_some() {
                match reload_control.reload_config() {
                    Ok(report) => eprintln!(
                        "[config] SIGHUP: mutable configuration reloaded: {}",
                        serde_json::to_string(&report)
                            .unwrap_or_else(|_| "<serialize error>".to_string())
                    ),
                    Err(error) => eprintln!("[config] SIGHUP reload failed: {error}"),
                }
            }
        });
        tokio::select! {
            result = axum::serve(listener, app) => {
                if let Err(e) = result {
                    eprintln!("server error: {e}");
                    shutdown_storage(standalone_for_native.as_ref(), &pidfile, args.daemon);
                    std::process::exit(1);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received SIGINT, shutting down gracefully...");
            }
            _ = sigterm.recv() => {
                eprintln!("received SIGTERM, shutting down gracefully...");
            }
            result = native_result_rx.recv() => {
                eprintln!("native RPC server stopped: {}", native_result_message(result));
            }
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            result = axum::serve(listener, app) => {
                if let Err(e) = result {
                    eprintln!("server error: {e}");
                    shutdown_storage(standalone_for_native.as_ref(), &pidfile, args.daemon);
                    std::process::exit(1);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received SIGINT, shutting down gracefully...");
            }
            result = native_result_rx.recv() => {
                eprintln!("native RPC server stopped: {}", native_result_message(result));
            }
        }
    }

    if let Some(shutdown) = native_shutdown {
        let _ = shutdown.send(());
    }
    #[cfg(feature = "native-rpc")]
    if let Some(task) = native_task {
        let _ = task.await;
    }
    let stuck_queries = server_control.shutdown().await;
    if stuck_queries > 0 {
        eprintln!("[shutdown] {stuck_queries} SQL query(s) exceeded cancellation grace");
    }
    shutdown_storage(standalone_for_native.as_ref(), &pidfile, args.daemon);
}

fn native_result_message(result: Option<Result<(), String>>) -> String {
    match result {
        Some(Ok(())) => "listener exited".into(),
        Some(Err(error)) => error,
        None => "result channel closed".into(),
    }
}

/// Flush all tables, checkpoint to a stable on-disk state, and remove the
/// pidfile (if we wrote one). The checkpoint ensures the database directory
/// is deterministic after shutdown — no stale WAL segments, no fragmented
/// runs — so `git status` shows clean when the directory is tracked.
///
/// Cluster mode has no peer standalone database; only the pidfile is cleaned.
fn shutdown_storage(db: Option<&Arc<Database>>, pidfile: &str, daemon: bool) {
    if let Some(db) = db {
        // Checkpoint: flush + compact + reap WAL segments + rotate active segment.
        // This normalizes the on-disk state to a deterministic form.
        match db.checkpoint() {
            Ok(()) => eprintln!("checkpoint complete"),
            Err(e) => {
                // Checkpoint failure is non-fatal during shutdown — fall back to
                // a best-effort close so the process can still exit cleanly.
                eprintln!("checkpoint failed (falling back to close): {e}");
                let _ = db.close();
            }
        }
    } else {
        eprintln!("cluster mode: no standalone database checkpoint (NodeRuntime drained)");
    }
    eprintln!("shutdown complete");
    if daemon {
        let _ = std::fs::remove_file(pidfile);
    }
}

// ── Subcommand handlers ─────────────────────────────────────────────────────

/// `mongreldb-server snapshot <db_dir>`
///
/// Produce a deterministic-stable byte image: flush all writes, compact all
/// tables, reap all WAL segments, rotate to a fresh empty segment. The
/// resulting directory can be safely `git add`-ed and `git checkout`-ed
/// without stale WAL tail bytes or segment count drift.
fn cmd_snapshot(db_dir: &str) {
    let db = match Database::open(db_dir).or_else(|_| Database::create(db_dir)) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: cannot open {}: {e}", db_dir);
            std::process::exit(1);
        }
    };
    match db.checkpoint() {
        Ok(()) => {
            println!("snapshot stable at {}", db_dir);
        }
        Err(e) => {
            eprintln!("error: checkpoint failed: {e}");
            std::process::exit(1);
        }
    }
}

/// `mongreldb-server restore <db_dir>`
///
/// Open the database (replaying any remaining WAL), verify integrity, then
/// checkpoint to a stable state. Use this after `git checkout` to ensure the
/// directory is in a consistent, deterministic state before use.
fn cmd_restore(db_dir: &str) {
    let db = match Database::open(db_dir).or_else(|_| Database::create(db_dir)) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("error: cannot open {}: {e}", db_dir);
            std::process::exit(1);
        }
    };

    // Verify integrity (check for issues like torn writes, checksum mismatches).
    let issues = db.check();
    if !issues.is_empty() {
        eprintln!("warning: {} integrity issue(s) found:", issues.len());
        for issue in &issues {
            eprintln!("  - {:?}", issue);
        }
    }

    match db.checkpoint() {
        Ok(()) => {
            println!("restored and checkpointed at {}", db_dir);
        }
        Err(e) => {
            eprintln!("error: checkpoint failed: {e}");
            std::process::exit(1);
        }
    }
}

// ── Cluster/node subcommands (spec §11.1, S2A-002) ───────────────────────────
//
// One-shot operator commands mapping 1:1 onto the cluster crate's bootstrap
// workflows; they operate on the node data (database) directory and exit
// without starting the HTTP daemon. Trust material is operator-supplied PEM
// (CA generation lands with the mTLS stage), and the node private key is
// never printed: status output uses the cluster crate's key-free
// `TrustSummary`, and `TrustConfig`'s `Debug` redacts the key.

/// PEM filenames inside a `--trust-dir` (default `<data-dir>/trust`).
#[cfg(feature = "cluster")]
const TRUST_CA_CERT_FILENAME: &str = "ca-cert.pem";
#[cfg(feature = "cluster")]
const TRUST_NODE_CERT_FILENAME: &str = "node-cert.pem";
#[cfg(feature = "cluster")]
const TRUST_NODE_KEY_FILENAME: &str = "node-key.pem";

#[cfg(feature = "cluster")]
const CLUSTER_USAGE: &str = "\
mongreldb-server cluster — cluster bootstrap workflows (spec section 11.1, S2A-002)

USAGE:
    mongreldb-server cluster init --data-dir <dir> [options]
    mongreldb-server cluster join --data-dir <dir> --cluster-id <hex> --endpoints <csv> [options]
    mongreldb-server cluster status --data-dir <dir>

OPTIONS:
    --data-dir <dir>          Node data (database) directory (required)
    --endpoints <csv>         Comma-separated member endpoints (host:port); init
                              advertises the first one as its RPC address
    --rpc-address <addr>      init: advertised RPC address (default: first
                              endpoint, else 127.0.0.1:8453)
    --locality <k=v,...>      init: locality tiers (e.g. region=us-central,zone=a)
    --cluster-id <hex>        join: cluster to join (32 hex digits)
    --trust-dir <dir>         Directory holding ca-cert.pem, node-cert.pem, and
                              node-key.pem (default: <data-dir>/trust); CA
                              generation lands with the mTLS stage
    --allowed-node-ids <csv>  Admitted node ids (default: this node; required on
                              first join, which mints the node id during join)
";

#[cfg(feature = "cluster")]
const NODE_USAGE: &str = "\
mongreldb-server node — cluster membership transitions (spec section 11.1, S2A-002)

USAGE:
    mongreldb-server node drain --data-dir <dir> [--node-id <hex>]
    mongreldb-server node remove --data-dir <dir> [--node-id <hex>] [--confirm-token <hex>]

OPTIONS:
    --data-dir <dir>        Node data (database) directory (required)
    --node-id <hex>         Target member (default: this node's own identity)
    --confirm-token <hex>   Removal confirmation token; run without it to print
                            the token and change nothing
";

/// `mongreldb-server cluster ...` — print the report or fail non-zero.
#[cfg(feature = "cluster")]
fn cmd_cluster(args: &[String]) {
    match cluster_command(args) {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    }
}

/// `mongreldb-server node ...` — print the report or fail non-zero.
#[cfg(feature = "cluster")]
fn cmd_node(args: &[String]) {
    match node_command(args) {
        Ok(report) => println!("{report}"),
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    }
}

/// `cluster init|join|status` as a fallible report string (the testable form
/// of [`cmd_cluster`]).
#[cfg(feature = "cluster")]
fn cluster_command(args: &[String]) -> Result<String, String> {
    let Some(subcommand) = args.first() else {
        return Err(format!(
            "a cluster subcommand is required\n\n{CLUSTER_USAGE}"
        ));
    };
    let rest = &args[1..];
    match subcommand.as_str() {
        "init" => cluster_init_command(&parse_subcommand_flags(
            rest,
            CLUSTER_USAGE,
            &[
                "data-dir",
                "endpoints",
                "rpc-address",
                "locality",
                "trust-dir",
                "allowed-node-ids",
            ],
        )?),
        "join" => cluster_join_command(&parse_subcommand_flags(
            rest,
            CLUSTER_USAGE,
            &[
                "data-dir",
                "cluster-id",
                "endpoints",
                "trust-dir",
                "allowed-node-ids",
            ],
        )?),
        "status" => {
            cluster_status_command(&parse_subcommand_flags(rest, CLUSTER_USAGE, &["data-dir"])?)
        }
        other => Err(format!(
            "unknown cluster subcommand `{other}`\n\n{CLUSTER_USAGE}"
        )),
    }
}

/// `node drain|remove` as a fallible report string (the testable form of
/// [`cmd_node`]).
#[cfg(feature = "cluster")]
fn node_command(args: &[String]) -> Result<String, String> {
    let Some(subcommand) = args.first() else {
        return Err(format!("a node subcommand is required\n\n{NODE_USAGE}"));
    };
    let rest = &args[1..];
    match subcommand.as_str() {
        "drain" => node_drain_command(&parse_subcommand_flags(
            rest,
            NODE_USAGE,
            &["data-dir", "node-id"],
        )?),
        "remove" => node_remove_command(&parse_subcommand_flags(
            rest,
            NODE_USAGE,
            &["data-dir", "node-id", "confirm-token"],
        )?),
        other => Err(format!("unknown node subcommand `{other}`\n\n{NODE_USAGE}")),
    }
}

/// Parsed `--flag value` pairs of one cluster/node subcommand.
#[cfg(feature = "cluster")]
type SubcommandFlags = BTreeMap<String, String>;

/// Parse subcommand arguments as `--flag value` pairs, rejecting positionals,
/// unknown flags, and missing values with the subcommand's usage text.
#[cfg(feature = "cluster")]
fn parse_subcommand_flags(
    args: &[String],
    usage: &str,
    allowed: &[&'static str],
) -> Result<SubcommandFlags, String> {
    let mut values = SubcommandFlags::new();
    let mut i = 0;
    while i < args.len() {
        let arg = &args[i];
        let Some(name) = arg.strip_prefix("--") else {
            return Err(format!("unexpected argument `{arg}`\n\n{usage}"));
        };
        if !allowed.contains(&name) {
            return Err(format!("unknown flag `--{name}`\n\n{usage}"));
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| format!("--{name} requires a value"))?;
        values.insert(name.to_owned(), value.clone());
        i += 2;
    }
    Ok(values)
}

#[cfg(feature = "cluster")]
fn required_flag<'a>(
    flags: &'a SubcommandFlags,
    name: &str,
    usage: &str,
) -> Result<&'a str, String> {
    flags
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("--{name} is required\n\n{usage}"))
}

#[cfg(feature = "cluster")]
fn optional_flag<'a>(flags: &'a SubcommandFlags, name: &str) -> Option<&'a str> {
    flags.get(name).map(String::as_str)
}

/// Load operator-supplied PEM trust material from a trust directory,
/// validating it through the cluster crate (fails closed).
#[cfg(feature = "cluster")]
fn load_trust_config(
    trust_dir: &Path,
    allowed_node_ids: Vec<NodeId>,
) -> Result<TrustConfig, String> {
    let read_pem = |filename: &str| -> Result<String, String> {
        let path = trust_dir.join(filename);
        std::fs::read_to_string(&path).map_err(|error| {
            format!(
                "cannot read cluster trust material {}: {error}",
                path.display()
            )
        })
    };
    TrustConfig::from_pems(
        read_pem(TRUST_CA_CERT_FILENAME)?,
        read_pem(TRUST_NODE_CERT_FILENAME)?,
        read_pem(TRUST_NODE_KEY_FILENAME)?,
        allowed_node_ids,
    )
    .map_err(|error| error.to_string())
}

#[cfg(feature = "cluster")]
fn trust_dir_for(flags: &SubcommandFlags, data_path: &Path) -> PathBuf {
    optional_flag(flags, "trust-dir")
        .map(PathBuf::from)
        .unwrap_or_else(|| data_path.join("trust"))
}

/// Parse a comma-separated node-id list (`--allowed-node-ids`).
#[cfg(feature = "cluster")]
fn parse_node_id_list(text: Option<&str>) -> Result<Option<Vec<NodeId>>, String> {
    let Some(text) = text else {
        return Ok(None);
    };
    let mut ids = Vec::new();
    for part in text
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        ids.push(
            part.parse::<NodeId>()
                .map_err(|error| format!("invalid node id `{part}`: {error}"))?,
        );
    }
    Ok(Some(ids))
}

/// Parse a comma-separated endpoint list (`--endpoints`), rejecting an
/// effectively empty list before the cluster crate sees it.
#[cfg(feature = "cluster")]
fn parse_endpoints(text: &str) -> Result<Vec<String>, String> {
    let endpoints: Vec<String> = text
        .split(',')
        .map(str::trim)
        .filter(|endpoint| !endpoint.is_empty())
        .map(str::to_owned)
        .collect();
    if endpoints.is_empty() {
        return Err("--endpoints names no usable endpoint".to_owned());
    }
    Ok(endpoints)
}

/// Resolve the member `node drain`/`node remove` targets: the explicit
/// `--node-id`, else this node's own persisted identity.
#[cfg(feature = "cluster")]
fn resolve_cli_node_id(data_path: &Path, requested: Option<&str>) -> Result<NodeId, String> {
    match requested {
        Some(text) => text
            .parse::<NodeId>()
            .map_err(|error| format!("invalid --node-id `{text}`: {error}")),
        None => NodeIdentity::load(data_path)
            .map_err(|error| error.to_string())?
            .map(|identity| identity.node_id)
            .ok_or_else(|| {
                "node has no cluster identity; pass --node-id explicitly or run \
                 `mongreldb-server cluster init` first"
                    .to_owned()
            }),
    }
}

/// Pretty-print one JSON report value.
#[cfg(feature = "cluster")]
fn json_report(value: serde_json::Value) -> String {
    serde_json::to_string_pretty(&value).expect("cluster report serialization")
}

/// `cluster init`: create the cluster on this node — cluster ID, initial
/// membership, the single database Raft group, and the trust configuration.
#[cfg(feature = "cluster")]
fn cluster_init_command(flags: &SubcommandFlags) -> Result<String, String> {
    let data_dir = required_flag(flags, "data-dir", CLUSTER_USAGE)?;
    let data_path = Path::new(data_dir);
    // OS-CSPRNG block source for identifier minting, drawn through the shared
    // id type's `new_random` so the server needs no direct `getrandom`
    // dependency; `new_random` panics if the OS CSPRNG is unavailable, which
    // matches the bootstrap code's fail-closed posture.
    let mut csprng = |buf: &mut [u8]| {
        for chunk in buf.chunks_mut(16) {
            let block = NodeId::new_random();
            chunk.copy_from_slice(&block.as_bytes()[..chunk.len()]);
        }
        Ok(())
    };
    // Pre-provision the identity so the default admitted-node list can name
    // this node; `cluster_init` adopts a persisted identity unchanged.
    let identity =
        NodeIdentity::load_or_create(data_path, &mut csprng).map_err(|error| error.to_string())?;
    let allowed_node_ids = parse_node_id_list(optional_flag(flags, "allowed-node-ids"))?
        .unwrap_or_else(|| vec![identity.node_id]);
    let trust = load_trust_config(&trust_dir_for(flags, data_path), allowed_node_ids)?;
    let endpoints = match optional_flag(flags, "endpoints") {
        Some(text) => parse_endpoints(text)?,
        None => Vec::new(),
    };
    let rpc_address = optional_flag(flags, "rpc-address")
        .map(str::to_owned)
        .or_else(|| endpoints.first().cloned())
        .unwrap_or_else(|| format!("127.0.0.1:{DEFAULT_PORT}"));
    let locality = match optional_flag(flags, "locality") {
        Some(text) => text
            .parse::<Locality>()
            .map_err(|error| format!("invalid --locality `{text}`: {error}"))?,
        None => Locality::default(),
    };
    let request = InitRequest {
        rpc_address,
        locality,
        capacity: NodeCapacity::default(),
        trust,
    };
    let report =
        cluster_init(data_path, &request, &mut csprng).map_err(|error| error.to_string())?;
    Ok(json_report(json!({
        "cluster_id": report.record.cluster_id,
        "node_id": report.identity.node_id,
        "rpc_address": report.record.members[0].rpc_address,
        "database_group": report.record.database_group,
        "members": report.record.members.len(),
    })))
}

/// `cluster join`: validate an invite (cluster ID, member endpoints, trust
/// material) and provision this node for the invited cluster.
#[cfg(feature = "cluster")]
fn cluster_join_command(flags: &SubcommandFlags) -> Result<String, String> {
    let data_dir = required_flag(flags, "data-dir", CLUSTER_USAGE)?;
    let data_path = Path::new(data_dir);
    let cluster_id = required_flag(flags, "cluster-id", CLUSTER_USAGE)?
        .parse::<ClusterId>()
        .map_err(|error| format!("invalid --cluster-id: {error}"))?;
    let member_endpoints = parse_endpoints(required_flag(flags, "endpoints", CLUSTER_USAGE)?)?;
    let allowed_node_ids = match parse_node_id_list(optional_flag(flags, "allowed-node-ids"))? {
        Some(ids) => ids,
        None => match NodeIdentity::load(data_path).map_err(|error| error.to_string())? {
            Some(identity) => vec![identity.node_id],
            None => {
                return Err(
                    "--allowed-node-ids is required on first join: the node id is minted \
                     during join, so the admitted node list must come from the inviting \
                     operator"
                        .to_owned(),
                )
            }
        },
    };
    let trust = load_trust_config(&trust_dir_for(flags, data_path), allowed_node_ids)?;
    let invite = JoinInvite {
        cluster_id,
        member_endpoints,
        trust,
    };
    let mut csprng = |buf: &mut [u8]| {
        for chunk in buf.chunks_mut(16) {
            let block = NodeId::new_random();
            chunk.copy_from_slice(&block.as_bytes()[..chunk.len()]);
        }
        Ok(())
    };
    let report =
        cluster_join(data_path, &invite, &mut csprng).map_err(|error| error.to_string())?;
    Ok(json_report(json!({
        "cluster_id": report.record.cluster_id,
        "node_id": report.identity.node_id,
        "member_endpoints": report.record.member_endpoints,
    })))
}

/// `cluster status`: identity, membership, and group descriptors; a directory
/// without a cluster identity reports `standalone`.
#[cfg(feature = "cluster")]
fn cluster_status_command(flags: &SubcommandFlags) -> Result<String, String> {
    let data_dir = required_flag(flags, "data-dir", CLUSTER_USAGE)?;
    let report =
        cluster_admin::status_report(Path::new(data_dir)).map_err(|error| error.to_string())?;
    Ok(json_report(report))
}

/// `node drain`: move a member from `Up` to `Draining` in the persisted
/// membership record.
#[cfg(feature = "cluster")]
fn node_drain_command(flags: &SubcommandFlags) -> Result<String, String> {
    let data_dir = required_flag(flags, "data-dir", NODE_USAGE)?;
    let data_path = Path::new(data_dir);
    let node_id = resolve_cli_node_id(data_path, optional_flag(flags, "node-id"))?;
    let updated = node_drain(data_path, node_id).map_err(|error| error.to_string())?;
    Ok(json_report(json!({ "member": updated })))
}

/// `node remove`: move a member to `Decommissioned`. Without
/// `--confirm-token` this prints the out-of-band confirmation token and
/// changes nothing.
#[cfg(feature = "cluster")]
fn node_remove_command(flags: &SubcommandFlags) -> Result<String, String> {
    let data_dir = required_flag(flags, "data-dir", NODE_USAGE)?;
    let data_path = Path::new(data_dir);
    let node_id = resolve_cli_node_id(data_path, optional_flag(flags, "node-id"))?;
    match optional_flag(flags, "confirm-token") {
        Some(token) => {
            let updated =
                node_remove(data_path, node_id, token).map_err(|error| error.to_string())?;
            Ok(json_report(json!({ "removed": true, "member": updated })))
        }
        None => {
            let identity = NodeIdentity::load(data_path)
                .map_err(|error| error.to_string())?
                .ok_or_else(|| {
                    "node has no cluster identity; run `mongreldb-server cluster init` or \
                     `cluster join` first"
                        .to_owned()
                })?;
            let token = removal_confirmation_token(identity.cluster_id, node_id);
            Ok(json_report(json!({
                "removed": false,
                "detail": "confirmation required; re-run with --confirm-token to remove the node",
                "cluster_id": identity.cluster_id,
                "node_id": node_id,
                "confirm_token": token,
            })))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENVIRONMENT_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn credentials(username: &str, password: &str) -> DatabaseCredentials {
        database_credentials_from_values(
            Some(OsString::from(username)),
            Some(OsString::from(password)),
        )
        .unwrap()
        .unwrap()
    }

    #[test]
    #[cfg(feature = "remote-embedding")]
    fn remote_embedding_provider_files_register_named_models() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("embedding.json");
        std::fs::write(
            &path,
            r#"{
                "provider_id":"tenant-embeddings",
                "model_id":"text-model",
                "model_version":"2026-07",
                "preprocessing_version":"1",
                "dimension":384,
                "normalization":"l2",
                "endpoint":"https://models.example.test/v1/embeddings",
                "allowed_hosts":["models.example.test"],
                "secret_reference":"MODEL_API_TOKEN",
                "tenant":"tenant-a"
            }"#,
        )
        .unwrap();
        let registry = EmbeddingProviderRegistry::new();
        load_embedding_providers(&[path.to_string_lossy().into_owned()], &registry).unwrap();
        let status = registry.status("tenant-embeddings").unwrap();
        assert_eq!(status.model_id, "text-model");
        assert_eq!(status.model_version, "2026-07");
    }

    #[test]
    fn database_credentials_must_be_paired_nonempty_utf8() {
        assert!(database_credentials_from_values(Some(OsString::from("admin")), None).is_err());
        assert!(database_credentials_from_values(None, Some(OsString::from("password"))).is_err());
        assert!(database_credentials_from_values(
            Some(OsString::from("")),
            Some(OsString::from("password"))
        )
        .is_err());
        assert!(database_credentials_from_values(
            Some(OsString::from("admin")),
            Some(OsString::from(""))
        )
        .is_err());
    }

    #[test]
    fn database_credentials_are_removed_from_the_environment() {
        let _guard = ENVIRONMENT_TEST_LOCK.lock().unwrap();
        std::env::set_var("MONGRELDB_DB_USERNAME", "admin");
        std::env::set_var("MONGRELDB_DB_PASSWORD", "database-password");

        let credentials = take_database_credentials_from_env().unwrap().unwrap();

        assert_eq!(credentials.username, "admin");
        assert_eq!(credentials.password.as_str(), "database-password");
        assert!(std::env::var_os("MONGRELDB_DB_USERNAME").is_none());
        assert!(std::env::var_os("MONGRELDB_DB_PASSWORD").is_none());
    }

    #[test]
    fn credentialed_plain_database_create_reopen_and_http_auth_validation() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let database = open_or_create_database(
            path,
            None,
            None,
            Some(credentials("admin", "database-password")),
        )
        .unwrap();
        assert!(database.require_auth_enabled());
        assert_eq!(
            database.principal_snapshot().unwrap().username,
            "admin".to_string()
        );
        assert!(validate_http_auth_configuration(&database, None, false).is_err());
        assert!(validate_http_auth_configuration(&database, Some("token"), false).is_ok());
        assert!(validate_http_auth_configuration(&database, None, true).is_ok());
        drop(database);

        let reopened = open_or_create_database(
            path,
            None,
            None,
            Some(credentials("admin", "database-password")),
        )
        .unwrap();
        assert_eq!(reopened.principal_snapshot().unwrap().username, "admin");
        drop(reopened);

        assert!(open_or_create_database(
            path,
            None,
            None,
            Some(credentials("admin", "wrong-password")),
        )
        .is_err());
    }

    #[test]
    fn credentialed_encrypted_database_create_and_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().to_str().unwrap();
        let database = open_or_create_database(
            path,
            Some("encryption-passphrase"),
            None,
            Some(credentials("admin", "database-password")),
        )
        .unwrap();
        assert!(database.require_auth_enabled());
        drop(database);

        let reopened = open_or_create_database(
            path,
            Some("encryption-passphrase"),
            None,
            Some(credentials("admin", "database-password")),
        )
        .unwrap();
        assert_eq!(reopened.principal_snapshot().unwrap().username, "admin");
    }

    // ── Cluster/node subcommand tests (spec §11.1, S2A-002) ─────────────────

    #[cfg(feature = "cluster")]
    const CA_PEM: &str = "-----BEGIN CERTIFICATE-----\nY2E=\n-----END CERTIFICATE-----\n";
    #[cfg(feature = "cluster")]
    const CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nbm9kZQ==\n-----END CERTIFICATE-----\n";
    #[cfg(feature = "cluster")]
    const KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nc2VjcmV0\n-----END PRIVATE KEY-----\n";

    /// Write operator-style PEM trust material under `<data>/trust` (the
    /// default `--trust-dir`).
    #[cfg(feature = "cluster")]
    fn write_trust_dir(data_path: &Path) {
        let trust = data_path.join("trust");
        std::fs::create_dir_all(&trust).unwrap();
        std::fs::write(trust.join(TRUST_CA_CERT_FILENAME), CA_PEM).unwrap();
        std::fs::write(trust.join(TRUST_NODE_CERT_FILENAME), CERT_PEM).unwrap();
        std::fs::write(trust.join(TRUST_NODE_KEY_FILENAME), KEY_PEM).unwrap();
    }

    #[cfg(feature = "cluster")]
    fn cli_args(args: &[&str]) -> Vec<String> {
        args.iter().map(|arg| arg.to_string()).collect()
    }

    #[cfg(feature = "cluster")]
    fn json_stdout(output: &str) -> serde_json::Value {
        serde_json::from_str(output)
            .unwrap_or_else(|error| panic!("stdout is not JSON: {error}\n{output}"))
    }

    /// `cluster init` on a fresh directory; returns `(cluster_id, node_id)`.
    #[cfg(feature = "cluster")]
    fn init_cluster(data_path: &Path) -> (String, String) {
        let data_dir = data_path.to_str().unwrap();
        let output = cluster_command(&cli_args(&[
            "init",
            "--data-dir",
            data_dir,
            "--endpoints",
            "10.0.0.1:8453,10.0.0.2:8453",
        ]))
        .unwrap();
        let report = json_stdout(&output);
        (
            report["cluster_id"].as_str().unwrap().to_owned(),
            report["node_id"].as_str().unwrap().to_owned(),
        )
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn cluster_init_creates_identity_record_group_and_never_prints_key_material() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        write_trust_dir(&data);

        let output = cluster_command(&cli_args(&[
            "init",
            "--data-dir",
            data.to_str().unwrap(),
            "--endpoints",
            "10.0.0.1:8453,10.0.0.2:8453",
            "--locality",
            "region=test,zone=a",
        ]))
        .unwrap();
        let report = json_stdout(&output);
        let cluster_id = report["cluster_id"].as_str().unwrap();
        let node_id = report["node_id"].as_str().unwrap();
        assert_eq!(cluster_id.len(), 32, "{report}");
        assert_eq!(node_id.len(), 32, "{report}");
        assert_eq!(report["rpc_address"], "10.0.0.1:8453");
        assert_eq!(report["members"], 1);
        assert_eq!(
            report["database_group"]["voter_ids"],
            serde_json::json!([node_id])
        );

        let meta = data.join("cluster-meta");
        assert!(meta.join("identity.json").is_file());
        assert!(meta.join("cluster.json").is_file());
        assert!(meta.join("trust.json").is_file());

        // Status reports the bootstrapped cluster and never leaks the node key.
        let status =
            cluster_command(&cli_args(&["status", "--data-dir", data.to_str().unwrap()])).unwrap();
        assert!(
            !status.contains("c2VjcmV0"),
            "key material leaked: {status}"
        );
        let status = json_stdout(&status);
        assert_eq!(status["mode"], "cluster");
        assert_eq!(status["identity"]["cluster_id"], cluster_id);
        assert_eq!(status["identity"]["node_id"], node_id);
        assert_eq!(status["membership"].as_array().unwrap().len(), 1);
        assert_eq!(status["membership"][0]["state"], "Up");
        assert_eq!(status["membership"][0]["locality"], "region=test,zone=a");
        assert_eq!(
            status["database_group"]["raft_group_id"]
                .as_str()
                .unwrap()
                .len(),
            32
        );
        assert_eq!(status["trust"]["has_node_key"], true);
        assert_eq!(
            status["version_info"]["binary_version"],
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn cluster_init_twice_and_bad_flags_fail_closed() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        write_trust_dir(&data);
        let data_dir = data.to_str().unwrap();

        cluster_command(&cli_args(&["init", "--data-dir", data_dir])).unwrap();
        let error = cluster_command(&cli_args(&["init", "--data-dir", data_dir])).unwrap_err();
        assert!(error.contains("already bootstrapped"), "{error}");

        let error = cluster_command(&cli_args(&["init"])).unwrap_err();
        assert!(error.contains("--data-dir is required"), "{error}");
        let error = cluster_command(&cli_args(&["init", "--data-dir", data_dir, "--wat", "1"]))
            .unwrap_err();
        assert!(error.contains("unknown flag `--wat`"), "{error}");
        let error = cluster_command(&cli_args(&["frobnicate"])).unwrap_err();
        assert!(error.contains("unknown cluster subcommand"), "{error}");
        let error = cluster_command(&cli_args(&[])).unwrap_err();
        assert!(error.contains("subcommand is required"), "{error}");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn cluster_init_requires_readable_trust_material() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        let error = cluster_command(&cli_args(&["init", "--data-dir", data.to_str().unwrap()]))
            .unwrap_err();
        assert!(
            error.contains("cannot read cluster trust material"),
            "{error}"
        );
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn cluster_join_provisions_and_reports() {
        let directory = tempfile::tempdir().unwrap();
        let data_a = directory.path().join("a");
        let data_b = directory.path().join("b");
        write_trust_dir(&data_a);
        write_trust_dir(&data_b);
        let (cluster_id, node_id_a) = init_cluster(&data_a);

        let output = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_b.to_str().unwrap(),
            "--cluster-id",
            &cluster_id,
            "--endpoints",
            "10.0.0.1:8453",
            "--allowed-node-ids",
            &node_id_a,
        ]))
        .unwrap();
        let report = json_stdout(&output);
        assert_eq!(report["cluster_id"], cluster_id);
        let node_id_b = report["node_id"].as_str().unwrap();
        assert_eq!(node_id_b.len(), 32);
        assert_ne!(node_id_b, node_id_a);
        assert_eq!(
            report["member_endpoints"],
            serde_json::json!(["10.0.0.1:8453"])
        );

        // A joined node reports the validated invite: no local membership or
        // database group until the meta group lands (Stage 2F/3A).
        let status = cluster_command(&cli_args(&[
            "status",
            "--data-dir",
            data_b.to_str().unwrap(),
        ]))
        .unwrap();
        let status = json_stdout(&status);
        assert_eq!(status["mode"], "cluster");
        assert_eq!(status["identity"]["cluster_id"], cluster_id);
        assert_eq!(status["identity"]["node_id"], node_id_b);
        assert!(status["membership"].as_array().unwrap().is_empty());
        assert_eq!(
            status["member_endpoints"],
            serde_json::json!(["10.0.0.1:8453"])
        );
        assert!(status["database_group"].is_null());

        // Joining twice fails closed.
        let error = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_b.to_str().unwrap(),
            "--cluster-id",
            &cluster_id,
            "--endpoints",
            "10.0.0.1:8453",
            "--allowed-node-ids",
            &node_id_a,
        ]))
        .unwrap_err();
        assert!(error.contains("already bootstrapped"), "{error}");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn cluster_join_rejects_bad_invites_and_mismatched_identity() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        write_trust_dir(&data);
        let data_dir = data.to_str().unwrap();
        let some_node = NodeId::new_random().to_hex();

        // The reserved all-zero cluster id is rejected.
        let error = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_dir,
            "--cluster-id",
            "00000000000000000000000000000000",
            "--endpoints",
            "10.0.0.1:8453",
            "--allowed-node-ids",
            &some_node,
        ]))
        .unwrap_err();
        assert!(error.contains("reserved zero"), "{error}");
        // Required flags are required.
        let error = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_dir,
            "--endpoints",
            "10.0.0.1:8453",
        ]))
        .unwrap_err();
        assert!(error.contains("--cluster-id is required"), "{error}");
        let error = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_dir,
            "--cluster-id",
            &ClusterId::new_random().to_hex(),
        ]))
        .unwrap_err();
        assert!(error.contains("--endpoints is required"), "{error}");
        // First join without --allowed-node-ids cannot default the trust list.
        let error = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_dir,
            "--cluster-id",
            &ClusterId::new_random().to_hex(),
            "--endpoints",
            "10.0.0.1:8453",
        ]))
        .unwrap_err();
        assert!(error.contains("--allowed-node-ids is required"), "{error}");

        // A persisted identity binds the node to its cluster (S2A-001).
        let mut csprng = |buf: &mut [u8]| {
            for chunk in buf.chunks_mut(16) {
                let block = NodeId::new_random();
                chunk.copy_from_slice(&block.as_bytes()[..chunk.len()]);
            }
            Ok(())
        };
        let persisted = NodeIdentity::load_or_create(&data, &mut csprng).unwrap();
        let mut other = ClusterId::new_random();
        while other == persisted.cluster_id {
            other = ClusterId::new_random();
        }
        let error = cluster_command(&cli_args(&[
            "join",
            "--data-dir",
            data_dir,
            "--cluster-id",
            &other.to_hex(),
            "--endpoints",
            "10.0.0.1:8453",
        ]))
        .unwrap_err();
        assert!(error.contains("cluster identity mismatch"), "{error}");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn cluster_status_on_uninitialized_directory_reports_standalone() {
        let directory = tempfile::tempdir().unwrap();
        let output = cluster_command(&cli_args(&[
            "status",
            "--data-dir",
            directory.path().to_str().unwrap(),
        ]))
        .unwrap();
        let status = json_stdout(&output);
        assert_eq!(status["mode"], "standalone");
        assert_eq!(
            status["version_info"]["binary_version"],
            env!("CARGO_PKG_VERSION")
        );
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn node_drain_and_remove_transition_membership_with_token_enforcement() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        write_trust_dir(&data);
        let data_dir = data.to_str().unwrap();
        let (_cluster_id, node_id) = init_cluster(&data);

        // Drain defaults to this node's own identity.
        let output = node_command(&cli_args(&["drain", "--data-dir", data_dir])).unwrap();
        let report = json_stdout(&output);
        assert_eq!(report["member"]["node_id"], node_id);
        assert_eq!(report["member"]["state"], "Draining");
        // Draining again is not a legal transition.
        let error = node_command(&cli_args(&["drain", "--data-dir", data_dir])).unwrap_err();
        assert!(error.contains("invalid node state transition"), "{error}");

        // Remove without the token prints it and changes nothing.
        let output = node_command(&cli_args(&["remove", "--data-dir", data_dir])).unwrap();
        let report = json_stdout(&output);
        assert_eq!(report["removed"], false);
        assert_eq!(report["node_id"], node_id);
        let token = report["confirm_token"].as_str().unwrap().to_owned();
        assert_eq!(token.len(), 64);
        let status = mongreldb_cluster::bootstrap::cluster_status(&data).unwrap();
        assert_eq!(
            status.membership[0].state,
            mongreldb_cluster::node::NodeState::Draining,
            "token printing must not change membership"
        );

        // A wrong token fails closed; the right token decommissions.
        let error = node_command(&cli_args(&[
            "remove",
            "--data-dir",
            data_dir,
            "--confirm-token",
            "not-the-token",
        ]))
        .unwrap_err();
        assert!(error.contains("confirmation token"), "{error}");
        let output = node_command(&cli_args(&[
            "remove",
            "--data-dir",
            data_dir,
            "--confirm-token",
            &token,
        ]))
        .unwrap();
        let report = json_stdout(&output);
        assert_eq!(report["removed"], true);
        assert_eq!(report["member"]["state"], "Decommissioned");
        let status = mongreldb_cluster::bootstrap::cluster_status(&data).unwrap();
        assert_eq!(
            status.membership[0].state,
            mongreldb_cluster::node::NodeState::Decommissioned
        );
        // Removing twice is not a legal transition.
        let error = node_command(&cli_args(&[
            "remove",
            "--data-dir",
            data_dir,
            "--confirm-token",
            &token,
        ]))
        .unwrap_err();
        assert!(error.contains("invalid node state transition"), "{error}");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn node_commands_require_bootstrap() {
        let directory = tempfile::tempdir().unwrap();
        let data_dir = directory.path().to_str().unwrap();

        // Without an identity the target member cannot be defaulted.
        let error = node_command(&cli_args(&["drain", "--data-dir", data_dir])).unwrap_err();
        assert!(error.contains("no cluster identity"), "{error}");
        let error = node_command(&cli_args(&["remove", "--data-dir", data_dir])).unwrap_err();
        assert!(error.contains("no cluster identity"), "{error}");
        // An explicit target on an unbootstrapped directory is NotInitialized.
        let some_node = NodeId::new_random().to_hex();
        let error = node_command(&cli_args(&[
            "drain",
            "--data-dir",
            data_dir,
            "--node-id",
            &some_node,
        ]))
        .unwrap_err();
        assert!(error.contains("not initialized"), "{error}");
        let error = node_command(&cli_args(&[
            "remove",
            "--data-dir",
            data_dir,
            "--node-id",
            &some_node,
            "--confirm-token",
            "token",
        ]))
        .unwrap_err();
        assert!(error.contains("not initialized"), "{error}");
    }

    #[test]
    #[cfg(feature = "cluster")]
    fn trust_config_debug_redacts_the_node_key() {
        let trust = TrustConfig::from_pems(
            CA_PEM.to_owned(),
            CERT_PEM.to_owned(),
            KEY_PEM.to_owned(),
            vec![NodeId::new_random()],
        )
        .unwrap();
        let debug = format!("{trust:?}");
        assert!(!debug.contains("c2VjcmV0"), "key material leaked: {debug}");
        assert!(debug.contains("<redacted>"), "{debug}");
    }
}
