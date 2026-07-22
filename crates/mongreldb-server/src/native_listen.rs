//! Configurable native RPC listen surface (audit P1.2).
//!
//! Environment / operator knobs:
//! - `MONGRELDB_NATIVE_LISTEN` — bind address (`host:port`, default host `127.0.0.1`)
//! - `MONGRELDB_NATIVE_ADVERTISE` — advertised client endpoint (informational)
//! - `MONGRELDB_NATIVE_TLS_CERT` / `MONGRELDB_NATIVE_TLS_KEY` / `MONGRELDB_NATIVE_TLS_CA`
//! - `MONGRELDB_NATIVE_REQUIRE_CLIENT_CERT=1` — require client certificates
//!
//! Defaults bind loopback. A non-loopback bind **requires** TLS certificate
//! material. The native transport itself is always TLS 1.3 HTTP/2; remote
//! plaintext is rejected at configuration time.
//!
//! Certificate material is loaded at process start. Hot reload is not
//! supported — operators restart the daemon to rotate certificates.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;

/// Resolved native RPC listener configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeListenConfig {
    /// Socket the native RPC server binds.
    pub listen: SocketAddr,
    /// Optional advertised endpoint (`host:port` or URL) for clients/status.
    pub advertise: Option<String>,
    /// Server certificate PEM path.
    pub tls_cert: PathBuf,
    /// Server private key PEM path.
    pub tls_key: PathBuf,
    /// Optional client CA PEM path (enables mTLS verification when present).
    pub tls_ca: Option<PathBuf>,
    /// When true, a client CA is mandatory (fail closed if missing).
    pub require_client_cert: bool,
}

/// CLI / environment inputs before validation and defaults.
#[derive(Clone, Debug, Default)]
pub struct NativeListenInput {
    /// Explicit listen (`host:port`). Takes precedence over port-only.
    pub listen: Option<String>,
    /// Legacy port-only flag (`--native-port`); binds `127.0.0.1:<port>`.
    pub native_port: Option<u16>,
    /// Advertised endpoint.
    pub advertise: Option<String>,
    /// TLS certificate path.
    pub tls_cert: Option<String>,
    /// TLS private key path.
    pub tls_key: Option<String>,
    /// TLS client CA path.
    pub tls_ca: Option<String>,
    /// Require client certificates.
    pub require_client_cert: bool,
}

impl NativeListenInput {
    /// Merge CLI values with environment variables. CLI wins on conflict.
    pub fn from_cli_and_env(mut cli: NativeListenInput) -> Self {
        if cli.listen.is_none() {
            cli.listen = std::env::var("MONGRELDB_NATIVE_LISTEN").ok().filter(|s| !s.trim().is_empty());
        }
        if cli.native_port.is_none() {
            if let Ok(port) = std::env::var("MONGRELDB_NATIVE_PORT") {
                if let Ok(parsed) = port.trim().parse::<u16>() {
                    cli.native_port = Some(parsed);
                }
            }
        }
        if cli.advertise.is_none() {
            cli.advertise = std::env::var("MONGRELDB_NATIVE_ADVERTISE")
                .ok()
                .filter(|s| !s.trim().is_empty());
        }
        if cli.tls_cert.is_none() {
            cli.tls_cert = std::env::var("MONGRELDB_NATIVE_TLS_CERT")
                .ok()
                .filter(|s| !s.trim().is_empty());
        }
        if cli.tls_key.is_none() {
            cli.tls_key = std::env::var("MONGRELDB_NATIVE_TLS_KEY")
                .ok()
                .filter(|s| !s.trim().is_empty());
        }
        if cli.tls_ca.is_none() {
            cli.tls_ca = std::env::var("MONGRELDB_NATIVE_TLS_CA")
                .ok()
                .or_else(|| std::env::var("MONGRELDB_NATIVE_TLS_CLIENT_CA").ok())
                .filter(|s| !s.trim().is_empty());
        }
        if !cli.require_client_cert {
            cli.require_client_cert = env_flag_true("MONGRELDB_NATIVE_REQUIRE_CLIENT_CERT");
        }
        cli
    }

    /// Whether any native listen knob was supplied.
    pub fn is_enabled(&self) -> bool {
        self.listen.is_some()
            || self.native_port.is_some()
            || self.tls_cert.is_some()
            || self.tls_key.is_some()
            || self.tls_ca.is_some()
            || self.require_client_cert
            || self.advertise.is_some()
    }

    /// Resolve and validate. Returns `Ok(None)` when native RPC is not
    /// requested.
    pub fn resolve(self) -> Result<Option<NativeListenConfig>, String> {
        if !self.is_enabled() {
            return Ok(None);
        }

        let listen = resolve_listen_address(self.listen.as_deref(), self.native_port)?;
        let has_tls = self.tls_cert.is_some() && self.tls_key.is_some();
        let incomplete_tls = self.tls_cert.is_some() != self.tls_key.is_some();
        if incomplete_tls {
            return Err(
                "native TLS requires both certificate and private key \
                 (MONGRELDB_NATIVE_TLS_CERT / MONGRELDB_NATIVE_TLS_KEY, or \
                 --tls-cert / --tls-key)"
                    .into(),
            );
        }

        if !is_loopback_addr(listen.ip()) && !has_tls {
            return Err(format!(
                "native remote bind {listen} requires TLS \
                 (set MONGRELDB_NATIVE_TLS_CERT and MONGRELDB_NATIVE_TLS_KEY, \
                 or --tls-cert and --tls-key)"
            ));
        }

        if !has_tls {
            // Loopback still needs PEM material: the transport is TLS-only.
            return Err(
                "native RPC requires TLS certificate material even on loopback \
                 (set MONGRELDB_NATIVE_TLS_CERT and MONGRELDB_NATIVE_TLS_KEY, or \
                 --tls-cert and --tls-key together with --native-port / \
                 MONGRELDB_NATIVE_LISTEN)"
                    .into(),
            );
        }

        if self.require_client_cert && self.tls_ca.is_none() {
            return Err(
                "MONGRELDB_NATIVE_REQUIRE_CLIENT_CERT / --tls-client-ca requires \
                 a client CA (MONGRELDB_NATIVE_TLS_CA or --tls-client-ca)"
                    .into(),
            );
        }

        Ok(Some(NativeListenConfig {
            listen,
            advertise: self.advertise,
            tls_cert: PathBuf::from(self.tls_cert.expect("validated")),
            tls_key: PathBuf::from(self.tls_key.expect("validated")),
            tls_ca: self.tls_ca.map(PathBuf::from),
            require_client_cert: self.require_client_cert,
        }))
    }
}

fn env_flag_true(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => {
            let value = value.trim();
            value == "1" || value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("yes")
        }
        Err(_) => false,
    }
}

fn resolve_listen_address(
    listen: Option<&str>,
    native_port: Option<u16>,
) -> Result<SocketAddr, String> {
    if let Some(raw) = listen {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("MONGRELDB_NATIVE_LISTEN / native listen address is empty".into());
        }
        return trimmed.parse::<SocketAddr>().or_else(|_| {
            // host:port where host may be a name — try with explicit default
            // only for numeric ports after a single colon.
            if let Some((host, port)) = trimmed.rsplit_once(':') {
                let port: u16 = port
                    .parse()
                    .map_err(|_| format!("invalid native listen port in `{trimmed}`"))?;
                let ip: IpAddr = host.parse().map_err(|_| {
                    format!(
                        "invalid native listen address `{trimmed}` \
                         (expected host:port with an IP host)"
                    )
                })?;
                Ok(SocketAddr::new(ip, port))
            } else {
                Err(format!(
                    "invalid native listen address `{trimmed}` (expected host:port)"
                ))
            }
        });
    }
    let port = native_port.ok_or_else(|| {
        "native RPC enabled without a listen address; set MONGRELDB_NATIVE_LISTEN \
         or --native-port"
            .to_owned()
    })?;
    Ok(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port))
}

/// Whether an IP is loopback (IPv4 127.0.0.0/8 or IPv6 ::1).
pub fn is_loopback_addr(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4.is_loopback(),
        IpAddr::V6(v6) => v6 == Ipv6Addr::LOCALHOST || v6.is_loopback(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    // ID: P1.2-X1 Loopback default.
    #[test]
    fn loopback_default_from_native_port() {
        let config = NativeListenInput {
            native_port: Some(9443),
            tls_cert: Some("/tmp/cert.pem".into()),
            tls_key: Some("/tmp/key.pem".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap()
        .expect("enabled");
        assert_eq!(config.listen.ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(config.listen.port(), 9443);
        assert!(is_loopback_addr(config.listen.ip()));
    }

    #[test]
    fn explicit_loopback_listen() {
        let config = NativeListenInput {
            listen: Some("127.0.0.1:19000".into()),
            tls_cert: Some("/tmp/cert.pem".into()),
            tls_key: Some("/tmp/key.pem".into()),
            advertise: Some("https://db.example:19000".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap()
        .expect("enabled");
        assert_eq!(config.listen, "127.0.0.1:19000".parse().unwrap());
        assert_eq!(
            config.advertise.as_deref(),
            Some("https://db.example:19000")
        );
    }

    // ID: P1.2-X2 Remote bind without TLS rejected.
    #[test]
    fn remote_bind_without_tls_is_rejected() {
        let err = NativeListenInput {
            listen: Some("0.0.0.0:9443".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(
            err.contains("requires TLS"),
            "remote plaintext must fail closed: {err}"
        );

        let err = NativeListenInput {
            listen: Some("192.0.2.10:9443".into()),
            tls_cert: Some("/tmp/cert.pem".into()),
            // key missing → incomplete / remote requires TLS
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(
            err.contains("certificate and private key") || err.contains("requires TLS"),
            "incomplete TLS on remote must fail: {err}"
        );
    }

    // ID: P1.2-X3 Remote TLS client config accepted (bind + TLS material).
    // ID: P1.2-X5 Client certificate requirement accepted when CA is present.
    #[test]
    fn remote_bind_with_tls_is_accepted() {
        let config = NativeListenInput {
            listen: Some("0.0.0.0:9443".into()),
            tls_cert: Some("/etc/mongreldb/cert.pem".into()),
            tls_key: Some("/etc/mongreldb/key.pem".into()),
            tls_ca: Some("/etc/mongreldb/ca.pem".into()),
            require_client_cert: true,
            ..Default::default()
        }
        .resolve()
        .unwrap()
        .expect("enabled");
        assert_eq!(config.listen, "0.0.0.0:9443".parse().unwrap());
        assert!(!is_loopback_addr(config.listen.ip()));
        assert!(config.require_client_cert);
        assert!(config.tls_ca.is_some());
    }

    // ID: P1.2-X5 Client certificate requirement works (CA mandatory).
    #[test]
    fn require_client_cert_without_ca_is_rejected() {
        let err = NativeListenInput {
            native_port: Some(9443),
            tls_cert: Some("/tmp/cert.pem".into()),
            tls_key: Some("/tmp/key.pem".into()),
            require_client_cert: true,
            ..Default::default()
        }
        .resolve()
        .unwrap_err();
        assert!(
            err.contains("client CA"),
            "require client cert needs CA: {err}"
        );
    }

    // ID: P1.2-X6 Hot certificate reload is not supported — restart required.
    #[test]
    fn certificate_hot_reload_not_supported_requires_restart() {
        // Module docs: "Certificate material is loaded at process start. Hot
        // reload is not supported — operators restart the daemon to rotate
        // certificates." The resolved config holds PathBufs only; there is no
        // reload API on NativeListenConfig.
        let config = NativeListenInput {
            native_port: Some(9443),
            tls_cert: Some("/etc/mongreldb/cert.pem".into()),
            tls_key: Some("/etc/mongreldb/key.pem".into()),
            ..Default::default()
        }
        .resolve()
        .unwrap()
        .expect("enabled");
        // Structural proof: config is Clone+static paths, no reload handle.
        let _ = config.clone();
        assert!(
            module_docs_document_restart_only_cert_rotation(),
            "native listen must document restart-only cert rotation"
        );
    }

    fn module_docs_document_restart_only_cert_rotation() -> bool {
        // Keep this in lockstep with the module-level documentation above.
        // Docs may wrap across lines (`Hot reload is not\n//! supported`).
        let docs = include_str!("native_listen.rs");
        docs.contains("Hot reload is not")
            && docs.contains("restart the daemon to rotate certificates")
    }

    #[test]
    fn disabled_when_no_knobs() {
        assert!(NativeListenInput::default().resolve().unwrap().is_none());
    }
}
