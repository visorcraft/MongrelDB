//! Fail-closed HTTPS OIDC discovery and JWKS retrieval.

use std::collections::BTreeSet;
use std::io::Read;
use std::time::Duration;

use mongreldb_core::{JwksDocument, JwksFetch, JwksProvider, JwtError};
use serde::Deserialize;

#[derive(Clone)]
pub struct HttpsJwksProvider {
    client: reqwest::blocking::Client,
    allowed_hosts: BTreeSet<String>,
    max_response_bytes: usize,
    default_max_age_seconds: u64,
}

impl HttpsJwksProvider {
    pub fn new(
        allowed_hosts: BTreeSet<String>,
        timeout: Duration,
        max_response_bytes: usize,
    ) -> Result<Self, JwtError> {
        if allowed_hosts.is_empty() || timeout.is_zero() || max_response_bytes == 0 {
            return Err(JwtError::JwksProvider("invalid OIDC network policy".into()));
        }
        let client = reqwest::blocking::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(timeout)
            .build()
            .map_err(jwks_error)?;
        Ok(Self {
            client,
            allowed_hosts,
            max_response_bytes,
            default_max_age_seconds: 300,
        })
    }

    fn validate_url(&self, url: &reqwest::Url) -> Result<(), JwtError> {
        let host = url
            .host_str()
            .ok_or_else(|| JwtError::JwksProvider("OIDC URL has no host".into()))?;
        if url.scheme() != "https"
            || !self.allowed_hosts.contains(host)
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(JwtError::JwksProvider(
                "OIDC URL violates HTTPS/egress policy".into(),
            ));
        }
        Ok(())
    }

    fn get(&self, url: reqwest::Url) -> Result<(Vec<u8>, u64), JwtError> {
        self.validate_url(&url)?;
        let response = self.client.get(url).send().map_err(jwks_error)?;
        if !response.status().is_success() {
            return Err(JwtError::JwksProvider(format!(
                "OIDC endpoint returned HTTP {}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.max_response_bytes as u64)
        {
            return Err(JwtError::JwksProvider("OIDC response is too large".into()));
        }
        let max_age = response
            .headers()
            .get(reqwest::header::CACHE_CONTROL)
            .and_then(|value| value.to_str().ok())
            .and_then(cache_max_age)
            .unwrap_or(self.default_max_age_seconds)
            .clamp(1, 86_400);
        let mut bytes = Vec::new();
        response
            .take(self.max_response_bytes as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(jwks_error)?;
        if bytes.len() > self.max_response_bytes {
            return Err(JwtError::JwksProvider("OIDC response is too large".into()));
        }
        Ok((bytes, max_age))
    }
}

impl JwksProvider for HttpsJwksProvider {
    fn fetch(&self, issuer: &str) -> Result<JwksFetch, JwtError> {
        let issuer = reqwest::Url::parse(issuer)
            .map_err(|_| JwtError::JwksProvider("invalid OIDC issuer".into()))?;
        self.validate_url(&issuer)?;
        let discovery = issuer
            .join(".well-known/openid-configuration")
            .map_err(|_| JwtError::JwksProvider("invalid OIDC discovery URL".into()))?;
        let (document, _) = self.get(discovery)?;
        let discovery: DiscoveryDocument = serde_json::from_slice(&document).map_err(jwks_error)?;
        let jwks_url = reqwest::Url::parse(&discovery.jwks_uri)
            .map_err(|_| JwtError::JwksProvider("invalid jwks_uri".into()))?;
        let (document, max_age_seconds) = self.get(jwks_url)?;
        Ok(JwksFetch {
            document: serde_json::from_slice::<JwksDocument>(&document).map_err(jwks_error)?,
            max_age_seconds,
        })
    }
}

#[derive(Deserialize)]
struct DiscoveryDocument {
    jwks_uri: String,
}

fn cache_max_age(value: &str) -> Option<u64> {
    value.split(',').find_map(|directive| {
        directive
            .trim()
            .strip_prefix("max-age=")?
            .parse::<u64>()
            .ok()
    })
}

fn jwks_error(error: impl std::fmt::Display) -> JwtError {
    JwtError::JwksProvider(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_control_parser_is_exact() {
        assert_eq!(cache_max_age("public, max-age=600"), Some(600));
        assert_eq!(cache_max_age("public, s-maxage=600"), None);
        assert_eq!(cache_max_age("max-age=bad"), None);
    }

    #[test]
    fn provider_rejects_non_https_and_non_allowlisted_issuers() {
        let provider = HttpsJwksProvider::new(
            ["issuer.example".into()].into_iter().collect(),
            Duration::from_secs(1),
            1024,
        )
        .unwrap();
        assert!(provider.fetch("http://issuer.example").is_err());
        assert!(provider.fetch("https://other.example").is_err());
    }
}
