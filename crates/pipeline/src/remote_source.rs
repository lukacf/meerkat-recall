//! Bounded, SSRF-resistant retrieval for remote source material.
//!
//! Redirects are followed manually so every destination is subject to the
//! same network policy. DNS results are validated before a request and pinned
//! into the request client to narrow the DNS-rebinding window.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use reqwest::header::{
    CONTENT_LENGTH, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE, IF_NONE_MATCH, LAST_MODIFIED, LOCATION,
};
use reqwest::{StatusCode, Url};
use thiserror::Error;

/// Safety and resource limits for a remote source request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RemoteFetchPolicy {
    /// Maximum accepted response body size.
    pub max_bytes: usize,
    /// Total request timeout for each redirect hop.
    pub timeout: Duration,
    /// Maximum number of redirects to follow.
    pub max_redirects: usize,
    /// Permit loopback and non-public addresses. Intended only for controlled tests.
    pub allow_private_networks: bool,
}

impl RemoteFetchPolicy {
    /// Policy for user-requested URL ingestion, including bounded media files.
    #[must_use]
    pub const fn ingest() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024,
            timeout: Duration::from_secs(120),
            max_redirects: 5,
            allow_private_networks: false,
        }
    }

    /// Policy for recurring textual web-source checks.
    #[must_use]
    pub const fn web_watch() -> Self {
        Self {
            max_bytes: 16 * 1024 * 1024,
            timeout: Duration::from_secs(30),
            max_redirects: 5,
            allow_private_networks: false,
        }
    }
}

/// Validators supplied by a prior successful fetch.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RemoteValidators {
    /// HTTP entity tag.
    pub etag: Option<String>,
    /// HTTP Last-Modified value.
    pub last_modified: Option<String>,
}

/// Successful result of a remote source fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteFetchOutcome {
    /// The server confirmed that the prior representation is still current.
    NotModified {
        /// Final URL after validated redirects.
        final_url: Url,
        /// Validators returned by the server, falling back to the request values.
        validators: RemoteValidators,
    },
    /// A complete, bounded representation was retrieved.
    Snapshot(RemoteSnapshot),
}

/// A retrieved remote representation and its provenance metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSnapshot {
    /// Final URL after validated redirects.
    pub final_url: Url,
    /// Parsed media type without parameters, if supplied by the server.
    pub content_type: Option<String>,
    /// HTTP response validators.
    pub validators: RemoteValidators,
    /// Complete response bytes, bounded by the fetch policy.
    pub bytes: Vec<u8>,
}

/// Remote retrieval failures.
#[derive(Debug, Error)]
pub enum RemoteFetchError {
    /// URL could not be parsed or violates the URL policy.
    #[error("invalid remote source URL: {0}")]
    InvalidUrl(String),
    /// Destination resolves to an address outside the allowed network policy.
    #[error("remote source destination is not permitted: {0}")]
    DestinationDenied(String),
    /// DNS resolution failed or returned no addresses.
    #[error("remote source DNS resolution failed: {0}")]
    Resolution(String),
    /// HTTP request failed.
    #[error("remote source request failed: {0}")]
    Request(String),
    /// Server returned a non-success response.
    #[error("remote source returned HTTP status {0}")]
    HttpStatus(StatusCode),
    /// Redirect response was incomplete or exceeded policy.
    #[error("remote source redirect failed: {0}")]
    Redirect(String),
    /// Response body exceeded its configured limit.
    #[error("remote source body exceeds {limit} bytes")]
    BodyTooLarge {
        /// Configured response limit.
        limit: usize,
    },
}

/// Fetches one remote representation under an explicit network and size policy.
///
/// # Errors
///
/// Returns an error for invalid or unsafe destinations, network failures,
/// redirect policy violations, non-success statuses, and oversized bodies.
pub async fn fetch_remote_source(
    raw_url: &str,
    validators: &RemoteValidators,
    policy: RemoteFetchPolicy,
) -> Result<RemoteFetchOutcome, RemoteFetchError> {
    if policy.max_bytes == 0 {
        return Err(RemoteFetchError::InvalidUrl(
            "max_bytes must be greater than zero".to_string(),
        ));
    }

    let mut url =
        Url::parse(raw_url).map_err(|error| RemoteFetchError::InvalidUrl(error.to_string()))?;
    let mut redirects = 0usize;

    loop {
        let destination = validate_and_resolve(&url, policy.allow_private_networks).await?;
        let host = url
            .host_str()
            .ok_or_else(|| RemoteFetchError::InvalidUrl("URL has no host".to_string()))?;

        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(policy.timeout)
            .connect_timeout(policy.timeout)
            .resolve(host, destination)
            .build()
            .map_err(|error| RemoteFetchError::Request(error.to_string()))?;

        let mut request = client.get(url.clone());
        if let Some(etag) = validators.etag.as_deref() {
            request = request.header(IF_NONE_MATCH, etag);
        }
        if let Some(last_modified) = validators.last_modified.as_deref() {
            request = request.header(IF_MODIFIED_SINCE, last_modified);
        }

        let mut response = request
            .send()
            .await
            .map_err(|error| RemoteFetchError::Request(error.to_string()))?;

        if response.status() == StatusCode::NOT_MODIFIED {
            return Ok(RemoteFetchOutcome::NotModified {
                final_url: url,
                validators: RemoteValidators {
                    etag: header_string(response.headers(), ETAG)
                        .or_else(|| validators.etag.clone()),
                    last_modified: header_string(response.headers(), LAST_MODIFIED)
                        .or_else(|| validators.last_modified.clone()),
                },
            });
        }

        if response.status().is_redirection() {
            if redirects >= policy.max_redirects {
                return Err(RemoteFetchError::Redirect(format!(
                    "more than {} redirects",
                    policy.max_redirects
                )));
            }
            let location = response
                .headers()
                .get(LOCATION)
                .ok_or_else(|| {
                    RemoteFetchError::Redirect("response omitted Location header".to_string())
                })?
                .to_str()
                .map_err(|error| RemoteFetchError::Redirect(error.to_string()))?;
            url = url
                .join(location)
                .map_err(|error| RemoteFetchError::Redirect(error.to_string()))?;
            redirects += 1;
            continue;
        }

        let response_validators = RemoteValidators {
            etag: header_string(response.headers(), ETAG).or_else(|| validators.etag.clone()),
            last_modified: header_string(response.headers(), LAST_MODIFIED)
                .or_else(|| validators.last_modified.clone()),
        };

        if !response.status().is_success() {
            return Err(RemoteFetchError::HttpStatus(response.status()));
        }

        if response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|length| length > policy.max_bytes as u64)
        {
            return Err(RemoteFetchError::BodyTooLarge {
                limit: policy.max_bytes,
            });
        }

        let content_type = header_string(response.headers(), CONTENT_TYPE).map(|value| {
            value
                .split(';')
                .next()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase()
        });
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| RemoteFetchError::Request(error.to_string()))?
        {
            let new_len =
                bytes
                    .len()
                    .checked_add(chunk.len())
                    .ok_or(RemoteFetchError::BodyTooLarge {
                        limit: policy.max_bytes,
                    })?;
            if new_len > policy.max_bytes {
                return Err(RemoteFetchError::BodyTooLarge {
                    limit: policy.max_bytes,
                });
            }
            bytes.extend_from_slice(&chunk);
        }

        return Ok(RemoteFetchOutcome::Snapshot(RemoteSnapshot {
            final_url: url,
            content_type,
            validators: response_validators,
            bytes,
        }));
    }
}

fn header_string(
    headers: &reqwest::header::HeaderMap,
    name: reqwest::header::HeaderName,
) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

async fn validate_and_resolve(
    url: &Url,
    allow_private_networks: bool,
) -> Result<SocketAddr, RemoteFetchError> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(RemoteFetchError::InvalidUrl(
            "only http and https schemes are supported".to_string(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(RemoteFetchError::InvalidUrl(
            "embedded credentials are not supported".to_string(),
        ));
    }
    let host = url
        .host_str()
        .ok_or_else(|| RemoteFetchError::InvalidUrl("URL has no host".to_string()))?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| RemoteFetchError::InvalidUrl("URL has no usable port".to_string()))?;
    let addresses: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| RemoteFetchError::Resolution(error.to_string()))?
        .collect();
    if addresses.is_empty() {
        return Err(RemoteFetchError::Resolution(
            "host resolved to no addresses".to_string(),
        ));
    }
    if !allow_private_networks
        && addresses
            .iter()
            .any(|address| !is_public_destination(address.ip()))
    {
        return Err(RemoteFetchError::DestinationDenied(host.to_string()));
    }
    addresses
        .into_iter()
        .next()
        .ok_or_else(|| RemoteFetchError::Resolution("host resolved to no addresses".to_string()))
}

fn is_public_destination(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => is_public_ipv4(address),
        IpAddr::V6(address) => is_public_ipv6(address),
    }
}

fn is_public_ipv4(address: Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    !(a == 0
        || a == 10
        || a == 127
        || (a == 100 && (64..=127).contains(&b))
        || (a == 169 && b == 254)
        || (a == 172 && (16..=31).contains(&b))
        || (a == 192 && b == 0 && c == 0)
        || (a == 192 && b == 0 && c == 2)
        || (a == 192 && b == 168)
        || (a == 198 && (b == 18 || b == 19))
        || (a == 198 && b == 51 && c == 100)
        || (a == 203 && b == 0 && c == 113)
        || a >= 224)
}

fn is_public_ipv6(address: Ipv6Addr) -> bool {
    let segments = address.segments();
    if address.is_unspecified() || address.is_loopback() || address.is_multicast() {
        return false;
    }
    if segments[0] & 0xfe00 == 0xfc00 || segments[0] & 0xffc0 == 0xfe80 {
        return false;
    }
    if segments[0] == 0x2001 && segments[1] == 0x0db8 {
        return false;
    }
    address.to_ipv4_mapped().is_none_or(is_public_ipv4)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn network_policy_rejects_non_public_ranges() {
        for address in [
            "127.0.0.1",
            "10.0.0.1",
            "172.16.1.1",
            "192.168.1.1",
            "169.254.169.254",
            "100.64.0.1",
            "192.0.2.1",
            "198.51.100.1",
            "203.0.113.1",
            "::1",
            "fe80::1",
            "fc00::1",
            "2001:db8::1",
            "::ffff:127.0.0.1",
        ] {
            let parsed = address
                .parse::<IpAddr>()
                .expect("test address should parse");
            assert!(!is_public_destination(parsed), "accepted {address}");
        }
    }

    #[test]
    fn network_policy_accepts_public_ranges() {
        for address in ["1.1.1.1", "8.8.8.8", "2606:4700:4700::1111"] {
            let parsed = address
                .parse::<IpAddr>()
                .expect("test address should parse");
            assert!(is_public_destination(parsed), "rejected {address}");
        }
    }

    #[tokio::test]
    async fn production_policy_rejects_loopback_fetches() {
        let error = fetch_remote_source(
            "http://127.0.0.1:9/private",
            &RemoteValidators::default(),
            RemoteFetchPolicy::web_watch(),
        )
        .await
        .expect_err("loopback must be denied before connection");
        assert!(matches!(error, RemoteFetchError::DestinationDenied(_)));
    }

    #[tokio::test]
    async fn conditional_fetch_records_not_modified() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 2048];
            let read = socket.read(&mut request).await.expect("read request");
            let request = String::from_utf8_lossy(&request[..read]);
            assert!(request.contains("if-none-match: \"v1\""));
            socket
                .write_all(
                    b"HTTP/1.1 304 Not Modified\r\nETag: \"v1\"\r\nContent-Length: 0\r\n\r\n",
                )
                .await
                .expect("write response");
        });

        let outcome = fetch_remote_source(
            &format!("http://{address}/document"),
            &RemoteValidators {
                etag: Some("\"v1\"".to_string()),
                last_modified: None,
            },
            RemoteFetchPolicy {
                allow_private_networks: true,
                ..RemoteFetchPolicy::web_watch()
            },
        )
        .await
        .expect("conditional fetch");
        assert!(matches!(
            outcome,
            RemoteFetchOutcome::NotModified { validators, .. }
                if validators.etag.as_deref() == Some("\"v1\"")
        ));
    }

    #[tokio::test]
    async fn streaming_limit_rejects_oversized_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 1024];
            let _ = socket.read(&mut request).await.expect("read request");
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\nhello",
                )
                .await
                .expect("write response");
        });
        let error = fetch_remote_source(
            &format!("http://{address}/too-large"),
            &RemoteValidators::default(),
            RemoteFetchPolicy {
                max_bytes: 4,
                allow_private_networks: true,
                ..RemoteFetchPolicy::web_watch()
            },
        )
        .await
        .expect_err("body must exceed configured limit");
        assert!(matches!(error, RemoteFetchError::BodyTooLarge { limit: 4 }));
    }
}
