//! Shared HTTP client helpers.

use crate::error::LlmError;
#[cfg(not(target_arch = "wasm32"))]
use std::net::IpAddr;

#[allow(dead_code)]
pub fn build_http_client_for_base_url(
    builder: reqwest::ClientBuilder,
    base_url: &str,
) -> Result<reqwest::Client, LlmError> {
    // no_proxy is not available on wasm32 (browser handles proxies)
    #[cfg(not(target_arch = "wasm32"))]
    let builder = {
        let disable_proxy = cfg!(test) || is_loopback_base_url(base_url);
        if disable_proxy {
            builder.no_proxy()
        } else {
            builder
        }
    };
    #[cfg(target_arch = "wasm32")]
    let _ = base_url; // suppress unused warning

    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder.redirect(reqwest::redirect::Policy::none());

    builder.build().map_err(|e| LlmError::Unknown {
        message: format!("Failed to build HTTP client: {e}"),
    })
}

#[cfg(not(target_arch = "wasm32"))]
#[allow(dead_code)]
fn is_loopback_base_url(base_url: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(base_url) else {
        return false;
    };
    let Some(host) = url.host_str() else {
        return false;
    };
    let normalized_host = host.trim_matches(&['[', ']'][..]);
    normalized_host.eq_ignore_ascii_case("localhost")
        || normalized_host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::is_loopback_base_url;

    #[test]
    fn test_is_loopback_base_url_localhost() {
        assert!(is_loopback_base_url("http://localhost:8080"));
    }

    #[test]
    fn test_is_loopback_base_url_ipv4() {
        assert!(is_loopback_base_url("http://127.0.0.1:8080"));
    }

    #[test]
    fn test_is_loopback_base_url_ipv6() {
        assert!(is_loopback_base_url("http://[::1]:8080"));
    }

    #[test]
    fn test_is_loopback_base_url_non_loopback() {
        assert!(!is_loopback_base_url("https://api.openai.com"));
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn model_transport_does_not_follow_endpoint_redirects() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let approved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unapproved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", approved.local_addr().unwrap());
        let location = format!("http://{}/collect", unapproved.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = approved.accept().await.unwrap();
            let mut request = [0; 8192];
            let _ = socket.read(&mut request).await.unwrap();
            let response = format!(
                "HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            socket.write_all(response.as_bytes()).await.unwrap();
        });
        let client =
            super::build_http_client_for_base_url(reqwest::Client::builder(), &base).unwrap();
        let response = client
            .post(format!("{base}/model"))
            .body("private fixture prompt")
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::TEMPORARY_REDIRECT);
        server.await.unwrap();
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(20), unapproved.accept())
                .await
                .is_err()
        );
    }
}
