//! How a server URL and optional TLS settings become a connection.
//!
//! The client, the worker's drivers and the actor clients all decide TLS here,
//! so they cannot disagree about it.

use crate::client::ClientTlsConfig;
use tonic::transport::Endpoint;

/// Whether `url` uses the `https` scheme.
pub(crate) fn is_https(url: &str) -> bool {
    url.trim()
        .get(..8)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("https://"))
}

/// The TLS settings to use for `url`.
///
/// Explicit settings always win; that is how a private CA or a client
/// certificate is supplied. Otherwise the scheme decides: `https://` gets TLS
/// verified against the system trust store, since nobody writing `https://`
/// means plaintext, and `http://` gets none.
pub(crate) fn tls_for_url(
    url: &str,
    explicit: Option<&ClientTlsConfig>,
) -> Option<ClientTlsConfig> {
    match explicit {
        Some(tls) => Some(tls.clone()),
        None if is_https(url) => Some(ClientTlsConfig::new()),
        None => None,
    }
}

/// `error` followed by each cause in its source chain.
///
/// tonic reports a failed TLS handshake as a bare "transport error"; why it
/// failed (an untrusted certificate, a client certificate the server
/// required) is only in the chain.
pub(crate) fn with_causes(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

/// The same settings in the form sdk-core's drivers take.
pub(crate) fn to_core(tls: &ClientTlsConfig) -> orcher_sdk_core::poller::TlsConfig {
    let mut core = orcher_sdk_core::poller::TlsConfig::new();
    if let Some(ref ca) = tls.ca_cert {
        core = core.with_ca_cert(ca.clone());
    }
    if let (Some(ref cert), Some(ref key)) = (&tls.client_cert, &tls.client_key) {
        core = core.with_client_identity(cert.clone(), key.clone());
    }
    if let Some(ref domain) = tls.domain_name {
        core = core.with_domain_name(domain.clone());
    }
    core
}

/// Checks settings that would otherwise be dropped without a word.
///
/// Returns a reason when they are unusable for `url`; logs a warning for TLS
/// settings on an `http://` address, which tonic dials in plaintext.
pub(crate) fn validate(url: &str, tls: Option<&ClientTlsConfig>) -> Result<(), String> {
    let Some(tls) = tls else {
        return Ok(());
    };
    if tls.client_cert.is_some() != tls.client_key.is_some() {
        return Err(
            "TLS client certificate and key must be set together (with_client_identity)"
                .to_string(),
        );
    }
    if !is_https(url) {
        tracing::warn!(
            server_url = %url,
            "TLS settings are ignored for a non-https:// address; the connection is plaintext. Use https:// to enable TLS"
        );
    }
    Ok(())
}

/// Builds the endpoint for `url`, with TLS as [`tls_for_url`] decides.
///
/// Returns a reason when the address is invalid or the TLS settings are
/// rejected (for example, PEM that does not parse).
pub(crate) fn endpoint(url: &str, explicit: Option<&ClientTlsConfig>) -> Result<Endpoint, String> {
    validate(url, explicit)?;
    let endpoint =
        Endpoint::from_shared(url.to_string()).map_err(|e| format!("Invalid server URL: {}", e))?;
    let Some(tls) = tls_for_url(url, explicit) else {
        return Ok(endpoint);
    };

    // Without a custom CA, verify against the system trust store. mTLS to a
    // publicly-trusted server then needs only the client identity.
    let mut tls_config = match tls.ca_cert {
        Some(ref ca) => tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(ca)),
        None => tonic::transport::ClientTlsConfig::new().with_native_roots(),
    };
    if let (Some(ref cert), Some(ref key)) = (&tls.client_cert, &tls.client_key) {
        tls_config = tls_config.identity(tonic::transport::Identity::from_pem(cert, key));
    }
    if let Some(ref domain) = tls.domain_name {
        tls_config = tls_config.domain_name(domain.clone());
    }
    endpoint
        .tls_config(tls_config)
        .map_err(|e| format!("TLS configuration error: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn https_gets_tls_by_default_and_http_does_not() {
        assert!(tls_for_url("https://orcher.example:443", None).is_some());
        assert!(tls_for_url("  HTTPS://orcher.example", None).is_some());
        assert!(tls_for_url("http://localhost:50051", None).is_none());
        assert!(tls_for_url("httpsx", None).is_none());
    }

    #[test]
    fn explicit_settings_win_over_the_scheme() {
        let tls = ClientTlsConfig::new().with_domain_name("orcher.internal");
        let chosen = tls_for_url("https://10.0.0.1", Some(&tls)).unwrap();
        assert_eq!(chosen.domain_name.as_deref(), Some("orcher.internal"));
    }

    #[test]
    fn to_core_carries_every_setting() {
        let tls = ClientTlsConfig::new()
            .with_ca_cert(b"ca".to_vec())
            .with_client_identity(b"cert".to_vec(), b"key".to_vec())
            .with_domain_name("orcher.internal");
        let core = to_core(&tls);
        assert_eq!(core.ca_cert.as_deref(), Some(&b"ca"[..]));
        assert_eq!(core.client_cert.as_deref(), Some(&b"cert"[..]));
        assert_eq!(core.client_key.as_deref(), Some(&b"key"[..]));
        assert_eq!(core.domain_name.as_deref(), Some("orcher.internal"));
    }

    #[test]
    fn a_certificate_without_a_key_is_rejected() {
        let mut tls = ClientTlsConfig::new();
        tls.client_cert = Some(b"cert".to_vec());
        let err = endpoint("https://orcher.example", Some(&tls)).unwrap_err();
        assert!(err.contains("must be set together"), "{err}");
    }

    /// Connects to a bare TCP listener and returns the first bytes the client
    /// sent, which tell a TLS ClientHello from an HTTP/2 preface.
    async fn first_bytes_sent(scheme: &str) -> Vec<u8> {
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 16];
            let n = socket.read(&mut buf).await.unwrap();
            buf.truncate(n);
            buf
        });
        let endpoint = endpoint(&format!("{scheme}://localhost:{port}"), None)
            .expect("endpoint builds")
            .connect_timeout(Duration::from_secs(2));
        // The listener hangs up after reading, so the connect itself fails;
        // only what reached the wire matters.
        let _ = endpoint.connect().await;
        tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the client wrote something")
            .unwrap()
    }

    #[tokio::test]
    async fn an_https_endpoint_speaks_tls_without_explicit_settings() {
        let sent = first_bytes_sent("https").await;
        // A TLS record starts with content type 22 (handshake).
        assert_eq!(sent.first(), Some(&0x16), "sent {sent:?}");
    }

    #[tokio::test]
    async fn a_failed_handshake_names_its_cause() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(socket);
        });
        let err = endpoint(&format!("https://localhost:{port}"), None)
            .unwrap()
            .connect()
            .await
            .expect_err("must fail");
        let text = with_causes(&err);
        assert!(
            text.starts_with("transport error: ") && text.len() > 17,
            "{text}"
        );
    }

    #[tokio::test]
    async fn an_http_endpoint_stays_plaintext() {
        let sent = first_bytes_sent("http").await;
        assert!(sent.starts_with(b"PRI * HTTP/2.0"), "sent {sent:?}");
    }
}
